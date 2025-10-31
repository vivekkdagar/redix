from app.resp import resp_parser, resp_encoder, simple_string_encoder, error_encoder, array_encoder, parse_all
from app.utils import getter, setter, rpush, lrange, lpush, llen, lpop, blpop, type_getter_lists, increment, store_rdb, \
    keys
from app.utils2 import xadd, type_getter_streams, xrange, xread, blocks_xread
from time import sleep
import threading

blocked = {}
blocked_xread = {}
queue = []
REPLICAS = []
BYTES_READ = 0
replica_acks = 0
prev_cmd = ""
SUBSCRIBE = 0

RDB_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'

subscriptions = {}     # {connection: set(channel_names)}
channel_subs = {}      # {channel_name: set(connections)}
subscriber_mode = set()  # track connections in SUBSCRIBE mode
transaction_queues = {}  # global transaction state

import datetime

class Value:
    def __init__(self, value, expiry=None):
        self.value = value
        self.expiry = expiry

db = {}

def cmd_executor(decoded_data, connection, config, executing=False):
    global db, subscriber_mode, REPLICAOF_MODE, MASTER_REPL_ID, MASTER_REPL_OFFSET
    db = config['store']
    if not decoded_data:
        return [], False

    cmd = decoded_data[0].upper()
    args = decoded_data[1:]
    response = None
    queued = False

    # --- Transaction queuing logic ---
    if connection in transaction_queues and cmd not in ["MULTI", "EXEC", "DISCARD"]:
        transaction_queues[connection].append(decoded_data)
        response = simple_string_encoder("QUEUED")
        if executing:
            return response, True
        connection.sendall(response)
        return [], True

    # --- PING ---
    if cmd == "PING":
        if connection in subscriber_mode:
            response = resp_encoder(["pong", ""])
        else:
            response = simple_string_encoder("PONG")
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- ECHO ---
    elif cmd == "ECHO":
        msg = args[0] if args else ""
        response = bulk_string_encoder(msg)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- MULTI ---

    elif cmd == "MULTI":

        transaction_queues[connection] = []

        connection.sendall(simple_string_encoder("OK"))

        return [], True

    # --- EXEC ---
    elif cmd == "EXEC":
        if connection not in transaction_queues:
            connection.sendall(error_encoder("ERR EXEC without MULTI"))
            return [], False

        queued_cmds = transaction_queues.pop(connection, [])

        if not queued_cmds:
            connection.sendall(b"*0\r\n")
            return [], False

        # Send RESP array header first
        connection.sendall(f"*{len(queued_cmds)}\r\n".encode())

        for queued in queued_cmds:
            res, _ = cmd_executor(queued, connection, executing=True)
            if isinstance(res, bytes):
                connection.sendall(res)
            elif isinstance(res, str):
                connection.sendall(res.encode())
            elif isinstance(res, list):
                connection.sendall(resp_encoder(res))
            else:
                connection.sendall(b"$-1\r\n")

        return [], False

    # --- DISCARD ---
    elif cmd == "DISCARD":
        if connection in transaction_queues:
            del transaction_queues[connection]
            response = simple_string_encoder("OK")
        else:
            response = error_encoder("ERR DISCARD without MULTI")
        connection.sendall(response)
        return [], False

    # --- SET ---
    elif cmd == "SET":
        key, value = args[0], args[1]
        expiry = None
        if len(args) > 2:
            if args[2].upper() == "EX" and len(args) >= 4:
                expiry = datetime.datetime.now() + datetime.timedelta(seconds=int(args[3]))
            elif args[2].upper() == "PX" and len(args) >= 4:
                expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=int(args[3]))
        db[key] = Value(value=value, expiry=expiry)
        response = simple_string_encoder("OK")
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- GET ---
    elif cmd == "GET":
        key = args[0]
        value = db.get(key)
        if value is None or (value.expiry and datetime.datetime.now() >= value.expiry):
            response = bulk_string_encoder(None)
        else:
            response = bulk_string_encoder(value.value)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- INCR ---
    elif cmd == "INCR":
        key = args[0]
        entry = db.get(key)
        if entry is None:
            db[key] = Value(value="1")
            response = integer_encoder(1)
        else:
            try:
                new_val = int(entry.value) + 1
                db[key] = Value(value=str(new_val))
                response = integer_encoder(new_val)
            except ValueError:
                response = error_encoder("ERR value is not an integer or out of range")
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- RPUSH ---
    elif cmd == "RPUSH":
        key = args[0]
        values = args[1:]
        if key not in db or not isinstance(db[key].value, list):
            db[key] = Value(value=[])
        db[key].value.extend(values)
        response = integer_encoder(len(db[key].value))
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- LRANGE ---
    elif cmd == "LRANGE":
        key = args[0]
        start, stop = int(args[1]), int(args[2])
        if key not in db or not isinstance(db[key].value, list):
            response = resp_encoder([])
        else:
            response = resp_encoder(db[key].value[start:stop + 1])
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- EXPIRE ---
    elif cmd == "EXPIRE":
        key = args[0]
        ttl = int(args[1])
        if key in db:
            db[key].expiry = datetime.datetime.now() + datetime.timedelta(seconds=ttl)
            response = integer_encoder(1)
        else:
            response = integer_encoder(0)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- KEYS ---
    elif cmd == "KEYS":
        pattern = args[0]
        if pattern == "*":
            keys = list(db.keys())
        else:
            keys = [k for k in db.keys() if re.match(pattern.replace("*", ".*"), k)]
        response = resp_encoder(keys)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- CONFIG GET ---
    elif cmd == "CONFIG" and len(args) >= 2 and args[0].upper() == "GET":
        param = args[1]
        if param == "dir":
            response = resp_encoder(["dir", "/tmp"])
        elif param == "dbfilename":
            response = resp_encoder(["dbfilename", "dump.rdb"])
        else:
            response = resp_encoder([])
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- INFO replication ---
    elif cmd == "INFO":
        section = args[0].lower() if args else "all"
        if section == "replication":
            role = "master" if not REPLICAOF_MODE else "slave"
            info_str = f"role:{role}\nmaster_replid:{MASTER_REPL_ID}\nmaster_repl_offset:{MASTER_REPL_OFFSET}"
            response = bulk_string_encoder(info_str)
        else:
            response = bulk_string_encoder("ok")
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --- REPLCONF ---
    elif cmd == "REPLCONF":
        response = simple_string_encoder("OK")
        connection.sendall(response)
        return [], queued

    # --- PSYNC ---
    elif cmd == "PSYNC":
        response = simple_string_encoder(f"+FULLRESYNC {MASTER_REPL_ID} {MASTER_REPL_OFFSET}")
        connection.sendall(response)
        return [], queued

    # --- UNKNOWN ---
    else:
        response = error_encoder(f"ERR unknown command '{cmd}'")
        connection.sendall(response)
        return [], queued

def handle_client(connection, config, data=b""):
    global prev_cmd
    # store the data
    print(config)
    store_rdb(config['store'])
    print(f"handle_client called with data: {data}")
    buffer = data
    queued = False
    executing = False
    with connection:
        while True:
            print(f"handle_client called with buffer: {data}")
            if b"FULL" in data:
                _, data = data.split(b"\r\n", 1)
            print(f"handle_client called with buffer2: {buffer}")
            if not buffer:
                chunk = connection.recv(1024)
                if not chunk:
                    break
                buffer += chunk
            # else:
            #     chunk = connection.recv(1024)
            #     if not chunk:
            #         break
            #     buffer += chunk

            print(f"Received chunk: {buffer}")

            # Try to parse one or more complete RESP messages from buffer
            messages = parse_all(buffer)
            if not messages:
                continue  # incomplete data, wait for more

            for msg in messages:
                try:
                    # Convert bulk strings (bytes) to text
                    decoded_data = [
                        x.decode() if isinstance(x, (bytes, bytearray)) else x
                        for x in msg
                    ]

                    print(f"Parsed command: {decoded_data}")

                    if decoded_data[0] == 82 or decoded_data[0] == 'F':
                        continue

                    res, _ = cmd_executor(queued, connection, config, executing=True)

                    prev_cmd = decoded_data[0]

                except Exception as e:
                    print(f"Error handling command {msg}: {e}")

            buffer = b""