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

# ----- Replace cmd_executor and handle_client with the code below -----

db = {}

transaction_queues = {}

def cmd_executor(decoded_data, connection, config, executing=False):
    global db
    db = config['store']
    if not decoded_data:
        return [], False

    cmd = decoded_data[0].upper()
    args = decoded_data[1:]
    response = None
    queued = False

    # --- Queueing inside MULTI ---
    if connection in transaction_queues and cmd not in ["MULTI", "EXEC", "DISCARD"]:
        transaction_queues[connection].append(decoded_data)
        response = simple_string_encoder("QUEUED")
        if executing:
            return response, True
        connection.sendall(response)
        return [], True

    # --- MULTI ---
    if cmd == "MULTI":
        transaction_queues[connection] = []
        connection.sendall(simple_string_encoder("OK"))
        return [], True

    # --- EXEC ---
    elif cmd == "EXEC":
        if connection not in transaction_queues:
            connection.sendall(error_encoder("ERR EXEC without MULTI"))
            return [], False

        queued_cmds = transaction_queues.pop(connection)
        if not queued_cmds:
            connection.sendall(b"*0\r\n")
            return [], False

        results = []
        for queued in queued_cmds:
            try:
                res, _ = cmd_executor(queued, connection, config, executing=True)
            except Exception:
                res = error_encoder("EXECABORT Transaction discarded because of previous errors.")

            if res is None:
                qcmd = queued[0].upper()
                if qcmd == "SET":
                    res = simple_string_encoder("OK")
                elif qcmd in ("INCR", "DECR"):
                    val = db.get(queued[1])
                    num = int(val.value) if isinstance(val, Value) else 0
                    res = integer_encoder(num)
                elif qcmd == "GET":
                    val = db.get(queued[1])
                    if val is None or (val.expiry and datetime.datetime.now() >= val.expiry):
                        res = bulk_string_encoder(None)
                    else:
                        res = bulk_string_encoder(val.value)
                else:
                    res = simple_string_encoder("OK")
            results.append(res)

        merged = f"*{len(results)}\r\n".encode()
        for r in results:
            if isinstance(r, bytes):
                merged += r
            elif isinstance(r, str):
                merged += r.encode()
            else:
                merged += simple_string_encoder("OK")

        connection.sendall(merged)
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

    # --- PING ---
    elif cmd == "PING":
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

    # --- Default ---
    else:
        response = error_encoder(f"ERR unknown command '{cmd}'")
        connection.sendall(response)
        return [], queued


def handle_client(connection, config, data=b""):
    global prev_cmd
    store_rdb(config['store'])
    buffer = data
    queued = False
    executing = False
    with connection:
        while True:
            if b"FULL" in buffer:
                _, buffer = buffer.split(b"\r\n", 1)

            if not buffer:
                chunk = connection.recv(65536)
                if not chunk:
                    break
                buffer += chunk

            messages = parse_all(buffer)
            if not messages:
                continue

            for msg in messages:
                try:
                    decoded_data = [
                        x if isinstance(x, (bytes, bytearray)) else x
                        for x in msg
                    ]

                    # call cmd_executor with config; let cmd_executor handle sending unless it returns bytes in executing mode
                    res, queued = cmd_executor(decoded_data, connection, config, executing=False)

                    prev_cmd = decoded_data[0]

                    # If cmd_executor returned bytes when not already sent, send them.
                    if res and isinstance(res, (bytes, bytearray)):
                        connection.sendall(res)

                except Exception as e:
                    print(f"Error handling command {msg}: {e}")

            buffer = b""