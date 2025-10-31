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

def cmd_executor(decoded_data, connection, config, executing=False):
    global db, subscriber_mode, REPLICAOF_MODE, MASTER_REPL_ID, MASTER_REPL_OFFSET
    db = config['store']

    if not decoded_data:
        return None, False

    cmd = decoded_data[0].upper()
    args = decoded_data[1:]
    queued = False

    # transaction queuing: if client is in MULTI mode, queue commands except control commands
    if connection in transaction_queues and cmd not in ["MULTI", "EXEC", "DISCARD"]:
        transaction_queues[connection].append(decoded_data)
        resp = simple_string_encoder("QUEUED")
        if executing:
            return resp, True
        connection.sendall(resp)
        return None, True

    # PING
    if cmd == "PING":
        resp = resp_encoder(["pong", ""]) if connection in subscriber_mode else simple_string_encoder("PONG")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # ECHO
    if cmd == "ECHO":
        msg = args[0] if args else b""
        resp = resp_encoder(msg)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # MULTI
    if cmd == "MULTI":
        transaction_queues[connection] = []
        resp = simple_string_encoder("OK")
        if executing:
            return resp, True
        connection.sendall(resp)
        return None, True

    # EXEC
    if cmd == "EXEC":
        if connection not in transaction_queues:
            connection.sendall(error_encoder("ERR EXEC without MULTI"))
            return None, False

        queued_cmds = transaction_queues.pop(connection, [])
        if not queued_cmds:
            connection.sendall(b"*0\r\n")
            return None, False

        # Execute each queued command in "executing" mode (so they return encoded bytes)
        results_encoded = []
        for q in queued_cmds:
            # q is a list like [b"SET", b"key", b"val"]
            res, _ = cmd_executor(q, connection, config, executing=True)
            # res should be bytes (RESP-encoded) when executing=True
            if isinstance(res, bytes):
                results_encoded.append(res)
            elif isinstance(res, str):
                results_encoded.append(resp_encoder(res))
            elif isinstance(res, int):
                results_encoded.append(resp_encoder(res))
            else:
                # null bulk / unknown
                results_encoded.append(resp_encoder(None))

        # Wrap all encoded results into a single RESP array and send once
        merged = array_encoder(results_encoded)
        connection.sendall(merged)
        return None, False

    # DISCARD
    if cmd == "DISCARD":
        if connection in transaction_queues:
            del transaction_queues[connection]
            resp = simple_string_encoder("OK")
        else:
            resp = error_encoder("ERR DISCARD without MULTI")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # SET
    if cmd == "SET":
        key, value = args[0], args[1]
        expiry = None
        if len(args) > 2:
            if args[2].upper() == "EX" and len(args) >= 4:
                expiry = datetime.datetime.now() + datetime.timedelta(seconds=int(args[3]))
            elif args[2].upper() == "PX" and len(args) >= 4:
                expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=int(args[3]))
        db[key] = Value(value=value, expiry=expiry)
        resp = simple_string_encoder("OK")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # GET
    if cmd == "GET":
        key = args[0]
        val_obj = db.get(key)
        if val_obj is None or (val_obj.expiry and datetime.datetime.now() >= val_obj.expiry):
            resp = resp_encoder(None)
        else:
            resp = resp_encoder(val_obj.value)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # INCR
    if cmd == "INCR":
        key = args[0]
        entry = db.get(key)
        if entry is None:
            db[key] = Value(value=b"1")
            resp = resp_encoder(1)
        else:
            try:
                new_val = int(entry.value.decode()) + 1
                db[key] = Value(value=str(new_val).encode())
                resp = resp_encoder(new_val)
            except Exception:
                resp = error_encoder("ERR value is not an integer or out of range")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # RPUSH
    if cmd == "RPUSH":
        key = args[0]
        values = args[1:]
        if key not in db or not isinstance(db[key].value, list):
            db[key] = Value(value=[])
        db[key].value.extend(values)
        resp = resp_encoder(len(db[key].value))
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # LRANGE
    if cmd == "LRANGE":
        key = args[0]
        start, stop = int(args[1]), int(args[2])
        if key not in db or not isinstance(db[key].value, list):
            resp = resp_encoder([])
        else:
            resp = resp_encoder(db[key].value[start:stop + 1])
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # EXPIRE
    if cmd == "EXPIRE":
        key = args[0]
        ttl = int(args[1])
        if key in db:
            db[key].expiry = datetime.datetime.now() + datetime.timedelta(seconds=ttl)
            resp = resp_encoder(1)
        else:
            resp = resp_encoder(0)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # KEYS
    if cmd == "KEYS":
        pattern = args[0]
        if pattern == "*":
            ks = list(db.keys())
        else:
            import re
            ks = [k for k in db.keys() if re.match(pattern.replace("*", ".*"), k)]
        resp = resp_encoder(ks)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # CONFIG GET
    if cmd == "CONFIG" and len(args) >= 2 and args[0].upper() == "GET":
        param = args[1]
        if param == "dir":
            resp = resp_encoder(["dir", "/tmp"])
        elif param == "dbfilename":
            resp = resp_encoder(["dbfilename", "dump.rdb"])
        else:
            resp = resp_encoder([])
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # INFO replication
    if cmd == "INFO":
        section = args[0].lower() if args else "all"
        if section == "replication":
            role = "master" if not globals().get("REPLICAOF_MODE") else "slave"
            info_str = f"role:{role}\nmaster_replid:{globals().get('MASTER_REPL_ID','')}\nmaster_repl_offset:{globals().get('MASTER_REPL_OFFSET',0)}"
            resp = resp_encoder(info_str.encode() if isinstance(info_str, str) else info_str)
        else:
            resp = resp_encoder("ok")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # REPLCONF
    if cmd == "REPLCONF":
        resp = simple_string_encoder("OK")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # PSYNC
    if cmd == "PSYNC":
        resp = simple_string_encoder(f"FULLRESYNC {globals().get('MASTER_REPL_ID','')} {globals().get('MASTER_REPL_OFFSET',0)}")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # Unknown
    resp = error_encoder(f"ERR unknown command '{cmd}'")
    if executing:
        return resp, False
    connection.sendall(resp)
    return None, False


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