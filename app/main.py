import socket
import threading
import time
import sys
import os

# ============================================================
# In-memory store and configuration
# ============================================================
data_store = {}
expiry_times = {}
config = {
    "dir": "/tmp",
    "dbfilename": "dump.rdb"
}
blocking_queues = {}

# ============================================================
# RESP Encoding Helpers
# ============================================================

def encode_simple_string(s):
    return f"+{s}\r\n".encode()

def encode_bulk_string(s):
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()

def encode_integer(num):
    return f":{num}\r\n".encode()

def encode_array(arr):
    if arr is None:
        return b"*-1\r\n"
    res = f"*{len(arr)}\r\n"
    for item in arr:
        if item is None:
            res += "$-1\r\n"
        else:
            res += f"${len(item)}\r\n{item}\r\n"
    return res.encode()

def decode_resp(data):
    """Basic RESP command decoder"""
    parts = []
    if not data.startswith(b'*'):
        return []
    lines = data.split(b'\r\n')
    i = 1
    while i < len(lines):
        line = lines[i]
        if line.startswith(b'$'):
            length = int(line[1:])
            i += 1
            parts.append(lines[i].decode())
        i += 1
    return parts

# ============================================================
# Expiry Utilities
# ============================================================

def is_expired(key):
    if key in expiry_times:
        if time.time() > expiry_times[key]:
            del expiry_times[key]
            if key in data_store:
                del data_store[key]
            return True
    return False

# ============================================================
# Command Implementations
# ============================================================

def handle_ping(args):
    if len(args) == 1:
        return encode_simple_string("PONG")
    else:
        return encode_bulk_string(args[1])

def handle_echo(args):
    if len(args) < 2:
        return encode_bulk_string("")
    return encode_bulk_string(args[1])

def handle_set(args):
    if len(args) < 3:
        return encode_simple_string("ERR wrong number of arguments for 'set' command")

    key = args[1]
    value = args[2]
    data_store[key] = value

    if len(args) > 3:
        if args[3].upper() == "PX":
            expiry_ms = int(args[4])
            expiry_times[key] = time.time() + expiry_ms / 1000.0
        elif args[3].upper() == "EX":
            expiry_s = int(args[4])
            expiry_times[key] = time.time() + expiry_s

    return encode_simple_string("OK")

def handle_get(args):
    if len(args) < 2:
        return encode_bulk_string(None)
    key = args[1]
    if key not in data_store or is_expired(key):
        return encode_bulk_string(None)
    return encode_bulk_string(data_store[key])

# ============================================================
# List Commands
# ============================================================

def handle_lpush(args):
    key = args[1]
    values = args[2:]
    if key not in data_store:
        data_store[key] = []
    for v in values:
        data_store[key].insert(0, v)
    return encode_integer(len(data_store[key]))

def handle_rpush(args):
    key = args[1]
    values = args[2:]
    if key not in data_store:
        data_store[key] = []
    data_store[key].extend(values)
    return encode_integer(len(data_store[key]))

def handle_llen(args):
    key = args[1]
    if key not in data_store or not isinstance(data_store[key], list):
        return encode_integer(0)
    return encode_integer(len(data_store[key]))

def handle_lrange(args):
    key = args[1]
    start = int(args[2])
    end = int(args[3])
    if key not in data_store or not isinstance(data_store[key], list):
        return encode_array([])
    lst = data_store[key]
    if end == -1:
        end = len(lst) - 1
    result = lst[start:end + 1]
    return encode_array(result)

def handle_lpop(args):
    key = args[1]
    if key not in data_store or not isinstance(data_store[key], list) or len(data_store[key]) == 0:
        return encode_bulk_string(None)
    val = data_store[key].pop(0)
    return encode_bulk_string(val)

def handle_rpop(args):
    key = args[1]
    if key not in data_store or not isinstance(data_store[key], list) or len(data_store[key]) == 0:
        return encode_bulk_string(None)
    val = data_store[key].pop()
    return encode_bulk_string(val)

# ============================================================
# Blocking POP
# ============================================================

def handle_blpop(args):
    keys = args[1:-1]
    timeout = float(args[-1])

    end_time = time.time() + timeout
    while time.time() < end_time:
        for key in keys:
            if key in data_store and isinstance(data_store[key], list) and len(data_store[key]) > 0:
                val = data_store[key].pop(0)
                return encode_array([key, val])
        time.sleep(0.05)
    return b"*-1\r\n"

# ============================================================
# CONFIG command for RDB setup
# ============================================================

def handle_config(args):
    if len(args) >= 3 and args[1].upper() == "GET":
        param = args[2]
        if param in config:
            return encode_array([param, config[param]])
        else:
            return encode_array([])
    return encode_simple_string("ERR wrong config command")

# ============================================================
# Command Dispatcher
# ============================================================

def handle_command(args):
    if len(args) == 0:
        return b""

    cmd = args[0].upper()

    if cmd == "PING":
        return handle_ping(args)
    elif cmd == "ECHO":
        return handle_echo(args)
    elif cmd == "SET":
        return handle_set(args)
    elif cmd == "GET":
        return handle_get(args)
    elif cmd == "LPUSH":
        return handle_lpush(args)
    elif cmd == "RPUSH":
        return handle_rpush(args)
    elif cmd == "LLEN":
        return handle_llen(args)
    elif cmd == "LRANGE":
        return handle_lrange(args)
    elif cmd == "LPOP":
        return handle_lpop(args)
    elif cmd == "RPOP":
        return handle_rpop(args)
    elif cmd == "BLPOP":
        return handle_blpop(args)
    elif cmd == "CONFIG":
        return handle_config(args)
    else:
        return encode_simple_string("ERR unknown command")

# ============================================================
# Client Handler
# ============================================================

def handle_client(conn):
    with conn:
        buf = b""
        while True:
            data = conn.recv(1024)
            if not data:
                break
            buf += data
            if buf.endswith(b"\r\n"):
                args = decode_resp(buf)
                response = handle_command(args)
                conn.sendall(response)
                buf = b""

# ============================================================
# Main Entry Point
# ============================================================

def main():
    # parse args for dir and dbfilename
    args = sys.argv[1:]
    for i in range(len(args)):
        if args[i] == "--dir":
            config["dir"] = args[i + 1]
        elif args[i] == "--dbfilename":
            config["dbfilename"] = args[i + 1]

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(5)

    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()