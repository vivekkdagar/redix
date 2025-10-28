import socket
import threading
import time
import sys
import os
import struct
from collections import defaultdict

# ==============================
# Global stores
# ==============================
data_store = {}
expiry_store = {}
blocking_conditions = defaultdict(threading.Condition)

# Default config (can be overridden via CLI args)
config = {
    "dir": ".",
    "dbfilename": "dump.rdb"
}

# ==============================
# RESP helpers
# ==============================
def encode_simple_string(s):
    return f"+{s}\r\n".encode()

def encode_error(msg):
    return f"-{msg}\r\n".encode()

def encode_integer(i):
    return f":{i}\r\n".encode()

def encode_bulk_string(s):
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()

def encode_array(items):
    if items is None:
        return b"*-1\r\n"
    out = f"*{len(items)}\r\n"
    for it in items:
        if it is None:
            out += "$-1\r\n"
        else:
            out += f"${len(it)}\r\n{it}\r\n"
    return out.encode()

# ==============================
# RESP parser (binary-safe)
# ==============================
def read_line(conn):
    buf = b""
    while not buf.endswith(b"\r\n"):
        chunk = conn.recv(1)
        if not chunk:
            return None
        buf += chunk
    return buf[:-2].decode()

def parse_resp_message(conn):
    line = read_line(conn)
    if not line:
        return None
    if line[0] != '*':
        return None
    num_args = int(line[1:])
    args = []
    for _ in range(num_args):
        line = read_line(conn)
        if not line or line[0] != '$':
            return None
        length = int(line[1:])
        if length == -1:
            args.append(None)
            continue
        arg = b""
        while len(arg) < length:
            chunk = conn.recv(length - len(arg))
            if not chunk:
                return None
            arg += chunk
        conn.recv(2)  # consume \r\n
        args.append(arg.decode())
    return args

# ==============================
# Basic RDB Loader (for KEYS *)
# ==============================
def load_rdb_file(dir_path, filename):
    path = os.path.join(dir_path, filename)
    if not os.path.exists(path):
        print(f"No RDB file found at {path}. Starting with empty DB.")
        return

    try:
        with open(path, "rb") as f:
            data = f.read()
        # RDB files start with "REDIS" signature
        if not data.startswith(b"REDIS"):
            print("Invalid RDB file signature.")
            return
        # Extremely simplified parser: just detect key names in ASCII
        # (Not full RDB decode, enough for KEYS * in early stages)
        # We'll look for printable strings between known markers.
        keys_found = []
        i = 0
        while i < len(data):
            if data[i] == 0x00:  # type: string key
                # next is length-prefixed key
                keylen = data[i + 1]
                key = data[i + 2 : i + 2 + keylen].decode(errors="ignore")
                keys_found.append(key)
                i += 2 + keylen
            else:
                i += 1
        for k in keys_found:
            data_store[k] = "(rdb_loaded_value)"
        print(f"Loaded keys from RDB: {keys_found}")
    except Exception as e:
        print(f"Error reading RDB file: {e}")

# ==============================
# Commands
# ==============================
def handle_ping(args):
    return encode_bulk_string(args[1]) if len(args) > 1 else encode_simple_string("PONG")

def handle_echo(args):
    return encode_bulk_string(args[1])

def handle_set(args):
    key, val = args[1], args[2]
    data_store[key] = val
    expiry_store.pop(key, None)
    if len(args) > 3 and args[3].upper() == "PX":
        expiry_store[key] = time.time() + int(args[4]) / 1000.0
    return encode_simple_string("OK")

def handle_get(args):
    key = args[1]
    if key in expiry_store and time.time() > expiry_store[key]:
        data_store.pop(key, None)
        expiry_store.pop(key, None)
        return encode_bulk_string(None)
    return encode_bulk_string(data_store.get(key))

def handle_lpush(args):
    key, values = args[1], args[2:]
    if key not in data_store:
        data_store[key] = []
    for v in values:
        data_store[key].insert(0, v)
    with blocking_conditions[key]:
        blocking_conditions[key].notify_all()
    return encode_integer(len(data_store[key]))

def handle_rpush(args):
    key, values = args[1], args[2:]
    if key not in data_store:
        data_store[key] = []
    data_store[key].extend(values)
    with blocking_conditions[key]:
        blocking_conditions[key].notify_all()
    return encode_integer(len(data_store[key]))

def handle_lrange(args):
    key, start, end = args[1], int(args[2]), int(args[3])
    if key not in data_store:
        return encode_array([])
    lst = data_store[key]
    n = len(lst)
    if start < 0: start = n + start
    if end < 0: end = n + end
    start = max(start, 0)
    end = min(end, n - 1)
    if start > end:
        return encode_array([])
    return encode_array(lst[start:end+1])

def handle_llen(args):
    key = args[1]
    return encode_integer(len(data_store.get(key, [])))

def handle_lpop(args):
    key = args[1]
    count = int(args[2]) if len(args) > 2 else 1
    if key not in data_store or len(data_store[key]) == 0:
        return encode_bulk_string(None) if count == 1 else encode_array([])
    lst = data_store[key]
    popped = [lst.pop(0) for _ in range(min(count, len(lst)))]
    if count == 1:
        return encode_bulk_string(popped[0])
    return encode_array(popped)

def handle_blpop(args):
    keys = args[1:-1]
    timeout = float(args[-1])
    end_time = time.time() + timeout if timeout > 0 else None

    while True:
        for key in keys:
            if key in data_store and data_store[key]:
                val = data_store[key].pop(0)
                return encode_array([key, val])
        if end_time and time.time() > end_time:
            return b"*-1\r\n"
        for key in keys:
            cond = blocking_conditions[key]
            with cond:
                cond.wait(timeout=timeout if end_time else None)
                break

def handle_config_get(args):
    param = args[1]
    if param in config:
        return encode_array([param, config[param]])
    else:
        return encode_array([])

def handle_keys(args):
    if len(args) < 2 or args[1] != "*":
        return encode_array([])
    return encode_array(list(data_store.keys()))

# ==============================
# Dispatcher
# ==============================
def execute_command(args):
    cmd = args[0].upper()
    if cmd == "PING": return handle_ping(args)
    if cmd == "ECHO": return handle_echo(args)
    if cmd == "SET": return handle_set(args)
    if cmd == "GET": return handle_get(args)
    if cmd == "LPUSH": return handle_lpush(args)
    if cmd == "RPUSH": return handle_rpush(args)
    if cmd == "LRANGE": return handle_lrange(args)
    if cmd == "LLEN": return handle_llen(args)
    if cmd == "LPOP": return handle_lpop(args)
    if cmd == "BLPOP": return handle_blpop(args)
    if cmd == "CONFIG" and len(args) > 1 and args[1].upper() == "GET":
        return handle_config_get(args[1:])
    if cmd == "KEYS": return handle_keys(args)
    return encode_error(f"ERR unknown command '{cmd}'")

# ==============================
# Networking
# ==============================
def handle_client(conn):
    try:
        while True:
            args = parse_resp_message(conn)
            if not args:
                break
            resp = execute_command(args)
            conn.sendall(resp)
    except Exception as e:
        print(f"Client error: {e}")
    finally:
        conn.close()

# ==============================
# Main
# ==============================
def main():
    # Parse CLI args for --dir and --dbfilename
    for i, arg in enumerate(sys.argv):
        if arg == "--dir" and i + 1 < len(sys.argv):
            config["dir"] = sys.argv[i + 1]
        elif arg == "--dbfilename" and i + 1 < len(sys.argv):
            config["dbfilename"] = sys.argv[i + 1]

    load_rdb_file(config["dir"], config["dbfilename"])

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(5)
    print(f"Redis clone running on port 6379 with dir={config['dir']} dbfilename={config['dbfilename']}")
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()