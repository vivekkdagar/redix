import socket
import threading
import time
from collections import defaultdict

# ==============================
# In-memory data store
# ==============================
data_store = {}
expiry_store = {}

# For blocking list operations
blocking_conditions = defaultdict(threading.Condition)

# ==============================
# RESP encoding helpers
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
# Command Implementations
# ==============================
def handle_ping(args):
    if len(args) == 1:
        return encode_simple_string("PONG")
    else:
        return encode_bulk_string(args[1])

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

# ==============================
# List Operations
# ==============================
def handle_lpush(args):
    key = args[1]
    values = args[2:]
    if key not in data_store:
        data_store[key] = []
    for v in values:
        data_store[key].insert(0, v)
    # Notify waiting BLPOP threads
    cond = blocking_conditions[key]
    with cond:
        cond.notify_all()
    return encode_integer(len(data_store[key]))

def handle_rpush(args):
    key = args[1]
    values = args[2:]
    if key not in data_store:
        data_store[key] = []
    data_store[key].extend(values)
    # Notify waiting BLPOP threads
    cond = blocking_conditions[key]
    with cond:
        cond.notify_all()
    return encode_integer(len(data_store[key]))

def handle_lrange(args):
    key, start, end = args[1], int(args[2]), int(args[3])
    if key not in data_store:
        return encode_array([])
    lst = data_store[key]
    n = len(lst)
    if start < 0:
        start = n + start
    if end < 0:
        end = n + end
    start = max(start, 0)
    end = min(end, n - 1)
    if start > end:
        return encode_array([])
    return encode_array(lst[start:end+1])

def handle_llen(args):
    key = args[1]
    if key not in data_store:
        return encode_integer(0)
    return encode_integer(len(data_store[key]))

def handle_lpop(args):
    key = args[1]
    count = int(args[2]) if len(args) > 2 else 1
    if key not in data_store or len(data_store[key]) == 0:
        return encode_bulk_string(None) if count == 1 else encode_array([])
    lst = data_store[key]
    popped = []
    for _ in range(min(count, len(lst))):
        popped.append(lst.pop(0))
    if count == 1:
        return encode_bulk_string(popped[0])
    return encode_array(popped)

# ==============================
# BLPOP (Blocking pop)
# ==============================
def handle_blpop(args):
    keys = args[1:-1]
    timeout = float(args[-1])
    end_time = time.time() + timeout

    while time.time() < end_time:
        for key in keys:
            if key in data_store and isinstance(data_store[key], list) and len(data_store[key]) > 0:
                val = data_store[key].pop(0)
                return encode_array([key, val])
        # Wait for push notification
        for key in keys:
            cond = blocking_conditions[key]
            with cond:
                remaining = end_time - time.time()
                if remaining <= 0:
                    break
                cond.wait(timeout=remaining)
    # Timeout reached, return null array
    return b"*-1\r\n"

# ==============================
# Command Dispatcher
# ==============================
def execute_command(args):
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
    elif cmd == "LRANGE":
        return handle_lrange(args)
    elif cmd == "LLEN":
        return handle_llen(args)
    elif cmd == "LPOP":
        return handle_lpop(args)
    elif cmd == "BLPOP":
        return handle_blpop(args)
    else:
        return encode_error(f"ERR unknown command '{cmd}'")

# ==============================
# RESP parser
# ==============================
def parse_resp_message(conn):
    line = conn.recv(1024).decode()
    if not line:
        return None
    if line[0] != '*':
        return None
    num_args = int(line[1:].strip())
    args = []
    for _ in range(num_args):
        conn.recv(1)  # skip $
        arg_len = int(conn.recv(1024).decode().strip())
        arg = conn.recv(arg_len).decode()
        conn.recv(2)  # \r\n
        args.append(arg)
    return args

# ==============================
# Client handler
# ==============================
def handle_client(conn):
    try:
        while True:
            args = parse_resp_message(conn)
            if not args:
                break
            response = execute_command(args)
            conn.sendall(response)
    except Exception as e:
        print(f"Client error: {e}")
    finally:
        conn.close()

# ==============================
# Main entrypoint
# ==============================
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(5)
    print("Redis clone running on port 6379")
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()