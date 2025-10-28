import socket
import threading
import time

data_store = {}
list_blocked_clients = {}  # key -> list of (conn, start_time, timeout)

def encode_simple_string(s): return f"+{s}\r\n".encode()
def encode_error(s): return f"-{s}\r\n".encode()
def encode_integer(n): return f":{n}\r\n".encode()
def encode_bulk_string(s):
    if s is None: return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()
def encode_array(arr):
    if arr is None: return b"*-1\r\n"
    return f"*{len(arr)}\r\n".encode() + b"".join(
        encode_bulk_string(x) for x in arr
    )

def parse_command(data):
    parts = data.split(b"\r\n")
    if not parts or parts[0] == b"": return []
    arr_len = int(parts[0][1:])
    args = []
    i = 2
    for _ in range(arr_len):
        args.append(parts[i - 1 + 1].decode())
        i += 2
    return args

def handle_client(conn):
    buffer = b""
    while True:
        chunk = conn.recv(1024)
        if not chunk: break
        buffer += chunk
        if b"\r\n" not in buffer: continue
        if buffer.startswith(b"*"):
            try:
                args = parse_command(buffer)
            except Exception:
                conn.sendall(encode_error("ERR parse error"))
                buffer = b""
                continue
            buffer = b""
            if not args: continue
            command = args[0].upper()

            if command == "PING":
                resp = encode_simple_string("PONG") if len(args) == 1 else encode_bulk_string(args[1])
                conn.sendall(resp)

            elif command == "ECHO":
                conn.sendall(encode_bulk_string(args[1]))

            elif command == "SET":
                key, value = args[1], args[2]
                data_store[key] = value
                conn.sendall(encode_simple_string("OK"))

            elif command == "GET":
                val = data_store.get(args[1])
                conn.sendall(encode_bulk_string(val))

            elif command in ["LPUSH", "RPUSH"]:
                key = args[1]
                values = args[2:]
                lst = data_store.get(key, [])
                if not isinstance(lst, list): lst = []
                if command == "LPUSH":
                    for v in values: lst.insert(0, v)
                else:
                    for v in values: lst.append(v)
                data_store[key] = lst
                conn.sendall(encode_integer(len(lst)))

                # wake up any BLPOP clients waiting
                if key in list_blocked_clients and lst:
                    blocked = list_blocked_clients.pop(key)
                    val = lst.pop(0)
                    data_store[key] = lst
                    for c, _, _ in blocked:
                        c.sendall(encode_array([key, val]))

            elif command in ["LPOP", "RPOP"]:
                key = args[1]
                lst = data_store.get(key, [])
                if not lst:
                    conn.sendall(encode_bulk_string(None))
                else:
                    val = lst.pop(0) if command == "LPOP" else lst.pop()
                    data_store[key] = lst
                    conn.sendall(encode_bulk_string(val))

            elif command == "LRANGE":
                key, start, end = args[1], int(args[2]), int(args[3])
                lst = data_store.get(key, [])
                if not isinstance(lst, list): lst = []
                if end < 0: end = len(lst) + end
                end = min(end, len(lst) - 1)
                sub = lst[start:end + 1] if lst else []
                conn.sendall(encode_array(sub))

            elif command == "BLPOP":
                keys = args[1:-1]
                timeout = int(args[-1])
                found = False
                for key in keys:
                    lst = data_store.get(key, [])
                    if lst:
                        val = lst.pop(0)
                        data_store[key] = lst
                        conn.sendall(encode_array([key, val]))
                        found = True
                        break
                if not found:
                    for key in keys:
                        list_blocked_clients.setdefault(key, []).append((conn, time.time(), timeout))
                    # don’t send yet — wait for data or timeout

            else:
                conn.sendall(encode_error(f"ERR unknown command '{command}'"))

def unblock_thread():
    while True:
        time.sleep(0.5)
        now = time.time()
        for key, waiters in list(list_blocked_clients.items()):
            still_waiting = []
            for conn, start, timeout in waiters:
                if now - start >= timeout:
                    conn.sendall(encode_bulk_string(None))
                else:
                    still_waiting.append((conn, start, timeout))
            if still_waiting:
                list_blocked_clients[key] = still_waiting
            else:
                del list_blocked_clients[key]

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 6379))
    server.listen(5)
    threading.Thread(target=unblock_thread, daemon=True).start()
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()