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
    return f"*{len(arr)}\r\n".encode() + b"".join(encode_bulk_string(x) for x in arr)

def parse_command(data):
    parts = data.split(b"\r\n")
    if not parts or not parts[0].startswith(b"*"): return []
    arr_len = int(parts[0][1:])
    args = []
    idx = 2
    for _ in range(arr_len):
        args.append(parts[idx - 1 + 1].decode())
        idx += 2
    return args

def handle_client(conn):
    buffer = b""
    while True:
        chunk = conn.recv(1024)
        if not chunk: break
        buffer += chunk
        if b"\r\n" not in buffer: continue
        if not buffer.startswith(b"*"): continue
        try:
            args = parse_command(buffer)
        except Exception:
            conn.sendall(encode_error("ERR parse error"))
            buffer = b""
            continue
        buffer = b""
        if not args: continue

        cmd = args[0].upper()

        # ---------------- PING / ECHO / GET / SET ----------------
        if cmd == "PING":
            conn.sendall(encode_simple_string("PONG") if len(args) == 1 else encode_bulk_string(args[1]))

        elif cmd == "ECHO":
            conn.sendall(encode_bulk_string(args[1]))

        elif cmd == "SET":
            key, value = args[1], args[2]
            data_store[key] = value
            conn.sendall(encode_simple_string("OK"))

        elif cmd == "GET":
            conn.sendall(encode_bulk_string(data_store.get(args[1])))

        # ---------------- LIST COMMANDS ----------------
        elif cmd in ["LPUSH", "RPUSH"]:
            key, values = args[1], args[2:]
            lst = data_store.get(key, [])
            if not isinstance(lst, list): lst = []
            if cmd == "LPUSH":
                for v in values: lst.insert(0, v)
            else:
                for v in values: lst.append(v)
            data_store[key] = lst
            conn.sendall(encode_integer(len(lst)))

            # wake any blocked BLPOP clients
            if key in list_blocked_clients and lst:
                blocked = list_blocked_clients.pop(key)
                val = lst.pop(0)
                data_store[key] = lst
                for c, _, _ in blocked:
                    try: c.sendall(encode_array([key, val]))
                    except: pass

        elif cmd in ["LPOP", "RPOP"]:
            key = args[1]
            lst = data_store.get(key, [])
            if not lst:
                conn.sendall(encode_bulk_string(None))
            else:
                val = lst.pop(0) if cmd == "LPOP" else lst.pop()
                data_store[key] = lst
                conn.sendall(encode_bulk_string(val))

        elif cmd == "LRANGE":
            key, start, end = args[1], int(args[2]), int(args[3])
            lst = data_store.get(key, [])
            if not isinstance(lst, list): lst = []
            if end < 0: end = len(lst) + end
            end = min(end, len(lst) - 1)
            sub = lst[start:end + 1] if lst else []
            conn.sendall(encode_array(sub))

        # ---------------- BLPOP ----------------
        elif cmd == "BLPOP":
            keys = args[1:-1]
            try:
                timeout = float(args[-1])
            except ValueError:
                timeout = 0.0  # default if malformed

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

        else:
            conn.sendall(encode_error(f"ERR unknown command '{cmd}'"))


def unblock_thread():
    while True:
        time.sleep(0.05)
        now = time.time()
        for key, waiters in list(list_blocked_clients.items()):
            still_waiting = []
            for conn, start, timeout in waiters:
                if now - start >= timeout:
                    try:
                        conn.sendall(encode_array(None))
                    except: pass
                else:
                    still_waiting.append((conn, start, timeout))
            if still_waiting:
                list_blocked_clients[key] = still_waiting
            else:
                list_blocked_clients.pop(key, None)

def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("localhost", 6379))
    srv.listen(5)
    threading.Thread(target=unblock_thread, daemon=True).start()
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()