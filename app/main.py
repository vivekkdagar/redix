import socket
import threading
import time

store = {}
expiry = {}

def cleanup_expired_keys():
    current_time = time.time()
    expired = [key for key, exp in expiry.items() if exp <= current_time]
    for key in expired:
        store.pop(key, None)
        expiry.pop(key, None)

def encode_bulk_string(value):
    if value is None:
        return b"$-1\r\n"
    return f"${len(value)}\r\n{value}\r\n".encode()

def encode_integer(num):
    return f":{num}\r\n".encode()

def encode_array(items):
    resp = f"*{len(items)}\r\n"
    for item in items:
        resp += f"${len(item)}\r\n{item}\r\n"
    return resp.encode()

def handle_client(conn):
    while True:
        try:
            data = conn.recv(1024)
            if not data:
                break

            parts = data.decode().strip().split("\r\n")
            if len(parts) < 2:
                continue

            command = parts[2].upper()
            args = [p for i, p in enumerate(parts) if i >= 4 and i % 2 == 0]

            cleanup_expired_keys()

            # --- PING ---
            if command == "PING":
                conn.send(b"+PONG\r\n")

            # --- ECHO ---
            elif command == "ECHO" and args:
                conn.send(encode_bulk_string(args[0]))

            # --- SET ---
            elif command == "SET" and len(args) >= 2:
                key, value = args[0], args[1]
                store[key] = value
                if len(args) == 4 and args[2].upper() == "PX":
                    expiry[key] = time.time() + int(args[3]) / 1000
                conn.send(b"+OK\r\n")

            # --- GET ---
            elif command == "GET" and len(args) == 1:
                key = args[0]
                val = store.get(key)
                conn.send(encode_bulk_string(val))

            # --- RPUSH ---
            elif command == "RPUSH" and len(args) >= 2:
                key = args[0]
                values = args[1:]
                if key not in store:
                    store[key] = []
                store[key].extend(values)
                conn.send(encode_integer(len(store[key])))

            # --- LPUSH ---
            elif command == "LPUSH" and len(args) >= 2:
                key = args[0]
                values = args[1:]
                if key not in store:
                    store[key] = []
                for v in values:
                    store[key].insert(0, v)
                conn.send(encode_integer(len(store[key])))

            # --- LLEN ---
            elif command == "LLEN" and len(args) == 1:
                key = args[0]
                length = len(store.get(key, []))
                conn.send(encode_integer(length))

            # --- LRANGE ---
            elif command == "LRANGE" and len(args) == 3:
                key = args[0]
                start = int(args[1])
                end = int(args[2])
                lst = store.get(key, [])
                if end == -1:
                    end = len(lst) - 1
                conn.send(encode_array(lst[start:end + 1]))

            # --- LPOP (new) ---
            elif command == "LPOP" and len(args) == 1:
                key = args[0]
                lst = store.get(key)
                if not lst:
                    conn.send(b"$-1\r\n")
                else:
                    value = lst.pop(0)
                    conn.send(encode_bulk_string(value))

            else:
                conn.send(b"-ERR unknown command\r\n")

        except Exception as e:
            conn.send(b"-ERR internal error\r\n")
            break

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 6379))
    server.listen()
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

if __name__ == "__main__":
    main()