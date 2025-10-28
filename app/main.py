import socket
import threading
import time

# In-memory store: key -> {"value": str, "expiry": float | None}
store = {}


def parse_resp(data: bytes):
    parts = data.split(b"\r\n")
    items = []
    i = 0
    while i < len(parts):
        if parts[i].startswith(b"$"):
            i += 1
            if i < len(parts) and parts[i] != b"":
                items.append(parts[i].decode())
        i += 1
    return items


def is_expired(key):
    if key in store:
        exp = store[key].get("expiry")
        if exp is not None and time.time() > exp:
            del store[key]
            return True
    return False


def handle_client(conn):
    while True:
        data = conn.recv(1024)
        if not data:
            break

        parts = parse_resp(data)
        if not parts:
            continue

        cmd = parts[0].upper()

        # --- PING ---
        if cmd == "PING":
            conn.sendall(b"+PONG\r\n")

        # --- ECHO ---
        elif cmd == "ECHO" and len(parts) > 1:
            msg = parts[1].encode()
            resp = b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
            conn.sendall(resp)

        # --- SET [key] [value] [EX seconds]? ---
        elif cmd == "SET" and len(parts) >= 3:
            key, value = parts[1], parts[2]
            expiry = None

            # Handle optional EX argument
            if len(parts) >= 5 and parts[3].upper() == "EX":
                try:
                    expiry = time.time() + int(parts[4])
                except ValueError:
                    expiry = None

            store[key] = {"value": value, "expiry": expiry}
            conn.sendall(b"+OK\r\n")

        # --- GET [key] ---
        elif cmd == "GET" and len(parts) >= 2:
            key = parts[1]
            if key not in store or is_expired(key):
                conn.sendall(b"$-1\r\n")
            else:
                val = store[key]["value"].encode()
                resp = b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
                conn.sendall(resp)

        # --- RPUSH (Lists) ---
        elif cmd == "RPUSH" and len(parts) == 3:
            key, value = parts[1], parts[2]
            if key not in store:
                store[key] = {"value": [value], "expiry": None}
            else:
                current = store[key]["value"]
                if isinstance(current, list):
                    current.append(value)
                else:
                    store[key]["value"] = [value]
            length = len(store[key]["value"])
            conn.sendall(f":{length}\r\n".encode())

        else:
            conn.sendall(b"-ERR unknown command\r\n")

    conn.close()


def main():
    server = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()


if __name__ == "__main__":
    main()