import socket
import threading
import time

# ---------------------------
# In-memory data store
# ---------------------------
db = {}

# ---------------------------
# Helper: Encode RESP responses
# ---------------------------
def encode_resp(value):
    if isinstance(value, str):
        return b"$" + str(len(value)).encode() + b"\r\n" + value.encode() + b"\r\n"
    elif isinstance(value, list):
        result = b"*" + str(len(value)).encode() + b"\r\n"
        for v in value:
            result += encode_resp(v)
        return result
    elif isinstance(value, int):
        return b":" + str(value).encode() + b"\r\n"
    elif value is None:
        return b"$-1\r\n"
    else:
        return b"+OK\r\n"


# ---------------------------
# Expiry cleanup thread
# ---------------------------
def cleanup_expired():
    while True:
        now = time.time()
        expired_keys = [k for k, v in db.items() if isinstance(v, dict) and v.get("expiry") and v["expiry"] < now]
        for k in expired_keys:
            del db[k]
        time.sleep(1)


threading.Thread(target=cleanup_expired, daemon=True).start()


# ---------------------------
# Command handler
# ---------------------------
def handle_client(conn):
    while True:
        data = conn.recv(1024)
        if not data:
            break

        parts = data.split(b"\r\n")
        parts = [p for p in parts if p not in (b"", b"*", b"$")]

        if len(parts) < 2:
            continue

        command = parts[2].upper()

        # ---------------------------
        # PING
        # ---------------------------
        if command == b"PING":
            conn.sendall(b"+PONG\r\n")

        # ---------------------------
        # ECHO
        # ---------------------------
        elif command == b"ECHO":
            msg = parts[4].decode()
            conn.sendall(encode_resp(msg))

        # ---------------------------
        # SET (with optional expiry)
        # ---------------------------
        elif command == b"SET":
            key = parts[4].decode()
            value = parts[6].decode()
            expiry = None
            if len(parts) > 8 and parts[8].upper() == b"PX":
                expiry = time.time() + int(parts[10].decode()) / 1000
            db[key] = {"value": value, "expiry": expiry}
            conn.sendall(b"+OK\r\n")

        # ---------------------------
        # GET
        # ---------------------------
        elif command == b"GET":
            key = parts[4].decode()
            entry = db.get(key)
            if not entry:
                conn.sendall(b"$-1\r\n")
                continue
            if entry.get("expiry") and entry["expiry"] < time.time():
                del db[key]
                conn.sendall(b"$-1\r\n")
            else:
                conn.sendall(encode_resp(entry["value"]))

        # ---------------------------
        # RPUSH (multiple elements supported)
        # ---------------------------
        elif command == b"RPUSH":
            key = parts[4].decode()
            values = []
            i = 6
            while i < len(parts) and parts[i]:
                values.append(parts[i].decode())
                i += 2

            if key not in db:
                db[key] = {"value": [], "expiry": None}

            lst = db[key]["value"]
            lst.extend(values)
            db[key]["value"] = lst

            conn.sendall(encode_resp(len(lst)))

        # ---------------------------
        # LRANGE (with negative indexes)
        # ---------------------------
        elif command == b"LRANGE":
            key = parts[4].decode()
            start = int(parts[6].decode())
            stop = int(parts[8].decode())

            if key not in db:
                conn.sendall(encode_resp([]))
                continue

            lst = db[key]["value"]
            if not isinstance(lst, list):
                conn.sendall(encode_resp([]))
                continue

            # Convert negative indexes
            n = len(lst)
            if start < 0:
                start = n + start
            if stop < 0:
                stop = n + stop

            # Clamp bounds
            start = max(start, 0)
            stop = min(stop, n - 1)

            if start > stop or start >= n:
                result = []
            else:
                result = lst[start : stop + 1]

            conn.sendall(encode_resp(result))

        else:
            conn.sendall(b"-ERR unknown command\r\n")


# ---------------------------
# Server setup
# ---------------------------
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(5)

    print("Redis clone running on port 6379...")

    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()