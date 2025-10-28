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
    """
    Encode Python value into RESP bytes.
    - str -> bulk string
    - list -> array (elements encoded recursively)
    - int -> integer
    - None -> null bulk string
    - other -> simple string OK
    """
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
# Expiry cleanup thread (passive + periodic cleanup)
# ---------------------------
def cleanup_expired():
    while True:
        now = time.time()
        expired_keys = [
            k for k, v in db.items() if isinstance(v, dict) and v.get("expiry") and v["expiry"] < now
        ]
        for k in expired_keys:
            del db[k]
        time.sleep(1)


threading.Thread(target=cleanup_expired, daemon=True).start()


# ---------------------------
# Command handler
# ---------------------------
def handle_client(conn: socket.socket):
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break

            # Very small RESP parser tailored to this challenge.
            # Split by CRLF and remove empty markers and RESP markers we don't need.
            parts = data.split(b"\r\n")
            # Remove empty segments and the literal '*' or '$' markers we don't need here
            parts = [p for p in parts if p not in (b"", b"*", b"$")]

            if len(parts) < 3:
                # Not enough data to form a command
                continue

            # Command token is usually at index 2 after parsing pattern used above
            command = parts[2].upper()

            # ---------------------------
            # PING
            # ---------------------------
            if command == b"PING":
                conn.sendall(b"+PONG\r\n")

            # ---------------------------
            # ECHO
            # Example RESP: *2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n
            # parts -> [b'*2', b'$4', b'ECHO', b'$6', b'banana', ...] -> we cleaned markers
            # so parts[4] is the message
            # ---------------------------
            elif command == b"ECHO":
                msg = parts[4].decode()
                conn.sendall(encode_resp(msg))

            # ---------------------------
            # SET (with optional PX)
            # Example: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
            # Or: *5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n
            # parts[4] -> key, parts[6] -> value, optional parts[8]==b'PX' and parts[10]==expiry_ms
            # ---------------------------
            elif command == b"SET":
                key = parts[4].decode()
                value = parts[6].decode()
                expiry = None
                if len(parts) > 8 and parts[8].upper() == b"PX":
                    try:
                        expiry = time.time() + int(parts[10].decode()) / 1000.0
                    except Exception:
                        expiry = None
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
                    # expired -> delete and return null
                    del db[key]
                    conn.sendall(b"$-1\r\n")
                else:
                    conn.sendall(encode_resp(entry["value"]))

            # ---------------------------
            # LPUSH (prepend multiple elements)
            # Behavior: LPUSH key a b c  => elements are pushed left-to-right inserting at head:
            # insert 'a' => [a]
            # insert 'b' => [b,a]
            # insert 'c' => [c,b,a]
            # which matches Redis semantics that the last argument becomes the head if you think order,
            # but this simple left-to-right insert(0) yields the expected behavior for LPUSH.
            # ---------------------------
            elif command == b"LPUSH":
                key = parts[4].decode()
                values = []
                i = 6
                # collect all provided element values (every other entry in RESP)
                while i < len(parts) and parts[i]:
                    values.append(parts[i].decode())
                    i += 2

                # create list if missing
                if key not in db:
                    db[key] = {"value": [], "expiry": None}

                if not isinstance(db[key]["value"], list):
                    # If key exists and is not a list, Redis would error; for challenge we'll replace
                    # with a list (or you could choose to return an error). We'll replace to keep tests simple.
                    db[key]["value"] = []

                lst = db[key]["value"]
                # Prepend each element in left->right order by inserting at 0
                for v in values:
                    lst.insert(0, v)
                db[key]["value"] = lst

                # RESP integer with new length
                conn.sendall(encode_resp(len(lst)))

            # ---------------------------
            # RPUSH (append multiple elements)
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

                if not isinstance(db[key]["value"], list):
                    db[key]["value"] = []

                lst = db[key]["value"]
                lst.extend(values)
                db[key]["value"] = lst

                conn.sendall(encode_resp(len(lst)))

            # ---------------------------
            # LRANGE (with negative indexes supported)
            # Example: LRANGE key start stop
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

                n = len(lst)
                # convert negative indices to positive
                if start < 0:
                    start = n + start
                if stop < 0:
                    stop = n + stop

                # clamp bounds
                start = max(start, 0)
                stop = min(stop, n - 1)

                if start > stop or start >= n:
                    result = []
                else:
                    result = lst[start : stop + 1]

                conn.sendall(encode_resp(result))

            else:
                conn.sendall(b"-ERR unknown command\r\n")
    except Exception as e:
        # Keep server alive, print error for debugging
        print("Error handling client:", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------
# Server setup
# ---------------------------
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(64)

    print("Redis clone running on port 6379...")

    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()