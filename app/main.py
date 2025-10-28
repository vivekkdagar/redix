import socket
import threading
import time

# In-memory database
data_store = {}
expiry = {}

def set_key(key, value, px=None):
    data_store[key] = value
    if px:
        expiry[key] = time.time() + (px / 1000)
    elif key in expiry:
        del expiry[key]

def get_key(key):
    if key in expiry and time.time() > expiry[key]:
        del data_store[key]
        del expiry[key]
        return None
    return data_store.get(key)

def encode_bulk_string(value):
    if value is None:
        return b"$-1\r\n"
    return f"${len(value)}\r\n{value}\r\n".encode()

def encode_simple_string(value):
    return f"+{value}\r\n".encode()

def encode_integer(value):
    return f":{value}\r\n".encode()

def encode_array(values):
    res = f"*{len(values)}\r\n"
    for v in values:
        if v is None:
            res += "$-1\r\n"
        else:
            res += f"${len(v)}\r\n{v}\r\n"
    return res.encode()

def parse_request(data):
    lines = data.split(b"\r\n")
    parts = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith(b"*") or line == b"":
            i += 1
            continue
        if line.startswith(b"$"):
            length = int(line[1:])
            i += 1
            parts.append(lines[i].decode())
        i += 1
    return parts

def handle_client(conn):
    while True:
        data = b""
        try:
            chunk = conn.recv(1024)
            if not chunk:
                break
            data += chunk
        except:
            break

        if not data:
            break

        parts = parse_request(data)
        if not parts:
            continue

        cmd = parts[0].upper()

        # ----------------------------
        # PING
        # ----------------------------
        if cmd == "PING":
            response = encode_simple_string("PONG")

        # ----------------------------
        # ECHO
        # ----------------------------
        elif cmd == "ECHO":
            response = encode_bulk_string(parts[1])

        # ----------------------------
        # SET
        # ----------------------------
        elif cmd == "SET":
            key, value = parts[1], parts[2]
            px = None
            if len(parts) > 4 and parts[3].upper() == "PX":
                px = int(parts[4])
            set_key(key, value, px)
            response = encode_simple_string("OK")

        # ----------------------------
        # GET
        # ----------------------------
        elif cmd == "GET":
            value = get_key(parts[1])
            response = encode_bulk_string(value)

        # ----------------------------
        # LPUSH / RPUSH
        # ----------------------------
        elif cmd in ["LPUSH", "RPUSH"]:
            key = parts[1]
            values = parts[2:]

            if key not in data_store or not isinstance(data_store[key], list):
                data_store[key] = []

            if cmd == "LPUSH":
                data_store[key] = list(reversed(values)) + data_store[key]
            else:
                data_store[key].extend(values)

            response = encode_integer(len(data_store[key]))

        # ----------------------------
        # LLEN
        # ----------------------------
        elif cmd == "LLEN":
            key = parts[1]
            length = len(data_store.get(key, [])) if isinstance(data_store.get(key, []), list) else 0
            response = encode_integer(length)

        # ----------------------------
        # LRANGE
        # ----------------------------
        elif cmd == "LRANGE":
            key, start, end = parts[1], int(parts[2]), int(parts[3])
            arr = data_store.get(key, [])
            if not isinstance(arr, list):
                arr = []

            # Handle negative indices
            n = len(arr)
            if start < 0:
                start = n + start
            if end < 0:
                end = n + end
            start = max(start, 0)
            end = min(end, n - 1)

            if start > end or n == 0:
                result = []
            else:
                result = arr[start:end + 1]

            response = encode_array(result)

        # ----------------------------
        # LPOP / RPOP
        # ----------------------------
        elif cmd in ["LPOP", "RPOP"]:
            key = parts[1]
            count = 1
            if len(parts) > 2:
                try:
                    count = int(parts[2])
                except:
                    count = 1

            arr = data_store.get(key, [])
            if not isinstance(arr, list) or not arr:
                response = encode_bulk_string(None)
            else:
                if cmd == "LPOP":
                    popped = arr[:count]
                    data_store[key] = arr[count:]
                else:
                    popped = arr[-count:]
                    data_store[key] = arr[:-count]

                if count == 1:
                    response = encode_bulk_string(popped[0])
                else:
                    response = encode_array(popped)

        # ----------------------------
        # Default
        # ----------------------------
        else:
            response = encode_simple_string("UNKNOWN")

        conn.sendall(response)
    conn.close()


def main():
    HOST = "localhost"
    PORT = 6379
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()

    print("Redis clone running on port 6379")

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()