import socket
import threading

# In-memory data store
data_store = {}

# --- RESP ENCODING HELPERS ---

def encode_bulk_string(s):
    return f"${len(s)}\r\n{s}\r\n"

def encode_simple_string(s):
    return f"+{s}\r\n"

def encode_integer(i):
    return f":{i}\r\n"

def encode_null_bulk_string():
    return "$-1\r\n"

def encode_array(arr):
    resp = f"*{len(arr)}\r\n"
    for el in arr:
        resp += encode_bulk_string(el)
    return resp


# --- RESP PARSER ---
def parse_request(data):
    lines = data.split("\r\n")
    if not lines or not lines[0].startswith("*"):
        return None
    try:
        count = int(lines[0][1:])
        args = []
        i = 1
        while i < len(lines) and len(args) < count:
            if lines[i].startswith("$"):
                args.append(lines[i + 1])
                i += 2
            else:
                i += 1
        return args
    except Exception:
        return None


# --- CLIENT HANDLER ---
def handle_client(conn):
    buffer = ""
    while True:
        try:
            data = conn.recv(1024)
            if not data:
                break
            buffer += data.decode()
            if "\r\n" not in buffer:
                continue

            request = parse_request(buffer)
            buffer = ""

            if not request:
                continue

            command = request[0].upper()

            # --- CORE COMMANDS ---

            if command == "PING":
                conn.sendall(encode_simple_string("PONG").encode())

            elif command == "ECHO":
                if len(request) > 1:
                    conn.sendall(encode_bulk_string(request[1]).encode())
                else:
                    conn.sendall(encode_null_bulk_string().encode())

            elif command == "SET":
                key, value = request[1], request[2]
                data_store[key] = value
                conn.sendall(encode_simple_string("OK").encode())

            elif command == "GET":
                key = request[1]
                val = data_store.get(key)
                if val is None:
                    conn.sendall(encode_null_bulk_string().encode())
                else:
                    conn.sendall(encode_bulk_string(val).encode())

            # --- LIST COMMANDS ---

            elif command == "RPUSH":
                key = request[1]
                values = request[2:]
                if key not in data_store:
                    data_store[key] = []
                data_store[key].extend(values)
                conn.sendall(encode_integer(len(data_store[key])).encode())

            elif command == "LPUSH":
                key = request[1]
                values = request[2:]
                if key not in data_store:
                    data_store[key] = []
                # prepend (reverse order like Redis)
                for v in values:
                    data_store[key].insert(0, v)
                conn.sendall(encode_integer(len(data_store[key])).encode())

            elif command == "LLEN":
                key = request[1]
                length = len(data_store.get(key, []))
                conn.sendall(encode_integer(length).encode())

            elif command == "LRANGE":
                key = request[1]
                start, end = int(request[2]), int(request[3])
                lst = data_store.get(key, [])
                if end < 0:
                    end = len(lst) + end
                result = lst[start:end + 1]
                conn.sendall(encode_array(result).encode())

            elif command == "LPOP":
                key = request[1]
                lst = data_store.get(key, [])

                if len(request) == 2:  # single pop
                    if not lst:
                        conn.sendall(encode_null_bulk_string().encode())
                    else:
                        val = lst.pop(0)
                        conn.sendall(encode_bulk_string(val).encode())

                elif len(request) == 3:  # multiple pop
                    count = int(request[2])
                    if not lst:
                        conn.sendall(encode_array([]).encode())
                    else:
                        popped = lst[:count]
                        data_store[key] = lst[count:]
                        conn.sendall(encode_array(popped).encode())

            else:
                conn.sendall(encode_simple_string("ERR unknown command").encode())

        except Exception:
            break

    conn.close()


# --- MAIN SERVER ---
def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 6379))
    server.listen()

    print("Redis clone running on port 6379")

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()


if __name__ == "__main__":
    main()