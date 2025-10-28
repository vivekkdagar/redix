import socket
import threading

# In-memory datastore (dict: key -> value)
data_store = {}

def encode_bulk_string(s: str) -> bytes:
    return f"${len(s)}\r\n{s}\r\n".encode()

def encode_array(arr) -> bytes:
    resp = f"*{len(arr)}\r\n"
    for item in arr:
        resp += f"${len(item)}\r\n{item}\r\n"
    return resp.encode()

def handle_client(client_socket):
    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                break

            parts = parse_resp(data)
            if not parts:
                continue

            command = parts[0].upper()

            # ---------------- PING ----------------
            if command == "PING":
                if len(parts) == 1:
                    client_socket.sendall(b"+PONG\r\n")
                else:
                    msg = parts[1]
                    client_socket.sendall(encode_bulk_string(msg))

            # ---------------- ECHO ----------------
            elif command == "ECHO":
                if len(parts) != 2:
                    client_socket.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")
                    continue
                client_socket.sendall(encode_bulk_string(parts[1]))

            # ---------------- SET ----------------
            elif command == "SET":
                if len(parts) < 3:
                    client_socket.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
                    continue
                key, value = parts[1], parts[2]
                data_store[key] = value
                client_socket.sendall(b"+OK\r\n")

            # ---------------- GET ----------------
            elif command == "GET":
                if len(parts) != 2:
                    client_socket.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
                    continue
                key = parts[1]
                if key not in data_store:
                    client_socket.sendall(b"$-1\r\n")
                else:
                    val = data_store[key]
                    if isinstance(val, list):
                        client_socket.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                    else:
                        client_socket.sendall(encode_bulk_string(val))

            # ---------------- RPUSH ----------------
            elif command == "RPUSH":
                if len(parts) < 3:
                    client_socket.sendall(b"-ERR wrong number of arguments for 'rpush' command\r\n")
                    continue
                key = parts[1]
                values = parts[2:]
                if key not in data_store:
                    data_store[key] = []
                if not isinstance(data_store[key], list):
                    client_socket.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                    continue
                data_store[key].extend(values)
                client_socket.sendall(f":{len(data_store[key])}\r\n".encode())

            # ---------------- LRANGE ----------------
            elif command == "LRANGE":
                if len(parts) != 4:
                    client_socket.sendall(b"-ERR wrong number of arguments for 'lrange' command\r\n")
                    continue
                key, start, end = parts[1], int(parts[2]), int(parts[3])
                if key not in data_store or not isinstance(data_store[key], list):
                    client_socket.sendall(b"*0\r\n")
                    continue
                lst = data_store[key]
                if end == -1:
                    end = len(lst) - 1
                sublist = lst[start:end+1]
                client_socket.sendall(encode_array(sublist))

            # ---------------- LPOP ----------------
            elif command == "LPOP":
                if len(parts) < 2:
                    client_socket.sendall(b"-ERR wrong number of arguments for 'lpop' command\r\n")
                    continue

                key = parts[1]
                count = 1  # default single pop

                if len(parts) == 3:
                    try:
                        count = int(parts[2])
                    except ValueError:
                        client_socket.sendall(b"-ERR value is not an integer or out of range\r\n")
                        continue

                if key not in data_store or not isinstance(data_store[key], list) or len(data_store[key]) == 0:
                    client_socket.sendall(b"$-1\r\n")
                    continue

                popped = []
                for _ in range(min(count, len(data_store[key]))):
                    popped.append(data_store[key].pop(0))

                if count == 1:
                    client_socket.sendall(encode_bulk_string(popped[0]))
                else:
                    client_socket.sendall(encode_array(popped))

            # ---------------- UNKNOWN COMMAND ----------------
            else:
                client_socket.sendall(b"-ERR unknown command\r\n")

        except ConnectionResetError:
            break
        except Exception as e:
            client_socket.sendall(f"-ERR {str(e)}\r\n".encode())
            break

    client_socket.close()


def parse_resp(data: bytes):
    try:
        text = data.decode().strip()
        if text.startswith("*"):
            lines = text.split("\r\n")
            arr = []
            i = 1
            while i < len(lines):
                if lines[i].startswith("$"):
                    arr.append(lines[i+1])
                    i += 2
                else:
                    i += 1
            return arr
        else:
            return text.split()
    except:
        return []


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 6379))
    server.listen(5)
    print("Redis clone running on port 6379")

    while True:
        client_socket, _ = server.accept()
        threading.Thread(target=handle_client, args=(client_socket,), daemon=True).start()


if __name__ == "__main__":
    main()