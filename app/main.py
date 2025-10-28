import socket
import threading
import time

data_store = {}          # key → value (string or list)
expiry_store = {}        # key → expiry_timestamp


def remove_expired_keys():
    """Remove expired keys from data_store."""
    now = time.time()
    expired = [key for key, exp in expiry_store.items() if exp <= now]
    for key in expired:
        expiry_store.pop(key, None)
        data_store.pop(key, None)


def handle_client(client_socket):
    buffer = b""

    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                break
            buffer += data

            while b"\r\n" in buffer:
                line, buffer = buffer.split(b"\r\n", 1)
                if not line:
                    continue

                parts = line.decode().strip().split()
                if not parts:
                    continue

                command = parts[0].upper()

                # ---------------- PING ----------------
                if command == "PING":
                    client_socket.sendall(b"+PONG\r\n")

                # ---------------- ECHO ----------------
                elif command == "ECHO":
                    if len(parts) < 2:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")
                    else:
                        msg = " ".join(parts[1:])
                        client_socket.sendall(f"${len(msg)}\r\n{msg}\r\n".encode())

                # ---------------- SET ----------------
                elif command == "SET":
                    if len(parts) < 3:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
                        continue

                    key, value = parts[1], parts[2]
                    data_store[key] = value
                    expiry_store.pop(key, None)

                    if len(parts) == 5 and parts[3].upper() == "PX":
                        try:
                            expiry_ms = int(parts[4])
                            expiry_store[key] = time.time() + expiry_ms / 1000
                        except:
                            client_socket.sendall(b"-ERR invalid PX value\r\n")
                            continue

                    client_socket.sendall(b"+OK\r\n")

                # ---------------- GET ----------------
                elif command == "GET":
                    if len(parts) != 2:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
                        continue
                    key = parts[1]
                    remove_expired_keys()
                    if key not in data_store:
                        client_socket.sendall(b"$-1\r\n")
                    else:
                        val = data_store[key]
                        if isinstance(val, list):
                            client_socket.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                        else:
                            client_socket.sendall(f"${len(val)}\r\n{val}\r\n".encode())

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

                # ---------------- LPUSH ----------------
                elif command == "LPUSH":
                    if len(parts) < 3:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'lpush' command\r\n")
                        continue
                    key = parts[1]
                    values = parts[2:]
                    if key not in data_store:
                        data_store[key] = []
                    if not isinstance(data_store[key], list):
                        client_socket.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                        continue
                    for v in values:
                        data_store[key].insert(0, v)
                    client_socket.sendall(f":{len(data_store[key])}\r\n".encode())

                # ---------------- LRANGE ----------------
                elif command == "LRANGE":
                    if len(parts) != 4:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'lrange' command\r\n")
                        continue
                    key = parts[1]
                    start = int(parts[2])
                    end = int(parts[3])
                    if key not in data_store or not isinstance(data_store[key], list):
                        client_socket.sendall(b"*0\r\n")
                        continue
                    lst = data_store[key]
                    if end == -1:
                        end = len(lst) - 1
                    result = lst[start:end+1]
                    resp = f"*{len(result)}\r\n"
                    for item in result:
                        resp += f"${len(item)}\r\n{item}\r\n"
                    client_socket.sendall(resp.encode())

                # ---------------- LLEN ----------------
                elif command == "LLEN":
                    if len(parts) != 2:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'llen' command\r\n")
                        continue
                    key = parts[1]
                    if key not in data_store:
                        client_socket.sendall(b":0\r\n")
                    elif not isinstance(data_store[key], list):
                        client_socket.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                    else:
                        length = len(data_store[key])
                        client_socket.sendall(f":{length}\r\n".encode())

                # ---------------- LPOP ----------------
                elif command == "LPOP":
                    if len(parts) < 2:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'lpop' command\r\n")
                        continue
                    key = parts[1]
                    count = 1
                    if len(parts) == 3:
                        try:
                            count = int(parts[2])
                        except:
                            client_socket.sendall(b"-ERR value is not an integer\r\n")
                            continue
                    if key not in data_store or not isinstance(data_store[key], list) or len(data_store[key]) == 0:
                        client_socket.sendall(b"$-1\r\n")
                        continue

                    lst = data_store[key]
                    removed = []
                    for _ in range(min(count, len(lst))):
                        removed.append(lst.pop(0))

                    if count == 1:
                        val = removed[0]
                        client_socket.sendall(f"${len(val)}\r\n{val}\r\n".encode())
                    else:
                        resp = f"*{len(removed)}\r\n"
                        for val in removed:
                            resp += f"${len(val)}\r\n{val}\r\n"
                        client_socket.sendall(resp.encode())

                # ---------------- UNKNOWN COMMAND ----------------
                else:
                    client_socket.sendall(b"-ERR unknown command\r\n")

        except ConnectionResetError:
            break

    client_socket.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 6379))
    server.listen(5)
    print("Redis clone running on port 6379")

    while True:
        client_socket, _ = server.accept()
        thread = threading.Thread(target=handle_client, args=(client_socket,))
        thread.daemon = True
        thread.start()


if __name__ == "__main__":
    main()