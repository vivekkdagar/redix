import socket
import threading
import time

store = {}

def handle_client(conn):
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break

            parts = data.split(b"\r\n")
            if len(parts) < 3:
                continue

            try:
                command = parts[2].decode().upper()
            except:
                continue

            # --- RESP Command Handling ---
            if command == "PING":
                conn.sendall(b"+PONG\r\n")

            elif command == "ECHO":
                message = parts[4]
                conn.sendall(b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n")

            elif command == "SET":
                key = parts[4].decode()
                value = parts[6].decode()
                if len(parts) > 8 and parts[8].decode().upper() == "PX":
                    expiry = int(parts[10].decode()) / 1000
                    store[key] = (value, time.time() + expiry)
                else:
                    store[key] = (value, None)
                conn.sendall(b"+OK\r\n")

            elif command == "GET":
                key = parts[4].decode()
                if key not in store:
                    conn.sendall(b"$-1\r\n")
                else:
                    value, expiry = store[key]
                    if expiry and expiry < time.time():
                        del store[key]
                        conn.sendall(b"$-1\r\n")
                    else:
                        conn.sendall(b"$" + str(len(value)).encode() + b"\r\n" + value.encode() + b"\r\n")

            # --- LIST COMMANDS ---

            elif command == "RPUSH":
                key = parts[4].decode()
                values = [parts[i].decode() for i in range(6, len(parts) - 1, 2)]

                if key not in store:
                    store[key] = ([], None)

                lst, expiry = store[key]
                lst.extend(values)
                store[key] = (lst, expiry)

                response = b":" + str(len(lst)).encode() + b"\r\n"
                conn.sendall(response)

            elif command == "LPUSH":
                key = parts[4].decode()
                values = [parts[i].decode() for i in range(6, len(parts) - 1, 2)]

                if key not in store:
                    store[key] = ([], None)

                lst, expiry = store[key]
                for v in values:
                    lst.insert(0, v)
                store[key] = (lst, expiry)

                response = b":" + str(len(lst)).encode() + b"\r\n"
                conn.sendall(response)

            elif command == "LLEN":
                key = parts[4].decode()
                if key not in store:
                    conn.sendall(b":0\r\n")
                else:
                    lst, expiry = store[key]
                    conn.sendall(b":" + str(len(lst)).encode() + b"\r\n")

            elif command == "LRANGE":
                key = parts[4].decode()
                start = int(parts[6].decode())
                end = int(parts[8].decode())

                if key not in store:
                    conn.sendall(b"*0\r\n")
                    continue

                lst, expiry = store[key]
                if end == -1:
                    sublist = lst[start:]
                else:
                    sublist = lst[start:end + 1]

                conn.sendall(b"*" + str(len(sublist)).encode() + b"\r\n")
                for item in sublist:
                    conn.sendall(b"$" + str(len(item)).encode() + b"\r\n" + item.encode() + b"\r\n")

            else:
                conn.sendall(b"-ERR unknown command\r\n")


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("localhost", 6379))
    server.listen()
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()


if __name__ == "__main__":
    main()