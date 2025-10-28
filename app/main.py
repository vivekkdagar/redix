import socket
import threading

store = {}  # key -> list


def handle_client(connection):
    while True:
        data = connection.recv(1024)
        if not data:
            break

        parts = data.split(b"\r\n")

        # RESP parsing
        if len(parts) < 3:
            continue

        if parts[2].upper() == b"PING":
            connection.sendall(b"+PONG\r\n")
            continue

        if parts[2].upper() == b"ECHO":
            msg = parts[4]
            connection.sendall(b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n")
            continue

        if parts[2].upper() == b"SET":
            key = parts[4].decode()
            value = parts[6].decode()
            store[key] = value
            connection.sendall(b"+OK\r\n")
            continue

        if parts[2].upper() == b"GET":
            key = parts[4].decode()
            value = store.get(key)
            if value is None:
                connection.sendall(b"$-1\r\n")
            else:
                connection.sendall(
                    b"$" + str(len(value)).encode() + b"\r\n" + value.encode() + b"\r\n"
                )
            continue

        # ------------------------
        # RPUSH implementation
        # ------------------------
        if parts[2].upper() == b"RPUSH":
            key = parts[4].decode()
            values = []

            # Elements start at parts[6], step by 2 ($len, value)
            i = 6
            while i < len(parts) and parts[i]:
                values.append(parts[i].decode())
                i += 2

            if key not in store:
                store[key] = []

            store[key].extend(values)

            connection.sendall(
                b":" + str(len(store[key])).encode() + b"\r\n"
            )
            continue

    connection.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()


if __name__ == "__main__":
    main()