import socket
import threading

# Global in-memory key-value store
store = {}


def parse_resp(data: bytes):
    """
    Minimal RESP parser that handles arrays of bulk strings.
    Example:
        *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        -> ["SET", "foo", "bar"]
    """
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


def handle_client(connection):
    while True:
        data = connection.recv(1024)
        if not data:
            break

        command_parts = parse_resp(data)
        if not command_parts:
            continue

        command = command_parts[0].upper()

        if command == "PING":
            connection.sendall(b"+PONG\r\n")

        elif command == "ECHO" and len(command_parts) > 1:
            msg = command_parts[1].encode()
            response = b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
            connection.sendall(response)

        elif command == "SET" and len(command_parts) >= 3:
            key = command_parts[1]
            value = command_parts[2]
            store[key] = value
            connection.sendall(b"+OK\r\n")

        elif command == "GET" and len(command_parts) >= 2:
            key = command_parts[1]
            if key in store:
                value = store[key].encode()
                response = (
                    b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                )
                connection.sendall(response)
            else:
                # Null bulk string for missing key
                connection.sendall(b"$-1\r\n")

        else:
            connection.sendall(b"-ERR unknown command\r\n")

    connection.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(connection,))
        thread.start()


if __name__ == "__main__":
    main()