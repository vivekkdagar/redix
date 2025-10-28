import socket
import threading


def parse_resp(data: bytes):
    """
    Minimal RESP parser that handles arrays of bulk strings.
    Example input:
        *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    Returns:
        ["ECHO", "hey"]
    """
    parts = data.split(b"\r\n")
    items = []
    i = 0
    while i < len(parts):
        if parts[i].startswith(b"$"):
            # Next line after $length is the actual string
            i += 1
            if i < len(parts):
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
            # RESP bulk string format: $<len>\r\n<data>\r\n
            response = b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
            connection.sendall(response)

        else:
            # Ignore unsupported commands for now
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