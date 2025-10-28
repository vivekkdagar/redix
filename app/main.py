import socket


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept()

    # Continuously read and respond to PING commands
    while True:
        data = connection.recv(1024)
        if not data:
            break  # client closed connection
        connection.sendall(b"+PONG\r\n")  # hardcoded RESP response


if __name__ == "__main__":
    main()