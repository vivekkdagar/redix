import socket


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept()  # Wait for a client to connect
    connection.sendall(b"+PONG\r\n")        # Send Redis PONG response


if __name__ == "__main__":
    main()