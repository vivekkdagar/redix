import time
import socket  # noqa: F401
import threading


class Handler:
    def __init__(self):
        # Store value and expiry (None or timestamp in seconds)
        self.dictionary = dict()

    def handle(self, connection):
        while True:
            data = connection.recv(1024)
            if data is None:
                break
            if data.startswith(b"*1\r\n$4\r\nPING"):
                self.handle_ping(connection)
            elif data.startswith(b"*2\r\n$4\r\nECHO"):
                self.handle_echo(connection, data)
            elif data.startswith(b"*3\r\n$3\r\nSET") or data.startswith(
                b"*5\r\n$3\r\nSET"
            ):
                self.handle_set(connection, data)
            elif data.startswith(b"*2\r\n$3\r\nGET"):
                self.handle_get(connection, data)
            elif b"\r\n$5\r\nRPUSH\r\n" in data:
                self.handle_rpush(connection, data)
            else:
                connection.sendall(b"-ERR unknown command\r\n")
        connection.close()

    def handle_ping(self, connection):
        connection.sendall(b"+PONG\r\n")

    def handle_echo(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [
                i for i, part in enumerate(parts) if part.startswith(b"$")
            ]
            if len(dollar_indices) >= 2:
                arg_index = dollar_indices[1] + 1
                message = parts[arg_index]
                response = (
                    b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
                )
                connection.sendall(response)
            else:
                connection.sendall(
                    b"-ERR wrong number of arguments for 'echo' command\r\n"
                )
        except Exception:
            connection.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")

    def handle_set(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [
                i for i, part in enumerate(parts) if part.startswith(b"$")
            ]
            if len(dollar_indices) >= 3:
                key_index = dollar_indices[1] + 1
                value_index = dollar_indices[2] + 1
                key = parts[key_index]
                value = parts[value_index]
                expiry = None
                # Check for PX argument
                if len(dollar_indices) >= 5:
                    px_index = dollar_indices[3] + 1
                    px_value_index = dollar_indices[4] + 1
                    if parts[px_index].upper() == b"PX":
                        try:
                            ms = int(parts[px_value_index])
                            expiry = time.time() + ms / 1000.0
                        except Exception:
                            connection.sendall(b"-ERR PX value is not an integer\r\n")
                            return
                # Store value and expiry (None if not set)
                self.dictionary[key] = (value, expiry)
                connection.sendall(b"+OK\r\n")
            else:
                connection.sendall(
                    b"-ERR wrong number of arguments for 'set' command\r\n"
                )
        except Exception:
            connection.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")

    def handle_get(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [
                i for i, part in enumerate(parts) if part.startswith(b"$")
            ]
            if len(dollar_indices) >= 2:
                key_index = dollar_indices[1] + 1
                key = parts[key_index]
                entry = self.dictionary.get(key, None)
                if entry is None:
                    connection.sendall(b"$-1\r\n")
                    return
                value, expiry = entry
                # Check expiry
                if expiry is not None and time.time() > expiry:
                    # Key expired, remove it
                    del self.dictionary[key]
                    connection.sendall(b"$-1\r\n")
                    return
                response = b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                connection.sendall(response)
            else:
                connection.sendall(
                    b"-ERR wrong number of arguments for 'get' command\r\n"
                )
        except Exception:
            connection.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")

    def handle_rpush(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [
                i for i, part in enumerate(parts) if part.startswith(b"$")
            ]
            if len(dollar_indices) < 3:
                connection.sendall(
                    b"-ERR wrong number of arguments for 'rpush' command\r\n"
                )
                return
            key_index = dollar_indices[1] + 1
            key = parts[key_index]
            # All remaining $ indices are values
            values = []
            for i in range(2, len(dollar_indices)):
                value_index = dollar_indices[i] + 1
                values.append(parts[value_index])
            # Store or update the list in the dictionary
            entry = self.dictionary.get(key)
            if entry is not None:
                current_value, expiry = entry
                if not isinstance(current_value, list):
                    connection.sendall(b"-ERR wrong type\r\n")
                    return
                current_value.extend(values)
                lst = current_value
            else:
                lst = values
                expiry = None
            self.dictionary[key] = (lst, expiry)
            # Respond with the length of the list
        response = b":" + str(len(lst)).encode() + b"\r\n"
        connection.sendall(response)

    except Exception:
    connection.sendall(b"-ERR error processing 'rpush' command\r\n")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    handler = Handler()
    while True:
        connection, _ = server_socket.accept()  # wait for client
        client_thread = threading.Thread(target=handler.handle, args=(connection,))
        client_thread.start()


if __name__ == "__main__":
    main()
