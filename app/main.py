import socket
import threading

# In-memory data store
store = {}


def parse_resp(data: bytes):
    """Minimal RESP parser to decode arrays of bulk strings."""
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

        parts = parse_resp(data)
        if not parts:
            continue

        cmd = parts[0].upper()

        # PING
        if cmd == "PING":
            connection.sendall(b"+PONG\r\n")

        # ECHO
        elif cmd == "ECHO" and len(parts) > 1:
            msg = parts[1].encode()
            response = b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
            connection.sendall(response)

        # SET
        elif cmd == "SET" and len(parts) >= 3:
            key, value = parts[1], parts[2]
            store[key] = value
            connection.sendall(b"+OK\r\n")

        # GET
        elif cmd == "GET" and len(parts) >= 2:
            key = parts[1]
            if key in store and not isinstance(store[key], list):
                val = store[key].encode()
                response = (
                    b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
                )
                connection.sendall(response)
            else:
                connection.sendall(b"$-1\r\n")

        # RPUSH (Create or append element)
        elif cmd == "RPUSH" and len(parts) == 3:
            key, value = parts[1], parts[2]
            if key not in store:
                # create a new list
                store[key] = [value]
            else:
                # append to existing list
                if isinstance(store[key], list):
                    store[key].append(value)
                else:
                    # overwrite non-list type for simplicity in this stage
                    store[key] = [value]
            # return list length
            length = len(store[key])
            response = f":{length}\r\n".encode()
            connection.sendall(response)

        else:
            connection.sendall(b"-ERR unknown command\r\n")

    connection.close()


def main():
    server = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()


if __name__ == "__main__":
    main()