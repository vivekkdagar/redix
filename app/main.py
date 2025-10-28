import socket
import threading
import time

# In-memory key-value store
store = {}
# Expiry timestamps for keys (in milliseconds)
expiry_map = {}

# Thread lock to prevent race conditions when accessing store and expiry_map
lock = threading.Lock()


def parse_resp(data: bytes):
    """
    Minimal RESP parser for arrays of bulk strings.
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


def is_expired(key: str) -> bool:
    """
    Passive expiry check.
    Remove expired keys when accessed.
    """
    if key not in expiry_map:
        return False

    current_time_ms = int(time.time() * 1000)
    expiry_time = expiry_map[key]

    if current_time_ms >= expiry_time:
        # Expired: clean up
        with lock:
            store.pop(key, None)
            expiry_map.pop(key, None)
        return True
    return False


def handle_client(connection):
    while True:
        data = connection.recv(1024)
        if not data:
            break

        command_parts = parse_resp(data)
        if not command_parts:
            continue

        command = command_parts[0].upper()

        # PING
        if command == "PING":
            connection.sendall(b"+PONG\r\n")

        # ECHO
        elif command == "ECHO" and len(command_parts) > 1:
            msg = command_parts[1].encode()
            response = b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
            connection.sendall(response)

        # SET with optional PX (expiry)
        elif command == "SET" and len(command_parts) >= 3:
            key = command_parts[1]
            value = command_parts[2]

            with lock:
                store[key] = value
                # Handle optional expiry (PX <milliseconds>)
                if len(command_parts) >= 5 and command_parts[3].upper() == "PX":
                    try:
                        expiry_ms = int(command_parts[4])
                        expiry_map[key] = int(time.time() * 1000) + expiry_ms
                    except ValueError:
                        pass  # ignore invalid PX values

            connection.sendall(b"+OK\r\n")

        # GET with expiry validation
        elif command == "GET" and len(command_parts) >= 2:
            key = command_parts[1]

            if is_expired(key):
                connection.sendall(b"$-1\r\n")
                continue

            value = store.get(key)
            if value is None:
                connection.sendall(b"$-1\r\n")
            else:
                val_bytes = value.encode()
                response = (
                    b"$" + str(len(val_bytes)).encode() + b"\r\n" + val_bytes + b"\r\n"
                )
                connection.sendall(response)

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