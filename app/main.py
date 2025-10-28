import socket
import threading
import time

# In-memory data store
store = {}
expiry_map = {}
lock = threading.Lock()


def parse_resp(data: bytes):
    """Parse RESP arrays of bulk strings into a Python list."""
    parts = data.split(b"\r\n")
    result = []
    i = 0
    while i < len(parts):
        if parts[i].startswith(b"$"):
            i += 1
            if i < len(parts) and parts[i]:
                result.append(parts[i].decode())
        i += 1
    return result


def is_expired(key: str) -> bool:
    """Passive expiry check."""
    if key not in expiry_map:
        return False
    if int(time.time() * 1000) >= expiry_map[key]:
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

        parts = parse_resp(data)
        if not parts:
            continue

        cmd = parts[0].upper()

        # --- PING ---
        if cmd == "PING":
            connection.sendall(b"+PONG\r\n")

        # --- ECHO ---
        elif cmd == "ECHO" and len(parts) > 1:
            msg = parts[1].encode()
            resp = b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
            connection.sendall(resp)

        # --- SET ---
        elif cmd == "SET" and len(parts) >= 3:
            key, value = parts[1], parts[2]
            with lock:
                store[key] = value
                # Optional PX expiry
                if len(parts) >= 5 and parts[3].upper() == "PX":
                    try:
                        ttl_ms = int(parts[4])
                        expiry_map[key] = int(time.time() * 1000) + ttl_ms
                    except ValueError:
                        pass
            connection.sendall(b"+OK\r\n")

        # --- GET ---
        elif cmd == "GET" and len(parts) >= 2:
            key = parts[1]
            if is_expired(key):
                connection.sendall(b"$-1\r\n")
                continue
            value = store.get(key)
            if value is None:
                connection.sendall(b"$-1\r\n")
            else:
                v = value.encode()
                connection.sendall(b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n")

        # --- RPUSH (Create a new list) ---
        elif cmd == "RPUSH" and len(parts) >= 3:
            key, element = parts[1], parts[2]
            with lock:
                # If key doesn't exist, create a new list
                if key not in store:
                    store[key] = [element]
                else:
                    # For now, per this stage — only handle list creation
                    # (Later stages will append additional elements)
                    store[key].append(element)
                length = len(store[key])
            # RESP integer response
            connection.sendall(b":" + str(length).encode() + b"\r\n")

        else:
            connection.sendall(b"-ERR unknown command\r\n")

    connection.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()


if __name__ == "__main__":
    main()