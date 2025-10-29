import socket

# In-memory store
store = {}


def encode_bulk_string(value: str) -> str:
    return f"${len(value)}\r\n{value}\r\n"


def encode_simple_string(value: str) -> str:
    return f"+{value}\r\n"


def encode_error(value: str) -> str:
    return f"-{value}\r\n"


def encode_integer(value: int) -> str:
    return f":{value}\r\n"


def encode_array(values: list[str]) -> str:
    if not values:
        return "*0\r\n"
    resp = f"*{len(values)}\r\n"
    for v in values:
        resp += encode_bulk_string(v)
    return resp


def handle_command(parts: list[str]) -> str:
    if not parts:
        return encode_error("ERR empty command")

    command = parts[0].upper()

    # ------------------ PING ------------------
    if command == "PING":
        if len(parts) == 1:
            return encode_simple_string("PONG")
        else:
            return encode_bulk_string(parts[1])

    # ------------------ ECHO ------------------
    elif command == "ECHO":
        if len(parts) < 2:
            return encode_error("ERR wrong number of arguments for 'echo' command")
        return encode_bulk_string(parts[1])

    # ------------------ SET ------------------
    elif command == "SET":
        if len(parts) < 3:
            return encode_error("ERR wrong number of arguments for 'set' command")
        key, value = parts[1], parts[2]
        store[key] = value
        return encode_simple_string("OK")

    # ------------------ GET ------------------
    elif command == "GET":
        if len(parts) < 2:
            return encode_error("ERR wrong number of arguments for 'get' command")
        key = parts[1]
        if key not in store:
            return "$-1\r\n"
        return encode_bulk_string(store[key])

    # ------------------ EXISTS ------------------
    elif command == "EXISTS":
        if len(parts) < 2:
            return encode_error("ERR wrong number of arguments for 'exists' command")
        count = sum(1 for k in parts[1:] if k in store)
        return encode_integer(count)

    # ------------------ RPUSH ------------------
    elif command == "RPUSH":
        if len(parts) < 3:
            return encode_error("ERR wrong number of arguments for 'rpush' command")
        key = parts[1]
        values = parts[2:]

        # Ensure list type
        if key not in store:
            store[key] = []
        elif not isinstance(store[key], list):
            return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")

        store[key].extend(values)
        return encode_integer(len(store[key]))

    # ------------------ LPOP ------------------
    elif command == "LPOP":
        if len(parts) < 2:
            return encode_error("ERR wrong number of arguments for 'lpop' command")
        key = parts[1]
        if key not in store or not store[key]:
            return "$-1\r\n"
        if not isinstance(store[key], list):
            return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        value = store[key].pop(0)
        if not store[key]:  # Remove key if list is now empty
            del store[key]
        return encode_bulk_string(value)

    # ------------------ RPOP ------------------
    elif command == "RPOP":
        if len(parts) < 2:
            return encode_error("ERR wrong number of arguments for 'rpop' command")
        key = parts[1]
        if key not in store or not store[key]:
            return "$-1\r\n"
        if not isinstance(store[key], list):
            return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        value = store[key].pop()
        if not store[key]:  # Remove key if list is now empty
            del store[key]
        return encode_bulk_string(value)

    # ------------------ KEYS ------------------
    elif command == "KEYS":
        if len(parts) != 2:
            return encode_error("ERR wrong number of arguments for 'keys' command")
        pattern = parts[1]
        if pattern == "*":
            # Return all keys, regardless of type
            return encode_array(list(store.keys()))
        # (No advanced pattern matching needed for Codecrafters)
        return encode_array([])

    # ------------------ CONFIG ------------------
    elif command == "CONFIG":
        if len(parts) < 2:
            return encode_error("ERR wrong number of arguments for 'config' command")
        sub = parts[1].upper()

        if sub == "GET":
            if len(parts) < 3:
                return encode_error("ERR wrong number of arguments for 'config get' command")
            key = parts[2].lower()

            if key == "dir":
                dir_path = "/tmp"
                return encode_array(["dir", dir_path])
            else:
                return encode_array([])
        elif sub == "SET":
            # Basic mock: CONFIG SET dir /tmp
            if len(parts) == 4 and parts[2].lower() == "dir":
                return encode_simple_string("OK")
            return encode_error("ERR unsupported config set parameter")
        else:
            return encode_error("ERR unknown subcommand")

    # ------------------ UNKNOWN ------------------
    else:
        return encode_error(f"ERR unknown command '{command}'")


# ------------------ SERVER LOOP ------------------
def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print(f"Redis server listening on localhost:6379", flush=True)
    while True:
        client, _ = server_socket.accept()
        data = client.recv(1024).decode().strip()
        print(f"RAW DATA RECEIVED: {repr(data)}", flush=True)

        if not data:
            client.close()
            continue

        # Parse RESP (naive but fine for Codecrafters)
        lines = data.split("\r\n")
        parts = []
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith("$") and i + 1 < len(lines):
                # Bulk string: next line is the actual content
                parts.append(lines[i + 1])
                i += 2
            elif line and not line.startswith("*"):
                # Simple string or other non-bulk content
                parts.append(line)
                i += 1
            else:
                i += 1

        print(f"Parsed command parts: {parts}", flush=True)
        print(f"Current store BEFORE command: {store}", flush=True)

        response = handle_command(parts)

        print(f"Current store AFTER command: {store}", flush=True)
        print(f"Response: {repr(response)}", flush=True)

        client.sendall(response.encode())
        client.close()


if __name__ == "__main__":
    main()