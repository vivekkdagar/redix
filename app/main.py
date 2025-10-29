import socket
import sys
import os

# In-memory store
store = {}
config = {
    "dir": "/tmp",
    "dbfilename": "dump.rdb"
}


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


def parse_rdb_file(filepath: str):
    """Parse RDB file and load keys into store"""
    try:
        if not os.path.exists(filepath):
            print(f"RDB file not found: {filepath}", flush=True)
            return

        with open(filepath, "rb") as f:
            print(f"Reading RDB file: {filepath}", flush=True)

            # Read until we find 0xFB (hash table size info)
            while True:
                operand = f.read(1)
                if not operand:
                    print("Reached end of file before finding 0xFB", flush=True)
                    return
                if operand == b"\xfb":
                    print("Found 0xFB opcode", flush=True)
                    break

            # Read number of keys
            num_keys = int.from_bytes(f.read(1), byteorder="little")
            f.read(1)  # Skip expire hash table size
            print(f"Number of keys: {num_keys}", flush=True)

            # Read each key-value pair
            for i in range(num_keys):
                expired = False
                print(f"Reading key {i + 1}/{num_keys}", flush=True)

                # Check for expiry
                top = f.read(1)
                if top == b"\xfc":  # Expiry in milliseconds
                    milli_time = int.from_bytes(f.read(8), byteorder="little")
                    import time
                    now = time.time() * 1000
                    if milli_time < now:
                        expired = True
                        print(f"Key expired (ms): {milli_time} < {now}", flush=True)
                    f.read(1)  # Skip value type
                elif top == b"\xfd":  # Expiry in seconds
                    sec_time = int.from_bytes(f.read(4), byteorder="little")
                    import time
                    if sec_time < time.time():
                        expired = True
                        print(f"Key expired (s)", flush=True)
                    f.read(1)  # Skip value type

                # Read key length and key
                length = int.from_bytes(f.read(1), byteorder="little")
                if (length >> 6) == 0b00:
                    length = length & 0b00111111
                else:
                    length = 0
                key = f.read(length).decode('utf-8')
                print(f"Key: {key}", flush=True)

                # Read value length and value
                length = int.from_bytes(f.read(1), byteorder="little")
                if (length >> 6) == 0b00:
                    length = length & 0b00111111
                else:
                    length = 0
                value = f.read(length).decode('utf-8')
                print(f"Value: {value}", flush=True)

                # Store if not expired
                if not expired:
                    store[key] = value
                    print(f"Stored: {key} = {value}", flush=True)
                else:
                    print(f"Skipped expired key: {key}", flush=True)

    except Exception as e:
        print(f"Error parsing RDB file: {e}", flush=True)
        import traceback
        traceback.print_exc()


def read_length_encoded(data: bytes, idx: int):
    """Read length-encoded integer"""
    first_byte = data[idx]

    # 00xxxxxx: 6-bit length
    if (first_byte & 0xC0) == 0x00:
        return first_byte & 0x3F, 1

    # 01xxxxxx: 14-bit length
    elif (first_byte & 0xC0) == 0x40:
        length = ((first_byte & 0x3F) << 8) | data[idx + 1]
        return length, 2

    # 10xxxxxx: 32-bit length
    elif (first_byte & 0xC0) == 0x80:
        length = int.from_bytes(data[idx + 1:idx + 5], byteorder='big')
        return length, 5

    # 11xxxxxx: special encoding
    else:
        return 0, 1


def read_key_value_pair(data: bytes, idx: int, value_type: int):
    """Read a key-value pair from RDB file"""
    start_idx = idx
    # Read key (always a string)
    key_length, bytes_read = read_length_encoded(data, idx)
    idx += bytes_read
    key = data[idx:idx + key_length].decode('utf-8')
    idx += key_length

    # Read value based on type
    if value_type == 0x00:  # String
        value_length, bytes_read = read_length_encoded(data, idx)
        idx += bytes_read
        value = data[idx:idx + value_length].decode('utf-8')
        idx += value_length
        return key, value, idx - start_idx

    return None, None, 0


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

        # Debug: Try to load RDB file NOW if store is empty
        if len(store) == 0:
            rdb_path = os.path.join(config["dir"], config["dbfilename"])
            print(f"KEYS: Store is empty, trying to load RDB from: {rdb_path}", flush=True)
            print(f"KEYS: File exists? {os.path.exists(rdb_path)}", flush=True)
            if os.path.exists(rdb_path):
                print(f"KEYS: File size: {os.path.getsize(rdb_path)} bytes", flush=True)
                parse_rdb_file(rdb_path)
                print(f"KEYS: Store after parse attempt: {list(store.keys())}", flush=True)

        print(f"KEYS command - pattern: {pattern}, store keys: {list(store.keys())}", flush=True)
        print(f"KEYS command - config: {config}", flush=True)
        print(f"KEYS command - sys.argv: {sys.argv}", flush=True)
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

            if key in config:
                return encode_array([key, config[key]])
            else:
                return encode_array([])
        elif sub == "SET":
            # Basic mock: CONFIG SET dir /tmp
            if len(parts) == 4 and parts[2].lower() in config:
                config[parts[2].lower()] = parts[3]
                return encode_simple_string("OK")
            return encode_error("ERR unsupported config set parameter")
        else:
            return encode_error("ERR unknown subcommand")

    # ------------------ UNKNOWN ------------------
    else:
        return encode_error(f"ERR unknown command '{command}'")


# ------------------ SERVER LOOP ------------------
def main():
    # Parse command line arguments
    args = sys.argv[1:]
    sys.stderr.write(f"STARTUP: Command line args: {args}\n")
    sys.stderr.flush()
    print(f"Command line args: {args}", flush=True)
    for i in range(len(args)):
        if args[i] == "--dir" and i + 1 < len(args):
            config["dir"] = args[i + 1]
        elif args[i] == "--dbfilename" and i + 1 < len(args):
            config["dbfilename"] = args[i + 1]

    sys.stderr.write(f"STARTUP: Config: {config}\n")
    sys.stderr.flush()
    print(f"Config: {config}", flush=True)

    # Load RDB file if it exists
    rdb_path = os.path.join(config["dir"], config["dbfilename"])
    sys.stderr.write(f"STARTUP: Loading RDB file from: {rdb_path}\n")
    sys.stderr.flush()
    print(f"Loading RDB file from: {rdb_path}", flush=True)
    parse_rdb_file(rdb_path)
    sys.stderr.write(f"STARTUP: Store after loading RDB: {store}\n")
    sys.stderr.flush()
    print(f"Store after loading RDB: {store}", flush=True)
    print(f"Number of keys loaded: {len(store)}", flush=True)

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