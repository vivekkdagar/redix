import socket
import struct
import datetime
import os

# ------------------- RESP ENCODER -------------------

def encode(data):
    """Encode Python objects into RESP2 format."""
    if isinstance(data, str):
        return f"+{data}\r\n".encode()
    elif isinstance(data, int):
        return f":{data}\r\n".encode()
    elif isinstance(data, list):
        res = f"*{len(data)}\r\n".encode()
        for item in data:
            res += encode_bulk_string(item)
        return res
    elif data is None:
        return b"$-1\r\n"
    else:
        raise TypeError(f"Unsupported type for encode(): {type(data)}")

def encode_bulk_string(s):
    """Encode a bulk string ($)"""
    if s is None:
        return b"$-1\r\n"
    if not isinstance(s, (str, bytes)):
        raise TypeError("Bulk string must be str or bytes")
    if isinstance(s, str):
        s = s.encode()
    return b"$" + str(len(s)).encode() + b"\r\n" + s + b"\r\n"


# ------------------- RESP DECODER -------------------

def parse(data):
    """Parse RESP2 array commands from redis-cli"""
    if not data.startswith(b"*"):
        raise ValueError("Invalid RESP array")
    parts = data.split(b"\r\n")
    count = int(parts[0][1:])
    items = []
    i = 1
    while len(items) < count and i < len(parts):
        if parts[i].startswith(b"$"):
            strlen = int(parts[i][1:])
            i += 1
            items.append(parts[i].decode())
        i += 1
    return items


# ------------------- RDB READERS -------------------

def read_rdb_key(dir, dbfilename):
    """Read all keys from the RDB file"""
    rdb_file_loc = os.path.join(dir, dbfilename)
    if not os.path.exists(rdb_file_loc):
        return []

    with open(rdb_file_loc, "rb") as f:
        # find start of DB
        while True:
            b = f.read(1)
            if not b:
                return []
            if b == b"\xfb":
                break

        numKeys = struct.unpack("B", f.read(1))[0]
        f.read(1)
        ans = []

        for _ in range(numKeys):
            top = f.read(1)
            if not top:
                break
            if top == b"\xfc":
                f.read(9)
            elif top == b"\xfd":
                f.read(5)

            # key length
            key_len_byte = f.read(1)
            if not key_len_byte:
                break
            length = key_len_byte[0] & 0b00111111
            key = f.read(length).decode()

            # value length
            val_len_byte = f.read(1)
            if not val_len_byte:
                break
            val_len = val_len_byte[0] & 0b00111111
            f.read(val_len)
            ans.append(key)
        return ans


def read_key_val_from_db(dir, dbfilename, data):
    """Load all key-values from RDB into memory"""
    rdb_file_loc = os.path.join(dir, dbfilename)
    if not os.path.exists(rdb_file_loc):
        return
    with open(rdb_file_loc, "rb") as f:
        while True:
            b = f.read(1)
            if not b:
                return
            if b == b"\xfb":
                break

        numKeys = struct.unpack("B", f.read(1))[0]
        f.read(1)

        for _ in range(numKeys):
            expired = False
            top = f.read(1)
            if not top:
                break
            if top == b"\xfc":
                milliTime = int.from_bytes(f.read(8), "little")
                if milliTime < datetime.datetime.now().timestamp() * 1000:
                    expired = True
                f.read(1)
            elif top == b"\xfd":
                secTime = int.from_bytes(f.read(4), "little")
                if secTime < datetime.datetime.now().timestamp():
                    expired = True
                f.read(1)

            key_len = f.read(1)
            if not key_len:
                break
            length = key_len[0] & 0b00111111
            key = f.read(length).decode()

            val_len = f.read(1)
            if not val_len:
                break
            val_length = val_len[0] & 0b00111111
            val = f.read(val_length).decode()

            if not expired:
                data[key] = (val, -1)


# ------------------- SERVER -------------------

def main():
    HOST = "localhost"
    PORT = 6379

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)

    data = {}
    current_dir = os.getcwd()
    current_dbfilename = "dump.rdb"

    # Load initial RDB if available
    read_key_val_from_db(current_dir, current_dbfilename, data)

    print(f"Redis clone running on {HOST}:{PORT}")

    while True:
        conn, _ = server.accept()
        try:
            buffer = conn.recv(1024)
            if not buffer:
                conn.close()
                continue

            cmd_parts = parse(buffer)
            if not cmd_parts:
                conn.close()
                continue

            command = cmd_parts[0].upper()
            args = cmd_parts[1:]

            # ------------------- COMMANDS -------------------

            if command == "PING":
                conn.sendall(encode("PONG"))

            elif command == "ECHO":
                conn.sendall(encode_bulk_string(args[0]))

            elif command == "SET":
                key, value = args[0], args[1]
                data[key] = (value, -1)
                conn.sendall(encode("OK"))

            elif command == "GET":
                key = args[0]
                val = data.get(key)
                if val:
                    conn.sendall(encode_bulk_string(val[0]))
                else:
                    # fallback to RDB
                    rdb_val = None
                    try:
                        for k, v in data.items():
                            if k == key:
                                rdb_val = v[0]
                        if rdb_val:
                            conn.sendall(encode_bulk_string(rdb_val))
                        else:
                            conn.sendall(encode_bulk_string(None))
                    except:
                        conn.sendall(encode_bulk_string(None))

            elif command == "CONFIG" and len(args) == 2 and args[0].upper() == "GET":
                param = args[1]
                if param.lower() == "dir":
                    conn.sendall(encode([current_dir]))
                elif param.lower() == "dbfilename":
                    conn.sendall(encode([current_dbfilename]))
                else:
                    conn.sendall(encode([]))

            elif command == "KEYS":
                pattern = args[0] if args else "*"
                keys = []

                # in-memory keys
                keys.extend(list(data.keys()))

                # include RDB keys if available
                try:
                    rdb_keys = read_rdb_key(current_dir, current_dbfilename)
                    keys.extend(rdb_keys)
                except:
                    pass

                # unique keys
                keys = list(set(keys))
                conn.sendall(encode(keys))

            else:
                conn.sendall(encode(f"Unknown command: {command}"))

        except Exception as e:
            conn.sendall(encode(f"Error: {str(e)}"))
        finally:
            conn.close()


if __name__ == "__main__":
    main()