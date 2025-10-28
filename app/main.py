import socket
import sys
import struct
import os

# ---------------- CONFIG ----------------
HOST = "0.0.0.0"
PORT = 6379
data = {}  # In-memory store: {key: (value, expiry)}


# ---------------- RDB PARSING HELPERS ----------------
def _read_length_encoding(f):
    """Read a length-encoded integer from RDB file"""
    first_byte = f.read(1)
    if not first_byte:
        return 0

    first = first_byte[0]
    encoding_type = (first & 0xC0) >> 6

    if encoding_type == 0:  # 00 - next 6 bits is length
        return first & 0x3F
    elif encoding_type == 1:  # 01 - next 14 bits is length
        next_byte = f.read(1)[0]
        return ((first & 0x3F) << 8) | next_byte
    elif encoding_type == 2:  # 10 - next 4 bytes is length
        return struct.unpack(">I", f.read(4))[0]
    else:  # 11 - special encoding
        return first & 0x3F


def _read_string(f):
    """Read a string from RDB file"""
    length = _read_length_encoding(f)
    if length == 0:
        return ""
    string_bytes = f.read(length)
    return string_bytes.decode('utf-8', errors='ignore')


def read_key_val_from_db(dir_path, dbfilename, data):
    """Load key-values from RDB (if exists)"""
    rdb_file_loc = os.path.join(dir_path, dbfilename)
    if not os.path.isfile(rdb_file_loc):
        return

    try:
        with open(rdb_file_loc, "rb") as f:
            # Read and verify header (REDIS + version)
            header = f.read(9)
            if not header.startswith(b"REDIS"):
                return

            # Read until we find the database selector (0xFE) or key-value pairs
            while True:
                opcode = f.read(1)
                if not opcode:
                    break

                # 0xFE = Database selector
                if opcode == b"\xfe":
                    f.read(1)  # skip db number
                    continue

                # 0xFB = Resizedb - hash table size info
                elif opcode == b"\xfb":
                    _read_length_encoding(f)
                    _read_length_encoding(f)
                    continue

                # 0xFD = Expiry time in seconds
                elif opcode == b"\xfd":
                    f.read(4)  # Skip 4 bytes for timestamp
                    opcode = f.read(1)  # Read actual value type

                # 0xFC = Expiry time in milliseconds
                elif opcode == b"\xfc":
                    f.read(8)  # Skip 8 bytes for timestamp
                    opcode = f.read(1)  # Read actual value type

                # 0xFF = End of RDB file
                elif opcode == b"\xff":
                    break

                # 0x00 = String encoding - this is a key-value pair
                elif opcode == b"\x00":
                    key = _read_string(f)
                    value = _read_string(f)
                    if key and value:
                        data[key] = (value, -1)  # -1 means no expiry

                else:
                    # Unknown opcode, stop
                    break

    except Exception as e:
        print(f"Error reading RDB file: {e}")


# ---------------- RESP HELPERS ----------------
def encode_simple(s):
    return f"+{s}\r\n".encode()


def encode_bulk(s):
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()


def encode_array(items):
    if items is None:
        return b"*-1\r\n"
    out = f"*{len(items)}\r\n"
    for item in items:
        out += f"${len(item)}\r\n{item}\r\n"
    return out.encode()


def decode_command(data):
    """Parse RESP command"""
    lines = data.split(b"\r\n")
    if not lines or not lines[0].startswith(b"*"):
        return []
    count = int(lines[0][1:])
    parts = []
    i = 1
    for _ in range(count):
        i += 1  # skip $len
        if i < len(lines):
            parts.append(lines[i])
        i += 1
    return [p.decode() for p in parts if p]


# ---------------- COMMAND HANDLER ----------------
def handle_command(cmd):
    if not cmd:
        return encode_simple("")

    c = cmd[0].upper()

    if c == "PING":
        return encode_simple("PONG")

    elif c == "ECHO" and len(cmd) > 1:
        return encode_bulk(cmd[1])

    elif c == "SET" and len(cmd) >= 3:
        data[cmd[1]] = (cmd[2], -1)
        return encode_simple("OK")

    elif c == "GET" and len(cmd) >= 2:
        val = data.get(cmd[1])
        return encode_bulk(val[0] if val else None)

    elif c == "CONFIG" and len(cmd) >= 3:
        param = cmd[2]
        if param == "dir":
            return encode_array(["dir", dir_path])
        elif param == "dbfilename":
            return encode_array(["dbfilename", db_filename])
        else:
            return encode_array([])

    elif c == "KEYS" and len(cmd) >= 2:
        pattern = cmd[1]
        if pattern == "*":
            return encode_array(list(data.keys()))
        else:
            matched = [k for k in data.keys() if pattern in k]
            return encode_array(matched)

    else:
        return encode_simple("")


# ---------------- MAIN SERVER ----------------
if __name__ == "__main__":
    dir_path = "."
    db_filename = "dump.rdb"

    # Parse CLI args
    if "--dir" in sys.argv:
        dir_path = sys.argv[sys.argv.index("--dir") + 1]
    if "--dbfilename" in sys.argv:
        db_filename = sys.argv[sys.argv.index("--dbfilename") + 1]

    # Load data from RDB file
    read_key_val_from_db(dir_path, db_filename, data)

    # Start server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(1)
        print(f"Server started on {HOST}:{PORT}")

        while True:
            conn, _ = s.accept()
            with conn:
                data_in = conn.recv(4096)
                if not data_in:
                    continue
                cmd = decode_command(data_in)
                response = handle_command(cmd)
                conn.sendall(response)