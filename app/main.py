import socket
import sys
import struct
import os

HOST = "0.0.0.0"
PORT = 6379
data = {}  # in-memory key-value store


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


# ---------------- RDB HELPERS ----------------
def _read_length_encoding(f):
    """Read a length-encoded integer from RDB file."""
    first_byte = f.read(1)
    if not first_byte:
        return 0

    fb = first_byte[0]
    type_ = (fb & 0xC0) >> 6

    if type_ == 0:  # 00 = 6-bit length
        return fb & 0x3F
    elif type_ == 1:  # 01 = 14-bit length
        next_byte = f.read(1)[0]
        return ((fb & 0x3F) << 8) | next_byte
    elif type_ == 2:  # 10 = 32-bit length
        return struct.unpack(">I", f.read(4))[0]
    else:  # 11 = special encoding
        return fb & 0x3F


def _read_string(f):
    """Read a string (length-prefixed) from RDB file."""
    length = _read_length_encoding(f)
    if length == 0:
        return ""
    raw = f.read(length)
    return raw.decode("utf-8", errors="ignore")


def read_key_val_from_db(dir_path, dbfilename, data):
    """Parse RDB and load keys into memory."""
    rdb_file = os.path.join(dir_path, dbfilename)
    if not os.path.exists(rdb_file):
        return

    try:
        with open(rdb_file, "rb") as f:
            header = f.read(9)
            if not header.startswith(b"REDIS"):
                return

            while True:
                b = f.read(1)
                if not b:
                    break

                # Database selector
                if b == b"\xFE":
                    f.read(1)  # skip db number
                    continue

                # RESIZEDB
                elif b == b"\xFB":
                    _read_length_encoding(f)
                    _read_length_encoding(f)
                    continue

                # Expiry seconds
                elif b == b"\xFD":
                    f.read(4)
                    b = f.read(1)

                # Expiry ms
                elif b == b"\xFC":
                    f.read(8)
                    b = f.read(1)

                # End of file
                elif b == b"\xFF":
                    break

                # String key-value pair
                if b == b"\x00":
                    key = _read_string(f)
                    val = _read_string(f)
                    if key:
                        data[key] = (val, -1)
    except Exception as e:
        print(f"Error loading RDB: {e}")


# ---------------- COMMAND HANDLER ----------------
def handle_command(cmd, dir_path, db_filename):
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
        matched = [k for k in data.keys() if pattern in k]
        return encode_array(matched)

    else:
        return encode_simple("")


# ---------------- MAIN SERVER ----------------
if __name__ == "__main__":
    dir_path = "."
    db_filename = "dump.rdb"

    if "--dir" in sys.argv:
        dir_path = sys.argv[sys.argv.index("--dir") + 1]
    if "--dbfilename" in sys.argv:
        db_filename = sys.argv[sys.argv.index("--dbfilename") + 1]

    read_key_val_from_db(dir_path, db_filename, data)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(1)
        print(f"Server started on {HOST}:{PORT}")

        while True:
            conn, _ = s.accept()
            with conn:
                req = conn.recv(4096)
                if not req:
                    continue
                cmd = decode_command(req)
                resp = handle_command(cmd, dir_path, db_filename)
                conn.sendall(resp)