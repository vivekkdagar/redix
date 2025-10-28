import os
import socket
import struct
import string

HOST = "0.0.0.0"
PORT = 6379
data_store = {}

# ============================================================
# RESP protocol
# ============================================================
def encode_resp(value):
    if isinstance(value, str):
        return f"+{value}\r\n".encode()
    elif isinstance(value, list):
        resp = f"*{len(value)}\r\n"
        for v in value:
            resp += f"${len(v)}\r\n{v}\r\n"
        return resp.encode()
    elif value is None:
        return b"$-1\r\n"
    return f"+{str(value)}\r\n".encode()


def parse_resp(data):
    if not data:
        return []
    lines = data.split(b"\r\n")
    if lines[0].startswith(b"*"):
        arr_len = int(lines[0][1:])
        args = []
        i = 1
        while i < len(lines):
            if i < len(lines) and lines[i].startswith(b"$"):
                i += 1
                if i < len(lines) and lines[i]:
                    args.append(lines[i].decode())
            i += 1
        return args[:arr_len]
    return [data.decode().strip()]

# ============================================================
# RDB Parser (realistic)
# ============================================================
def read_length(data, idx):
    """Decode Redis RDB length field."""
    if idx >= len(data):
        return 0, 1
    b = data[idx]
    enc_type = (b & 0xC0) >> 6
    if enc_type == 0:  # 6-bit
        return (b & 0x3F), 1
    elif enc_type == 1:  # 14-bit
        if idx + 1 >= len(data):
            return 0, 1
        val = ((b & 0x3F) << 8) | data[idx + 1]
        return val, 2
    elif enc_type == 2:  # 32-bit
        if idx + 4 >= len(data):
            return 0, 1
        val = struct.unpack(">I", data[idx + 1 : idx + 5])[0]
        return val, 5
    else:
        return 0, 1  # special encoding, ignore


def read_string(data, idx):
    """Read string of given length."""
    length, lbytes = read_length(data, idx)
    idx += lbytes
    val = data[idx : idx + length]
    return val.decode(errors="ignore"), idx + length


def load_rdb_file(dir_path, filename):
    """Parse minimal RDB file with key/value pairs."""
    path = os.path.join(dir_path, filename)
    if not os.path.exists(path):
        print(f"[RDB] No file found at {path}")
        return

    with open(path, "rb") as f:
        data = f.read()

    if not data.startswith(b"REDIS"):
        print("[RDB] Invalid RDB header")
        return

    i = 9  # skip header bytes ("REDIS" + version)
    while i < len(data):
        b = data[i]
        if b == 0xFE:  # Select DB
            i += 1
            db_num, step = read_length(data, i)
            i += step
        elif b == 0xFB:  # Resize DB
            i += 1
            _, step1 = read_length(data, i)
            i += step1
            _, step2 = read_length(data, i)
            i += step2
        elif b == 0x00:  # String key/value pair
            i += 1
            key, i = read_string(data, i)
            val, i = read_string(data, i)
            key_clean = "".join(ch for ch in key if ch in string.printable).strip()
            if key_clean:
                data_store[key_clean] = val
                print(f"[RDB] Loaded key: {key_clean}")
        elif b == 0xFF:  # EOF
            break
        else:
            i += 1

    print(f"[RDB] Keys loaded: {len(data_store)}")

# ============================================================
# Command handlers
# ============================================================
def handle_command(cmd_parts):
    if not cmd_parts:
        return encode_resp("ERR empty command")

    command = cmd_parts[0].upper()

    # --- Basic Redis commands ---
    if command == "PING":
        return encode_resp(cmd_parts[1] if len(cmd_parts) > 1 else "PONG")

    if command == "ECHO":
        return encode_resp(cmd_parts[1] if len(cmd_parts) > 1 else "")

    if command == "SET":
        if len(cmd_parts) < 3:
            return encode_resp("ERR wrong number of arguments for SET")
        data_store[cmd_parts[1]] = cmd_parts[2]
        return encode_resp("OK")

    if command == "GET":
        if len(cmd_parts) < 2:
            return b"$-1\r\n"
        val = data_store.get(cmd_parts[1])
        if val is None:
            return b"$-1\r\n"
        return f"${len(val)}\r\n{val}\r\n".encode()

    if command == "KEYS":
        keys = list(data_store.keys())
        return encode_resp(keys)

    # --- CONFIG commands ---
    if command == "CONFIG" and len(cmd_parts) >= 2 and cmd_parts[1].upper() == "GET":
        param = cmd_parts[2] if len(cmd_parts) > 2 else ""
        if param == "dir":
            return encode_resp(["dir", os.getcwd()])
        if param == "dbfilename":
            return encode_resp(["dbfilename", "dump.rdb"])
        return encode_resp([])

    # --- Replication handshake ---
    if command == "REPLCONF":
        return encode_resp("OK")

    if command == "PSYNC":
        repl_id = "?" * 40
        return f"+FULLRESYNC {repl_id} 0\r\n$0\r\n".encode()

    if command == "INFO":
        info = "role:master\r\n"
        return f"${len(info)}\r\n{info}\r\n".encode()

    return encode_resp(f"ERR unknown command '{command}'")


# ============================================================
# Server loop
# ============================================================
def main():
    dir_path = os.getenv("DIR", ".")
    dbfilename = os.getenv("DBFILENAME", "dump.rdb")
    load_rdb_file(dir_path, dbfilename)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(5)
        print(f"[Redis] Listening on {HOST}:{PORT}")

        while True:
            conn, _ = server.accept()
            with conn:
                data = conn.recv(4096)
                if not data:
                    continue
                cmd = parse_resp(data)
                resp = handle_command(cmd)
                conn.sendall(resp)


if __name__ == "__main__":
    main()