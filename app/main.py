import os
import socket
import struct
import string

HOST = "0.0.0.0"
PORT = 6379

data_store = {}

# ============================================================
# RESP protocol parsing and serialization
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
    """
    Parse RESP data from client into list of arguments.
    """
    if not data:
        return []
    lines = data.split(b"\r\n")
    if lines[0].startswith(b"*"):
        arr_len = int(lines[0][1:])
        args = []
        i = 1
        while i < len(lines):
            if lines[i].startswith(b"$"):
                length = int(lines[i][1:])
                if i + 1 < len(lines):
                    args.append(lines[i + 1].decode())
                i += 2
            else:
                i += 1
        return args[:arr_len]
    return [data.decode().strip()]


# ============================================================
# RDB parsing utilities
# ============================================================
def read_length(data, i):
    """Parse Redis RDB length encoding."""
    if i >= len(data):
        return 0, 1
    first = data[i]
    typebits = (first & 0xC0) >> 6  # top 2 bits
    if typebits == 0:  # 6-bit length
        return (first & 0x3F), 1
    elif typebits == 1:  # 14-bit length
        if i + 1 >= len(data):
            return 0, 1
        val = ((first & 0x3F) << 8) | data[i + 1]
        return val, 2
    elif typebits == 2:  # 32-bit length
        if i + 4 >= len(data):
            return 0, 1
        val = struct.unpack(">I", data[i + 1 : i + 5])[0]
        return val, 5
    else:  # special encoding
        return 0, 1


def load_rdb_file(dir_path, filename):
    """Safely load RDB file and extract string keys."""
    path = os.path.join(dir_path, filename)
    if not os.path.exists(path):
        print(f"[RDB] No file found at {path}")
        return

    try:
        with open(path, "rb") as f:
            data = f.read()

        if not data.startswith(b"REDIS"):
            print("[RDB] Invalid signature")
            return

        i = 0
        while i < len(data):
            byte = data[i]
            if byte in (0x00, 0x01, 0x02, 0x03, 0x06):
                i += 1
                keylen, step = read_length(data, i)
                i += step
                if i + keylen > len(data):
                    break
                raw_key = data[i : i + keylen]
                i += keylen

                # Decode & sanitize key
                key = raw_key.decode("utf-8", errors="ignore")
                key = "".join(ch for ch in key if ch in string.ascii_letters)
                if key:
                    data_store[key] = "(rdb_value)"
                    print(f"[RDB] Loaded key: {key}")
            else:
                i += 1

        print(f"[RDB] Total keys loaded: {len(data_store)}")
    except Exception as e:
        print("[RDB] Parse error:", e)


# ============================================================
# Command handlers
# ============================================================
def handle_command(cmd_parts):
    if not cmd_parts:
        return encode_resp("ERR empty command")

    command = cmd_parts[0].upper()

    # --- Basic Redis commands ---
    if command == "PING":
        if len(cmd_parts) > 1:
            return encode_resp(cmd_parts[1])
        return encode_resp("PONG")

    if command == "ECHO":
        if len(cmd_parts) < 2:
            return encode_resp("ERR wrong number of arguments for 'ECHO'")
        return encode_resp(cmd_parts[1])

    if command == "SET":
        if len(cmd_parts) < 3:
            return encode_resp("ERR wrong number of arguments for 'SET'")
        data_store[cmd_parts[1]] = cmd_parts[2]
        return encode_resp("OK")

    if command == "GET":
        if len(cmd_parts) < 2:
            return encode_resp(None)
        val = data_store.get(cmd_parts[1])
        if val is None:
            return b"$-1\r\n"
        return f"${len(val)}\r\n{val}\r\n".encode()

    if command == "KEYS":
        pattern = cmd_parts[1] if len(cmd_parts) > 1 else "*"
        keys = list(data_store.keys())
        return encode_resp(keys)

    # --- CONFIG command ---
    if command == "CONFIG" and len(cmd_parts) >= 2 and cmd_parts[1].upper() == "GET":
        param = cmd_parts[2] if len(cmd_parts) > 2 else ""
        if param == "dir":
            return encode_resp(["dir", os.getcwd()])
        if param == "dbfilename":
            return encode_resp(["dbfilename", "dump.rdb"])
        return encode_resp([])

    # --- REPL (replication handshake) commands ---
    if command == "REPLCONF":
        return encode_resp("OK")

    if command == "PSYNC":
        # Minimal mock replication response
        repl_id = "?" * 40
        offset = 0
        return f"+FULLRESYNC {repl_id} {offset}\r\n".encode() + b"$0\r\n"

    if command == "INFO":
        info_str = "role:master\r\n"
        return f"${len(info_str)}\r\n{info_str}\r\n".encode()

    return encode_resp(f"ERR unknown command '{command}'")


# ============================================================
# Server loop
# ============================================================
def main():
    # Load RDB if provided
    dir_path = os.getenv("DIR", ".")
    dbfilename = os.getenv("DBFILENAME", "dump.rdb")
    load_rdb_file(dir_path, dbfilename)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(5)
        print(f"[Redis] Listening on {HOST}:{PORT}")

        while True:
            conn, addr = server.accept()
            with conn:
                data = conn.recv(4096)
                if not data:
                    continue
                cmds = parse_resp(data)
                response = handle_command(cmds)
                conn.sendall(response)


if __name__ == "__main__":
    main()