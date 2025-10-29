import socket
import threading
import os
import time
import sys

# ---------------- In-memory storage ----------------
data_store = {}
config_store = {
    "dir": ".",
    "dbfilename": "dump.rdb"
}

# ---------------- RESP Encoding ----------------
def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()

def encode_bulk_string(s: str) -> bytes:
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()

def encode_array(arr) -> bytes:
    if arr is None:
        return b"*-1\r\n"
    res = f"*{len(arr)}\r\n"
    for item in arr:
        res += f"${len(item)}\r\n{item}\r\n"
    return res.encode()

# ---------------- RESP Parsing ----------------
def parse_resp_array(data: bytes):
    if not data or not data.startswith(b"*"):
        return []
    parts = data.split(b"\r\n")
    arr_len = int(parts[0][1:])
    result = []
    i = 1
    for _ in range(arr_len):
        if parts[i].startswith(b"$"):
            strlen = int(parts[i][1:])
            i += 1
            result.append(parts[i].decode())
            i += 1
    return result

# ---------------- RDB Parsing ----------------
def _read_length(f):
    first = f.read(1)
    if not first:
        return 0
    fb = first[0]
    prefix = (fb & 0xC0) >> 6
    if prefix == 0:
        return fb & 0x3F
    elif prefix == 1:
        b2 = f.read(1)
        return ((fb & 0x3F) << 8) | b2[0]
    elif prefix == 2:
        b4 = f.read(4)
        return int.from_bytes(b4, "little")
    else:
        return 0

def _read_string(f):
    """Read an RDB encoded string (supports length encodings)"""
    first = f.read(1)
    if not first:
        return None
    fb = first[0]
    prefix = (fb & 0xC0) >> 6
    if prefix == 0:
        strlen = fb & 0x3F
    elif prefix == 1:
        b2 = f.read(1)
        strlen = ((fb & 0x3F) << 8) | b2[0]
    elif prefix == 2:
        b4 = f.read(4)
        strlen = int.from_bytes(b4, "little")
    else:
        return None
    return f.read(strlen).decode("utf-8", errors="ignore")

def read_rdb_file(dir_path, dbfile):
    """Load keys from dump.rdb"""
    path = os.path.join(dir_path, dbfile)
    if not os.path.exists(path):
        return
    try:
        with open(path, "rb") as f:
            header = f.read(9)  # REDISxxxx
            if not header.startswith(b"REDIS"):
                return
            while True:
                b = f.read(1)
                if not b:
                    break
                if b == b'\x00':  # type: string
                    key = _read_string(f)
                    val = _read_string(f)
                    if key:
                        data_store[key] = (val, -1)
                elif b == b'\xfe':  # SELECTDB opcode
                    _ = _read_length(f)
                elif b == b'\xfb':  # RESIZEDB
                    _ = _read_length(f)
                    _ = _read_length(f)
                elif b == b'\xff':  # EOF
                    break
    except Exception as e:
        print("Error parsing RDB:", e)

# ---------------- Command Handler ----------------
def handle_command(cmd):
    if not cmd:
        return b""

    op = cmd[0].upper()

    if op == "PING":
        return encode_simple_string("PONG" if len(cmd) == 1 else cmd[1])
    elif op == "ECHO":
        return encode_bulk_string(cmd[1] if len(cmd) > 1 else "")
    elif op == "SET":
        key, val = cmd[1], cmd[2]
        expiry = -1
        if len(cmd) > 3 and cmd[3].upper() == "PX":
            expiry = time.time() + int(cmd[4]) / 1000.0
        data_store[key] = (val, expiry)
        return encode_simple_string("OK")
    elif op == "GET":
        key = cmd[1]
        if key not in data_store:
            return encode_bulk_string(None)
        val, exp = data_store[key]
        if exp != -1 and time.time() > exp:
            del data_store[key]
            return encode_bulk_string(None)
        return encode_bulk_string(val)
    elif op == "CONFIG" and len(cmd) >= 3 and cmd[1].upper() == "GET":
        key = cmd[2]
        if key in config_store:
            return encode_array([key, config_store[key]])
        return encode_array([])
    elif op == "KEYS":
        if cmd[1] == "*":
            return encode_array(list(data_store.keys()))
        else:
            return encode_array([])
    else:
        return encode_simple_string("OK")

# ---------------- Networking ----------------
def handle_client(conn):
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            cmd = parse_resp_array(data)
            resp = handle_command(cmd)
            conn.sendall(resp)
    except Exception as e:
        print("Client error:", e)
    finally:
        conn.close()

def main():
    args = sys.argv[1:]
    if "--dir" in args:
        config_store["dir"] = args[args.index("--dir") + 1]
    if "--dbfilename" in args:
        config_store["dbfilename"] = args[args.index("--dbfilename") + 1]

    read_rdb_file(config_store["dir"], config_store["dbfilename"])
    print("Loaded keys:", list(data_store.keys()))

    server = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()