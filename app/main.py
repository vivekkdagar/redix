import socket
import threading
import time
import os
import struct

HOST = "0.0.0.0"
PORT = 6379

data = {}  # key -> (value, expiry_timestamp)


# -------------------- RDB PARSER --------------------
def read_key_val_from_db(dir_path, dbfilename, data_store):
    """Load key-values from RDB (if exists)"""
    rdb_file_loc = os.path.join(dir_path, dbfilename)
    if not os.path.isfile(rdb_file_loc):
        return

    try:
        with open(rdb_file_loc, "rb") as f:
            header = f.read(9)
            if not header.startswith(b"REDIS"):
                return

            while True:
                opcode = f.read(1)
                if not opcode:
                    break

                if opcode == b"\xfe":
                    f.read(1)  # DB number
                    continue
                elif opcode == b"\xfb":
                    _read_length_encoding(f)
                    _read_length_encoding(f)
                    continue
                elif opcode == b"\xfd":
                    f.read(4)
                    opcode = f.read(1)
                elif opcode == b"\xfc":
                    f.read(8)
                    opcode = f.read(1)
                elif opcode == b"\xff":
                    break
                elif opcode == b"\x00":
                    key = _read_string(f)
                    value = _read_string(f)
                    if key and value:
                        data_store[key] = (value, -1)
                else:
                    break

    except Exception as e:
        print(f"Error reading RDB file: {e}")


def _read_length_encoding(f):
    """Read a length-encoded integer from RDB file"""
    first_byte = f.read(1)
    if not first_byte:
        return 0

    first = first_byte[0]
    encoding_type = (first & 0xC0) >> 6

    if encoding_type == 0:
        return first & 0x3F
    elif encoding_type == 1:
        next_byte = f.read(1)[0]
        return ((first & 0x3F) << 8) | next_byte
    elif encoding_type == 2:
        return struct.unpack(">I", f.read(4))[0]
    else:
        return first & 0x3F


def _read_string(f):
    """Read a string from RDB file"""
    length = _read_length_encoding(f)
    if length == 0:
        return ""
    string_bytes = f.read(length)
    return string_bytes.decode("utf-8", errors="ignore")


# -------------------- COMMAND HANDLER --------------------
def handle_command(cmd):
    parts = cmd.strip().split()
    if not parts:
        return b""

    command = parts[0].upper()

    # ----------- PING -----------
    if command == "PING":
        if len(parts) == 1:
            return b"+PONG\r\n"
        else:
            msg = " ".join(parts[1:])
            return f"+{msg}\r\n".encode()

    # ----------- ECHO -----------
    elif command == "ECHO":
        msg = " ".join(parts[1:])
        return f"${len(msg)}\r\n{msg}\r\n".encode()

    # ----------- SET -----------
    elif command == "SET":
        key = parts[1]
        value = parts[2]
        expiry = -1
        if len(parts) > 4 and parts[3].upper() == "PX":
            expiry = time.time() + int(parts[4]) / 1000.0
        data[key] = (value, expiry)
        return b"+OK\r\n"

    # ----------- GET -----------
    elif command == "GET":
        key = parts[1]
        if key not in data:
            return b"$-1\r\n"
        value, expiry = data[key]
        if expiry != -1 and time.time() > expiry:
            del data[key]
            return b"$-1\r\n"
        return f"${len(value)}\r\n{value}\r\n".encode()

    # ----------- KEYS -----------
    elif command == "KEYS":
        pattern = parts[1]
        all_keys = [k for k in data.keys()]
        resp = f"*{len(all_keys)}\r\n"
        for k in all_keys:
            resp += f"${len(k)}\r\n{k}\r\n"
        return resp.encode()

    # ----------- EXPIRE -----------
    elif command == "EXPIRE":
        key = parts[1]
        ttl = int(parts[2])
        if key not in data:
            return b":0\r\n"
        val, _ = data[key]
        data[key] = (val, time.time() + ttl)
        return b":1\r\n"

    # ----------- BLPOP -----------
    elif command == "BLPOP":
        if len(parts) != 3:
            return b"-ERR wrong number of arguments for 'blpop' command\r\n"

        key = parts[1]
        timeout = float(parts[2])

        # If the key exists and is a list with data
        if key in data and isinstance(data[key][0], list) and data[key][0]:
            val = data[key][0].pop(0)
            return f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n".encode()

        # Wait for timeout
        time.sleep(timeout)

        # After timeout, check again
        if key in data and isinstance(data[key][0], list) and data[key][0]:
            val = data[key][0].pop(0)
            return f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n".encode()

        # If still empty, return null array
        return b"*-1\r\n"

    return b"-ERR unknown command\r\n"


# -------------------- CLIENT HANDLER --------------------
def handle_client(conn):
    try:
        while True:
            data_in = conn.recv(1024)
            if not data_in:
                break

            cmd = data_in.decode().strip()
            response = handle_command(cmd)
            conn.sendall(response)

    except Exception as e:
        print(f"Client error: {e}")
    finally:
        conn.close()


# -------------------- MAIN ENTRY --------------------
def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default=".")
    parser.add_argument("--dbfilename", default="")
    args = parser.parse_args()

    if args.dbfilename:
        read_key_val_from_db(args.dir, args.dbfilename, data)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()

        print(f"Server started on {HOST}:{PORT}")

        while True:
            conn, _ = s.accept()
            threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()