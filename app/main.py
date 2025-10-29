import socket
import threading
import sys
import os
import struct
import time


# ---------------------------------------------------------------------
# RDB LOADING
# ---------------------------------------------------------------------

def read_key_val_from_db(dir_path, dbfilename, data):
    """Load key-values from an RDB file (Stage JZ6)."""
    rdb_path = os.path.join(dir_path, dbfilename)
    if not os.path.exists(rdb_path):
        return

    try:
        with open(rdb_path, "rb") as f:
            header = f.read(9)
            if not header.startswith(b"REDIS"):
                return

            # Keep track of the current expiry time for the next key
            current_expiry = -1

            while True:
                opcode = f.read(1)
                if not opcode:
                    break

                # The order is usually: EXPIRY -> TYPE -> KEY -> VALUE
                if opcode == b'\xfe':  # SELECTDB
                    f.read(1)
                    continue

                elif opcode == b'\xfb':  # RESIZEDB
                    _read_length_encoding(f)
                    _read_length_encoding(f)
                    continue

                elif opcode == b'\xfd':  # EXPIRETIME (seconds)
                    # Expiry is loaded as Unix timestamp (seconds)
                    expiry_sec = struct.unpack("<I", f.read(4))[0]
                    current_expiry = expiry_sec

                elif opcode == b'\xfc':  # EXPIRETIME_MS
                    # Expiry is loaded as Unix timestamp (milliseconds)
                    expiry_ms = struct.unpack("<Q", f.read(8))[0]
                    # Convert to seconds float
                    current_expiry = expiry_ms / 1000.0

                # If we encounter the TYPE byte (e.g., STRING, LIST, HASH), it means a key-value pair follows.
                elif opcode == b'\x00':  # STRING (or any other data type)
                    key = _read_string(f)
                    value = _read_string(f)
                    if key:
                        # Store the value and the associated expiry
                        data[key] = (value, current_expiry)

                        # Reset expiry after the key is stored
                    current_expiry = -1
                    continue

                elif opcode == b'\xff':  # EOF
                    break

                else:
                    # Should handle other data types here, but for now, break if unknown
                    break

    except Exception as e:
        print("Error reading RDB:", e)


def _read_length_encoding(f):
    """Decode Redis RDB length encoding."""
    first = f.read(1)
    if not first:
        return 0
    first = first[0]
    type_bits = (first & 0xC0) >> 6

    if type_bits == 0:
        return first & 0x3F
    elif type_bits == 1:
        second = f.read(1)[0]
        return ((first & 0x3F) << 8) | second
    elif type_bits == 2:
        return struct.unpack(">I", f.read(4))[0]
    else:
        # Handle 3: Special length encoding (not used for length)
        # This could be for LZF compression or other encodings, but for length reading, return value bits
        return first & 0x3F


def _read_string(f):
    """Decode Redis RDB encoded string."""
    length = _read_length_encoding(f)

    # Check for special encodings (type_bits == 3)
    # 0, 1, 2 are special integer encodings. 3 is LZF.
    # For now, just handle the simple length read

    if length == 0:
        return ""

    val = f.read(length)

    # Simple strings are returned as bytes, but we decode them for the data store
    return val.decode("utf-8", errors="ignore")


# ---------------------------------------------------------------------
# RESP ENCODING HELPERS
# ---------------------------------------------------------------------

def encode_simple_string(s):
    return f"+{s}\r\n".encode()


def encode_bulk_string(s):
    if s is None:
        return b"$-1\r\n"
    # Ensure s is a string for len() and formatting
    if isinstance(s, bytes):
        s = s.decode("utf-8", errors="ignore")

    return f"${len(s)}\r\n{s}\r\n".encode()


def encode_integer(num):
    return f":{num}\r\n".encode()


def encode_array(arr):
    if arr is None:
        return b"*-1\r\n"

    resp = f"*{len(arr)}\r\n"
    for el in arr:
        # Ensure array elements are treated as bulk strings
        if isinstance(el, bytes):
            el = el.decode("utf-8", errors="ignore")

        resp += f"${len(el)}\r\n{el}\r\n"
    return resp.encode()


# ---------------------------------------------------------------------
# RESP PARSER
# ---------------------------------------------------------------------

def parse_resp(data):
    """Parse RESP array like *2\r\n$4\r\nPING\r\n$4\r\nECHO\r\n"""
    try:
        parts = data.split(b"\r\n")
        if not parts or not parts[0].startswith(b"*"):
            return []
        n = int(parts[0][1:])
        arr = []
        idx = 1
        while idx < len(parts) and len(arr) < n:
            if parts[idx].startswith(b"$"):
                # length = int(parts[idx][1:]) # Length is not strictly needed here
                # Append the bulk string content, decoded
                arr.append(parts[idx + 1].decode())
                idx += 2
            else:
                idx += 1
        return arr
    except Exception:
        return []


# ---------------------------------------------------------------------
# REDIS SERVER LOGIC
# ---------------------------------------------------------------------

def handle_client(conn, addr, data_store, dir_path, dbfilename):
    while True:
        try:
            req = conn.recv(1024)
            if not req:
                break

            cmd_parts = parse_resp(req)
            if not cmd_parts:
                continue

            cmd = cmd_parts[0].upper()

            if cmd == "PING":
                resp = encode_simple_string("PONG")

            elif cmd == "ECHO":
                resp = encode_bulk_string(cmd_parts[1])

            elif cmd == "SET":
                key, val = cmd_parts[1], cmd_parts[2]
                expiry = -1
                if len(cmd_parts) > 4 and cmd_parts[3].upper() == "PX":
                    expiry = time.time() + int(cmd_parts[4]) / 1000.0
                data_store[key] = (val, expiry)
                resp = encode_simple_string("OK")

            elif cmd == "GET":
                key = cmd_parts[1]
                if key in data_store:
                    val, expiry = data_store[key]
                    # Check for expiration
                    if expiry != -1 and time.time() > expiry:
                        del data_store[key]
                        resp = encode_bulk_string(None)
                    else:
                        resp = encode_bulk_string(val)
                else:
                    resp = encode_bulk_string(None)

            elif cmd == "KEYS":
                # pattern = cmd_parts[1] # Not used for '*' pattern in this simple implementation

                # --- FIX START: Filter out expired keys ---
                valid_keys = []
                current_time = time.time()
                keys_to_check = list(data_store.keys())  # Iterate over a copy of keys

                for key in keys_to_check:
                    val, expiry = data_store[key]

                    # Check for expiration
                    if expiry != -1 and current_time > expiry:
                        # Key is expired, delete it and do not include in result
                        del data_store[key]
                    else:
                        # Key is not expired, include it in the result
                        valid_keys.append(key)

                resp = encode_array(valid_keys)
                # --- FIX END ---

            elif cmd == "CONFIG":
                if len(cmd_parts) >= 3 and cmd_parts[1].upper() == "GET":
                    param = cmd_parts[2]
                    if param == "dir":
                        resp = encode_array(["dir", dir_path])
                    elif param == "dbfilename":
                        resp = encode_array(["dbfilename", dbfilename])
                    else:
                        resp = encode_array([])
                else:
                    resp = encode_array([])

            else:
                resp = encode_simple_string("ERR unknown command")

            conn.sendall(resp)

        except ConnectionResetError:
            break
        except Exception as e:
            print("Error:", e)
            break

    conn.close()


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    dir_path = "."
    dbfilename = "dump.rdb"

    # Parse CLI args
    for i, arg in enumerate(sys.argv):
        if arg == "--dir" and i + 1 < len(sys.argv):
            dir_path = sys.argv[i + 1]
        elif arg == "--dbfilename" and i + 1 < len(sys.argv):
            dbfilename = sys.argv[i + 1]

    data_store = {}
    read_key_val_from_db(dir_path, dbfilename, data_store)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", 6379))
    server.listen(5)
    print("Server started on 0.0.0.0:6379")

    while True:
        conn, addr = server.accept()
        threading.Thread(
            target=handle_client, args=(conn, addr, data_store, dir_path, dbfilename)
        ).start()


if __name__ == "__main__":
    main()