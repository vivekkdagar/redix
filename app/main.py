#!/usr/bin/env python3
import socket
import threading
import os
import time
import sys
import struct
import datetime

# ---------------- In-memory storage ----------------
data_store = {}  # key -> (value_or_list, expiry)
config_store = {
    "dir": ".",
    "dbfilename": "dump.rdb"
}

# Condition used to wake BLPOP waiters when RPUSH/LPUSH modifies lists
store_condition = threading.Condition()


# ---------------- RESP Encoding ----------------
def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()


def encode_bulk_string(s) -> bytes:
    if s is None:
        return b"$-1\r\n"
    if isinstance(s, bytes):
        b = s
    else:
        b = str(s).encode("utf-8")
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"


def encode_integer(num: int) -> bytes:
    return f":{num}\r\n".encode()


def encode_array(arr) -> bytes:
    if arr is None:
        return b"*-1\r\n"
    parts = [f"*{len(arr)}\r\n".encode()]
    for it in arr:
        parts.append(encode_bulk_string(it))
    return b"".join(parts)


# ---------------- RESP Parsing ----------------
def parse_resp_array(data: bytes):
    if not data or not data.startswith(b"*"):
        return []
    ptr = 0
    nl = data.find(b"\r\n", ptr)
    arr_len = int(data[ptr+1:nl])
    ptr = nl + 2
    out = []
    for _ in range(arr_len):
        nl = data.find(b"\r\n", ptr)
        blen = int(data[ptr+1:nl])
        ptr = nl + 2
        if blen == -1:
            out.append(None)
        else:
            val = data[ptr:ptr+blen].decode(errors="ignore")
            out.append(val)
            ptr += blen + 2
    return out


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
    path = os.path.join(dir_path, dbfile)
    if not os.path.exists(path):
        return
    try:
        with open(path, "rb") as f:
            header = f.read(9)
            if not header.startswith(b"REDIS"):
                return
            while True:
                b = f.read(1)
                if not b:
                    break
                if b == b'\x00':  # string object
                    key = _read_string(f)
                    val = _read_string(f)
                    if key:
                        data_store[key] = (val, -1)
                elif b == b'\xfe':
                    _ = _read_length(f)
                elif b == b'\xfb':
                    _ = _read_length(f)
                    _ = _read_length(f)
                elif b in [b'\xfc', b'\xfd']:
                    f.read(8 if b == b'\xfc' else 4)
                elif b == b'\xff':
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
        if len(cmd) < 3:
            return b"-ERR wrong number of arguments for 'set'\r\n"
        key, val = cmd[1], cmd[2]
        expiry = -1
        if len(cmd) >= 5 and cmd[3].upper() == "PX":
            expiry = time.time() + int(cmd[4]) / 1000.0
        data_store[key] = (val, expiry)
        return encode_simple_string("OK")

    elif op == "GET":
        key = cmd[1]
        v = data_store.get(key)
        if not v:
            return encode_bulk_string(None)
        val, exp = v
        if exp != -1 and time.time() > exp:
            del data_store[key]
            return encode_bulk_string(None)
        if isinstance(val, list):
            return encode_bulk_string(None)
        return encode_bulk_string(val)

    elif op == "LPUSH":
        key, *elements = cmd[1:]
        with store_condition:
            if key not in data_store:
                data_store[key] = ([], -1)
            lst, expiry = data_store[key]
            for e in elements:
                lst.insert(0, e)
            store_condition.notify_all()
            return encode_integer(len(lst))

    elif op == "RPUSH":
        key, *elements = cmd[1:]
        with store_condition:
            if key not in data_store:
                data_store[key] = ([], -1)
            lst, expiry = data_store[key]
            for e in elements:
                lst.append(e)
            store_condition.notify_all()
            return encode_integer(len(lst))

    elif op == "BLPOP":
        keys = cmd[1:-1]
        timeout = float(cmd[-1])
        end_time = time.time() + timeout if timeout > 0 else None
        with store_condition:
            while True:
                for k in keys:
                    v = data_store.get(k)
                    if v:
                        lst, expiry = v
                        if isinstance(lst, list) and lst:
                            val = lst.pop(0)
                            if not lst:
                                del data_store[k]
                            return encode_array([k, val])
                if timeout == 0:
                    store_condition.wait()
                    continue
                remaining = end_time - time.time()
                if remaining <= 0:
                    return b"*-1\r\n"
                store_condition.wait(timeout=remaining)

    elif op == "LRANGE":
        if len(cmd) != 4:
            return b"-ERR wrong number of arguments for 'lrange'\r\n"
        key = cmd[1]
        start, stop = int(cmd[2]), int(cmd[3])
        if key not in data_store:
            return encode_array([])
        val, _ = data_store[key]
        if not isinstance(val, list):
            return encode_array([])
        n = len(val)
        if start < 0:
            start = n + start
        if stop < 0:
            stop = n + stop
        start = max(start, 0)
        stop = min(stop, n - 1)
        if start > stop:
            return encode_array([])
        return encode_array(val[start:stop + 1])

    elif op == "LLEN":
        key = cmd[1]
        if key not in data_store:
            return encode_integer(0)
        val, _ = data_store[key]
        if not isinstance(val, list):
            return encode_integer(0)
        return encode_integer(len(val))

    elif op == "CONFIG" and len(cmd) >= 3 and cmd[1].upper() == "GET":
        key = cmd[2]
        if key in config_store:
            return encode_array([key, config_store[key]])
        return encode_array([])

    elif op == "KEYS":
        pattern = cmd[1]
        if pattern == "*":
            keys_out = [k for k, (v, exp) in list(data_store.items())
                        if exp == -1 or time.time() <= exp]
            return encode_array(keys_out)
        return encode_array([])

    else:
        return b"-ERR unknown command\r\n"


# ---------------- Networking ----------------
def handle_client(conn):
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            cmd = parse_resp_array(data)
            if not cmd:
                continue
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
    print("Server started on 0.0.0.0:6379")
    server = socket.create_server(("0.0.0.0", 6379), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()