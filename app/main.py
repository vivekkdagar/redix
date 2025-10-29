#!/usr/bin/env python3
import socket
import threading
import os
import time
import sys
import struct

# ---------------- In-memory storage ----------------
DATA = {}  # key -> (value_or_list, expiry)
CONFIG = {"dir": ".", "dbfilename": "dump.rdb"}

# BLPOP condition variable
STORE_COND = threading.Condition()

# Pub/Sub structures
CHANNEL_SUBSCRIBERS = {}  # channel -> set(connections)
CLIENT_SUBSCRIPTIONS = {}  # conn -> set(channels)


# ---------------- RESP helpers ----------------
def encode_simple(s: str) -> bytes:
    return f"+{s}\r\n".encode()


def encode_bulk(obj) -> bytes:
    if obj is None:
        return b"$-1\r\n"
    if isinstance(obj, bytes):
        b = obj
    else:
        b = str(obj).encode("utf-8")
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"


def encode_integer(n: int) -> bytes:
    return f":{n}\r\n".encode()


def encode_array(arr) -> bytes:
    if arr is None:
        return b"*-1\r\n"
    parts = [f"*{len(arr)}\r\n".encode()]
    for it in arr:
        parts.append(encode_bulk(it))
    return b"".join(parts)


def parse_resp_array(data: bytes):
    if not data or not data.startswith(b"*"):
        return []
    ptr = 0
    nl = data.find(b"\r\n", ptr)
    arr_len = int(data[ptr + 1:nl])
    ptr = nl + 2
    out = []
    for _ in range(arr_len):
        if data[ptr:ptr + 1] != b"$":
            return out
        nl = data.find(b"\r\n", ptr)
        blen = int(data[ptr + 1:nl])
        ptr = nl + 2
        if blen == -1:
            out.append(None)
        else:
            val = data[ptr:ptr + blen].decode(errors="ignore")
            out.append(val)
            ptr += blen + 2
    return out


# ---------------- RDB parsing (simplified) ----------------
def _read_length(f):
    b = f.read(1)
    if not b:
        return None
    fb = b[0]
    prefix = (fb & 0xC0) >> 6
    if prefix == 0:
        return fb & 0x3F
    elif prefix == 1:
        b2 = f.read(1)
        return ((fb & 0x3F) << 8) | b2[0]
    elif prefix == 2:
        b4 = f.read(4)
        return int.from_bytes(b4, byteorder="little")
    else:
        return fb & 0x3F


def _read_string(f):
    length = _read_length(f)
    if length is None:
        return None
    if length == 0:
        return ""
    raw = f.read(length)
    return raw.decode("utf-8", errors="ignore")


def read_rdb_with_expiry(dir_path, dbfile):
    path = os.path.join(dir_path, dbfile)
    if not os.path.exists(path):
        return
    try:
        with open(path, "rb") as f:
            header = f.read(9)
            if not header.startswith(b"REDIS"):
                return
            pending_expiry = None
            while True:
                op = f.read(1)
                if not op:
                    break
                if op == b'\xfc':  # expire in ms
                    b8 = f.read(8)
                    expiry_ms = int.from_bytes(b8, "little")
                    pending_expiry = expiry_ms / 1000.0
                    continue
                if op == b'\xfd':  # expire in sec
                    b4 = f.read(4)
                    expiry_s = int.from_bytes(b4, "little")
                    pending_expiry = float(expiry_s)
                    continue
                if op == b'\xfe':  # SELECTDB
                    _ = _read_length(f)
                    pending_expiry = None
                    continue
                if op == b'\xfb':  # RESIZEDB
                    _ = _read_length(f)
                    _ = _read_length(f)
                    pending_expiry = None
                    continue
                if op == b'\xff':
                    break
                if op == b'\x00':  # string
                    key = _read_string(f)
                    val = _read_string(f)
                    if key:
                        exp = pending_expiry if pending_expiry is not None else -1
                        DATA[key] = (val, exp)
                    pending_expiry = None
                    continue
                pending_expiry = None
    except Exception:
        return


# ---------------- Command handling ----------------
def handle_command(cmd, conn):
    if not cmd:
        return b""

    op = cmd[0].upper()

    # PING
    if op == "PING":
        if len(cmd) == 1:
            return encode_simple("PONG")
        return encode_bulk(cmd[1])

    # ECHO
    if op == "ECHO":
        return encode_bulk(cmd[1] if len(cmd) > 1 else "")

    # SET / GET
    if op == "SET":
        key, val = cmd[1], cmd[2]
        expiry = -1
        if len(cmd) >= 5 and cmd[3].upper() == "PX":
            expiry = time.time() + int(cmd[4]) / 1000.0
        DATA[key] = (val, expiry)
        return encode_simple("OK")

    if op == "GET":
        key = cmd[1]
        v = DATA.get(key)
        if not v:
            return encode_bulk(None)
        val, exp = v
        if exp != -1 and time.time() > exp:
            del DATA[key]
            return encode_bulk(None)
        if isinstance(val, list):
            return encode_bulk(None)
        return encode_bulk(val)

    # LPUSH / RPUSH / LPOP / BLPOP
    if op == "LPUSH":
        key, *elements = cmd[1:]
        with STORE_COND:
            if key not in DATA:
                DATA[key] = ([], -1)
            lst, exp = DATA[key]
            for e in elements:
                lst.insert(0, e)
            STORE_COND.notify_all()
            return encode_integer(len(lst))

    if op == "RPUSH":
        key, *elements = cmd[1:]
        with STORE_COND:
            if key not in DATA:
                DATA[key] = ([], -1)
            lst, exp = DATA[key]
            for e in elements:
                lst.append(e)
            STORE_COND.notify_all()
            return encode_integer(len(lst))

    if op == "LPOP":
        key = cmd[1]
        v = DATA.get(key)
        if not v:
            return encode_bulk(None)
        lst, exp = v
        if not lst:
            del DATA[key]
            return encode_bulk(None)
        val = lst.pop(0)
        if not lst:
            del DATA[key]
        return encode_bulk(val)

    if op == "BLPOP":
        keys = cmd[1:-1]
        timeout = float(cmd[-1])
        end_time = time.time() + timeout if timeout > 0 else None
        with STORE_COND:
            while True:
                for k in keys:
                    v = DATA.get(k)
                    if v and isinstance(v[0], list) and len(v[0]) > 0:
                        lst, exp = v
                        val = lst.pop(0)
                        if not lst:
                            del DATA[k]
                        return encode_array([k, val])
                if timeout == 0:
                    STORE_COND.wait()
                    continue
                remaining = end_time - time.time()
                if remaining <= 0:
                    return b"*-1\r\n"
                STORE_COND.wait(timeout=remaining)

    if op == "LRANGE":
        key, start, stop = cmd[1], int(cmd[2]), int(cmd[3])
        if key not in DATA:
            return encode_array([])
        val, _ = DATA[key]
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

    if op == "LLEN":
        key = cmd[1]
        if key not in DATA:
            return encode_integer(0)
        val, _ = DATA[key]
        if not isinstance(val, list):
            return encode_integer(0)
        return encode_integer(len(val))

    # CONFIG / KEYS
    if op == "CONFIG" and len(cmd) >= 3 and cmd[1].upper() == "GET":
        key = cmd[2]
        if key in CONFIG:
            return encode_array([key, CONFIG[key]])
        return encode_array([])

    if op == "KEYS":
        pattern = cmd[1]
        if pattern == "*":
            now = time.time()
            keys_out = []
            for k, (v, exp) in list(DATA.items()):
                if exp != -1 and now > exp:
                    del DATA[k]
                else:
                    keys_out.append(k)
            return encode_array(keys_out)
        return encode_array([])

    # ---------------- SUBSCRIBE ----------------
    if op == "SUBSCRIBE":
        if len(cmd) < 2:
            return b"-ERR wrong number of arguments for 'subscribe'\r\n"

        if conn not in CLIENT_SUBSCRIPTIONS:
            CLIENT_SUBSCRIPTIONS[conn] = set()

        for channel in cmd[1:]:
            if channel not in CHANNEL_SUBSCRIBERS:
                CHANNEL_SUBSCRIBERS[channel] = set()
            CHANNEL_SUBSCRIBERS[channel].add(conn)
            CLIENT_SUBSCRIPTIONS[conn].add(channel)

            count = len(CLIENT_SUBSCRIPTIONS[conn])
            msg = b"".join([
                b"*3\r\n",
                b"$9\r\nsubscribe\r\n",
                f"${len(channel)}\r\n{channel}\r\n".encode(),
                f":{count}\r\n".encode()
            ])
            conn.sendall(msg)

        # Enter subscribed mode (block)
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                # (future stages handle UNSUBSCRIBE/PING here)
        except Exception:
            pass
        finally:
            if conn in CLIENT_SUBSCRIPTIONS:
                for ch in CLIENT_SUBSCRIPTIONS[conn]:
                    CHANNEL_SUBSCRIBERS.get(ch, set()).discard(conn)
                del CLIENT_SUBSCRIPTIONS[conn]
        return b""

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
            resp = handle_command(cmd, conn)
            if resp:
                conn.sendall(resp)
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    args = sys.argv[1:]
    if "--dir" in args:
        CONFIG["dir"] = args[args.index("--dir") + 1]
    if "--dbfilename" in args:
        CONFIG["dbfilename"] = args[args.index("--dbfilename") + 1]

    read_rdb_with_expiry(CONFIG["dir"], CONFIG["dbfilename"])

    print("Server started on 0.0.0.0:6379")
    server = socket.create_server(("0.0.0.0", 6379), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()