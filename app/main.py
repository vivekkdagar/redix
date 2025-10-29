#!/usr/bin/env python3
# main.py — Redis clone with RDB expiries, lists & basic pub/sub
import socket
import threading
import os
import time
import sys
import struct

# ---------------- In-memory store ----------------
DATA = {}          # key -> (value_or_list, expiry_timestamp or -1)
CONFIG = {"dir": ".", "dbfilename": "dump.rdb"}
CHANNELS = {}      # channel -> list of subscribed connections
STORE_COND = threading.Condition()


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
    out = [f"*{len(arr)}\r\n".encode()]
    for x in arr:
        out.append(encode_bulk(x))
    return b"".join(out)


def parse_resp_array(data: bytes):
    if not data.startswith(b"*"):
        return []
    ptr = 0
    nl = data.find(b"\r\n", ptr)
    if nl == -1:
        return []
    try:
        n = int(data[1:nl])
    except:
        return []
    ptr = nl + 2
    out = []
    for _ in range(n):
        if data[ptr:ptr+1] != b"$":
            return out
        nl = data.find(b"\r\n", ptr)
        if nl == -1:
            return out
        length = int(data[ptr+1:nl])
        ptr = nl + 2
        if length == -1:
            out.append(None)
        else:
            val = data[ptr:ptr+length].decode(errors="ignore")
            ptr += length + 2
            out.append(val)
    return out


# ---------------- RDB parser (with expiries) ----------------
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
        return int.from_bytes(b4, "little")
    else:
        return fb & 0x3F


def _read_string(f):
    length = _read_length(f)
    if length is None:
        return None
    if length == 0:
        return ""
    raw = f.read(length)
    if len(raw) < length:
        return None
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
                if op == b'\xfc':  # expiry in ms
                    b8 = f.read(8)
                    pending_expiry = int.from_bytes(b8, "little") / 1000.0
                    continue
                if op == b'\xfd':  # expiry in s
                    b4 = f.read(4)
                    pending_expiry = float(int.from_bytes(b4, "little"))
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
                if op in [b'\x00', b'\x01', b'\x02', b'\x03', b'\x04']:
                    key = _read_string(f)
                    val = _read_string(f)
                    if key:
                        exp = pending_expiry if pending_expiry else -1
                        DATA[key] = (val, exp)
                    pending_expiry = None
    except Exception:
        return


# ---------------- Command handlers ----------------
def _expired(key):
    v = DATA.get(key)
    if not v:
        return True
    val, exp = v
    if exp != -1 and time.time() > exp:
        del DATA[key]
        return True
    return False


def handle_command(cmd, conn=None):
    if not cmd:
        return b""
    op = cmd[0].upper()

    # --- PING ---
    if op == "PING":
        return encode_simple("PONG") if len(cmd) == 1 else encode_bulk(cmd[1])

    # --- ECHO ---
    if op == "ECHO":
        return encode_bulk(cmd[1] if len(cmd) > 1 else "")

    # --- SET key value [PX ms] ---
    if op == "SET":
        if len(cmd) < 3:
            return b"-ERR wrong number of arguments for 'set'\r\n"
        key, val = cmd[1], cmd[2]
        expiry = -1
        if len(cmd) >= 5 and cmd[3].upper() == "PX":
            try:
                expiry = time.time() + int(cmd[4]) / 1000.0
            except:
                pass
        DATA[key] = (val, expiry)
        return encode_simple("OK")

    # --- GET key ---
    if op == "GET":
        if len(cmd) < 2:
            return b"-ERR wrong number of arguments for 'get'\r\n"
        key = cmd[1]
        if _expired(key):
            return encode_bulk(None)
        val, _ = DATA[key]
        return encode_bulk(val if not isinstance(val, list) else None)

    # --- Lists: LPUSH / RPUSH / LPOP / BLPOP / LRANGE / LLEN ---
    if op in ("LPUSH", "RPUSH"):
        key, elems = cmd[1], cmd[2:]
        with STORE_COND:
            if key not in DATA or not isinstance(DATA[key][0], list):
                DATA[key] = ([], -1)
            lst, exp = DATA[key]
            if op == "LPUSH":
                for e in elems:
                    lst.insert(0, e)
            else:
                lst.extend(elems)
            STORE_COND.notify_all()
            return encode_integer(len(lst))

    if op == "LPOP":
        key = cmd[1]
        count = int(cmd[2]) if len(cmd) > 2 else None
        v = DATA.get(key)
        if not v or not isinstance(v[0], list):
            return encode_bulk(None) if count is None else encode_array([])
        lst, exp = v
        if not lst:
            return encode_bulk(None) if count is None else encode_array([])
        if count is None:
            val = lst.pop(0)
            if not lst:
                del DATA[key]
            return encode_bulk(val)
        popped = [lst.pop(0) for _ in range(min(count, len(lst)))]
        if not lst:
            del DATA[key]
        return encode_array(popped)

    if op == "BLPOP":
        keys, timeout = cmd[1:-1], float(cmd[-1])
        end_time = None if timeout == 0 else time.time() + timeout
        with STORE_COND:
            while True:
                for k in keys:
                    v = DATA.get(k)
                    if v and isinstance(v[0], list) and v[0]:
                        lst, _ = v
                        val = lst.pop(0)
                        if not lst:
                            del DATA[k]
                        return encode_array([k, val])
                if timeout == 0:
                    STORE_COND.wait()
                else:
                    remain = end_time - time.time()
                    if remain <= 0:
                        return b"*-1\r\n"
                    STORE_COND.wait(timeout=remain)

    if op == "LRANGE":
        key, start, stop = cmd[1], int(cmd[2]), int(cmd[3])
        v = DATA.get(key)
        if not v or not isinstance(v[0], list):
            return encode_array([])
        lst = v[0]
        n = len(lst)
        if start < 0:
            start = n + start
        if stop < 0:
            stop = n + stop
        start = max(start, 0)
        stop = min(stop, n - 1)
        return encode_array(lst[start:stop+1] if start <= stop else [])

    if op == "LLEN":
        key = cmd[1]
        v = DATA.get(key)
        return encode_integer(len(v[0]) if v and isinstance(v[0], list) else 0)

    # --- CONFIG GET ---
    if op == "CONFIG" and len(cmd) >= 3 and cmd[1].upper() == "GET":
        key = cmd[2]
        if key in CONFIG:
            return encode_array([key, CONFIG[key]])
        return encode_array([])

    # --- KEYS * ---
    if op == "KEYS":
        if len(cmd) != 2:
            return b"-ERR wrong number of arguments for 'keys'\r\n"
        pattern = cmd[1]
        if pattern == "*":
            now = time.time()
            keys = []
            for k, (v, exp) in list(DATA.items()):
                if exp != -1 and now > exp:
                    del DATA[k]
                    continue
                keys.append(k)
            return encode_array(keys)
        return encode_array([])

    # --- SUBSCRIBE channel ---
    if op == "SUBSCRIBE":
        if len(cmd) < 2:
            return b"-ERR wrong number of arguments for 'subscribe'\r\n"
        channel = cmd[1]
        with STORE_COND:
            if channel not in CHANNELS:
                CHANNELS[channel] = []
            CHANNELS[channel].append(conn)
        # Respond immediately per Redis spec
        resp = encode_array(["subscribe", channel, 1])
        conn.sendall(resp)
        # Keep the connection open (no further command reads)
        try:
            while True:
                time.sleep(1)
        except Exception:
            pass
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
        except:
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
    server.listen(64)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()