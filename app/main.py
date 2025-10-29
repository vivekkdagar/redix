#!/usr/bin/env python3
import socket
import threading
import os
import time
import sys

# ---------------- In-memory storage ----------------
DATA = {}  # key -> (value_or_list, expiry)
CONFIG = {"dir": ".", "dbfilename": "dump.rdb"}

STORE_COND = threading.Condition()

# Pub/Sub tracking
CLIENT_SUBSCRIPTIONS = {}   # conn -> set(channels)
CHANNEL_SUBSCRIBERS = {}    # channel -> set(conns)


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
    if nl == -1:
        return []
    try:
        arr_len = int(data[ptr + 1:nl])
    except Exception:
        return []
    ptr = nl + 2
    out = []
    for _ in range(arr_len):
        if data[ptr:ptr + 1] != b"$":
            return out
        nl = data.find(b"\r\n", ptr)
        if nl == -1:
            return out
        blen = int(data[ptr + 1:nl])
        ptr = nl + 2
        if blen == -1:
            out.append(None)
        else:
            val = data[ptr:ptr + blen]
            out.append(val.decode("utf-8", errors="ignore"))
            ptr += blen + 2
    return out


# ---------------- RDB Parsing (simplified) ----------------
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
                    expiry_ms = int.from_bytes(f.read(8), "little")
                    pending_expiry = expiry_ms / 1000.0
                elif op == b'\xfd':  # expiry in s
                    expiry_s = int.from_bytes(f.read(4), "little")
                    pending_expiry = float(expiry_s)
                elif op == b'\xfe':  # SELECTDB
                    _ = _read_length(f)
                    pending_expiry = None
                elif op == b'\xfb':  # RESIZEDB
                    _ = _read_length(f)
                    _ = _read_length(f)
                    pending_expiry = None
                elif op == b'\xff':  # EOF
                    break
                elif op == b'\x00':  # string object
                    key = _read_string(f)
                    val = _read_string(f)
                    if key is not None:
                        expiry_ts = pending_expiry if pending_expiry else -1
                        DATA[key] = (val, expiry_ts)
                    pending_expiry = None
    except Exception:
        return


# ---------------- Command Handling ----------------
def handle_command(cmd, conn):
    if not cmd:
        return b""
    op = cmd[0].upper()

    # --- PING ---
    if op == "PING":
        if len(cmd) == 1:
            return encode_simple("PONG")
        return encode_bulk(cmd[1])

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
            except Exception:
                expiry = -1
        DATA[key] = (val, expiry)
        return encode_simple("OK")

    # --- GET key ---
    if op == "GET":
        if len(cmd) < 2:
            return b"-ERR wrong number of arguments for 'get'\r\n"
        key = cmd[1]
        v = DATA.get(key)
        if v is None:
            return encode_bulk(None)
        val, exp = v
        if exp != -1 and time.time() > exp:
            del DATA[key]
            return encode_bulk(None)
        if isinstance(val, list):
            return encode_bulk(None)
        return encode_bulk(val)

    # --- LPUSH / RPUSH ---
    if op in ("LPUSH", "RPUSH"):
        if len(cmd) < 3:
            return b"-ERR wrong number of arguments\r\n"
        key, *elems = cmd[1:]
        with STORE_COND:
            if key not in DATA or not isinstance(DATA[key][0], list):
                DATA[key] = ([], -1)
            lst, expiry = DATA[key]
            if op == "LPUSH":
                for e in elems:
                    lst.insert(0, e)
            else:
                for e in elems:
                    lst.append(e)
            STORE_COND.notify_all()
            return encode_integer(len(lst))

    # --- LPOP ---
    if op == "LPOP":
        key = cmd[1]
        count = None
        if len(cmd) > 2:
            try:
                count = int(cmd[2])
            except Exception:
                return b"-ERR value is not an integer\r\n"
        v = DATA.get(key)
        if not v or not isinstance(v[0], list) or len(v[0]) == 0:
            return encode_bulk(None) if count is None else encode_array([])
        lst, expiry = v
        if count is None:
            val = lst.pop(0)
            if not lst:
                del DATA[key]
            return encode_bulk(val)
        popped = []
        for _ in range(min(count, len(lst))):
            popped.append(lst.pop(0))
        if not lst:
            del DATA[key]
        return encode_array(popped)

    # --- BLPOP ---
    if op == "BLPOP":
        keys = cmd[1:-1]
        timeout = float(cmd[-1])
        end_time = time.time() + timeout if timeout > 0 else None
        with STORE_COND:
            while True:
                for k in keys:
                    v = DATA.get(k)
                    if v and isinstance(v[0], list) and v[0]:
                        lst, expiry = v
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

    # --- LRANGE ---
    if op == "LRANGE":
        key = cmd[1]
        start, stop = int(cmd[2]), int(cmd[3])
        v = DATA.get(key)
        if not v or not isinstance(v[0], list):
            return encode_array([])
        lst, expiry = v
        n = len(lst)
        if start < 0:
            start = n + start
        if stop < 0:
            stop = n + stop
        start = max(start, 0)
        stop = min(stop, n - 1)
        if start > stop:
            return encode_array([])
        return encode_array(lst[start:stop + 1])

    # --- LLEN ---
    if op == "LLEN":
        key = cmd[1]
        v = DATA.get(key)
        if not v or not isinstance(v[0], list):
            return encode_integer(0)
        return encode_integer(len(v[0]))

    # --- CONFIG GET ---
    if op == "CONFIG" and len(cmd) >= 3 and cmd[1].upper() == "GET":
        key = cmd[2]
        if key in CONFIG:
            return encode_array([key, CONFIG[key]])
        return encode_array([])

    # --- KEYS ---
    if op == "KEYS":
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

    # --- SUBSCRIBE ---
    # --- SUBSCRIBE ---
    if op == "SUBSCRIBE":
        if len(cmd) < 2:
            return b"-ERR wrong number of arguments for 'subscribe'\r\n"

        # Initialize per-client subscription set
        if conn not in CLIENT_SUBSCRIPTIONS:
            CLIENT_SUBSCRIPTIONS[conn] = set()

        for channel in cmd[1:]:
            # Add channel to global and client maps
            if channel not in CHANNEL_SUBSCRIBERS:
                CHANNEL_SUBSCRIBERS[channel] = set()
            CHANNEL_SUBSCRIBERS[channel].add(conn)
            CLIENT_SUBSCRIPTIONS[conn].add(channel)

            count = len(CLIENT_SUBSCRIPTIONS[conn])

            # Send the subscription acknowledgment
            msg = b"".join([
                b"*3\r\n",
                b"$9\r\nsubscribe\r\n",
                f"${len(channel)}\r\n{channel}\r\n".encode(),
                f":{count}\r\n".encode()
            ])
            conn.sendall(msg)

        # Enter subscribed mode (client waits for messages)
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                # Redis ignores most commands while in subscribed mode
                # except PING/UNSUBSCRIBE in later stages
        except Exception:
            pass
        finally:
            # Cleanup on disconnect
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

    server = socket.create_server(("0.0.0.0", 6379), reuse_port=True)
    print("Server started on 0.0.0.0:6379")

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()