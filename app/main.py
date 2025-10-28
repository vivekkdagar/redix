# app/main.py
# Complete single-file Redis clone implementing:
# PING, ECHO, SET (with EX/PX), GET, RPUSH, LPUSH, LLEN, LRANGE, LPOP (count),
# BLPOP (blocking with timeout), basic RESP parsing/encoding.
#
# Uses threads for concurrency and condition variables to wake BLPOP waiters.

import socket
import threading
import time
from collections import deque, defaultdict
from datetime import datetime, timedelta

HOST = "0.0.0.0"
PORT = 6379

# -------------------------
# In-memory storage
# -------------------------
_store_lock = threading.Lock()
_strings = {}          # key -> SetEntry or raw string
_lists = defaultdict(list)  # key -> list of str
_ttls = {}             # key -> expiry timestamp (seconds since epoch)
# waiters: key -> deque of (Condition, container_dict)
_waiters = defaultdict(deque)

# -------------------------
# Helpers: expiry
# -------------------------
def _now_ts():
    return time.time()

def _is_expired_key(key):
    exp = _ttls.get(key)
    if exp is None:
        return False
    return _now_ts() >= exp

def _clean_expired_key(key):
    if key in _ttls and _now_ts() >= _ttls[key]:
        _ttls.pop(key, None)
        _strings.pop(key, None)
        _lists.pop(key, None)

# -------------------------
# RESP encoding helpers
# -------------------------
def encode_simple(s: str) -> bytes:
    return f"+{s}\r\n".encode()

def encode_error(s: str) -> bytes:
    return f"-{s}\r\n".encode()

def encode_integer(n: int) -> bytes:
    return f":{n}\r\n".encode()

def encode_bulk(s: str | None) -> bytes:
    if s is None:
        return b"$-1\r\n"
    b = s.encode()
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"

def encode_array(items: list[str]) -> bytes:
    out = b"*" + str(len(items)).encode() + b"\r\n"
    for it in items:
        out += encode_bulk(it)
    return out

def encode_null_array() -> bytes:
    return b"*-1\r\n"

# -------------------------
# RESP parser: expects an Array of Bulk Strings
# returns list[str] on success, None on incomplete/invalid
# -------------------------
def parse_resp_array(buf: bytes):
    # Returns (args_list, remaining_bytes) or (None, buf) if incomplete/invalid
    if not buf:
        return None, buf
    if buf[0:1] != b"*":
        return None, buf
    try:
        idx = buf.index(b"\r\n")
    except ValueError:
        return None, buf
    try:
        count = int(buf[1:idx].decode())
    except Exception:
        return None, buf
    pos = idx + 2
    args = []
    for _ in range(count):
        if pos >= len(buf):
            return None, buf
        if buf[pos:pos+1] != b"$":
            return None, buf
        try:
            len_end = buf.index(b"\r\n", pos)
        except ValueError:
            return None, buf
        blen = int(buf[pos+1:len_end].decode())
        start = len_end + 2
        end = start + blen
        if end + 2 > len(buf):
            return None, buf
        arg = buf[start:end].decode()
        if buf[end:end+2] != b"\r\n":
            return None, buf
        args.append(arg)
        pos = end + 2
    remaining = buf[pos:]
    return args, remaining

# -------------------------
# Command implementations
# -------------------------
class SetEntry:
    def __init__(self, value: str, expiry_ms: int | None = None):
        self.value = value
        self.expiry_ts = None
        if expiry_ms is not None:
            self.expiry_ts = _now_ts() + (expiry_ms / 1000.0)

    def get(self):
        if self.expiry_ts is not None and _now_ts() >= self.expiry_ts:
            return None
        return self.value

def cmd_ping(args):
    return encode_simple("PONG")

def cmd_echo(args):
    if len(args) >= 2:
        return encode_bulk(args[1])
    return encode_bulk(None)

def cmd_set(args):
    # args: [key, value, ... optional PX/EX ...]
    if len(args) < 3:
        return encode_error("ERR wrong number of arguments for 'set' command")
    key = args[1]
    val = args[2]
    expiry_ms = None
    # parse optional args (supports EX, PX). Case-insensitive.
    i = 3
    while i + 1 < len(args):
        opt = args[i].upper()
        if opt == "PX":
            try:
                expiry_ms = int(float(args[i+1]))
            except Exception:
                expiry_ms = None
            i += 2
        elif opt == "EX":
            try:
                expiry_ms = int(float(args[i+1])) * 1000
            except Exception:
                expiry_ms = None
            i += 2
        else:
            i += 1
    with _store_lock:
        _strings[key] = SetEntry(val, expiry_ms)
        if expiry_ms is not None:
            _ttls[key] = _now_ts() + expiry_ms/1000.0
        else:
            _ttls.pop(key, None)
    return encode_simple("OK")

def cmd_get(args):
    if len(args) < 2:
        return encode_error("ERR wrong number of arguments for 'get' command")
    key = args[1]
    with _store_lock:
        # cleanup expiry if necessary
        if key in _ttls and _now_ts() >= _ttls[key]:
            _ttls.pop(key, None)
            _strings.pop(key, None)
        ent = _strings.get(key)
        if ent is None:
            return encode_bulk(None)
        if isinstance(ent, SetEntry):
            v = ent.get()
            if v is None:
                _strings.pop(key, None)
                _ttls.pop(key, None)
                return encode_bulk(None)
            return encode_bulk(v)
        # fallback if raw stored
        return encode_bulk(str(ent))

def cmd_rpush(args):
    # RPUSH key val [val ...]
    if len(args) < 3:
        return encode_error("ERR wrong number of arguments for 'rpush' command")
    key = args[1]
    vals = args[2:]
    pushed = 0
    delivered_to_waiters = 0
    with _store_lock:
        # First, satisfy waiters (FIFO) with incoming values
        while vals and _waiters.get(key):
            waiter_cond, result_slot = _waiters[key].popleft()
            val = vals.pop(0)
            # deliver value to waiter
            with waiter_cond:
                result_slot["pair"] = (key, val)
                waiter_cond.notify()
            delivered_to_waiters += 1
        # Any remaining values go to the list
        if vals:
            _lists[key].extend(vals)
        pushed = len(_lists[key])
    # return list length after operation
    return encode_integer(pushed)

def cmd_lpush(args):
    if len(args) < 3:
        return encode_error("ERR wrong number of arguments for 'lpush' command")
    key = args[1]
    vals = args[2:]
    with _store_lock:
        lst = _lists.setdefault(key, [])
        # LPUSH semantics: leftmost becomes last in args -> push left in order
        for v in vals:
            lst.insert(0, v)
        length = len(lst)
    return encode_integer(length)

def cmd_llen(args):
    if len(args) < 2:
        return encode_error("ERR wrong number of arguments for 'llen' command")
    key = args[1]
    with _store_lock:
        length = len(_lists.get(key, []))
    return encode_integer(length)

def cmd_lrange(args):
    if len(args) < 4:
        return encode_error("ERR wrong number of arguments for 'lrange' command")
    key = args[1]
    start = int(float(args[2]))
    stop = int(float(args[3]))
    with _store_lock:
        lst = list(_lists.get(key, []))
    n = len(lst)
    # handle negative indices
    if start < 0:
        start = n + start
    if stop < 0:
        stop = n + stop
    if start < 0:
        start = 0
    if stop < 0:
        # e.g., stop < 0 and too negative -> empty
        return b"*0\r\n"
    if start >= n or start > stop:
        return b"*0\r\n"
    stop = min(stop, n - 1)
    slice_list = lst[start:stop + 1]
    return encode_array(slice_list)

def cmd_lpop(args):
    # LPOP key [count]
    if len(args) < 2:
        return encode_error("ERR wrong number of arguments for 'lpop' command")
    key = args[1]
    count = 1
    if len(args) >= 3:
        try:
            count = int(float(args[2]))
        except Exception:
            return encode_error("ERR value is not an integer or out of range")
    with _store_lock:
        lst = _lists.get(key, [])
        if not lst:
            return encode_bulk(None)
        if count == 1:
            val = lst.pop(0)
            return encode_bulk(val)
        popped = []
        for _ in range(min(count, len(lst))):
            popped.append(lst.pop(0))
    # return array of popped
    return encode_array(popped)

def cmd_blpop(args):
    # BLPOP key [key ...] timeout
    if len(args) < 3:
        return encode_error("ERR wrong number of arguments for 'blpop' command")
    *keys, timeout_token = args[1:]
    # keys are strings, timeout may be float or int
    try:
        timeout = float(timeout_token)
    except Exception:
        return encode_error("ERR timeout is not a number")
    # Immediate check for available elements
    end_time = None if timeout == 0 else (_now_ts() + timeout)
    # We'll poll quickly but use Condition to wait efficiently if no item exists.
    # Approach: for this blocking call, create a Condition and append to _waiters for each key if not found.
    # But to support multiple keys, we poll first then register waiter on each key sequentially in small loop.
    # Simple approach: try immediate pop for each key first.
    with _store_lock:
        for k in keys:
            lst = _lists.get(k, [])
            if lst:
                val = lst.pop(0)
                return encode_array([k, val])
    # Not available immediately; if timeout==0 => block indefinitely until any key receives a value.
    # We'll create a per-call Condition and register it with each key (so RPUSH can notify one waiter per key).
    cond = threading.Condition()
    result = {"pair": None}
    # Register this waiter to all keys (the RPUSH implementation will notify only on the specific key)
    with _store_lock:
        for k in keys:
            _waiters[k].append((cond, result))
    # Now wait until notified or timeout
    remaining = None if timeout == 0 else (end_time - _now_ts())
    got = False
    try:
        with cond:
            while True:
                if result["pair"] is not None:
                    got = True
                    break
                if timeout == 0:
                    cond.wait(timeout=0.1)
                else:
                    if remaining <= 0:
                        break
                    start_wait = _now_ts()
                    cond.wait(timeout=min(0.1, remaining))
                    remaining -= (_now_ts() - start_wait)
    finally:
        # cleanup: remove this waiter from any waiters deque where still present
        with _store_lock:
            for k in keys:
                # remove matching (cond,result) tuples if still present
                if not _waiters.get(k):
                    continue
                newdq = deque()
                while _waiters[k]:
                    it = _waiters[k].popleft()
                    if it[0] is cond and it[1] is result:
                        continue
                    newdq.append(it)
                _waiters[k] = newdq
    if not got or result.get("pair") is None:
        return encode_null_array()
    k, v = result["pair"]
    return encode_array([k, v])

# -------------------------
# Dispatcher
# -------------------------
def dispatch_command(args):
    if not args:
        return encode_error("ERR empty request")
    cmd = args[0].upper()
    if cmd == "PING":
        return cmd_ping(args)
    if cmd == "ECHO":
        return cmd_echo(args)
    if cmd == "SET":
        return cmd_set(args)
    if cmd == "GET":
        return cmd_get(args)
    if cmd == "RPUSH":
        return cmd_rpush(args)
    if cmd == "LPUSH":
        return cmd_lpush(args)
    if cmd == "LLEN":
        return cmd_llen(args)
    if cmd == "LRANGE":
        return cmd_lrange(args)
    if cmd == "LPOP":
        return cmd_lpop(args)
    if cmd == "BLPOP":
        return cmd_blpop(args)
    return encode_error("ERR unknown command")

# -------------------------
# Networking: per-client handler
# -------------------------
def client_thread(conn, addr):
    # simple buffer to accumulate bytes
    buf = b""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data
            # try parse as many RESP arrays as available
            while True:
                parsed, remaining = parse_resp_array(buf)
                if parsed is None:
                    break
                buf = remaining
                try:
                    resp = dispatch_command(parsed)
                except Exception as e:
                    resp = encode_error(f"ERR {e}")
                # send bytes
                try:
                    conn.sendall(resp)
                except BrokenPipeError:
                    break
    finally:
        try:
            conn.close()
        except Exception:
            pass

# -------------------------
# Server entrypoint
# -------------------------
def main():
    print("Logs from your program will appear here!")
    server = socket.create_server((HOST, PORT), reuse_port=True)
    server.listen()
    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=client_thread, args=(conn, addr), daemon=True)
        t.start()

if __name__ == "__main__":
    main()