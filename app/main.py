import socket
import threading
import time
from datetime import datetime, timedelta
from typing import Any, List
from collections import defaultdict

# ============================
# RESP DATA TYPES
# ============================

class RESPType:
    def encode(self) -> str: ...
    def decode(self) -> Any: ...
    @classmethod
    def parse(cls, content: bytes): ...

class SimpleString(RESPType):
    def __init__(self, string): self.data = string
    def encode(self): return f"+{self.data}\r\n"
    def decode(self): return self.data
    @classmethod
    def parse(cls, content: bytes):
        p = 1
        data = bytearray()
        while content[p] != ord('\r'):
            data.append(content[p]); p += 1
        p += 2
        return cls(data.decode()), content[p:]

class SimpleError(RESPType):
    def __init__(self, string): self.data = string
    def encode(self): return f"-{self.data}\r\n"
    def decode(self): return self.data
    @classmethod
    def parse(cls, content: bytes):
        p = 1
        data = bytearray()
        while content[p] != ord('\r'):
            data.append(content[p]); p += 1
        p += 2
        return cls(data.decode()), content[p:]

class Integer(RESPType):
    def __init__(self, num): self.data = num
    def encode(self): return f":{self.data}\r\n"
    def decode(self): return self.data
    @classmethod
    def parse(cls, content: bytes):
        p = 1
        data = bytearray()
        while content[p] != ord('\r'):
            data.append(content[p]); p += 1
        p += 2
        return cls(int(data.decode())), content[p:]

class BulkString(RESPType):
    def __init__(self, string, length=None):
        self.data = string
        self.length = len(string) if length is None else length
    def encode(self):
        if self.length == -1: return "$-1\r\n"
        return f"${self.length}\r\n{self.data}\r\n"
    def decode(self): return self.data
    @classmethod
    def parse(cls, content: bytes):
        p = 1
        length = bytearray()
        while content[p] != ord('\r'):
            length.append(content[p]); p += 1
        p += 2
        length = int(length.decode())
        if length == -1: return cls("", -1), content[p:]
        data = bytearray()
        while content[p] != ord('\r'):
            data.append(content[p]); p += 1
        p += 2
        return cls(data.decode(), length), content[p:]

class Array(RESPType):
    def __init__(self, arr: List[RESPType]):
        self.data = arr
        self.length = len(arr)
    def encode(self):
        out = f"*{self.length}\r\n"
        for item in self.data: out += item.encode()
        return out
    def decode(self): return [x.decode() for x in self.data]
    def __getitem__(self, i): return self.data[i]
    @classmethod
    def parse(cls, content: bytes):
        p = 1
        length = bytearray()
        while content[p] != ord('\r'):
            length.append(content[p]); p += 1
        p += 2
        length = int(length.decode())
        arr = []
        rest = content[p:]
        for _ in range(length):
            t = rest[:1].decode()
            if t == '+': x, rest = SimpleString.parse(rest)
            elif t == '-': x, rest = SimpleError.parse(rest)
            elif t == ':': x, rest = Integer.parse(rest)
            elif t == '$': x, rest = BulkString.parse(rest)
            elif t == '*': x, rest = Array.parse(rest)
            arr.append(x)
        return cls(arr), rest


# ============================
# REDIS COMMANDS
# ============================

_LISTS = defaultdict(list)
_DATA = {}
_BLOCKED = defaultdict(list)  # key -> list of waiting clients

class SetEntry:
    def __init__(self, val, expiry=None):
        self._val = val
        self.expire = datetime.now() + timedelta(milliseconds=int(expiry)) if expiry else None
    @property
    def value(self):
        if self.expire and datetime.now() > self.expire: return None
        return self._val


def cmd_ping(args): return SimpleString("PONG")
def cmd_echo(args): return BulkString(args[0].decode())

def cmd_set(args):
    key, val = args[0].decode(), args[1].decode()
    px = None
    if len(args) > 2:
        opts = [a.decode().lower() for a in args[2:]]
        if "px" in opts:
            px_index = opts.index("px")
            px = args[px_index+3].decode() if len(args) > px_index+3 else args[px_index+1].decode()
    _DATA[key] = SetEntry(val, px)
    return SimpleString("OK")

def cmd_get(args):
    key = args[0].decode()
    entry = _DATA.get(key)
    if not entry or entry.value is None:
        return BulkString("", -1)
    return BulkString(entry.value)

def cmd_rpush(args, client=None):
    key = args[0].decode()
    values = [v.decode() for v in args[1:]]
    # If blocked clients exist, wake one
    if key in _BLOCKED and len(_BLOCKED[key]) > 0:
        waiting_client = _BLOCKED[key].pop(0)
        value = values[0]
        resp = Array([BulkString(key), BulkString(value)])
        waiting_client.sendall(resp.encode().encode())
        return Integer(1)
    _LISTS[key].extend(values)
    return Integer(len(_LISTS[key]))

def cmd_lpush(args):
    key = args[0].decode()
    values = [v.decode() for v in args[1:]]
    for v in values:
        _LISTS[key].insert(0, v)
    return Integer(len(_LISTS[key]))

def cmd_llen(args):
    key = args[0].decode()
    return Integer(len(_LISTS[key]))

def cmd_lpop(args):
    key = args[0].decode()
    n = 1
    if len(args) > 1: n = int(args[1].decode())
    if key not in _LISTS or len(_LISTS[key]) == 0:
        return BulkString("", -1)
    vals = []
    for _ in range(min(n, len(_LISTS[key]))):
        vals.append(BulkString(_LISTS[key].pop(0)))
    if n == 1: return vals[0]
    return Array(vals)

def cmd_lrange(args):
    key = args[0].decode()
    start, end = int(args[1].decode()), int(args[2].decode())
    lst = _LISTS.get(key, [])
    start = len(lst) + start if start < 0 else start
    end = len(lst) + end if end < 0 else end
    end = min(end+1, len(lst))
    return Array([BulkString(v) for v in lst[start:end]])

def cmd_blpop(args, client=None):
    key = args[0].decode()
    timeout = int(args[1].decode())
    while True:
        if len(_LISTS[key]) > 0:
            value = _LISTS[key].pop(0)
            return Array([BulkString(key), BulkString(value)])
        # Block indefinitely (timeout = 0)
        _BLOCKED[key].append(client)
        time.sleep(0.1)  # Yield control


COMMANDS = {
    "ping": cmd_ping,
    "echo": cmd_echo,
    "set": cmd_set,
    "get": cmd_get,
    "rpush": cmd_rpush,
    "lpush": cmd_lpush,
    "llen": cmd_llen,
    "lpop": cmd_lpop,
    "lrange": cmd_lrange,
    "blpop": cmd_blpop
}


# ============================
# SERVER
# ============================

def handle_client(con, addr):
    try:
        while True:
            data = con.recv(1024)
            if not data: break
            arr, _ = Array.parse(data)
            cmd = arr[0].decode().lower()
            args = arr[1:]
            if cmd in ["rpush", "blpop"]:
                res = COMMANDS[cmd](args, con)
                if res is None: continue  # handled by another client
            else:
                res = COMMANDS.get(cmd, lambda _: SimpleError("ERR unknown command"))(args)
            con.sendall(res.encode().encode())
    except Exception as e:
        print("Client error:", e)
    finally:
        con.close()

def main():
    HOST, PORT = "localhost", 6379
    print(f"Listening on {HOST}:{PORT}")
    s = socket.create_server((HOST, PORT), reuse_port=True)
    while True:
        con, addr = s.accept()
        threading.Thread(target=handle_client, args=(con, addr), daemon=True).start()

if __name__ == "__main__":
    main()