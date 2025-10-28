import socket
import threading
import time
from datetime import datetime, timedelta

# ---------------- RESP TYPES ---------------- #

class RESPType:
    def encode(self):
        raise NotImplementedError

class SimpleString(RESPType):
    def __init__(self, value):
        self.value = value

    def encode(self):
        return f"+{self.value}\r\n"

class SimpleError(RESPType):
    def __init__(self, value):
        self.value = value

    def encode(self):
        return f"-{self.value}\r\n"

class Integer(RESPType):
    def __init__(self, value):
        self.value = int(value)

    def encode(self):
        return f":{self.value}\r\n"

class BulkString(RESPType):
    def __init__(self, value, null=False):
        self.value = value
        self.null = null or (value is None)

    def encode(self):
        if self.null:
            return "$-1\r\n"
        return f"${len(self.value)}\r\n{self.value}\r\n"

class Array(RESPType):
    def __init__(self, items):
        self.items = items or []

    def encode(self):
        if self.items is None:
            return "*-1\r\n"
        out = f"*{len(self.items)}\r\n"
        for item in self.items:
            out += item.encode()
        return out


# ---------------- SERVER CORE ---------------- #

COMMANDS = {}

def command(name):
    def decorator(fn):
        COMMANDS[name.lower()] = fn
        return fn
    return decorator


# ---------------- DATA STORES ---------------- #

_DATA = {}   # for SET/GET
_LISTS = {}  # for list commands


# ---------------- STRING COMMANDS ---------------- #

class SetEntry:
    def __init__(self, value, expiry_ms=None):
        self.value = value
        if expiry_ms is None:
            self.expiry = None
        else:
            self.expiry = datetime.now() + timedelta(milliseconds=int(expiry_ms))

    def get(self):
        if self.expiry and datetime.now() > self.expiry:
            return None
        return self.value


@command("set")
def set_command(args):
    key = args[0].decode()
    value = args[1].decode()

    expiry_ms = None
    if len(args) > 2:
        for i in range(2, len(args) - 1):
            if args[i].decode().lower() == "px":
                expiry_ms = int(args[i + 1].decode())

    _DATA[key] = SetEntry(value, expiry_ms)
    return SimpleString("OK")


@command("get")
def get_command(args):
    key = args[0].decode()
    entry = _DATA.get(key)
    if not entry:
        return BulkString(None, null=True)
    val = entry.get()
    if val is None:
        return BulkString(None, null=True)
    return BulkString(val)


# ---------------- LIST COMMANDS ---------------- #

@command("rpush")
def rpush_command(args):
    key = args[0].decode()
    values = [a.decode() for a in args[1:]]
    lst = _LISTS.setdefault(key, [])
    lst.extend(values)
    return Integer(len(lst))


@command("lpush")
def lpush_command(args):
    key = args[0].decode()
    values = [a.decode() for a in args[1:]]
    lst = _LISTS.setdefault(key, [])
    for val in values:
        lst.insert(0, val)
    return Integer(len(lst))


@command("llen")
def llen_command(args):
    key = args[0].decode()
    lst = _LISTS.get(key, [])
    return Integer(len(lst))


@command("lpop")
def lpop_command(args):
    key = args[0].decode()
    count = 1
    if len(args) > 1:
        count = int(args[1].decode())

    lst = _LISTS.get(key)
    if not lst:
        return BulkString(None, null=True)

    count = min(count, len(lst))
    if count == 1:
        return BulkString(lst.pop(0))
    popped = [BulkString(lst.pop(0)) for _ in range(count)]
    return Array(popped)


@command("lrange")
def lrange_command(args):
    key = args[0].decode()
    start = int(args[1].decode())
    end = int(args[2].decode())

    lst = _LISTS.get(key, [])
    n = len(lst)
    if start < 0:
        start = n + start
    if end < 0:
        end = n + end
    end = min(end, n - 1)
    if start < 0:
        start = 0
    if start > end or not lst:
        return Array([])

    result = [BulkString(v) for v in lst[start:end + 1]]
    return Array(result)


@command("blpop")
def blpop_command(args):
    keys = [a.decode() for a in args[:-1]]
    timeout = int(args[-1].decode())

    start_time = time.time()
    while True:
        for key in keys:
            lst = _LISTS.get(key, [])
            if lst:
                val = lst.pop(0)
                return Array([BulkString(key), BulkString(val)])

        if timeout == 0:
            time.sleep(0.05)
            continue

        if (time.time() - start_time) >= timeout:
            return BulkString(None, null=True)

        time.sleep(0.05)


# ---------------- BASIC COMMANDS ---------------- #

@command("ping")
def ping_command(args):
    return SimpleString("PONG")


@command("echo")
def echo_command(args):
    return BulkString(args[0].decode())


# ---------------- EXECUTION ---------------- #

def execute(data):
    cmd_name = data[0].decode().lower()
    args = data[1:]
    fn = COMMANDS.get(cmd_name)
    if not fn:
        return SimpleError(f"ERR unknown command {cmd_name}")
    return fn(args)


# ---------------- RESP PARSER ---------------- #

def parse_array(data):
    lines = data.split(b"\r\n")
    if not lines[0].startswith(b"*"):
        raise ValueError("Invalid array")
    n = int(lines[0][1:])
    items = []
    i = 1
    for _ in range(n):
        assert lines[i].startswith(b"$")
        l = int(lines[i][1:])
        val = lines[i + 1][:l]
        items.append(val)
        i += 2
    return items


# ---------------- SOCKET SERVER ---------------- #

HOST = "localhost"
PORT = 6379

def handle_client(conn, addr):
    while True:
        data = conn.recv(4096)
        if not data:
            break
        try:
            parsed = parse_array(data)
            resp = execute(parsed)
            conn.sendall(resp.encode().encode())
        except Exception as e:
            err = SimpleError(f"ERR {str(e)}")
            conn.sendall(err.encode().encode())
    conn.close()


def main():
    print("Starting Redis clone on 6379...")
    server_socket = socket.create_server((HOST, PORT), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


if __name__ == "__main__":
    main()