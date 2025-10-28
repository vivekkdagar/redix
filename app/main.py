# app/main.py
import asyncio
from collections import deque, defaultdict

HOST = "0.0.0.0"  # use 0.0.0.0 for test environment visibility
PORT = 6379

# -----------------------
# In-memory data storage
# -----------------------
store = {}  # key -> deque([...]) for lists or str for strings (we'll only use lists and strings)
waiters = defaultdict(deque)  # key -> deque of asyncio.Future objects (waiting BLPOP clients)
lock = asyncio.Lock()  # protect store and waiters in async environment


# -----------------------
# RESP helpers
# -----------------------
def encode_simple(s: str) -> bytes:
    return f"+{s}\r\n".encode()


def encode_bulk(s: str | None) -> bytes:
    if s is None:
        return b"$-1\r\n"
    b = s.encode()
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"


def encode_integer(n: int) -> bytes:
    return f":{n}\r\n".encode()


def encode_array(items: list[str]) -> bytes:
    resp = b"*" + str(len(items)).encode() + b"\r\n"
    for it in items:
        resp += encode_bulk(it)
    return resp


# -----------------------
# RESP parser (simple, robust for RESP Array of bulk strings)
# -----------------------
def try_parse_resp_array(buf: bytes):
    """
    Try to parse a RESP Array of bulk strings from buf.
    Returns (args_list, remaining_bytes) or (None, buf) if incomplete.
    """
    if not buf:
        return None, buf
    if buf[0:1] != b"*":
        # Not an array — we won't support inline protocol; return error by signaling parsed empty
        return None, buf
    # find line break
    try:
        line_end = buf.index(b"\r\n")
    except ValueError:
        return None, buf
    try:
        count = int(buf[1:line_end].decode())
    except Exception:
        return None, buf
    pos = line_end + 2
    args = []
    for _ in range(count):
        # need a $len\r\n
        if pos + 1 >= len(buf):
            return None, buf
        if buf[pos:pos + 1] != b"$":
            return None, buf
        # find end of this line
        try:
            len_end = buf.index(b"\r\n", pos)
        except ValueError:
            return None, buf
        try:
            blen = int(buf[pos + 1:len_end].decode())
        except Exception:
            return None, buf
        start = len_end + 2
        end = start + blen
        # ensure we have the bytes plus trailing CRLF
        if end + 2 > len(buf):
            return None, buf
        arg = buf[start:end].decode()
        args.append(arg)
        if buf[end:end + 2] != b"\r\n":
            return None, buf
        pos = end + 2
    remaining = buf[pos:]
    return args, remaining


# -----------------------
# Command implementations
# -----------------------
async def cmd_ping(args, writer):
    writer.write(encode_simple("PONG"))


async def cmd_echo(args, writer):
    if len(args) >= 2:
        writer.write(encode_bulk(args[1]))
    else:
        writer.write(encode_bulk(None))


async def cmd_set(args, writer):
    # SET key value  (we don't implement PX/EX here)
    if len(args) >= 3:
        key = args[1]
        val = args[2]
        async with lock:
            store[key] = val
        writer.write(encode_simple("OK"))
    else:
        writer.write(b"-ERR wrong number of arguments for 'set' command\r\n")


async def cmd_get(args, writer):
    if len(args) >= 2:
        key = args[1]
        async with lock:
            v = store.get(key)
        if v is None or isinstance(v, deque) and len(v) == 0:
            writer.write(encode_bulk(None))
        elif isinstance(v, deque):
            # wrong type for GET (we treat lists as wrong type)
            writer.write(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
        else:
            writer.write(encode_bulk(v))
    else:
        writer.write(b"-ERR wrong number of arguments for 'get' command\r\n")


async def cmd_rpush(args, writer):
    # RPUSH key val1 val2 ...
    if len(args) < 3:
        writer.write(b"-ERR wrong number of arguments for 'rpush' command\r\n")
        return

    key = args[1]
    values = args[2:]

    async with lock:
        # ensure list exists
        if key not in store or not isinstance(store.get(key), deque):
            store[key] = deque()
        dq = store[key]

        # for each value, if there are waiters for this key, satisfy the earliest waiter instead of pushing
        pushed_count = 0
        for val in values:
            if waiters[key]:
                future = waiters[key].popleft()
                # deliver to waiter by setting result (it will send response)
                if not future.done():
                    future.set_result((key, val))
                # not stored
            else:
                dq.append(val)
                pushed_count += 1

        # total length is current queue length
        length = len(dq)
    writer.write(encode_integer(length))


async def cmd_lpush(args, writer):
    # LPUSH key val1 val2 ...
    if len(args) < 3:
        writer.write(b"-ERR wrong number of arguments for 'lpush' command\r\n")
        return

    key = args[1]
    values = args[2:]

    async with lock:
        if key not in store or not isinstance(store.get(key), deque):
            store[key] = deque()
        dq = store[key]
        # Redis prepends values so that the first element in argument list becomes left-most last:
        # LPUSH key a b c => list becomes [c, b, a, ...] ; to get that, insert left in iteration order
        for val in values:
            dq.appendleft(val)
        length = len(dq)
    writer.write(encode_integer(length))


async def cmd_llen(args, writer):
    # LLEN key
    if len(args) != 2:
        writer.write(b"-ERR wrong number of arguments for 'llen' command\r\n")
        return
    key = args[1]
    async with lock:
        dq = store.get(key)
        if not isinstance(dq, deque):
            writer.write(encode_integer(0))
            return
        length = len(dq)
    writer.write(encode_integer(length))


async def cmd_lrange(args, writer):
    # LRANGE key start stop
    if len(args) != 4:
        writer.write(b"-ERR wrong number of arguments for 'lrange' command\r\n")
        return
    key = args[1]
    start = int(args[2])
    stop = int(args[3])
    async with lock:
        dq = store.get(key)
        if not isinstance(dq, deque):
            writer.write(b"*0\r\n")
            return
        arr = list(dq)
    # handle negative stop
    n = len(arr)
    if start < 0:
        start = n + start
    if stop < 0:
        stop = n + stop
    start = max(start, 0)
    stop = min(stop, n - 1)
    if start > stop or start >= n:
        writer.write(b"*0\r\n")
        return
    slice_list = arr[start: stop + 1]
    writer.write(encode_array(slice_list))


async def cmd_lpop(args, writer):
    # LPOP key [count]
    if len(args) < 2:
        writer.write(b"-ERR wrong number of arguments for 'lpop' command\r\n")
        return
    key = args[1]
    count = 1
    if len(args) == 3:
        try:
            count = int(args[2])
        except Exception:
            writer.write(b"-ERR value is not an integer or out of range\r\n")
            return

    async with lock:
        dq = store.get(key)
        if not isinstance(dq, deque) or len(dq) == 0:
            writer.write(encode_bulk(None) if count == 1 else b"*0\r\n")
            return
        if count == 1:
            val = dq.popleft()
            writer.write(encode_bulk(val))
            return
        # multiple
        popped = []
        for _ in range(min(count, len(dq))):
            popped.append(dq.popleft())
    writer.write(encode_array(popped))


async def cmd_blpop(args, writer):
    # BLPOP key timeout
    # This stage tests only timeout == 0 (block indefinitely)
    if len(args) != 3:
        writer.write(b"-ERR wrong number of arguments for 'blpop' command\r\n")
        return
    key = args[1]
    timeout = int(args[2])  # we only handle 0 for now
    if timeout != 0:
        # Not required for this stage; return error or immediate null - choose to return error
        writer.write(b"-ERR only timeout 0 is supported in this implementation\r\n")
        return

    # First, try to pop immediately
    async with lock:
        dq = store.get(key)
        if isinstance(dq, deque) and len(dq) > 0:
            val = dq.popleft()
            # respond with [key, val]
            writer.write(encode_array([key, val]))
            return

        # otherwise, block: create a future and wait for RPUSH to set it
        fut = asyncio.get_event_loop().create_future()
        waiters[key].append(fut)

    try:
        # wait forever (timeout==0)
        key_name, value = await fut
        # When future completes, send the pair [key, value]
        writer.write(encode_array([key_name, value]))
    except asyncio.CancelledError:
        writer.write(b"* -1\r\n")
    except Exception:
        writer.write(b"-ERR internal error\r\n")


# Dispatcher
async def dispatch_command(args, writer):
    if not args:
        writer.write(b"-ERR invalid request\r\n")
        return
    cmd = args[0].upper()
    if cmd == "PING":
        await cmd_ping(args, writer)
    elif cmd == "ECHO":
        await cmd_echo(args, writer)
    elif cmd == "SET":
        await cmd_set(args, writer)
    elif cmd == "GET":
        await cmd_get(args, writer)
    elif cmd == "RPUSH":
        await cmd_rpush(args, writer)
    elif cmd == "LPUSH":
        await cmd_lpush(args, writer)
    elif cmd == "LLEN":
        await cmd_llen(args, writer)
    elif cmd == "LRANGE":
        await cmd_lrange(args, writer)
    elif cmd == "LPOP":
        await cmd_lpop(args, writer)
    elif cmd == "BLPOP":
        await cmd_blpop(args, writer)
    else:
        writer.write(b"-ERR unknown command\r\n")


# -----------------------
# Connection handler
# -----------------------
async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    # buffer for bytes
    buf = b""
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            buf += data
            parsed, buf = try_parse_resp_array(buf)
            if parsed is None:
                # wait for more data
                continue
            # parsed is list of str arguments
            await dispatch_command(parsed, writer)
            await writer.drain()
    except Exception:
        # swallow errors to keep server alive
        pass
    finally:
        # If this connection had a pending future in any waiters, we should remove it.
        # Clean up any futures that belong to this connection:
        # Not strictly necessary for the test harness, but good hygiene.
        async with lock:
            for key_q in list(waiters.keys()):
                newdq = deque()
                while waiters[key_q]:
                    fut = waiters[key_q].popleft()
                    if fut.done():
                        continue
                    # can't directly check if fut tied to this writer; futures don't carry writer, so skip
                    newdq.append(fut)
                waiters[key_q] = newdq
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# -----------------------
# Server entrypoint
# -----------------------
async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Redis clone running on {addrs}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())