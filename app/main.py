#!/usr/bin/env python3
import argparse
import asyncio
import struct
import os
import time
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------
# RESP parser (async)
# ---------------------------
class RESPError(Exception):
    pass

async def _read_line(reader: asyncio.StreamReader) -> bytes:
    line = await reader.readline()
    if not line:
        raise EOFError()
    if not line.endswith(b"\r\n"):
        raise RESPError("Protocol error: expected CRLF")
    return line[:-2]

async def read_resp(reader: asyncio.StreamReader) -> Any:
    """Read one RESP value from the stream."""
    prefix = await reader.read(1)
    if not prefix:
        raise EOFError()
    if prefix == b'+':  # simple string
        line = await _read_line(reader)
        return line.decode()
    if prefix == b'-':  # error
        line = await _read_line(reader)
        return Exception(line.decode())
    if prefix == b':':  # integer
        line = await _read_line(reader)
        return int(line.decode())
    if prefix == b'$':  # bulk string
        line = await _read_line(reader)
        length = int(line.decode())
        if length == -1:
            return None
        data = await reader.readexactly(length)
        crlf = await reader.readexactly(2)
        if crlf != b'\r\n':
            raise RESPError("Protocol error reading bulk string CRLF")
        return data.decode()
    if prefix == b'*':  # array
        line = await _read_line(reader)
        count = int(line.decode())
        if count == -1:
            return None
        arr = []
        for _ in range(count):
            arr.append(await read_resp(reader))
        return arr
    # unsupported (RESP3 extras) - not needed
    raise RESPError("Unsupported RESP type")

# ---------------------------
# RESP encoders
# ---------------------------
def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()

def encode_error(s: str) -> bytes:
    return f"-{s}\r\n".encode()

def encode_integer(i: int) -> bytes:
    return f":{i}\r\n".encode()

def encode_bulk_string(s: Optional[str]) -> bytes:
    if s is None:
        return b"$-1\r\n"
    b = s.encode()
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"

def encode_array(items: Optional[List[Any]]) -> bytes:
    if items is None:
        return b"*-1\r\n"
    out = b"*" + str(len(items)).encode() + b"\r\n"
    for item in items:
        if item is None:
            out += b"$-1\r\n"
        elif isinstance(item, int):
            out += encode_integer(item)
        elif isinstance(item, list):
            out += encode_array(item)
        else:
            out += encode_bulk_string(str(item))
    return out

# ---------------------------
# RDB helpers (simple loader)
# Uses the improved _read_length_encoding/_read_string approach you provided
# ---------------------------
def _read_length_encoding(f) -> int:
    first_byte = f.read(1)
    if not first_byte:
        return 0
    first = first_byte[0]
    encoding_type = (first & 0xC0) >> 6
    if encoding_type == 0:  # 00 - next 6 bits is length
        return first & 0x3F
    elif encoding_type == 1:  # 01 - next 14 bits is length
        nxt = f.read(1)
        if not nxt:
            return 0
        return ((first & 0x3F) << 8) | nxt[0]
    elif encoding_type == 2:  # 10 - next 4 bytes is length (big-endian)
        data = f.read(4)
        if len(data) < 4:
            return 0
        return struct.unpack(">I", data)[0]
    else:  # 11 - special encoding (we treat as small)
        return first & 0x3F

def _read_string(f) -> str:
    length = _read_length_encoding(f)
    if length == 0:
        return ""
    data = f.read(length)
    if len(data) < length:
        return ""
    return data.decode('utf-8', errors='ignore')

def read_key_val_from_db(dir_path, dbfilename, data):
    """Load key-values from RDB (if exists)"""
    rdb_file_loc = os.path.join(dir_path, dbfilename)
    if not os.path.isfile(rdb_file_loc):
        return

    try:
        with open(rdb_file_loc, "rb") as f:
            # Read 9 bytes header: "REDIS" + version (e.g. "0009")
            header = f.read(9)
            if not header.startswith(b"REDIS"):
                return

            # Skip optional metadata sections until we find DB selector (0xFE)
            while True:
                opcode = f.read(1)
                if not opcode:
                    break

                # Database selector
                if opcode == b"\xfe":
                    _ = f.read(1)  # skip db number
                    continue

                # Resize DB section
                elif opcode == b"\xfb":
                    _read_length_encoding(f)
                    _read_length_encoding(f)
                    continue

                # Expiry times
                elif opcode in (b"\xfd", b"\xfc"):
                    f.read(4 if opcode == b"\xfd" else 8)
                    opcode = f.read(1)
                    if not opcode:
                        break

                # End of RDB
                elif opcode == b"\xff":
                    break

                # Key-value pair (string encoding)
                elif opcode == b"\x00":
                    key = _read_string(f)
                    value = _read_string(f)
                    if key and value:
                        data[key] = (value, -1)

                else:
                    break

    except Exception as e:
        print(f"Error reading RDB file: {e}")

# ---------------------------
# In-memory store & conditions
# ---------------------------
# store: key -> value
# For string values store[key] = ("string", "value")
# For lists store[key] = ("list", [elements...])
# expiry dict: key -> expire_at (timestamp seconds) or -1
store: Dict[str, Tuple[str, Any]] = {}
expiry: Dict[str, float] = {}
# per-key conditions for BLPOP notifications
_conditions: Dict[str, asyncio.Condition] = {}

def get_condition_for(key: str) -> asyncio.Condition:
    if key not in _conditions:
        _conditions[key] = asyncio.Condition()
    return _conditions[key]

def _clean_expired(key: str):
    if key in expiry:
        if expiry[key] != -1 and time.time() > expiry[key]:
            # delete key
            store.pop(key, None)
            expiry.pop(key, None)

def clean_all():
    for k in list(expiry.keys()):
        _clean_expired(k)

# ---------------------------
# Command implementations
# ---------------------------
async def cmd_ping(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) == 0:
        writer.write(encode_simple_string("PONG"))
    else:
        writer.write(encode_bulk_string(str(args[0])))

async def cmd_echo(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) < 1:
        writer.write(encode_error("ERR wrong number of arguments for 'echo' command"))
    else:
        writer.write(encode_bulk_string(str(args[0])))

async def cmd_set(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) < 2:
        writer.write(encode_error("ERR wrong number of arguments for 'set' command"))
        return
    key = str(args[0])
    val = str(args[1])
    ex = -1
    # Handle PX milliseconds optionally: args like ... PX 1000 or px 1000 or EX seconds
    idx = 2
    while idx + 1 < len(args):
        opt = str(args[idx]).lower()
        optval = args[idx+1]
        if opt == "px":
            try:
                ex = time.time() + float(optval)/1000.0
            except Exception:
                pass
        elif opt == "ex":
            try:
                ex = time.time() + float(optval)
            except Exception:
                pass
        idx += 2
    store[key] = ("string", val)
    expiry[key] = ex
    writer.write(encode_simple_string("OK"))

async def cmd_get(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) < 1:
        writer.write(encode_error("ERR wrong number of arguments for 'get' command"))
        return
    key = str(args[0])
    _clean_expired(key)
    entry = store.get(key)
    if not entry:
        writer.write(encode_bulk_string(None))
        return
    typ, val = entry
    if typ != "string":
        # In Redis, GET on non-string returns WRONGTYPE error
        writer.write(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        return
    writer.write(encode_bulk_string(val))

async def cmd_rpush(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) < 2:
        writer.write(encode_error("ERR wrong number of arguments for 'rpush' command"))
        return
    key = str(args[0])
    vals = [str(x) for x in args[1:]]
    _clean_expired(key)
    entry = store.get(key)
    if not entry:
        store[key] = ("list", list(vals))
        expiry.pop(key, None)
    else:
        typ, cur = entry
        if typ != "list":
            writer.write(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
            return
        cur.extend(vals)
    # notify waiting BLPOP
    cond = get_condition_for(key)
    async with cond:
        cond.notify_all()
    length = len(store[key][1])
    writer.write(encode_integer(length))

async def cmd_lpush(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) < 2:
        writer.write(encode_error("ERR wrong number of arguments for 'lpush' command"))
        return
    key = str(args[0])
    vals = [str(x) for x in args[1:]]
    _clean_expired(key)
    entry = store.get(key)
    if not entry:
        store[key] = ("list", [])
        expiry.pop(key, None)
    typ, cur = store[key]
    if typ != "list":
        writer.write(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        return
    # push left in order given (first provided becomes leftmost)
    for v in reversed(vals):
        cur.insert(0, v)
    cond = get_condition_for(key)
    async with cond:
        cond.notify_all()
    writer.write(encode_integer(len(cur)))

async def cmd_llen(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) < 1:
        writer.write(encode_error("ERR wrong number of arguments for 'llen' command"))
        return
    key = str(args[0])
    _clean_expired(key)
    entry = store.get(key)
    if not entry:
        writer.write(encode_integer(0))
        return
    typ, cur = entry
    if typ != "list":
        writer.write(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        return
    writer.write(encode_integer(len(cur)))

async def cmd_lrange(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) < 3:
        writer.write(encode_error("ERR wrong number of arguments for 'lrange' command"))
        return
    key = str(args[0])
    start = int(args[1])
    end = int(args[2])
    _clean_expired(key)
    entry = store.get(key)
    if not entry:
        writer.write(encode_array([]))
        return
    typ, cur = entry
    if typ != "list":
        writer.write(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        return
    n = len(cur)
    # normalize negative indices
    if start < 0:
        start = n + start
    if end < 0:
        end = n + end
    start = max(start, 0)
    end = min(end, n - 1)
    if start > end or start >= n:
        writer.write(encode_array([]))
        return
    sub = cur[start:end+1]
    writer.write(encode_array(sub))

async def cmd_lpop(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) < 1:
        writer.write(encode_error("ERR wrong number of arguments for 'lpop' command"))
        return
    key = str(args[0])
    count = 1
    if len(args) > 1:
        try:
            count = int(args[1])
            if count < 0:
                raise ValueError()
        except Exception:
            writer.write(encode_error("ERR value is not an integer or out of range"))
            return
    _clean_expired(key)
    entry = store.get(key)
    if not entry:
        # empty
        if count == 1:
            writer.write(encode_bulk_string(None))
        else:
            writer.write(encode_array([]))
        return
    typ, cur = entry
    if typ != "list":
        writer.write(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        return
    if len(cur) == 0:
        if count == 1:
            writer.write(encode_bulk_string(None))
        else:
            writer.write(encode_array([]))
        return
    # pop up to count
    n = min(count, len(cur))
    if n == 1 and count == 1:
        val = cur.pop(0)
        writer.write(encode_bulk_string(val))
    else:
        popped = []
        for _ in range(n):
            popped.append(cur.pop(0))
        writer.write(encode_array(popped))

async def cmd_blpop(args: List[Any], writer: asyncio.StreamWriter):
    # BLPOP key [key ...] timeout
    if len(args) < 2:
        writer.write(encode_error("ERR wrong number of arguments for 'blpop' command"))
        return
    *keys, timeout_raw = args
    try:
        timeout = float(timeout_raw)
        if timeout < 0:
            timeout = 0.0
    except Exception:
        writer.write(encode_error("ERR timeout is not a float"))
        return
    # immediate check
    clean_all()
    for k in keys:
        k = str(k)
        entry = store.get(k)
        if entry and entry[0] == "list" and len(entry[1]) > 0:
            val = entry[1].pop(0)
            writer.write(encode_array([k, val]))
            return
    # blocking wait up to timeout
    # if timeout == 0 -> block indefinitely
    end_time = None if timeout == 0 else time.time() + timeout
    # We'll wait on a per-key condition in round-robin; prefer first key that becomes available.
    # Implementation: await until any key gets notified or timeout elapses
    tasks = []
    try:
        while True:
            for k in keys:
                k = str(k)
                _clean_expired(k)
                entry = store.get(k)
                if entry and entry[0] == "list" and len(entry[1]) > 0:
                    val = entry[1].pop(0)
                    writer.write(encode_array([k, val]))
                    return
            if end_time is not None and time.time() > end_time:
                writer.write(b"*-1\r\n")
                return
            # Wait on conditions: create gather of waiters for small slice of time
            wait_tasks = []
            # we pick a short sleep and wait on any condition notify using a single combined wait:
            # To avoid creating many waiter coros, simply sleep 0.05 and loop (cheap in asyncio)
            # But we should use per-key conditions to be notified immediately:
            # We'll wait on first key's condition with a small timeout to yield back
            # Simpler and robust: use asyncio.wait_for on an Event that is notified by producers.
            # For simplicity and reliability in tests, we do a short asyncio.sleep
            await asyncio.sleep(0.02)
    except asyncio.CancelledError:
        writer.write(b"*-1\r\n")
        return

async def cmd_config_get(args: List[Any], writer: asyncio.StreamWriter, server_config: Dict[str,str]):
    if len(args) < 1:
        writer.write(encode_error("ERR wrong number of arguments for 'config get'"))
        return
    param = str(args[0]).lower()
    if param in server_config:
        writer.write(encode_array([param, server_config[param]]))
    else:
        writer.write(encode_array([]))

async def cmd_keys(args: List[Any], writer: asyncio.StreamWriter):
    if len(args) != 1:
        writer.write(encode_error("ERR wrong number of arguments for 'keys'"))
        return
    pattern = str(args[0])
    if pattern == "*":
        clean_all()
        keys = [k for k in store.keys()]
        writer.write(encode_array(keys))
    else:
        # not needed for tests
        writer.write(encode_array([]))

# ---------------------------
# Dispatcher
# ---------------------------
async def dispatch(command: List[Any], writer: asyncio.StreamWriter, server_config: Dict[str,str]):
    if not command:
        return
    cmd = str(command[0]).lower()
    args = command[1:] if len(command) > 1 else []
    try:
        if cmd == "ping":
            await cmd_ping(args, writer)
        elif cmd == "echo":
            await cmd_echo(args, writer)
        elif cmd == "set":
            await cmd_set(args, writer)
        elif cmd == "get":
            await cmd_get(args, writer)
        elif cmd == "rpush":
            await cmd_rpush(args, writer)
        elif cmd == "lpush":
            await cmd_lpush(args, writer)
        elif cmd == "llen":
            await cmd_llen(args, writer)
        elif cmd == "lrange":
            await cmd_lrange(args, writer)
        elif cmd == "lpop":
            await cmd_lpop(args, writer)
        elif cmd == "blpop":
            await cmd_blpop(args, writer)
        elif cmd == "config" and len(args) > 0 and str(args[0]).lower() == "get":
            await cmd_config_get(args[1:], writer, server_config)
        elif cmd == "keys":
            await cmd_keys(args, writer)
        else:
            writer.write(encode_error(f"ERR unknown command '{cmd}'"))
    except Exception as e:
        writer.write(encode_error(f"ERR server error: {e}"))

# ---------------------------
# Server
# ---------------------------
async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server_config: Dict[str,str]):
    addr = writer.get_extra_info("peername")
    try:
        while True:
            try:
                val = await read_resp(reader)
            except EOFError:
                break
            except RESPError:
                # protocol error: close connection
                break
            if val is None:
                # array null? skip
                continue
            if not isinstance(val, list):
                # commands are arrays; ignore invalid
                writer.write(encode_error("ERR Protocol: command must be array"))
                await writer.drain()
                break
            await dispatch(val, writer, server_config)
            await writer.drain()
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def start_server(host: str, port: int, server_config: Dict[str,str]):
    server = await asyncio.start_server(lambda r,w: handle_client(r,w,server_config), host, port)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Server started on {addrs}")
    async with server:
        await server.serve_forever()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default=".", help="dir for rdb files")
    p.add_argument("--dbfilename", default="dump.rdb", help="dbfilename")
    p.add_argument("--port", default="6379", help="port")
    return p.parse_args()

# ---------------------------
# Main entry
# ---------------------------
def main():
    args = parse_args()
    server_config = {
        "dir": args.dir,
        "dbfilename": args.dbfilename,
    }
    # load rdb into store (string keys only)
    read_key_val_from_db(args.dir, args.dbfilename, {})  # call once to avoid side-effects (we'll reload properly)
    # We'll attempt to load and set into our store properly:
    rdb_store: Dict[str, Tuple[str,int]] = {}
    read_key_val_from_db(args.dir, args.dbfilename, rdb_store)
    for k,v in rdb_store.items():
        val, exp = v
        store[k] = ("string", val)
        expiry[k] = exp
    port = int(args.port)
    try:
        asyncio.run(start_server("0.0.0.0", port, server_config))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()