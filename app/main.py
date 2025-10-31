#!/usr/bin/env python3
"""
redis_clone_allinone.py

Single-file Redis-like server (asyncio) merging:
- MULTI / EXEC from threaded Server logic
- Pub/Sub with correct integer in subscribe/unsubscribe replies
- Lists, Streams, Blocking, Transactions, RDB stub, INFO/CONFIG/KEYS
"""

import asyncio
import time
import threading
from collections import defaultdict, deque

BUF_SIZE = 65536

# ------------------------
# In-memory stores
# ------------------------
kv = {}                        # key -> [value, expiry_ts_or_None]
lists = defaultdict(list)      # key -> list
streams = defaultdict(list)    # key -> list of entry dicts {'id':..., field: val, ...}

# Blocking waiters
blpop_blocked = defaultdict(deque)   # key -> deque of (writer)
xread_blocked = defaultdict(list)    # key -> list of (writer, start_id)

# Transactions
multi_clients = set()                # set of writer objects in MULTI mode
multi_deques = defaultdict(deque)    # writer_id -> deque of queued commands (list-of-bytes argv)

# Pub/Sub
subscriptions = {}                   # writer -> set(channels)
channel_subs = defaultdict(set)      # channel -> set(writers)
subscriber_mode = set()              # writers currently in subscribed mode

# Helpers
def writer_id(writer):
    return str(id(writer))

# ------------------------
# RESP parsing & encoding
# ------------------------

def parse_next(data: bytes):
    """Parse one RESP item from the start of data. Return (item, remaining)."""
    if not data:
        raise RuntimeError("Incomplete")
    t = data[0:1]
    if t == b'*':
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        n = int(data[1:nl])
        rest = data[nl+2:]
        arr = []
        for _ in range(n):
            item, rest = parse_next(rest)
            arr.append(item)
        return arr, rest
    if t == b'$':
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        blen = int(data[1:nl])
        rest = data[nl+2:]
        if blen == -1:
            return None, rest
        if len(rest) < blen + 2:
            raise RuntimeError("Incomplete")
        val = rest[:blen]
        rest = rest[blen+2:]
        return val, rest
    if t == b'+':
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        return data[1:nl].decode(), data[nl+2:]
    if t == b'-':
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        return Exception(data[1:nl].decode()), data[nl+2:]
    if t == b':':
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        return int(data[1:nl]), data[nl+2:]
    raise RuntimeError("Unknown RESP type")

def parse_all(buffer: bytes):
    """Parse as many complete RESP messages as possible and return list of items."""
    msgs = []
    buf = buffer
    consumed = 0
    while buf:
        try:
            item, buf = parse_next(buf)
            msgs.append(item)
            consumed = len(buffer) - len(buf)
        except RuntimeError:
            break
    return msgs, consumed

def resp_simple(s: str):
    return f"+{s}\r\n".encode()

def resp_error(s: str):
    return f"-{s}\r\n".encode()

def resp_int(n: int):
    return f":{n}\r\n".encode()

def resp_bulk(b):
    if b is None:
        return b"$-1\r\n"
    if isinstance(b, str):
        b = b.encode()
    return b"${}\r\n".format(len(b)).encode() + b + b"\r\n"

def resp_array_from_encoded(elements: list[bytes]):
    out = f"*{len(elements)}\r\n".encode()
    for e in elements:
        out += e
    return out

def resp_subscribe_reply(kind: str, channel: str, count: int):
    # *3\r\n$<len(kind)>\r\nkind\r\n$<len(channel)>\r\nchannel\r\n:<count>\r\n
    return (f"*3\r\n${len(kind)}\r\n{kind}\r\n${len(channel)}\r\n{channel}\r\n:{count}\r\n").encode()

# ------------------------
# Utilities
# ------------------------
def now_s():
    return time.time()

def ensure_kv_cleanup(key):
    v = kv.get(key)
    if not v:
        return
    _, expiry = v
    if expiry is not None and expiry <= time.time():
        del kv[key]

# ------------------------
# Command handlers (return RESP bytes or None if blocking)
# ------------------------

async def handle_ping(argv, reader=None, writer=None):
    # PING in subscribed mode should return array ["pong", msg] (bulk strings)
    if writer in subscriber_mode:
        msg = argv[1].decode() if len(argv) > 1 and argv[1] is not None else ""
        return (f"*2\r\n$4\r\npong\r\n${len(msg)}\r\n{msg}\r\n").encode()
    return resp_simple("PONG")

async def handle_echo(argv, reader=None, writer=None):
    msg = argv[1].decode() if len(argv) > 1 and argv[1] is not None else ""
    return resp_bulk(msg)

async def handle_set(argv, reader=None, writer=None):
    key = argv[1].decode()
    val = argv[2].decode()
    expiry = None
    if len(argv) >= 5:
        opt = argv[3].decode().upper()
        if opt == "PX":
            expiry = time.time() + int(argv[4].decode())/1000.0
        elif opt == "EX":
            expiry = time.time() + int(argv[4].decode())
    kv[key] = [val, expiry]
    return resp_simple("OK")

async def handle_get(argv, reader=None, writer=None):
    key = argv[1].decode()
    ensure_kv_cleanup(key)
    v = kv.get(key)
    if not v:
        return resp_bulk(None)
    return resp_bulk(v[0])

async def handle_incr(argv, reader=None, writer=None):
    key = argv[1].decode()
    ensure_kv_cleanup(key)
    v = kv.get(key)
    if not v:
        kv[key] = ["1", None]
        return resp_int(1)
    val, expiry = v
    try:
        n = int(val) + 1
    except Exception:
        return resp_error("ERR value is not an integer or out of range")
    kv[key] = [str(n), expiry]
    return resp_int(n)

# --- Lists ---
async def handle_rpush(argv, reader=None, writer=None):
    key = argv[1].decode()
    for b in argv[2:]:
        lists[key].append(b.decode())
    # wake blpop if any
    if key in blpop_blocked and blpop_blocked[key]:
        w = blpop_blocked[key].popleft()
        element = lists[key].pop(0)
        resp = (f"*2\r\n${len(key)}\r\n{key}\r\n${len(element)}\r\n{element}\r\n").encode()
        try:
            w.write(resp)
            await w.drain()
        except Exception:
            pass
    return resp_int(len(lists[key]))

async def handle_lpush(argv, reader=None, writer=None):
    key = argv[1].decode()
    for b in argv[2:]:
        lists[key].insert(0, b.decode())
    return resp_int(len(lists[key]))

async def handle_llen(argv, reader=None, writer=None):
    key = argv[1].decode()
    return resp_int(len(lists.get(key, [])))

async def handle_lrange(argv, reader=None, writer=None):
    key = argv[1].decode()
    start = int(argv[2].decode())
    end = int(argv[3].decode())
    arr = lists.get(key, [])
    n = len(arr)
    if n == 0:
        return b"*0\r\n"
    if start < 0:
        start = n + start
    if end < 0:
        end = n + end
    start = max(0, start)
    end = min(n-1, end)
    if start > end:
        return b"*0\r\n"
    sub = arr[start:end+1]
    out = f"*{len(sub)}\r\n".encode()
    for s in sub:
        out += resp_bulk(s)
    return out

async def handle_lpop(argv, reader=None, writer=None):
    key = argv[1].decode()
    if key not in lists or not lists[key]:
        return resp_bulk(None)
    if len(argv) == 2:
        v = lists[key].pop(0)
        return resp_bulk(v)
    else:
        count = int(argv[2].decode())
        popped = []
        for _ in range(min(count, len(lists[key]))):
            popped.append(lists[key].pop(0))
        out = f"*{len(popped)}\r\n".encode()
        for p in popped:
            out += resp_bulk(p)
        return out

async def handle_blpop(argv, reader=None, writer=None):
    key = argv[1].decode()
    timeout = float(argv[2].decode())
    if key in lists and lists[key]:
        v = lists[key].pop(0)
        return (f"*2\r\n${len(key)}\r\n{key}\r\n${len(v)}\r\n{v}\r\n").encode()
    # block
    blpop_blocked[key].append(writer)
    async def timeout_unblock():
        if timeout == 0:
            return
        await asyncio.sleep(timeout)
        # if still blocked:
        try:
            if writer in blpop_blocked.get(key, ()):
                try:
                    blpop_blocked[key].remove(writer)
                except ValueError:
                    return
                try:
                    writer.write(b"*-1\r\n")
                    await writer.drain()
                except Exception:
                    pass
        except Exception:
            pass
    asyncio.create_task(timeout_unblock())
    return None

# --- Streams (basic) ---
def _allot_stream_id(key, tstamp=None):
    t = int(time.time()*1000) if tstamp is None else int(tstamp)
    seq = 0
    if streams[key]:
        last = streams[key][-1]['id']
        last_t, last_s = map(int, last.split('-',1))
        if t < last_t:
            t = last_t
            seq = last_s + 1
        elif t == last_t:
            seq = last_s + 1
    return f"{t}-{seq}"

async def handle_xadd(argv, reader=None, writer=None):
    key = argv[1].decode()
    entry_id = argv[2].decode()
    if entry_id == '*':
        eid = _allot_stream_id(key)
    else:
        if '-' in entry_id:
            tpart, spart = entry_id.split('-',1)
            if spart == '*':
                eid = _allot_stream_id(key, tstamp=int(tpart))
            else:
                eid = entry_id
        else:
            eid = _allot_stream_id(key)
    fields = {}
    for i in range(3, len(argv), 2):
        fields[argv[i].decode()] = argv[i+1].decode()
    entry = {'id': eid}
    entry.update(fields)
    streams[key].append(entry)
    # notify xread blocked waiters
    if key in xread_blocked and xread_blocked[key]:
        pending = list(xread_blocked[key])
        xread_blocked[key].clear()
        for w, start_id in pending:
            out_entries = []
            for e in streams[key]:
                if e['id'] > start_id:
                    fv = []
                    for kf, vf in e.items():
                        if kf == 'id': continue
                        fv.append([kf, vf])
                    out_entries.append([e['id'], fv])
            if out_entries:
                s = f"*1\r\n*2\r\n${len(key)}\r\n{key}\r\n*{len(out_entries)}\r\n".encode()
                for eid, fv in out_entries:
                    s += f"${len(eid)}\r\n{eid}\r\n".encode()
                    s += f"*{len(fv)*2}\r\n".encode()
                    for p in fv:
                        s += resp_bulk(p[0])
                        s += resp_bulk(p[1])
                try:
                    w.write(s)
                    await w.drain()
                except Exception:
                    pass
    return resp_bulk(eid)

async def handle_xrange(argv, reader=None, writer=None):
    key = argv[1].decode()
    start = argv[2].decode()
    end = argv[3].decode()
    if key not in streams:
        return b"*0\r\n"
    out = []
    for e in streams[key]:
        if start <= e['id'] <= end:
            fv = []
            for kf, vf in e.items():
                if kf == 'id': continue
                fv.append([kf, vf])
            out.append([e['id'], fv])
    if not out:
        return b"*0\r\n"
    s = f"*{len(out)}\r\n".encode()
    for eid, fv in out:
        s += f"*2\r\n${len(eid)}\r\n{eid}\r\n".encode()
        s += f"*{len(fv)}\r\n".encode()
        for p in fv:
            # flatten each pair as array [field, val]
            s += f"*2\r\n".encode()
            s += resp_bulk(p[0])
            s += resp_bulk(p[1])
    return s

async def handle_xread(argv, reader=None, writer=None):
    # only supporting BLOCK STREAMS key id and simple non-blocking variant
    if argv[0].decode().upper() == 'BLOCK':
        timeout_ms = int(argv[1].decode())
        # expect: BLOCK <ms> STREAMS <key> <id>
        if argv[2].decode().upper() != 'STREAMS':
            return resp_error("ERR unsupported XREAD format")
        key = argv[3].decode()
        start = argv[4].decode()
        start_id = streams[key][-1]['id'] if streams[key] and start == '$' else start
        found = []
        for e in streams[key]:
            if e['id'] > start_id:
                fv = []
                for kf, vf in e.items():
                    if kf == 'id': continue
                    fv.append([kf, vf])
                found.append([e['id'], fv])
        if found:
            s = f"*1\r\n*2\r\n${len(key)}\r\n{key}\r\n*{len(found)}\r\n".encode()
            for eid, fv in found:
                s += f"${len(eid)}\r\n{eid}\r\n".encode()
                s += f"*{len(fv)*2}\r\n".encode()
                for p in fv:
                    s += resp_bulk(p[0])
                    s += resp_bulk(p[1])
            return s
        # block
        xread_blocked[key].append((writer, start if start != '$' else ('0-0')))
        async def timeout_unblock():
            if timeout_ms == 0:
                return
            await asyncio.sleep(timeout_ms/1000.0)
            # if still blocked
            try:
                pending = list(xread_blocked.get(key, []))
                for w,sid in pending:
                    if w is writer:
                        try:
                            writer.write(b"*-1\r\n")
                            await writer.drain()
                        except Exception:
                            pass
                        try:
                            xread_blocked[key].remove((w,sid))
                        except ValueError:
                            pass
                        break
            except Exception:
                pass
        asyncio.create_task(timeout_unblock())
        return None
    else:
        # non-blocking XREAD STREAMS k1 k2 ... id1 id2 ...
        half = len(argv)//2
        keys = [a.decode() for a in argv[:half]]
        ids = [a.decode() for a in argv[half:]]
        result = []
        for key, start in zip(keys, ids):
            entries = []
            for e in streams.get(key, []):
                if e['id'] > start:
                    fv = []
                    for kf, vf in e.items():
                        if kf == 'id': continue
                        fv.append([kf, vf])
                    entries.append([e['id'], fv])
            if entries:
                result.append([key, entries])
        if not result:
            return b"*0\r\n"
        s = f"*{len(result)}\r\n".encode()
        for key, entries in result:
            s += f"*2\r\n${len(key)}\r\n{key}\r\n*{len(entries)}\r\n".encode()
            for eid, fv in entries:
                s += f"${len(eid)}\r\n{eid}\r\n".encode()
                s += f"*{len(fv)*2}\r\n".encode()
                for p in fv:
                    s += resp_bulk(p[0])
                    s += resp_bulk(p[1])
        return s

# ------------------------
# Pub/Sub
# ------------------------

def subscribe_writer_to_channels(writer, channels):
    if writer not in subscriptions:
        subscriptions[writer] = set()
    for ch in channels:
        subscriptions[writer].add(ch)
        channel_subs[ch].add(writer)
    subscriber_mode.add(writer)

def unsubscribe_writer_from_channels(writer, channels):
    if writer not in subscriptions:
        subscriptions[writer] = set()
    for ch in channels:
        if ch in subscriptions[writer]:
            subscriptions[writer].remove(ch)
        if writer in channel_subs.get(ch, set()):
            channel_subs[ch].remove(writer)
            if not channel_subs[ch]:
                del channel_subs[ch]
    if not subscriptions.get(writer):
        subscriptions.pop(writer, None)
        subscriber_mode.discard(writer)

async def handle_subscribe(argv, reader=None, writer=None):
    channels = [a.decode() for a in argv[1:]] if len(argv) > 1 else [""]
    out = b""
    if writer not in subscriptions:
        subscriptions[writer] = set()
    subscriber_mode.add(writer)
    for ch in channels:
        if ch not in subscriptions[writer]:
            subscriptions[writer].add(ch)
            channel_subs[ch].add(writer)
        out += resp_subscribe_reply("subscribe", ch, len(subscriptions[writer]))
    return out

async def handle_unsubscribe(argv, reader=None, writer=None):
    channels = [a.decode() for a in argv[1:]] if len(argv) > 1 else []
    out = b""
    if writer not in subscriptions:
        subscriptions[writer] = set()
    if not channels:
        channels = list(subscriptions.get(writer, [])) or [""]
    for ch in channels:
        if ch in subscriptions.get(writer, set()):
            subscriptions[writer].remove(ch)
        if writer in channel_subs.get(ch, set()):
            channel_subs[ch].remove(writer)
            if not channel_subs[ch]:
                del channel_subs[ch]
        out += resp_subscribe_reply("unsubscribe", ch, len(subscriptions.get(writer, set())))
    if len(subscriptions.get(writer, set())) == 0:
        subscriptions.pop(writer, None)
        subscriber_mode.discard(writer)
    return out

async def handle_publish(argv, reader=None, writer=None):
    ch = argv[1].decode()
    msg = argv[2].decode()
    receivers = list(channel_subs.get(ch, set()))
    count = 0
    for w in receivers:
        try:
            data = (f"*3\r\n$7\r\nmessage\r\n${len(ch)}\r\n{ch}\r\n${len(msg)}\r\n{msg}\r\n").encode()
            w.write(data)
            await w.drain()
            count += 1
        except Exception:
            pass
    return resp_int(count)

# ------------------------
# INFO/CONFIG/KEYS
# ------------------------

async def handle_info(argv, reader=None, writer=None, role='master'):
    s = f"role:{role}\nmaster_replid:000000\nmaster_repl_offset:0\n"
    return resp_bulk(s)

async def handle_config_get(argv, reader=None, writer=None, config_dir="", dbfilename="dump.rdb"):
    param = argv[2].decode() if len(argv) > 2 else ""
    if param == "dir":
        return resp_array_from_encoded([resp_bulk("dir"), resp_bulk(config_dir)])
    if param == "dbfilename":
        return resp_array_from_encoded([resp_bulk("dbfilename"), resp_bulk(dbfilename)])
    return resp_array_from_encoded([])

async def handle_keys(argv, reader=None, writer=None):
    pattern = argv[1].decode() if len(argv) > 1 else "*"
    if pattern == "*":
        ks = list(kv.keys())
    else:
        import re
        regex = pattern.replace("*", ".*")
        ks = [k for k in kv.keys() if re.match(regex, k)]
    out = f"*{len(ks)}\r\n".encode()
    for k in ks:
        out += resp_bulk(k)
    return out

# ------------------------
# Command dispatcher with MULTI/EXEC handling
# ------------------------

async def dispatch(argv, reader, writer):
    """
    argv: list where argv[0] is command (bytes or str), following are bulk args (bytes or None).
    Return RESP bytes or None if blocked.
    """
    # Normalize cmd name
    cmd_raw = argv[0]
    cmd_name = cmd_raw.decode().lower() if isinstance(cmd_raw, bytes) else str(cmd_raw).lower()

    # If subscriber mode: only SUBSCRIBE, UNSUBSCRIBE, PING are allowed
    if writer in subscriber_mode:
        if cmd_name == "subscribe":
            return await handle_subscribe(argv, reader, writer)
        if cmd_name == "unsubscribe":
            return await handle_unsubscribe(argv, reader, writer)
        if cmd_name == "ping":
            return await handle_ping(argv, reader, writer)
        # other commands not allowed
        return resp_error(f"ERR Can't execute '{cmd_name.upper()}' in this context")

    # Transaction queuing logic
    wid = writer
    if wid in multi_clients and cmd_name not in ("multi", "exec", "discard"):
        # queue the command bytes (ensure each arg is bytes)
        queued = []
        for a in argv:
            if isinstance(a, bytes):
                queued.append(a)
            elif a is None:
                queued.append(None)
            else:
                queued.append(str(a).encode())
        multi_deques[writer_id(writer)].append(queued)
        return resp_simple("QUEUED")

    # Dispatch
    try:
        if cmd_name == "ping":
            return await handle_ping(argv, reader, writer)
        if cmd_name == "echo":
            return await handle_echo(argv, reader, writer)
        if cmd_name == "set":
            return await handle_set(argv, reader, writer)
        if cmd_name == "get":
            return await handle_get(argv, reader, writer)
        if cmd_name == "incr":
            return await handle_incr(argv, reader, writer)
        if cmd_name == "rpush":
            return await handle_rpush(argv, reader, writer)
        if cmd_name == "lpush":
            return await handle_lpush(argv, reader, writer)
        if cmd_name == "llen":
            return await handle_llen(argv, reader, writer)
        if cmd_name == "lrange":
            return await handle_lrange(argv, reader, writer)
        if cmd_name == "lpop":
            return await handle_lpop(argv, reader, writer)
        if cmd_name == "blpop":
            return await handle_blpop(argv, reader, writer)
        if cmd_name == "xadd":
            return await handle_xadd(argv, reader, writer)
        if cmd_name == "xrange":
            return await handle_xrange(argv, reader, writer)
        if cmd_name == "xread":
            return await handle_xread(argv, reader, writer)
        if cmd_name == "multi":
            multi_clients.add(writer)
            multi_deques[writer_id(writer)] = deque()
            return resp_simple("OK")
        if cmd_name == "discard":
            if writer in multi_clients:
                multi_clients.discard(writer)
                multi_deques[writer_id(writer)].clear()
                return resp_simple("OK")
            return resp_error("ERR DISCARD without MULTI")
        elif decoded_data[0].upper() == "EXEC":
            if queued:
                queued = False
                print(f"EXEC queue: {queue}")

                # If no queued commands, return empty array
                if not queue or len(queue[0]) == 0:
                    connection.sendall(b"*0\r\n")
                    return [], queued

                executing = True
                result = []
                q = queue.pop(0)

                for cmd in q:
                    try:
                        output, _ = cmd_executor(cmd, connection, config, queued, executing)
                        if output is None:
                            # Default responses for commands with no explicit return
                            if cmd[0].upper() == "SET":
                                output = b"+OK\r\n"
                            else:
                                output = b":1\r\n"
                        elif isinstance(output, str):
                            output = output.encode()
                    except Exception as e:
                        # Instead of aborting, add error inside array
                        output = f"-ERR {str(e)}\r\n".encode()
                    result.append(output)

                # Build proper RESP array
                merged = f"*{len(result)}\r\n".encode()
                for r in result:
                    merged += r
                connection.sendall(merged)

                executing = False
                return [], queued
            else:
                # If EXEC called without MULTI
                connection.sendall(b"-ERR EXEC without MULTI\r\n")
                return [], queued
        if cmd_name == "subscribe":
            return await handle_subscribe(argv, reader, writer)
        if cmd_name == "unsubscribe":
            return await handle_unsubscribe(argv, reader, writer)
        if cmd_name == "publish":
            return await handle_publish(argv, reader, writer)
        if cmd_name == "info":
            return await handle_info(argv, reader, writer, role='master')
        if cmd_name == "config":
            # expect CONFIG GET <param>
            if len(argv) >= 3 and (isinstance(argv[1], bytes) and argv[1].decode().lower() == 'get'):
                return await handle_config_get(argv, reader, writer, config_dir="", dbfilename="dump.rdb")
            return resp_error("ERR")
        if cmd_name == "keys":
            return await handle_keys(argv, reader, writer)
        return resp_error(f"ERR unknown command '{cmd_name.upper()}'")
    except Exception as e:
        # For queued/exec errors we respond with EXECABORT message similar to your earlier tests
        return resp_error("EXECABORT Transaction discarded because of previous errors.") if cmd_name == 'exec' else resp_error(str(e))

# ------------------------
# Per-connection read loop
# ------------------------

async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    buffer = b""
    try:
        while True:
            data = await reader.read(BUF_SIZE)
            if not data:
                break
            buffer += data
            msgs, consumed = parse_all(buffer)
            # consume that many bytes
            buffer = buffer[consumed:]
            # msgs is list of parsed RESP items (top-level arrays etc)
            for item in msgs:
                # Only process array commands
                if not isinstance(item, list):
                    continue
                # Ensure command args are in expected form: bulk strings (bytes/None)
                # item is list of bytes/None or nested items
                res = await dispatch(item, reader, writer)
                if res is None:
                    # blocked operation: don't send any immediate response
                    continue
                try:
                    writer.write(res)
                    await writer.drain()
                except Exception:
                    pass
    except Exception:
        pass
    finally:
        # cleanup on disconnect: remove from subscriptions and multi state
        if writer in subscriptions:
            for ch in list(subscriptions[writer]):
                if writer in channel_subs.get(ch, set()):
                    channel_subs[ch].remove(writer)
                    if not channel_subs[ch]:
                        channel_subs.pop(ch, None)
            subscriptions.pop(writer, None)
        subscriber_mode.discard(writer)
        if writer in multi_clients:
            multi_clients.discard(writer)
            multi_deques.pop(writer_id(writer), None)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

# ------------------------
# Server entrypoint
# ------------------------

async def main(host="localhost", port=6379):
    server = await asyncio.start_server(handle_connection, host, port)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Async Redis clone running on {addrs}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass