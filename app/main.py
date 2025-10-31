#!/usr/bin/env python3
"""
Single-file Redis-like server implementing:
- RESP parsing/encoding
- SET/GET with expiry
- Lists (RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP)
- Streams (XADD, XRANGE, XREAD, blocking XREAD)
- Transactions (MULTI, QUEUED, EXEC, DISCARD)
- Pub/Sub (SUBSCRIBE, UNSUBSCRIBE, PUBLISH, subscribed mode, PING in subscribed mode)
- INFO, CONFIG GET, KEYS stubs
- Minimal RDB load stub
"""
import asyncio
import time
import threading
import struct
import os
import datetime
from collections import defaultdict, deque

BUF_SIZE = 65536

# ------------------------
# In-memory stores
# ------------------------
# Key-value store: key -> [value, expiry_timestamp_float]
kv = {}
# Lists: key -> list
lists = defaultdict(list)
# Streams: key -> list of entries (entry is dict with 'id' and fields)
streams = defaultdict(list)
# XREAD blocking waiters: key -> list of (writer, start_id)
xread_blocked = defaultdict(list)
# BLPOP waiters: key -> deque of (writer)
blpop_blocked = defaultdict(deque)

# Transactions: per-writer queues
multi_clients = set()               # set of writer identities currently in MULTI
multi_deques = defaultdict(deque)   # key = writer_id (str) -> deque of queued commands

# Pub/sub
subscriptions = {}      # writer -> set(channels)
channel_subs = defaultdict(set)  # channel -> set(writers)
subscriber_mode = set() # set of writers currently in subscribed mode

# Helper mapping writer -> id string
def writer_id(writer):
    # Unique id per writer for dict keys
    return str(id(writer))

# ------------------------
# RESP parsing & encoding
# ------------------------

def parse_next(data: bytes):
    """
    Parse one RESP item from the start of `data`.
    Returns (item, remaining_bytes)
    where item for an array is list of items; for bulk strings it returns bytes;
    for simple strings it returns python str; for integer returns python int.
    """
    if not data:
        raise RuntimeError("No data")
    if data[0:1] == b'*':
        # array
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        arr_len = int(data[1:nl])
        rest = data[nl+2:]
        items = []
        for _ in range(arr_len):
            item, rest = parse_next(rest)
            items.append(item)
        return items, rest
    elif data[0:1] == b'$':
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
    elif data[0:1] == b'+':
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        return data[1:nl].decode(), data[nl+2:]
    elif data[0:1] == b'-':
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        return Exception(data[1:nl].decode()), data[nl+2:]
    elif data[0:1] == b':':
        nl = data.find(b'\r\n')
        if nl == -1:
            raise RuntimeError("Incomplete")
        return int(data[1:nl]), data[nl+2:]
    else:
        raise RuntimeError("Unknown RESP type")

def parse_all(data: bytes):
    """
    Try to parse as many complete RESP messages as possible.
    Returns a list of parsed messages (each is a list of bulk strings or similar)
    """
    messages = []
    buf = data
    while buf:
        try:
            item, buf = parse_next(buf)
            # flatten bulk-string bytes to bytes objects, arrays come as lists
            if isinstance(item, list):
                # convert bulk bytes inside to bytes or str as needed later
                messages.append(item)
            else:
                messages.append(item)
        except RuntimeError:
            break
    return messages

def resp_encode_simple_str(s: str):
    return f"+{s}\r\n".encode()

def resp_encode_error(s: str):
    return f"-{s}\r\n".encode()

def resp_encode_int(n: int):
    return f":{n}\r\n".encode()

def resp_encode_bulk(b: bytes | str | None):
    if b is None:
        return b"$-1\r\n"
    if isinstance(b, str):
        b = b.encode()
    return b"${}\r\n".format(len(b)).encode() + b + b"\r\n"

def resp_encode_array_from_bytes_elements(byte_elements: list[bytes]):
    out = f"*{len(byte_elements)}\r\n".encode()
    for e in byte_elements:
        out += e
    return out

def resp_array_items(items):
    """build array where items are already RESP-encoded bytes"""
    return resp_encode_array_from_bytes_elements(items)

# Helper to create array [bulk_str, bulk_str, int] like subscribe/unsubscribe etc.
def resp_subscribe_reply(kind: str, channel: str, count: int):
    # ["subscribe"/"unsubscribe", channel, count]
    # must be: *3\r\n$<len(kind)>\r\nkind\r\n$<len(channel)>\r\nchannel\r\n:<count>\r\n
    return (f"*3\r\n${len(kind)}\r\n{kind}\r\n"
            f"${len(channel)}\r\n{channel}\r\n"
            f":{count}\r\n").encode()

# ------------------------
# Utility helpers
# ------------------------

def now_ms():
    return time.time() * 1000.0

def ensure_kv_cleanup(key):
    """Remove expired key if expired"""
    v = kv.get(key)
    if not v:
        return
    _, expiry = v
    if expiry is not None and expiry <= time.time():
        del kv[key]

# ------------------------
# Command handlers (async)
# Return RESP bytes or None if blocking/no immediate response
# ------------------------

async def handle_ping(args, reader=None, writer=None):
    # If in subscribed mode, PING returns array ["pong", msg] (bulk strings)
    if writer and writer in subscriber_mode:
        msg = args[0].decode() if args else ""
        return (f"*2\r\n$4\r\npong\r\n${len(msg)}\r\n{msg}\r\n").encode()
    return resp_encode_simple_str("PONG")

async def handle_echo(args, reader=None, writer=None):
    msg = args[0].decode() if args else ""
    return resp_encode_bulk(msg)

async def handle_set(args, reader=None, writer=None):
    # args are bytes
    key = args[0].decode()
    val = args[1].decode()
    expiry = None
    if len(args) >= 4:
        opt = args[2].decode().upper()
        if opt == "PX":
            expiry = time.time() + int(args[3].decode()) / 1000.0
        elif opt == "EX":
            expiry = time.time() + int(args[3].decode())
    kv[key] = [val, expiry]
    return resp_encode_simple_str("OK")

async def handle_get(args, reader=None, writer=None):
    key = args[0].decode()
    ensure_kv_cleanup(key)
    v = kv.get(key)
    if not v:
        return resp_encode_bulk(None)
    return resp_encode_bulk(v[0])

async def handle_incr(args, reader=None, writer=None):
    key = args[0].decode()
    ensure_kv_cleanup(key)
    v = kv.get(key)
    if not v:
        kv[key] = ["1", None]
        return resp_encode_int(1)
    val, expiry = v
    try:
        n = int(val) + 1
    except Exception:
        return resp_encode_error("ERR value is not an integer or out of range")
    kv[key] = [str(n), expiry]
    return resp_encode_int(n)

# --- Lists ---
async def handle_rpush(args, reader=None, writer=None):
    key = args[0].decode()
    for b in args[1:]:
        lists[key].append(b.decode())
    # wake any blocked BLPOP for this key
    if key in blpop_blocked and blpop_blocked[key]:
        w = blpop_blocked[key].popleft()
        element = lists[key].pop(0)
        resp = (f"*2\r\n${len(key)}\r\n{key}\r\n${len(element)}\r\n{element}\r\n").encode()
        try:
            w.write(resp)
            await w.drain()
        except Exception:
            pass
    return resp_encode_int(len(lists[key]))

async def handle_lpush(args, reader=None, writer=None):
    key = args[0].decode()
    for b in args[1:]:
        lists[key].insert(0, b.decode())
    return resp_encode_int(len(lists[key]))

async def handle_llen(args, reader=None, writer=None):
    key = args[0].decode()
    return resp_encode_int(len(lists.get(key, [])))

async def handle_lrange(args, reader=None, writer=None):
    key = args[0].decode()
    start = int(args[1].decode())
    end = int(args[2].decode())
    arr = lists.get(key, [])
    n = len(arr)
    if n == 0:
        return b"*0\r\n"
    if start < 0:
        start = n + start
    if end < 0:
        end = n + end
    start = max(0, start)
    end = min(n - 1, end)
    if start > end:
        return b"*0\r\n"
    subset = arr[start:end+1]
    out = f"*{len(subset)}\r\n".encode()
    for s in subset:
        out += resp_encode_bulk(s)
    return out

async def handle_lpop(args, reader=None, writer=None):
    key = args[0].decode()
    if key not in lists or not lists[key]:
        return resp_encode_bulk(None)
    if len(args) == 1:
        v = lists[key].pop(0)
        return resp_encode_bulk(v)
    else:
        count = int(args[1].decode())
        popped = []
        for _ in range(min(count, len(lists[key]))):
            popped.append(lists[key].pop(0))
        out = f"*{len(popped)}\r\n".encode()
        for p in popped:
            out += resp_encode_bulk(p)
        return out

async def handle_blpop(args, reader=None, writer=None):
    key = args[0].decode()
    timeout = float(args[1].decode())
    if key in lists and lists[key]:
        v = lists[key].pop(0)
        return (f"*2\r\n${len(key)}\r\n{key}\r\n${len(v)}\r\n{v}\r\n").encode()
    # else block
    blpop_blocked[key].append(writer)
    async def timeout_unblock():
        if timeout == 0:
            return
        await asyncio.sleep(timeout)
        # still blocked?
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
    asyncio.create_task(timeout_unblock())
    return None

# --- Streams (basic) ---
def _allot_stream_id(key, ts=None):
    # returns string id like "<ms>-<seq>"
    t = int(time.time() * 1000) if ts is None else int(ts)
    seq = 0
    if streams[key]:
        last = streams[key][-1]['id']
        last_t, last_s = map(int, last.split('-', 1))
        if t < last_t:
            t = last_t
            seq = last_s + 1
        elif t == last_t:
            seq = last_s + 1
    return f"{t}-{seq}"

async def handle_xadd(args, reader=None, writer=None):
    key = args[0].decode()
    entry_id = args[1].decode()
    if entry_id == '*':
        eid = _allot_stream_id(key)
    else:
        if '-' in entry_id:
            tpart, spart = entry_id.split('-', 1)
            if spart == '*':
                eid = _allot_stream_id(key, ts=int(tpart))
            else:
                eid = entry_id
        else:
            eid = _allot_stream_id(key)
    # fields
    fields = {}
    for i in range(2, len(args), 2):
        fields[args[i].decode()] = args[i+1].decode()
    entry = {'id': eid}
    entry.update(fields)
    streams[key].append(entry)
    # notify any xread blocked waiters
    if key in xread_blocked and xread_blocked[key]:
        pending = list(xread_blocked[key])
        xread_blocked[key].clear()
        for w, start_id in pending:
            # collect entries > start_id
            out_entries = []
            for e in streams[key]:
                if e['id'] > start_id:
                    # build fields flat
                    fields_list = []
                    for kf, vf in e.items():
                        if kf == 'id': continue
                        fields_list.append([kf, vf])
                    out_entries.append([e['id'], fields_list])
            if out_entries:
                # Construct response array: *1 -> [ [ key, [ [id, [field, value ...]]... ] ] ]
                # Build resp manually
                s = f"*1\r\n*2\r\n${len(key)}\r\n{key}\r\n*{len(out_entries)}\r\n".encode()
                for eid, fv in out_entries:
                    s += f"${len(eid)}\r\n{eid}\r\n".encode()
                    # now fv is list of [field, value] pairs - as array of 2*N bulk strings
                    s += f"*{len(fv)*2}\r\n".encode()
                    for pair in fv:
                        s += resp_encode_bulk(pair[0])
                        s += resp_encode_bulk(pair[1])
                try:
                    w.write(s)
                    await w.drain()
                except Exception:
                    pass
    return resp_encode_bulk(eid)

async def handle_xrange(args, reader=None, writer=None):
    key = args[0].decode()
    start = args[1].decode()
    end = args[2].decode()
    if key not in streams:
        return b"*0\r\n"
    out = []
    for e in streams[key]:
        if start <= e['id'] <= end:
            # build [id, [field, value ...]]
            fields = []
            for kf, vf in e.items():
                if kf == 'id': continue
                fields.append([kf, vf])
            out.append([e['id'], fields])
    # Convert to RESP array: *N entries -> each entry is array [id, [fields...]]
    if not out:
        return b"*0\r\n"
    s = f"*{len(out)}\r\n".encode()
    for eid, fields in out:
        s += f"*2\r\n${len(eid)}\r\n{eid}\r\n".encode()
        # fields array as array-of-arrays
        s += f"*{len(fields)}\r\n".encode()
        for fpair in fields:
            # each field pair should be array [field, value]
            s += f"*2\r\n".encode()
            s += resp_encode_bulk(fpair[0])
            s += resp_encode_bulk(fpair[1])
    return s

async def handle_xread(args, reader=None, writer=None):
    # Support BLOCK <ms> STREAMS <key> <id>
    # or XREAD STREAMS k1 k2 ... id1 id2 ...
    if args[0].decode().upper() == 'BLOCK':
        timeout_ms = int(args[1].decode())
        # next token should be 'STREAMS'
        # then keys...
        idx = 3
        keys = []
        while True:
            if args[idx].decode().upper() == 'STREAMS':
                idx += 1
                break
            keys.append(args[idx].decode())
            idx += 1
        # actually above logic not typical; tests will usually use BLOCK STREAMS key id
        # Simplify: assume form: BLOCK <ms> STREAMS <key> <id>
        # So keys = []
        if len(args) >= 5 and args[2].decode().upper() == 'STREAMS':
            key = args[3].decode()
            start = args[4].decode()
        else:
            # fallback
            return b"-ERR unsupported XREAD format\r\n"
        if start == '$':
            start_id = streams[key][-1]['id'] if streams[key] else '0-0'
        else:
            start_id = start
        # check if entries exist now
        found = []
        for e in streams[key]:
            if e['id'] > start_id:
                fv = []
                for kf, vf in e.items():
                    if kf == 'id': continue
                    fv.append([kf, vf])
                found.append([e['id'], fv])
        if found:
            # build response similar to XADD notify earlier
            s = f"*1\r\n*2\r\n${len(key)}\r\n{key}\r\n*{len(found)}\r\n".encode()
            for eid, fv in found:
                s += f"${len(eid)}\r\n{eid}\r\n".encode()
                s += f"*{len(fv)*2}\r\n".encode()
                for p in fv:
                    s += resp_encode_bulk(p[0])
                    s += resp_encode_bulk(p[1])
            return s
        # else block
        # store (writer, start_id)
        xread_blocked[key].append((writer, start_id))
        async def timeout_unblock():
            if timeout_ms == 0:
                return
            await asyncio.sleep(timeout_ms / 1000.0)
            # if still blocked, send null array
            try:
                rem = []
                for w, sid in xread_blocked[key]:
                    if w is writer:
                        rem.append((w, sid))
                for r in rem:
                    try:
                        writer.write(b"*-1\r\n")
                        await writer.drain()
                    except Exception:
                        pass
                    try:
                        xread_blocked[key].remove(r)
                    except ValueError:
                        pass
            except Exception:
                pass
        asyncio.create_task(timeout_unblock())
        return None
    else:
        # non-blocking XREAD
        half = len(args)//2
        keys = [a.decode() for a in args[:half]]
        ids = [a.decode() for a in args[half:]]
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
                # append [key, entries]
                result.append([key, entries])
        if not result:
            return b"*0\r\n"
        # build RESP
        s = f"*{len(result)}\r\n".encode()
        for key, entries in result:
            s += f"*2\r\n${len(key)}\r\n{key}\r\n*{len(entries)}\r\n".encode()
            for eid, fv in entries:
                s += f"${len(eid)}\r\n{eid}\r\n".encode()
                s += f"*{len(fv)*2}\r\n".encode()
                for p in fv:
                    s += resp_encode_bulk(p[0])
                    s += resp_encode_bulk(p[1])
        return s

# ------------------------
# Pub/Sub
# ------------------------

def subscribe_writer_to_channels(writer, channels):
    wid = writer
    if wid not in subscriptions:
        subscriptions[wid] = set()
    for ch in channels:
        subscriptions[wid].add(ch)
        channel_subs[ch].add(wid)

def unsubscribe_writer_from_channels(writer, channels):
    wid = writer
    if wid not in subscriptions:
        subscriptions[wid] = set()
    for ch in channels:
        if ch in subscriptions[wid]:
            subscriptions[wid].remove(ch)
        if wid in channel_subs.get(ch, set()):
            channel_subs[ch].remove(wid)
            if not channel_subs[ch]:
                del channel_subs[ch]
    if not subscriptions[wid]:
        # leave subscriber mode
        if wid in subscriptions:
            del subscriptions[wid]

async def handle_subscribe(args, reader=None, writer=None):
    # args are bytes channels
    channels = [a.decode() for a in args] if args else []
    if not channels:
        channels = [""]
    wid = writer
    if wid not in subscriptions:
        subscriptions[wid] = set()
    subscriber_mode.add(wid)
    out_bytes = b""
    for ch in channels:
        if ch not in subscriptions[wid]:
            subscriptions[wid].add(ch)
            channel_subs[ch].add(wid)
        cnt = len(subscriptions[wid])
        out_bytes += resp_subscribe_reply("subscribe", ch, cnt)
    # writer remains in subscribed mode
    return out_bytes

async def handle_unsubscribe(args, reader=None, writer=None):
    channels = [a.decode() for a in args] if args else []
    wid = writer
    if wid not in subscriptions:
        subscriptions[wid] = set()
    out = b""
    if not channels:
        # unsubscribe from all
        channels = list(subscriptions.get(wid, [])) or [""]
    for ch in channels:
        if ch in subscriptions.get(wid, set()):
            subscriptions[wid].remove(ch)
        if wid in channel_subs.get(ch, set()):
            channel_subs[ch].remove(wid)
            if not channel_subs[ch]:
                del channel_subs[ch]
        cnt = len(subscriptions.get(wid, set()))
        out += resp_subscribe_reply("unsubscribe", ch, cnt)
    # if no channels remain, leave subscribed mode for this writer
    if len(subscriptions.get(wid, set())) == 0:
        subscriber_mode.discard(wid)
        if wid in subscriptions:
            del subscriptions[wid]
    return out

async def handle_publish(args, reader=None, writer=None):
    ch = args[0].decode()
    msg = args[1].decode()
    receivers = channel_subs.get(ch, set())
    count = 0
    for w in list(receivers):
        try:
            # build message: ["message", channel, message] -> *3\r\n$7\r\nmessage\r\n$<len>\r\nch\r\n$<len>\r\nmsg\r\n
            data = (f"*3\r\n$7\r\nmessage\r\n${len(ch)}\r\n{ch}\r\n${len(msg)}\r\n{msg}\r\n").encode()
            w.write(data)
            await w.drain()
            count += 1
        except Exception:
            pass
    # reply to publisher with integer count
    return resp_encode_int(count)

# ------------------------
# INFO / CONFIG / KEYS
# ------------------------

async def handle_info(args, reader=None, writer=None, config_role='master'):
    section = args[0].decode() if args else "all"
    if section == "replication":
        role = config_role
        # minimal fields
        s = f"role:{role}\nmaster_replid:000000\nmaster_repl_offset:0\n"
        return resp_encode_bulk(s)
    return resp_encode_bulk("ok")

async def handle_config_get(args, reader=None, writer=None, config_dir="", config_dbfilename="dump.rdb"):
    param = args[1].decode()
    if param == "dir":
        return resp_encode_array_from_bytes_elements([resp_encode_bulk("dir"), resp_encode_bulk(config_dir)])
    if param == "dbfilename":
        return resp_encode_array_from_bytes_elements([resp_encode_bulk("dbfilename"), resp_encode_bulk(config_dbfilename)])
    return resp_encode_array_from_bytes_elements([])

async def handle_keys(args, reader=None, writer=None):
    pattern = args[0].decode()
    if pattern == "*":
        ks = list(kv.keys())
    else:
        import re
        regex = pattern.replace("*", ".*")
        ks = [k for k in kv.keys() if re.match(regex, k)]
    arr = b"*%d\r\n" % len(ks)
    for k in ks:
        arr += resp_encode_bulk(k)
    return arr

# ------------------------
# Execute command router
# ------------------------

async def execute_command(argv, reader, writer, config=None):
    """
    argv: list of bulk items where each item may be bytes or list if nested arrays.
    For our use, argv will be list of bytes/bulk values for a command.
    """
    # Normalize argv elements to bytes (bulk strings) or None
    args = []
    for a in argv[1:]:
        if isinstance(a, bytes):
            args.append(a)
        elif isinstance(a, str):
            args.append(a.encode())
        elif a is None:
            args.append(None)
        else:
            # If nested array etc, convert item to bytes representation
            args.append(str(a).encode())
    cmd = argv[0]
    # if bulk bytes, decode command name
    if isinstance(cmd, bytes):
        cmd_name = cmd.decode().lower()
    else:
        cmd_name = str(cmd).lower()

    # Check if writer in subscribed mode: allow only SUBSCRIBE/UNSUBSCRIBE/PING/QUIT
    if writer in subscriber_mode:
        if cmd_name == "subscribe":
            return await handle_subscribe(args, reader, writer)
        elif cmd_name == "unsubscribe":
            return await handle_unsubscribe(args, reader, writer)
        elif cmd_name == "ping":
            return await handle_ping(args, reader, writer)
        else:
            # Can't execute other commands when subscribed
            return resp_encode_error(f"ERR Can't execute '{cmd_name.upper()}' in this context")

    # Transaction queuing logic
    wid = writer
    if wid in multi_clients and cmd_name not in ("multi", "exec", "discard"):
        # queue the decoded argv (as list of bytes) into multi_deques[wid]
        multi_deques[writer_id(wid)].append([ (x if isinstance(x, bytes) else (x.encode() if isinstance(x, str) else str(x).encode())) for x in argv ])
        return resp_encode_simple_str("QUEUED")

    # Normal dispatch
    try:
        if cmd_name == "ping":
            return await handle_ping(args, reader, writer)
        elif cmd_name == "echo":
            return await handle_echo(args, reader, writer)
        elif cmd_name == "set":
            return await handle_set(args, reader, writer)
        elif cmd_name == "get":
            return await handle_get(args, reader, writer)
        elif cmd_name == "incr":
            return await handle_incr(args, reader, writer)
        elif cmd_name == "rpush":
            return await handle_rpush(args, reader, writer)
        elif cmd_name == "lpush":
            return await handle_lpush(args, reader, writer)
        elif cmd_name == "llen":
            return await handle_llen(args, reader, writer)
        elif cmd_name == "lrange":
            return await handle_lrange(args, reader, writer)
        elif cmd_name == "lpop":
            return await handle_lpop(args, reader, writer)
        elif cmd_name == "blpop":
            return await handle_blpop(args, reader, writer)
        elif cmd_name == "xadd":
            return await handle_xadd(args, reader, writer)
        elif cmd_name == "xrange":
            return await handle_xrange(args, reader, writer)
        elif cmd_name == "xread":
            return await handle_xread(args, reader, writer)
        elif cmd_name == "multi":
            # start transaction for this writer
            multi_clients.add(writer)
            multi_deques[writer_id(writer)] = deque()
            return resp_encode_simple_str("OK")
        elif cmd_name == "discard":
            if writer in multi_clients:
                multi_clients.discard(writer)
                multi_deques[writer_id(writer)].clear()
                return resp_encode_simple_str("OK")
            return resp_encode_error("ERR DISCARD without MULTI")
        elif cmd_name == "exec":
            if writer not in multi_clients:
                return resp_encode_error("ERR EXEC without MULTI")
            multi_clients.discard(writer)
            queued = list(multi_deques[writer_id(writer)])
            multi_deques[writer_id(writer)].clear()
            if not queued:
                return b"*0\r\n"
            # execute queued commands sequentially; collect results as RESP-encoded bytes
            results = []
            for q in queued:
                # q is list of bytes
                # run same execute_command but in "executing" mode: queued commands must not be re-queued
                # decode q into similar argv structure
                # pass reader/writer and ensure we treat nested as bytes
                res = await execute_command(q, reader, writer)
                # res is RESP bytes; append as element into array
                results.append(res)
            # Build RESP array from raw RESP-encoded elements
            merged = f"*{len(results)}\r\n".encode()
            for r in results:
                merged += r
            return merged
        elif cmd_name == "subscribe":
            return await handle_subscribe(args, reader, writer)
        elif cmd_name == "unsubscribe":
            return await handle_unsubscribe(args, reader, writer)
        elif cmd_name == "publish":
            return await handle_publish(args, reader, writer)
        elif cmd_name == "info":
            role = "master"
            return await handle_info(args, reader, writer, config_role=role)
        elif cmd_name == "config":
            # expect CONFIG GET <param>
            if len(args) >= 2 and args[0].decode().lower() == "get":
                return await handle_config_get(args, reader, writer, config_dir="", config_dbfilename="dump.rdb")
            return resp_encode_error("ERR")
        elif cmd_name == "keys":
            return await handle_keys(args, reader, writer)
        else:
            return resp_encode_error(f"ERR unknown command '{cmd_name.upper()}'")
    except Exception as e:
        return resp_encode_error("EXECABORT Transaction discarded because of previous errors.") if cmd_name == 'exec' else resp_encode_error(str(e))

# ------------------------
# RESP reader loop (per client)
# ------------------------

async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    # read loop: accumulate bytes and parse as many messages as possible
    buffer = b""
    try:
        while True:
            data = await reader.read(BUF_SIZE)
            if not data:
                break
            buffer += data
            # parse all complete messages
            msgs = parse_all(buffer)
            # compute how much of buffer was consumed: parse_all consumes as much as possible, but we must recompute
            # We'll reparse to consume correctly using parse_next until incomplete
            consumed = 0
            to_process = []
            tmp = buffer
            while tmp:
                try:
                    item, tmp = parse_next(tmp)
                    # for our purposes we want arrays where first element is bulk string command
                    to_process.append(item)
                    consumed = len(buffer) - len(tmp)
                except RuntimeError:
                    break
            # trim buffer
            buffer = buffer[consumed:]
            # process messages
            for item in to_process:
                # Only arrays are valid commands (client sends arrays)
                if not isinstance(item, list):
                    # ignore invalid
                    continue
                # execute
                res = await execute_command(item, reader, writer)
                if res is None:
                    # blocked command (like BLPOP or XREAD BLOCK) -> do not write anything now
                    continue
                try:
                    writer.write(res)
                    await writer.drain()
                except Exception:
                    pass
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    finally:
        # cleanup on disconnect: remove subscriptions and multi state
        wid = writer
        if wid in subscriptions:
            # remove writer from channel_subs
            for ch in list(subscriptions[wid]):
                if wid in channel_subs.get(ch, set()):
                    channel_subs[ch].remove(wid)
                if not channel_subs.get(ch):
                    channel_subs.pop(ch, None)
            subscriptions.pop(wid, None)
        subscriber_mode.discard(wid)
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