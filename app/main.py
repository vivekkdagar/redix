#!/usr/bin/env python3
# main.py — single-file merged server (no local imports)

import socket
import threading
import time
import struct
import os
import datetime
import argparse
from time import sleep

# ---------------------------
# Global data / stores
# ---------------------------
# Generic key/value store (strings)
store = {}               # key -> value or (value, expiry)
# List store for list commands
store_list = {}          # key -> list
# Streams store
streams = {}             # key -> list of entries (dicts with "id" and fields)
# Blocked clients for BLPOP: key -> list of connections
blocked = {}
# Blocked xread: key -> list of (connection, last_id)
blocked_xread = {}
# Replicas list for replication propagation
REPLICAS = []
# Subscription tracking (per-connection)
subscriptions = {}       # connection -> set(channels)
# Transaction queue per connection id
connections_tx = {}      # conn_id -> {'in_transaction': bool, 'commands': []}
# Misc global counters
BYTES_READ = 0
replica_acks = 0
prev_cmd = ""
SUBSCRIBE_MODE = set()   # set of connections that have entered subscribed mode

# RDB placeholder hex used in some PSYNC responses in your snippets
RDB_HEX = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'

# ---------------------------
# RESP helpers (parser + encoders)
# ---------------------------

def resp_encoder(data):
    """Encode Python value(s) to RESP bytes.
       Accepts: None, list, str, int.
    """
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, list):
        out = f"*{len(data)}\r\n".encode()
        for item in data:
            out += resp_encoder(item)
        return out
    if isinstance(data, bytes):
        return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"
    if isinstance(data, str):
        s = data
        return f"${len(s)}\r\n{s}\r\n".encode()
    if isinstance(data, int):
        return f":{data}\r\n".encode()
    # fallback
    return b"$-1\r\n"

def simple_string_encoder(message):
    return f"+{message}\r\n".encode()

def error_encoder(message):
    return f"-{message}\r\n".encode()

def array_encoder(list_of_resp_bytes):
    """Wrap pre-encoded RESP values into an array response."""
    merged = f"*{len(list_of_resp_bytes)}\r\n".encode()
    for r in list_of_resp_bytes:
        merged += r
    return merged

# RESP parsing: parse as many messages as possible from buffer
def parse_all(data: bytes):
    messages = []
    buffer = data
    while buffer:
        try:
            msg, buffer = parse_next(buffer)
            messages.append(msg)
        except Exception:
            break
    return messages

def parse_next(data: bytes):
    """Return (message, remaining_buffer).
       message is a list of decoded elements (bytes or str depending).
       This implements minimal RESP array/bulk parsing expected from redis-cli.
    """
    if not data:
        raise RuntimeError("No data")

    # find line break for header
    idx = data.find(b"\r\n")
    if idx == -1:
        raise RuntimeError("Partial")
    first = data[:idx]
    rest = data[idx+2:]

    if first.startswith(b"*"):
        # array
        cnt = int(first[1:].decode())
        items = []
        cur = rest
        for _ in range(cnt):
            item, cur = parse_next(cur)
            items.append(item)
        return items, cur

    if first.startswith(b"$"):
        length = int(first[1:].decode())
        if length == -1:
            return None, rest
        if len(rest) < length + 2:
            raise RuntimeError("Partial")
        blk = rest[:length]
        # ensure CRLF after block
        if rest[length:length+2] != b"\r\n":
            # strict parser would error; attempt to continue gracefully
            return blk, rest[length+2:]
        return blk, rest[length+2:]

    if first.startswith(b"+"):
        return first[1:].decode(), rest

    if first.startswith(b":"):
        return int(first[1:].decode()), rest

    if first.startswith(b"-"):
        return first[1:].decode(), rest

    raise RuntimeError("Unknown RESP type")

# Utility to convert parsed message elements (bytes) to text strings for command handling
def normalize_parsed(msg):
    # msg is list of elements (bytes or None or str)
    out = []
    for e in msg:
        if e is None:
            out.append(None)
        elif isinstance(e, (bytes, bytearray)):
            try:
                out.append(e.decode())
            except Exception:
                out.append(e.decode('utf-8','ignore'))
        else:
            out.append(e)
    return out

# ---------------------------
# RDB reading helpers (simplified)
# ---------------------------
# This is a minimal RDB reader based on your snippets. It reads some simple
# grader-produced RDB files with string keys and optional expiries (0xFC/0xFD).

class RDBParserMinimal:
    HEADER_MAGIC = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31"  # "REDIS0011"
    META_START = 0xFA
    EOF = 0xFF
    DB_START = 0xFE
    HASH_START = 0xFB
    STRING_START = 0x00
    EXPIRE_TIME = 0xFD
    EXPIRE_TIME_MS = 0xFC

    def __init__(self, path):
        self.path = path
        self.content = None
        self.pointer = 0
        try:
            with open(path, "rb") as f:
                self.content = f.read()
        except FileNotFoundError:
            self.content = None

    def _read(self, length):
        if self.content is None:
            raise EOFError("No content")
        if self.pointer + length > len(self.content):
            raise EOFError("Unexpected EOF")
        data = self.content[self.pointer:self.pointer+length]
        self.pointer += length
        return data

    def _read_byte(self):
        return self._read(1)[0]

    def _read_length(self):
        first = self._read_byte()
        prefix = (first & 0xC0) >> 6
        if prefix == 0:
            return first & 0x3F
        elif prefix == 1:
            b2 = self._read_byte()
            return ((first & 0x3F) << 8) | b2
        elif prefix == 2:
            b4 = self._read(4)
            return int.from_bytes(b4, 'little')
        else:
            return first

    def _read_string(self):
        length = self._read_length()
        raw = self._read(length)
        try:
            return raw.decode('utf-8', errors='ignore')
        except Exception:
            return raw.decode('utf-8', errors='ignore')

    def parse(self):
        out = {}
        if not self.content:
            return out
        # check header
        magic = self._read(len(self.HEADER_MAGIC))
        if magic != self.HEADER_MAGIC:
            # not a supported RDB in this minimal parser
            return out
        pending_expiry = None
        try:
            while self.pointer < len(self.content):
                op = self._read_byte()
                if op == self.META_START:
                    # aux key and value
                    _ = self._read_string()
                    _ = self._read_string()
                elif op == self.DB_START:
                    _ = self._read_length()
                elif op == self.HASH_START:
                    # skip two lengths
                    _ = self._read_length()
                    _ = self._read_length()
                elif op == self.EXPIRE_TIME:
                    b4 = self._read(4)
                    expiry_s = int.from_bytes(b4, 'little')
                    pending_expiry = expiry_s
                elif op == self.EXPIRE_TIME_MS:
                    b8 = self._read(8)
                    expiry_ms = int.from_bytes(b8, 'little')
                    pending_expiry = expiry_ms / 1000.0
                elif op == self.EOF:
                    break
                else:
                    # treat as string/object: read key + value
                    # in some test dumps opcode may be string-type or other, attempt to read strings
                    # We try to treat op as a type code and read two strings
                    # Move pointer back by 1 because we consumed one byte which might be part of length encoding.
                    self.pointer -= 1
                    key = self._read_string()
                    val = self._read_string()
                    if key is not None:
                        if pending_expiry is None:
                            out[key] = (val, -1)
                        else:
                            # pending_expiry may be seconds or epoch seconds; treat if > current time it's valid
                            now = time.time()
                            # if it's bigger than now (expected epoch seconds), accept, else skip
                            if pending_expiry > now:
                                out[key] = (val, pending_expiry)
                            else:
                                # expired: skip
                                pass
                    pending_expiry = None
        except EOFError:
            pass
        return out

def read_key_val_from_db(dir, dbfilename, data_out):
    path = os.path.join(dir, dbfilename)
    if not os.path.isfile(path):
        return
    parser = RDBParserMinimal(path)
    parsed = parser.parse()
    for k, v in parsed.items():
        data_out[k] = v

# ---------------------------
# Utility functions (lists / streams / keys / etc)
# ---------------------------

def store_rdb(info):
    global store
    store = info

def expire_key_thread(key, expire_time):
    time.sleep(expire_time)
    if key in store:
        del store[key]

def keys_list():
    return list(store.keys())

def setter(args):
    # args: [key, value] or [key, value, "PX", ms]
    if len(args) >= 2:
        key, value = args[0], args[1]
        if len(args) >= 4 and isinstance(args[2], str) and args[2].upper() == "PX":
            try:
                ms = int(args[3])
                store[key] = (value, time.time() + ms / 1000.0)
                threading.Thread(target=expire_key_thread, args=(key, ms / 1000.0), daemon=True).start()
            except Exception:
                store[key] = value
        else:
            store[key] = value

def getter(key):
    v = store.get(key)
    if isinstance(v, tuple):
        return v[0]
    return v

def rpush(args, blocked_map):
    # args: [key, v1, v2, ...]
    key = args[0]
    vals = args[1:]
    if key not in store_list:
        store_list[key] = []
    store_list[key].extend(vals)
    # If there are blocked BLPOP clients, deliver immediately in FIFO order
    while key in blocked_map and blocked_map[key] and store_list[key]:
        conn = blocked_map[key].pop(0)
        val = store_list[key].pop(0)
        try:
            conn.sendall(resp_encoder([key, val]))
        except Exception:
            pass
    return len(store_list[key])

def lrange(args):
    # args: [key, start, stop]
    key = args[0]
    start = int(args[1])
    stop = int(args[2])
    if key not in store_list:
        return []
    lst = store_list[key]
    n = len(lst)
    if start < 0:
        start = n + start
    if stop < 0:
        stop = n + stop
    start = max(0, start)
    stop = min(n-1, stop)
    if start > stop:
        return []
    return lst[start:stop+1]

def lpush(args):
    # args: [key, v1, v2...]
    key = args[0]
    vals = args[1:]
    if key not in store_list:
        store_list[key] = []
    # LPUSH inserts left-to-right so first element becomes leftmost
    for v in vals:
        store_list[key].insert(0, v)
    return len(store_list[key])

def llen(key):
    return len(store_list.get(key, []))

def lpop(args):
    # args: [key] or [key, count]
    key = args[0]
    if key not in store_list or not store_list[key]:
        return None if len(args) == 1 else []
    if len(args) == 1:
        val = store_list[key].pop(0)
        if not store_list[key]:
            del store_list[key]
        return val
    else:
        cnt = int(args[1])
        popped = []
        for _ in range(min(cnt, len(store_list[key]))):
            popped.append(store_list[key].pop(0))
        if not store_list[key]:
            del store_list[key]
        return popped

def blpop(args, connection, blocked_map):
    # args: [key, timeout]
    key = args[0]
    timeout = float(args[1])
    if key in store_list and store_list[key]:
        return [key, store_list[key].pop(0)]
    # else block
    if key not in blocked_map:
        blocked_map[key] = []
    blocked_map[key].append(connection)
    if timeout > 0:
        # start timer to send null after timeout
        def unblock():
            time.sleep(timeout)
            if key in blocked_map and connection in blocked_map[key]:
                try:
                    connection.sendall(b"*-1\r\n")
                except Exception:
                    pass
                blocked_map[key].remove(connection)
                if not blocked_map[key]:
                    del blocked_map[key]
        threading.Thread(target=unblock, daemon=True).start()
    return None

# Streams helpers
def allot_stream_id(key, requested_ts):
    if key in streams and streams[key]:
        last_id = streams[key][-1]["id"]
        last_ts, last_seq = map(int, last_id.split("-"))
        if int(requested_ts) < last_ts:
            new_time = last_ts
            new_seq = last_seq + 1
        elif int(requested_ts) == last_ts:
            new_time = last_ts
            new_seq = last_seq + 1
        else:
            new_time = int(requested_ts)
            new_seq = 0
    else:
        new_time = int(requested_ts)
        new_seq = 0 if new_time > 0 else 1
    return f"{new_time}-{new_seq}"

def xadd(args, blocked_xread_map):
    # args: [key, id, field, val, ...]
    key = args[0]
    id = args[1]
    if id != '*':
        ts, seq = id.split("-")
        if seq == '*':
            id = allot_stream_id(key, ts)
        else:
            # validate increasing
            if key in streams and streams[key]:
                last_id = streams[key][-1]["id"]
                last_ts, last_seq = map(int, last_id.split("-"))
                if int(ts) < last_ts or (int(ts) == last_ts and int(seq) <= last_seq):
                    return ("err", "ERR The ID specified in XADD is equal or smaller than the target stream top item")
    else:
        ms = int(time.time() * 1000)
        id = allot_stream_id(key, ms)
    fields = args[2:]
    entry = {"id": id}
    for i in range(0, len(fields), 2):
        if i+1 < len(fields):
            entry[fields[i]] = fields[i+1]
    streams.setdefault(key, []).append(entry)
    # wake blocked xread for this key if any
    if key in blocked_xread_map and blocked_xread_map[key]:
        pending = blocked_xread_map[key][:]
        blocked_xread_map[key] = []
        for conn, last_id in pending:
            # produce results newer than last_id
            res = []
            for e in streams[key]:
                if e["id"] > last_id:
                    # result format per your code: [[key, [[id, [field, val, ...]]]]]
                    inner = [e["id"]]
                    fieldslist = []
                    for kkk, vvv in e.items():
                        if kkk != "id":
                            fieldslist.append([kkk, vvv])
                    inner2 = [inner]  # list containing single entry
                    res.append([key, inner2])
            try:
                if res:
                    conn.sendall(resp_encoder(res))
            except Exception:
                pass
    return ("id", id)

def xrange_cmd(args):
    key = args[0]
    start = args[1]
    end = args[2]
    if key not in streams:
        return []
    # normalize start/end
    if start == '-':
        start = streams[key][0]["id"]
    if end == '+':
        end = streams[key][-1]["id"]
    res = []
    for e in streams[key]:
        if e["id"] >= start and e["id"] <= end:
            fields = []
            for kkk, vvv in e.items():
                if kkk != "id":
                    fields.append([kkk, vvv])
            res.append([e["id"], fields])
    return res

def xread(args):
    # args: keys..., ids...
    n = len(args)//2
    keys = args[:n]
    ids = args[n:]
    res = []
    for key, id in zip(keys, ids):
        if key not in streams:
            continue
        last_id = id
        if '-' not in last_id:
            last_id += '-0'
        entries = []
        for e in streams[key]:
            if e["id"] > last_id:
                fields = []
                for kkk, vvv in e.items():
                    if kkk != "id":
                        fields.append([kkk, vvv])
                entries.append([e["id"], fields])
        if entries:
            res.append([key, entries])
    return res

def blocks_xread(args, connection, blocked_xread_map):
    # args: [block, COUNT? ...] simplified per your snippet usage
    # fallback to blocking mechanics: args structured in your previous functions.
    # We'll support: BLOCK <ms> STREAMS <key> <id>
    try:
        # if args[0] is block ms
        timeout_ms = int(args[0])
        timeout = timeout_ms / 1000.0
        key = args[2]
        id = args[3]
    except Exception:
        return None
    if key not in streams:
        last_id = "0-0"
    else:
        last_id = streams[key][-1]["id"]
    if key not in blocked_xread_map:
        blocked_xread_map[key] = []
    blocked_xread_map[key].append((connection, id))
    # spawn timer
    if timeout > 0:
        def to_unblock():
            time.sleep(timeout)
            if key in blocked_xread_map:
                for conn, _ in blocked_xread_map[key][:]:
                    if conn == connection:
                        try:
                            conn.sendall(b"*-1\r\n")
                        except Exception:
                            pass
                        blocked_xread_map[key].remove((conn, _))
                        break
                if not blocked_xread_map[key]:
                    del blocked_xread_map[key]
        threading.Thread(target=to_unblock, daemon=True).start()
    return None

def type_getter_lists(key):
    if key in store:
        return "string"
    if key in store_list:
        return "list"
    return "none"

def type_getter_streams(key):
    if key in streams:
        return "stream"
    return "none"

# ---------------------------
# Command executor
# ---------------------------

def cmd_executor(decoded_data, connection, config, queued, executing):
    """
    decoded_data: list of strings (command + args)
    connection: socket
    config: config dict for this server (role, etc)
    queued: boolean — currently queuing transaction commands
    executing: boolean — currently executing queued commands
    Returns: (response_or_empty, queued_boolean)
    """
    global BYTES_READ, replica_acks, prev_cmd, SUBSCRIBE_MODE, subscriptions

    if not decoded_data:
        return (b"", queued)

    cmd = decoded_data[0].upper()

    # If connection is in subscribed mode, limit allowed commands
    if connection in SUBSCRIBE_MODE:
        # allowed: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT
        if cmd == "SUBSCRIBE":
            ch = decoded_data[1]
            subscriptions.setdefault(connection, set())
            if ch not in subscriptions[connection]:
                subscriptions[connection].add(ch)
            # reply ["subscribe", channel, count]
            response = resp_encoder(["subscribe", ch, len(subscriptions[connection])])
            connection.sendall(response)
            return (b"", queued)
        if cmd == "UNSUBSCRIBE":
            ch = decoded_data[1] if len(decoded_data) > 1 else None
            if ch and connection in subscriptions and ch in subscriptions[connection]:
                subscriptions[connection].remove(ch)
            # reply generic unsubscribed array
            response = resp_encoder(["unsubscribe", ch, len(subscriptions.get(connection, []))])
            connection.sendall(response)
            return (b"", queued)
        if cmd == "PING":
            # In subscribed mode PING can have special behavior — we return ["pong",""] per your snippet
            response = resp_encoder(["pong", ""])
            connection.sendall(response)
            return (b"", queued)
        if cmd in ("PSUBSCRIBE", "PUNSUBSCRIBE", "QUIT"):
            # minimal handling
            return (b"", queued)
        # not allowed
        connection.sendall(error_encoder(f"ERR Can't execute '{decoded_data[0]}'"))
        return (b"", queued)

    # Transaction queueing
    conn_id = id(connection)
    if conn_id in connections_tx and connections_tx[conn_id].get('in_transaction', False):
        # if it's not EXEC or DISCARD, queue
        if cmd not in ("EXEC", "DISCARD"):
            connections_tx[conn_id]['commands'].append(decoded_data)
            connection.sendall(simple_string_encoder("QUEUED"))
            return (b"", True)
    # Implement commands
    if cmd == "PING":
        if len(decoded_data) == 1:
            connection.sendall(simple_string_encoder("PONG"))
        else:
            connection.sendall(resp_encoder(decoded_data[1]))
        return (b"", queued)

    if cmd == "ECHO":
        if len(decoded_data) > 1:
            connection.sendall(resp_encoder(decoded_data[1]))
        else:
            connection.sendall(resp_encoder(""))
        return (b"", queued)

    if cmd == "SET":
        if len(decoded_data) >= 3:
            setter(decoded_data[1:])
            connection.sendall(simple_string_encoder("OK"))
            prev_cmd = "SET"
            return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'set'"))
        return (b"", queued)

    elif cmd.upper() == "GET":
        print("role", config["role"])
        key = decoded_data[1]

        # First check in-memory store via getter()
        value = getter(key)

        # Fallback: check in RDB-loaded config store
        if value is None and "store" in config and key in config["store"]:
            val = config["store"][key]
            value = val[0] if isinstance(val, tuple) else val

        # Encode and send
        response = resp_encoder(value)
        print(f"GET response: {response}")
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # Lists
    if cmd == "RPUSH":
        if len(decoded_data) >= 3:
            size = rpush(decoded_data[1:], blocked)
            connection.sendall(resp_encoder(size))
            return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'rpush'"))
        return (b"", queued)

    if cmd == "LPUSH":
        if len(decoded_data) >= 3:
            size = lpush(decoded_data[1:])
            connection.sendall(resp_encoder(size))
            return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'lpush'"))
        return (b"", queued)

    if cmd == "LRANGE":
        if len(decoded_data) == 4:
            res = lrange(decoded_data[1:])
            connection.sendall(resp_encoder(res))
            return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'lrange'"))
        return (b"", queued)

    if cmd == "LLEN":
        if len(decoded_data) == 2:
            res = llen(decoded_data[1])
            connection.sendall(resp_encoder(res))
            return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'llen'"))
        return (b"", queued)

    if cmd == "LPOP":
        if len(decoded_data) >= 2:
            res = lpop(decoded_data[1:])
            if res is None:
                connection.sendall(b"$-1\r\n")
            elif isinstance(res, list):
                connection.sendall(resp_encoder(res))
            else:
                connection.sendall(resp_encoder(res))
            return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'lpop'"))
        return (b"", queued)

    if cmd == "BLPOP":
        if len(decoded_data) >= 3:
            res = blpop(decoded_data[1:], connection, blocked)
            if res is None:
                # blocked: no immediate response
                return (b"", queued)
            else:
                connection.sendall(resp_encoder(res))
                return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'blpop'"))
        return (b"", queued)

    # Streams
    if cmd == "XADD":
        if len(decoded_data) >= 4:
            status, payload = xadd(decoded_data[1:], blocked_xread)
            if status == "id":
                connection.sendall(resp_encoder(payload))
            else:
                connection.sendall(error_encoder(payload))
            return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'xadd'"))
        return (b"", queued)

    if cmd == "XRANGE":
        if len(decoded_data) >= 4:
            res = xrange_cmd(decoded_data[1:])
            connection.sendall(resp_encoder(res))
            return (b"", queued)
        connection.sendall(error_encoder("ERR wrong number of arguments for 'xrange'"))
        return (b"", queued)

    if cmd == "XREAD":
        # minimal support
        if decoded_data[1].upper() == "BLOCK":
            res = blocks_xread(decoded_data[2:], connection, blocked_xread)
            if res is None:
                return (b"", queued)
            else:
                connection.sendall(resp_encoder(res))
                return (b"", queued)
        else:
            res = xread(decoded_data[2:])
            connection.sendall(resp_encoder(res))
            return (b"", queued)

    # Transactions
    if cmd == "INCR":
        if len(decoded_data) >= 2:
            k = decoded_data[1]
            try:
                val = int(getter(k) or "0") + 1
                store[k] = str(val)
                connection.sendall(resp_encoder(val))
                return (b"", queued)
            except Exception:
                connection.sendall(error_encoder("ERR value is not an integer or out of range"))
                return (b"", queued)

    if cmd == "MULTI":
        connections_tx[id(connection)] = {'in_transaction': True, 'commands': []}
        connection.sendall(simple_string_encoder("OK"))
        return (b"", True)

    if cmd == "EXEC":
        txinfo = connections_tx.get(id(connection))
        if not txinfo or not txinfo.get('in_transaction'):
            connection.sendall(error_encoder("ERR EXEC without MULTI"))
            return (b"", queued)
        # execute queued commands
        q = txinfo['commands']
        responses = []
        for c in q:
            # each c is a list of strings; run executor in executing mode to get raw response
            out, _ = cmd_executor(c, connection, config, False, True)
            responses.append(out or resp_encoder(None))
        # cleanup
        del connections_tx[id(connection)]
        connection.sendall(array_encoder(responses))
        return (b"", False)

    if cmd == "DISCARD":
        if id(connection) in connections_tx:
            del connections_tx[id(connection)]
            connection.sendall(simple_string_encoder("OK"))
            return (b"", False)
        connection.sendall(error_encoder("ERR DISCARD without MULTI"))
        return (b"", queued)

    # Replication & introspection
    if cmd == "INFO":
        section = decoded_data[1] if len(decoded_data) > 1 else None
        resp = f"role:{config.get('role','master')}\n"
        if config.get('role') == 'master':
            resp += f"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n"
            resp += f"master_repl_offset:0\n"
        connection.sendall(resp_encoder(resp))
        return (b"", queued)

    if cmd == "REPLCONF":
        # minimal handling: ACK, GETACK
        if len(decoded_data) >= 2 and decoded_data[1].upper() == "GETACK":
            connection.sendall(resp_encoder(["REPLCONF", "ACK", str(BYTES_READ)]))
            return (b"", queued)
        if len(decoded_data) >= 2 and decoded_data[1].upper() == "ACK":
            global replica_acks
            replica_acks += 1
            connection.sendall(simple_string_encoder("OK"))
            return (b"", queued)
        connection.sendall(simple_string_encoder("OK"))
        return (b"", queued)

    if cmd == "PSYNC":
        # Send FULLRESYNC and push RDB bytes (minimalized)
        resp = simple_string_encoder("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0")
        connection.sendall(resp)
        rdb_bytes = bytes.fromhex(RDB_HEX)
        header = b"$" + str(len(rdb_bytes)).encode() + b"\r\n"
        connection.sendall(header + rdb_bytes)
        REPLICAS.append(connection)
        return (b"", queued)

    if cmd == "WAIT":
        # simple: return number of replicas
        connection.sendall(resp_encoder(len(REPLICAS)))
        return (b"", queued)

    if cmd == "CONFIG":
        # CONFIG GET field
        if len(decoded_data) >= 3 and decoded_data[1].upper() == "GET":
            field = decoded_data[2]
            if field == "dir":
                connection.sendall(resp_encoder(["dir", config.get('dir','')]))
                return (b"", queued)
            if field == "dbfilename":
                connection.sendall(resp_encoder(["dbfilename", config.get('dbfilename','')]))
                return (b"", queued)
        connection.sendall(error_encoder("ERR"))
        return (b"", queued)

    if cmd == "KEYS":
        if len(decoded_data) >= 2 and decoded_data[1] == "*":
            connection.sendall(resp_encoder(keys_list()))
            return (b"", queued)
        connection.sendall(resp_encoder([]))
        return (b"", queued)

    if cmd == "SUBSCRIBE":
        # enter subscribed mode for this connection
        SUBSCRIBE_MODE.add(connection)
        ch = decoded_data[1]
        subscriptions.setdefault(connection, set())
        subscriptions[connection].add(ch)
        connection.sendall(resp_encoder(["subscribe", ch, len(subscriptions[connection])]))
        return (b"", queued)

    # Unknown
    connection.sendall(error_encoder("ERR unknown command"))
    return (b"", queued)

# ---------------------------
# handle_client: receive bytes, parse, execute
# ---------------------------

def handle_client(connection, config, initial_data=b""):
    """
    connection: socket
    config: server config dictionary (role, dir, dbfilename, store etc)
    initial_data: bytes pre-filled (for replication master->slave handshake)
    """
    buffer = initial_data or b""
    queued = False
    executing = False
    with connection:
        while True:
            try:
                if not buffer:
                    chunk = connection.recv(4096)
                    if not chunk:
                        break
                    buffer += chunk
                # parse messages
                messages = parse_all(buffer)
                if not messages:
                    # incomplete; continue reading
                    buffer = buffer  # no-op
                    # try to read more
                    more = connection.recv(4096)
                    if not more:
                        break
                    buffer += more
                    continue
                # process each message
                # parse_all returns list of parsed messages, but the underlying parse_next consumes the whole buffer.
                for m in messages:
                    # normalize parsed message to strings
                    decoded = normalize_parsed(m)
                    # If bytes got returned for FULLRESYNC or raw RDB, skip if it starts with b'FULL' or 'F'
                    if decoded and ((isinstance(decoded[0], bytes) and decoded[0].startswith(b'FULL')) or decoded[0] == 'F'):
                        continue
                    # Execute
                    _, queued = cmd_executor(decoded, connection, config, queued, executing)
                # everything consumed — reset buffer
                buffer = b""
            except Exception:
                # On any parse/IO error, break connection loop
                break

# ---------------------------
# Master / Slave runnable classes
# ---------------------------

class Master:
    def __init__(self, args):
        self.args = args
        self.config = {
            'role': 'master',
            'master_replid': '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
            'master_replid_offset': '0',
            'dir': args.dir or '',
            'dbfilename': args.dbfilename or '',
            'store': {}
        }
        if self.config['dir'] and self.config['dbfilename']:
            read_key_val_from_db(self.config['dir'], self.config['dbfilename'], self.config['store'])
            store_rdb(self.config['store'])

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", int(args.port)))
        server_socket.listen(64)
        print("Server started on localhost:%s" % args.port)
        while True:
            client_socket, _ = server_socket.accept()
            t = threading.Thread(target=handle_client, args=(client_socket, self.config))
            t.daemon = True
            t.start()

class Slave:
    def __init__(self, args):
        self.args = args
        self.config = {'role': 'slave', 'dir': args.dir or '', 'dbfilename': args.dbfilename or '', 'store': {}}
        # parse replicaof (format "host port")
        master_host, master_port = args.replicaof.split(' ')
        self.config['master_host'] = master_host
        self.config['master_port'] = int(master_port)
        if self.config['dir'] and self.config['dbfilename']:
            read_key_val_from_db(self.config['dir'], self.config['dbfilename'], self.config['store'])
            store_rdb(self.config['store'])

        # start server socket for clients as well
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", int(args.port)))
        server_socket.listen(64)

        # Connect to master and perform handshake sequence
        self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_socket.connect((self.config['master_host'], self.config['master_port']))
        # send PING
        self.master_socket.sendall(resp_encoder(["PING"]))
        _ = self.master_socket.recv(1024)
        # REPLCONF listening-port
        self.master_socket.sendall(resp_encoder(["REPLCONF", "listening-port", str(args.port)]))
        _ = self.master_socket.recv(1024)
        # REPLCONF capa
        self.master_socket.sendall(resp_encoder(["REPLCONF", "capa", "psync2"]))
        _ = self.master_socket.recv(1024)
        # PSYNC
        self.master_socket.sendall(resp_encoder(["PSYNC", "?", "-1"]))
        # read master response (FULLRESYNC etc.)
        try:
            data = self.master_socket.recv(8192)
        except Exception:
            data = b""
        print(f"[Replica] PSYNC response (raw): {data[:200]}")
        # Launch a thread to handle incoming replication commands from master
        threading.Thread(target=handle_client, args=(self.master_socket, self.config, data), daemon=True).start()

        # Accept local client connections
        while True:
            client_socket, _ = server_socket.accept()
            t = threading.Thread(target=handle_client, args=(client_socket, self.config))
            t.daemon = True
            t.start()

# ---------------------------
# Utility: read_key_val_from_db used earlier (minimal)
# Duplicate of RDB minimal for the other code paths
# ---------------------------
def read_key_val_from_db(dir_path, dbfilename, data):
    path = os.path.join(dir_path, dbfilename)
    if not os.path.isfile(path):
        return
    # Attempt a simple parse similar to the RDBParserMinimal
    try:
        parser = RDBParserMinimal(path)
        parsed = parser.parse()
        for k, v in parsed.items():
            data[k] = v
    except Exception:
        return

# ---------------------------
# Main entrypoint
# ---------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, default="")
    parser.add_argument("--dir", type=str, default="")
    parser.add_argument("--dbfilename", type=str, default="")
    args = parser.parse_args()

    print("Logs from your program will appear here!")
    if args.replicaof:
        Slave(args)
    else:
        Master(args)

if __name__ == "__main__":
    main()