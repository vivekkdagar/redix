import socket
import threading
import struct
import os
import time
import datetime
from collections import deque, defaultdict
from app.resp import read_key_val_from_db  # keep your existing RDB reader if present

# ---------- RESP parsing / encoding ----------
def parse_next(data: bytes):
    first, rest = data.split(b"\r\n", 1)
    t = first[:1]
    if t == b"*":
        n = int(first[1:].decode())
        arr = []
        leftover = rest
        for _ in range(n):
            item, leftover = parse_next(leftover)
            arr.append(item)
        return arr, leftover
    if t == b"$":
        l = int(first[1:].decode())
        blk = rest[:l]
        if rest[l:l + 2] != b"\r\n":
            # invalid/incomplete
            raise RuntimeError("Invalid bulk string formatting")
        return blk, rest[l + 2:]
    if t == b"+":
        return first[1:].decode(), rest
    if t == b":":
        return int(first[1:].decode()), rest
    if t == b"-":
        return ("ERR", first[1:].decode()), rest
    raise RuntimeError(f"Unknown RESP type {t!r}")

def parse_all(data: bytes):
    msgs = []
    buf = data
    while buf:
        try:
            msg, buf = parse_next(buf)
            msgs.append(msg)
        except Exception:
            break
    return msgs

def simple_string_encoder(s: str):
    return f"+{s}\r\n".encode()

def error_encoder(s: str):
    return f"-{s}\r\n".encode()

def bulk_string_encoder(v):
    if v is None:
        return b"$-1\r\n"
    s = str(v)
    return f"${len(s)}\r\n{s}\r\n".encode()

def integer_encoder(n: int):
    return f":{n}\r\n".encode()

def array_merge(bytes_list):
    out = f"*{len(bytes_list)}\r\n".encode()
    for b in bytes_list:
        out += b
    return out

# ---------- In-memory structures ----------
class Value:
    def __init__(self, value, expiry=None):
        self.value = value
        self.expiry = expiry

store_lock = threading.Lock()
# store will be populated from config['store'] per connection in cmd_executor

# Lists and blocking
lists = defaultdict(list)
blocked_blpop = defaultdict(list)  # key -> list of connections blocked

# Streams
streams = defaultdict(list)  # key -> list of {"id": "time-seq", "fields": [k,v,...] }
_last_stream_time = 0
_last_stream_seq = defaultdict(int)
xread_block = defaultdict(list)  # key -> list of (connection, start_id)

# Transactions
transaction_queues = {}  # connection -> deque of queued commands (list[str])

# ---------- Utility ----------
def now_ms():
    return int(time.time() * 1000)

# ---------- Command executor ----------
def cmd_executor(decoded_data, connection, config, executing=False):
    """
    decoded_data: list of strings (command and args)
    connection: socket
    config: server config dict (contains 'store' in config['store'])
    executing: when True, we return response bytes instead of sending them
    Returns: (response_bytes_or_None, queued_flag)
    """
    db = config['store']
    if not decoded_data:
        return None, False

    cmd = decoded_data[0].upper()
    args = decoded_data[1:]
    queued = False

    # If client is in transaction mode and this is not MULTI/EXEC/DISCARD -> queue
    if connection in transaction_queues and cmd not in ("MULTI", "EXEC", "DISCARD"):
        # store decoded_data (strings) for later execution
        transaction_queues[connection].append(decoded_data)
        resp = simple_string_encoder("QUEUED")
        if executing:
            return resp, True
        try:
            connection.sendall(resp)
        except Exception:
            pass
        return None, True

    # MULTI
    if cmd == "MULTI":
        transaction_queues[connection] = deque()
        resp = simple_string_encoder("OK")
        if executing:
            return resp, True
        connection.sendall(resp)
        return None, True

    # DISCARD
    if cmd == "DISCARD":
        if connection in transaction_queues:
            del transaction_queues[connection]
            resp = simple_string_encoder("OK")
        else:
            resp = error_encoder("ERR DISCARD without MULTI")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # EXEC
    if cmd == "EXEC":
        if connection not in transaction_queues:
            resp = error_encoder("ERR EXEC without MULTI")
            if executing:
                return resp, False
            connection.sendall(resp)
            return None, False

        queued_cmds = list(transaction_queues.pop(connection, []))
        if not queued_cmds:
            if executing:
                return b"*0\r\n", False
            connection.sendall(b"*0\r\n")
            return None, False

        results = []
        for q in queued_cmds:
            try:
                # q is list of strings
                res, _ = cmd_executor(q, connection, config, executing=True)
            except Exception:
                res = error_encoder("EXECABORT Transaction discarded because of previous errors.")
            if res is None:
                # If a command produced no resp (shouldn't happen), encode nil
                res = b"$-1\r\n"
            results.append(res)

        merged = array_merge(results)
        if executing:
            return merged, False
        connection.sendall(merged)
        return None, False

    # PING
    if cmd == "PING":
        resp = simple_string_encoder("PONG")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # ECHO
    if cmd == "ECHO":
        msg = args[0] if args else ""
        resp = bulk_string_encoder(msg)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # SET
    if cmd == "SET":
        key = args[0]
        val = args[1] if len(args) > 1 else ""
        expiry = None
        if len(args) >= 4:
            # support EX and PX simplistically
            if args[2].upper() == "EX":
                expiry = datetime.datetime.now() + datetime.timedelta(seconds=int(args[3]))
            elif args[2].upper() == "PX":
                expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=int(args[3]))
        with store_lock:
            db[key] = Value(val, expiry)
        resp = simple_string_encoder("OK")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # GET
    if cmd == "GET":
        key = args[0]
        with store_lock:
            valobj = db.get(key)
        if valobj is None:
            resp = bulk_string_encoder(None)
        elif valobj.expiry and datetime.datetime.now() >= valobj.expiry:
            with store_lock:
                db.pop(key, None)
            resp = bulk_string_encoder(None)
        else:
            resp = bulk_string_encoder(valobj.value)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # INCR
    if cmd == "INCR":
        key = args[0]
        with store_lock:
            entry = db.get(key)
            if entry is None:
                db[key] = Value("1")
                resp = integer_encoder(1)
            else:
                try:
                    new_val = int(entry.value) + 1
                    db[key] = Value(str(new_val), entry.expiry)
                    resp = integer_encoder(new_val)
                except Exception:
                    resp = error_encoder("ERR value is not an integer or out of range")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # RPUSH
    if cmd == "RPUSH":
        key = args[0]
        values = args[1:]
        with store_lock:
            lists[key].extend(values)
        # unblock one blocked BLPOP if present
        if key in blocked_blpop and blocked_blpop[key]:
            blocked_conn = blocked_blpop[key].pop(0)
            # respond to blocked_conn with [key, value]
            try:
                v = lists[key].pop(0) if lists[key] else None
                if v is None:
                    blocked_conn.sendall(b"$-1\r\n")
                else:
                    resp = array_merge([bulk_string_encoder(key), bulk_string_encoder(v)])
                    blocked_conn.sendall(resp)
            except Exception:
                pass
        resp = integer_encoder(len(lists[key]))
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # LPUSH
    if cmd == "LPUSH":
        key = args[0]
        values = args[1:]
        with store_lock:
            for v in values:
                lists[key].insert(0, v)
            length = len(lists[key])
        resp = integer_encoder(length)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # LRANGE
    if cmd == "LRANGE":
        key = args[0]
        start = int(args[1])
        stop = int(args[2])
        with store_lock:
            arr = lists.get(key, [])
            n = len(arr)
            if start < 0:
                start += n
            if stop < 0:
                stop += n
            start = max(0, start)
            stop = min(stop, n - 1)
            if start > stop or n == 0:
                resp = b"*0\r\n"
            else:
                subset = arr[start:stop + 1]
                resp_parts = []
                for it in subset:
                    resp_parts.append(bulk_string_encoder(it))
                resp = array_merge(resp_parts)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # LPOP
    if cmd == "LPOP":
        key = args[0]
        count = 1
        if len(args) >= 2:
            count = int(args[1])
        with store_lock:
            arr = lists.get(key, [])
            if not arr:
                if count == 1:
                    resp = bulk_string_encoder(None)
                else:
                    resp = b"*0\r\n"
            else:
                if count == 1:
                    v = arr.pop(0)
                    resp = bulk_string_encoder(v)
                else:
                    popped = [arr.pop(0) for _ in range(min(count, len(arr)))]
                    resp_parts = [bulk_string_encoder(x) for x in popped]
                    resp = array_merge(resp_parts)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # BLPOP
    if cmd == "BLPOP":
        key = args[0]
        timeout = float(args[1]) if len(args) >= 2 else 0.0
        with store_lock:
            if lists.get(key):
                v = lists[key].pop(0)
                resp = array_merge([bulk_string_encoder(key), bulk_string_encoder(v)])
                if executing:
                    return resp, False
                connection.sendall(resp)
                return None, False
            else:
                # block
                blocked_blpop[key].append(connection)
                if timeout > 0:
                    def unblock_after():
                        time.sleep(timeout)
                        try:
                            if connection in blocked_blpop.get(key, []):
                                blocked_blpop[key].remove(connection)
                                connection.sendall(b"*-1\r\n")
                        except Exception:
                            pass
                    t = threading.Thread(target=unblock_after, daemon=True)
                    t.start()
                return None, False

    # XADD (very simplified)
    if cmd == "XADD":
        key = args[0]
        entry_id = args[1]
        fields = args[2:]
        # generate id if '*' or partial logic
        global _last_stream_time, _last_stream_seq
        if entry_id == '*':
            t = now_ms()
            seq = _last_stream_seq.get(t, 0)
            final_id = f"{t}-{seq}"
            _last_stream_seq[t] = seq + 1
            _last_stream_time = t
        elif "-" in entry_id:
            final_id = entry_id
            parts = entry_id.split("-")
            try:
                t = int(parts[0])
                seq = int(parts[1])
            except:
                t = now_ms()
                seq = 0
        else:
            t = now_ms()
            seq = _last_stream_seq.get(t, 0)
            final_id = f"{t}-{seq}"
            _last_stream_seq[t] = seq + 1
            _last_stream_time = t
        streams[key].append({"id": final_id, "fields": fields})
        resp = bulk_string_encoder(final_id)
        if executing:
            return resp, False
        connection.sendall(resp)
        # notify xread blocked clients
        if key in xread_block and xread_block[key]:
            pending = xread_block.pop(key)
            for (conn, startid) in pending:
                # assemble available entries > startid
                entries = []
                for e in streams[key]:
                    if e["id"] > startid:
                        # send as [key, [[id, [field,val,...]]]]
                        entries.append([key, [[e["id"], e["fields"]]]])
                if entries:
                    # encode simply as array of entries (not full fidelity)
                    out_parts = []
                    for ent in entries:
                        out_parts.append(bulk_string_encoder(ent[0]))
                        # inner array
                        for idfields in ent[1]:
                            out_parts.append(bulk_string_encoder(idfields[0]))
                            out_parts.append(array_merge([bulk_string_encoder(x) for x in idfields[1]]))
                    try:
                        conn.sendall(array_merge(out_parts))
                    except Exception:
                        pass
        return None, False

    # XRANGE (simplified)
    if cmd == "XRANGE":
        key = args[0]
        start = args[1]
        end = args[2]
        result = []
        with store_lock:
            for e in streams.get(key, []):
                if start <= e["id"] <= end:
                    result.append(e)
        if not result:
            resp = b"*0\r\n"
        else:
            # Build RESP: array of [id, [field, value, ...]]
            parts = []
            for e in result:
                # id
                parts.append(bulk_string_encoder(e["id"]))
                # fields array
                field_parts = [bulk_string_encoder(x) for x in e["fields"]]
                parts.append(array_merge(field_parts))
            resp = array_merge(parts)
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # XREAD (very simplified: non-blocking)
    if cmd == "XREAD":
        # basic: XREAD COUNT <n> STREAMS key id
        # support BLOCK by queueing to xread_block
        if args and args[0].upper() == "BLOCK":
            # BLOCK <ms> STREAMS <key> <id>
            timeout_ms = int(args[1])
            # args[2] == "STREAMS"
            key = args[3]
            startid = args[4]
            # check if entries available
            avail = [e for e in streams.get(key, []) if e["id"] > startid]
            if avail:
                # send immediately
                parts = []
                for e in avail:
                    parts.append(bulk_string_encoder(key))
                    parts.append(bulk_string_encoder(e["id"]))
                    parts.append(array_merge([bulk_string_encoder(x) for x in e["fields"]]))
                resp = array_merge(parts)
                if executing:
                    return resp, False
                connection.sendall(resp)
                return None, False
            else:
                # block until new entry or timeout
                xread_block[key].append((connection, startid))
                if timeout_ms > 0:
                    def timeout_unblock():
                        time.sleep(timeout_ms / 1000)
                        try:
                            if (connection, startid) in xread_block.get(key, []):
                                xread_block[key].remove((connection, startid))
                                connection.sendall(b"*-1\r\n")
                        except Exception:
                            pass
                    threading.Thread(target=timeout_unblock, daemon=True).start()
                return None, False
        else:
            # Non-blocking XREAD of form: XREAD STREAMS key id
            # we'll only support single key
            # locate key and id
            # find newer entries than id
            if "STREAMS" in args:
                idx = args.index("STREAMS")
                key = args[idx + 1]
                startid = args[idx + 2]
                avail = [e for e in streams.get(key, []) if e["id"] > startid]
                if not avail:
                    resp = b"*0\r\n"
                else:
                    parts = []
                    for e in avail:
                        parts.append(bulk_string_encoder(key))
                        parts.append(bulk_string_encoder(e["id"]))
                        parts.append(array_merge([bulk_string_encoder(x) for x in e["fields"]]))
                    resp = array_merge(parts)
                if executing:
                    return resp, False
                connection.sendall(resp)
                return None, False

    # CONFIG GET dir/dbfilename stubs used during replication handshake
    if cmd == "CONFIG" and len(args) >= 2 and args[0].upper() == "GET":
        param = args[1]
        if param == "dir":
            resp = array_merge([bulk_string_encoder("dir"), bulk_string_encoder(config.get("dir", "/tmp"))])
        elif param == "dbfilename":
            resp = array_merge([bulk_string_encoder("dbfilename"), bulk_string_encoder(config.get("dbfilename", "dump.rdb"))])
        else:
            resp = array_merge([])
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # REPLCONF / PSYNC stubs
    if cmd == "REPLCONF":
        resp = simple_string_encoder("OK")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    if cmd == "PSYNC":
        resp = simple_string_encoder(f"FULLRESYNC {config.get('master_replid','') or ''} {config.get('master_replid_offset','0')}")
        if executing:
            return resp, False
        connection.sendall(resp)
        return None, False

    # Unknown
    resp = error_encoder(f"ERR unknown command '{cmd}'")
    if executing:
        return resp, False
    connection.sendall(resp)
    return None, False

# ---------- Client handler ----------
def handle_client(conn, config, initial_data=b""):
    store_rdb(config.get('store', {}))
    buffer = initial_data
    with conn:
        while True:
            if b"FULL" in buffer:
                _, buffer = buffer.split(b"\r\n", 1)
            if not buffer:
                try:
                    chunk = conn.recv(65536)
                except Exception:
                    break
                if not chunk:
                    break
                buffer += chunk
            msgs = parse_all(buffer)
            if not msgs:
                # incomplete, wait for more
                continue
            for msg in msgs:
                # msg is a list (array) of items (bytes or already-decoded simple strings)
                # Convert bulk bytes to decoded strings so executor receives strings
                try:
                    decoded = []
                    for x in msg:
                        if isinstance(x, bytes):
                            decoded.append(x.decode())
                        elif isinstance(x, int):
                            decoded.append(str(x))
                        elif isinstance(x, tuple) and x[0] == "ERR":
                            decoded.append(f"-{x[1]}")
                        else:
                            decoded.append(str(x))
                    # dispatch
                    cmd_executor(decoded, conn, config)
                except Exception:
                    pass
            buffer = b""

# ---------- Server bootstrap classes ----------
class Master:
    def __init__(self, args):
        self.slaves = {}
        self.args = args
        self.config = {}
        self.config['role'] = 'master'
        self.config['master_replid'] = 'master-replid-example'
        self.config['master_replid_offset'] = '0'
        self.config['dir'] = args.dir
        self.config['dbfilename'] = args.dbfilename
        self.config['store'] = {}
        read_key_val_from_db(self.config['dir'], self.config['dbfilename'], self.config['store'])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", int(args.port)))
        s.listen(10)
        while True:
            client_socket, _ = s.accept()
            t = threading.Thread(target=handle_client, args=(client_socket, self.config))
            t.daemon = True
            t.start()

class Slave:
    def __init__(self, args):
        self.args = args
        self.config = {}
        self.config['role'] = 'slave'
        master_host, master_port = self.args.replicaof.split(' ')
        self.config['master_host'] = master_host
        self.config['master_port'] = int(master_port)
        self.config['store'] = {}
        read_key_val_from_db(args.dir, args.dbfilename, self.config['store'])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", int(args.port)))
        s.listen(10)
        # handshake
        self.master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_sock.connect((self.config['master_host'], self.config['master_port']))
        # send PING and basic replconf/psync as in previous code
        try:
            self.master_sock.sendall(array_merge([bulk_string_encoder("PING")]))
            self.master_sock.recv(1024)
        except Exception:
            pass
        # spawn handler to process master messages
        t = threading.Thread(target=handle_client, args=(self.master_sock, self.config, b""))
        t.daemon = True
        t.start()
        while True:
            client_socket, _ = s.accept()
            t2 = threading.Thread(target=handle_client, args=(client_socket, self.config))
            t2.daemon = True
            t2.start()

# ---------- helper used by handle_client ----------
def store_rdb(info):
    # noop here (keeps compatibility with original code)
    return

# ---------- If run as script ----------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, default="")
    parser.add_argument("--dir", type=str, default="")
    parser.add_argument("--dbfilename", type=str, default="")
    args = parser.parse_args()
    if args.replicaof:
        Slave(args)
    else:
        Master(args)