#!/usr/bin/env python3
import socket, threading, argparse, os, struct, datetime, time, re
from collections import defaultdict

# ---------- RESP parsing/encoding ----------

def parse_next(data: bytes):
    # returns (value, rest)
    first, rest = data.split(b"\r\n", 1)
    t = first[:1]
    if t == b'*':
        n = int(first[1:].decode())
        arr = []
        tail = rest
        for _ in range(n):
            item, tail = parse_next(tail)
            arr.append(item)
        return arr, tail
    if t == b'$':
        ln = int(first[1:].decode())
        if ln == -1:
            return None, rest
        blk = rest[:ln]
        # expect \r\n after block
        tail = rest[ln + 2:] if rest[ln:ln + 2] == b"\r\n" else rest[ln:]
        return blk, tail
    if t == b'+':
        return first[1:].decode(), rest
    if t == b':':
        return int(first[1:].decode()), rest
    if t == b'-':
        return Exception(first[1:].decode()), rest
    raise RuntimeError("unknown RESP type")

def parse_all(buffer: bytes):
    msgs = []
    b = buffer
    while b:
        try:
            msg, b = parse_next(b)
            msgs.append(msg)
        except Exception:
            break
    return msgs

def simple_string_encoder(msg: str):
    return f"+{msg}\r\n".encode()

def error_encoder(msg: str):
    return f"-{msg}\r\n".encode()

def bulk_string_encoder(val):
    if val is None:
        return b"$-1\r\n"
    s = str(val)
    return f"${len(s)}\r\n{s}\r\n".encode()

def integer_encoder(n):
    return f":{n}\r\n".encode()

def array_encoder(list_of_resp_bytes):
    # list_of_resp_bytes: list of already-RESP-encoded byte sequences
    merged = f"*{len(list_of_resp_bytes)}\r\n".encode()
    for rb in list_of_resp_bytes:
        merged += rb
    return merged

# resp_encoder convenience for python objects -> bytes (strings -> bulk)
def resp_encoder(obj):
    if obj is None:
        return b"$-1\r\n"
    if isinstance(obj, bytes):
        return obj
    if isinstance(obj, int):
        return integer_encoder(obj)
    if isinstance(obj, list):
        out = f"*{len(obj)}\r\n".encode()
        for it in obj:
            if isinstance(it, bytes):
                out += bulk_string_encoder(it.decode())
            elif isinstance(it, int):
                out += integer_encoder(it)
            elif isinstance(it, str):
                out += bulk_string_encoder(it)
            else:
                out += bulk_string_encoder(str(it))
        return out
    return bulk_string_encoder(str(obj))

# ---------- Storage classes / small utils ----------

class Value:
    def __init__(self, value, expiry=None):
        self.value = value
        self.expiry = expiry

store = {}         # key -> Value or list or stream structure
store_list = {}    # key -> list
streams = {}       # key -> list of dict entries
subscriptions = {} # conn -> set(channels)
channel_subs = defaultdict(set)
replicas = []      # connections for replication (if used)
BYTES_READ = 0
replica_acks = 0
prev_cmd = ""
blocked = {}       # key -> list of connections for BLPOP
blocked_xread = {} # stream key -> list of (conn, last_id)

# minimal rdb reader stub (if file provided, load simple key/val pairs)
def read_key_val_from_db(dirpath, dbfilename, dest_dict):
    if not dirpath or not dbfilename:
        return
    path = os.path.join(dirpath, dbfilename)
    if not os.path.isfile(path):
        return
    try:
        with open(path, "rb") as f:
            data = f.read().decode(errors="ignore").splitlines()
            for ln in data:
                if '=' in ln:
                    k,v = ln.split('=',1)
                    dest_dict[k] = Value(v, None)
    except Exception:
        pass

# ---------- List, stream, helper ops ----------

def setter(args):
    # args: [key, value, ...maybe PX/EX...]
    key, value = args[0], args[1]
    expiry = None
    if len(args) > 2:
        if args[2].upper() == "EX" and len(args) >= 4:
            expiry = datetime.datetime.now() + datetime.timedelta(seconds=int(args[3]))
        elif args[2].upper() == "PX" and len(args) >= 4:
            expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=int(args[3]))
    store[key] = Value(value=value, expiry=expiry)

def getter(key):
    v = store.get(key)
    if v is None:
        return None
    if isinstance(v, Value):
        if v.expiry and datetime.datetime.now() >= v.expiry:
            del store[key]
            return None
        return v.value
    return v  # list or other types

def rpush(args):
    key = args[0]
    vals = args[1:]
    if key not in store_list:
        store_list[key] = []
    store_list[key].extend(vals)
    # wake blocked blpop connections
    while key in blocked and blocked[key] and store_list[key]:
        conn = blocked[key].pop(0)
        val = store_list[key].pop(0)
        try:
            conn.sendall(resp_encoder([key, val]))
        except Exception:
            pass
    return len(store_list[key])

def lpush(args):
    key = args[0]
    vals = args[1:]
    if key not in store_list:
        store_list[key] = []
    for v in vals:
        store_list[key].insert(0, v)
    return len(store_list[key])

def lrange_fn(args):
    key = args[0]
    start = int(args[1])
    stop = int(args[2])
    if key not in store_list:
        return []
    arr = store_list[key]
    n = len(arr)
    if start < 0:
        start += n
    if stop < 0:
        stop += n
    start = max(0, start)
    stop = min(stop, n - 1)
    if start > stop or n == 0:
        return []
    return arr[start:stop+1]

def lpop_fn(args):
    key = args[0]
    if key not in store_list or not store_list[key]:
        return None
    if len(args) == 1:
        return store_list[key].pop(0)
    else:
        cnt = int(args[1])
        res = []
        for _ in range(min(cnt, len(store_list[key]))):
            res.append(store_list[key].pop(0))
        return res

def blpop_fn(args, conn):
    key = args[0]
    timeout = float(args[1])
    if key in store_list and store_list[key]:
        return [key, store_list[key].pop(0)]
    # block
    if key not in blocked:
        blocked[key] = []
    blocked[key].append(conn)
    if timeout > 0:
        def timeout_unblock():
            time.sleep(timeout)
            if key in blocked and conn in blocked[key]:
                try:
                    blocked[key].remove(conn)
                    conn.sendall(b"*-1\r\n")
                except Exception:
                    pass
        t = threading.Thread(target=timeout_unblock, daemon=True)
        t.start()
    return None

# Streams (basic xadd / xrange / xread)
def allot_id(key, t_str):
    if key not in streams or not streams[key]:
        return f"{t_str}-0"
    last = streams[key][-1]['id']
    last_time, last_seq = last.split("-")
    if int(t_str) > int(last_time):
        return f"{t_str}-0"
    else:
        return f"{last_time}-{int(last_seq)+1}"

def xadd_fn(args):
    key = args[0]
    entry_id = args[1]
    if entry_id == '*':
        t = str(int(time.time()*1000))
        entry_id = allot_id(key, t)
    elif '-' in entry_id:
        t_str, seq = entry_id.split('-',1)
        if seq == '*':
            entry_id = allot_id(key, t_str)
    else:
        entry_id = allot_id(key, str(int(time.time()*1000)))
    fields = args[2:]
    dct = {'id': entry_id}
    for i in range(0, len(fields),2):
        if i+1 < len(fields):
            dct[fields[i]] = fields[i+1]
    streams.setdefault(key, []).append(dct)
    # wake blocked xread clients
    if key in blocked_xread and blocked_xread[key]:
        pend = blocked_xread[key][:]
        blocked_xread[key] = []
        for conn, last_id in pend:
            res = []
            for e in streams[key]:
                if e['id'] > last_id:
                    fields_arr = []
                    for k,v in e.items():
                        if k != 'id':
                            fields_arr.append([k,v])
                    res.append([key, [[e['id'], fields_arr]]])
            if res:
                try:
                    conn.sendall(resp_encoder(res))
                except Exception:
                    pass
    return entry_id

def xrange_fn(args):
    key = args[0]
    start = args[1]
    end = args[2]
    if key not in streams:
        return []
    res = []
    for e in streams[key]:
        if start <= e['id'] <= end:
            fields = []
            for k,v in e.items():
                if k != 'id':
                    fields.append([k,v])
            res.append([e['id'], fields])
    return res

def xread_fn(args):
    # args = [key1, key2..., id1, id2...]
    n = len(args)//2
    keys = args[:n]
    ids = args[n:]
    out = []
    for k, last in zip(keys, ids):
        if k not in streams:
            continue
        entries = []
        for e in streams[k]:
            if e['id'] > last:
                fields=[]
                for kk,v in e.items():
                    if kk!='id':
                        fields.append([kk,v])
                entries.append([e['id'], fields])
        if entries:
            out.append([k, entries])
    return out

def blocks_xread_fn(args, conn):
    # args: [ms, 'STREAMS', key, id]
    try:
        ms = float(args[0]); key = args[2]; id0 = args[3]
    except Exception:
        return None
    if key not in streams or not any(e['id']>id0 for e in streams[key]):
        # block
        blocked_xread.setdefault(key, []).append((conn, id0))
        if ms > 0:
            def tmout():
                time.sleep(ms/1000)
                # if still blocked, respond nil
                for pair in list(blocked_xread.get(key, [])):
                    if pair[0] == conn:
                        try:
                            blocked_xread[key].remove(pair)
                            conn.sendall(b"*-1\r\n")
                        except Exception:
                            pass
            threading.Thread(target=tmout, daemon=True).start()
        return None
    else:
        return xread_fn([key, id0])

# ---------- Transactions state ----------

transaction_queues = {}  # conn -> list of decoded-commands (strings)

# ---------- Command executor ----------

def cmd_executor(decoded, conn, config, executing=False):
    # decoded: list of bulk-items bytes or str (we will normalize to str)
    if not decoded:
        return None
    # normalize
    args = [ (x.decode() if isinstance(x, (bytes,bytearray)) else x) for x in decoded ]
    cmd = args[0].upper()
    # queueing logic
    if conn in transaction_queues and cmd not in ("MULTI","EXEC","DISCARD"):
        transaction_queues[conn].append(args)
        return simple_string_encoder("QUEUED")
    # MULTI
    if cmd == "MULTI":
        transaction_queues[conn] = []
        return simple_string_encoder("OK")
    # EXEC
    if cmd == "EXEC":
        if conn not in transaction_queues:
            return error_encoder("ERR EXEC without MULTI")
        queued_cmds = transaction_queues.pop(conn, [])
        if not queued_cmds:
            return b"*0\r\n"
        results = []
        for q in queued_cmds:
            try:
                r = cmd_executor(q, conn, config, executing=True)
                if r is None:
                    r = simple_string_encoder("OK")
            except Exception as e:
                r = error_encoder("EXECABORT Transaction discarded because of previous errors.")
            results.append(r if isinstance(r, bytes) else str(r).encode())
        return array_encoder(results)
    # DISCARD
    if cmd == "DISCARD":
        if conn in transaction_queues:
            transaction_queues.pop(conn, None)
            return simple_string_encoder("OK")
        else:
            return error_encoder("ERR DISCARD without MULTI")
    # PING
    if cmd == "PING":
        return simple_string_encoder("PONG")
    # ECHO
    if cmd == "ECHO":
        msg = args[1] if len(args)>1 else ""
        return bulk_string_encoder(msg)
    # SET
    if cmd == "SET":
        if len(args) < 3:
            return error_encoder("ERR wrong number of args for SET")
        setter(args[1:])
        # replicate to replicas if needed
        return simple_string_encoder("OK")
    # GET
    if cmd == "GET":
        if len(args) < 2:
            return error_encoder("ERR wrong number of args for GET")
        v = getter(args[1])
        return bulk_string_encoder(v)
    # INCR
    if cmd == "INCR":
        k = args[1]
        v = getter(k)
        if v is None:
            setter([k, "1"])
            return integer_encoder(1)
        try:
            nv = int(v) + 1
            setter([k, str(nv)])
            return integer_encoder(nv)
        except Exception:
            return error_encoder("ERR value is not an integer or out of range")
    # RPUSH
    if cmd == "RPUSH":
        if len(args) < 3:
            return error_encoder("ERR wrong number of args for RPUSH")
        n = rpush(args[1:])
        return integer_encoder(n)
    # LPUSH
    if cmd == "LPUSH":
        if len(args) < 3:
            return error_encoder("ERR wrong number of args for LPUSH")
        n = lpush(args[1:])
        return integer_encoder(n)
    # LRANGE
    if cmd == "LRANGE":
        if len(args) < 4:
            return error_encoder("ERR wrong number of args for LRANGE")
        res = lrange_fn(args[1:])
        return resp_encoder(res)
    # LLEN
    if cmd == "LLEN":
        n = len(store_list.get(args[1], []))
        return integer_encoder(n)
    # LPOP
    if cmd == "LPOP":
        res = lpop_fn(args[1:])
        if res is None:
            return bulk_string_encoder(None)
        if isinstance(res, list):
            return resp_encoder(res)
        return bulk_string_encoder(res)
    # BLPOP
    if cmd == "BLPOP":
        r = blpop_fn(args[1:], conn)
        if r is None:
            return None
        return resp_encoder(r)
    # TYPE
    if cmd == "TYPE":
        k = args[1]
        if k in store and isinstance(store[k], Value):
            return simple_string_encoder("string")
        if k in store_list:
            return simple_string_encoder("list")
        if k in streams:
            return simple_string_encoder("stream")
        return simple_string_encoder("none")
    # XADD
    if cmd == "XADD":
        res = xadd_fn(args[1:])
        return bulk_string_encoder(res)
    # XRANGE
    if cmd == "XRANGE":
        res = xrange_fn(args[1:])
        return resp_encoder(res)
    # XREAD
    if cmd == "XREAD":
        if args[1].upper() == "BLOCK":
            r = blocks_xread_fn(args[1:], conn)
            if r is None:
                return None
            return resp_encoder(r)
        else:
            res = xread_fn(args[1:])
            return resp_encoder(res)
    # SUBSCRIBE
    if cmd == "SUBSCRIBE":
        ch = args[1]
        subscriptions.setdefault(conn, set()).add(ch)
        channel_subs[ch].add(conn)
        return resp_encoder(["subscribe", ch, str(len(subscriptions[conn]))])
    # UNSUBSCRIBE
    if cmd == "UNSUBSCRIBE":
        ch = args[1] if len(args)>=2 else None
        if conn in subscriptions:
            if ch:
                subscriptions[conn].discard(ch)
                channel_subs[ch].discard(conn)
            else:
                # unsubscribe all
                for c in list(subscriptions[conn]):
                    channel_subs[c].discard(conn)
                subscriptions[conn].clear()
        count = len(subscriptions.get(conn, set()))
        return resp_encoder(["unsubscribe", ch if ch else "", str(count)])
    # PUBLISH
    if cmd == "PUBLISH":
        ch = args[1]; msg = args[2]
        subs = channel_subs.get(ch, set())
        sent = 0
        for c in list(subs):
            try:
                c.sendall(resp_encoder(["message", ch, msg]))
                sent += 1
            except Exception:
                pass
        return integer_encoder(sent)
    # CONFIG GET
    if cmd == "CONFIG" and len(args)>=3 and args[1].upper()=="GET":
        p = args[2]
        if p == "dir":
            return resp_encoder(["dir", config.get('dir','')])
        if p == "dbfilename":
            return resp_encoder(["dbfilename", config.get('dbfilename','')])
        return resp_encoder([])
    # INFO
    if cmd == "INFO":
        section = args[1] if len(args)>1 else ""
        if section.lower() == "replication":
            role = config.get('role','master')
            s = f"role:{role}\n"
            s += f"master_replid:{config.get('master_replid','')}\n"
            s += f"master_repl_offset:{config.get('master_replid_offset','0')}\n"
            return bulk_string_encoder(s)
        return bulk_string_encoder("ok")
    # REPLCONF / PSYNC / WAIT minimal stubs for tests
    if cmd == "REPLCONF":
        return simple_string_encoder("OK")
    if cmd == "PSYNC":
        # send FULLRESYNC + rdb blob
        rid = config.get('master_replid','0')
        roff = config.get('master_replid_offset','0')
        return simple_string_encoder(f"FULLRESYNC {rid} {roff}")
    if cmd == "WAIT":
        return integer_encoder(0)
    return error_encoder(f"ERR unknown command '{cmd}'")

# ---------- Client handler ----------

def handle_client(conn, config, initial_data=b""):
    global prev_cmd
    read_key_val_from_db(config.get('dir',''), config.get('dbfilename',''), config.setdefault('store',{}))
    buffer = initial_data
    with conn:
        while True:
            try:
                if not buffer:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buffer += chunk
                msgs = parse_all(buffer)
                # If parse_all consumed nothing (partial), wait for more
                if not msgs:
                    # try to detect if buffer is incomplete: break to recv more
                    # we will keep buffer as-is
                    buffer = buffer
                    continue
                # If parse_all returns at least one message, we must empty buffer fully,
                # because parse_all consumed what it could (we ignore leftover)
                buffer = b""
                for m in msgs:
                    # m is array (list) of bulk items (bytes) or nested arrays
                    # top-level expected: an array with bulk strings (command + args)
                    if not isinstance(m, list) or len(m)==0:
                        continue
                    decoded = [item for item in m]  # keep bytes for cmd_executor normalization
                    resp = cmd_executor(decoded, conn, config)
                    # If resp is None => command blocked (BLPOP/XREAD) or queued (no immediate response)
                    if resp is None:
                        # blocked or delayed; just continue to next recv
                        continue
                    # If resp is already a full RESP array (EXEC) we should write it directly.
                    try:
                        conn.sendall(resp)
                    except Exception:
                        pass
            except Exception as e:
                # swallow and close
                break

# ---------- Server bootstrap classes ----------

class Master:
    def __init__(self, args):
        self.args = args
        self.config = {}
        self.config['role'] = 'master'
        self.config['master_replid'] = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
        self.config['master_replid_offset'] = '0'
        self.config['dir'] = args.dir
        self.config['dbfilename'] = args.dbfilename
        self.config['store'] = {}
        read_key_val_from_db(self.config['dir'], self.config['dbfilename'], self.config['store'])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", args.port))
        s.listen(50)
        while True:
            c,_ = s.accept()
            t = threading.Thread(target=handle_client, args=(c, self.config))
            t.daemon = True
            t.start()

class Slave:
    def __init__(self, args):
        self.args = args
        self.config = {}
        self.config['role'] = 'slave'
        master_host, master_port = args.replicaof.split(' ')
        self.config['master_host'] = master_host
        self.config['master_port'] = int(master_port)
        self.config['dir'] = args.dir
        self.config['dbfilename'] = args.dbfilename
        self.config['store'] = {}
        read_key_val_from_db(self.config['dir'], self.config['dbfilename'], self.config['store'])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", args.port))
        s.listen(10)
        # handshake simplified
        master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_sock.connect((self.config['master_host'], self.config['master_port']))
        master_sock.sendall(resp_encoder(["PING"]))
        # read PONG
        try:
            data = master_sock.recv(1024)
        except Exception:
            data = b""
        # start client handler thread for master connection (replication)
        t = threading.Thread(target=handle_client, args=(master_sock, self.config, data))
        t.daemon = True
        t.start()
        while True:
            c,_ = s.accept()
            t2 = threading.Thread(target=handle_client, args=(c, self.config))
            t2.daemon = True
            t2.start()

# ---------- main ----------

def main():
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

if __name__ == "__main__":
    main()