#!/usr/bin/env python3
import socket
import threading
import argparse
import time
import datetime
import heapq
import os

# --- RESP parsing / encoding ---
def parse_next(data: bytes):
    # returns (value, remaining)
    if not data:
        raise ValueError("no data")
    first, rest = data.split(b"\r\n", 1)
    t = first[:1]
    if t == b"*":
        count = int(first[1:].decode())
        arr = []
        remaining = rest
        for _ in range(count):
            itm, remaining = parse_next(remaining)
            arr.append(itm)
        return arr, remaining
    if t == b"$":
        length = int(first[1:].decode())
        if length == -1:
            return None, rest
        blk = rest[:length]
        remaining = rest[length:]
        if remaining.startswith(b"\r\n"):
            remaining = remaining[2:]
        return blk, remaining
    if t == b"+":
        return first[1:].decode(), rest
    if t == b":":
        return int(first[1:].decode()), rest
    if t == b"-":
        return first[1:].decode(), rest
    raise RuntimeError("parse_next unknown prefix")

def parse_all(data: bytes):
    msgs = []
    buf = data
    while buf:
        try:
            v, buf = parse_next(buf)
            msgs.append(v)
        except Exception:
            break
    return msgs

def resp_encode(v):
    if v is None:
        return b"$-1\r\n"
    if isinstance(v, bytes):
        return b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
    if isinstance(v, str):
        # as bulk string for most returns (match prior code)
        return b"$" + str(len(v)).encode() + b"\r\n" + v.encode() + b"\r\n"
    if isinstance(v, int):
        return b":" + str(v).encode() + b"\r\n"
    if isinstance(v, list):
        out = b"*" + str(len(v)).encode() + b"\r\n"
        for item in v:
            out += resp_encode(item)
        return out
    raise RuntimeError("resp encode unknown type")

def simple_string(s):
    return b"+" + s.encode() + b"\r\n"

def error_str(s):
    return b"-" + s.encode() + b"\r\n"

# --- Storage and globals ---
db = {}  # key (bytes) -> {"value": any, "expiry": datetime or None}
store_lists = {}  # key -> [bytes,...]
streams = {}  # key -> list of dicts with 'id' and fields
sorted_set_dict = {}  # key -> list as heap [(score, member_bytes), ...]
blocked = {}  # key->list of connections for BLPOP
blocked_xread = {}  # key->list of (conn, last_id)
subscriptions = {}  # conn -> set(channel bytes)
channel_subs = {}  # channel -> set(conns)
subscriber_mode = set()  # conn set
REPLICAS = []
BYTES_READ = 0
replica_acks = 0
prev_cmd = ""

# --- Helper utilities ---
def now_ms():
    return int(time.time() * 1000)

def check_expired(key):
    ent = db.get(key)
    if not ent:
        return False
    exp = ent.get("expiry")
    if exp is None:
        return False
    if datetime.datetime.now() >= exp:
        db.pop(key, None)
        return True
    return False

# --- Command executor ---
def cmd_executor(connection, decoded, config, queued=False, executing=False):
    global BYTES_READ, REPLICAS, replica_acks, prev_cmd, subscriber_mode
    if not decoded or len(decoded) == 0:
        connection.sendall(error_str("ERR empty command"))
        return queued
    cmd = decoded[0]
    # ensure bytes for comparisons
    if isinstance(cmd, bytes):
        cmd_u = cmd.upper()
    elif isinstance(cmd, str):
        cmd_u = cmd.upper().encode()
    else:
        cmd_u = cmd
    # simplify uppercase str for logic
    try:
        cmd_text = cmd_u.decode().upper()
    except Exception:
        cmd_text = str(cmd_u).upper()

    # subscriber-mode guard
    if connection in subscriber_mode:
        allowed = {"SUBSCRIBE","UNSUBSCRIBE","PUBLISH","PSUBSCRIBE","PUNSUBSCRIBE","PING","QUIT"}
        if cmd_text not in allowed:
            connection.sendall(error_str(f"ERR Can't execute '{cmd_text}'"))
            return queued

    # PING
    if cmd_text == "PING":
        BYTES_READ += len(resp_encode(decoded))
        if connection in subscriber_mode:
            response = resp_encode([b"pong", b""])
        else:
            response = simple_string("PONG")
        connection.sendall(response)
        return queued

    # ECHO
    if cmd_text == "ECHO":
        if len(decoded) < 2:
            connection.sendall(error_str("ERR wrong number of arguments for 'ECHO'"))
            return queued
        connection.sendall(resp_encode(decoded[1]))
        return queued

    # GET
    if cmd_text == "GET":
        if len(decoded) < 2:
            connection.sendall(error_str("ERR wrong number of arguments for 'GET'"))
            return queued
        k = decoded[1]
        if check_expired(k):
            connection.sendall(resp_encode(None))
            return queued
        ent = db.get(k)
        if not ent:
            connection.sendall(resp_encode(None))
        else:
            connection.sendall(resp_encode(ent["value"]))
        return queued

    # SET
    if cmd_text == "SET":
        if len(decoded) < 3:
            connection.sendall(error_str("ERR wrong number of arguments for 'SET'"))
            return queued
        k = decoded[1]
        v = decoded[2]
        expiry = None
        if len(decoded) >= 5:
            # support PX
            opt = decoded[3]
            if isinstance(opt, bytes) and opt.upper() == b"PX":
                ms = int(decoded[4].decode())
                expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=ms)
        db[k] = {"value": v, "expiry": expiry}
        # replicate to connected replicas
        for rep in REPLICAS:
            try:
                rep.sendall(resp_encode(decoded))
            except Exception:
                pass
        connection.sendall(simple_string("OK"))
        return queued

    # INCR
    if cmd_text == "INCR":
        if len(decoded) < 2:
            connection.sendall(error_str("ERR wrong number of arguments for 'INCR'"))
            return queued
        k = decoded[1]
        if check_expired(k):
            # removed
            db.pop(k, None)
        ent = db.get(k)
        if ent is None:
            nv = 1
        else:
            try:
                nv = int(ent["value"].decode()) + 1
            except Exception:
                connection.sendall(error_str("ERR value is not an integer or out of range"))
                return queued
        db[k] = {"value": str(nv).encode(), "expiry": None}
        connection.sendall(resp_encode(nv))
        return queued

    # RPUSH
    if cmd_text == "RPUSH":
        if len(decoded) < 3:
            connection.sendall(error_str("ERR wrong number of arguments for 'RPUSH'"))
            return queued
        k = decoded[1]
        vals = decoded[2:]
        if k not in store_lists:
            store_lists[k] = []
        store_lists[k].extend(vals)
        # wake BLPOP waiters
        while k in blocked and blocked[k] and len(store_lists[k])>0:
            conn = blocked[k].pop(0)
            v = store_lists[k].pop(0)
            try:
                conn.sendall(resp_encode([k, v]))
            except:
                pass
        connection.sendall(resp_encode(len(store_lists[k])))
        return queued

    # LRANGE
    if cmd_text == "LRANGE":
        if len(decoded) < 4:
            connection.sendall(error_str("ERR wrong number of arguments for 'LRANGE'"))
            return queued
        k = decoded[1]; s = int(decoded[2]); e = int(decoded[3])
        arr = store_lists.get(k, [])
        # adapt negative
        if e < 0:
            e = len(arr) + e
        if s < 0:
            s = len(arr) + s
        if s < 0: s = 0
        if e < 0:
            result = []
        else:
            result = arr[s:e+1]
        connection.sendall(resp_encode(result))
        return queued

    # BLPOP
    if cmd_text == "BLPOP":
        if len(decoded) < 3:
            connection.sendall(error_str("ERR wrong number of arguments for 'BLPOP'"))
            return queued
        k = decoded[1]; timeout = float(decoded[2].decode())
        if k in store_lists and len(store_lists[k])>0:
            v = store_lists[k].pop(0)
            connection.sendall(resp_encode([k, v]))
            return queued
        # block
        if k not in blocked:
            blocked[k] = []
        blocked[k].append(connection)
        def bl_timeout():
            time.sleep(timeout)
            if k in blocked and connection in blocked[k]:
                try:
                    connection.sendall(b"*-1\r\n")  # null
                except:
                    pass
                blocked[k].remove(connection)
        if timeout>0:
            threading.Thread(target=bl_timeout, daemon=True).start()
        return queued

    # XADD (simple)
    if cmd_text == "XADD":
        if len(decoded) < 4:
            connection.sendall(error_str("ERR wrong number of arguments for 'XADD'"))
            return queued
        key = decoded[1]
        seq = decoded[2]
        fields = decoded[3:]
        if key not in streams:
            streams[key] = []
        # allot id
        if seq == b'*':
            id_ms = str(now_ms()).encode()
            seq_id = b"0"
            entry_id = id_ms + b"-" + seq_id
        else:
            entry_id = seq
        entry = {"id": entry_id}
        for i in range(0, len(fields), 2):
            entry[fields[i]] = fields[i+1]
        streams[key].append(entry)
        # notify blocked xread
        if key in blocked_xread and blocked_xread[key]:
            pend = blocked_xread[key][:]
            blocked_xread[key] = []
            for conn, last in pend:
                res = []
                for e in streams[key]:
                    if e["id"] > last:
                        # convert
                        fields_list = []
                        for kf, vf in e.items():
                            if kf!="id":
                                fields_list.append(kf)
                                fields_list.append(vf)
                        res.append([key, [[e["id"], fields_list]]])
                if res:
                    try:
                        conn.sendall(resp_encode(res))
                    except:
                        pass
        connection.sendall(resp_encode(entry_id))
        return queued

    # XRANGE
    if cmd_text == "XRANGE":
        if len(decoded) < 4:
            connection.sendall(error_str("ERR wrong number of arguments for 'XRANGE'"))
            return queued
        k = decoded[1]; start = decoded[2]; end = decoded[3]
        if k not in streams:
            connection.sendall(resp_encode([]))
            return queued
        res = []
        for e in streams[k]:
            # simple bytewise compare works because entries are numeric strings
            if (start==b"-" or e["id"]>=start) and (end==b"+" or e["id"]<=end):
                fl = []
                for fk, fv in e.items():
                    if fk!="id":
                        fl.append(fk); fl.append(fv)
                res.append([e["id"], fl])
        connection.sendall(resp_encode(res))
        return queued

    # XREAD (non-blocking)
    if cmd_text == "XREAD":
        # format: XREAD STREAMS key id
        # simplistic: support XREAD STREAMS key id
        if len(decoded) < 4:
            connection.sendall(error_str("ERR wrong number of arguments for 'XREAD'"))
            return queued
        if decoded[1].upper() == b"STREAMS":
            key = decoded[2]; last = decoded[3]
            res = []
            if key in streams:
                entries = []
                for e in streams[key]:
                    if e["id"] > last:
                        fl = []
                        for fk,fv in e.items():
                            if fk!="id":
                                fl.append(fk); fl.append(fv)
                        entries.append([e["id"], fl])
                if entries:
                    res.append([key, entries])
            connection.sendall(resp_encode(res))
            return queued

    # CONFIG GET
    if cmd_text == "CONFIG":
        if len(decoded)>=3 and decoded[1].upper() == b"GET":
            param = decoded[2]
            if param == b"dir":
                connection.sendall(resp_encode([b"dir", b"./"]))
            elif param == b"dbfilename":
                connection.sendall(resp_encode([b"dbfilename", b"dump.rdb"]))
            else:
                connection.sendall(resp_encode([]))
            return queued

    # INFO
    if cmd_text == "INFO":
        s = b"role:master\n"
        connection.sendall(resp_encode(s))
        return queued

    # PUB/SUB - SUBSCRIBE (basic)
    if cmd_text == "SUBSCRIBE":
        channels = decoded[1:]
        if connection not in subscriptions:
            subscriptions[connection] = set()
        subscriber_mode.add(connection)
        for ch in channels:
            if ch not in channel_subs:
                channel_subs[ch] = set()
            if connection not in channel_subs[ch]:
                channel_subs[ch].add(connection)
                subscriptions[connection].add(ch)
            connection.sendall(resp_encode([b"subscribe", ch, str(len(subscriptions[connection])).encode()]))
        return queued

    if cmd_text == "UNSUBSCRIBE":
        channels = decoded[1:] if len(decoded)>1 else list(subscriptions.get(connection,[]))
        for ch in channels:
            if ch in channel_subs and connection in channel_subs[ch]:
                channel_subs[ch].remove(connection)
            if connection in subscriptions:
                subscriptions[connection].discard(ch)
            connection.sendall(resp_encode([b"unsubscribe", ch, str(len(subscriptions.get(connection,[]))).encode()]))
        if not subscriptions.get(connection):
            subscriber_mode.discard(connection)
        return queued

    if cmd_text == "PUBLISH":
        if len(decoded) < 3:
            connection.sendall(error_str("ERR wrong number of arguments for 'PUBLISH'"))
            return queued
        ch = decoded[1]; msg = decoded[2]
        count = 0
        if ch in channel_subs:
            for conn in list(channel_subs[ch]):
                try:
                    conn.sendall(resp_encode([b"message", ch, msg]))
                    count += 1
                except:
                    pass
        connection.sendall(resp_encode(count))
        return queued

    # ZADD - create sorted set with single member (stage CT1)
    if cmd_text == "ZADD":
        if len(decoded) < 4:
            connection.sendall(error_str("ERR wrong number of arguments for 'ZADD'"))
            return queued
        zkey = decoded[1]
        score_bytes = decoded[2]
        member = decoded[3]
        try:
            score = float(score_bytes.decode())
        except:
            connection.sendall(error_str("ERR invalid score"))
            return queued
        if zkey not in sorted_set_dict:
            sorted_set_dict[zkey] = []
        # check if member exists
        exists = None
        for i, it in enumerate(sorted_set_dict[zkey]):
            if it[1] == member:
                exists = i
                break
        if exists is not None:
            # update: remove and push new
            sorted_set_dict[zkey].pop(exists)
            heapq.heappush(sorted_set_dict[zkey], (score, member))
            connection.sendall(resp_encode(0))
            return queued
        else:
            heapq.heappush(sorted_set_dict[zkey], (score, member))
            connection.sendall(resp_encode(1))
            return queued

    # default unknown
    connection.sendall(error_str("ERR unknown command"))
    return queued

# --- Client loop / server ---
def handle_client(conn, config, initial_data=b""):
    global prev_cmd
    buf = initial_data or b""
    queued = False
    executing = False
    with conn:
        while True:
            if not buf:
                try:
                    chunk = conn.recv(65536)
                except Exception:
                    break
                if not chunk:
                    break
                buf += chunk
            msgs = parse_all(buf)
            if not msgs:
                buf = b""
                continue
            for msg in msgs:
                try:
                    # msg is list of elements (bytes/str/int)
                    decoded = msg
                    # ensure elements are bytes or ints; parse_next returns bytes for bulk
                    # call executor
                    queued = cmd_executor(conn, decoded, config, queued, executing)
                    prev_cmd = decoded[0] if decoded else prev_cmd
                except Exception as e:
                    try:
                        conn.sendall(error_str("ERR " + str(e)))
                    except:
                        pass
            buf = b""

class Master:
    def __init__(self, port, dir_, dbfilename):
        self.port = port
        self.dir = dir_
        self.dbfilename = dbfilename
        self.config = {'role':'master','master_replid':'mreplid','master_replid_offset':'0','dir':dir_,'dbfilename':dbfilename,'store':{}}
        read_key_val_from_db(self.config['dir'], self.config['dbfilename'], self.config['store'])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", self.port))
        s.listen(10)
        while True:
            c,_ = s.accept()
            threading.Thread(target=handle_client, args=(c,self.config), daemon=True).start()

class Slave:
    def __init__(self, port, replicaof, dir_, dbfilename):
        host, p = replicaof.split(' ')
        p = int(p)
        self.config = {'role':'slave','master_host':host,'master_port':p,'dir':dir_,'dbfilename':dbfilename,'store':{}}
        read_key_val_from_db(dir_, dbfilename, self.config['store'])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", port))
        s.listen(10)
        # handshake
        master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_sock.connect((host,p))
        master_sock.sendall(resp_encode([b"PING"]))
        try:
            data = master_sock.recv(65536)
        except:
            data = b""
        master_sock.sendall(resp_encode([b"REPLCONF", b"listening-port", str(port).encode()]))
        try:
            data = master_sock.recv(65536)
        except:
            data = b""
        master_sock.sendall(resp_encode([b"REPLCONF", b"capa", b"psync2"]))
        try:
            data = master_sock.recv(65536)
        except:
            data = b""
        master_sock.sendall(resp_encode([b"PSYNC", b"?", b"-1"]))
        try:
            data = master_sock.recv(65536)
        except:
            data = b""
        threading.Thread(target=handle_client, args=(master_sock,self.config,data), daemon=True).start()
        while True:
            c,_ = s.accept()
            threading.Thread(target=handle_client, args=(c,self.config), daemon=True).start()

# simple RDB reader used earlier
def read_key_val_from_db(dir, dbfilename, data):
    path = os.path.join(dir, dbfilename)
    if not os.path.isfile(path):
        return
    # minimal: not used often in tests
    try:
        with open(path,'rb') as f:
            pass
    except:
        pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, default="")
    parser.add_argument("--dir", type=str, default="")
    parser.add_argument("--dbfilename", type=str, default="")
    args = parser.parse_args()
    if args.replicaof:
        Slave(args.port, args.replicaof, args.dir, args.dbfilename)
    else:
        Master(args.port, args.dir, args.dbfilename)

if __name__ == "__main__":
    main()