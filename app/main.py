import dataclasses
import socket
import threading
import datetime
import argparse
import time
from datetime import timedelta
from typing import Any, Dict, Optional, List, Tuple

from app import rdb_parser
from app.rdb_parser import XADDValue
import heapq

EMPTY_RDB = bytes.fromhex(
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)
transaction_enabled = {}
transactions = {}
bl_pop_queue = {}  # {key: [conn,...] }
bl_pop_lock = threading.Lock()
xread_block_queue = {}  # {key: [{'conn': conn, 'keys': keys, 'expiry': time, 'event': threading.Event()}, ...]}
xread_lock = threading.Lock()
processed_bytes = 0
sorted_set_dict = {}  # {key: [], ...}
subscribe_dict = {}  # {channel: [conn, ...]}
subscriber_dict = {}  # {conn: [channel, ...]}
subscriber_allowed_commands = [b"SUBSCRIBE", b"UNSUBSCRIBE", b"PUBLISH", b"PSUBSCRIBE", b"PUNSUBSCRIBE", b"PING",
                               b"QUIT"]


@dataclasses.dataclass
class Args:
    port: int
    replicaof: Optional[str]
    dir: str
    dbfilename: str


def parse_single(data: bytes):
    """Parse a single RESP command from data"""
    if not data:
        raise ValueError("Not enough data")

    first_byte = data[0:1]

    match first_byte:
        case b"*":
            # Array - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            count = int(first_line[1:].decode())

            value = []
            for _ in range(count):
                item, remaining = parse_single(remaining)
                value.append(item)
            return value, remaining

        case b"$":
            # Bulk string - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            length = int(first_line[1:].decode())

            if length == -1:  # Null bulk string
                return None, remaining
            if len(remaining) < length:  # Not enough data for content
                raise ValueError("Not enough data for bulk string")

            blk = remaining[:length]
            remaining = remaining[length:]

            # Check if next 2 bytes are \r\n and consume them if present
            if remaining.startswith(b"\r\n"):
                remaining = remaining[2:]

            return blk, remaining

        case b"+":
            # Simple string - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            return first_line[1:].decode(), remaining

        case b":":
            # Integer - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            return int(first_line[1:].decode()), remaining

        case b"-":
            # Error - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            return first_line[1:].decode(), remaining

        case _:
            raise RuntimeError(f"Parse not implemented: {first_byte}")


def parse_next(data: bytes):
    """Parse all RESP commands from data and return list of commands"""
    print("next_data", data[:100] + b"..." if len(data) > 100 else data)
    commands = []
    remaining_data = data

    while remaining_data:
        if not remaining_data:
            break
        first_byte = remaining_data[0:1]
        if first_byte not in [b"*", b"$", b"+", b":", b"-"]:
            print(f"Unknown command start: {first_byte}, remaining: {remaining_data[:20]}...")
            break
        try:
            command, remaining_data = parse_single(remaining_data)
            commands.append(command)
            # print(f"Parsed command: {command}")
        except (ValueError, IndexError) as e:
            print(f"Parse error: {e}, stopping parse")
            break

    return commands, remaining_data


def encode_resp(
        data: Any, trailing_crlf: bool = True, encoded_list: bool = False
) -> bytes:
    if isinstance(data, bytes):
        return b"$%b\r\n%b%b" % (
            str(len(data)).encode(),
            data,
            b"\r\n" if trailing_crlf else b"",
        )
    if isinstance(data, str):
        if len(data) > 0 and data[0] == "-":  # Error Messages
            return b"%b\r\n" % (data.encode(),)
        return b"+%b\r\n" % (data.encode(),)
    if isinstance(data, int):
        return b":%b\r\n" % (str(data).encode())
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, list) or isinstance(data, tuple):
        return b"*%b\r\n%b" % (
            str(len(data)).encode(),
            b"".join(map(encode_resp, data)),
        )

    raise RuntimeError(f"Encode not implemented: {data}, {type(data)}")


db: Dict[Any, rdb_parser.Value] = {}


@dataclasses.dataclass
class Replication:
    master_replid: str
    master_repl_offset: int
    connected_replicas: List[socket.socket]


replication = Replication(
    master_replid="8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    master_repl_offset=0,
    connected_replicas=[],
)


def handle_conn(args: Args, conn: socket.socket, is_replica_conn: bool = False):
    data = b""
    global transaction_enabled, transactions, processed_bytes

    while data or (data := conn.recv(65536)):
        # FIX: Does not differentiate between separate client and master connection
        if is_replica_conn:
            processed_bytes += len(data)
        commands, data = parse_next(data)

        for value in commands:
            trailing_crlf = True

            if conn not in transaction_enabled.keys():
                transaction_enabled[conn] = False
                transactions[conn] = []
            print("handle_conn, command: ", value)
            response = handle_command(args, value, conn, is_replica_conn, trailing_crlf)

            if not transaction_enabled[conn] and response != "custom":
                encoded_resp = encode_resp(response, trailing_crlf)
                print("Encoded_response", encoded_resp)
                conn.send(encoded_resp)


def handle_neg_index(s: int, e: int, list_len: int):
    if s < 0:
        if abs(s) <= list_len:
            s = list_len + s
        else:
            s = 0
    if e < 0:
        if abs(e) <= list_len:
            e = list_len + e
        else:
            e = 0
    if e >= list_len:
        e = list_len - 1

    if s >= list_len:
        s = list_len - 1

    return s, e


def handle_blpop(k: str, t: datetime.datetime, conn: socket.socket):
    while True:
        if datetime.datetime.now() > t:
            conn.send(encode_resp(None))
            return
        else:
            with bl_pop_lock:
                if k in db.keys() and len(db[k].value) > 0:
                    conn = bl_pop_queue[k].pop(0)
                    conn.send(encode_resp([k, db[k].value[0]]))
                    db[k].value = db[k].value[1:]
                    return


def generate_sequence(key, m_second, seq) -> Tuple[int, int]:
    global db

    if m_second == "":
        return round(time.time() * 1000), 0

    if key in db.keys():
        if db[key].value[-1].milliseconds == int(m_second):
            if seq == "*":
                return db[key].value[-1].milliseconds, db[key].value[-1].sequence + 1
            return db[key].value[-1].milliseconds, int(seq)
        else:
            if seq != "*":
                return int(m_second), int(seq)
            return int(m_second), 0
    else:
        if m_second == "0":
            if seq != "*":
                return int(m_second), int(seq)
            return 0, 1
        if seq != "*":
            return int(m_second), int(seq)
        return int(m_second), 0


def handle_subscriber_command(value: List, conn: socket.socket):
    global subscriber_dict, subscriber_allowed_commands
    response = "custom"
    if conn in subscriber_dict:
        if value[0] not in subscriber_allowed_commands:
            response = f"-ERR Can't execute '{value[0].decode()}' in subscribed mode"
    return response


def handle_command(
        args: Args,
        value: List,
        conn: socket.socket,
        is_replica_conn: bool = False,
        trailing_crlf: bool = True,
) -> bytes | str | None | List[Any]:
    global transaction_enabled, transactions, sorted_set_dict, subscribe_dict, subscriber_dict
    response = "custom"
    print("handle_command", value, is_replica_conn)

    response = handle_subscriber_command(value, conn)
    if response != "custom":
        return response

    match value:
        case [b"PING"]:
            if conn in subscriber_dict:
                response = [b'pong', b'']
            elif not is_replica_conn:
                response = "PONG"
        case [b"ECHO", s]:
            response = s
        case [b"GET", k]:
            if not queue_transaction(value, conn):
                now = datetime.datetime.now()
                db_val = db.get(k)
                if db_val is None:
                    # ✅ Return empty bulk string ($0\r\n\r\n) instead of null bulk ($-1\r\n)
                    response = b"$0\r\n\r\n"
                elif db_val.expiry is not None and now >= db_val.expiry:
                    db.pop(k)
                    response = b"$0\r\n\r\n"
                else:
                    val = db_val.value
                    # Always return proper bulk string
                    response = b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
        case [b"INFO", b"replication"]:
            if args.replicaof is None:
                response = f"""\
role:master
master_replid:{replication.master_replid}
master_repl_offset:{replication.master_repl_offset}
""".encode()
            else:
                response = b"""role:slave\n"""
        case [b"REPLCONF", b"listening-port", port]:
            response = "OK"
        case [b"REPLCONF", b"capa", b"psync2"]:
            response = "OK"
        case [b"REPLCONF", b"GETACK", b"*"]:
            print(processed_bytes, get_processed_bytes())
            response = [b'REPLCONF', b'ACK', str(get_processed_bytes()).encode()]
        case [b"PSYNC", replid, offset]:
            response = "custom"
            conn.send(encode_resp(
                f"FULLRESYNC "
                f"{replication.master_replid} "
                f"{replication.master_repl_offset}"
            ))
            conn.send(encode_resp(EMPTY_RDB, False))
            # conn.send(b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n')
            replication.connected_replicas.append(conn)
        case [b"DISCARD"]:
            if transaction_enabled[conn]:
                transaction_enabled[conn] = False
                transactions[conn] = []
                response = "OK"
            else:
                response = "-ERR DISCARD without MULTI"
        case [b"MULTI"]:
            transaction_enabled[conn] = True
            conn.send(encode_resp("OK"))
        case [b"EXEC"]:
            if not transaction_enabled[conn]:
                response = "-ERR EXEC without MULTI"
            else:
                transaction_enabled[conn] = False
                if len(transactions[conn]) == 0:
                    return []
                response = handle_transaction(args, conn, is_replica_conn)
                transactions[conn] = []
        case [b"TYPE", k]:
            if k in db.keys():
                if isinstance(db[k].value, list) and len(db[k].value) > 0 and isinstance(db[k].value[-1], XADDValue):
                    response = "stream"
                else:
                    response = "string"
            else:
                response = "none"

        case [b'LLEN', k]:
            if k in db.keys():
                response = len(db[k].value)
            else:
                response = 0
        case [b'LPOP', k, *v]:
            if k in db.keys():
                if len(db.keys()) == 0:
                    response = None
                else:
                    if len(v) == 0:
                        response = db[k].value[0]
                        db[k].value = db[k].value[1:]
                    else:
                        if int(v[0]) >= len(db[k].value):
                            response = db[k].value
                            db[k].value = []
                        else:
                            response = db[k].value[:int(v[0])]
                            db[k].value = db[k].value[int(v[0]):]

        case [b'BLPOP', k, t]:
            global bl_pop_queue
            if k in bl_pop_queue.keys() or k not in db.keys() or (k in db.keys() and len(db[k].value) == 0):
                if k in bl_pop_queue.keys():
                    bl_pop_queue[k].append(conn)
                else:
                    bl_pop_queue[k] = [conn]

                dt = datetime.datetime.now()
                td = timedelta(seconds=float(t))
                if float(t) == 0:
                    td = timedelta(days=1)
                dt = dt.__add__(td)
                threading.Thread(
                    target=handle_blpop,
                    args=(k, dt, conn,),
                ).start()
                response = "custom"
                # once the list is empty make sure to destory the key from dict
            else:
                response = db[k].value[0]
                db[k].value = db[k].value[1:]

        case [b"LPUSH", k, *v]:
            if not queue_transaction(value, conn):
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                if k in db.keys():
                    v = v[::-1]
                    v.extend(db[k].value)
                    db[k].value = v
                else:
                    db[k] = rdb_parser.Value(value=v[::-1], expiry=None)
                if not is_replica_conn:
                    response = len(db[k].value)

        case [b"RPUSH", k, *v]:
            if not queue_transaction(value, conn):
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                if k in db.keys():
                    db[k].value.extend(v)
                else:
                    db[k] = rdb_parser.Value(value=v, expiry=None)
                if not is_replica_conn:
                    response = len(db[k].value)
        case [b"LRANGE", k, s, e]:
            if k in db.keys():
                int_s, int_e = handle_neg_index(int(s), int(e), len(db[k].value))
                response = db[k].value[int_s: int_e + 1]
            else:
                response = []
        case [b"INCR", k]:
            if not queue_transaction(value, conn):
                db_value = db.get(k)
                if db_value is None:
                    new_value = 1
                else:
                    try:
                        current_int = int(db_value.value.decode())
                        new_value = current_int + 1
                    except Exception:
                        response = "-ERR value is not an integer or out of range"
                        return response

                db[k] = rdb_parser.Value(
                    value=str(new_value).encode(),
                    expiry=None,
                )

                response = new_value
        case [b"SET", k, v, b"px", expiry_ms]:
            if not queue_transaction(value, conn):
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                now = datetime.datetime.now()
                expiry_ms = datetime.timedelta(
                    milliseconds=int(expiry_ms.decode()),
                )
                db[k] = rdb_parser.Value(
                    value=v,
                    expiry=now + expiry_ms,
                )
                if not is_replica_conn:
                    response = "OK"
        case [b"SET", k, v]:
            if not queue_transaction(value, conn):
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                db[k] = rdb_parser.Value(value=v, expiry=None)
                if not is_replica_conn:
                    response = "OK"

        case [b"XADD", key, sequence, *kv]:
            m_second, seq = extract_msec_and_sequence(sequence.decode())
            err_msg = invalid_sequence(key, m_second, seq)
            if err_msg != "":
                response = err_msg
            else:
                m_second, seq = generate_sequence(key, m_second, seq)
                temp = dict()
                for i in range(0, len(kv), 2):
                    temp[kv[i]] = kv[i + 1]
                xadd_value = XADDValue(
                    value=temp, milliseconds=int(m_second), sequence=seq
                )

                values_list: List[XADDValue] = []
                existing = db.get(key)
                if existing is not None and isinstance(existing.value, list):
                    values_list = existing.value  # type: ignore[assignment]
                values_list.append(xadd_value)
                # Need to add value instead of replacing values
                db[key] = rdb_parser.Value(value=values_list, expiry=None)

                # Notify any blocking XREAD operations for this key
                notify_blocking_xread(key)

                response = f"{m_second}-{seq}".encode()
        case [b"XRANGE", k, start, end]:
            if k not in db or not isinstance(db[k].value, list):
                response = []
            else:
                start_id = start.decode()
                end_id = end.decode()

                if start_id == "-":
                    start_m_second, start_seq = -1, -1
                else:
                    s_ms, s_seq = extract_msec_and_sequence(start_id)
                    start_m_second, start_seq = int(s_ms), int(s_seq)

                if end_id == "+":
                    end_m_second, end_seq = 2 ** 63 - 1, 2 ** 31 - 1
                else:
                    e_ms, e_seq = extract_msec_and_sequence(end_id)
                    end_m_second, end_seq = int(e_ms), int(e_seq)

                result: List[Any] = []
                for item in db[k].value:  # type: ignore[index]
                    if not isinstance(item, XADDValue):
                        continue
                    ms = int(item.milliseconds)
                    sq = int(item.sequence)
                    in_lower = (ms > start_m_second) or (ms == start_m_second and sq >= start_seq)
                    in_upper = (ms < end_m_second) or (ms == end_m_second and sq <= end_seq)
                    if in_lower and in_upper:
                        entry_id = f"{ms}-{sq}".encode()
                        fields_and_values: List[Any] = []
                        for f, v in item.value.items():
                            fields_and_values.append(f)
                            fields_and_values.append(v)
                        result.append([entry_id, fields_and_values])
                response = result

        case [b"XREAD", b"streams", *key_and_sequence]:
            response = handle_xread(key_and_sequence)

        case [b"XREAD", b"block", expiry_ms, b"streams", *key_and_sequence]:
            resp = handle_xread(key_and_sequence, blocking=True)
            print(key_and_sequence)
            if resp and key_and_sequence[1].decode() != "$":
                response = resp
            else:
                response = "custom"
                keys, _ = extract_key_and_sequence(key_and_sequence)
                block_event = threading.Event()
                expiry_ms_decode = int(expiry_ms.decode())
                if expiry_ms_decode:
                    expiry = (int(expiry_ms.decode()) / 1000)
                else:
                    expiry = None
                with xread_lock:
                    for key in keys:
                        if key not in xread_block_queue:
                            xread_block_queue[key] = []
                        xread_block_queue[key].append({
                            'conn': conn,
                            'keys': key_and_sequence,
                            'expiry': expiry,
                            'event': block_event
                        })

                threading.Thread(
                    target=handle_xread_block,
                    args=(conn, key_and_sequence, expiry, block_event),
                    daemon=True
                ).start()

        case [b"ZRANK", zset_key, zset_member]:
            popped_elements = []
            if zset_key in sorted_set_dict:
                i = 0
                while len(sorted_set_dict[zset_key]) != 0:
                    elem = heapq.heappop(sorted_set_dict[zset_key])
                    popped_elements.append(elem)
                    if elem[1] == zset_member:
                        response = i
                        push_elements_to_sorted_set(popped_elements, sorted_set_dict, zset_key)
                        break
                    i += 1
                else:
                    push_elements_to_sorted_set(popped_elements, sorted_set_dict, zset_key)
                    response = None
            else:
                push_elements_to_sorted_set(popped_elements, sorted_set_dict, zset_key)
                response = None

        case [b"ZRANGE", zset_key, start_index, end_index]:
            if zset_key not in sorted_set_dict:
                response = []
            else:
                start_index, end_index = map(int, (start_index, end_index))
                start_index, end_index = handle_neg_index(start_index, end_index, len(sorted_set_dict[zset_key]))
                print("s, e index:", start_index, " ", end_index)

                if start_index > end_index:
                    response = []
                else:
                    # if start_index > len(values):
                    #     response = []
                    # if end_index > len(values):
                    #     end_index = len(values)
                    # else:
                    #     end_index += 1
                    popped_elements, response = [], []
                    for i in range(0, end_index + 1):
                        ele = heapq.heappop(sorted_set_dict[zset_key])
                        popped_elements.append(ele)
                        if i >= start_index:
                            response.append(ele[1])
                    push_elements_to_sorted_set(
                        popped_elements, sorted_set_dict, zset_key
                    )

        case [b"ZCARD", zset_key]:
            if zset_key in sorted_set_dict:
                response = len(sorted_set_dict[zset_key])
            else:
                response = 0

        case [b"ZREM", zset_key, zset_member]:
            if zset_key in sorted_set_dict:
                for i in range(len(sorted_set_dict[zset_key])):
                    v, k = sorted_set_dict[zset_key][i]
                    if k == zset_member:
                        response = 1
                        sorted_set_dict[zset_key].remove((v, k))
                        heapq.heapify(sorted_set_dict[zset_key])
                        break
                else:
                    response = 0
            else:
                response = None

        case [b"ZSCORE", zset_key, zset_member]:
            if zset_key in sorted_set_dict:
                for i in range(len(sorted_set_dict[zset_key])):
                    v, k = sorted_set_dict[zset_key][i]
                    print(k, zset_member)
                    if k == zset_member:
                        print("found")
                        print(v)
                        response = str(v).encode()
                        break
            if response == "custom":
                response = None

        case [b"ZADD", zset_key, value, zset_member]:
            if zset_key in sorted_set_dict:
                member_found = False
                for i in range(len(sorted_set_dict[zset_key])):
                    v, k = sorted_set_dict[zset_key][i]
                    if k == zset_member:
                        response = 0
                        sorted_set_dict[zset_key][i] = (float(value.decode()), zset_member)
                        heapq.heapify(sorted_set_dict[zset_key])
                        member_found = True
                        break
                if not member_found:
                    heapq.heappush(sorted_set_dict[zset_key], (float(value.decode()), zset_member))
                    response = 1
            else:
                sorted_set_dict[zset_key] = []
                heapq.heappush(sorted_set_dict[zset_key], (float(value.decode()), zset_member))
                response = 1
            print(sorted_set_dict[zset_key])

        case [b"SUBSCRIBE", channel]:
            response = ["subscribe", channel.decode()]
            if channel not in subscribe_dict:
                subscribe_dict[channel] = set()
            if conn not in subscriber_dict:
                subscriber_dict[conn] = 0

            if conn not in subscribe_dict[channel]:
                subscriber_dict[conn] += 1
                response.append(subscriber_dict[conn])
                subscribe_dict[channel].add(conn)
            else:
                response.append(subscriber_dict[conn])

        case [b"PUBLISH", channel, message]:
            if channel in subscribe_dict:
                response = len(subscribe_dict[channel])
            else:
                response = 0

        case _:
            raise RuntimeError(f"Command not implemented: {value}")

    return response


def push_elements_to_sorted_set(popped_elements, sorted_set_dict, zset_key):
    for ele in popped_elements:
        heapq.heappush(sorted_set_dict[zset_key], ele)


def handle_xread(key_and_sequence, blocking=False):
    global db
    keys, values = extract_key_and_sequence(key_and_sequence)
    print(f"keys: {keys}, values: {values}")
    response = []

    for key, value in zip(keys, values):
        if key in db.keys():
            db_value = db[key].value
            sequence_str = value.decode()
            m_second, seq = extract_msec_and_sequence(sequence_str)

            target_ms = int(m_second) if m_second else 0
            target_seq = int(seq) if seq else 0

            temp_key = []
            for val in db_value:
                if not isinstance(val, XADDValue):
                    continue

                val_ms = int(val.milliseconds)
                val_seq = int(val.sequence)

                if (val_ms > target_ms) or (val_ms == target_ms and val_seq > target_seq):
                    temp = []
                    for k, v in val.value.items():
                        temp.extend([k, v])
                    entry_id = f"{val_ms}-{val_seq}".encode()
                    temp_key.append([entry_id, temp])

            if temp_key:
                if blocking:
                    response.append([key, [temp_key[-1]]])
                else:
                    response.append([key, temp_key])

    return response


def extract_key_and_sequence(key_and_sequence: list) -> tuple[list[Any], list[Any]]:
    return key_and_sequence[0:len(key_and_sequence) // 2], key_and_sequence[len(key_and_sequence) // 2:]


def handle_xread_block(conn: socket.socket, key_and_sequence: list, timeout: float, block_event: threading.Event):
    global xread_block_queue

    if block_event.wait(timeout=timeout):
        resp = handle_xread(key_and_sequence, blocking=True)
        if resp:
            encoded_resp = encode_resp(resp)
            conn.send(encoded_resp)
    else:
        encoded_resp = encode_resp(None)
        conn.send(encoded_resp)

    remove_from_xread_queues(conn)


def remove_from_xread_queues(conn: socket.socket):
    """Remove a connection from all XREAD blocking queues"""
    global xread_block_queue

    with xread_lock:
        for key in list(xread_block_queue.keys()):
            for op in xread_block_queue[key]:
                if op['conn'] == conn:
                    op['event'].set()

            xread_block_queue[key] = [op for op in xread_block_queue[key] if op['conn'] != conn]
            if not xread_block_queue[key]:
                del xread_block_queue[key]


def notify_blocking_xread(key: bytes):
    """Notify any blocking XREAD operations when new entries are added"""
    global xread_block_queue

    if key not in xread_block_queue:
        return

    with xread_lock:
        operations = xread_block_queue[key][:]  # Copy list to avoid modification during iteration

        for operation in operations:
            resp = handle_xread(operation['keys'], blocking=True)
            if resp:
                operation['event'].set()

        if not xread_block_queue[key]:
            del xread_block_queue[key]


def extract_msec_and_sequence(sequence: str):
    if len(sequence) == 1 and (sequence == "*" or sequence == "$"):
        m_second, seq = "", ""
    else:
        m_second, seq = sequence.split("-")
    return m_second, seq


def invalid_sequence(key, m_second, seq) -> str:
    global db
    if m_second != "":
        if seq == "*":
            m_second = int(m_second)
            if key in db.keys():
                if db[key].value[-1].milliseconds > m_second:
                    return "-ERR The ID specified in XADD is equal or smaller than the target stream top item"
            return ""

    if m_second == "":
        return ""

    m_second, seq = int(m_second), int(seq)
    if m_second == 0 and seq == 0:
        return "-ERR The ID specified in XADD must be greater than 0-0"

    if key in db.keys():
        if db[key].value[-1].milliseconds > m_second:
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item"
        elif db[key].value[-1].milliseconds == m_second:
            if db[key].value[-1].sequence >= seq:
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item"
    return ""


def get_processed_bytes():
    global processed_bytes
    if processed_bytes - 37 < 0:
        return 0
    return processed_bytes - 37


def queue_transaction(command: List, conn: socket.socket):
    global transaction_enabled, transactions
    if conn in transaction_enabled.keys() and transaction_enabled[conn] == True:
        transactions[conn].append(command)
        conn.send(encode_resp("QUEUED"))
        return True
    return False


def handle_transaction(args: Args, conn: socket.socket, is_replica_conn: bool):
    global transactions
    response = []
    for transaction in transactions[conn]:
        response.append(handle_command(args, transaction, conn, is_replica_conn))
    return response


def main(args: Args):
    global db, processed_bytes
    db = rdb_parser.read_file_and_construct_kvm(args.dir, args.dbfilename)
    server_socket = socket.create_server(
        ("localhost", args.port),
        reuse_port=True,
    )
    if args.replicaof is not None:
        (host, port) = args.replicaof.split(" ")
        port = int(port)
        master_conn = socket.create_connection((host, port))

        t = threading.Thread(
            target=handle_conn,
            args=(args, master_conn, True),
            daemon=True,
        )
        # Handshake PING
        master_conn.send(encode_resp([b"PING"]))
        responses, _ = parse_next(master_conn.recv(65536))
        resp = responses[0] if responses else None
        assert resp == "PONG"
        # Handshake REPLCONF listening-port
        master_conn.send(
            encode_resp(
                [
                    b"REPLCONF",
                    b"listening-port",
                    str(args.port).encode(),
                ]
            )
        )
        responses, _ = parse_next(master_conn.recv(65536))
        resp = responses[0] if responses else None
        assert resp == "OK"
        # Handshake REPLCONF capabilities
        master_conn.send(encode_resp([b"REPLCONF", b"capa", b"psync2"]))
        responses, _ = parse_next(master_conn.recv(65536))
        resp = responses[0] if responses else None
        assert resp == "OK"

        # Handshake PSYNC
        master_conn.send(encode_resp([b"PSYNC", b"?", b"-1"]))

        # Handling this since the tcp messages are broken in different permutations
        responses, _ = parse_next(master_conn.recv(65536))
        count = len(responses)
        while True:
            # print("loop responses: ", responses)
            if count == 2:
                # print("1")
                break
            if count > 2:
                # print("2")
                for command in responses[2:]:
                    print(encode_resp(command), len(encode_resp(command)))
                    processed_bytes += len(encode_resp(command))
                    response = handle_command(args, command, master_conn, False)
                    print(response)
                    print("encode : ", encode_resp(response))
                    master_conn.send(encode_resp(response))
                break
            else:
                # print("3")
                temp_resp, _ = parse_next(master_conn.recv(65536))
                responses.extend(temp_resp)
                count += len(responses)

        print("PSYNC response", responses)

        t.start()
        print(f"Handshake with master completed: {resp=}")

    with server_socket:
        while True:
            (conn, _) = server_socket.accept()
            threading.Thread(
                target=handle_conn,
                args=(
                    args,
                    conn,
                    False,
                ),
                daemon=True,
            ).start()


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--port", type=int, default=6379)
    args.add_argument("--replicaof", required=False)
    args.add_argument("--dir", default=".")
    args.add_argument("--dbfilename", default="empty.rdb")

    parsed_args = args.parse_args()

    args = Args(
        port=parsed_args.port,
        replicaof=parsed_args.replicaof,
        dir=parsed_args.dir,
        dbfilename=parsed_args.dbfilename
    )

    main(args)