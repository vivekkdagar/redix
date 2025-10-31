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
bl_pop_queue = {}
bl_pop_lock = threading.Lock()
xread_block_queue = {}
xread_lock = threading.Lock()
processed_bytes = 0
sorted_set_dict = {}
subscribe_dict = {}
subscriber_dict = {}
replica_ack_counter = 0
replica_ack_lock = threading.Lock()
prev_command = ""
subscriber_allowed_commands = [b"SUBSCRIBE", b"UNSUBSCRIBE", b"PUBLISH", b"PSUBSCRIBE", b"PUNSUBSCRIBE", b"PING", b"QUIT"]

@dataclasses.dataclass
class Args:
    port: int
    replicaof: Optional[str]
    dir: str
    dbfilename: str


def parse_single(data: bytes):
    if not data:
        raise ValueError("Not enough data")
    first_byte = data[0:1]
    match first_byte:
        case b"*":
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
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            length = int(first_line[1:].decode())
            if length == -1:
                return None, remaining
            if len(remaining) < length:
                raise ValueError("Not enough data for bulk string")
            blk = remaining[:length]
            remaining = remaining[length:]
            if remaining.startswith(b"\r\n"):
                remaining = remaining[2:]
            return blk, remaining
        case b"+":
            first_line, remaining = data.split(b"\r\n", 1)
            return first_line[1:].decode(), remaining
        case b":":
            first_line, remaining = data.split(b"\r\n", 1)
            return int(first_line[1:].decode()), remaining
        case b"-":
            first_line, remaining = data.split(b"\r\n", 1)
            return first_line[1:].decode(), remaining
        case _:
            raise RuntimeError(f"Parse not implemented: {first_byte}")


def parse_next(data: bytes):
    commands = []
    remaining_data = data
    while remaining_data:
        first_byte = remaining_data[0:1]
        if first_byte not in [b"*", b"$", b"+", b":", b"-"]:
            break
        try:
            command, remaining_data = parse_single(remaining_data)
            commands.append(command)
        except (ValueError, IndexError):
            break
    return commands, remaining_data


def encode_resp(data: Any, trailing_crlf: bool = True) -> bytes:
    if isinstance(data, bytes):
        return b"$%d\r\n%b\r\n" % (len(data), data)
    if isinstance(data, str):
        if data.startswith("-"):
            return f"{data}\r\n".encode()
        return f"+{data}\r\n".encode()
    if isinstance(data, int):
        return f":{data}\r\n".encode()
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, list):
        return b"*%d\r\n%b" % (len(data), b"".join(map(encode_resp, data)))
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
    global transaction_enabled, transactions, processed_bytes, prev_command
    while data or (data := conn.recv(65536)):
        if is_replica_conn:
            processed_bytes += len(data)
        commands, data = parse_next(data)
        for value in commands:
            if conn not in transaction_enabled:
                transaction_enabled[conn] = False
                transactions[conn] = []
            response = handle_command(args, value, conn, is_replica_conn)
            prev_command = value[0]
            if not transaction_enabled[conn] and response != "custom":
                conn.send(encode_resp(response))

def queue_transaction(command: List, conn: socket.socket):
    if conn in transaction_enabled and transaction_enabled[conn]:
        transactions[conn].append(command)
        conn.send(encode_resp("QUEUED"))
        return True
    return False

def handle_command(args: Args, value: List, conn: socket.socket, is_replica_conn: bool = False) -> Any:
    global db
    now = datetime.datetime.now()
    match value:
        case [b"PING"]:
            return "PONG"
        case [b"ECHO", s]:
            return s
        case [b"GET", k]:
            if not queue_transaction(value, conn):
                db_value = db.get(k)
                if db_value is None:
                    return None
                expiry = db_value.expiry
                if expiry is not None and expiry <= now:
                    db.pop(k, None)
                    return None
                return db_value.value.encode() if isinstance(db_value.value, str) else db_value.value
        case [b"SET", k, v]:
            if not queue_transaction(value, conn):
                db[k] = rdb_parser.Value(value=v, expiry=None)
                return "OK"
        case [b"MULTI"]:
            transaction_enabled[conn] = True
            return "OK"
        case [b"EXEC"]:
            if not transaction_enabled[conn]:
                return "-ERR EXEC without MULTI"
            transaction_enabled[conn] = False
            results = []
            for cmd in transactions[conn]:
                results.append(handle_command(args, cmd, conn, is_replica_conn))
            transactions[conn] = []
            return results
        case _:
            return "-ERR unknown command"

def main(args: Args):
    global db
    db = rdb_parser.read_file_and_construct_kvm(args.dir, args.dbfilename)
    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    with server_socket:
        while True:
            conn, _ = server_socket.accept()
            threading.Thread(target=handle_conn, args=(args, conn), daemon=True).start()

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--port", type=int, default=6379)
    args.add_argument("--replicaof", required=False)
    args.add_argument("--dir", default=".")
    args.add_argument("--dbfilename", default="empty.rdb")
    parsed = args.parse_args()
    main(Args(port=parsed.port, replicaof=parsed.replicaof, dir=parsed.dir, dbfilename=parsed.dbfilename))