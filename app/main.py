import dataclasses
import socket
import threading
import datetime
import argparse
from typing import Any, Dict, Optional, cast, List

from app import rdb_parser

EMPTY_RDB = bytes.fromhex(
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)
transaction_enabled = {}
transactions = {}


@dataclasses.dataclass
class Args:
    port: int
    replicaof: Optional[str]


def parse_next(data: bytes):
    first, data = data.split(b"\r\n", 1)
    match first[:1]:
        case b"*":
            value = []
            l = int(first[1:].decode())
            for _ in range(l):
                item, data = parse_next(data)
                value.append(item)
            return value, data
        case b"$":
            l = int(first[1:].decode())
            blk = data[:l]
            data = data[l + 2 :]
            return blk, data

        case b"+":
            return first[1:].decode(), data

        case _:
            raise RuntimeError(f"Parse not implemented: {first[:1]}")


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
        if data[0] == "-":
            return b"%b\r\n" % (data.encode(),)
        return b"+%b\r\n" % (data.encode(),)
    if isinstance(data, int):
        return b":%b\r\n" % (str(data).encode())
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, list):
        return b"*%b\r\n%b" % (
            str(len(data)).encode(),
            b"".join(map(encode_resp, data)),
        )

    raise RuntimeError(f"Encode not implemented: {data}")


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
    global transaction_enabled, transactions

    while data or (data := conn.recv(4096)):
        value, data = parse_next(data)
        trailing_crlf = True

        if conn not in transaction_enabled.keys():
            transaction_enabled[conn] = False
            transactions[conn] = []

        response = handle_command(value, conn, is_replica_conn, trailing_crlf)

        if not transaction_enabled[conn]:
            encoded_resp = encode_resp(response, trailing_crlf)
            conn.send(encoded_resp)


def handle_command(
    value: List,
    conn: socket.socket,
    is_replica_conn: bool = False,
    trailing_crlf: bool = True,
) -> str | None | List[Any]:
    global transaction_enabled, transactions
    response = b""
    match value:
        case [b"PING"]:
            response = "PONG"
        case [b"ECHO", s]:
            response = s
        case [b"GET", k]:
            if not queue_transaction(value, conn):
                now = datetime.datetime.now()
                value = db.get(k)
                if value is None:
                    response = None
                elif value.expiry is not None and now >= value.expiry:
                    db.pop(k)
                    response = None
                else:
                    response = value.value
        case [b"INFO", b"replication"]:
            if args.replicaof is None:
                info = f"""\
role:master
master_replid:{replication.master_replid}
master_repl_offset:{replication.master_repl_offset}
""".encode()
            else:
                info = b"""role:slave\n"""
            response = info
        case [b"REPLCONF", b"listening-port", port]:
            response = "OK"
        case [b"REPLCONF", b"capa", b"psync2"]:
            response = "OK"
        case [b"REPLCONF", b"GETACK", b"*"]:
            response = ["REPLCONF", "ACK", "0"]
        case [b"PSYNC", replid, offset]:
            response = (
                f"FULLRESYNC "
                f"{replication.master_replid} "
                f"{replication.master_repl_offset}"
            )

            response = EMPTY_RDB
            trailing_crlf = False
            # response = ["REPLCONF", "GETACK", "*"])
            replication.connected_replicas.append(conn)
            # print('sent')
        case [b"REPLCONF", b"GETACK", b"*"]:
            response = ["REPLCONF", "ACK", "0"]
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
                response = handle_transaction(conn, is_replica_conn)
                transactions[conn] = []

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
                response = db[k].value[int(s) : int(e) + 1]
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
                db[k] = rdb_parser.Value(
                    value=v,
                    expiry=None,
                )
                if not is_replica_conn:
                    response = "OK"
        case _:
            raise RuntimeError(f"Command not implemented: {value}")

    return response


def queue_transaction(command: List, conn: socket.socket):
    global transaction_enabled, transactions
    if conn in transaction_enabled.keys() and transaction_enabled[conn] == True:
        transactions[conn].append(command)
        conn.send(encode_resp("QUEUED"))
        return True
    return False


def handle_transaction(conn: socket.socket, is_replica_conn: bool):
    global transactions
    response = []
    for transaction in transactions[conn]:
        response.append(handle_command(transaction, conn, is_replica_conn))
    print(response)
    return response


def main(args: Args):
    global db
    db = rdb_parser.read_file_and_construct_kvm(args.dir, args.dbfilename)
    server_socket = socket.create_server(
        ("localhost", args.port),
        reuse_port=True,
    )
    if args.replicaof is not None:
        (host, port) = args.replicaof.split(" ")
        port = int(port)
        master_conn = socket.create_connection((host, port))

        # Handshake PING
        master_conn.send(encode_resp([b"PING"]))
        resp, _ = parse_next(master_conn.recv(4096))
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
        resp, _ = parse_next(master_conn.recv(4096))
        assert resp == "OK"
        # Handshake REPLCONF capabilities
        master_conn.send(encode_resp([b"REPLCONF", b"capa", b"psync2"]))
        resp, _ = parse_next(master_conn.recv(4096))
        assert resp == "OK"

        # Handshake PSYNC
        master_conn.send(encode_resp([b"PSYNC", b"?", b"-1"]))
        resp, _ = parse_next(master_conn.recv(4096))
        assert isinstance(resp, str)
        assert resp.startswith("FULLRESYNC")
        # Receive db
        resp, _ = parse_next(master_conn.recv(4096))

        print(f"Handshake with master completed: {resp=}")

        threading.Thread(
            target=handle_conn,
            args=(args, master_conn, True),
            daemon=True,
        ).start()
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
    main(cast(Args, args.parse_args()))