# main.py
# Complete Redis server (Codecrafters) — all modules merged

import argparse
import asyncio
import datetime
import os
import struct
import time

# =========================================================
# RESP PARSER + ENCODER
# =========================================================

def parse(data: bytes):
    """Parse RESP array input from redis-cli"""
    if not data or data[0:1] != b"*":
        return []
    lines = data.split(b"\r\n")
    num_elems = int(lines[0][1:])
    i = 1
    result = []
    while num_elems > 0:
        if lines[i].startswith(b"$"):
            length = int(lines[i][1:])
            result.append(lines[i + 1].decode())
            i += 2
        num_elems -= 1
    return result


def encode(data):
    """Encode Python data to RESP protocol"""
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, str):
        return f"+{data}\r\n".encode()
    if isinstance(data, int):
        return f":{data}\r\n".encode()
    if isinstance(data, list):
        resp = f"*{len(data)}\r\n".encode()
        for item in data:
            if item is None:
                resp += b"$-1\r\n"
            else:
                s = str(item).encode()
                resp += f"${len(s)}\r\n".encode() + s + b"\r\n"
        return resp
    return f"-ERR unknown type\r\n".encode()


# =========================================================
# RDB READER (for persistence)
# =========================================================

def read_key_val_from_db(dir, dbfilename, data):
    rdb_file_loc = os.path.join(dir, dbfilename)
    if not os.path.isfile(rdb_file_loc):
        return

    with open(rdb_file_loc, "rb") as f:
        while b := f.read(1):
            if b == b"\xfb":
                break

        num_keys = struct.unpack("B", f.read(1))[0]
        f.read(1)  # DB selector
        for _ in range(num_keys):
            expired = False
            b = f.read(1)
            if b == b"\xfc":
                milliTime = int.from_bytes(f.read(8), "little")
                if milliTime < datetime.datetime.now().timestamp() * 1000:
                    expired = True
                f.read(1)
            elif b == b"\xfd":
                secTime = int.from_bytes(f.read(4), "little")
                if secTime < datetime.datetime.now().timestamp():
                    expired = True
                f.read(1)

            key_len = struct.unpack("B", f.read(1))[0] & 0b00111111
            key = f.read(key_len).decode()
            val_len = struct.unpack("B", f.read(1))[0] & 0b00111111
            val = f.read(val_len).decode()
            if not expired:
                data[key] = (val, -1)


# =========================================================
# REDIS CORE SERVER
# =========================================================

async def serveRedis():
    config = getConfig()
    server = await asyncio.start_server(
        lambda r, w: handleClient(config, r, w),
        config["host"],
        config["port"]
    )
    print(f"✅ Redis server started on {config['host']}:{config['port']}")
    async with server:
        await server.serve_forever()


def getConfig():
    flag = argparse.ArgumentParser()
    flag.add_argument("--dir", default=".", help="Directory for redis files")
    flag.add_argument("--dbfilename", default="dump.rdb", help="Database filename")
    flag.add_argument("--port", default="6379", help="Port to listen on")
    flag.add_argument("--replicaof", help="Replication target (format: HOST PORT)")
    args = flag.parse_args()

    config = {
        "store": {},
        "port": int(args.port),
        "host": "0.0.0.0",
        "role": "master",
        "dbfilename": args.dbfilename,
        "dir": args.dir,
        "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        "master_repl_offset": "0",
        "replicaof": args.replicaof,
    }

    read_key_val_from_db(config["dir"], config["dbfilename"], config["store"])
    return config


async def handleClient(config, reader, writer):
    while True:
        raw = await reader.read(1024)
        if not raw:
            break
        command = parse(raw)
        await execute(config, command, writer)
    writer.close()


async def execute(config, command, writer):
    if not command:
        return

    cmd = command[0].lower()
    args = command[1:]
    store = cleanStore(config["store"])

    # -------------------- BASIC COMMANDS --------------------
    if cmd == "ping":
        writer.write(encode("PONG"))

    elif cmd == "echo" and args:
        writer.write(encode(args[0]))

    elif cmd == "set" and len(args) >= 2:
        key, value = args[0], args[1]
        expiry = -1
        if len(args) == 4 and args[2].lower() == "px" and args[3].isdigit():
            expiry = time.time() + int(args[3]) / 1000
        store[key] = (value, expiry)
        config["store"] = store
        writer.write(encode("OK"))

    elif cmd == "get" and len(args) == 1:
        key = args[0]
        val = store.get(key)
        if not val:
            writer.write(b"$-1\r\n")
        else:
            v, exp = val
            if exp != -1 and exp < time.time():
                del store[key]
                writer.write(b"$-1\r\n")
            else:
                writer.write(f"${len(v)}\r\n{v}\r\n".encode())

    elif cmd == "del" and args:
        deleted = 0
        for k in args:
            if k in store:
                del store[k]
                deleted += 1
        writer.write(f":{deleted}\r\n".encode())

    elif cmd == "config" and len(args) > 1 and args[0].lower() == "get":
        result = []
        for a in args[1:]:
            if a in config:
                result.extend([a, config[a]])
        writer.write(encode(result))

    elif cmd == "keys" and len(args) == 1 and args[0] == "*":
        writer.write(encode(list(store.keys())))

    elif cmd == "info" and len(args) == 1 and args[0].lower() == "replication":
        lines = [f"role:{config['role']}"]
        if config["role"] == "master":
            lines.append(f"master_replid:{config['master_replid']}")
            lines.append(f"master_repl_offset:{config['master_repl_offset']}")
        writer.write(encode("\n".join(lines)))

    elif cmd == "replconf":
        writer.write(encode("OK"))

    elif cmd == "psync":
        fullsync = f"FULLRESYNC {config['master_replid']} {config['master_repl_offset']}"
        writer.write(encode(fullsync))
        file_contents = bytes.fromhex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        )
        writer.write(f"${len(file_contents)}\r\n".encode() + file_contents + b"\r\n")

    elif cmd == "blpop" and len(args) >= 2:
        key, timeout = args[0], float(args[1])
        end_time = time.time() + timeout
        while time.time() < end_time:
            if key in store:
                val, _ = store.pop(key)
                writer.write(encode([key, val]))
                return
            await asyncio.sleep(0.05)
        writer.write(b"*-1\r\n")  # RESP null array

    else:
        writer.write(b"-ERR unknown command\r\n")

    await writer.drain()


def cleanStore(store):
    now = time.time()
    return {k: v for k, v in store.items() if v[1] == -1 or v[1] > now}


# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    asyncio.run(serveRedis())