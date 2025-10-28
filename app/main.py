import argparse
import asyncio
import datetime
import os
import struct
import time

# ====================== RESP PARSER & ENCODER ======================

def parse(data):
    """Parse RESP data (basic array commands)."""
    if not data:
        return []
    if data.startswith(b"*"):
        parts = data.split(b"\r\n")
        n = int(parts[0][1:])
        arr = []
        i = 1
        while i < len(parts) and len(arr) < n:
            if parts[i].startswith(b"$"):
                length = int(parts[i][1:])
                arr.append(parts[i + 1].decode())
                i += 2
            else:
                i += 1
        return arr
    return [data.decode().strip()]


def encode(data):
    """Encode Python data into RESP format."""
    if isinstance(data, str):
        return f"+{data}\r\n".encode()
    elif isinstance(data, int):
        return f":{data}\r\n".encode()
    elif data is None:
        return b"$-1\r\n"
    elif isinstance(data, list):
        out = f"*{len(data)}\r\n".encode()
        for el in data:
            el_bytes = str(el).encode()
            out += f"${len(el_bytes)}\r\n".encode() + el_bytes + b"\r\n"
        return out
    else:
        s = str(data)
        return f"${len(s)}\r\n{s}\r\n".encode()


# ====================== RDB LOADER ======================

def read_key_val_from_db(dir_path, dbfilename, store):
    """Load key-value pairs from a Redis RDB file (Codecrafters subset)."""
    file_path = os.path.join(dir_path, dbfilename)
    if not os.path.isfile(file_path):
        return

    with open(file_path, "rb") as f:
        data = f.read()

    # Minimal validation
    if not data.startswith(b"REDIS"):
        return

    i = 9  # skip header
    while i < len(data):
        op = data[i:i+1]
        i += 1
        if op == b"\xfe":  # SELECT DB opcode
            i += 1
            continue
        elif op == b"\xfb":  # AUX field
            _ = _read_len(data, i)
            continue
        elif op == b"\xfc":  # expire in ms
            i += 8 + 1
        elif op == b"\xfd":  # expire in sec
            i += 4 + 1
        elif op == b"\x00":  # key-value pair
            key, i = _read_string(data, i)
            value, i = _read_string(data, i)
            store[key] = (value, -1)
        elif op == b"\xff":
            break
        else:
            break


def _read_len(data, i):
    first = data[i]
    i += 1
    enc_type = (first & 0xC0) >> 6
    if enc_type == 0:
        return first & 0x3F, i
    elif enc_type == 1:
        second = data[i]
        i += 1
        return ((first & 0x3F) << 8) | second, i
    elif enc_type == 2:
        val = struct.unpack(">I", data[i:i+4])[0]
        return val, i + 4
    else:
        return 0, i


def _read_string(data, i):
    length, i = _read_len(data, i)
    s = data[i:i+length].decode("utf-8", errors="ignore")
    return s, i + length


# ====================== REDIS SERVER ======================

async def serveRedis():
    config = getConfig()
    server = await asyncio.start_server(
        lambda r, w: handleClient(config, r, w),
        config["host"],
        config["port"]
    )
    print(f"Server started on {config['host']}:{config['port']}")
    async with server:
        await server.serve_forever()


def getConfig():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default=".")
    parser.add_argument("--dbfilename", default="dump.rdb")
    parser.add_argument("--port", default=6379, type=int)
    parser.add_argument("--replicaof", nargs=2, help="replicaof host port")
    args = parser.parse_args()

    cfg = {
        "store": {},
        "host": "0.0.0.0",
        "port": args.port,
        "role": "master",
        "dbfilename": args.dbfilename,
        "dir": args.dir,
        "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        "master_repl_offset": "0",
    }

    read_key_val_from_db(cfg["dir"], cfg["dbfilename"], cfg["store"])

    if args.replicaof:
        cfg["role"] = "slave"
        cfg["master_host"], cfg["master_port"] = args.replicaof
    else:
        cfg["master_host"], cfg["master_port"] = None, None

    return cfg


async def handleClient(config, reader, writer):
    while True:
        raw = await reader.read(1024)
        if not raw:
            break
        cmd = parse(raw)
        await execute(config, cmd, writer)
    writer.close()


async def execute(config, cmd, writer):
    if not cmd:
        return
    name = cmd[0].lower()
    args = cmd[1:]
    store = cleanStore(config["store"])

    if name == "ping":
        writer.write(encode("PONG"))
    elif name == "echo":
        writer.write(encode(" ".join(args)))
    elif name == "set":
        if len(args) >= 2:
            key, val = args[0], args[1]
            expiry = -1
            if len(args) == 4 and args[2].lower() == "px":
                expiry = time.time() + int(args[3]) / 1000.0
            store[key] = (val, expiry)
            writer.write(encode("OK"))
    elif name == "get":
        if len(args) == 1:
            key = args[0]
            val = store.get(key)
            if not val:
                writer.write(encode(None))
            else:
                value, exp = val
                if exp != -1 and exp < time.time():
                    del store[key]
                    writer.write(encode(None))
                else:
                    writer.write(encode(value))
    elif name == "keys" and args == ["*"]:
        keys = [k for k in store.keys() if not _is_expired(store[k])]
        writer.write(encode(keys))
    elif name == "config" and len(args) > 1 and args[0].lower() == "get":
        result = []
        for a in args[1:]:
            if a in config:
                result.extend([a, config[a]])
        writer.write(encode(result))
    elif name == "info":
        res = f"role:{config['role']}\n"
        if config["role"] == "master":
            res += f"master_replid:{config['master_replid']}\n"
            res += f"master_repl_offset:{config['master_repl_offset']}\n"
        writer.write(encode(res))
    elif name == "replconf":
        writer.write(encode("OK"))
    elif name == "psync":
        replid = config["master_replid"]
        offset = config["master_repl_offset"]
        fullsync = f"FULLRESYNC {replid} {offset}"
        writer.write(encode(fullsync))
        # minimal RDB snapshot
        file_bytes = bytes.fromhex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        )
        writer.write(file_bytes)
    else:
        writer.write(b"-ERR unknown command\r\n")

    config["store"] = store
    await writer.drain()


def _is_expired(val):
    _, exp = val
    return exp != -1 and exp < time.time()


def cleanStore(store):
    """Remove expired keys."""
    now = time.time()
    for k in list(store.keys()):
        _, exp = store[k]
        if exp != -1 and exp < now:
            del store[k]
    return store


# ====================== ENTRY ======================
if __name__ == "__main__":
    asyncio.run(serveRedis())