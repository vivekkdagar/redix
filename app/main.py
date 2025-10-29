import argparse
import asyncio
import time
import struct
import os
import datetime

# =========================================================
# =============== RESP PARSING AND ENCODING ===============
# =========================================================

def parse(data):
    """Parse RESP data into Python objects (used for commands)."""
    if not data:
        return []
    if data.startswith(b"*"):
        return _parse_array(data)
    raise ValueError("Unsupported RESP type")

def _parse_array(data):
    parts = data.split(b"\r\n")
    length = int(parts[0][1:])
    result = []
    i = 1
    for _ in range(length):
        if i >= len(parts):
            break
        if parts[i].startswith(b"$"):
            i += 1
            if i < len(parts):
                result.append(parts[i].decode())
            i += 1
    return result


# ----------- RESP ENCODERS -----------
def encode(data):
    """Encode Python data into RESP format."""
    if isinstance(data, str):
        return _encode_simple_string(data)
    elif isinstance(data, int):
        return _encode_integer(data)
    elif isinstance(data, list):
        return _encode_array(data)
    elif data is None:
        return b"$-1\r\n"
    else:
        return _encode_bulk_string(str(data))

def _encode_simple_string(s):
    return f"+{s}\r\n".encode("utf-8")

def _encode_integer(i):
    return f":{i}\r\n".encode("utf-8")

def _encode_bulk_string(s):
    return f"${len(s)}\r\n{s}\r\n".encode("utf-8")

def _encode_array(elements):
    result = f"*{len(elements)}\r\n"
    for el in elements:
        el = str(el)
        result += f"${len(el)}\r\n{el}\r\n"
    return result.encode("utf-8")


# =========================================================
# =================== RDB PARSING LOGIC ===================
# =========================================================

def read_key_val_from_db(dir_path, dbfilename, data):
    """Reads simple string keys/values from an RDB file."""
    file_path = os.path.join(dir_path, dbfilename)
    if not os.path.isfile(file_path):
        return

    with open(file_path, "rb") as f:
        # Skip until RESIZEDB opcode (0xFB)
        while b := f.read(1):
            if b == b"\xfb":
                break
        try:
            num_dbs = struct.unpack("B", f.read(1))[0]
        except Exception:
            return
        f.read(1)

        for _ in range(num_dbs):
            expired = False
            opcode = f.read(1)
            if opcode == b"\xfc":  # expire time in ms
                milli = int.from_bytes(f.read(8), "little")
                if milli < datetime.datetime.now().timestamp() * 1000:
                    expired = True
                f.read(1)
            elif opcode == b"\xfd":  # expire time in sec
                sec = int.from_bytes(f.read(4), "little")
                if sec < datetime.datetime.now().timestamp():
                    expired = True
                f.read(1)

            key_len = struct.unpack("B", f.read(1))[0]
            if key_len >> 6 == 0b00:
                key_len &= 0b00111111
            key = f.read(key_len).decode()

            val_len = struct.unpack("B", f.read(1))[0]
            if val_len >> 6 == 0b00:
                val_len &= 0b00111111
            val = f.read(val_len).decode()

            if not expired:
                data[key] = (val, -1)


# =========================================================
# =================== REDIS SERVER CORE ===================
# =========================================================

async def serveRedis():
    config = getConfig()
    server = await asyncio.start_server(
        lambda r, w: handleClient(config, r, w),
        config["host"],
        int(config["port"]),
    )
    print(f"Starting server on {config['host']}:{config['port']}")
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
        "port": args.port,
        "host": "0.0.0.0",
        "role": "master",
        "dbfilename": args.dbfilename,
        "dir": args.dir,
        "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        "master_repl_offset": "0",
        "replicaof": args.replicaof,
        "master_host": None,
        "master_port": None,
    }

    # load RDB data if exists
    read_key_val_from_db(config["dir"], config["dbfilename"], config["store"])
    if args.replicaof:
        config["role"] = "slave"
        config["master_host"], config["master_port"] = args.replicaof.split()

    return config


async def handleClient(config, reader, writer):
    while True:
        raw = await reader.read(1024)
        if not raw:
            break
        command = parse(raw)
        await execute(config, command, writer)
    await writer.drain()


async def execute(config, command, writer):
    if not command:
        return
    cmd = command[0].lower()
    args = command[1:]
    config["store"] = cleanStore(config["store"].copy())

    if cmd == "ping":
        writer.write(encode("PONG"))
    elif cmd == "echo":
        writer.write(encode(" ".join(args)))
    elif cmd == "set" and len(args) >= 2:
        val = args[1]
        expire = -1
        if len(args) > 3 and args[2].lower() == "px":
            expire = time.time() + int(args[3]) / 1000
        config["store"][args[0]] = (val, expire)
        writer.write(encode("OK"))
    elif cmd == "get" and len(args) == 1:
        key = args[0]
        val = config["store"].get(key)
        if not val:
            writer.write(b"$-1\r\n")
        else:
            writer.write(encode(val[0]))
    elif cmd == "keys" and len(args) == 1 and args[0] == "*":
        writer.write(encode(list(config["store"].keys())))
    elif cmd == "config" and len(args) >= 2 and args[0].lower() == "get":
        res = []
        for a in args[1:]:
            if a in config:
                res.extend([a, config[a]])
        writer.write(encode(res))
    elif cmd == "rpush":
        key = args[0]
        values = args[1:]
        if key not in config["store"]:
            config["store"][key] = ([], -1)
        if not isinstance(config["store"][key][0], list):
            config["store"][key] = ([], -1)
        config["store"][key][0].extend(values)
        writer.write(encode(len(config["store"][key][0])))
    elif cmd == "blpop":
        key = args[0]
        timeout = float(args[1]) if len(args) > 1 else 0
        start = time.time()
        while time.time() - start < timeout:
            if key in config["store"] and isinstance(config["store"][key][0], list) and config["store"][key][0]:
                val = config["store"][key][0].pop(0)
                writer.write(encode([key, val]))
                return
            await asyncio.sleep(0.05)
        writer.write(b"*-1\r\n")
    else:
        writer.write(b"-ERR unknown command\r\n")


def cleanStore(store):
    now = time.time()
    for key, (val, exp) in list(store.items()):
        if exp != -1 and exp < now:
            del store[key]
    return store


# =========================================================
# ======================= MAIN ============================
# =========================================================

if __name__ == "__main__":
    asyncio.run(serveRedis())