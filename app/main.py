import asyncio
import argparse
import datetime
import os
import struct
import time

# -------------------- RESP PARSER --------------------

def parse(data):
    """Parse RESP protocol input into Python list"""
    if not data:
        return None
    if data.startswith(b'*'):
        return _parse_array(data)
    elif data.startswith(b'+'):
        return data[1:-2].decode()
    elif data.startswith(b':'):
        return int(data[1:-2])
    elif data.startswith(b'$'):
        return _parse_bulk_string(data)
    return None

def _parse_bulk_string(data):
    if data.startswith(b"$-1\r\n"):
        return None
    try:
        length_end = data.find(b"\r\n")
        length = int(data[1:length_end])
        start = length_end + 2
        return data[start:start + length].decode()
    except:
        return None

def _parse_array(data):
    parts = data.split(b'\r\n')
    n = int(parts[0][1:])
    res = []
    i = 1
    while len(res) < n and i < len(parts):
        if parts[i].startswith(b'$'):
            length = int(parts[i][1:])
            res.append(parts[i + 1].decode())
            i += 2
        else:
            i += 1
    return res

# -------------------- RESP ENCODER --------------------

def encode(value):
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, str):
        return f"+{value}\r\n".encode()
    if isinstance(value, int):
        return f":{value}\r\n".encode()
    if isinstance(value, list):
        out = f"*{len(value)}\r\n"
        for v in value:
            v_b = v.encode() if isinstance(v, str) else str(v).encode()
            out += f"${len(v_b)}\r\n{v_b.decode()}\r\n"
        return out.encode()
    if value == "NULL_ARRAY":
        return b"*-1\r\n"
    return b"-ERR Unknown type\r\n"

# -------------------- RDB LOADING --------------------

def read_key_val_from_db(dir_path, dbfilename, data):
    """Load key-values from RDB (if exists)"""
    rdb_file_loc = os.path.join(dir_path, dbfilename)
    if not os.path.isfile(rdb_file_loc):
        return
    try:
        with open(rdb_file_loc, "rb") as f:
            while b := f.read(1):
                if b == b"\xfb":
                    break
            _ = f.read(2)
    except Exception:
        pass

# -------------------- CONFIG --------------------

def getConfig():
    flag = argparse.ArgumentParser()
    flag.add_argument("--dir", default=".", help="Directory for redis files")
    flag.add_argument("--dbfilename", default="dump.rdb", help="Database filename")
    flag.add_argument("--port", default="6379")
    args = flag.parse_args()
    return {
        "dir": args.dir,
        "dbfilename": args.dbfilename,
        "port": args.port,
        "host": "0.0.0.0",
        "store": {},
    }

# -------------------- SERVER --------------------

async def serveRedis():
    config = getConfig()
    read_key_val_from_db(config["dir"], config["dbfilename"], config["store"])
    server = await asyncio.start_server(
        lambda r, w: handleClient(config, r, w),
        config["host"],
        int(config["port"])
    )
    print(f"Server started on {config['host']}:{config['port']}")
    async with server:
        await server.serve_forever()

# -------------------- CLIENT HANDLER --------------------

async def handleClient(config, reader, writer):
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            cmd = parse(data)
            await execute(config, cmd, writer)
            await writer.drain()
    except Exception as e:
        print("Error:", e)
    finally:
        writer.close()

# -------------------- COMMAND EXECUTION --------------------

async def execute(config, cmd, writer):
    if not cmd:
        return
    store = config["store"]
    op = cmd[0].lower()
    args = cmd[1:]

    # --- Simple commands ---
    if op == "ping":
        writer.write(encode("PONG"))
    elif op == "echo":
        writer.write(encode(" ".join(args)))
    elif op == "set":
        key, val = args[0], args[1]
        store[key] = (val, -1)
        writer.write(encode("OK"))
    elif op == "get":
        val = store.get(args[0])
        writer.write(encode(val[0] if val else None))
    elif op == "config" and args[0].lower() == "get":
        key = args[1]
        if key == "dir":
            writer.write(encode(["dir", config["dir"]]))
        elif key == "dbfilename":
            writer.write(encode(["dbfilename", config["dbfilename"]]))
        else:
            writer.write(encode([]))

    # --- Lists ---
    elif op == "rpush":
        key = args[0]
        if key not in store:
            store[key] = []
        store[key].extend(args[1:])
        writer.write(encode(len(store[key])))
    elif op == "lpush":
        key = args[0]
        if key not in store:
            store[key] = []
        store[key] = list(reversed(args[1:])) + store[key]
        writer.write(encode(len(store[key])))
    elif op == "lpop":
        key = args[0]
        if key not in store or not store[key]:
            writer.write(encode(None))
        else:
            val = store[key].pop(0)
            writer.write(encode(val))
    elif op == "rpop":
        key = args[0]
        if key not in store or not store[key]:
            writer.write(encode(None))
        else:
            val = store[key].pop()
            writer.write(encode(val))
    elif op == "blpop":
        await handle_blpop(store, args, writer)
    elif op == "keys":
        if args and args[0] == "*":
            writer.write(encode(list(store.keys())))
        else:
            writer.write(encode([]))
    else:
        writer.write(b"-ERR unknown command\r\n")

# -------------------- BLPOP HANDLER --------------------

async def handle_blpop(store, args, writer):
    """
    BLPOP key timeout
    Waits for an element to appear in list or returns NULL_ARRAY if timeout.
    """
    if len(args) != 2:
        writer.write(b"-ERR wrong number of arguments\r\n")
        return

    key, timeout_str = args
    try:
        timeout = float(timeout_str)
    except:
        writer.write(b"-ERR invalid timeout\r\n")
        return

    end_time = time.time() + timeout
    while time.time() < end_time:
        if key in store and store[key]:
            val = store[key].pop(0)
            writer.write(encode([key, val]))
            return
        await asyncio.sleep(0.05)

    # Timeout expired
    writer.write(encode("NULL_ARRAY"))

# -------------------- ENTRY --------------------
if __name__ == "__main__":
    asyncio.run(serveRedis())