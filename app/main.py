import asyncio
import argparse
import datetime
import struct
import os
import time

# -------------------- RESP PARSER --------------------

def parse(data):
    """Parse RESP data into Python object"""
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
    else:
        raise ValueError("Unsupported RESP type")

def _parse_bulk_string(data):
    if data.startswith(b"$-1\r\n"):
        return None
    _, rest = data.split(b"\r\n", 1)
    length = int(data[1:data.find(b'\r\n')])
    return rest[:length].decode()

def _parse_array(data):
    """Parse RESP array (used for commands)"""
    parts = data.split(b'\r\n')
    count = int(parts[0][1:])
    result = []
    i = 1
    while len(result) < count and i < len(parts):
        if parts[i].startswith(b'$'):
            length = int(parts[i][1:])
            result.append(parts[i + 1].decode())
            i += 2
        else:
            i += 1
    return result

# -------------------- RESP ENCODER --------------------

def encode(data):
    """Encode Python data into RESP format"""
    if isinstance(data, str):
        return f"+{data}\r\n".encode()
    elif isinstance(data, int):
        return f":{data}\r\n".encode()
    elif data is None:
        return b"$-1\r\n"
    elif isinstance(data, list):
        res = f"*{len(data)}\r\n"
        for item in data:
            item_b = item.encode() if isinstance(item, str) else str(item).encode()
            res += f"${len(item_b)}\r\n".encode().decode()
            res += item_b.decode() + "\r\n"
        return res.encode()
    else:
        return f"-ERR unknown type\r\n".encode()

# -------------------- RDB FILE PARSER --------------------

def read_key_val_from_db(dir_path, dbfilename, data):
    """Load existing key-values from dump.rdb if exists"""
    rdb_file_loc = os.path.join(dir_path, dbfilename)
    if not os.path.isfile(rdb_file_loc):
        return

    with open(rdb_file_loc, "rb") as f:
        while operand := f.read(1):
            if operand == b"\xfb":  # database selector
                break
        numKeys = struct.unpack("B", f.read(1))[0]
        f.read(1)
        for _ in range(numKeys):
            expired = False
            top = f.read(1)
            if top == b"\xfc":
                milliTime = int.from_bytes(f.read(8), byteorder="little")
                if milliTime < datetime.datetime.now().timestamp() * 1000:
                    expired = True
                f.read(1)
            elif top == b"\xfd":
                secTime = int.from_bytes(f.read(4), byteorder="little")
                if secTime < datetime.datetime.now().timestamp():
                    expired = True
                f.read(1)

            key_len = struct.unpack("B", f.read(1))[0] & 0b00111111
            key = f.read(key_len).decode()

            val_len = struct.unpack("B", f.read(1))[0] & 0b00111111
            val = f.read(val_len).decode()

            if not expired:
                data[key] = (val, -1)

# -------------------- REDIS SERVER CORE --------------------

async def serveRedis():
    config = getConfig()
    server = await asyncio.start_server(
        lambda r, w: handleClient(config, r, w),
        config["host"],
        int(config["port"])
    )
    print(f"Server started on {config['host']}:{config['port']}")
    async with server:
        await server.serve_forever()

def getConfig():
    flag = argparse.ArgumentParser()
    flag.add_argument("--dir", default=".", help="Directory for redis files")
    flag.add_argument("--dbfilename", default="dump.rdb", help="Database filename")
    flag.add_argument("--port", default="6379", help="Port to listen on")
    flag.add_argument("--replicaof", nargs=2, help="Replication target: HOST PORT")
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
    }

    read_key_val_from_db(config["dir"], config["dbfilename"], config["store"])

    if args.replicaof:
        config["role"] = "slave"
        config["master_host"], config["master_port"] = args.replicaof

    return config

async def handleClient(config, reader, writer):
    try:
        while True:
            raw = await reader.read(1024)
            if not raw:
                break
            try:
                command = parse(raw)
                await execute(config, command, writer)
            except Exception as e:
                writer.write(f"-ERR {str(e)}\r\n".encode())
            await writer.drain()
    except Exception as e:
        print("Client error:", e)
    finally:
        writer.close()

# -------------------- COMMAND EXECUTION --------------------

async def execute(config, command, writer):
    if not command:
        return
    cmd = command[0].lower()
    args = command[1:]
    store = config["store"]
    store = cleanStore(store)

    if cmd == "ping":
        writer.write(encode("PONG"))
    elif cmd == "echo":
        writer.write(encode(" ".join(args)))
    elif cmd == "set" and len(args) >= 2:
        value, expiry = args[1], -1
        if len(args) == 4 and args[2].lower() == "px":
            expiry = time.time() + int(args[3]) / 1000
        store[args[0]] = (value, expiry)
        writer.write(encode("OK"))
    elif cmd == "get" and len(args) == 1:
        val = store.get(args[0])
        if not val:
            writer.write(encode(None))
        else:
            value, exp = val
            if exp != -1 and exp < time.time():
                del store[args[0]]
                writer.write(encode(None))
            else:
                writer.write(encode(value))
    elif cmd == "del":
        deleted = 0
        for key in args:
            if key in store:
                del store[key]
                deleted += 1
        writer.write(encode(deleted))
    elif cmd == "config" and len(args) > 1 and args[0].lower() == "get":
        res = []
        for key in args[1:]:
            if key in config:
                res.extend([key, config[key]])
        writer.write(encode(res))
    elif cmd == "keys" and len(args) == 1 and args[0] == "*":
        writer.write(encode(list(store.keys())))
    elif cmd == "info":
        info = f"role:{config['role']}\n"
        if config["role"] == "master":
            info += f"master_replid:{config['master_replid']}\n"
            info += f"master_repl_offset:{config['master_repl_offset']}\n"
        writer.write(encode(info))
    elif cmd == "replconf":
        writer.write(encode("OK"))
    elif cmd == "psync":
        repl = f"FULLRESYNC {config['master_replid']} {config['master_repl_offset']}"
        writer.write(encode(repl))
        file_bytes = bytes.fromhex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        )
        writer.write(file_bytes)
    else:
        writer.write(f"-ERR unknown command '{cmd}'\r\n".encode())

def cleanStore(store):
    for k, v in list(store.items()):
        if v[1] != -1 and v[1] < time.time():
            del store[k]
    return store

# -------------------- ENTRY POINT --------------------
if __name__ == "__main__":
    asyncio.run(serveRedis())