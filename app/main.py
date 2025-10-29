import asyncio
import os
import struct
import sys

data_store = {}

# ---------------- RESP ENCODING ----------------
def encode_simple_string(s): return f"+{s}\r\n".encode()
def encode_error(e): return f"-{e}\r\n".encode()
def encode_integer(i): return f":{i}\r\n".encode()
def encode_bulk_string(s):
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()
def encode_array(items):
    if items is None:
        return b"*-1\r\n"
    out = f"*{len(items)}\r\n".encode()
    for it in items:
        out += encode_bulk_string(it)
    return out


# ---------------- RDB PARSING ----------------
def read_length(f):
    b1 = f.read(1)
    if not b1:
        return 0
    b = b1[0]
    type_ = (b & 0xC0) >> 6
    if type_ == 0:  # 00: 6-bit
        return b & 0x3F
    elif type_ == 1:  # 01: 14-bit
        b2 = f.read(1)[0]
        return ((b & 0x3F) << 8) | b2
    elif type_ == 2:  # 10: 32-bit
        return struct.unpack(">I", f.read(4))[0]
    else:
        # 11: special encoding (ignore)
        return b & 0x3F


def read_string(f):
    length = read_length(f)
    if length == 0:
        return ""
    return f.read(length).decode("utf-8", errors="ignore")


def load_rdb(dir_path, dbfilename):
    """Parse Codecrafters RDB format to load key-values"""
    global data_store
    path = os.path.join(dir_path, dbfilename)
    if not os.path.exists(path):
        return

    try:
        with open(path, "rb") as f:
            header = f.read(9)
            if not header.startswith(b"REDIS"):
                return

            while True:
                opcode = f.read(1)
                if not opcode:
                    break
                op = opcode[0]

                if op == 0xFE:  # SELECTDB
                    f.read(1)
                elif op == 0xFB:  # RESIZEDB
                    read_length(f)
                    read_length(f)
                elif op == 0xFD:  # expire time in seconds
                    f.read(4)
                    op = f.read(1)[0]
                elif op == 0xFC:  # expire time in ms
                    f.read(8)
                    op = f.read(1)[0]
                elif op == 0x00:  # string type key-value
                    key = read_string(f)
                    val = read_string(f)
                    if key:
                        data_store[key] = val
                elif op == 0xFF:  # EOF
                    break
    except Exception as e:
        print("RDB read error:", e)


# ---------------- COMMAND HANDLER ----------------
async def handle_client(reader, writer):
    while True:
        line = await reader.readline()
        if not line:
            break
        if not line.startswith(b"*"):
            continue

        try:
            num = int(line[1:].strip())
            parts = []
            for _ in range(num):
                await reader.readline()  # $len line
                arg = (await reader.readline()).decode().strip()
                parts.append(arg)
        except Exception:
            writer.write(encode_error("ERR invalid protocol"))
            await writer.drain()
            continue

        cmd = parts[0].upper()
        resp = b""

        if cmd == "PING":
            resp = encode_simple_string("PONG")

        elif cmd == "ECHO":
            resp = encode_bulk_string(parts[1])

        elif cmd == "SET":
            data_store[parts[1]] = parts[2]
            resp = encode_simple_string("OK")

        elif cmd == "GET":
            val = data_store.get(parts[1])
            if isinstance(val, list):
                resp = encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
            else:
                resp = encode_bulk_string(val)

        elif cmd == "KEYS":
            resp = encode_array(list(data_store.keys()))

        elif cmd == "RPUSH":
            key = parts[1]
            vals = parts[2:]
            if key not in data_store:
                data_store[key] = []
            if not isinstance(data_store[key], list):
                resp = encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
            else:
                data_store[key].extend(vals)
                resp = encode_integer(len(data_store[key]))

        elif cmd == "BLPOP":
            key = parts[1]
            timeout = float(parts[2])
            if key in data_store and isinstance(data_store[key], list) and data_store[key]:
                val = data_store[key].pop(0)
                resp = encode_array([key, val])
            else:
                await asyncio.sleep(timeout)
                if key in data_store and isinstance(data_store[key], list) and data_store[key]:
                    val = data_store[key].pop(0)
                    resp = encode_array([key, val])
                else:
                    resp = b"*-1\r\n"

        else:
            resp = encode_error(f"ERR unknown command '{cmd.lower()}'")

        writer.write(resp)
        await writer.drain()


# ---------------- MAIN ENTRY ----------------
async def main():
    dir_path = None
    dbfilename = None

    if "--dir" in sys.argv:
        dir_path = sys.argv[sys.argv.index("--dir") + 1]
    if "--dbfilename" in sys.argv:
        dbfilename = sys.argv[sys.argv.index("--dbfilename") + 1]

    if dir_path and dbfilename:
        load_rdb(dir_path, dbfilename)

    server = await asyncio.start_server(handle_client, "0.0.0.0", 6379)
    print("Server started on 0.0.0.0:6379")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())