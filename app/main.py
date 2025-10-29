import asyncio
import os
import struct

# In-memory data store
data_store = {}

# RESP encoder helpers
def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()

def encode_error(err: str) -> bytes:
    return f"-{err}\r\n".encode()

def encode_integer(i: int) -> bytes:
    return f":{i}\r\n".encode()

def encode_bulk_string(s: str | None) -> bytes:
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()

def encode_array(items: list | None) -> bytes:
    if items is None:
        return b"*-1\r\n"
    resp = f"*{len(items)}\r\n".encode()
    for item in items:
        resp += encode_bulk_string(item)
    return resp

# Minimal RDB parser for Codecrafters stage
def load_rdb_file(dir_path: str, dbfilename: str):
    global data_store
    file_path = os.path.join(dir_path, dbfilename)
    if not os.path.exists(file_path):
        return

    with open(file_path, "rb") as f:
        content = f.read()

    # Very minimal parsing (enough for single key-value test)
    # "REDIS" magic header = first 5 bytes
    if not content.startswith(b"REDIS"):
        return

    idx = 9  # skip header + version
    while idx < len(content):
        if content[idx] == 0xFE:  # DB selector
            idx += 2
        elif content[idx] == 0xFA:  # Auxiliary field
            idx += 1
            _len = content[idx]
            idx += 1 + _len
            _len2 = content[idx]
            idx += 1 + _len2
        elif content[idx] == 0xFB:
            idx += 1
            _len = content[idx]
            idx += 1 + _len
            _len2 = content[idx]
            idx += 1 + _len2
        elif content[idx] == 0xFF:
            break
        elif content[idx] == 0x00:  # key-value pair
            idx += 1
            key_len = content[idx]
            idx += 1
            key = content[idx:idx+key_len].decode()
            idx += key_len
            val_len = content[idx]
            idx += 1
            val = content[idx:idx+val_len].decode()
            idx += val_len
            data_store[key] = val
        else:
            idx += 1

# RESP command parsing
async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        line = await reader.readline()
        if not line:
            break

        if not line.startswith(b"*"):
            continue

        try:
            arg_count = int(line[1:].strip())
            parts = []
            for _ in range(arg_count):
                await reader.readline()  # $len
                arg = (await reader.readline()).decode().strip()
                parts.append(arg)
        except Exception:
            writer.write(encode_error("ERR invalid protocol"))
            await writer.drain()
            continue

        cmd = parts[0].upper()

        # ---- COMMAND HANDLERS ----

        if cmd == "PING":
            resp = encode_simple_string("PONG")

        elif cmd == "ECHO":
            resp = encode_bulk_string(parts[1])

        elif cmd == "SET":
            if len(parts) < 3:
                resp = encode_error("ERR wrong number of arguments for 'set' command")
            else:
                key, value = parts[1], parts[2]
                data_store[key] = value
                resp = encode_simple_string("OK")

        elif cmd == "GET":
            key = parts[1]
            value = data_store.get(key)
            if isinstance(value, list):
                resp = encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
            else:
                resp = encode_bulk_string(value)

        elif cmd == "KEYS":
            resp = encode_array(list(data_store.keys()))

        elif cmd == "RPUSH":
            if len(parts) < 3:
                resp = encode_error("ERR wrong number of arguments for 'rpush' command")
            else:
                key = parts[1]
                values = parts[2:]
                if key not in data_store:
                    data_store[key] = []
                if not isinstance(data_store[key], list):
                    resp = encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
                else:
                    data_store[key].extend(values)
                    resp = encode_integer(len(data_store[key]))

        elif cmd == "BLPOP":
            if len(parts) < 3:
                resp = encode_error("ERR wrong number of arguments for 'blpop' command")
            else:
                key = parts[1]
                timeout = float(parts[2])
                if key in data_store and isinstance(data_store[key], list) and data_store[key]:
                    value = data_store[key].pop(0)
                    resp = encode_array([key, value])
                else:
                    await asyncio.sleep(timeout)
                    if key in data_store and isinstance(data_store[key], list) and data_store[key]:
                        value = data_store[key].pop(0)
                        resp = encode_array([key, value])
                    else:
                        resp = b"*-1\r\n"

        else:
            resp = encode_error(f"ERR unknown command '{cmd.lower()}'")

        writer.write(resp)
        await writer.drain()

# ---- Entry Point ----
async def main():
    dir_path = None
    dbfilename = None

    # Parse command line args
    import sys
    if "--dir" in sys.argv:
        dir_path = sys.argv[sys.argv.index("--dir") + 1]
    if "--dbfilename" in sys.argv:
        dbfilename = sys.argv[sys.argv.index("--dbfilename") + 1]

    if dir_path and dbfilename:
        load_rdb_file(dir_path, dbfilename)

    server = await asyncio.start_server(handle_client, "0.0.0.0", 6379)
    print("Server started on 0.0.0.0:6379")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())