import argparse
import asyncio
import time
import utils.parser as resp


async def serveRedis():
    config = getConfig()
    server = await asyncio.start_server(
        lambda r, w: handleClient(config, r, w), config["host"], config["port"]
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
    resp.read_key_val_from_db(config["dir"], config["dbfilename"], config["store"])
    if args.replicaof:
        config["role"] = "slave"
        config["master_host"], config["master_port"] = args.replicaof
    return config


async def handleClient(config, reader, writer):
    while True:
        raw = await reader.read(1024)
        if not raw:
            break
        command = resp.parse(raw)
        await execute(config, command, writer)
    await writer.drain()


async def execute(config, command, writer):
    if not command:
        return
    cmd, args = command[0].lower(), command[1:]
    print("CMD:", repr(cmd), "ARGS:", repr(args))
    config["store"] = cleanStore(config["store"].copy())
    if cmd == "ping":
        writer.write(resp.encode("PONG"))
    elif cmd == "echo":
        writer.write(resp.encode(" ".join(args)))
    elif cmd == "set" and len(args) > 1:
        val = None
        print("Setting value: ", args)
        if len(args) == 2:
            val = args[1], -1
        elif args[3].isdigit():
            if args[2].lower() == "px":
                val = args[1], time.time() + int(args[3]) / 1000
        if val:
            config.get("store")[args[0]] = val
            writer.write(resp.encode("OK"))
    elif cmd == "get" and len(args) == 1:
        raw = config.get("store").get(args[0])
        raw = raw[0] if raw else None
        writer.write(resp.encode(raw))
    elif cmd == "del" and len(args) > 0:
        for arg in args:
            if arg in config["store"]:
                del config["store"][arg]
        writer.write(f":{len(args)}\r\n".encode())
    elif cmd == "config" and len(args) > 1 and args[0].lower() == "get":
        raw = []
        for attr in args[1:]:
            if attr in config:
                raw.extend([attr, config[attr]])
        writer.write(resp.encode(raw))
    elif cmd == "keys" and len(args) == 1 and args[0] == "*":
        writer.write(resp.encode(list(config["store"].keys())))
    elif cmd == "info" and len(args) == 1:
        result = f"role:{main.config.role}\n"
        if main.config.role == "master":
            result += f"master_replid:{main.config.master_replid}\n"
            result += f"master_repl_offset:{main.config.master_repl_offset}\n"
        writer.write(resp.encode(result))
    elif cmd == "replconf":
        writer.write(resp.encode("OK"))
    elif cmd == "psync":
        fullsync = (
            f"FULLRESYNC {main.config.master_replid} {main.config.master_repl_offset}"
        )
        file_contents = bytes.fromhex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        )
        writer.write(resp.encode(fullsync))
        writer.write(resp.encode(file_contents))
    else:
        writer.write("-ERR unknown command\r\n".encode())


def cleanStore(store):
    for key, value in list(store.items()):
        if value[1] != -1 and value[1] < time.time():
            del store[key]
    return store


if __name__ == "__main__":
    asyncio.run(serveRedis())