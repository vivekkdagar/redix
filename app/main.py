import asyncio
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional


# ==========================
# ENUM: Command Types
# ==========================
class Command(Enum):
    ECHO = 1
    SET = 2
    GET = 3
    PING = 4
    RPUSH = 5


# ==========================
# PARSER
# ==========================
class Parser:
    def parse_command(self, payload: bytes) -> tuple[Command, ...]:
        message = payload.decode()
        upper = message.upper()

        if "ECHO" in upper:
            return Command.ECHO, *self._parse(message)
        elif "SET" in upper:
            return Command.SET, *self._parse(message)
        elif "GET" in upper:
            return Command.GET, *self._parse(message)
        elif "PING" in upper:
            return Command.PING, *("PING",)
        elif "RPUSH" in upper:
            return Command.RPUSH, *self._parse(message)
        else:
            raise RuntimeError("Unknown command")

    @staticmethod
    def _parse(message: str) -> list[str]:
        parts = message.split("\r\n")
        result = []
        i = 0
        while i < len(parts):
            if parts[i].startswith("$"):
                i += 1
                if i < len(parts) and parts[i] != "":
                    result.append(parts[i])
            i += 1
        return result


parser = Parser()


# ==========================
# FORMATTER
# ==========================
class Formatter:
    @staticmethod
    def format_echo_expression(args: tuple[str, ...]) -> bytes:
        msg = args[1].encode()
        return b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"

    @staticmethod
    def format_ok_expression() -> bytes:
        return b"+OK\r\n"

    @staticmethod
    def format_get_response(value: Any) -> bytes:
        if value is None:
            return b"$-1\r\n"
        if isinstance(value, Value):
            val = str(value.item).encode()
        else:
            val = str(value).encode()
        return b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"

    @staticmethod
    def format_rpush_response(values: list) -> bytes:
        # RESP integer
        return f":{len(values)}\r\n".encode()


formatter = Formatter()


# ==========================
# STORAGE
# ==========================
@dataclass
class Value:
    item: Any
    expire: Optional[datetime] = None


class Storage:
    def __init__(self):
        self.data: dict[Any, Any] = {}

    def set(self, key: str, value: Value) -> None:
        self.data[key] = value

    def rpush(self, key: str, value: Value) -> list[Value]:
        if key in self.data and isinstance(self.data[key], list):
            self.data[key].append(value)
        elif key not in self.data:
            self.data[key] = [value]
        else:
            raise RuntimeError(f"Key {key} exists and is not a list")
        return self.data[key]

    def get(self, key: str) -> Any:
        if key not in self.data:
            return None
        val = self.data[key]
        if isinstance(val, Value) and val.expire and val.expire <= datetime.now():
            return None
        return val


storage = Storage()


# ==========================
# PROCESSOR
# ==========================
async def process_command(command: tuple[Command, str, ...], writer: Any) -> None:
    """Process a single Redis-like command and send the result."""

    if command[0] == Command.ECHO:
        writer.write(formatter.format_echo_expression(command[1:]))

    elif command[0] == Command.SET:
        req = command[1:]
        key, value = req[1], req[2]
        if len(req) > 3:
            expiration = (
                datetime.now() + timedelta(seconds=int(req[4]))
                if req[3].upper() == "EX"
                else datetime.now() + timedelta(milliseconds=int(req[4]))
            )
        else:
            expiration = None
        storage.set(key, Value(value, expiration))
        writer.write(formatter.format_ok_expression())

    elif command[0] == Command.GET:
        key = command[2]
        val = storage.get(key)
        writer.write(formatter.format_get_response(val))

    elif command[0] == Command.PING:
        writer.write(b"+PONG\r\n")

    elif command[0] == Command.RPUSH:
        key = command[2]
        val = command[3]
        result = storage.rpush(key, Value(val))
        writer.write(formatter.format_rpush_response(result))

    await writer.drain()


# ==========================
# ASYNC SERVER
# ==========================
async def handle_client(reader, writer):
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            cmd = parser.parse_command(data)
            await process_command(cmd, writer)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    print("Logs from your program will appear here!")
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())