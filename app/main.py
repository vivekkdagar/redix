import asyncio
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass
from typing import Any, Optional

# ---------------------------
# ENUM: Supported Commands
# ---------------------------
class Command(Enum):
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    PING = "PING"
    RPUSH = "RPUSH"


# ---------------------------
# Data storage structure
# ---------------------------
@dataclass
class Value:
    item: Any
    expire: Optional[datetime] = None


class Storage:
    def __init__(self):
        self.data: dict[str, Any] = {}

    def set(self, key: str, value: Value) -> None:
        self.data[key] = value

    def get(self, key: str) -> Optional[Any]:
        value = self.data.get(key)
        if isinstance(value, Value):
            if value.expire and value.expire <= datetime.now():
                del self.data[key]
                return None
            return value.item
        return value

    def rpush(self, key: str, *values: str) -> int:
        # If key doesn't exist, create list
        if key not in self.data:
            self.data[key] = []

        # If key exists and is not a list, error
        if not isinstance(self.data[key], list):
            raise RuntimeError(f"Key {key} exists and is not a list")

        # Append all values
        self.data[key].extend(values)
        return len(self.data[key])


storage = Storage()


# ---------------------------
# RESP Formatter helpers
# ---------------------------
class Formatter:
    @staticmethod
    def format_simple_string(s: str) -> bytes:
        return f"+{s}\r\n".encode()

    @staticmethod
    def format_bulk_string(s: Optional[str]) -> bytes:
        if s is None:
            return b"$-1\r\n"
        return f"${len(s)}\r\n{s}\r\n".encode()

    @staticmethod
    def format_integer(n: int) -> bytes:
        return f":{n}\r\n".encode()


formatter = Formatter()


# ---------------------------
# RESP Parser
# ---------------------------
class Parser:
    def parse_command(self, payload: bytes) -> tuple[Command, list[str]]:
        message = payload.decode()
        elements = message.split("\r\n")
        if not elements or not elements[0].startswith("*"):
            raise Exception("Invalid RESP array format")

        # Parse RESP array
        result = []
        i = 2  # skip "*<n>" and "$<len>"
        while i < len(elements):
            if elements[i] == "":
                break
            result.append(elements[i])
            i += 2  # skip $len entries

        cmd_str = result[0].upper()
        try:
            cmd = Command(cmd_str)
        except ValueError:
            raise RuntimeError(f"Unknown command: {cmd_str}")

        return cmd, result


parser = Parser()


# ---------------------------
# Command Processor
# ---------------------------
async def process_command(cmd: tuple[Command, list[str]], writer: Any) -> None:
    command, args = cmd

    if command == Command.PING:
        writer.write(formatter.format_simple_string("PONG"))

    elif command == Command.ECHO:
        # ECHO <message>
        writer.write(formatter.format_bulk_string(args[1]))

    elif command == Command.SET:
        # SET <key> <value> [EX seconds | PX milliseconds]
        key = args[1]
        value = args[2]
        expiration = None

        if len(args) > 3:
            if args[3].upper() == "EX":
                expiration = datetime.now() + timedelta(seconds=int(args[4]))
            elif args[3].upper() == "PX":
                expiration = datetime.now() + timedelta(milliseconds=int(args[4]))

        storage.set(key, Value(value, expiration))
        writer.write(formatter.format_simple_string("OK"))

    elif command == Command.GET:
        # GET <key>
        key = args[1]
        val = storage.get(key)
        writer.write(formatter.format_bulk_string(val))

    elif command == Command.RPUSH:
        # RPUSH <key> <value1> [value2 ...]
        key = args[1]
        values = args[2:]
        length = storage.rpush(key, *values)
        writer.write(formatter.format_integer(length))

    await writer.drain()


# ---------------------------
# TCP Server
# ---------------------------
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
    print("Redis clone running on port 6379...")
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())