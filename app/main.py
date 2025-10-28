import socket
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Any, Optional


# -------------------------------
# Data Classes and Storage Layer
# -------------------------------

@dataclass
class Value:
    item: Any
    expire: Optional[datetime] = None


class Storage:
    def __init__(self):
        self.data: dict[str, Value] = {}

    def set(self, key: str, value: Value) -> None:
        self.data[key] = value

    def get(self, key: str) -> Optional[str]:
        if key not in self.data:
            return None

        val = self.data[key]

        # Check expiry
        if val.expire is not None and datetime.now() > val.expire:
            del self.data[key]
            return None

        return val.item


# -------------------------------
# RESP Protocol Helpers
# -------------------------------

def parse_resp(data: bytes) -> list[str]:
    """Parse a RESP array like *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"""
    parts = data.split(b"\r\n")
    items = []
    i = 0
    while i < len(parts):
        if not parts[i]:
            i += 1
            continue
        if parts[i].startswith(b"$"):
            # Next line is the actual data
            items.append(parts[i + 1].decode())
            i += 2
        else:
            i += 1
    return [x.upper() if idx == 0 else x for idx, x in enumerate(items)]


def encode_bulk_string(value: Optional[str]) -> bytes:
    if value is None:
        return b"$-1\r\n"
    return f"${len(value)}\r\n{value}\r\n".encode()


def encode_simple_string(msg: str) -> bytes:
    return f"+{msg}\r\n".encode()


def encode_integer(num: int) -> bytes:
    return f":{num}\r\n".encode()


# -------------------------------
# Command Handling
# -------------------------------

storage = Storage()


def handle_command(command: str, args: list[str]) -> bytes:
    if command == "PING":
        return encode_simple_string("PONG")

    elif command == "ECHO":
        return encode_bulk_string(args[0] if args else "")

    elif command == "SET":
        key = args[0]
        value = args[1]
        expire_time = None

        # Optional EX / PX
        if len(args) >= 4:
            option = args[2].upper()
            exp_value = int(args[3])
            if option == "EX":
                expire_time = datetime.now() + timedelta(seconds=exp_value)
            elif option == "PX":
                expire_time = datetime.now() + timedelta(milliseconds=exp_value)

        storage.set(key, Value(value, expire_time))
        return encode_simple_string("OK")

    elif command == "GET":
        key = args[0]
        value = storage.get(key)
        return encode_bulk_string(value)

    else:
        return encode_simple_string("ERR unknown command")


# -------------------------------
# Networking
# -------------------------------

def handle_client(conn: socket.socket):
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break

            parts = parse_resp(data)
            if not parts:
                continue

            command = parts[0]
            args = parts[1:]
            response = handle_command(command, args)
            conn.sendall(response)

    except ConnectionResetError:
        pass
    finally:
        conn.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 6379))
    server.listen(5)
    print("Redis clone running on port 6379...")

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()