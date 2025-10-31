# app/rdb_parser.py
import io
import time
import datetime
import traceback
import app.storage as storage
from app.storage import DataEntry
import dataclasses
from typing import Any, Dict, Optional, List

# --- GLOBALS ---
global_file_dir = ""
global_file_name = ""

# --- Opcodes ---
OPCODE_EXPIRETIME_MS = 0xfc
OPCODE_EXPIRETIME_SEC = 0xfd
OPCODE_SELECTDB = 0xfe
OPCODE_EOF = 0xff
OPCODE_RESIZEDB = 0xfb
OPCODE_AUX = 0xfa

# --- Value Types ---
VALUE_TYPE_STRING = 0

@dataclasses.dataclass
class XADDValue:
    value: Any
    milliseconds: Optional[int]
    sequence: Optional[int]


@dataclasses.dataclass
class Value:
    value: Any | List[XADDValue]
    expiry: Optional[datetime.datetime]


class RdbParser:
    def __init__(self, content: bytes):
        self.file = io.BytesIO(content)
        self.expiry_ms = None

    def read_bytes(self, n):
        data = self.file.read(n)
        if len(data) < n:
            raise EOFError("Unexpected EOF")
        return data

    def read_byte(self):
        return self.read_bytes(1)[0]

    def read_length_encoded(self):
        first = self.read_byte()
        enc_type = (first & 0b11000000) >> 6
        if enc_type == 0:
            return first & 0b00111111, False
        elif enc_type == 1:
            next_b = self.read_byte()
            return ((first & 0b00111111) << 8) | next_b, False
        elif enc_type == 2:
            return int.from_bytes(self.read_bytes(4), 'big'), False
        elif enc_type == 3:
            subtype = first & 0b00111111
            return subtype, True
        else:
            raise ValueError("Invalid length encoding")

    def read_string(self):
        length, is_special = self.read_length_encoded()
        if is_special:
            if length == 0:
                val = int.from_bytes(self.read_bytes(1), 'little', signed=True)
                return str(val).encode()
            elif length == 1:
                val = int.from_bytes(self.read_bytes(2), 'little', signed=True)
                return str(val).encode()
            elif length == 2:
                val = int.from_bytes(self.read_bytes(4), 'little', signed=True)
                return str(val).encode()
            else:
                raise ValueError(f"Unsupported special encoding {length}")
        else:
            return self.read_bytes(length)

    def parse(self) -> Dict[str, Value]:
        # Verify magic
        magic = self.read_bytes(5)
        if magic != b"REDIS":
            raise ValueError("Invalid RDB header")

        version = self.read_bytes(4).decode()
        print(f"RDB Version: {version}")
        db: Dict[str, Value] = {}

        while True:
            opcode = self.read_byte()

            if opcode == OPCODE_EOF:
                print("Reached EOF marker.")
                break
            elif opcode == OPCODE_AUX:
                key = self.read_string().decode()
                value = self.read_string().decode()
                print(f"AUX field: {key}={value}")
            elif opcode == OPCODE_SELECTDB:
                db_num, _ = self.read_length_encoded()
                print(f"Selecting DB {db_num}")
            elif opcode == OPCODE_RESIZEDB:
                db_size, _ = self.read_length_encoded()
                expire_size, _ = self.read_length_encoded()
                print(f"DB size hint: {db_size}, expires: {expire_size}")
            elif opcode == OPCODE_EXPIRETIME_MS:
                ts = int.from_bytes(self.read_bytes(8), 'little')
                self.expiry_ms = ts
            elif opcode == OPCODE_EXPIRETIME_SEC:
                ts = int.from_bytes(self.read_bytes(4), 'little')
                self.expiry_ms = ts * 1000
            else:
                # Data type (string)
                value_type = opcode
                key_bytes = self.read_string()
                key = key_bytes.decode()
                expiry_dt = None
                if self.expiry_ms:
                    expiry_dt = datetime.datetime.fromtimestamp(self.expiry_ms / 1000.0)
                    self.expiry_ms = None

                if value_type == VALUE_TYPE_STRING:
                    val_bytes = self.read_string()
                    db[key] = Value(value=val_bytes, expiry=expiry_dt)
                else:
                    print(f"Skipping unsupported type {value_type}")
                    continue
        return db


def read_file_and_construct_kvm(file_dir: str, file_name: str) -> Dict[str, Value]:
    """Wrapper to load and return parsed RDB contents."""
    global global_file_dir, global_file_name
    global_file_dir = file_dir
    global_file_name = file_name

    try:
        with open(f"{file_dir}/{file_name}", "rb") as f:
            content = f.read()
        if not content:
            print("Empty RDB file")
            return {}
        parser = RdbParser(content)
        data = parser.parse()
        print(f"Parsed {len(data)} keys from RDB.")
        return data
    except FileNotFoundError:
        print(f"RDB not found at {file_dir}/{file_name}")
        return {}
    except Exception as e:
        print(f"Error reading RDB: {e}")
        traceback.print_exc()
        return {}


def send_rdb_file() -> bytes:
    """For replication (PSYNC): send the RDB file with length prefix."""
    with open(f"{global_file_dir}/{global_file_name}", "rb") as f:
        buf = f.read()
    header = f"${len(buf)}\r\n".encode()
    return header + buf