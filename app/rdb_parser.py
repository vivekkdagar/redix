from typing import Any, Dict, Optional, List
import datetime
import dataclasses

# Global tracking for current RDB file (useful for PSYNC and replication)
global_file_dir = ""
global_file_name = ""


@dataclasses.dataclass
class XADDValue:
    value: Any
    milliseconds: Optional[int]
    sequence: Optional[int]


@dataclasses.dataclass
class Value:
    value: Any | List[XADDValue]
    expiry: Optional[datetime.datetime]


def read_file_and_construct_kvm(file_dir: str, file_name: str) -> Dict[str, Value]:
    """Reads a Redis RDB file and returns a {key: Value(...)} dict."""
    global global_file_dir, global_file_name
    global_file_dir = file_dir
    global_file_name = file_name

    db: Dict[str, Value] = {}

    try:
        with open(f"{file_dir}/{file_name}", "rb") as f:
            buf = f.read()

        # Must start with REDIS0011
        if not buf.startswith(b"REDIS"):
            print("Invalid RDB header")
            return {}

        pos = 9  # skip "REDIS0011"

        # Skip until 0xFE (database selector)
        while pos < len(buf) and buf[pos] != 0xFE:
            pos += 1
        pos += 1  # skip 0xFE
        pos += 1  # skip db number

        # Skip next metadata bytes until actual key-value pairs
        while pos < len(buf) and buf[pos] != 0xFB and buf[pos] != 0xFC and buf[pos] != 0x00:
            pos += 1

        # Start reading key-value pairs
        while pos < len(buf) and buf[pos] != 0xFF:
            expiry_type, expiry_value, pos = read_expiry(buf, pos)

            # Value type byte (0x00 for string)
            val_type = buf[pos]
            pos += 1
            if val_type != 0x00:
                # Codecrafters only tests string values
                raise NotImplementedError(f"Unsupported value type: {val_type}")

            key, pos = read_string(buf, pos)
            val, pos = read_string(buf, pos)

            key_str = key.decode()
            val_bytes = val  # keep as bytes (not str)

            # Handle expiry conversion
            expiry = None
            if expiry_type == "ms":
                expiry = datetime.datetime.fromtimestamp(expiry_value / 1000.0)
            elif expiry_type == "s":
                expiry = datetime.datetime.fromtimestamp(expiry_value)

            db[key_str] = Value(value=val_bytes, expiry=expiry)

        return db

    except FileNotFoundError:
        print(f"RDB file not found: {file_dir}/{file_name}")
        return {}
    except Exception as e:
        print("Error parsing RDB:", e)
        return {}


def read_length(buf: bytes, pos: int):
    """Reads Redis-style length-encoded integers."""
    first_byte = buf[pos]
    pos += 1
    type_bits = (first_byte & 0xC0) >> 6

    if type_bits == 0:
        length = first_byte & 0x3F
    elif type_bits == 1:
        second_byte = buf[pos]
        pos += 1
        length = ((first_byte & 0x3F) << 8) | second_byte
    elif type_bits == 2:
        length = int.from_bytes(buf[pos:pos + 4], "big")
        pos += 4
    else:
        raise ValueError("Invalid length encoding")

    return length, pos


def read_string(buf: bytes, pos: int):
    """Reads a string with Redis length encoding."""
    length, pos = read_length(buf, pos)
    val = buf[pos:pos + length]
    pos += length
    return val, pos


def read_expiry(buf: bytes, pos: int):
    """Reads expiry metadata, if present."""
    expiry_type = buf[pos]
    if expiry_type == 0xFC:  # Millisecond precision expiry
        expiry_value = int.from_bytes(buf[pos + 1:pos + 9], "little")
        return "ms", expiry_value, pos + 9
    elif expiry_type == 0xFD:  # Second precision expiry
        expiry_value = int.from_bytes(buf[pos + 1:pos + 5], "little")
        return "s", expiry_value, pos + 5
    else:
        return None, None, pos


def send_rdb_file() -> bytes:
    """Reads and returns RDB file contents for PSYNC (replication)."""
    with open(f"{global_file_dir}/{global_file_name}", "rb") as f:
        buf = f.read()
        header = f"${len(buf)}\r\n".encode()
        return header + buf