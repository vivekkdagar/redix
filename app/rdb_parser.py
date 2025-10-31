from typing import Any, Dict, Optional, List
import datetime
import dataclasses
import os

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


def read_file_and_construct_kvm(file_dir: str, file_name: str) -> Dict[Any, Value]:
    rdb_dict = {}

    global global_file_dir, global_file_name
    global_file_dir = file_dir
    global_file_name = file_name

    file_path = os.path.join(file_dir, file_name)

    if not os.path.exists(file_path):
        print(f"[RDB] File not found: {file_path}")
        return rdb_dict

    try:
        with open(file_path, "rb") as f:
            buf = f.read()
            print(f"[RDB] File size: {len(buf)} bytes")

            if len(buf) < 9:
                print(f"[RDB] File too small")
                return rdb_dict

            pos = 9  # Skip "REDIS" + version header

            # Skip auxiliary fields and find database selector (0xFE)
            while pos < len(buf) and buf[pos] != 0xFE:
                if buf[pos] == 0xFA:  # Auxiliary field
                    pos += 1
                    # Skip key
                    key_len, pos = read_length(buf, pos)
                    pos += key_len if isinstance(key_len, int) else 0
                    # Skip value
                    val_len, pos = read_length(buf, pos)
                    pos += val_len if isinstance(val_len, int) else 0
                else:
                    pos += 1

            if pos >= len(buf):
                print(f"[RDB] No database selector found")
                return rdb_dict

            pos += 1  # Skip 0xFE
            if pos >= len(buf):
                return rdb_dict

            db_num = buf[pos]
            pos += 1
            print(f"[RDB] Database: {db_num}")

            # Handle hash table size info (0xFB) if present
            if pos < len(buf) and buf[pos] == 0xFB:
                pos += 1
                hash_size, pos = read_length(buf, pos)
                expiry_hash_size, pos = read_length(buf, pos)
                print(f"[RDB] Hash sizes: {hash_size}, {expiry_hash_size}")

            # Read key-value pairs until EOF marker (0xFF)
            entry_count = 0
            while pos < len(buf) and buf[pos] != 0xFF:
                # Check for expiry
                expiry_datetime = None
                if buf[pos] == 0xFC:  # Milliseconds
                    pos += 1
                    expiry_ms = int.from_bytes(buf[pos:pos + 8], "little")
                    expiry_datetime = datetime.datetime.fromtimestamp(expiry_ms / 1000)
                    pos += 8
                elif buf[pos] == 0xFD:  # Seconds
                    pos += 1
                    expiry_s = int.from_bytes(buf[pos:pos + 4], "little")
                    expiry_datetime = datetime.datetime.fromtimestamp(expiry_s)
                    pos += 4

                # Read value type
                if pos >= len(buf):
                    break
                val_type = buf[pos]
                pos += 1

                if val_type != 0x00:  # Only support string type
                    print(f"[RDB] Unsupported type: {hex(val_type)}")
                    break

                # Read key and value
                key, pos = read_string(buf, pos)
                val, pos = read_string(buf, pos)

                # Store as bytes (not decoded string)
                entry = Value(value=val, expiry=expiry_datetime)
                rdb_dict[key.decode()] = entry
                entry_count += 1

            print(f"[RDB] Loaded {entry_count} keys: {list(rdb_dict.keys())}")
        return rdb_dict
    except Exception as e:
        print(f"[RDB] Error: {e}")
        import traceback
        traceback.print_exc()
        return {}


def read_length(buf, pos):
    """Read Redis-style length-encoded field starting at pos"""
    if pos >= len(buf):
        return 0, pos

    first_byte = buf[pos]
    pos += 1
    type_bits = (first_byte & 0xC0) >> 6

    if type_bits == 0:
        length = first_byte & 0x3F
        return length, pos
    elif type_bits == 1:
        if pos >= len(buf):
            return 0, pos
        second_byte = buf[pos]
        pos += 1
        length = ((first_byte & 0x3F) << 8) | second_byte
        return length, pos
    elif type_bits == 2:
        if pos + 4 > len(buf):
            return 0, pos
        length = int.from_bytes(buf[pos:pos + 4], "big")
        pos += 4
        return length, pos
    elif type_bits == 3:
        enc_type = first_byte & 0x3F
        return ("special", enc_type), pos
    else:
        return 0, pos


def read_string(buf, pos):
    length, pos = read_length(buf, pos)
    if isinstance(length, tuple) and length[0] == "special":
        # Handle special encodings for integers
        enc_type = length[1]
        if enc_type == 0:  # 8-bit integer
            val = buf[pos:pos + 1]
            pos += 1
        elif enc_type == 1:  # 16-bit integer
            val = buf[pos:pos + 2]
            pos += 2
        elif enc_type == 2:  # 32-bit integer
            val = buf[pos:pos + 4]
            pos += 4
        else:
            raise NotImplementedError(f"Special encoding {enc_type} not supported")
        return val, pos

    if pos + length > len(buf):
        return b"", pos
    val = buf[pos:pos + length]
    pos += length
    return val, pos


def read_expiry(buf, pos) -> tuple:
    """Reads expiry time if present and returns (expiry_type, expiry_value, new_pos)"""
    if pos >= len(buf):
        return None, None, pos
    expiry_type = buf[pos]
    if expiry_type == 0xFC:  # milliseconds
        if pos + 9 > len(buf):
            return None, None, pos
        expiry_value = int.from_bytes(buf[pos + 1:pos + 9], "little")
        return "ms", expiry_value, pos + 9
    elif expiry_type == 0xFD:  # seconds
        if pos + 5 > len(buf):
            return None, None, pos
        expiry_value = int.from_bytes(buf[pos + 1:pos + 5], "little")
        return "s", expiry_value, pos + 5
    else:
        return None, None, pos  # No expiry


def send_rdb_file() -> bytes:
    file_path = os.path.join(global_file_dir, global_file_name)
    with open(file_path, "rb") as f:
        buf = f.read()
        header = f"${len(buf)}\r\n".encode()
        return header + buf