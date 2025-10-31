from typing import Any, Dict, Optional, List
import datetime
import dataclasses
import struct

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

    try:
        with open(f"{file_dir}/{file_name}", "rb") as f:
            buf = f.read()

        pos = 9  # Skip "REDIS0011" header
        # Find database selector (0xFE)
        while pos < len(buf) and buf[pos] != 0xFE:
            pos += 1
        if pos >= len(buf):
            return {}
        pos += 2  # skip 0xFE + db number

        while pos < len(buf) and buf[pos] != 0xFF:
            expiry_type, expiry_value, pos = read_expiry(buf, pos)

            if pos >= len(buf):
                break

            val_type = buf[pos]
            pos += 1

            if val_type != 0x00:  # only string supported
                raise NotImplementedError(f"Value type {val_type} not implemented")

            key, pos = read_string(buf, pos)
            val, pos = read_string(buf, pos)

            entry = Value(value=val, expiry=None)

            if expiry_type:
                if expiry_type == "ms":
                    expiry_value /= 1000
                entry.expiry = datetime.datetime.fromtimestamp(expiry_value)

            rdb_dict[key] = entry

        return rdb_dict

    except Exception as e:
        print("RDB parse error:", e)
        return {}


def read_length(buf, pos):
    first_byte = buf[pos]
    pos += 1
    type_bits = (first_byte & 0xC0) >> 6

    if type_bits == 0:
        length = first_byte & 0x3F
        return length, pos
    elif type_bits == 1:
        length = ((first_byte & 0x3F) << 8) | buf[pos]
        pos += 1
        return length, pos
    elif type_bits == 2:
        length = struct.unpack(">I", buf[pos:pos+4])[0]
        pos += 4
        return length, pos
    elif type_bits == 3:
        enc_type = first_byte & 0x3F
        raise NotImplementedError(f"Special encoding {enc_type} not supported")
    else:
        raise ValueError("Invalid length encoding")


def read_string(buf, pos):
    length, pos = read_length(buf, pos)
    val = buf[pos:pos + length]
    pos += length
    return val, pos


def read_expiry(buf, pos):
    expiry_type = buf[pos]
    if expiry_type == 0xFC:  # ms
        expiry_value = int.from_bytes(buf[pos + 1:pos + 9], "little")
        return "ms", expiry_value, pos + 9
    elif expiry_type == 0xFD:  # s
        expiry_value = int.from_bytes(buf[pos + 1:pos + 5], "little")
        return "s", expiry_value, pos + 5
    else:
        return None, None, pos


def send_rdb_file() -> bytes:
    with open(f"{global_file_dir}/{global_file_name}", "rb") as f:
        buf = f.read()
    return f"${len(buf)}\r\n".encode() + buf