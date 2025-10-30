from typing import Any, Dict, Optional, List
import datetime
import dataclasses

global_file_dir = "" # do we need this
global_file_name =""

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
        with open(file_dir + "/" + file_name, "rb") as f:
            buf = f.read()
            pos = 9  # Skip "REDIS0011" header

            # Find database selector (0xFE)
            while pos < len(buf) and buf[pos] != 0xFE:
                pos += 1

            if pos >= len(buf):
                return {}

            pos += 1  # Skip 0xFE

            # Read database number (length-encoded)
            db_number, pos = read_length(buf, pos)

            # Skip the hash table size information (0xFB)
            if pos < len(buf) and buf[pos] == 0xFB:
                pos += 1
                hash_table_size, pos = read_length(buf, pos)
                expiry_hash_table_size, pos = read_length(buf, pos)

            # Read key-value pairs
            while pos < len(buf) and buf[pos] != 0xFF:
                # Check for expiry first
                expiry_type, expiry_value, pos = read_expiry(buf, pos)

                val_type = buf[pos]
                pos += 1

                if val_type != 0x00:
                    raise NotImplementedError(f"Value type {val_type} not implemented")

                key, pos = read_string(buf, pos)
                val, pos = read_string(buf, pos)

                entry = Value(value=val.decode(), expiry=None)

                if expiry_type:
                    if expiry_type == "ms":
                        expiry_value /= 1000  # type: ignore
                    entry.expiry = datetime.datetime.fromtimestamp(expiry_value)  # Fixed this line

                rdb_dict[key.decode()] = entry
        return rdb_dict
    except Exception as e:
        print(f"Error reading RDB file: {e}")
        import traceback
        traceback.print_exc()
        return {}


def read_length(buf, pos):
    """Read Redis-style length-encoded field starting at pos"""
    first_byte = buf[pos]
    pos += 1
    type_bits = (first_byte & 0xC0) >> 6

    if type_bits == 0:
        length = first_byte & 0x3F
        return length, pos
    elif type_bits == 1:
        second_byte = buf[pos]
        pos += 1
        length = ((first_byte & 0x3F) << 8) | second_byte
        return length, pos
    elif type_bits == 2:
        length = int.from_bytes(buf[pos:pos+4], "big")
        pos += 4
        return length, pos
    elif type_bits == 3:
        enc_type = first_byte & 0x3F
        return ("special", enc_type), pos
    else:
        raise ValueError("Invalid length encoding")

def read_string(buf, pos):
    length, pos = read_length(buf, pos)
    if isinstance(length, tuple) and length[0] == "special":
        raise NotImplementedError(f"Special encoding {length[1]} not supported yet")
    val = buf[pos:pos+length]
    pos += length
    return val, pos

def read_expiry(buf, pos) -> tuple:
    """Reads expiry time if present and returns (expiry_type, expiry_value, new_pos)"""
    expiry_type = buf[pos]
    if expiry_type == 0xFC:  # milliseconds
        expiry_value = int.from_bytes(buf[pos+1:pos+9], "little")
        return "ms", expiry_value, pos + 9
    elif expiry_type == 0xFD:  # seconds
        expiry_value = int.from_bytes(buf[pos+1:pos+5], "little")
        return "s", expiry_value, pos + 5
    else:
        return None, None, pos  # No expiry

def send_rdb_file() -> bytes: 
    with open(global_file_dir+"/"+global_file_name, "rb") as f:
        buf = f.read()
        header = f"${len(buf)}\r\n".encode()
        return header + buf