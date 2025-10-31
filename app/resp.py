import struct
import os
import datetime


def resp_parser(data):
    if data.startswith(b"*"):
        parts = data.split(b"\r\n")
        num_elements = int(parts[0][1:])
        elements = []
        index = 1
        for _ in range(num_elements):
            length = int(parts[index][1:])
            element = parts[index + 1][:length].decode()
            print(f"Parsed element: {element}")
            elements.append(element)
            index += 2
        return elements
    return []


def parse_all(data):
    """Parse as many complete RESP messages as possible from data."""
    messages = []
    buffer = data

    while buffer:
        try:
            msg, buffer = parse_next(buffer)
            messages.append(msg)
        except Exception:
            # Partial message or invalid data, keep buffer for next recv
            break
    print(f"Messages parsed: {messages}")
    return messages


def parse_next(data):
    print(f"parse_next called with data: {data}")
    first, data = data.split(b"\r\n", 1)
    match first[:1]:
        case b"*":
            value = []
            l = int(first[1:].decode())
            for _ in range(l):
                item, data = parse_next(data)
                value.append(item)
            return value, data
        case b"$":
            l = int(first[1:].decode())
            blk = data[:l]
            if (data[l:l + 2] != b"\r\n"):
                data = data[l:]
                return blk, data
            data = data[l + 2:]
            return blk, data

        case b"+":
            return first[1:].decode(), data

        case _:
            raise RuntimeError(f"Parse not implemented: {first[:1]}")


def resp_encoder(data):
    if data is None:
        return b"$-1\r\n"
    elif isinstance(data, list):
        out = f"*{len(data)}\r\n".encode()
        for item in data:
            out += resp_encoder(item)
        return out
    elif isinstance(data, str):
        return_string = "$"
        return_string += str(len(data)) + "\r\n"
        return_string += data + "\r\n"
        return return_string.encode()
    elif isinstance(data, int):
        return_string = ":" + str(data) + "\r\n"
        return return_string.encode()
    return b"$-1\r\n"


def simple_string_encoder(message):
    return f"+{message}\r\n".encode()


def error_encoder(message):
    return f"-{message}\r\n".encode()


def array_encoder(data):
    """
    Takes a list of RESP-encoded byte strings (results)
    and wraps them in a single RESP array.
    """

    # Build the RESP array header
    merged = f"*{len(data)}\r\n".encode()

    # Append all byte responses directly
    for r in data:
        merged += r
    return merged


def read_key_val_from_db(dir, dbfilename, data):
    rdb_file_loc = dir + "/" + dbfilename
    if not os.path.isfile(rdb_file_loc):
        return
    with open(rdb_file_loc, "rb") as f:
        while operand := f.read(1):
            if operand == b"\xfb":
                break
        numKeys = struct.unpack("B", f.read(1))[0]
        f.read(1)
        print("NumKeys: ", numKeys)
        for i in range(numKeys):
            expired = False
            print(i)
            top = f.read(1)
            if top == b"\xfc":
                milliTime = int.from_bytes(f.read(8), byteorder="little")
                now = datetime.datetime.now().timestamp() * 1000
                if milliTime < now:
                    print("milliTime: ", milliTime)
                    print("now: ", now)
                    expired = True
                f.read(1)
            elif top == b"\xfd":
                secTime = int.from_bytes(f.read(4), byteorder="little")
                if secTime < datetime.datetime.now().timestamp():
                    expired = True
                f.read(1)

            length = struct.unpack("B", f.read(1))[0]
            if length >> 6 == 0b00:
                length = length & 0b00111111
            else:
                length = 0
            currKey = f.read(length).decode()

            length = struct.unpack("B", f.read(1))[0]
            if length >> 6 == 0b00:
                length = length & 0b00111111
            else:
                length = 0
            print("Key:", currKey)
            val = f.read(length).decode()
            print("Value:", val)
            if not expired:
                data[currKey] = (val, -1)

        return

def bulk_string_encoder(value):
    if value is None:
        return b"$-1\r\n"
    return f"${len(value)}\r\n{value}\r\n".encode()

def integer_encoder(num):
    return f":{num}\r\n".encode()