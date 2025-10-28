import asyncio
import time
from collections import deque

# ==============================
# REDIS CONSTANTS
# ==============================
ECHO_COMMAND = "ECHO"
PING_COMMAND = "PING"
SET_COMMAND = "SET"
GET_COMMAND = "GET"
RPUSH_COMMAND = "RPUSH"
LPUSH_COMMAND = "LPUSH"
LRANGE_COMMAND = "LRANGE"
LPOP_COMMAND = "LPOP"
LLEN_COMMAND = "LLEN"

PONG_RESP = b"+PONG\r\n"
OK_RESP = b"+OK\r\n"
NULL_RESP = b"$-1\r\n"
EMPTY_ARRAY = b"*0\r\n"

# ==============================
# IN-MEMORY DATABASE
# ==============================
REDIS_DATABASE = {}


# ==============================
# DATA HELPERS
# ==============================
def get_data_from_redis(key):
    """Get value (with expiry check)"""
    data = REDIS_DATABASE.get(key)
    if data is None:
        return None
    if "exp" in data and data["exp"]:
        now = int(time.time() * 1000)
        if now > data["exp"]:
            del REDIS_DATABASE[key]
            return None
    return data["value"]


def add_data_to_redis(key, value, exp=None):
    """Add key-value pair with optional expiration"""
    now = int(time.time() * 1000)
    REDIS_DATABASE[key] = {"value": value, "exp": now + exp if exp else None}


def add_data_to_redis_list(key, values, side):
    """Append or prepend elements to a list"""
    if key not in REDIS_DATABASE:
        REDIS_DATABASE[key] = {"value": deque(), "exp": None}

    if side == "left":
        for v in values:
            REDIS_DATABASE[key]["value"].appendleft(v)
    else:
        for v in values:
            REDIS_DATABASE[key]["value"].append(v)

    return len(REDIS_DATABASE[key]["value"])


def remove_data_from_redis_list(key, count=None):
    """Pop one or multiple elements from list"""
    if key not in REDIS_DATABASE:
        return None

    dq = REDIS_DATABASE[key]["value"]

    if not dq:
        return None

    if count is None:
        return dq.popleft()

    popped = []
    for _ in range(int(count)):
        if not dq:
            break
        popped.append(dq.popleft())
    return popped


# ==============================
# RESP CONVERTERS
# ==============================
def convert_str_to_redis_response(output):
    return f"${len(output)}\r\n{output}\r\n".encode()


def convert_int_to_redis_response(output):
    return f":{output}\r\n".encode()


def convert_array_to_redis_response(output):
    resp = f"*{len(output)}\r\n"
    for el in output:
        resp += f"${len(el)}\r\n{el}\r\n"
    return resp.encode()


# ==============================
# COMMAND PARSERS
# ==============================
def parse_echo_command(keywords, index):
    return convert_str_to_redis_response(keywords[index + 2])


def parse_ping_command(*args, **kwargs):
    return PONG_RESP


def parse_set_command(keywords, index):
    key = keywords[index + 2]
    value = keywords[index + 4]
    exp_value = None

    if len(keywords) > index + 5:
        exp_key = keywords[index + 6]
        exp_value = keywords[index + 8]
        if exp_key == "EX":
            exp_value = int(exp_value) * 1000
        else:
            exp_value = int(exp_value)

    add_data_to_redis(key, value, exp_value)
    return OK_RESP


def parse_get_command(keywords, index):
    key = keywords[index + 2]
    val = get_data_from_redis(key)
    if val is None:
        return NULL_RESP
    return convert_str_to_redis_response(val)


def parse_push_command(keywords, index, side):
    key = keywords[index + 2]
    to_add = []
    start = index + 4
    while start < len(keywords):
        to_add.append(keywords[start])
        start += 2

    resp = add_data_to_redis_list(key, to_add, side)
    return convert_int_to_redis_response(resp)


def parse_lpush_command(keywords, index):
    return parse_push_command(keywords, index, "left")


def parse_rpush_command(keywords, index):
    return parse_push_command(keywords, index, "right")


def parse_lrange_command(keywords, index):
    key = keywords[index + 2]
    start = int(keywords[index + 4])
    end = int(keywords[index + 6])
    val = get_data_from_redis(key)
    if val is None:
        return EMPTY_ARRAY

    val = list(val)
    if end == -1:
        sliced = val[start:]
    else:
        sliced = val[start:end + 1]
    return convert_array_to_redis_response(sliced)


def parse_lpop_command(keywords, index):
    key = keywords[index + 2]
    count = None
    if len(keywords) > index + 3:
        count = keywords[index + 4]

    resp = remove_data_from_redis_list(key, count)
    if resp is None:
        return NULL_RESP
    if isinstance(resp, list):
        return convert_array_to_redis_response(resp)
    return convert_str_to_redis_response(resp)


def parse_llen_command(keywords, index):
    key = keywords[index + 2]
    val = get_data_from_redis(key)
    if val is None:
        return convert_int_to_redis_response(0)
    return convert_int_to_redis_response(len(val))


# ==============================
# REDIS COMMAND DISPATCHER
# ==============================
command_arg_map = {
    ECHO_COMMAND: parse_echo_command,
    PING_COMMAND: parse_ping_command,
    SET_COMMAND: parse_set_command,
    GET_COMMAND: parse_get_command,
    RPUSH_COMMAND: parse_rpush_command,
    LPUSH_COMMAND: parse_lpush_command,
    LRANGE_COMMAND: parse_lrange_command,
    LPOP_COMMAND: parse_lpop_command,
    LLEN_COMMAND: parse_llen_command,
}


def parse_redis_command(data):
    keywords = [kw for kw in data.split("\r\n") if kw]
    i = 1
    command = keywords[i + 1].upper()
    func = command_arg_map.get(command)
    if func:
        return func(keywords, i + 1)
    return b"-ERR unknown command\r\n"


# ==============================
# ASYNCIO SERVER
# ==============================
async def handle_client(reader, writer):
    while True:
        data = await reader.read(1024)
        if not data:
            break
        response = parse_redis_command(data.decode())
        writer.write(response)
        await writer.drain()
    writer.close()


async def main():
    print("Redis clone running on port 6379")
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())