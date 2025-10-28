import socket  # noqa: F401
import threading  # noqa: F401
import time  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    redis_data = {}
    redis_ttl = {}
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen()

    def handle_client(conn: socket.socket):
        try:
            while True:
                cmd = conn.recv(1024)
                if not cmd:
                    break
                if b"PING" in cmd:
                    conn.send(create_resp_simple_string("PONG"))
                elif b"ECHO" in cmd:
                    message = cmd.split(b"\r\n")[-2]
                    conn.send(create_resp_bulk_string(message))
                elif b"SET" in cmd:
                    redis_data[cmd.split(b"\r\n")[4]] = cmd.split(b"\r\n")[6]
                    if len(cmd.split(b"\r\n")) > 8:
                        now = time.time()
                        if cmd.split(b"\r\n")[8] == b"PX":
                            expiry = now + int(cmd.split(b"\r\n")[10]) / 1000
                            redis_ttl[cmd.split(b"\r\n")[4]] = expiry
                        elif cmd.split(b"\r\n")[8] == b"EX":
                            expiry = now + int(cmd.split(b"\r\n")[10])
                            redis_ttl[cmd.split(b"\r\n")[4]] = expiry
                    conn.send(create_resp_simple_string("OK"))
                elif b"GET" in cmd:
                    key = cmd.split(b"\r\n")[-2]
                    if key not in redis_data:
                        conn.send(create_resp_bulk_string(b""))
                        continue
                    if key in redis_ttl:
                        if time.time() > redis_ttl[key]:
                            del redis_data[key]
                            del redis_ttl[key]
                    value = redis_data.get(key, b"")
                    conn.send(create_resp_bulk_string(value))
                elif b"RPUSH" in cmd:
                    key = cmd.split(b"\r\n")[4]
                    if len(cmd.split(b"\r\n")) > 7:
                        values = []
                        for i in range(6, len(cmd.split(b"\r\n")) - 1, 2):
                            values.append(cmd.split(b"\r\n")[i])
                        if key not in redis_data:
                            redis_data[key] = []
                        redis_data[key].extend(values)
                        conn.send(create_resp_integer(len(redis_data[key])))
                        continue
                    value = cmd.split(b"\r\n")[6]
                    if key not in redis_data:
                        redis_data[key] = []
                    redis_data[key].append(value)
                    conn.send(create_resp_integer(len(redis_data[key])))
                elif b"LRANGE" in cmd:
                    key = cmd.split(b"\r\n")[4]
                    start = int(cmd.split(b"\r\n")[6])
                    end = int(cmd.split(b"\r\n")[8])
                    if (
                        key not in redis_data
                        or start >= len(redis_data[key])
                        or (end > 0 and start > end)
                    ):
                        conn.send(b"*0\r\n")
                        continue
                    lst = redis_data[key]
                    if end == -1:
                        end = len(lst) - 1
                    else:
                        end = min(end, len(lst) - 1)
                    result = lst[start : end + 1]
                    resp = b"*" + str(len(result)).encode() + b"\r\n"
                    for item in result:
                        resp += create_resp_bulk_string(item)
                    conn.send(resp)
                elif b"LPUSH" in cmd:
                    key = cmd.split(b"\r\n")[4]
                    if len(cmd.split(b"\r\n")) > 7:
                        values = []
                        for i in range(6, len(cmd.split(b"\r\n")) - 1, 2):
                            values.append(cmd.split(b"\r\n")[i])
                        if key not in redis_data:
                            redis_data[key] = []
                        redis_data[key] = values[::-1] + redis_data[key]
                        conn.send(create_resp_integer(len(redis_data[key])))
                        continue
                    value = cmd.split(b"\r\n")[6]
                    if key not in redis_data:
                        redis_data[key] = []
                    redis_data[key].insert(0, value)
                    conn.send(create_resp_integer(len(redis_data[key])))
                elif b"LLEN" in cmd:
                    key = cmd.split(b"\r\n")[-2]
                    length = len(redis_data.get(key, []))
                    conn.send(create_resp_integer(length))
                elif b"BLPOP" in cmd:
                    key = cmd.split(b"\r\n")[4]
                    if len(cmd.split(b"\r\n")) > 6:
                        timeout = float(cmd.split(b"\r\n")[6])
                    else:
                        timeout = 0
                    end_time = time.time() + timeout
                    flag = False
                    lock = threading.Lock()
                    with lock:
                        while time.time() < end_time or timeout == 0:
                            if key in redis_data and len(redis_data[key]) > 0:
                                value = redis_data[key].pop(0)
                                conn.send(create_resp_array([key, value], 2))
                                flag = True
                                break
                            time.sleep(0.1)
                    if not flag:
                        conn.send(create_resp_array([], -1))
                elif b"LPOP" in cmd:
                    key = cmd.split(b"\r\n")[4]
                    if key not in redis_data or len(redis_data[key]) == 0:
                        conn.send(create_resp_bulk_string(b""))
                        continue
                    if len(cmd.split(b"\r\n")) > 6:
                        count = int(cmd.split(b"\r\n")[6])
                        count = min(count, len(redis_data[key]))
                        values = []
                        for _ in range(count):
                            value = redis_data[key].pop(0)
                            values.append(value)
                        conn.send(create_resp_array(values, count))
                        continue
                    value = redis_data[key].pop(0)
                    conn.send(create_resp_bulk_string(value))
                else:
                    break
        finally:
            conn.close()

    def create_resp_array(items: list, count: int) -> bytes:
        if count == -1:
            return b"*-1\r\n"
        resp = b"*" + str(count).encode() + b"\r\n"
        for item in items:
            resp += create_resp_bulk_string(item)
        return resp

    def create_resp_simple_string(message: str) -> bytes:
        return b"+" + message.encode() + b"\r\n"

    def create_resp_integer(number: int) -> bytes:
        return b":" + str(number).encode() + b"\r\n"

    def create_resp_bulk_string(message: bytes) -> bytes:
        if not message:
            return b"$-1\r\n"
        return b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"

    while True:
        connection, _ = server_socket.accept()  # wait for client
        t = threading.Thread(target=handle_client, args=(connection,), daemon=True)
        t.start()


if __name__ == "__main__":
    main()