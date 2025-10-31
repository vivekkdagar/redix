from app.resp import resp_parser, resp_encoder, simple_string_encoder, error_encoder, array_encoder, parse_all
from app.utils import getter, setter, rpush, lrange, lpush, llen, lpop, blpop, type_getter_lists, increment, store_rdb, \
    keys
from app.utils2 import xadd, type_getter_streams, xrange, xread, blocks_xread
from time import sleep
import threading

blocked = {}
blocked_xread = {}
queue = []
REPLICAS = []
BYTES_READ = 0
replica_acks = 0
prev_cmd = ""
SUBSCRIBE = 0

RDB_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'

subscriptions = {}     # {connection: set(channel_names)}
channel_subs = {}      # {channel_name: set(connections)}
subscriber_mode = set()  # track connections in SUBSCRIBE mode
def cmd_executor(decoded_data, connection, config, queued, executing):
    global BYTES_READ, replica_acks, prev_cmd, SUBSCRIBE
    global subscriptions, channel_subs, subscriber_mode, REPLICAS

    print(f"decoded_data: {decoded_data}")

    # --------------------------- SUBSCRIBER MODE GUARD ---------------------------
    if connection in subscriber_mode:
        allowed = ["SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"]
        if decoded_data[0].upper() not in allowed:
            response = error_encoder(f"ERR Can't execute '{decoded_data[0]}' in subscriber mode")
            connection.sendall(response)
            return [], queued

    # --------------------------- EXEC TRANSACTION QUEUE ---------------------------
    if queued and decoded_data[0].upper() not in ["EXEC", "DISCARD"]:
        queue[len(queue) - 1].append(decoded_data)
        response = simple_string_encoder("QUEUED")
        connection.sendall(response)
        return [], queued

    # --------------------------- BASIC COMMANDS ---------------------------

    # PING
    # PING
    if decoded_data[0].upper() == "PING":
        BYTES_READ += len(resp_encoder(decoded_data))
        if connection in subscriber_mode:
            # Redis sends array form of PING when in SUBSCRIBE mode
            response = resp_encoder(["pong", ""])
        else:
            response = simple_string_encoder("PONG")
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # ECHO
    elif decoded_data[0].upper() == "ECHO" and len(decoded_data) > 1:
        response = resp_encoder(decoded_data[1])
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # GET
    elif decoded_data[0].upper() == "GET":
        response = resp_encoder(getter(decoded_data[1]))
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # SET
    elif decoded_data[0].upper() == "SET" and len(decoded_data) > 2:
        BYTES_READ += len(resp_encoder(decoded_data))
        if config['role'] == 'master':
            for replica in REPLICAS:
                replica.sendall(resp_encoder(decoded_data))
        setter(decoded_data[1:])
        response = simple_string_encoder("OK")
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued

    # --------------------------- LIST COMMANDS ---------------------------

    elif decoded_data[0].upper() == "RPUSH" and len(decoded_data) > 2:
        size = rpush(decoded_data[1:], blocked)
        response = resp_encoder(size)
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "LRANGE" and len(decoded_data) > 3:
        response = resp_encoder(lrange(decoded_data[1:]))
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "LPUSH":
        size = lpush(decoded_data[1:])
        response = resp_encoder(size)
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "LLEN" and len(decoded_data) > 1:
        size = llen(decoded_data[1])
        response = resp_encoder(size)
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "LPOP" and len(decoded_data) > 1:
        response = resp_encoder(lpop(decoded_data[1:]))
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "BLPOP" and len(decoded_data) > 2:
        response = blpop(decoded_data[1:], connection, blocked)
        if response is None:
            return [], queued
        else:
            response = resp_encoder(response)
            connection.sendall(response)
        return [], queued

    # --------------------------- STREAM COMMANDS ---------------------------

    elif decoded_data[0].upper() == "TYPE" and len(decoded_data) > 1:
        response = type_getter_lists(decoded_data[1])
        if response == "none":
            response2 = simple_string_encoder(type_getter_streams(decoded_data[1]))
            connection.sendall(response2)
        else:
            response = simple_string_encoder(response)
            connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "XADD" and len(decoded_data) > 4:
        result = xadd(decoded_data[1:], blocked_xread)
        if result[0] == "id":
            response = resp_encoder(result[1])
        else:
            response = error_encoder(result[1])
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "XRANGE" and len(decoded_data) >= 4:
        result = xrange(decoded_data[1:])
        response = resp_encoder(result)
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "XREAD" and len(decoded_data) >= 4:
        if decoded_data[1].upper() == "BLOCK":
            result = blocks_xread(decoded_data[2:], connection, blocked_xread)
            if result is None:
                return [], queued
        else:
            result = xread(decoded_data[2:])
            response = resp_encoder(result)
            connection.sendall(response)
        return [], queued

    # --------------------------- TRANSACTION COMMANDS ---------------------------

    elif decoded_data[0].upper() == "INCR" and len(decoded_data) > 1:
        response = increment(decoded_data[1])
        if response == -1:
            response = error_encoder("ERR value is not an integer or out of range")
        else:
            response = resp_encoder(response)
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == 'MULTI': #hi
        queued = True
        queue.append([])
        connection.sendall(simple_string_encoder("OK"))
        return [], queued

    elif decoded_data[0].upper() == 'EXEC': #exec command
        if queued:
            queued = False
            if not queue:
                connection.sendall(resp_encoder([]))
                return [], queued
            executing = True
            result = []
            q = queue.pop(0)
            for cmd in q:
                output, queued = cmd_executor(cmd, connection, config, queued, executing)
                result.append(output)
            executing = False
            connection.sendall(array_encoder(result))
            return [], queued
        else:
            connection.sendall(error_encoder("ERR EXEC without MULTI"))
            return [], queued

    elif decoded_data[0].upper() == 'DISCARD':
        if queued:
            queued = False
            queue.clear()
            connection.sendall(simple_string_encoder("OK"))
        else:
            connection.sendall(error_encoder("ERR DISCARD without MULTI"))
        return [], queued

    # --------------------------- REPLICATION COMMANDS ---------------------------

    elif decoded_data[0].upper() == "INFO":
        response = "role:" + config['role']
        if config['role'] == 'master':
            response += f"\nmaster_replid:{config['master_replid']}\nmaster_repl_offset:{config['master_replid_offset']}"
        connection.sendall(resp_encoder(response))
        return [], queued

    elif decoded_data[0].upper() == "REPLCONF":
        if decoded_data[1].upper() == "GETACK" and config['role'] == 'slave':
            response = resp_encoder(["REPLCONF", "ACK", str(BYTES_READ)])
            BYTES_READ += len(resp_encoder(decoded_data))
            connection.sendall(response)
        elif decoded_data[1].upper() == "ACK" and config['role'] == 'master':
            with threading.Lock():
                replica_acks += 1
        else:
            connection.sendall(simple_string_encoder("OK"))
        return [], queued

    elif decoded_data[0].upper() == "PSYNC":
        response = simple_string_encoder(f"FULLRESYNC {config['master_replid']} {config['master_replid_offset']}")
        connection.sendall(response)
        length = str(len(bytes.fromhex(RDB_hex))).encode('utf-8')
        rdb_resp = b"$" + length + b"\r\n" + bytes.fromhex(RDB_hex)
        connection.sendall(rdb_resp)
        REPLICAS.append(connection)
        return [], queued

    elif decoded_data[0].upper() == "WAIT":
        if prev_cmd != "SET":
            connection.sendall(resp_encoder(len(REPLICAS)))
            return [], queued
        else:
            min_replicas = int(decoded_data[1])
            timeout = int(decoded_data[2])
            for replica_conn in REPLICAS:
                replica_conn.send(resp_encoder(["REPLCONF", "GETACK", "*"]))
            sleep(timeout / 1000)
            with threading.Lock():
                ack_count = replica_acks
                replica_acks = 0
            connection.sendall(resp_encoder(ack_count))
            return [], queued

    # --------------------------- CONFIG & KEYS ---------------------------

    elif decoded_data[0].upper() == "CONFIG":
        if decoded_data[2] == "dir":
            response = resp_encoder(["dir", config['dir']])
        elif decoded_data[2] == "dbfilename":
            response = resp_encoder(["dbfilename", config['dbfilename']])
        else:
            response = error_encoder("ERR")
        connection.sendall(response)
        return [], queued

    elif decoded_data[0].upper() == "KEYS":
        connection.sendall(resp_encoder(keys()))
        return [], queued

    # --------------------------- PUB/SUB ---------------------------

    elif decoded_data[0].upper() == "SUBSCRIBE":
        SUBSCRIBE = 1
        channels = decoded_data[1:]
        if connection not in subscriptions:
            subscriptions[connection] = set()
        subscriber_mode.add(connection)
        for channel in channels:
            if channel not in channel_subs:
                channel_subs[channel] = set()
            if connection not in channel_subs[channel]:
                channel_subs[channel].add(connection)
                subscriptions[connection].add(channel)
            resp = resp_encoder(["subscribe", channel, len(subscriptions[connection])])
            connection.sendall(resp)
        return [], queued

    elif decoded_data[0].upper() == "UNSUBSCRIBE":
        channels = decoded_data[1:] or list(subscriptions.get(connection, []))
        for channel in channels:
            if connection in channel_subs.get(channel, set()):
                channel_subs[channel].remove(connection)
            if connection in subscriptions:
                subscriptions[connection].discard(channel)
            resp = resp_encoder(["unsubscribe", channel, len(subscriptions.get(connection, []))])
            connection.sendall(resp)
        if not subscriptions.get(connection):
            subscriber_mode.discard(connection)
        return [], queued

        # PUBLISH
    elif decoded_data[0].upper() == "PUBLISH":
        channel_name = decoded_data[1]
        message = decoded_data[2]
        tot_subscribers = 0
        for conn in subscriptions:
            if channel_name in subscriptions[conn]:
                tot_subscribers += 1
                conn.sendall(resp_encoder(["message", channel_name, message]))
        response = resp_encoder(tot_subscribers)
        connection.sendall(response)
        return [], queued

    # --------------------------- UNKNOWN ---------------------------
    else:
        connection.sendall(error_encoder("ERR unknown command"))
        return [], queued

def handle_client(connection, config, data=b""):
    global prev_cmd
    # store the data
    print(config)
    store_rdb(config['store'])
    print(f"handle_client called with data: {data}")
    buffer = data
    queued = False
    executing = False
    with connection:
        while True:
            print(f"handle_client called with buffer: {data}")
            if b"FULL" in data:
                _, data = data.split(b"\r\n", 1)
            print(f"handle_client called with buffer2: {buffer}")
            if not buffer:
                chunk = connection.recv(1024)
                if not chunk:
                    break
                buffer += chunk
            # else:
            #     chunk = connection.recv(1024)
            #     if not chunk:
            #         break
            #     buffer += chunk

            print(f"Received chunk: {buffer}")

            # Try to parse one or more complete RESP messages from buffer
            messages = parse_all(buffer)
            if not messages:
                continue  # incomplete data, wait for more

            for msg in messages:
                try:
                    # Convert bulk strings (bytes) to text
                    decoded_data = [
                        x.decode() if isinstance(x, (bytes, bytearray)) else x
                        for x in msg
                    ]

                    print(f"Parsed command: {decoded_data}")

                    if decoded_data[0] == 82 or decoded_data[0] == 'F':
                        continue

                    _, queued = cmd_executor(
                        decoded_data, connection, config, queued, executing
                    )

                    prev_cmd = decoded_data[0]

                except Exception as e:
                    print(f"Error handling command {msg}: {e}")

            buffer = b""