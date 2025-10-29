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
subscriptions = {}

RDB_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'


def cmd_executor(decoded_data, connection, config, queued, executing):
    global BYTES_READ
    global replica_acks
    global prev_cmd
    global SUBSCRIBE
    global subscriptions
    print(f"decoded_data: {decoded_data}")
    # EXEC Checker
    if SUBSCRIBE == 1 and connection in subscriptions:
        if decoded_data[0].upper() == "UNSUBSCRIBE":
            pass
        elif decoded_data[0].upper() == "SUBSCRIBE":
            if connection not in subscriptions:
                subscriptions[connection] = set()
            if decoded_data[1] not in subscriptions[connection]:
                subscriptions[connection].add(decoded_data[1])
            # subscriptions = list(set(subscriptions))
            response = resp_encoder(['subscribe', decoded_data[1], len(subscriptions[connection])])
            connection.sendall(response)
            return [], queued
        elif decoded_data[0].upper() == "PSUBSCRIBE":
            pass
        elif decoded_data[0].upper() == "PUNSUBSCRIBE":
            pass
        elif decoded_data[0].upper() == "PING":
            response = resp_encoder(['pong', ""])
            connection.sendall(response)
            return [], queued
        elif decoded_data[0].upper() == "QUIT":
            pass
        else:
            response = error_encoder(f"ERR Can't execute '{decoded_data[0]}'")
            connection.sendall(response)
        return [], queued
    elif queued and decoded_data[0] != "EXEC" and decoded_data[0] != "DISCARD":
        # append to last queue
        queue[len(queue) - 1].append(decoded_data)
        response = simple_string_encoder("QUEUED")
        # if queued:
        #     return response, queued
        connection.sendall(response)
        return [], queued

    # PING
    elif decoded_data[0] == "PING":
        BYTES_READ += len(resp_encoder(decoded_data))
        response = simple_string_encoder("PONG")
        if executing:
            return response, queued
        if config['role'] == 'master':
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
        print("role", config['role'])
        response = resp_encoder(getter(decoded_data[1]))
        print(f"GET response: {response}")
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued
    # SET
    elif decoded_data[0].upper() == "SET" and len(decoded_data) > 2:
        print(f"SET data: {decoded_data}")
        BYTES_READ += len(resp_encoder(decoded_data))
        if config['role'] == 'master':
            for replica in REPLICAS:
                print(f"replica: {replica}")
                replica.sendall(resp_encoder(decoded_data))
        print("role", config['role'])
        setter(decoded_data[1:])
        response = "+OK\r\n".encode()
        if executing:
            return response, queued
        if config['role'] == 'master':
            connection.sendall(response)
        return [], queued

    ############################## LISTS ##############################

    # RPUSH
    elif decoded_data[0].upper() == "RPUSH" and len(decoded_data) > 2:
        # For simplicity, we treat RPUSH similar to SET in this implementation
        size = rpush(decoded_data[1:], blocked)
        response = resp_encoder(size)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued
    # LRANGE
    elif decoded_data[0].upper() == "LRANGE" and len(decoded_data) > 3:
        response = resp_encoder(lrange(decoded_data[1:]))
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued
    # LPUSH
    elif decoded_data[0].upper() == "LPUSH":
        size = lpush(decoded_data[1:])
        response = resp_encoder(size)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued
    # LLEN
    elif decoded_data[0].upper() == "LLEN" and len(decoded_data) > 1:
        size = llen(decoded_data[1])
        response = resp_encoder(size)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued
    # LPOP
    elif decoded_data[0].upper() == "LPOP" and len(decoded_data) > 1:
        response = resp_encoder(lpop(decoded_data[1:]))
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued
    # BLPOP
    elif decoded_data[0].upper() == "BLPOP" and len(decoded_data) > 2:
        response = blpop(decoded_data[1:], connection, blocked)
        if response is None:
            return [], queued
        else:
            response = resp_encoder(response)
            if executing:
                return response, queued
            connection.sendall(response)
        return [], queued

    ############################## STREAMS ##############################

    # TYPE
    elif decoded_data[0].upper() == "TYPE" and len(decoded_data) > 1:
        response = type_getter_lists(decoded_data[1])
        print(f"TYPE response: {response}")
        if response == "none":
            response2 = simple_string_encoder(type_getter_streams(decoded_data[1]))
            if executing:
                return response2, queued
            connection.sendall(response2)
        else:
            response = simple_string_encoder(response)
            if executing:
                return response, queued
            connection.sendall(response)
        return [], queued

    # XADD
    elif decoded_data[0].upper() == "XADD" and len(decoded_data) > 4:
        result = xadd(decoded_data[1:], blocked_xread)
        print(f"XADD result: {result}")
        if result[0] == "id":
            response = resp_encoder(result[1])
            if executing:
                return response, queued
            connection.sendall(response)
        else:
            response = error_encoder(result[1])
            if executing:
                return response, queued
            connection.sendall(response)
        return [], queued
    # XRANGE
    elif decoded_data[0].upper() == "XRANGE" and len(decoded_data) >= 4:
        # print(f"XRANGE called with data: {decoded_data}")
        result = xrange(decoded_data[1:])
        response = resp_encoder(result)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued
    # XREAD STREAMS
    elif decoded_data[0].upper() == "XREAD" and len(decoded_data) >= 4:
        if decoded_data[1].upper() == "BLOCK":
            result = blocks_xread(decoded_data[2:], connection, blocked_xread)
            if result is None:
                return [], queued
            # response = resp_encoder(result)
        else:
            result = xread(decoded_data[2:])
            response = resp_encoder(result)
            if executing:
                return response, queued
            connection.sendall(response)

        return [], queued

    ########################## TRANSACTION ############################

    # INCR
    elif decoded_data[0].upper() == "INCR" and len(decoded_data) > 1:
        response = increment(decoded_data[1])
        if response == -1:
            response = error_encoder("ERR value is not an integer or out of range")
        else:
            response = resp_encoder(response)
        if executing:
            return response, queued
        connection.sendall(response)
        return [], queued
    # MULTI
    elif decoded_data[0].upper() == 'MULTI':
        # queue.append(decoded_data)
        queued = True
        queue.append([])
        response = simple_string_encoder("OK")
        connection.sendall(response)
        return [], queued
    # EXEC
    elif decoded_data[0].upper() == 'EXEC':
        if queued == True:
            queued = False
            print(f"EXEC queue: {queue}")
            if len(queue) == 0:
                response = resp_encoder([])
                # print(f"EXEC response: {response}")
                connection.sendall(response)
                return [], queued
                # continue
            else:
                executing = True
                result = []
                q = queue.pop(0)
                for cmd in q:
                    print(f"EXEC cmd: {cmd}")
                    output, queued = cmd_executor(cmd, connection, config, queued, executing)
                    result.append(output)
                    print(f"EXEC result: {result}")
                # queue.clear()
                executing = False
                response = array_encoder(result)
                connection.sendall(response)
                return [], queued
        else:
            response = error_encoder("ERR EXEC without MULTI")
            connection.sendall(response)
            return [], queued
    # DISCARD
    elif decoded_data[0].upper() == 'DISCARD':
        if queued == True:
            queued = False
            queue.clear()
            response = simple_string_encoder("OK")
            connection.sendall(response)
            return [], queued
        else:
            response = error_encoder("ERR DISCARD without MULTI")
            connection.sendall(response)
            return [], queued

    ############################### REPLCATION ##############################

    # INFO
    elif decoded_data[0].upper() == "INFO":
        response = "role:" + config['role']
        if config['role'] == 'master':
            response += f"\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:{config['master_replid_offset']}"
        response = resp_encoder(response)
        if executing:
            return response, queued
        print(f"INFO response: {response}")
        connection.sendall(response)
        return [], queued

    # REPLCONF
    elif decoded_data[0].upper() == "REPLCONF":
        if decoded_data[1].upper() == "GETACK" and config['role'] == 'slave':
            response = resp_encoder(["REPLCONF", "ACK", str(BYTES_READ)])
            BYTES_READ += len(resp_encoder(decoded_data))
            # if executing:
            #     return response, queued
            connection.sendall(response)
        elif decoded_data[1].upper() == "ACK" and config['role'] == 'master':
            with threading.Lock():
                replica_acks += 1
        else:
            response = simple_string_encoder("OK")
            # if executing:
            #     return response, queued
            connection.sendall(response)
        return [], queued

    # PSYNC
    elif decoded_data[0].upper() == "PSYNC":
        response = "FULLRESYNC " + config['master_replid'] + " " + str(config['master_replid_offset'])
        response = simple_string_encoder(response)
        # if executing:
        #     return response, queued
        connection.sendall(response)
        response = b"$"
        # decode RDB_hex to binary
        length = str(len(bytes.fromhex(RDB_hex))).encode('utf-8')
        response += length
        response += b"\r\n"
        response += bytes.fromhex(RDB_hex)
        print(f"PSYNC response: {response}")
        connection.sendall(response)
        REPLICAS.append(connection)
        return [], queued
    # WAIT
    elif decoded_data[0].upper() == "WAIT":
        print("prev_cmd:", prev_cmd)
        if prev_cmd != "SET":
            response = resp_encoder(len(REPLICAS))
            if config['role'] == 'master':
                connection.sendall(response)
            return [], queued
        else:
            min_replicas = int(decoded_data[1])
            timeout = int(decoded_data[2])
            ack_count = 0
            for replica_conn in REPLICAS:
                print("replica_conn:", replica_conn)
                replica_conn.send(resp_encoder(["REPLCONF", "GETACK", "*"]))

            sleep(timeout / 1000)

            with threading.Lock():
                ack_count = replica_acks
                replica_acks = 0
            response = resp_encoder(ack_count)
            # if executing:
            #     return response, queued
            if config['role'] == 'master':
                connection.sendall(response)
            return [], queued

    ############################### RDB ###############################

    # CONGIG GET
    elif decoded_data[0].upper() == "CONFIG":
        print(f"CONFIG command")
        response = ""
        if decoded_data[2] == "dir":
            response = resp_encoder(["dir", config['dir']])
        elif decoded_data[2] == "dbfilename":
            response = resp_encoder(["dbfilename", config['dbfilename']])
        else:
            response = error_encoder("ERR")
        # if executing:
        #     return response, queued
        connection.sendall(response)
        return [], queued
    # KEYS
    elif decoded_data[0].upper() == "KEYS":
        response = resp_encoder(keys())
        # if executing:
        #     return response, queued
        connection.sendall(response)
        return [], queued

    ############################# PUB/SUB ########################
    elif decoded_data[0].upper() == "SUBSCRIBE":
        print(connection)
        SUBSCRIBE = 1
        if connection not in subscriptions:
            subscriptions[connection] = set()
        if decoded_data[1] not in subscriptions[connection]:
            subscriptions[connection].add(decoded_data[1])
        # subscriptions = list(set(subscriptions))
        response = resp_encoder(['subscribe', decoded_data[1], len(subscriptions[connection])])
        connection.sendall(response)
        return [], queued
    # ERR
    else:
        response = error_encoder("ERR")
        connection.sendall(response)
        return [], queued


# def handle_client(connection, config):
#     queued = False
#     executing = False
#     with connection:
#         while True:
#             data = connection.recv(1024)
#             if not data:
#                 break
#             print(f"Received data: {data}")

#             decoded_data = resp_parser(data)
#             _, queued = cmd_executor(decoded_data, connection, config, queued, executing)

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
