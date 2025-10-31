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
def cmd_executor(args: Args, value: List, conn: socket.socket, is_replica_conn: bool = False) -> bytes | str | None | List[Any]:
    """
    Execute a parsed Redis command and return a RESP-encoded response or 'custom' if manually handled.
    """
    global db, sorted_set_dict, transaction_enabled, transactions
    response = "custom"

    if not value:
        return "-ERR empty command"

    cmd = value[0].upper()

    # --- Basic Commands ---
    if cmd == b"PING":
        response = "PONG"

    elif cmd == b"ECHO" and len(value) == 2:
        response = value[1]

    elif cmd == b"GET" and len(value) == 2:
        k = value[1]
        if not queue_transaction(value, conn):
            now = datetime.datetime.now()
            db_value = db.get(k)
            if db_value is None:
                response = None
            elif db_value.expiry and now >= db_value.expiry:
                db.pop(k)
                response = None
            else:
                response = db_value.value

    elif cmd == b"SET":
        if len(value) == 3:  # SET key value
            k, v = value[1], value[2]
            if not queue_transaction(value, conn):
                db[k] = rdb_parser.Value(value=v, expiry=None)
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                response = "OK"

        elif len(value) == 5 and value[3].lower() == b"px":  # SET key value px expiry_ms
            k, v, expiry_ms = value[1], value[2], int(value[4].decode())
            if not queue_transaction(value, conn):
                now = datetime.datetime.now()
                expiry_time = now + datetime.timedelta(milliseconds=expiry_ms)
                db[k] = rdb_parser.Value(value=v, expiry=expiry_time)
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                response = "OK"

    elif cmd == b"INCR" and len(value) == 2:
        k = value[1]
        if not queue_transaction(value, conn):
            db_value = db.get(k)
            if db_value is None:
                new_val = 1
            else:
                try:
                    new_val = int(db_value.value.decode()) + 1
                except Exception:
                    return "-ERR value is not an integer or out of range"
            db[k] = rdb_parser.Value(value=str(new_val).encode(), expiry=None)
            response = new_val

    # --- Sorted Sets (ZADD) ---
    elif cmd == b"ZADD" and len(value) == 4:
        zset_key, score_bytes, member = value[1], value[2], value[3]
        score = float(score_bytes.decode())

        if zset_key not in sorted_set_dict:
            sorted_set_dict[zset_key] = []

        # Check if member already exists
        existing = [item for item in sorted_set_dict[zset_key] if item[1] == member]
        if existing:
            # Update existing score
            sorted_set_dict[zset_key].remove(existing[0])
            heapq.heappush(sorted_set_dict[zset_key], [score, member])
            response = 0
        else:
            heapq.heappush(sorted_set_dict[zset_key], [score, member])
            response = 1

    # --- LIST / TRANSACTION / STREAM commands (delegated to existing handler) ---
    else:
        try:
            response = handle_command(args, value, conn, is_replica_conn)
        except Exception as e:
            print(f"Error executing {value}: {e}")
            response = f"-ERR {str(e)}"

    # Encode and return
    if response == "custom":
        return "custom"
    return encode_resp(response)

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