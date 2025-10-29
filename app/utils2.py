#####################    STREAM OPERATIONS     #####################
streams = {}
from app.resp import resp_encoder
import threading
import time


def checker(key, id_time, id_sequence):
    """Checks if the provided id_time and id_sequence are valid integers."""
    # id_time and id_sequence must be greater than 0-0
    if id_time == "0" and id_sequence == "0":
        return "ERR The ID specified in XADD must be greater than 0-0"

    last_entries = streams.get(key, [])
    if last_entries:
        last_id, last_sequence = last_entries[-1].get("id").split("-")
        if int(id_time) < int(last_id):
            return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        elif int(id_time) == int(last_id) and int(id_sequence) <= int(last_sequence):
            return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    return "ok"


def allot(key, id_time):
    """Allots a new ID based on the provided id_time."""
    last_entries = streams.get(key, [])
    if last_entries:
        last_id, last_sequence = last_entries[-1].get("id").split("-")
        if int(id_time) < int(last_id):
            new_time = int(last_id)
            new_sequence = int(last_sequence) + 1
        elif int(id_time) == int(last_id):
            new_time = int(last_id)
            new_sequence = int(last_sequence) + 1
        else:
            new_time = int(id_time)
            new_sequence = 0
    else:
        new_time = int(id_time)
        new_sequence = 0 if new_time > 0 else 1
    return f"{new_time}-{new_sequence}"


def xadd(info, blocked_xread):
    """Adds an entry to a stream."""
    key = info[0]
    entry_id = info[1]

    if entry_id != '*':
        id_time, id_sequence = entry_id.split("-")
        if id_sequence == '*':
            entry_id = allot(key, id_time)
        else:
            check = checker(key, id_time, id_sequence)
            if check != "ok":
                return "err", check
    else:
        # Allot ID as 'current unix time in milliseconds'-'0'
        current_millis = int(time.time() * 1000)
        entry_id = allot(key, str(current_millis))

    field_values = info[2:]
    if key not in streams:
        streams[key] = []

    entry = {"id": entry_id}
    for i in range(0, len(field_values), 2):
        field = field_values[i]
        value = field_values[i + 1]
        entry[field] = value

    streams[key].append(entry)

    # Unblock any blocked XREAD clients
    if key in blocked_xread and blocked_xread[key]:
        pending = blocked_xread[key][:]
        blocked_xread[key] = []
        for conn, last_id in pending:
            result = []
            for entry in streams[key]:
                if entry["id"] > last_id:
                    temp = [key]
                    entry_data = [entry["id"]]
                    for field, value in entry.items():
                        if field != "id":
                            entry_data.append([field, value])
                    temp.append([entry_data])
                    result.append(temp)
            if result:
                response = resp_encoder(result)
                try:
                    conn.sendall(response)
                except Exception:
                    pass

    return "id", entry_id  # Return the entry ID


def xrange(info):
    """Returns a range of entries from a stream."""
    key = info[0]
    start_id = info[1]
    end_id = info[2]

    if start_id == "-":
        start_id = "0-0"
    elif "-" not in start_id:
        start_id += "-0"

    if end_id == "+":
        end_id = "9999999999999-9999999999999"
    elif "-" not in end_id:
        end_id += "-9999999999999"

    if key not in streams:
        return []

    result = []
    for entry in streams[key]:
        entry_id = entry["id"]
        if start_id <= entry_id <= end_id:
            fields = []
            for field, value in entry.items():
                if field != "id":
                    fields.append([field, value])
            result.append([entry_id, fields])
    return result


def xread(info):
    """Reads entries from multiple streams starting from specified IDs."""
    mid = len(info) // 2
    keys = info[:mid]
    ids = info[mid:]

    # Normalize IDs
    ids = [id + "-0" if "-" not in id else id for id in ids]

    result = []
    for key, last_id in zip(keys, ids):
        if key not in streams:
            continue
        entries = []
        for entry in streams[key]:
            if entry["id"] > last_id:
                fields = []
                for field, value in entry.items():
                    if field != "id":
                        fields.append([field, value])
                entries.append([entry["id"], fields])
        if entries:
            result.append([key, entries])
    return result


def blocks_xread(info, connection, blocked_xread):
    """Blocks a connection until new stream entries are available or timeout."""
    # Handle BLOCK <ms> STREAMS <key> <id>
    try:
        timeout = float(info[0]) / 1000  # ms → s
        key = info[2]
        id = info[3]
    except Exception:
        return None

    if key not in streams:
        id = "0-0"

    if key not in blocked_xread:
        blocked_xread[key] = []
    blocked_xread[key].append((connection, id))

    def timeout_unblock():
        time.sleep(timeout)
        if key in blocked_xread:
            for conn, _ in blocked_xread[key][:]:
                if conn == connection:
                    try:
                        conn.sendall(b"*-1\r\n")
                    except Exception:
                        pass
                    blocked_xread[key].remove((conn, _))
                    if not blocked_xread[key]:
                        del blocked_xread[key]
                    break

    if timeout > 0:
        threading.Thread(target=timeout_unblock, daemon=True).start()

    return None


def type_getter_streams(key):
    """Returns the type of the value stored at key."""
    return "stream" if key in streams else "none"