#!/usr/bin/env python3
import socket
import threading
import time
import argparse

# ------------------- GLOBALS -------------------
dictionary = {}
list_dict = {}
streams = {}
connections = {}
subscriptions = {}

replica_of = None
replicas = []
replica_offsets = {}
master_repl_offset = 0
replica_offset = 0
write_commands = {"SET", "DEL", "INCR", "DECR", "RPUSH", "LPUSH", "LPOP", "XADD"}

master_connection_socket = None
RDB_HEADER = b'\x52\x45\x44\x49\x53\x30\x30\x31\x31'  # "REDIS0011"

dir = None
dbfilename = None

# Track which clients are in subscribed mode
subscribed_clients = set()

# ------------------- RESP PARSING -------------------
def parse_bulk_string(buffer, s_len):
    if len(buffer) < s_len + 2:
        return None, buffer, 0
    try:
        bulk_str = buffer[:s_len].decode('utf-8')
        remaining_buffer = buffer[s_len + 2:]
        return bulk_str, remaining_buffer, s_len + 2
    except UnicodeDecodeError:
        remaining_buffer = buffer[s_len + 2:]
        return None, remaining_buffer, s_len + 2

def parse_array(buffer, num_args):
    elements = []
    current_buffer = buffer
    total_bytes = 0
    for _ in range(num_args):
        element, current_buffer, bytes_processed = parse_stream(current_buffer)
        if element is None and bytes_processed == 0:
            return None, buffer, 0
        total_bytes += bytes_processed
        if element is not None:
            elements.append(element)
    if not elements:
        return None, current_buffer, total_bytes
    return elements, current_buffer, total_bytes

def parse_stream(buffer):
    if not buffer:
        return None, buffer, 0
    crlf_pos = buffer.find(b'\r\n')
    if crlf_pos == -1:
        return None, buffer, 0
    header = buffer[:crlf_pos].decode()
    remaining_buffer = buffer[crlf_pos + 2:]
    cmd_type = header[0]
    header_bytes = len(header) + 2
    if cmd_type == '*':
        num_args = int(header[1:])
        result, remaining_buffer, bytes_processed = parse_array(remaining_buffer, num_args)
        return result, remaining_buffer, header_bytes + bytes_processed
    elif cmd_type == '$':
        s_len = int(header[1:])
        if s_len >= 0:
            result, remaining_buffer, bytes_processed = parse_bulk_string(remaining_buffer, s_len)
            return result, remaining_buffer, header_bytes + bytes_processed
        else:
            return None, remaining_buffer, header_bytes
    else:
        return None, remaining_buffer, header_bytes

def parse_commands(buffer):
    commands = []
    current_buffer = buffer
    while current_buffer:
        command, current_buffer, bytes_processed = parse_stream(current_buffer)
        if bytes_processed == 0:
            break
        if command is not None and isinstance(command, list) and len(command) > 0:
            commands.append((command, bytes_processed))
    return commands, current_buffer, 0

# ------------------- COMMAND HANDLERS -------------------
def handle_ping(connection):
    if connection == master_connection_socket:
        return None
    return connection.sendall(b"+PONG\r\n")

def handle_echo(connection, message):
    response = f"${len(message)}\r\n{message}\r\n"
    return connection.sendall(response.encode())

def handle_set(connection, args):
    key, value = args[0], args[1]
    dictionary[key] = value
    if len(args) > 2 and args[2].upper() == "PX":
        try:
            expire_time = int(args[3]) / 1000.0
            threading.Timer(expire_time, lambda: dictionary.pop(key, None)).start()
        except (ValueError, IndexError):
            return connection.sendall(b"-ERR invalid PX value\r\n")
    if connection not in replicas:
        return connection.sendall(b"+OK\r\n")

def handle_get(connection, key):
    if key in dictionary:
        value = dictionary[key]
        return connection.sendall(f"${len(value)}\r\n{value}\r\n".encode())
    else:
        return connection.sendall(b"$-1\r\n")

def handle_rpush(connection, key, values):
    if key not in list_dict:
        list_dict[key] = []
    for value in values:
        list_dict[key].append(value)
    return connection.sendall(f":{len(list_dict[key])}\r\n".encode())

def handle_lpush(connection, key, values):
    if key not in list_dict:
        list_dict[key] = []
    for value in values:
        list_dict[key].insert(0, value)
    return connection.sendall(f":{len(list_dict[key])}\r\n".encode())

def handle_llen(connection, key):
    return connection.sendall(f":{len(list_dict.get(key, []))}\r\n".encode())

def handle_lpop(connection, args):
    key = args[0]
    if key not in list_dict or not list_dict[key]:
        return connection.sendall(b"$-1\r\n")
    count = int(args[1]) if len(args) > 1 else 1
    popped = [list_dict[key].pop(0) for _ in range(min(count, len(list_dict[key])))]
    if count == 1:
        return connection.sendall(f"${len(popped[0])}\r\n{popped[0]}\r\n".encode())
    resp = f"*{len(popped)}\r\n" + "".join(f"${len(v)}\r\n{v}\r\n" for v in popped)
    return connection.sendall(resp.encode())

def handle_blpop(connection, key, timeout):
    timeout = float(timeout)
    start = time.time()
    while True:
        if key in list_dict and list_dict[key]:
            val = list_dict[key].pop(0)
            resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
            return connection.sendall(resp.encode())
        if timeout > 0 and time.time() - start > timeout:
            return connection.sendall(b"*-1\r\n")
        time.sleep(0.05)

def handle_subscribe(connection, channel):
    # Mark this connection as subscribed
    subscribed_clients.add(connection)
    if connection not in subscriptions:
        subscriptions[connection] = set()
    subscriptions[connection].add(channel)
    response = f"*3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{len(subscriptions[connection])}\r\n"
    connection.sendall(response.encode())
    # Keep client alive (subscribed mode)
    return

# ------------------- CORE EXECUTION -------------------
ALLOWED_IN_SUBSCRIBED_MODE = {"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"}

def execute_command(connection, command):
    cmd = command[0].upper()

    # If in subscribed mode, restrict commands
    if connection in subscribed_clients and cmd not in ALLOWED_IN_SUBSCRIBED_MODE:
        error = f"-ERR Can't execute '{cmd.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT allowed in this context\r\n"
        connection.sendall(error.encode())
        return

    if cmd == "PING": handle_ping(connection)
    elif cmd == "ECHO" and len(command) == 2: handle_echo(connection, command[1])
    elif cmd == "SET" and len(command) >= 3: handle_set(connection, command[1:])
    elif cmd == "GET" and len(command) == 2: handle_get(connection, command[1])
    elif cmd == "RPUSH": handle_rpush(connection, command[1], command[2:])
    elif cmd == "LPUSH": handle_lpush(connection, command[1], command[2:])
    elif cmd == "LLEN": handle_llen(connection, command[1])
    elif cmd == "LPOP": handle_lpop(connection, command[1:])
    elif cmd == "BLPOP": handle_blpop(connection, command[1], command[2])
    elif cmd == "SUBSCRIBE" and len(command) == 2: handle_subscribe(connection, command[1])
    else: connection.sendall(b"-ERR unknown command\r\n")

# ------------------- NETWORK HANDLER -------------------
def send_response(connection, initial_buffer=b""):
    buffer = initial_buffer
    try:
        while True:
            if not buffer:
                data = connection.recv(1024)
                if not data: break
                buffer += data
            commands, remaining_buffer, _ = parse_commands(buffer)
            if not commands:
                data = connection.recv(1024)
                if not data: break
                buffer += data
                continue
            for command, _ in commands:
                execute_command(connection, command)
            buffer = remaining_buffer
    except Exception as e:
        print("Error:", e)
    finally:
        connection.close()

# ------------------- RDB PARSER -------------------
class RDBParser:
    HEADER_MAGIC = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31"
    META_START = 0xFA
    EOF = 0xFF
    DB_START = 0xFE
    HASH_START = 0xFB
    EXPIRE_TIME = 0xFD
    EXPIRE_TIME_MS = 0xFC

    def __init__(self, path):
        try:
            with open(path, 'rb') as f:
                self.content = f.read()
        except FileNotFoundError:
            self.content = None
        self.pointer = 0

    def _read(self, n):
        d = self.content[self.pointer:self.pointer + n]
        self.pointer += n
        return d

    def _read_byte(self):
        return self._read(1)[0]

    def _read_length(self):
        first = self._read_byte()
        t = (first & 0xC0) >> 6
        if t == 0b00: return first & 0x3F
        elif t == 0b01: return ((first & 0x3F) << 8) | self._read_byte()
        elif t == 0b10: return int.from_bytes(self._read(4), 'big')
        else: return first

    def _read_string(self):
        length = self._read_length()
        return self._read(length).decode()

    def parse(self):
        if not self.content: return {}
        self._read(len(self.HEADER_MAGIC))
        data = {}
        expiry = None
        while self.pointer < len(self.content):
            opcode = self._read_byte()
            if opcode == self.META_START:
                self._read_string(); self._read_string()
            elif opcode == self.DB_START:
                self._read_length()
            elif opcode == self.HASH_START:
                self._read_length(); self._read_length()
            elif opcode == self.EXPIRE_TIME:
                expiry = int.from_bytes(self._read(4), 'little') * 1000
                continue
            elif opcode == self.EXPIRE_TIME_MS:
                expiry = int.from_bytes(self._read(8), 'little')
                continue
            elif opcode == self.EOF:
                break
            else:
                key = self._read_string()
                val = self._read_string()
                if expiry is None or expiry > int(time.time() * 1000):
                    data[key] = val
                expiry = None
        print(f"RDB parsed {len(data)} keys.")
        return data

# ------------------- MAIN -------------------
def main(args):
    global dir, dbfilename, dictionary
    print("Server started!")
    if args.dir and args.dbfilename:
        rdb = RDBParser(f"{args.dir}/{args.dbfilename}")
        dictionary = rdb.parse()
    server = socket.create_server(("localhost", int(args.port)), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=send_response, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--dir", type=str)
    parser.add_argument("--dbfilename", type=str)
    args = parser.parse_args()
    main(args)