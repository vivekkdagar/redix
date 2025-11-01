import socket
from app.replication.utils import read_simple_string_response
from app import command_execution as ce

PING_COMMAND_RESP = b"*1\r\n$4\r\nPING\r\n"
REPLCONF_CAPA_PSYNC2 = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
PSYNC_COMMAND_RESP = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"

def connect_to_master(listening_port: int):
    master_host = ce.MASTER_HOST
    master_port = ce.MASTER_PORT
    master_socket = None

    if not master_host or not master_port:
        print("Replication Error: Master host or port not configured for replica.")
        return

    print(f"Replication: Connecting to master at {master_host}:{master_port}...")

    try:
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((master_host, master_port))

        master_socket.sendall(PING_COMMAND_RESP)
        if not read_simple_string_response(master_socket, b"+PONG\r\n"):
            return

        port_str = str(listening_port)
        replconf_listening_port = (
            b"*3\r\n" +
            b"$8\r\nREPLCONF\r\n" +
            b"$14\r\nlistening-port\r\n" +
            b"$" + str(len(port_str)).encode() + b"\r\n" +
            port_str.encode() + b"\r\n"
        )
        master_socket.sendall(replconf_listening_port)
        if not read_simple_string_response(master_socket, b"+OK\r\n"):
            return

        master_socket.sendall(REPLCONF_CAPA_PSYNC2)
        if not read_simple_string_response(master_socket, b"+OK\r\n"):
            return

        print("Replication: Sending PSYNC ? -1...")
        master_socket.sendall(PSYNC_COMMAND_RESP)
        print("Replication: Handshake complete (PSYNC sent).")

        ce.MASTER_SOCKET = master_socket
        return master_socket

    except Exception as e:
        print(f"Replication Error: Could not connect to master or send PING: {e}")