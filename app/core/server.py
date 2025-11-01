import socket
import threading
import sys

from app.protocol.constants import *
from command_execution import handle_connection
import command_execution as ce





def replica_command_listener(master_socket: socket.socket):
    while True:
        try:
            data = master_socket.recv(4096)
            if not data:
                print("Replication: Master closed connection.")
                break

            print(f"Replica: Received propagated data from master: {data!r}")

            buffer = data
            while buffer:
                parsed_command, bytes_consumed = ce.parsed_resp_array(buffer)

                if not parsed_command:
                    if buffer.startswith(b'+') or buffer.startswith(b'$'):
                        next_command_start = buffer.find(b'*')

                        if next_command_start != -1:
                            print(
                                f"Replica: Ignoring master handshake response/RDB payload ({next_command_start} bytes).")
                            buffer = buffer[next_command_start:]
                            continue
                        else:
                            print("Replica: Ignoring remaining master handshake response/RDB payload.")
                            break

                    print(f"Replica: Could not parse propagated command. Skipping remaining buffer: {buffer!r}")
                    break

                command = parsed_command[0].upper()
                arguments = parsed_command[1:]

                print(f"Command: Parsed command: {command}, Arguments: {arguments}")

                ce.handle_command(command, arguments, master_socket)
                ce.REPLICA_REPL_OFFSET += bytes_consumed

                buffer = buffer[bytes_consumed:]

        except Exception as e:
            print(f"Replication Listener Error: {e}")
            break


def read_simple_string_response(sock: socket.socket, expected: bytes):
    response = sock.recv(1024)
    if not response or not response.startswith(b"+"):
        print(f"Replication Error: Master sent unexpected response: {response!r}")
        return False

    if response.strip() == expected.strip():
        print(f"Replication: Received expected response: {response!r}")
        return True

    print(f"Replication Error: Received response {response!r} did not match expected {expected!r}")
    return False


def connect_to_master(listening_port: int) -> socket.socket | None:
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

        print("Replication: Handshake steps 1, 2, & 3 complete (PSYNC sent).")

        ce.MASTER_SOCKET = master_socket

        return master_socket

    except Exception as e:
        print(f"Replication Error: Could not connect to master or send PING: {e}")


def main():
    port = 6379
    args = sys.argv[1:]

    is_replica = False
    master_host = None
    master_port = None

    i = 0
    while i < len(args):
        arg = args[i]

        if arg == "--port":
            if i + 1 >= len(args):
                print("Server Error: Missing port number after --port.")
                return
            try:
                port = int(args[i + 1])
                i += 2
            except ValueError:
                print("Server Error: Port value is not an integer.")
                return

        elif arg == "--replicaof":
            if i + 1 >= len(args):
                print("Server Error: Missing argument after --replicaof.")
                return

            replicaof_value = args[i + 1]
            parts = replicaof_value.split()

            if len(parts) != 2:
                print("Server Error: --replicaof value must be 'host port'.")
                return

            try:
                master_host = parts[0]
                master_port = int(parts[1])
                is_replica = True
                i += 2
            except ValueError:
                print("Server Error: Master port value is not an integer.")
                return

        elif arg == "--dir" or arg == "--dbfilename":
            if i + 1 >= len(args):
                print(f"Server Error: Missing value for {arg}.")
                return
            i += 2

        else:
            i += 1
    master_socket = None
    if is_replica:
        ce.SERVER_ROLE = "slave"
        ce.MASTER_HOST = master_host
        ce.MASTER_PORT = master_port

        master_socket = connect_to_master(port)

    if master_socket:
        threading.Thread(target=replica_command_listener, args=(master_socket,), daemon=True).start()

    try:
        server_socket = socket.create_server(("localhost", port), reuse_port=True)
        print(f"Server: Starting server on localhost:{port}...")
        print("Server: Listening for connections...")
    except OSError as e:
        print(f"Server Error: Could not start server: {e}")
        return

    while True:
        try:
            connection, client_address = server_socket.accept()
            threading.Thread(target=handle_connection, args=(connection, client_address)).start()
        except Exception as e:
            print(f"Server Error: Exception during connection acceptance or thread creation: {e}")
            break


if __name__ == "__main__":
    main()