import socket

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