import socket
import threading
from app.handler import handle_client
from app.resp import resp_encoder, read_key_val_from_db
from time import sleep

class Master():
    def __init__(self, args):
        self.slaves = {}
        self.args = args
        self.config = {}
        self.config['role'] = 'master'
        self.config['master_replid'] = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
        self.config['master_replid_offset'] = '0'
        self.config['dir'] = self.args.dir
        self.config['dbfilename'] = self.args.dbfilename
        self.config['store'] = {}
        read_key_val_from_db(self.config['dir'], self.config['dbfilename'], self.config['store'])

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", int(self.args.port)))
        server_socket.listen(10)

        while True:
            client_socket, _ = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,self.config))
            client_thread.daemon = True
            client_thread.start()

class Slave():
    def __init__(self, args):
        self.args = args
        self.config = {}
        self.config['role'] = 'slave'
        master_host, master_port = self.args.replicaof.split(' ')
        self.config['master_host'] = master_host
        self.config['master_port'] = int(master_port)
        self.dir = self.args.dir
        self.dbfilename = self.args.dbfilename
        self.config['store'] = {}
        read_key_val_from_db(self.dir, self.dbfilename, self.config['store'])

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", int(self.args.port)))
        server_socket.listen(10)

        ############################## HANDSHAKE WITH MASTER ##############################
        # connect to master
        self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_socket.connect((self.config['master_host'], self.config['master_port']))
        self.master_socket.sendall(resp_encoder(["PING"]))

        # Read response from master
        data = self.master_socket.recv(1024)
        # assert data == "+PONG\r\n"
        # print(data)

        # REPLCONF
        self.master_socket.sendall(resp_encoder(["REPLCONF", "listening-port", str(self.args.port)]))
        data = self.master_socket.recv(1024)
        self.master_socket.sendall(resp_encoder(["REPLCONF", "capa", "psync2"]))
        data = self.master_socket.recv(1024)

        # PSYNC
        self.master_socket.sendall(resp_encoder(["PSYNC", '?', '-1']))
        data = self.master_socket.recv(1024)
        print(f"[Replica] PSYNC response: {data}")
        sleep(1)
        # RDB
        # self.master_socket.sendall(resp_encoder(["RDB"]))
        # data = self.master_socket.recv(1024)
        # print(f"[Replica] RDB response: {data}")
        threading.Thread(
            target=handle_client,
            args=(self.master_socket, self.config, data),
            daemon=True,
        ).start()

        while True:
            client_socket, _ = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,self.config))
            client_thread.daemon = True
            client_thread.start()