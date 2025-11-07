"""
Redis Server Module

This module implements the TCP server that handles client connections
and processes Redis commands.
"""

import socket
import threading
from typing import Optional

from app.protocol.resp import parse_resp_array
from app.commands import CommandHandler
from app.store import DataStore


class RedisServer:
    """A simple Redis-compatible TCP server."""
    
    def __init__(self, host: str = "localhost", port: int = 6379):
        """
        Initialize the Redis server.
        
        Args:
            host: Host address to bind to
            port: Port number to listen on
        """
        self.host = host
        self.port = port
        self.datastore = DataStore()
        self.command_handler = CommandHandler(self.datastore)
        self.server_socket: Optional[socket.socket] = None
    
    def handle_client(self, client_socket: socket.socket, client_address: tuple):
        """
        Handle a client connection.
        
        Args:
            client_socket: The client's socket connection
            client_address: The client's address tuple (host, port)
        """
        print(f"Server: Client connected from {client_address}")
        
        try:
            buffer = b""
            
            while True:
                # Receive data from client
                data = client_socket.recv(4096)
                
                if not data:
                    # Client disconnected
                    break
                
                buffer += data
                
                # Process commands from buffer
                while buffer:
                    # Try to parse a complete command
                    parsed_command, bytes_consumed = parse_resp_array(buffer)
                    
                    if parsed_command is None:
                        # Incomplete command, wait for more data
                        break
                    
                    # Extract command and arguments
                    if len(parsed_command) == 0:
                        break
                    
                    command = parsed_command[0]
                    arguments = parsed_command[1:] if len(parsed_command) > 1 else []
                    
                    print(f"Server: Executing command: {command} {arguments}")
                    
                    # Execute command and get response
                    response = self.command_handler.execute(command, arguments)
                    
                    # Send response to client
                    client_socket.sendall(response)
                    
                    # Remove processed bytes from buffer
                    buffer = buffer[bytes_consumed:]
        
        except Exception as e:
            print(f"Server: Error handling client {client_address}: {e}")
        
        finally:
            print(f"Server: Client disconnected from {client_address}")
            client_socket.close()
    
    def start(self):
        """Start the Redis server and listen for connections."""
        try:
            # Create server socket
            self.server_socket = socket.create_server(
                (self.host, self.port),
                reuse_port=True
            )
            
            print(f"Server: Starting Redis server on {self.host}:{self.port}")
            print("Server: Listening for connections...")
            
            while True:
                # Accept incoming connections
                client_socket, client_address = self.server_socket.accept()
                
                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address),
                    daemon=True
                )
                client_thread.start()
        
        except KeyboardInterrupt:
            print("\nServer: Shutting down...")
        except OSError as e:
            print(f"Server: Error starting server: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()
