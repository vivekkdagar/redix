"""
Command Handlers Module

This module contains handlers for all supported Redis commands.
"""

from typing import List, Optional
from app.store import DataStore
from app.protocol.resp import (
    encode_simple_string,
    encode_bulk_string,
    encode_null_bulk_string,
    encode_error
)


class CommandHandler:
    """Handles execution of Redis commands."""
    
    def __init__(self, datastore: DataStore):
        """
        Initialize command handler with a data store.
        
        Args:
            datastore: The data store to use for command execution
        """
        self.datastore = datastore
        
        # Map command names to handler methods
        self.commands = {
            'PING': self.handle_ping,
            'ECHO': self.handle_echo,
            'SET': self.handle_set,
            'GET': self.handle_get,
        }
    
    def execute(self, command: str, arguments: List[str]) -> bytes:
        """
        Execute a command with given arguments.
        
        Args:
            command: The command name (e.g., 'PING', 'SET')
            arguments: List of command arguments
            
        Returns:
            RESP-encoded response bytes
        """
        command_upper = command.upper()
        
        if command_upper not in self.commands:
            return encode_error(f"ERR unknown command '{command}'")
        
        handler = self.commands[command_upper]
        return handler(arguments)
    
    def handle_ping(self, arguments: List[str]) -> bytes:
        """
        Handle PING command.
        
        Args:
            arguments: Command arguments (unused for PING)
            
        Returns:
            RESP-encoded PONG response
        """
        return encode_simple_string("PONG")
    
    def handle_echo(self, arguments: List[str]) -> bytes:
        """
        Handle ECHO command.
        
        Args:
            arguments: Command arguments (expects one argument to echo)
            
        Returns:
            RESP-encoded echoed message or error
        """
        if len(arguments) != 1:
            return encode_error("ERR wrong number of arguments for 'echo' command")
        
        return encode_bulk_string(arguments[0])
    
    def handle_set(self, arguments: List[str]) -> bytes:
        """
        Handle SET command.
        
        Supports: SET key value [EX seconds] [PX milliseconds]
        
        Args:
            arguments: Command arguments [key, value, optional_flags...]
            
        Returns:
            RESP-encoded OK response or error
        """
        if len(arguments) < 2:
            return encode_error("ERR wrong number of arguments for 'set' command")
        
        key = arguments[0]
        value = arguments[1]
        expiry_ms = None
        
        # Parse optional EX or PX flags
        i = 2
        while i < len(arguments):
            option = arguments[i].upper()
            
            if option in ('EX', 'PX'):
                if i + 1 >= len(arguments):
                    return encode_error("ERR syntax error")
                
                try:
                    duration = int(arguments[i + 1])
                    
                    if option == 'EX':
                        expiry_ms = duration * 1000  # Convert seconds to milliseconds
                    elif option == 'PX':
                        expiry_ms = duration
                    
                    i += 2
                    break  # Only support one expiration option
                    
                except ValueError:
                    return encode_error("ERR value is not an integer or out of range")
            else:
                return encode_error("ERR syntax error")
        
        # Store the value with optional expiration
        self.datastore.set(key, value, expiry_ms)
        return encode_simple_string("OK")
    
    def handle_get(self, arguments: List[str]) -> bytes:
        """
        Handle GET command.
        
        Args:
            arguments: Command arguments (expects one key)
            
        Returns:
            RESP-encoded value or null bulk string if key doesn't exist
        """
        if len(arguments) != 1:
            return encode_error("ERR wrong number of arguments for 'get' command")
        
        key = arguments[0]
        value = self.datastore.get(key)
        
        if value is None:
            return encode_null_bulk_string()
        
        return encode_bulk_string(value)
