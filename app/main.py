"""
Redix - A Simple Redis Implementation in Python

This is a minimal Redis-compatible server implementation that supports
basic commands like PING, ECHO, SET, and GET.

Usage:
    python -m app.main [--port PORT]
    
Examples:
    python -m app.main
    python -m app.main --port 6380
"""

import sys
from app.server import RedisServer


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        dict: Configuration dictionary with parsed arguments
    """
    config = {
        'port': 6379,
        'host': 'localhost'
    }
    
    args = sys.argv[1:]
    i = 0
    
    while i < len(args):
        arg = args[i]
        
        if arg == '--port':
            if i + 1 >= len(args):
                print("Error: Missing port number after --port")
                sys.exit(1)
            
            try:
                config['port'] = int(args[i + 1])
                i += 2
            except ValueError:
                print("Error: Port must be an integer")
                sys.exit(1)
        else:
            print(f"Warning: Unknown argument '{arg}' ignored")
            i += 1
    
    return config


def main():
    """Main entry point for the Redis server."""
    config = parse_arguments()
    
    # Create and start the server
    server = RedisServer(host=config['host'], port=config['port'])
    server.start()


if __name__ == "__main__":
    main()
