# Redix - A Simple Redis Implementation in Python

A minimal, well-structured Redis-compatible server implementation in Python that supports basic Redis commands.

## Features

Redix implements the core Redis functionality with a focus on simplicity and clean architecture:

### Supported Commands

- **PING** - Test server connectivity
- **ECHO** - Echo the given string
- **SET** - Set a key to a string value with optional expiration
  - `SET key value [EX seconds] [PX milliseconds]`
- **GET** - Get the value of a key

### Key Features

- ✅ **RESP Protocol** - Full Redis Serialization Protocol (RESP) support
- ✅ **TCP Server** - Multi-threaded TCP server handling concurrent clients
- ✅ **Key Expiration** - TTL support with lazy deletion
- ✅ **Thread-Safe** - Concurrent client handling with proper locking
- ✅ **Modular Design** - Clean separation of concerns with professional architecture

## Architecture

```
app/
├── main.py                 # Entry point and CLI argument parsing
├── server/                 # TCP server implementation
│   ├── __init__.py
│   └── redis_server.py     # RedisServer class
├── commands/               # Command handlers
│   ├── __init__.py
│   └── handlers.py         # CommandHandler class
├── store/                  # Data storage layer
│   ├── __init__.py
│   └── datastore.py        # DataStore class with expiration
└── protocol/               # RESP protocol implementation
    ├── __init__.py
    └── resp.py             # RESP parser and encoder
```

## Installation

### Prerequisites

- Python 3.8 or higher
- pipenv (optional, for dependency management)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/alwaysvivek/redix.git
cd redix
```

2. Install dependencies (if using pipenv):
```bash
pipenv install
```

## Usage

### Starting the Server

Run with default port (6379):
```bash
./your_program.sh
```

Or with pipenv:
```bash
pipenv run python3 -m app.main
```

Run on a custom port:
```bash
pipenv run python3 -m app.main --port 6380
```

### Connecting with redis-cli

Once the server is running, connect using the official Redis CLI:

```bash
redis-cli -p 6379
```

### Example Commands

```bash
# Test connectivity
127.0.0.1:6379> PING
PONG

# Echo a message
127.0.0.1:6379> ECHO "Hello, Redis!"
"Hello, Redis!"

# Set a key-value pair
127.0.0.1:6379> SET mykey "Hello World"
OK

# Get a value
127.0.0.1:6379> GET mykey
"Hello World"

# Set with expiration (5 seconds)
127.0.0.1:6379> SET tempkey "expires soon" EX 5
OK

# Get before expiration
127.0.0.1:6379> GET tempkey
"expires soon"

# Wait 5 seconds and try again
127.0.0.1:6379> GET tempkey
(nil)
```

## Technical Details

### RESP Protocol

Redix implements the Redis Serialization Protocol (RESP) for client-server communication:

- **Simple Strings**: `+OK\r\n`
- **Errors**: `-Error message\r\n`
- **Integers**: `:1000\r\n`
- **Bulk Strings**: `$6\r\nfoobar\r\n`
- **Arrays**: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

### Data Store

The `DataStore` class provides:
- Thread-safe operations using locks
- In-memory key-value storage
- Expiration timestamp tracking (milliseconds)
- Lazy deletion of expired keys on access

### Concurrency

- Each client connection is handled in a separate thread
- Thread-safe data store operations with proper locking
- Non-blocking accept loop for new connections

## Development

### Project Structure

The codebase is organized into logical modules:

- **server**: Handles TCP connections and client communication
- **commands**: Implements command parsing and execution
- **store**: Manages data storage with expiration
- **protocol**: RESP protocol encoding/decoding

### Code Quality

- Comprehensive docstrings for all modules and functions
- Type hints for better code clarity
- Clean separation of concerns
- Professional naming conventions

## Limitations

This is a simplified Redis implementation focused on core functionality. It does not include:

- Persistence (RDB/AOF)
- Replication (Master-Slave)
- Pub/Sub messaging
- Transactions (MULTI/EXEC)
- Complex data types (Lists, Sets, Sorted Sets, Hashes, Streams)
- Geospatial commands
- Authentication
- Cluster mode
- Advanced key management (SCAN, KEYS, etc.)

## License

This project is part of the CodeCrafters Redis challenge.

## Contributing

This is an educational project. Feel free to fork and experiment!

## Acknowledgments

- Built as part of the [CodeCrafters](https://codecrafters.io) "Build Your Own Redis" challenge
- Inspired by the [Redis](https://redis.io) project

## Resources

- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [Redis Commands Documentation](https://redis.io/commands/)
- [CodeCrafters Redis Challenge](https://codecrafters.io/challenges/redis)
