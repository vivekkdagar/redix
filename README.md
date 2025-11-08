# Redix - Full-Featured Redis Implementation in Python

A comprehensive Redis-compatible server implementation in Python with support for all major Redis features including Lists, Streams, Sorted Sets, Transactions, Pub/Sub, Replication, and Geospatial commands.

## Features

### Basic Commands
- **PING** - Test server connectivity
- **ECHO** - Echo the given string
- **SET** - Set a key to a string value with optional expiration (EX/PX)
- **GET** - Get the value of a key
- **TYPE** - Determine the type of value stored at a key
- **CONFIG** - Get configuration parameters
- **KEYS** - Find all keys matching a pattern

### Lists
- **LPUSH** - Prepend one or multiple elements to a list
- **RPUSH** - Append one or multiple elements to a list
- **LPOP** - Remove and return the first element of a list
- **LRANGE** - Get a range of elements from a list
- **LLEN** - Get the length of a list
- **BLPOP** - Blocking list pop with timeout

### Streams
- **XADD** - Append a new entry to a stream
- **XRANGE** - Query a range of entries from a stream
- **XREAD** - Read entries from one or multiple streams
- Support for auto-generated and partially auto-generated IDs
- Blocking reads with and without timeout

### Sorted Sets
- **ZADD** - Add members with scores to a sorted set
- **ZRANK** - Get the rank of a member in a sorted set
- **ZRANGE** - Get a range of members from a sorted set
- **ZCARD** - Get the number of members in a sorted set
- **ZSCORE** - Get the score of a member in a sorted set
- **ZREM** - Remove members from a sorted set
- Support for negative indexes

### Transactions
- **MULTI** - Start a transaction
- **EXEC** - Execute all commands in a transaction
- **DISCARD** - Discard all commands in a transaction
- **INCR** - Increment the integer value of a key
- Queue commands during transaction
- Handle failures within transactions

### Pub/Sub
- **SUBSCRIBE** - Subscribe to one or more channels
- **UNSUBSCRIBE** - Unsubscribe from channels
- **PUBLISH** - Publish a message to a channel
- Enter subscribed mode
- Deliver messages to subscribers

### Replication
- **Master-Slave Replication** - Full master-slave setup
- **REPLCONF** - Configure replication parameters
- **PSYNC** - Synchronize with master server
- **INFO** - Get server information and replication status
- **WAIT** - Wait for replication acknowledgments
- Empty RDB transfer
- Single and multi-replica propagation
- Command processing and ACKs

### Geospatial Commands
- **GEOADD** - Add geospatial items with coordinates
- **GEOPOS** - Get coordinates of geospatial items
- **GEODIST** - Calculate distance between two geospatial items
- **GEOSEARCH** - Search for items within a radius
- Coordinate validation
- Haversine distance calculation
- Morton encoding for location scores

### Additional Features
- ✅ **RESP Protocol** - Full Redis Serialization Protocol support
- ✅ **TCP Server** - Multi-threaded TCP server handling concurrent clients
- ✅ **Key Expiration** - TTL support with lazy deletion
- ✅ **Thread-Safe** - Concurrent client handling with proper locking
- ✅ **RDB Persistence** - Read RDB files and load data
- ✅ **Blocking Operations** - Support for blocking list and stream operations

## Architecture

```
app/
├── main.py                      # Entry point
├── core/
│   ├── server.py                # TCP server and replication logic
│   ├── command_execution.py     # All command handlers
│   ├── context.py               # Server context and global state
│   └── datastore.py             # Complete data store implementation
│       ├── String storage with expiration
│       ├── List operations
│       ├── Stream operations
│       ├── Sorted set operations
│       ├── Transaction support
│       ├── Pub/Sub support
│       ├── RDB file loading
│       └── Blocking operations
├── protocol/
│   ├── resp.py                  # RESP protocol parser (new modular)
│   └── constants.py             # Protocol constants
├── replication/
│   ├── slave.py                 # Slave replication logic
│   ├── listener.py              # Replication listener
│   └── utils.py                 # Replication utilities
├── config.py                    # Server configuration
└── parser.py                    # Command parser
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

Or with python directly:
```bash
python3 -m app.main
```

Run on a custom port:
```bash
python3 -m app.main --port 6380
```

Run as a replica:
```bash
python3 -m app.main --port 6380 --replicaof localhost 6379
```

With RDB file:
```bash
python3 -m app.main --dir /path/to/dir --dbfilename dump.rdb
```

### Connecting with redis-cli

Once the server is running, connect using the official Redis CLI:

```bash
redis-cli -p 6379
```

### Example Commands

```bash
# Basic operations
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET mykey "Hello World"
OK
127.0.0.1:6379> GET mykey
"Hello World"
127.0.0.1:6379> TYPE mykey
string

# Lists
127.0.0.1:6379> LPUSH mylist "world"
(integer) 1
127.0.0.1:6379> LPUSH mylist "hello"
(integer) 2
127.0.0.1:6379> LRANGE mylist 0 -1
1) "hello"
2) "world"

# Sorted Sets
127.0.0.1:6379> ZADD myzset 1 "one"
(integer) 1
127.0.0.1:6379> ZADD myzset 2 "two"
(integer) 1
127.0.0.1:6379> ZRANGE myzset 0 -1
1) "one"
2) "two"

# Streams
127.0.0.1:6379> XADD mystream * field1 value1
"1234567890-0"
127.0.0.1:6379> XRANGE mystream - +
1) 1) "1234567890-0"
   2) 1) "field1"
      2) "value1"

# Transactions
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> SET key1 "value1"
QUEUED
127.0.0.1:6379> SET key2 "value2"
QUEUED
127.0.0.1:6379> EXEC
1) OK
2) OK

# Pub/Sub
# In one client:
127.0.0.1:6379> SUBSCRIBE mychannel
# In another client:
127.0.0.1:6379> PUBLISH mychannel "Hello"
(integer) 1

# Geospatial
127.0.0.1:6379> GEOADD locations 13.361389 38.115556 "Palermo"
(integer) 1
127.0.0.1:6379> GEOPOS locations "Palermo"
1) 1) "13.361389"
   2) "38.115556"
```

## Technical Details

### RESP Protocol

Redix implements the full Redis Serialization Protocol (RESP):

- **Simple Strings**: `+OK\r\n`
- **Errors**: `-Error message\r\n`
- **Integers**: `:1000\r\n`
- **Bulk Strings**: `$6\r\nfoobar\r\n`
- **Arrays**: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
- **Null**: `$-1\r\n`

### Data Store

The comprehensive data store (`app/core/datastore.py`) provides:
- Thread-safe operations using locks
- Multiple data type support (strings, lists, streams, sorted sets)
- Expiration timestamp tracking
- Transaction support with MULTI/EXEC
- Pub/Sub message routing
- Blocking operations with timeouts
- RDB file loading

### Concurrency

- Each client connection is handled in a separate thread
- Thread-safe data store operations with proper locking
- Blocking operations use condition variables
- Non-blocking accept loop for new connections

### Replication

Full master-slave replication support:
- Handshake process (PING, REPLCONF, PSYNC)
- RDB snapshot transfer
- Command propagation to replicas
- WAIT command for synchronization
- Offset tracking and acknowledgments

## Command Reference

### Complete Command List

**Basic**: PING, ECHO, SET, GET, TYPE, CONFIG, KEYS

**Lists**: LPUSH, RPUSH, LPOP, LRANGE, LLEN, BLPOP

**Streams**: XADD, XRANGE, XREAD (with blocking support)

**Sorted Sets**: ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM

**Transactions**: MULTI, EXEC, DISCARD, INCR

**Pub/Sub**: SUBSCRIBE, UNSUBSCRIBE, PUBLISH

**Replication**: REPLCONF, PSYNC, INFO, WAIT

**Geospatial**: GEOADD, GEOPOS, GEODIST, GEOSEARCH

## Development

### Code Organization

The codebase is organized with clear separation:

- **server**: TCP connection handling and replication
- **command_execution**: Centralized command processing
- **data_store**: Complete data storage implementation
- **protocol**: RESP protocol implementation
- **replication**: Master-slave replication logic

### Code Quality

- Comprehensive inline comments explaining complex logic
- Documented functions with clear purpose
- Modular helper functions for reusable code
- Professional error handling

## Testing

The implementation has been tested with:
- Basic command operations
- List operations with blocking
- Stream operations with auto-generated IDs
- Sorted set operations with ranks
- Transaction atomicity
- Pub/Sub message delivery
- Replication synchronization
- Geospatial distance calculations

## License

This project is part of the CodeCrafters Redis challenge.

## Contributing

This is an educational project demonstrating a full Redis implementation.

## Acknowledgments

- Built as part of the [CodeCrafters](https://codecrafters.io) "Build Your Own Redis" challenge
- Implements features from the official [Redis](https://redis.io) specification

## Resources

- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [Redis Commands Documentation](https://redis.io/commands/)
- [CodeCrafters Redis Challenge](https://codecrafters.io/challenges/redis)
- [RDB File Format](https://rdb.fnordig.de/file_format.html)
