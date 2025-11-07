"""
Redix - A Full-Featured Redis Implementation in Python

This is a comprehensive Redis-compatible server implementation supporting:
- Basic commands (PING, ECHO, SET, GET)
- Lists (LPUSH, RPUSH, LPOP, LRANGE, LLEN, BLPOP)
- Streams (XADD, XRANGE, XREAD)
- Sorted Sets (ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM)
- Transactions (MULTI, EXEC, DISCARD, INCR)
- Pub/Sub (SUBSCRIBE, PUBLISH, UNSUBSCRIBE)
- Replication (Master-Slave with REPLCONF, PSYNC, INFO, WAIT)
- Geospatial commands (GEOADD, GEOPOS, GEODIST, GEOSEARCH)
- Additional commands (TYPE, CONFIG, KEYS)

Usage:
    python -m app.main [--port PORT] [--replicaof HOST PORT] [--dir PATH] [--dbfilename NAME]
    
Examples:
    python -m app.main
    python -m app.main --port 6380
    python -m app.main --port 6380 --replicaof localhost 6379
"""

from app.core.server import main

if __name__ == "__main__":
    main()
