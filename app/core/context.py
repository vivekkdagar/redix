from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import socket
import threading
import datetime


def log(*args):
    """Lightweight internal logger."""
    print("[Context]", *args)


@dataclass
class RedisContext:
    """
    Shared runtime state for the Redis clone.
    Replaces scattered globals like MASTER_HOST, SERVER_ROLE, etc.
    """

    # --- Core configuration ---
    config: Any  # This will be an instance of Config (from config.py)

    # --- Server Role and Connections ---
    server_role: str = "master"  # or "replica"
    master_socket: Optional[socket.socket] = None
    replica_offset: int = 0

    # --- Replication Info ---
    replication_lock: threading.Lock = field(default_factory=threading.Lock)
    connected_replicas: list[socket.socket] = field(default_factory=list)

    # --- Data Store (key-value or stream objects) ---
    db: Dict[str, Any] = field(default_factory=dict)

    # --- Pub/Sub system ---
    pubsub_channels: Dict[str, list[socket.socket]] = field(default_factory=dict)

    # --- RDB (Persistence) ---
    rdb_last_save_time: Optional[datetime.datetime] = None
    rdb_dir: Optional[str] = None
    rdb_filename: Optional[str] = None

    # --- Thread safety ---
    db_lock: threading.Lock = field(default_factory=threading.Lock)

