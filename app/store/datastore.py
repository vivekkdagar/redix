"""
Data Store Module

This module manages the in-memory data store with support for:
- String values with optional expiration
- Thread-safe operations
- Lazy deletion of expired keys
"""

import threading
import time
from typing import Optional


class DataStore:
    """Thread-safe in-memory data store with expiration support."""
    
    def __init__(self):
        """Initialize the data store with a lock for thread safety."""
        self._data = {}
        self._lock = threading.Lock()
    
    def set(self, key: str, value: str, expiry_ms: Optional[int] = None) -> None:
        """
        Set a key-value pair with optional expiration.
        
        Args:
            key: The key to set
            value: The value to store
            expiry_ms: Optional expiration time in milliseconds from now
        """
        with self._lock:
            expiry_timestamp = None
            if expiry_ms is not None:
                current_time_ms = int(time.time() * 1000)
                expiry_timestamp = current_time_ms + expiry_ms
            
            self._data[key] = {
                'value': value,
                'expiry': expiry_timestamp
            }
    
    def get(self, key: str) -> Optional[str]:
        """
        Get a value by key, checking for expiration.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found and not expired, None otherwise
        """
        with self._lock:
            if key not in self._data:
                return None
            
            entry = self._data[key]
            expiry = entry.get('expiry')
            
            # Check if expired
            if expiry is not None:
                current_time_ms = int(time.time() * 1000)
                if current_time_ms >= expiry:
                    # Lazy deletion
                    del self._data[key]
                    return None
            
            return entry['value']
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists and is not expired.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists and is not expired, False otherwise
        """
        return self.get(key) is not None
