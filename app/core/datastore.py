import threading
DATA_LOCK = threading.Lock()
DATA_STORE = {}

def get_data_entry(key: str) -> dict | None:
    """
    Retrieves a key, checks for expiration, and performs lazy deletion if expired.
    Returns the valid data entry dictionary or None if the key is missing/expired.
    """
    with DATA_LOCK:
        data_entry = DATA_STORE.get(key)

        if data_entry is None:
            # Key does not exist
            return None

        expiry = data_entry.get("expiry")
        current_time_ms = int(time.time() * 1000)

        # Check for expiration
        if expiry is not None and current_time_ms >= expiry:
            # Key has expired; delete it
            del DATA_STORE[key]
            return None

        return data_entry