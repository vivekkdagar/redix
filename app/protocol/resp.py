"""
RESP (REdis Serialization Protocol) Parser

This module handles parsing and encoding of RESP protocol messages.
RESP is a simple text-based protocol used by Redis for client-server communication.
"""


def parse_resp_array(data: bytes) -> tuple[list[str] | None, int]:
    """
    Parse a RESP array from bytes.
    
    Args:
        data: Raw bytes containing RESP protocol data
        
    Returns:
        tuple: (parsed_command_list, bytes_consumed)
               Returns (None, 0) if parsing fails or incomplete data
    """
    if not data or not data.startswith(b'*'):
        return None, 0
    
    try:
        # Find the first \r\n to get array length
        first_crlf = data.find(b'\r\n')
        if first_crlf == -1:
            return None, 0
        
        # Parse array length
        array_length = int(data[1:first_crlf])
        
        offset = first_crlf + 2  # Skip '*N\r\n'
        parsed_elements = []
        
        # Parse each bulk string in the array
        for _ in range(array_length):
            if offset >= len(data):
                return None, 0
            
            # Each element should be a bulk string starting with '$'
            if data[offset:offset + 1] != b'$':
                return None, 0
            
            # Find the bulk string length
            length_end = data.find(b'\r\n', offset)
            if length_end == -1:
                return None, 0
            
            bulk_length = int(data[offset + 1:length_end])
            
            # Extract the bulk string content
            content_start = length_end + 2
            content_end = content_start + bulk_length
            
            if content_end + 2 > len(data):
                return None, 0
            
            content = data[content_start:content_end].decode('utf-8')
            parsed_elements.append(content)
            
            # Move offset past this bulk string
            offset = content_end + 2  # Skip '\r\n'
        
        return parsed_elements, offset
        
    except (ValueError, UnicodeDecodeError):
        return None, 0


def encode_simple_string(s: str) -> bytes:
    """
    Encode a simple string in RESP format.
    
    Args:
        s: String to encode
        
    Returns:
        RESP-encoded simple string
    """
    return f"+{s}\r\n".encode()


def encode_bulk_string(s: str) -> bytes:
    """
    Encode a bulk string in RESP format.
    
    Args:
        s: String to encode
        
    Returns:
        RESP-encoded bulk string
    """
    s_bytes = s.encode()
    return f"${len(s_bytes)}\r\n".encode() + s_bytes + b"\r\n"


def encode_null_bulk_string() -> bytes:
    """
    Encode a null bulk string in RESP format.
    
    Returns:
        RESP-encoded null bulk string
    """
    return b"$-1\r\n"


def encode_error(error_msg: str) -> bytes:
    """
    Encode an error message in RESP format.
    
    Args:
        error_msg: Error message to encode
        
    Returns:
        RESP-encoded error message
    """
    return f"-{error_msg}\r\n".encode()
