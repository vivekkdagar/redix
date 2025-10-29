import time

class RDBParser:
    HEADER_MAGIC = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31"  # "REDIS0001"
    META_START = 0xFA
    # REDIS_VERSION = b"\x09\x72\x65\x64\x69\x73\x2D\x76\x65\x72"
    EOF = 0xFF
    DB_START = 0xFE
    HASH_START = 0xFB
    STRING_START = 0x00
    INT_START = 0x01
    EXPIRE_TIME = 0xFD
    EXPIRE_TIME_MS = 0xFC

    def __init__(self, path):
        try:
            with open(path, 'rb') as f:
                self.content = f.read()
        except FileNotFoundError:
            self.content = None
        self.pointer = 0

    def _read(self, length):
        if self.pointer + length > len(self.content):
            raise EOFError("Unexpected end of file")
        data = self.content[self.pointer:self.pointer + length]
        self.pointer += length
        return data
    
    def _read_byte(self):
        return self._read(1)[0]
    
    def _read_length(self):
        first_byte = self._read_byte()
        type = (first_byte & 0xC0) >> 6
        if type == 0b00:
            return first_byte & 0x3F
        elif type == 0b01:
            second_byte = self._read_byte()
            return ((first_byte & 0x3F) << 8) | second_byte
        elif type == 0b10:
            return int.from_bytes(self._read(4), 'big')
        else:
            return first_byte
        
    def _read_string(self):
        length_or_type = self._read_length()

        if (length_or_type & 0xC0) >> 6 == 0b11:
            encoding_type = length_or_type & 0x3F
            if encoding_type == 0:  # int8
                return str(int.from_bytes(self._read(1), 'little'))
            elif encoding_type == 1:  # int16
                return str(int.from_bytes(self._read(2), 'little'))
            elif encoding_type == 2:  # int32
                return str(int.from_bytes(self._read(4), 'little'))

        length = length_or_type
        return self._read(length).decode()
    
    def parse(self):
        if not self.content:
            return {}
        
        # Check header
        magic = self._read(len(self.HEADER_MAGIC))
        if magic != self.HEADER_MAGIC:
            raise ValueError("Invalid RDB file: incorrect header magic")
        
        data = {}
        expiry_ms = None

        while self.pointer < len(self.content):
            opcode = self._read_byte()

            if opcode == self.META_START:
                self._read_string() # aux key
                self._read_string() # aux value
            elif opcode == self.DB_START:
                self._read_length() # db number
            elif opcode == self.HASH_START:
                self._read_length() # db_hash_table_size
                self._read_length() # expiry_hash_table_size
            elif opcode == self.EXPIRE_TIME:
                expiry_time = int.from_bytes(self._read(4), 'little')
                expiry_ms = expiry_time * 1000
                continue
            elif opcode == self.EXPIRE_TIME_MS:
                expiry_ms = int.from_bytes(self._read(8), 'little')
                continue
            elif opcode == self.EOF:
                break
            else:
                value_type = opcode
                key = self._read_string()
                value = self._read_string()

                if expiry_ms is None or (expiry_ms > int(time.time() * 1000)):
                    data[key] = value
                
                expiry_ms = None
        print(f"RDB parsing completed. Parsed {len(data)} keys.")
        return data