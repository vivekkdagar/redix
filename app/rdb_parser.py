class RDBParser:
    HEADER_MAGIC = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31"
    META_START = 0xFA
    EOF = 0xFF
    DB_START = 0xFE
    HASH_START = 0xFB
    EXPIRE_TIME = 0xFD
    EXPIRE_TIME_MS = 0xFC

    def __init__(self, path):
        try:
            with open(path, 'rb') as f:
                self.content = f.read()
        except FileNotFoundError:
            self.content = None
        self.pointer = 0

    def _read(self, n):
        d = self.content[self.pointer:self.pointer + n]
        self.pointer += n
        return d

    def _read_byte(self):
        return self._read(1)[0]

    def _read_length(self):
        first = self._read_byte()
        t = (first & 0xC0) >> 6
        if t == 0b00:
            return first & 0x3F
        elif t == 0b01:
            return ((first & 0x3F) << 8) | self._read_byte()
        elif t == 0b10:
            return int.from_bytes(self._read(4), 'little')
        else:
            return first

    def _read_string(self):
        length = self._read_length()
        raw = self._read(length)
        try:
            return raw.decode('utf-8')
        except UnicodeDecodeError:
            return raw.decode('latin1', errors='ignore')

    def parse(self):
        if not self.content:
            return {}

        self._read(len(self.HEADER_MAGIC))
        data = {}
        expiry = None

        while self.pointer < len(self.content):
            if self.pointer >= len(self.content):
                break
            opcode = self._read_byte()

            if opcode == self.META_START:
                # skip meta (key, val)
                try:
                    self._read_string()
                    self._read_string()
                except Exception:
                    continue
            elif opcode == self.DB_START:
                self._read_length()
            elif opcode == self.HASH_START:
                self._read_length()
                self._read_length()
            elif opcode == self.EXPIRE_TIME:
                expiry = int.from_bytes(self._read(4), 'little') * 1000
                continue
            elif opcode == self.EXPIRE_TIME_MS:
                expiry = int.from_bytes(self._read(8), 'little')
                continue
            elif opcode == self.EOF:
                break
            else:
                try:
                    key = self._read_string()
                    value = self._read_string()
                    if expiry is None or expiry > int(time.time() * 1000):
                        data[key] = value
                    expiry = None
                except Exception:
                    # skip unknown/invalid entries
                    continue

        print(f"RDB parsed {len(data)} keys successfully.")
        return data