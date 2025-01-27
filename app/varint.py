from dataclasses import dataclass
from io import BufferedReader


@dataclass
class Varint:
    value: int
    bytes_length: int

    @classmethod
    def from_data(cls, source: BufferedReader | bytes):
        value = 0
        bytes_read = 0

        # SQLite varints have at most 9 bytes
        while bytes_read < 9:
            # Get next byte depending on source type
            if isinstance(source, BufferedReader):
                byte = source.read(1)[0]
            else:
                byte = source[bytes_read]

            # & 0b01111111 will shave off the highest bit - that's the varint value
            # << 7 will create space for the new incoming bits
            # | will append the new bits to value
            value = (value << 7) | (byte & 0b01111111)
            bytes_read += 1

            # If highest bit is not set, we're done
            if not (byte & 0b10000000):
                break

        return cls(value=value, bytes_length=bytes_read)
