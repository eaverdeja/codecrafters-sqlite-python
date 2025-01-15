from io import BufferedReader
from .serial_type import SQLiteSerialType

from dataclasses import dataclass


def parse_varint(source: BufferedReader | bytes) -> tuple[int, int]:
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

    return value, bytes_read


@dataclass
class Record:
    record_type: str
    name: str
    table_name: str
    serial_types: list[SQLiteSerialType]

    @classmethod
    def from_data(cls, data: bytes):
        offset = 0
        # First piece of information is the record header size
        record_header_size, bytes_read = parse_varint(data)
        offset += bytes_read

        serial_types = []
        # The next pieces are varints describing the column types and sizes
        total_bytes_read = 0
        while total_bytes_read < record_header_size - 1:
            piece = data[offset + total_bytes_read :]
            serial_type, bytes_read = parse_varint(piece)
            total_bytes_read += bytes_read
            serial_types.append(SQLiteSerialType.decode(serial_type))

        offset += total_bytes_read

        # With the serial types and associated sizes for each column
        # we can start picking out the data
        bytes_length = serial_types[0][1]
        record_type = (data[offset : offset + bytes_length]).decode()
        offset += bytes_length

        bytes_length = serial_types[1][1]
        name = (data[offset : offset + bytes_length]).decode()
        offset += bytes_length

        bytes_length = serial_types[2][1]
        table_name = (data[offset : offset + bytes_length]).decode()
        offset += bytes_length

        return Record(
            record_type=record_type,
            name=name,
            table_name=table_name,
            serial_types=serial_types,
        )
