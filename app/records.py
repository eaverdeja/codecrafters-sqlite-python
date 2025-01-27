from dataclasses import dataclass

from .serial_type import SQLiteSerialType
from .varint import Varint


@dataclass
class RecordFormat:
    @classmethod
    def _parse_header(cls, data: bytes) -> tuple[int, list[SQLiteSerialType]]:
        offset = 0
        # First piece of information is the record header size
        record_header = Varint.from_data(data)
        offset += record_header.bytes_length

        serial_types = []
        # The next pieces are varints describing the column types and sizes
        total_bytes_read = 0
        while total_bytes_read < record_header.value - 1:
            piece = data[offset + total_bytes_read :]
            serial_type_varint = Varint.from_data(piece)
            total_bytes_read += serial_type_varint.bytes_length
            serial_types.append(SQLiteSerialType.decode(serial_type_varint.value))

        offset += total_bytes_read

        return offset, serial_types


@dataclass
class SqliteSchemaRecord(RecordFormat):
    record_type: str
    name: str
    table_name: str
    rootpage: bytes
    sql: str
    serial_types: list[SQLiteSerialType]

    @classmethod
    def from_record(cls, data: bytes):
        offset, serial_types = cls._parse_header(data)

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

        bytes_length = serial_types[3][1]
        rootpage = data[offset : offset + bytes_length]
        offset += bytes_length

        bytes_length = serial_types[4][1]
        sql = (data[offset : offset + bytes_length]).decode()
        offset += bytes_length

        return cls(
            record_type=record_type,
            name=name,
            table_name=table_name,
            rootpage=rootpage,
            sql=sql,
            serial_types=serial_types,
        )


class UserTableRecord(RecordFormat):
    @classmethod
    def from_record(cls, data: bytes, table_columns: list[str]):
        offset, serial_types = cls._parse_header(data)

        # For every column we have, pair it with a serial type
        # and retrieve the associated data
        columns = {}
        for column_idx, column in enumerate(table_columns):
            bytes_length = serial_types[column_idx][1]
            value = (data[offset : offset + bytes_length]).decode()
            columns[column] = value
            offset += bytes_length

        return columns
