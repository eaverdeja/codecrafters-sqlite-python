from dataclasses import dataclass
from typing import Any

from .serial_type import SQLiteSerialType
from .varint import Varint
from .database import Database
from .page import Page


@dataclass
class RecordFormat:
    @classmethod
    def get_records(cls, database: Database, page: Page) -> list[bytes]:
        records = []
        with database.reader() as database_file:
            for cell_pointer in page.cell_pointers:
                database_file.seek(database.page_size * page.page_number + cell_pointer)
                # The first relevant information in the cell is a varint that describes the record's size
                record_size_varint = Varint.from_data(database_file)
                # The second information is the rowid, also a varint - irrelevant for us now
                _rowid_varint = Varint.from_data(database_file)
                # The third information is the actual record
                data = database_file.read(record_size_varint.value)
                records.append(data)
        return records

    @classmethod
    def parse_header(cls, data: bytes) -> tuple[int, list[tuple[str, int]]]:
        """
        The header begins with a single varint which determines the total number of bytes in the header.
        The varint value is the size of the header in bytes including the size varint itself.
        Following the size varint are one or more additional varints, one per column.
        These additional varints are called "serial type" numbers and determine the datatype of each column,
        according to the following chart:
        ...
        The values for each column in the record immediately follow the header.

        https://www.sqlite.org/fileformat.html#record_format
        """
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
    serial_types: list[tuple[str, int]]

    @classmethod
    def from_record(cls, data: bytes):
        offset, serial_types = cls.parse_header(data)

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
    def from_record(cls, row_id: int, data: bytes, table_columns: list[str]):
        offset, serial_types = cls.parse_header(data)

        # For every column we have, pair it with a serial type
        # and retrieve the associated data
        columns: dict[str, Any] = {}
        for column_idx, column in enumerate(table_columns):
            if column == "id":
                columns[column] = row_id
                continue

            bytes_length = serial_types[column_idx][1]
            value: Any
            try:
                value = (data[offset : offset + bytes_length]).decode()
            except UnicodeDecodeError:
                # TODO: Check serial type instead of letting things blow up
                value = int.from_bytes(data[offset : offset + bytes_length])
            columns[column] = value
            offset += bytes_length

        return columns
