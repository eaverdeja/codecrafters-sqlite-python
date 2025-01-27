from io import BufferedReader
from .serial_type import SQLiteSerialType

from dataclasses import dataclass

from sqlparse import parse as parse_sql
from sqlparse import sql


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


@dataclass
class SqliteSchemaRecord:
    record_type: str
    name: str
    table_name: str
    rootpage: bytes
    sql: str
    serial_types: list[SQLiteSerialType]

    @classmethod
    def from_record(cls, data: bytes):
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


class UserTableRecord:
    @classmethod
    def from_record(cls, data: bytes, table_columns: list[str]):
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

        # For every column we have, pair it with a serial type
        # and retrieve the associated data
        columns = {}
        for column_idx, column in enumerate(table_columns):
            bytes_length = serial_types[column_idx][1]
            value = (data[offset : offset + bytes_length]).decode()
            columns[column] = value
            offset += bytes_length

        return columns


@dataclass
class SQL:
    operation: str
    columns: list[str]
    table: str
    where: dict[str, str] | None = None

    @classmethod
    def from_query(cls, query: str):
        sql_statement = parse_sql(query)
        tokens = sql_statement[0].tokens
        operation = tokens[0].value.lower()
        if operation == "create":
            table = next(
                token.value
                for token in tokens[1:-2]
                if isinstance(token, sql.Identifier)
            )
            columns = tokens[-1]
            columns = [
                # This will extract the column names
                token.lstrip(" \n\t(").split(" ")[0]
                for token in columns.value.split(",")
                if token not in ["(", ")"]
            ]
            return SQL(operation=operation, table=table, columns=columns)

        elif operation == "select":
            columns = []
            where = {}
            from_idx = next((idx for idx, t in enumerate(tokens) if t.value == "from"))
            # Parse column names
            for token in tokens[1:from_idx]:
                if isinstance(token, sql.Function) or isinstance(token, sql.Identifier):
                    columns.append(token.value)
                elif isinstance(token, sql.IdentifierList):
                    columns += map(lambda t: t.strip(), token.value.split(","))

            # Parse the WHERE clause
            where_token = next((t for t in tokens if isinstance(t, sql.Where)), None)
            if where_token:
                comparison_tokens = [
                    t for t in where_token.tokens if isinstance(t, sql.Comparison)
                ]
                for t in comparison_tokens:
                    key, value = t.value.split("=")
                    where[key.strip()] = value.strip(" '")

            # Skip the whitespace token and get to our table name
            table = tokens[from_idx + 2].value
            return SQL(operation=operation, columns=columns, table=table, where=where)
        else:
            raise Exception(f"Unsupported operation type: {operation}")
