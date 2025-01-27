from io import BufferedReader
import sys
from enum import Enum

from .parsers import UserTableRecord, Varint, SqliteSchemaRecord, SQL


class PageType(Enum):
    INTERIOR_INDEX_B_TREE = 0x02
    INTERIOR_TABLE_B_TREE = 0x05
    LEAF_INDEX_B_TREE = 0x0A
    LEAF_TABLE_B_TREE = 0x0D


def get_page_size(database_file: BufferedReader):
    # Skip the first 16 bytes of the header
    database_file.seek(16)
    return int.from_bytes(database_file.read(2), byteorder="big")


def get_cell_count(database_file: BufferedReader, page_size: int):
    # Skip file header
    database_file.seek(100)
    # Page header is directly after file header
    page = database_file.read(page_size)
    return get_cell_count_from_page(page)


def get_cell_count_from_page(page: bytes) -> int:
    # The cell count is available as a 2-byte integer,
    # starting from a 3-byte offset
    return int.from_bytes(page[3 : 3 + 2], byteorder="big")


def get_cell_pointers(page: bytes, cell_count: int) -> list[int]:
    # Page type is available as a 1-byte value from the 0 offset
    page_type = PageType(page[0])
    # The page header is 8 bytes in size for leaf pages, 12 bytes otherwise
    page_header_size = (
        8
        if page_type == PageType.LEAF_TABLE_B_TREE
        or page_type == PageType.LEAF_INDEX_B_TREE
        else 12
    )

    # Accumulate pointers to the cells for the sqlite_schema table
    cell_pointers = []
    for i in range(cell_count):
        # Cell pointers are available as 2-byte integers,
        # starting right aftet the page header
        b = page[page_header_size + i * 2 : page_header_size + (i + 1) * 2]
        cell_pointers.append(int.from_bytes(b, byteorder="big"))
    cell_pointers.sort()

    return cell_pointers


def get_records_for_sqlite_schema_table(
    database_file: BufferedReader, page: bytes, cell_count: int
) -> list[SqliteSchemaRecord]:
    return map(
        lambda r: SqliteSchemaRecord.from_record(r),
        _get_records(database_file, page, cell_count),
    )


def get_records_for_table(
    database_file: BufferedReader,
    page: bytes,
    cell_count: int,
    start: int,
    table_columns: list[str],
) -> list[UserTableRecord]:
    return map(
        lambda r: UserTableRecord.from_record(r, table_columns=table_columns),
        _get_records(database_file, page, cell_count, start),
    )


def _get_records(
    database_file: BufferedReader, page: bytes, cell_count: int, start: int = 0
) -> list[bytes]:
    records = []
    for cell_pointer in get_cell_pointers(page, cell_count):
        database_file.seek(start + cell_pointer)
        # The first relevant information in the cell is a varint that describes the record's size
        record_size_varint = Varint.from_data(database_file)
        # The second information is the rowid, also a varint - irrelevant for us now
        _rowid_varint = Varint.from_data(database_file)
        # The third information is the actual record
        data = database_file.read(record_size_varint.value)
        records.append(data)
    return records


def main():
    database_file_path = sys.argv[1]
    command = sys.argv[2].lower()

    if command == ".dbinfo":
        with open(database_file_path, "rb") as database_file:
            page_size = get_page_size(database_file)
            cell_count = get_cell_count(database_file, page_size)
            print(f"database page size: {page_size}")
            print(f"number of tables: {cell_count}")
    elif command == ".tables":
        with open(database_file_path, "rb") as database_file:
            page_size = get_page_size(database_file)
            cell_count = get_cell_count(database_file, page_size)
            # Skip file header
            database_file.seek(100)
            # Page header is directly after file header
            page = database_file.read(page_size)

            records = get_records_for_sqlite_schema_table(
                database_file, page, cell_count
            )
            table_names = [record.table_name for record in records]
            table_names.sort()
            print(" ".join(table_names))
    elif command.upper().startswith("SELECT "):
        user_command_sql = SQL.from_query(command)

        with open(database_file_path, "rb") as database_file:
            page_size = get_page_size(database_file)
            cell_count = get_cell_count(database_file, page_size)
            # Skip file header
            database_file.seek(100)
            # Page header is directly after file header
            page = database_file.read(page_size)

            # Get the schema record corresponding to the table we're looking up
            table_record = next(
                (
                    record
                    for record in get_records_for_sqlite_schema_table(
                        database_file, page, cell_count
                    )
                    if record.table_name == user_command_sql.table
                )
            )
            # rootpage is 1-indexed, so we need to subtract 1 to get to the correct page
            page_number = int.from_bytes(table_record.rootpage, byteorder="big") - 1

            # Read the page into memory
            database_file.seek(page_size * page_number)
            page = database_file.read(page_size)
            page_type = PageType(page[0])
            if not page_type == PageType.LEAF_TABLE_B_TREE:
                raise Exception("Expected page type to be leaf table b-tree")

            if "count(*)" in user_command_sql.columns:
                # Leaf table b-tree pages have 8-byte sized headers,
                # with the cell count on offset 3, using a 2-byte integer
                cell_count = int.from_bytes(page[3 : 3 + 2])
                print(cell_count)
            else:  # Assume a SELECT {columns} FROM {table} type query

                # Parse the CREATE TABLE query to figure out
                # the available columns and their ordering
                create_query = SQL.from_query(table_record.sql)
                # Retrive the data records for our lookup table
                records = get_records_for_table(
                    database_file,
                    page,
                    cell_count=get_cell_count_from_page(page),
                    start=page_size * page_number,
                    table_columns=create_query.columns,
                )
                # Print out the values for the lookup column
                for record in records:
                    # Compute any comparison and apply our where clause
                    # to filter out records
                    comparisons = {
                        col: user_command_sql.where.get(col)
                        for col in create_query.columns
                    }
                    column_values = {col: record[col] for col in create_query.columns}
                    pairs = zip(column_values.values(), comparisons.values())
                    if any(
                        comparison is not None and value.lower() != comparison.lower()
                        for value, comparison in pairs
                    ):
                        continue

                    # Keep things sorted as specified in the user command SQL
                    def _find(l: list, value: str) -> int:
                        try:
                            return l.index(value)
                        except ValueError:
                            return -1

                    sorted_columns = sorted(
                        column_values.items(),
                        key=lambda column: _find(user_command_sql.columns, column[0]),
                    )

                    # Join the column values and print them out,
                    # if they are specified in the user command SQL
                    line = "|".join(
                        column_value
                        for column_name, column_value in sorted_columns
                        if column_name in user_command_sql.columns
                    )
                    print(line)

    else:
        print(f"Invalid command: {command}")


if __name__ == "__main__":
    main()
