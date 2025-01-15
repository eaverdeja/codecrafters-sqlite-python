import sys
from enum import Enum

from .parsers import Varint, Record

# import sqlparse - available if you need it!


class PageType(Enum):
    INTERIOR_INDEX_B_TREE = 0x02
    INTERIOR_TABLE_B_TREE = 0x05
    LEAF_INDEX_B_TREE = 0x0A
    LEAF_TABLE_B_TREE = 0x0D


def get_page_size(database_file: bytes):
    # Skip the first 16 bytes of the header
    database_file.seek(16)
    return int.from_bytes(database_file.read(2), byteorder="big")


def get_cell_count(database_file: bytes, page_size: int):
    # Skip file header
    database_file.seek(100)
    # Page header is directly after file header
    page = database_file.read(page_size)
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


def main():
    database_file_path = sys.argv[1]
    command = sys.argv[2]

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

            records = []
            for cell_pointer in get_cell_pointers(page, cell_count):
                database_file.seek(cell_pointer)
                # The relevant information in the cell is a varint that describes the record's size
                record_size_varint = Varint.from_data(database_file)
                # The second information is the rowid, also a varint - irrelevant for us now
                _rowid_varint = Varint.from_data(database_file)
                # The third information is the actual record
                data = database_file.read(record_size_varint.value)
                records.append(Record.from_data(data))

            table_names = [record.table_name for record in records]
            table_names.sort()
            print(" ".join(table_names))
    else:
        print(f"Invalid command: {command}")


if __name__ == "__main__":
    main()
