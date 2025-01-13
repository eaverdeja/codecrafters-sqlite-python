import sys

from dataclasses import dataclass

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        # Skip the first 16 bytes of the header
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        print(f"database page size: {page_size}")

        # Skip file header
        database_file.seek(100)
        # Page header is directly after file header
        page = database_file.read(page_size)
        # The cell count is available as a 2-byte integer,
        # starting from a 3-byte offset
        cell_count = int.from_bytes(page[3 : 3 + 2], byteorder="big")
        print(f"number of tables: {cell_count}")
else:
    print(f"Invalid command: {command}")
