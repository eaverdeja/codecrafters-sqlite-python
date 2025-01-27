import sys

from .database import Database
from .page import PageType
from .records import RecordFormat, UserTableRecord, SqliteSchemaRecord
from .sql import SQL


def _get_records_for_sqlite_schema_table(
    database: Database,
) -> list[SqliteSchemaRecord]:
    page = database.get_page(0)
    return [
        SqliteSchemaRecord.from_record(r)
        for r in RecordFormat.get_records(database, page)
    ]


def main():
    database_file_path = sys.argv[1]
    command = sys.argv[2].lower()
    database = Database(database_file_path)

    if command == ".dbinfo":
        page = database.get_page(0)
        print(f"database page size: {database.page_size}")
        # In our case, the cell count on the first page will equal the
        # number of tables in the database. That's because we have no indexes,
        # so the SQLite schema table only contains references to other tables.
        print(f"number of tables: {page.cell_count}")
    elif command == ".tables":
        records = _get_records_for_sqlite_schema_table(database)
        table_names = [record.table_name for record in records]
        table_names.sort()
        print(" ".join(table_names))
    elif command.upper().startswith("SELECT "):
        user_command_sql = SQL.from_query(command)

        # Get the schema record corresponding to the table we're looking up
        table_record = next(
            (
                record
                for record in _get_records_for_sqlite_schema_table(database)
                if record.table_name == user_command_sql.table
            )
        )
        # rootpage is 1-indexed, so we need to subtract 1 to get to the correct page
        page_number = int.from_bytes(table_record.rootpage, byteorder="big") - 1

        # Read the page into memory
        page = database.get_page(page_number)
        if not page.type == PageType.LEAF_TABLE_B_TREE:
            raise Exception("Expected page type to be leaf table b-tree")

        if "count(*)" in user_command_sql.columns:
            print(page.cell_count)
        else:  # Assume a SELECT {columns} FROM {table} type query

            # Parse the CREATE TABLE query to figure out
            # the available columns and their ordering
            create_query = SQL.from_query(table_record.sql)
            # Retrieve the data records for our lookup table
            records = [
                UserTableRecord.from_record(r, table_columns=create_query.columns)
                for r in RecordFormat.get_records(database, page)
            ]
            # Print out the values for the lookup column
            for record in records:
                # Compute any comparison and apply our where clause
                # to filter out records
                comparisons = {
                    col: user_command_sql.where.get(col) for col in create_query.columns
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
