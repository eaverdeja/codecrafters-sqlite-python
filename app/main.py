import sys


from .database import Database
from .btree import IndexSearcher, RecordCollector, CellCounter
from .page import Page, search_index, walk_btree
from .records import RecordFormat, UserTableRecord, SqliteSchemaRecord
from .sql import SQL


def _get_records_for_sqlite_schema_table(
    database: Database,
) -> list[SqliteSchemaRecord]:
    page = Page.get_page(database, 0)
    if not page:
        raise Exception("Expected root page to exist")

    return [
        SqliteSchemaRecord.from_record(r)
        for r in RecordFormat.get_records(database, page)
    ]


def main():
    database_file_path = sys.argv[1]
    command = sys.argv[2].lower()
    database = Database(database_file_path)

    if command == ".dbinfo":
        page = Page.get_page(database, 0)
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
        sqlite_schema_records = _get_records_for_sqlite_schema_table(database)
        table_record = next(
            (
                record
                for record in sqlite_schema_records
                if record.table_name == user_command_sql.table
            )
        )
        # rootpage is 1-indexed, so we need to subtract 1 to get to the correct page
        page_number = int.from_bytes(table_record.rootpage, byteorder="big") - 1

        # Read the root page into memory
        root_page = Page.get_page(database, page_number)

        if "count(*)" in user_command_sql.columns:
            # Walk the b-tree from the root page, aggregating all
            # cell counts from leaf table pages
            cell_count = sum(walk_btree(root_page, database, CellCounter()))
            print(cell_count)
        else:  # Assume a SELECT {columns} FROM {table} type query
            # Parse the CREATE TABLE query to figure out
            # the available columns and their ordering
            create_query = SQL.from_query(table_record.sql)

            # Is there filtering going on?
            idxs = []
            if user_command_sql.where and "country" in user_command_sql.where.keys():
                # Let's look for an index we can use
                index_record = next(
                    (
                        record
                        for record in sqlite_schema_records
                        if record.record_type == "index"
                        and record.name == "idx_companies_country"
                    )
                )
                index_root_page = Page.get_page(
                    database, int.from_bytes(index_record.rootpage) - 1
                )
                key = user_command_sql.where["country"].encode()
                idxs = [
                    idx
                    for idxs in search_index(
                        index_root_page,
                        database,
                        IndexSearcher(database, key),
                    )
                    for idx in idxs
                ]

            records = []
            if idxs:
                # Retrieve the data records for our lookup table using the index
                for target_row_id in sorted(idxs):
                    for row_id, row in walk_btree(
                        root_page,
                        database,
                        RecordCollector(target_row_id),
                        target_row_id,
                    ):
                        records.append(
                            UserTableRecord.from_record(
                                row_id, row, table_columns=create_query.columns
                            )
                        )
            else:
                # Retrieve the data records for our lookup table using a full-table scan
                records = [
                    UserTableRecord.from_record(
                        row_id, row, table_columns=create_query.columns
                    )
                    for records in walk_btree(
                        root_page,
                        database,
                        RecordCollector(),
                    )
                    for row_id, row in records
                ]

            # Print out the values for the lookup column
            for record in records:
                column_values = {col: record[col] for col in create_query.columns}
                # Compute any comparison and apply our where clause
                # to filter out records
                if not idxs:
                    comparisons = {
                        col: user_command_sql.where.get(col)
                        for col in create_query.columns
                    }
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
                    str(column_value)
                    for column_name, column_value in sorted_columns
                    if column_name in user_command_sql.columns
                )
                print(line)

    else:
        print(f"Invalid command: {command}")


if __name__ == "__main__":
    main()
