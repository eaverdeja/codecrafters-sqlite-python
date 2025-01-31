from .serial_type import SQLiteSerialType
from .database import Database
from .page import BTreeWalker, Page
from .varint import Varint


class CellCounter(BTreeWalker[int]):
    def visit_leaf(self, page: Page) -> int:
        return page.cell_count


class RecordCollector(BTreeWalker[list[tuple[int, bytes]]]):
    def __init__(self, target_row_id: int | None = None):
        self.target_row_id = target_row_id

    def visit_leaf(self, page: Page):
        records = []
        for cell_pointer in page.cell_pointers:
            offset = 0
            data = page.data[cell_pointer:]

            # The first relevant information in the cell is a
            # varint that describes the record's payload size
            record_size_varint = Varint.from_data(data[offset:])
            offset += record_size_varint.bytes_length

            # The second information is the rowid, also a varint
            rowid_varint = Varint.from_data(data[offset:])
            offset += rowid_varint.bytes_length

            # The third information is the actual record payload
            data = data[offset : offset + record_size_varint.value]
            if not self.target_row_id:
                records.append((rowid_varint.value, data))
            elif rowid_varint.value == self.target_row_id:
                records.append((rowid_varint.value, data))

        return records


class IndexSearcher(BTreeWalker[list[int]]):
    def __init__(self, database: Database, search_key: bytes):
        self.database = database
        self.search_key = search_key

    def visit_leaf(self, page: Page) -> list[int]:
        """
        Search leaf page for matching key.
        """
        records = []
        with self.database.reader() as f:
            for cell_pointer in page.cell_pointers:
                # Position at start of cell content
                f.seek(self.database.page_size * page.page_number + cell_pointer)

                # Read payload size varint
                payload_size = Varint.from_data(f)

                # Read key data
                key_record = f.read(payload_size.value)
                key, rowid = self._parse_key_record(key_record)

                if self.search_key == key:
                    records.append(rowid)

                # Index entries are sorted, so we can stop if we've gone too far
                if key > self.search_key:
                    break

        return records

    def visit_interior(self, page: Page) -> list[int]:
        records = []
        with self.database.reader() as f:
            for cell_pointer in page.cell_pointers:
                f.seek(self.database.page_size * page.page_number + cell_pointer)
                f.read(4)  # Skip child pointer
                payload_size = Varint.from_data(f).value
                key_record = f.read(payload_size)
                key, rowid = self._parse_key_record(key_record)

                if key == self.search_key:
                    records.append(rowid)
                elif key > self.search_key:
                    break

        return records

    def choose_paths(self, page: Page) -> list[int]:
        """
        Determine which child pages to follow by comparing keys.
        """
        paths = []
        found_larger = False

        with self.database.reader() as f:
            keys = []
            for i, cell_pointer in enumerate(page.cell_pointers):
                f.seek(self.database.page_size * page.page_number + cell_pointer)
                f.read(4)  # Skip child pointer
                payload_size = Varint.from_data(f).value
                key_record = f.read(payload_size)
                key, _ = self._parse_key_record(key_record)
                keys.append(key)

        if self.search_key < keys[0]:
            return [0]
        if self.search_key > keys[-1]:
            return [-1]

        # Now analyze the keys
        for i, key in enumerate(keys):
            if key > self.search_key:
                paths.append(i)
                found_larger = True
                break
            elif self.search_key == key:
                paths.append(i)

        # If we haven't found a larger key, follow rightmost pointer
        if not found_larger:
            paths.append(-1)

        return paths

    def _parse_key_record(self, key_record: bytes) -> tuple[bytes, int]:
        # The key payload is in record format, so we need to parse it.
        # First is the record header, which gives us the total
        # number of bytes in the header
        record_header = Varint.from_data(key_record)
        offset = record_header.bytes_length

        # The next pieces are varints describing the column types
        # and their sizes
        serial_types = []
        total_bytes_read = 0
        while total_bytes_read < record_header.value:
            piece = key_record[offset + total_bytes_read - 1 :]
            serial_type_varint = Varint.from_data(piece)
            total_bytes_read += serial_type_varint.bytes_length
            serial_types.append(SQLiteSerialType.decode(serial_type_varint.value))

        offset = 0
        bytes_length = serial_types[0][1]
        # What is this? Not sure
        _ = key_record[offset : offset + bytes_length]
        offset += bytes_length

        bytes_length = serial_types[1][1]
        key = key_record[offset : offset + bytes_length]
        offset += bytes_length

        bytes_length = serial_types[2][1]
        rowid = int.from_bytes(key_record[offset : offset + bytes_length])

        offset += bytes_length
        return key, rowid
