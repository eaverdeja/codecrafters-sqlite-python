from .serial_type import SQLiteSerialType
from .database import Database
from .page import BTreeWalker, Page
from .varint import Varint


class CellCounter(BTreeWalker[int]):
    def visit_leaf(self, page: Page) -> int:
        return page.cell_count


class RecordCollector(BTreeWalker[list[tuple[int, bytes]]]):
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
            records.append((rowid_varint.value, data))

        return records


class IndexSearcher(BTreeWalker[list[tuple[bytes, int]]]):
    def __init__(self, database: Database, search_key: bytes):
        self.database = database
        self.search_key = search_key
        print(f"Searching for key: {self.search_key!r}")

    def visit_leaf(self, page: Page) -> list[tuple[bytes, int]]:
        """
        Search leaf page for matching key.
        """
        print(f"Visiting leaf page {page.page_number}")
        records = []
        keys = set()
        with self.database.reader() as f:
            for cell_pointer in page.cell_pointers:
                # Position at start of cell content
                f.seek(self.database.page_size * page.page_number + cell_pointer)

                # Read payload size varint
                payload_size = Varint.from_data(f)

                # Read key data
                key_record = f.read(payload_size.value)
                key, rowid = self._parse_key_record(key_record)

                keys.add(key)
                if self.search_key == key:
                    records.append((key, rowid))

                # Index entries are sorted, so we can stop if we've gone too far
                if key > self.search_key:
                    print(f"  Key {key!r} > search key {self.search_key!r}, stopping")
                    break

        return records

    def visit_interior(self, page: Page) -> list[tuple[bytes, int]] | None:
        keys = []
        rowids = []
        with self.database.reader() as f:
            for cell_pointer in page.cell_pointers:
                f.seek(self.database.page_size * page.page_number + cell_pointer)
                f.read(4)  # Skip child pointer
                payload_size = Varint.from_data(f).value
                key_record = f.read(payload_size)
                key, rowid = self._parse_key_record(key_record)
                keys.append(key)
                rowids.append(rowid)

        matches = []
        for key, rowid in zip(keys, rowids):
            if key == self.search_key:
                matches.append((key, rowid))
            elif key > self.search_key:
                break

        return matches

    def choose_paths(self, page: Page) -> list[int]:
        """
        Determine which child pages to follow by comparing keys.
        """
        print(f"Choosing paths in page {page.page_number}")
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
            print(
                f"  Search key {self.search_key!r} < first key {keys[0]!r}, using leftmost pointer"
            )
            return [0]
        if self.search_key > keys[-1]:
            print(
                f"  Search key {self.search_key!r} > last key {keys[-1]!r}, using rightmost pointer"
            )
            return [-1]

        # Now analyze the keys
        for i, key in enumerate(keys):
            if key > self.search_key:
                # Found a larger key after some matches
                print(f"  Found larger key {key!r} after matches")
                paths.append(i)
                found_larger = True
                break
            elif self.search_key == key:
                # Found a matching key - follow it and keep going
                print(f"  Found matching key at index {i}")
                paths.append(i)

        # If we haven't found a larger key, follow rightmost pointer
        if not found_larger:
            print("  No larger key found, using rightmost pointer")
            paths.append(-1)

        print(f"  Chosen paths: {paths}")
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
