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
