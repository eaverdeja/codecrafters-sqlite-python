from enum import Enum


class PageType(Enum):
    INTERIOR_INDEX_B_TREE = 0x02
    INTERIOR_TABLE_B_TREE = 0x05
    LEAF_INDEX_B_TREE = 0x0A
    LEAF_TABLE_B_TREE = 0x0D


class Page:
    def __init__(self, data: bytes):
        self.data = data

        # The one-byte flag at offset 0 indicating the b-tree page type.
        self.type = PageType(data[0])

        # The b-tree page header is 8 bytes in size for
        # leaf pages and 12 bytes for interior pages.
        self.header_size = (
            8
            if self.type in (PageType.LEAF_TABLE_B_TREE, PageType.LEAF_INDEX_B_TREE)
            else 12
        )

    @property
    def cell_count(self) -> int:
        # Leaf table b-tree pages have 8-byte sized headers,
        # with the cell count on offset 3, using a 2-byte integer
        return int.from_bytes(self.data[3:5], byteorder="big")

    @property
    def cell_pointers(self) -> list[int]:
        # The cell pointer array of a b-tree page immediately
        # follows the b-tree page header.
        #
        # It consists of K 2-byte integer offsets to the
        # cell contents, where K is the cell count.
        #
        # The cell pointers are arranged in key order with
        # the left-most cell (the cell with the smallest key) first
        # and the right-most cell (the cell with the largest key) last.
        pointers = []
        for i in range(self.cell_count):
            start = self.header_size + i * 2
            end = self.header_size + (i + 1) * 2
            pointer = int.from_bytes(
                self.data[start:end],
                byteorder="big",
            )
            pointers.append(pointer)
        return pointers
