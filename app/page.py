from enum import Enum


class PageType(Enum):
    INTERIOR_INDEX_B_TREE = 0x02
    INTERIOR_TABLE_B_TREE = 0x05
    LEAF_INDEX_B_TREE = 0x0A
    LEAF_TABLE_B_TREE = 0x0D


class Page:
    def __init__(self, data: bytes, page_number: int):
        self.data = data
        self.page_number = page_number

        # The one-byte flag at offset 0 indicating the b-tree page type.
        self.type = PageType(data[0])

        # The b-tree page header is 8 bytes in size for
        # leaf pages and 12 bytes for interior pages.
        self.header_size = (
            8
            if self.type in (PageType.LEAF_TABLE_B_TREE, PageType.LEAF_INDEX_B_TREE)
            else 12
        )

        # The four-byte page number at offset 8 is the right-most pointer.
        # This value appears in the header of interior b-tree pages only
        # and is omitted from all other pages.
        self.rightmost_pointer = (
            int.from_bytes(data[8:12], byteorder="big")
            if self.type == PageType.INTERIOR_TABLE_B_TREE
            else None
        )

    @property
    def cell_count(self) -> int:
        # The two-byte integer at offset 3 gives the number of cells on the page.
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
