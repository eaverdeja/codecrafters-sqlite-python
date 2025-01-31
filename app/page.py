from enum import Enum
from typing import Generator, Self, TypeVar, Protocol

from app.varint import Varint

from .database import Database

T = TypeVar("T", covariant=True)


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
            if self.type
            in (PageType.INTERIOR_TABLE_B_TREE, PageType.INTERIOR_INDEX_B_TREE)
            else None
        )

    @classmethod
    def get_page(cls, database: Database, page_number: int) -> Self:
        with database.reader() as f:
            # If we're on the first page, skip the file header
            offset = 100 if page_number == 0 else database.page_size * page_number
            f.seek(offset)

            data = f.read(database.page_size)
            return cls(data, page_number)

    def get_child_pointer(self, cell_pointer: int) -> int:
        # The format of a cell depends on which kind of b-tree page the cell appears on...
        # For Table B-Tree Interior Cells, the first piece of information
        # is a 4-byte big-endian page number which is the left child pointer.
        if not self.type in (
            PageType.INTERIOR_TABLE_B_TREE,
            PageType.INTERIOR_INDEX_B_TREE,
        ):
            raise Exception("Trying to get child pointer on leaf page!")
        return int.from_bytes(self.data[cell_pointer : cell_pointer + 4])

    def get_row_id(self, cell_pointer: int) -> Varint:
        # On interior table pages, after the child pointer we have
        # a varint which is the integer key, aka "rowid" or "row_id"
        if self.type == PageType.INTERIOR_TABLE_B_TREE:
            return Varint.from_data(self.data[cell_pointer + 4 :])
        # For table leaf pages, we need to grab it after the record size
        if self.type == PageType.LEAF_TABLE_B_TREE:
            record_size = self.get_record_size(cell_pointer)
            return Varint.from_data(
                self.data[cell_pointer + record_size.bytes_length :]
            )
        else:
            raise Exception("Trying to get row_id on non-table page")

    def get_record_size(self, cell_pointer: int) -> Varint:
        # This is the first piece of information in cells for leaf pages
        if self.type not in (PageType.LEAF_TABLE_B_TREE, PageType.LEAF_INDEX_B_TREE):
            raise Exception("Only leaf cells have a record size")
        return Varint.from_data(self.data[cell_pointer:])

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


class BTreeWalker(Protocol[T]):
    def visit_leaf(self, page: Page) -> T:
        "Process a leaf page and return a result"
        pass

    def visit_interior(self, _: Page) -> T | None:
        "Optionally process an interior page"
        return None

    def choose_paths(self, page: Page) -> list[int]:
        pass


def walk_btree(
    page: Page,
    database: Database,
    walker: BTreeWalker[T],
    target_row_id: int | None = None,
) -> Generator[T, None, None]:
    # Process leaf nodes
    if page.type == PageType.LEAF_TABLE_B_TREE:
        yield walker.visit_leaf(page)
        return

    # Optionally process interior nodes
    result = walker.visit_interior(page)
    if result is not None:
        yield result

    if target_row_id:
        # Index scan

        # First we check the lower and upper bounds
        first_cell_pointer = page.cell_pointers[0]
        first_row_id = page.get_row_id(first_cell_pointer)

        last_cell_pointer = page.cell_pointers[-1]
        last_row_id = page.get_row_id(last_cell_pointer)

        # If our target is larger than the last rowid on this node,
        # we need to use the rightmost pointer
        if target_row_id > last_row_id.value:
            if page.rightmost_pointer:
                rightmost_page = Page.get_page(database, page.rightmost_pointer - 1)
                yield from walk_btree(rightmost_page, database, walker, target_row_id)
                return
        # If our target is smaller than the first rowid on this node,
        # we need to use the leftmost pointer
        elif target_row_id < first_row_id.value:
            left_child_pointer = page.get_child_pointer(first_cell_pointer)
            leftmost_page = Page.get_page(database, left_child_pointer - 1)
            yield from walk_btree(leftmost_page, database, walker, target_row_id)
            return
        else:
            # If our target is in between our bounds,
            # let's use binary search to find the
            # leaf page we're interested in
            left, right = 0, page.cell_count - 1
            next_page_number = -1

            while left <= right:
                mid = (left + right) // 2

                cell_pointer = page.cell_pointers[mid]
                row_id = page.get_row_id(cell_pointer)

                if target_row_id <= row_id.value:
                    right = mid - 1
                    left_child_pointer = page.get_child_pointer(cell_pointer)
                    next_page_number = left_child_pointer - 1
                else:
                    left = mid + 1
                    cell_pointer = page.cell_pointers[left]
                    right_child_pointer = page.get_child_pointer(cell_pointer)
                    next_page_number = right_child_pointer - 1

            next_page = Page.get_page(database, next_page_number)
            yield from walk_btree(next_page, database, walker, target_row_id)
    else:
        # Full-table scan

        # Traverse through all the left pointers
        for cell_pointer in page.cell_pointers:
            left_child_pointer = page.get_child_pointer(cell_pointer)
            child_page = Page.get_page(database, left_child_pointer - 1)
            yield from walk_btree(child_page, database, walker, target_row_id)

        # Finally, traverse through the right pointer
        if page.rightmost_pointer:
            rightmost_page = Page.get_page(database, page.rightmost_pointer - 1)
            yield from walk_btree(rightmost_page, database, walker, target_row_id)


def search_index(
    page: Page, database: Database, walker: BTreeWalker[T]
) -> Generator[T, None, None]:
    """
    Search a B-tree index structure.
    """
    result = None
    if page.type == PageType.LEAF_INDEX_B_TREE:
        result = walker.visit_leaf(page)
        yield result
        return

    if result := walker.visit_interior(page):
        yield result

    child_idxs = walker.choose_paths(page)

    for child_idx in child_idxs:
        if child_idx == -1:
            if not page.rightmost_pointer:
                raise ValueError("Expected rightmost pointer in interior index page")
            child_page = Page.get_page(database, page.rightmost_pointer - 1)
        else:
            cell_pointer = page.cell_pointers[child_idx]
            with database.reader() as database_file:
                database_file.seek(database.page_size * page.page_number + cell_pointer)
                left_child_pointer = int.from_bytes(
                    database_file.read(4), byteorder="big"
                )
            child_page = Page.get_page(database, left_child_pointer - 1)

        yield from search_index(child_page, database, walker)
