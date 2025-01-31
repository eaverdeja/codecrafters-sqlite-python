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
        # print(f"\nLooking for rowid {target_row_id}")
        # Binary search to find leaf pages
        left, right = 0, page.cell_count - 1

        first_cell_pointer = page.cell_pointers[0]
        first_row_id = Varint.from_data(page.data[first_cell_pointer + 4 :])
        # print(f"First row id: {first_row_id.value}")

        last_cell_pointer = page.cell_pointers[-1]
        last_row_id = Varint.from_data(page.data[last_cell_pointer + 4 :])
        # print(f"Last row id: {last_row_id.value}")

        if target_row_id > last_row_id.value:
            print(f"{target_row_id} > {last_row_id.value}, using rightmost pointer")
            if page.rightmost_pointer:
                rightmost_page = Page.get_page(database, page.rightmost_pointer - 1)
                yield from walk_btree(rightmost_page, database, walker, target_row_id)
        elif target_row_id < first_row_id.value:
            print(f"{target_row_id} < {first_row_id.value}, using leftmost pointer")
            left_child_pointer = int.from_bytes(
                page.data[first_cell_pointer : first_cell_pointer + 4]
            )
            leftmost_page = Page.get_page(database, left_child_pointer - 1)
            yield from walk_btree(leftmost_page, database, walker, target_row_id)
        else:
            next_page_number = -1
            while left <= right:
                mid = (left + right) // 2

                cell_pointer = page.cell_pointers[mid]
                row_id = Varint.from_data(page.data[cell_pointer + 4 :])

                # print(f"Found rowid {row_id.value} in position {mid}")
                if target_row_id <= row_id.value:
                    # print(f"{target_row_id} < {row_id.value}, looking in left")
                    right = mid - 1
                    left_child_pointer = int.from_bytes(
                        page.data[cell_pointer : cell_pointer + 4]
                    )
                    next_page_number = left_child_pointer - 1
                else:
                    # print(f"{target_row_id} > {row_id.value}, looking in right")
                    left = mid + 1
                    assert page.rightmost_pointer is not None
                    next_page_number = page.rightmost_pointer - 1
                next_page = Page.get_page(database, next_page_number)
                yield from walk_btree(next_page, database, walker, target_row_id)
    else:
        # Full-table scan
        # Traverse through all the left pointers
        for cell_pointer in page.cell_pointers:
            # The format of a cell depends on which kind of b-tree page the cell appears on...
            # For Table B-Tree Interior Cells, the first piece of information
            # is a 4-byte big-endian page number which is the left child pointer.
            left_child_pointer = int.from_bytes(
                page.data[cell_pointer : cell_pointer + 4], byteorder="big"
            )
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
    # print(f"\nSearching page {page.page_number} (type: {page.type})")

    result = None
    if page.type == PageType.LEAF_INDEX_B_TREE:
        result = walker.visit_leaf(page)
        # print(f"Found {len(result)} matches in leaf page {page.page_number}")
        yield result
        return

    if result := walker.visit_interior(page):
        # print(f"Found {len(result)} matches in interior page {page.page_number}")
        yield result

    child_idxs = walker.choose_paths(page)

    # print(f"Following paths {child_idxs} in page {page.page_number}")

    for child_idx in child_idxs:
        if child_idx == -1:
            if not page.rightmost_pointer:
                raise ValueError("Expected rightmost pointer in interior index page")
            # print(
            # f"\nFollowing rightmost pointer ({page.rightmost_pointer-1}) from page {page.page_number}"
            # )
            child_page = Page.get_page(database, page.rightmost_pointer - 1)
        else:
            cell_pointer = page.cell_pointers[child_idx]
            with database.reader() as database_file:
                database_file.seek(database.page_size * page.page_number + cell_pointer)
                left_child_pointer = int.from_bytes(
                    database_file.read(4), byteorder="big"
                )
            # print(
            #     f"\nFollowing child pointer {left_child_pointer-1} at index {child_idx} from page {page.page_number}"
            # )
            child_page = Page.get_page(database, left_child_pointer - 1)

        yield from search_index(child_page, database, walker)
