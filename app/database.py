from contextlib import contextmanager
from io import BufferedReader
from typing import Generator

from .page import Page


class Database:
    def __init__(self, path: str):
        self.path = path
        with self.reader() as f:
            self.page_size = self._get_page_size(f)

    def get_page(self, page_number: int) -> Page | None:
        with open(self.path, "rb") as f:
            # If we're on the first page, skip the file header
            offset = 100 if page_number == 0 else self.page_size * page_number
            f.seek(offset)

            data = f.read(self.page_size)
            if not data:
                # Probably the right-most pointer of a
                # page pointing to the EOF
                return None
            return Page(data, page_number)

    @contextmanager
    def reader(self) -> Generator[BufferedReader, None, None]:
        with open(self.path, "rb") as f:
            yield f

    def _get_page_size(self, f: BufferedReader) -> int:
        # Skip the first 16 bytes of the header
        f.seek(16)
        # The next 2 bytes represent the DB's page size
        return int.from_bytes(f.read(2), byteorder="big")
