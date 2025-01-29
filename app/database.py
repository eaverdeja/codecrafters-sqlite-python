from contextlib import contextmanager
from io import BufferedReader
from typing import Generator


class Database:
    def __init__(self, path: str):
        self.path = path
        with self.reader() as f:
            self.page_size = self._get_page_size(f)

    @contextmanager
    def reader(self) -> Generator[BufferedReader, None, None]:
        with open(self.path, "rb") as f:
            yield f

    def _get_page_size(self, f: BufferedReader) -> int:
        # Skip the first 16 bytes of the header
        f.seek(16)
        # The next 2 bytes represent the DB's page size
        return int.from_bytes(f.read(2), byteorder="big")
