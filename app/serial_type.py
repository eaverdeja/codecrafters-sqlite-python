from enum import Enum
from typing import Optional


class SQLiteSerialType(Enum):
    # Predefined types
    NULL = (0, "NULL value", 0)
    INT8 = (1, "8-bit integer", 1)
    INT16 = (2, "16-bit integer", 2)
    INT24 = (3, "24-bit integer", 3)
    INT32 = (4, "32-bit integer", 4)
    INT48 = (5, "48-bit integer", 6)
    INT64 = (6, "64-bit integer", 8)
    FLOAT64 = (7, "64-bit float", 8)
    INT_0 = (8, "Integer value 0", 0)
    INT_1 = (9, "Integer value 1", 0)

    def __init__(self, code: int, description: str, bytes_length: Optional[int] = None):
        self.code = code
        self.description = description
        self.bytes_length = bytes_length

    @staticmethod
    def decode(code: int) -> tuple[str, int]:
        """Returns (description, bytes_length) for a given type code"""
        # Handle predefined types
        for member in SQLiteSerialType:
            if member.code == code:
                return member.description, member.bytes_length

        # Handle dynamic types
        if code >= 12:
            length = (code - 12) // 2
            if code % 2 == 0:
                return f"BLOB value ({length} bytes)", length
            else:
                return f"TEXT value ({length} bytes)", length

        raise ValueError(f"Invalid serial type code: {code}")

    @property
    def is_blob(self) -> bool:
        return self.code >= 12 and self.code % 2 == 0

    @property
    def is_text(self) -> bool:
        return self.code >= 13 and self.code % 2 == 1
