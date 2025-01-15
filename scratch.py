from app.serial_type import SQLiteSerialType


# Predefined type
print(SQLiteSerialType.INT64.description)  # "64-bit integer"
print(SQLiteSerialType.INT64.bytes_length)  # 8

desc, length = SQLiteSerialType.decode(7)
# Dynamic text type (5 bytes)
desc, length = SQLiteSerialType.decode(23)  # 13 + (5 * 2)
print(f"{desc}, Length: {length}")  # "TEXT value (5 bytes), Length: 5"

# Dynamic blob type (3 bytes)
desc, length = SQLiteSerialType.decode(18)  # 12 + (3 * 2)
print(f"{desc}, Length: {length}")  # "BLOB value (3 bytes), Length: 3"
