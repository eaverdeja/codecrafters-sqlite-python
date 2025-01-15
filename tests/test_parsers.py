from app.parsers import Varint, Record


class TestVarint:
    def test_should_parse_1_byte_varints(self):
        data = [0b01111111]
        varint = Varint.from_data(data)
        assert varint.value == 127
        assert varint.bytes_length == 1

        data = [0b01010101]
        varint = Varint.from_data(data)
        assert varint.value == 85
        assert varint.bytes_length == 1

    def test_should_parse_2_byte_varints(self):
        data = [0x81, 0x47]

        varint = Varint.from_data(data)
        assert varint.value == 199
        assert varint.bytes_length == 2


class TestRecord:
    def test_should_parse_record_data(self):
        record_data = b"\x07\x17\x1b\x1b\x01\x81\x47\x74\x61\x62\x6c\x65\x6f\x72\x61\x6e\x67\x65\x73\x6f\x72\x61\x6e\x67\x65\x73\x04\x43\x52\x45\x41\x54\x45\x20\x54\x41\x42\x4c\x45\x20\x6f\x72\x61\x6e\x67\x65\x73\x0a\x28\x0a\x09\x69\x64\x20\x69\x6e\x74\x65\x67\x65\x72\x20\x70\x72\x69\x6d\x61\x72\x79\x20\x6b\x65\x79\x20\x61\x75\x74\x6f\x69\x6e\x63\x72\x65\x6d\x65\x6e\x74\x2c\x0a\x09\x6e\x61\x6d\x65\x20\x74\x65\x78\x74\x2c\x0a\x09\x64\x65\x73\x63\x72\x69\x70\x74\x69\x6f\x6e\x20\x74\x65\x78\x74\x0a\x29\x50"
        record = Record.from_data(record_data)
        assert record.record_type == "table"
        assert record.name == "oranges"
        assert record.table_name == "oranges"
