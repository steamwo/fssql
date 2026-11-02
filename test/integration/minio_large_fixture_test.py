"""验证结果编码与抽样参考值的跨语言边界。"""

import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location("large_fixture", Path(__file__).with_name("minio_large_fixture.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class LargeFixtureContractTest(unittest.TestCase):
    """以独立字面值校验 NULL、宽整数、UTF-8 和无序结果比较。"""

    def test_canonical_row_types(self):
        """保证 NULL 与文本、布尔与整数不会得到相同编码。"""
        self.assertEqual(fixture.canonical_row((None, True, 1, 2**80, -0.5, "é")),
                         b"N;B1;I1;I1208925819614629174706176;F-0.5;S2:\xc3\xa9;\n")

    def test_unordered_reference_is_order_independent(self):
        """确认未排序 SQL 仅比较多重集合，而显式排序的结果必须保持顺序。"""
        with fixture.duckdb.connect() as conn:
            left = fixture.reference_digest(conn, "SELECT * FROM (VALUES (1), (2), (2))", False)
            right = fixture.reference_digest(conn, "SELECT * FROM (VALUES (2), (1), (2))", False)
            self.assertEqual(left, right)
            left = fixture.reference_digest(conn, "SELECT * FROM (VALUES (1), (2))", True)
            right = fixture.reference_digest(conn, "SELECT * FROM (VALUES (2), (1))", True)
            self.assertNotEqual(left, right)

    def test_reservoir_small_stream(self):
        """当流长度等于容量时，抽样必须保留全部有序输入。"""
        import hashlib
        expected = hashlib.sha256(b"I0;\nI1;\nI2;\n").hexdigest()
        self.assertEqual(fixture.reservoir_reference(3, size=3), expected)


if __name__ == "__main__":
    unittest.main()
