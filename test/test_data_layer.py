import sqlite3
import tempfile
import unittest
from contextlib import closing
from data_layer import DataLayer
from schema import create_schema


class TestDataLayer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Sets up a temporary SQLite database
        cls.file = tempfile.NamedTemporaryFile()
        cls.db_name = cls.file.name
        create_schema(cls.db_name)

    @classmethod
    def tearDownClass(cls):
        cls.file.close()

    def setUp(self):
        self.data_layer = DataLayer(self.db_name)

    def tearDown(self):
        # Resets the database for each test
        with closing(sqlite3.connect(self.db_name)) as con:
            with con:
                con.execute("DELETE FROM KEY_VALUE_PAIRS")

    def test_set_ascii_data(self):
        key = "greeting"
        value = b"Hello World!"
        flags = 1543
        self.data_layer.set_value(key, value, flags)
        all_values = list(self.data_layer.get_all_values())
        self.assertEqual(
            all_values, [{"key": key, "value": value, "flags": flags}]
        )

    def test_set_binary_data(self):
        byte_array = bytearray([120, 3, 255, 0, 100])
        data = bytes(byte_array)
        key = "foo"
        flags = 100
        self.data_layer.set_value(key, data, flags)
        all_values = list(self.data_layer.get_all_values())
        self.assertEqual(
            all_values, [{"key": key, "value": data, "flags": flags}]
        )

    def test_set_data_override(self):
        key = "greeting"
        old_value, old_flags = b"Hello World!", 1543
        new_value, new_flags = b"Goodbye World!", 777
        self.data_layer.set_value(key, old_value, old_flags)
        self.data_layer.set_value(key, new_value, new_flags)
        all_values = list(self.data_layer.get_all_values())
        self.assertEqual(
            all_values, [{"key": key, "value": new_value, "flags": new_flags}]
        )

    def test_get_one_value_that_exists(self):
        key = "greeting"
        value = b"Hello World!"
        flags = 1543
        self.data_layer.set_value(key, value, flags)
        values = list(self.data_layer.get_values((key,)))
        self.assertEqual(
            values, [{"key": key, "value": value, "flags": flags}]
        )

    def test_get_one_value_that_does_not_exist(self):
        key = "greeting"
        value = b"Hello World!"
        flags = 1543
        self.data_layer.set_value("foo", value, flags)
        values = list(self.data_layer.get_values((key,)))
        self.assertEqual(values, [])

    def test_get_multiple_values_which_all_exist(self):
        expected_values = self.insert_records(10)
        keys = [v["key"] for v in expected_values]
        keys.reverse()
        values = list(self.data_layer.get_values(keys))
        self.assertListEqual(expected_values, values)

    def test_get_multiple_values_some_of_which_exist(self):
        expected_values = self.insert_records(10)
        keys = [v["key"] for v in expected_values]
        keys.append("foo")
        keys.append("bar")
        keys.append("baz")
        values = list(self.data_layer.get_values(keys))
        self.assertListEqual(expected_values, values)

    def test_get_all_values(self):
        expected_values = self.insert_records(10)
        all_values = list(self.data_layer.get_all_values())
        self.assertListEqual(expected_values, all_values)

    def insert_records(self, n):
        expected_values = []
        for i in range(n):
            key = "key%d" % i
            value = "val%d" % i * 2
            flags = i * 3
            self.data_layer.set_value(key, value, flags)
            expected_values.append(
                {"key": key, "value": value, "flags": flags}
            )
        return expected_values


if __name__ == '__main__':
    unittest.main()
