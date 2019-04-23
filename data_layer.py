from contextlib import closing
import sqlite3


class DataLayer:
    """
    Data abstraction layer for interacting with key value pairs
    """

    def __init__(self, db):
        """
            :param db: name of the SQLite database
        """
        self.db = db

    def set_value(self, key, value, flags):
        """
        Sets or replaces 'key' with 'value' and also stores the associated
        'flags' value.

        :param key: The key to be stored (text)
        :param value: The value (binary) to be stored for this key
        :param flags: The flags (integer) to be stored for this key
        :return: nothing
        """
        with closing(sqlite3.connect(self.db)) as con:
            with con:
                # With newer versions of SQLite this can be handled more
                # elegantly in one statement using ON CONFLICT, but
                # unfortunately, Python 3.7 is bound to an older version of
                # SQLite.

                # First try to update the key if it exists
                con.execute("""
                    UPDATE KEY_VALUE_PAIRS 
                    SET VALUE = ?, FLAGS = ?
                    WHERE KEY = ?
                """, (value, flags, key)
                )

                # If they key did not exist, then insert a row for it
                con.execute("""
                    INSERT INTO KEY_VALUE_PAIRS (KEY, VALUE, FLAGS)
                    SELECT ?, ?, ?
                    WHERE (SELECT CHANGES() = 0)
                """, (key, value, flags))

    def get_all_values(self):
        """
        Fetch all key/value pairs from the database.

        :return: a generator which iterates over each key/value pair and
        returns a dictionary containing mapping the key, value and flags to
        their values.
        """
        with closing(sqlite3.connect(self.db)) as con:
            con.row_factory = sqlite3.Row
            with con:
                query = "SELECT * FROM KEY_VALUE_PAIRS"
                for row in con.execute(query):
                    yield DataLayer._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row):
        return {
            "key": row["key"],
            "value": row["value"],
            "flags": row["flags"],
        }