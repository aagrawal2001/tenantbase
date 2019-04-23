import sqlite3
from contextlib import closing


def create_schema(db):
    """
    Creates the SQLite database schema to store our key/value pairs.

    We don't store the expiration time for now. Only the key and the associated
    value and flags.

    :param db: name of the SQLite database file to use.
    :return: nothing
    """
    with closing(sqlite3.connect(db)) as con:
        con.execute("""
            CREATE TABLE KEY_VALUE_PAIRS(
                KEY     TEXT PRIMARY KEY,
                VALUE   BLOB,
                FLAGS   INTEGER
            )"""
                    )
