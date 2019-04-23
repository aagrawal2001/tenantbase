import sqlite3
from contextlib import closing

"""
Creates the SQLite database schema to store our key/value pairs. 

db is the name of the SQLite database file to use.

We don't store the expiration time for now. Only the key and the associated 
value and flags. 
"""


def create_schema(db):
    with closing(sqlite3.connect(db)) as con:
        con.execute("""
            CREATE TABLE KEY_VALUE_PAIRS(
                KEY     TEXT PRIMARY KEY, 
                VALUE   BLOB,
                FLAGS   INTEGER
            )"""
                    )
