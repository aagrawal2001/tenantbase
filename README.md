# TenantBase Take Home Assignment

Implements a key-value storage server that speaks a small subset of the 
memcached protocol and persists data in SQLite. It also has a command
line interface for seeing the cached content.

## Installation


```sh
python main.py install <sqlite-database>
```

## Usage example

To start the server, run:

```sh
python main.py serve <sqlite-database>
```

To view all the keys, run:

```sh
python main.py show <sqlite-database>
```

## Implementation Notes

1. This implementation assumes all keys are ASCII. In fact, it assumes the command lines
are in ASCII. In order to handle arbitrary binary data, the database schema would have to be
changed so the 'key' column is a binary BLOB and the parsing code would have to be changed
to parse the binary values. However, this does not seem to be the intent of the original
Memcache [specification](https://github.com/memcached/memcached/blob/master/doc/protocol.txt) 
because it explicitly refers to keys as containing characters. It seems to be assuming an 
encoding of the data and ASCII seems like the most logical choice.

2. With newer versions of SQLite, the set value operation can be greatly simplified by using
one SQL statement by making use of ON CONFLICT UPDATE.

3. This implementation makes use of [Twisted](https://twistedmatrix.com) to launch the server
and handle the socket calls for accepting connections, reading and writing data. 

## Meta

Andy Agrawal – aagrawal2001@gmail.com

Distributed under the GPL v3 license. See ``LICENSE`` for more information.