from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver

import logging
logger = logging.getLogger(__name__)


class MemcacheReceiver(LineReceiver):
    # Constants for parsing commands
    CMD_SET = "set"
    CMD_GET = "get"
    CMD_DELETE = "delete"
    NO_REPLY = "noreply"

    # Certain commands are considered "storage" commands
    STORAGE_COMMANDS = frozenset([CMD_SET])

    # Successful response strings
    STORED = b"STORED"
    DELETED = b"DELETED"
    NOT_FOUND = b"NOT_FOUND"
    VALUE = b"VALUE %s %d %d"
    END = b"END"

    # Error strings
    UNKNOWN_COMMAND_ERROR = b"ERROR"
    CLIENT_ERROR = b"CLIENT_ERROR %s"
    INCORRECT_NUM_ARGUMENTS = b"Incorrect number of arguments"
    EXPECTED_NO_REPLY = b"Invalid last argument - expected 'noreply'"
    SERVER_ERROR = b"SERVER_ERROR %s"

    def __init__(self, data_layer):
        self.data_layer = data_layer

    def sendClientError(self, error):
        self.sendLine(self.CLIENT_ERROR % error)

    def doSet(self, args):
        logger.info("set {}".format(args))
        if len(args) > 5 or len(args) < 4:
            self.sendClientError(self.INCORRECT_NUM_ARGUMENTS)
            return

        self.no_reply = False

        # If there are 5 args, then the last one better be 'noreply'
        if len(args) == 5:
            if args[4] != self.NO_REPLY:
                self.sendClientError(self.EXPECTED_NO_REPLY)
                return
            self.no_reply = True
            args.pop()

        self.key = args[0]
        self.flags = int(args[1])
        self.exptime = int(args[2])
        self.bytes = int(args[3])
        self.buffer = []
        self.buffer_length = 0

        # The data being stored is binary, so switch to binary mode.
        self.setRawMode()

    def doGet(self, args):
        logger.info("get {}".format(args))
        for row in self.data_layer.get_values(args):
            value = row["value"]
            self.sendLine(
                self.VALUE %
                (row["key"].encode("ascii"), row["flags"], len(value))
            )
            self.sendLine(row["value"])
        self.sendLine(self.END)

    def doDelete(self, args):
        logger.info("delete {}".format(args))
        # delete expects exactly one or two arguments
        if len(args) > 2 or len(args) == 0:
            self.sendClientError(self.INCORRECT_NUM_ARGUMENTS)
            return

        key = args[0]
        no_reply = False

        # If there are two arguments, the last one has to be 'noreply'
        if len(args) == 2:
            if args[1] != self.NO_REPLY:
                self.sendClientError(self.EXPECTED_NO_REPLY)
                return
            no_reply = True

        # Now delete the data
        anything_deleted = self.data_layer.delete_value(key)
        if no_reply:
            return

        self.sendLine(
            self.DELETED if anything_deleted else self.NOT_FOUND
        )

    # Reads in the payload to be stored for the 'set' command
    def rawDataReceived(self, data):
        try:
            self.buffer.append(data)
            self.buffer_length += len(data)

            # Keep on reading data until the expected number of bytes have been
            # encountered
            if self.buffer_length >= self.bytes + 2:
                full_buffer = b"".join(self.buffer)
                value = full_buffer[:self.bytes]
                left_over = full_buffer[self.bytes + 2:]

                # Store the value in the database
                self.data_layer.set_value(self.key, value, self.flags)
                if not self.no_reply:
                    self.sendLine(self.STORED)

                # now switch back to parsing new commands
                self.setLineMode(left_over)
        except Exception as e:
            logger.exception(e)
            self.sendLine(self.SERVER_ERROR % repr(e).encode("ascii"))

    # Extensible set of commands
    COMMAND_MAP = {
        CMD_SET: doSet,
        CMD_GET: doGet,
        CMD_DELETE: doDelete,
    }

    # Parses commands
    def lineReceived(self, line):
        parsed_line = line.decode("ascii").split(" ")
        cmd = parsed_line[0]
        args = parsed_line[1:]

        # Unrecognized command - abort
        if cmd not in self.COMMAND_MAP:
            self.sendLine(self.UNKNOWN_COMMAND_ERROR)
            return

        try:
            self.COMMAND_MAP[cmd](self, args)
        except Exception as e:
            logger.exception(e)
            self.transport.write(self.SERVER_ERROR % repr(e).encode("ascii"))


class MemcacheFactory(Factory):
    def __init__(self, data_layer):
        super().__init__()
        self.data_layer = data_layer

    def buildProtocol(self, addr):
        return MemcacheReceiver(self.data_layer)
