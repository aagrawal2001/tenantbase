from memcache_receiver import MemcacheFactory
from twisted.trial import unittest
from twisted.test import proto_helpers
from unittest import mock


class MemcacheReceiverTestCase(unittest.TestCase):
    def setUp(self):
        self.data_layer = mock.Mock()
        factory = MemcacheFactory(self.data_layer)
        self.proto = factory.buildProtocol(("127.0.0.1", 0))
        self.tr = proto_helpers.StringTransport()
        self.proto.makeConnection(self.tr)

    def _test_ascii_command(self, message, expected):
        self.proto.dataReceived(message.encode('ascii'))
        self.assertEqual(self.tr.value(), expected)

    def _test_binary_command(self, message, expected):
        self.proto.dataReceived(message)
        self.assertEqual(self.tr.value(), expected)

    def test_set_ascii(self):
        key = "foo"
        value = b"Hello World!"
        flags = 1024
        command = "set %s %d %d %d\r\n" % (key, flags, 0, len(value))
        command += "%s\r\n" % value.decode('ascii')
        self._test_ascii_command(command, b"STORED\r\n")
        self.data_layer.set_value.assert_called_once_with(key, value, flags)

    def test_set_binary(self):
        key = "foo"
        byte_array = bytearray([120, 3, 255, 0, 100])
        value = bytes(byte_array)
        flags = 1024
        command = \
            b"set %s %d %d %d\r\n" % \
            (key.encode('ascii'), flags, 0, len(value))
        command += b"%s\r\n" % value
        self._test_binary_command(command, b"STORED\r\n")
        self.data_layer.set_value.assert_called_once_with(key, value, flags)

    def test_set_noreply(self):
        key = "foo"
        value = b"Hello World!"
        flags = 1024
        command = "set %s %d %d %d noreply\r\n" % (key, flags, 0, len(value))
        command += "%s\r\n" % value.decode('ascii')
        self._test_ascii_command(command, b"")
        self.data_layer.set_value.assert_called_once_with(key, value, flags)

    def test_set_too_few_arguments(self):
        self._test_ascii_command(
            "set foo 1024 0\r\n",
            b"CLIENT_ERROR Incorrect number of arguments\r\n"
        )

    def test_set_too_many_arguments(self):
        self._test_ascii_command(
            "set foo 1024 0 11 noreply junk\r\n",
            b"CLIENT_ERROR Incorrect number of arguments\r\n"
        )

    def test_set_invalid_last_argument(self):
        self._test_ascii_command(
            "set foo 1024 0 11 noreplytypo\r\n",
            b"CLIENT_ERROR Invalid last argument - expected 'noreply'\r\n"
        )

    def test_unrecognized_command(self):
        self._test_ascii_command("blink foo bar\r\n", b"ERROR\r\n")

    def test_get_one_value(self):
        key = "foo"
        value = b"Value 1"
        flags = 64
        self.data_layer.get_values.return_value = [
            {"key": key, "value": value, "flags": flags}
        ]
        command = "get %s\r\n" % key
        expected_response = b"VALUE %s %d %d\r\n" % \
            (key.encode(), flags, len(value))
        expected_response += b"%s\r\n" % value
        expected_response += b"END\r\n"
        self._test_ascii_command(command, expected_response)
        self.data_layer.get_values.assert_called_once_with([key])

    def test_get_multiple_values_when_only_one_exists(self):
        key1, key2 = "foo", "bar"
        keys = [key1, key2]
        key2_data = {"key": key2, "value": b"Hello World!", "flags": 1024}
        self.data_layer.get_values.return_value = [key2_data]
        command = "get %s\r\n" % " ".join(keys)
        expected_response = b"VALUE %s %d %d\r\n" % \
            (key2.encode(), key2_data["flags"], len(key2_data["value"]))
        expected_response += b"%s\r\n" % key2_data["value"]
        expected_response += b"END\r\n"
        self._test_ascii_command(command, expected_response)
        self.data_layer.get_values.assert_called_once_with(keys)

    def test_get_multiple_values(self):
        keys = []
        key_data = []
        expected_response = b""
        for i in range(10):
            key = "key%d" % i
            keys.append(key)
            value = b"value %d" % i
            flags = i
            key_data.append({"key": key, "value": value, "flags": flags})
            expected_response += b"VALUE %s %d %d\r\n" % \
                                 (key.encode(), flags, len(value))
            expected_response += b"%s\r\n" % value
        expected_response += b"END\r\n"

        self.data_layer.get_values.return_value = key_data
        command = "get %s\r\n" % " ".join(keys)
        self._test_ascii_command(command, expected_response)
        self.data_layer.get_values.assert_called_once_with(keys)
