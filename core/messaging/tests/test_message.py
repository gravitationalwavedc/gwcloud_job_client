import math
import unittest

from core.messaging.message import Message
from utils.packet_scheduler import PacketScheduler


class TestMessage(unittest.TestCase):
    def setUp(self) -> None:
        self.msg = Message(msg_id=1234, source="test", priority=PacketScheduler.Priority.Medium)

    def test_constructor(self):
        test_data = self.msg.data

        # Check that we can't create a Message with neither a message id or data
        with self.assertRaises(Exception) as e:
            Message(msg_id=None, data=None)

        self.assertEqual(str(e.exception), "Can't create a message with neither a message id or initial data")

        # Check that we can't create a Message with both a message id and data
        with self.assertRaises(Exception) as e:
            Message(msg_id=1234, data=test_data)

        self.assertEqual(str(e.exception), "Can't create a message with both a message id and initial data")

        # Check providing the message id and source/priority works as expected
        with self.assertRaises(Exception) as e:
            Message(msg_id=1234)

        self.assertEqual(str(e.exception), "Source and priority were not provided")

        with self.assertRaises(Exception) as e:
            Message(msg_id=1234, source="test")

        self.assertEqual(str(e.exception), "Source and priority were not provided")

        with self.assertRaises(Exception) as e:
            Message(msg_id=1234, priority=PacketScheduler.Priority.Medium)

        self.assertEqual(str(e.exception), "Source and priority were not provided")

        def test_cb():
            pass

        msg = Message(msg_id=1234, source="test", priority=PacketScheduler.Priority.Medium, callback=test_cb)
        self.assertEqual(msg.data, test_data)
        self.assertEqual(msg.source, "test")
        self.assertEqual(msg.priority, PacketScheduler.Priority.Medium)
        self.assertEqual(msg.callback, test_cb)

        msg = Message(msg_id=1234, source="test", priority=PacketScheduler.Priority.Medium)
        self.assertEqual(msg.data, test_data)
        self.assertEqual(msg.source, "test")
        self.assertEqual(msg.priority, PacketScheduler.Priority.Medium)
        self.assertEqual(msg.callback, None)

        # Check that providing the data directly works as expected
        msg = Message(data=test_data)
        self.assertEqual(msg.data, test_data)
        self.assertEqual(msg.source, "test")
        self.assertEqual(msg.id, 1234)

    def test_bool(self):
        self.msg.push_bool(False)
        self.msg.push_bool(True)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_bool(), False)
        self.assertEqual(self.msg.pop_bool(), True)

    def test_ubyte(self):
        self.msg.push_ubyte(0xFF)
        self.msg.push_ubyte(0x12)
        with self.assertRaises(Exception):
            # Can't push a signed number
            self.msg.push_ubyte(-1)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_ubyte(), 0xFF)
        self.assertEqual(self.msg.pop_ubyte(), 0x12)

    def test_byte(self):
        self.msg.push_byte(0x7F)
        self.msg.push_byte(-1)
        with self.assertRaises(Exception):
            # Can't push a number bigger than 127
            self.msg.push_byte(0xFF)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_byte(), 0x7F)
        self.assertEqual(self.msg.pop_byte(), -1)

    def test_ushort(self):
        self.msg.push_ushort(0xFFFF)
        self.msg.push_ushort(0x1234)
        with self.assertRaises(Exception):
            # Can't push a signed number
            self.msg.push_ushort(-1)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_ushort(), 0xFFFF)
        self.assertEqual(self.msg.pop_ushort(), 0x1234)

    def test_short(self):
        self.msg.push_short(0x7FFF)
        self.msg.push_short(-1)
        with self.assertRaises(Exception):
            # Can't push a number bigger than 127
            self.msg.push_short(0xFFFF)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_short(), 0x7FFF)
        self.assertEqual(self.msg.pop_short(), -1)

    def test_uint(self):
        self.msg.push_uint(0xFFFFFFFF)
        self.msg.push_uint(0x12345678)
        with self.assertRaises(Exception):
            # Can't push a signed number
            self.msg.push_uint(-1)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_uint(), 0xFFFFFFFF)
        self.assertEqual(self.msg.pop_uint(), 0x12345678)

    def test_int(self):
        self.msg.push_int(0x7FFFFFFF)
        self.msg.push_int(-1)
        with self.assertRaises(Exception):
            # Can't push a number bigger than 127
            self.msg.push_int(0xFFFFFFFF)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_int(), 0x7FFFFFFF)
        self.assertEqual(self.msg.pop_int(), -1)

    def test_ulong(self):
        self.msg.push_ulong(0xFFFFFFFFFFFFFFFF)
        self.msg.push_ulong(0x12345678ABCDEF12)
        with self.assertRaises(Exception):
            # Can't push a signed number
            self.msg.push_ulong(-1)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_ulong(), 0xFFFFFFFFFFFFFFFF)
        self.assertEqual(self.msg.pop_ulong(), 0x12345678ABCDEF12)

    def test_long(self):
        self.msg.push_long(0x7FFFFFFFFFFFFFFF)
        self.msg.push_long(-1)
        with self.assertRaises(Exception):
            # Can't push a number bigger than 127
            self.msg.push_long(0xFFFFFFFFFFFFFFFF)

        self.msg = Message(data=self.msg.data)
        self.assertEqual(self.msg.pop_long(), 0x7FFFFFFFFFFFFFFF)
        self.assertEqual(self.msg.pop_long(), -1)

    def test_float(self):
        self.msg.push_float(0.12345678)
        self.msg.push_float(-0.12345678)
        self.msg.push_float(-1234578)

        self.msg = Message(data=self.msg.data)
        self.assertTrue(math.isclose(self.msg.pop_float(), 0.12345678, rel_tol=1e-07))
        self.assertTrue(math.isclose(self.msg.pop_float(), -0.12345678, rel_tol=1e-07))
        self.assertTrue(math.isclose(self.msg.pop_float(), -1234578, rel_tol=1e-07))

    def test_double(self):
        self.msg.push_double(0.1234567812345678)
        self.msg.push_double(-0.1234567812345678)
        self.msg.push_double(-123457812345678)

        self.msg = Message(data=self.msg.data)
        self.assertTrue(math.isclose(self.msg.pop_double(), 0.1234567812345678, rel_tol=1e-15))
        self.assertTrue(math.isclose(self.msg.pop_double(), -0.1234567812345678, rel_tol=1e-15))
        self.assertTrue(math.isclose(self.msg.pop_double(), -123457812345678, rel_tol=1e-15))

    def test_string(self):
        self.msg.push_string("test1")
        self.msg.push_string("test2")
        self.msg.push_string("test3")

        self.msg = Message(data=self.msg.data)
        self.assertTrue(self.msg.pop_string(), "test1")
        self.assertTrue(self.msg.pop_string(), "test2")
        self.assertTrue(self.msg.pop_string(), "test3")

    def test_bytes(self):
        self.msg.push_bytes(bytearray(b"test1"))
        self.msg.push_bytes(bytearray(b"test2"))
        self.msg.push_bytes(bytearray(b"test3"))

        self.msg = Message(data=self.msg.data)
        self.assertTrue(self.msg.pop_bytes(), bytearray(b"test1"))
        self.assertTrue(self.msg.pop_bytes(), bytearray(b"test2"))
        self.assertTrue(self.msg.pop_bytes(), bytearray(b"test3"))

    def test_tobytes(self):
        self.assertEqual(self.msg.to_bytes(), bytes(self.msg.data))

    def test_size(self):
        # Initial size should be string (ulong + len("test")) + uint = 16 bytes
        self.assertEqual(self.msg.size(), 16)

        self.msg.push_bool(True)
        self.assertEqual(self.msg.size(), 17)

        self.msg.push_byte(0x11)
        self.assertEqual(self.msg.size(), 18)

        self.msg.push_ubyte(0x11)
        self.assertEqual(self.msg.size(), 19)

        self.msg.push_short(0x1111)
        self.assertEqual(self.msg.size(), 21)

        self.msg.push_ushort(0x1111)
        self.assertEqual(self.msg.size(), 23)

        self.msg.push_int(0x11111111)
        self.assertEqual(self.msg.size(), 27)

        self.msg.push_uint(0x11111111)
        self.assertEqual(self.msg.size(), 31)

        self.msg.push_long(0x1111111111111111)
        self.assertEqual(self.msg.size(), 39)

        self.msg.push_ulong(0x1111111111111111)
        self.assertEqual(self.msg.size(), 47)

        self.msg.push_float(0.12345678)
        self.assertEqual(self.msg.size(), 51)

        self.msg.push_double(0.1234567812345678)
        self.assertEqual(self.msg.size(), 59)

        self.msg.push_string("Hello")
        self.assertEqual(self.msg.size(), 72)

        self.msg.push_bytes(b"Test")
        self.assertEqual(self.msg.size(), 84)

        msg = Message(data=self.msg.data)
        self.assertEqual(msg.pop_bool(), True)
        self.assertEqual(msg.pop_byte(), 0x11)
        self.assertEqual(msg.pop_ubyte(), 0x11)
        self.assertEqual(msg.pop_short(), 0x1111)
        self.assertEqual(msg.pop_ushort(), 0x1111)
        self.assertEqual(msg.pop_int(), 0x11111111)
        self.assertEqual(msg.pop_uint(), 0x11111111)
        self.assertEqual(msg.pop_long(), 0x1111111111111111)
        self.assertEqual(msg.pop_ulong(), 0x1111111111111111)
        self.assertTrue(math.isclose(msg.pop_float(), 0.12345678, rel_tol=1e-07))
        self.assertTrue(math.isclose(msg.pop_double(), 0.1234567812345678, rel_tol=1e-15))
        self.assertEqual(msg.pop_string(), "Hello")
        self.assertEqual(msg.pop_bytes(), b"Test")
