import unittest
from time import sleep

from core.messaging.message import Message
from utils.packet_scheduler import PacketScheduler


class TestPacketScheduler(unittest.TestCase):
    class MockScheduler:
        async def send(self, data):
            print(data)

    def test_scheduler(self):
        sched = PacketScheduler(self.MockScheduler())
        sched.server_ready()

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high1", priority=PacketScheduler.Priority.Highest)
            result.push_string("high1")
            sched.queue_message(result)

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high2", priority=PacketScheduler.Priority.Highest)
            result.push_string("high2")
            sched.queue_message(result)

        # Queue up 10 medium priority messages
        for _ in range(10):
            result = Message(5, source="medium", priority=PacketScheduler.Priority.Medium)
            result.push_string("medium")
            sched.queue_message(result)

        # Queue up 10 low priority messages
        for _ in range(10):
            result = Message(10, source="low", priority=PacketScheduler.Priority.Lowest)
            result.push_string("low")
            sched.queue_message(result)

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high3", priority=PacketScheduler.Priority.Highest)
            result.push_string("high3")
            sched.queue_message(result)

        # Queue up 10 medium priority messages
        for _ in range(10):
            result = Message(5, source="medium2", priority=PacketScheduler.Priority.Medium)
            result.push_string("medium2")
            sched.queue_message(result)

        sleep(5000)

