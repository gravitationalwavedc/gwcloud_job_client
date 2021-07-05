import aiounittest

from core.messaging.message import Message
from utils.packet_scheduler import PacketScheduler


class TestPacketScheduler(aiounittest.AsyncTestCase):
    class MockScheduler:
        async def send(self, data):
            print(data)

    async def test_scheduler(self):
        sched = PacketScheduler(self.MockScheduler())
        sched.server_ready()

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high1", priority=PacketScheduler.Priority.Highest)
            result.push_string("high1")
            await sched.queue_message(result)

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high2", priority=PacketScheduler.Priority.Highest)
            result.push_string("high2")
            await sched.queue_message(result)

        # Queue up 10 medium priority messages
        for _ in range(10):
            result = Message(5, source="medium", priority=PacketScheduler.Priority.Medium)
            result.push_string("medium")
            await sched.queue_message(result)

        # Queue up 10 low priority messages
        for _ in range(10):
            result = Message(10, source="low", priority=PacketScheduler.Priority.Lowest)
            result.push_string("low")
            await sched.queue_message(result)

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high3", priority=PacketScheduler.Priority.Highest)
            result.push_string("high3")
            await sched.queue_message(result)

        # Queue up 10 medium priority messages
        for _ in range(10):
            result = Message(5, source="medium2", priority=PacketScheduler.Priority.Medium)
            result.push_string("medium2")
            await sched.queue_message(result)

