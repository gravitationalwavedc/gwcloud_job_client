import asyncio

import aiounittest

from core.messaging.message import Message
from utils.packet_scheduler import PacketScheduler


class TestPacketScheduler(aiounittest.AsyncTestCase):
    def setUp(self):
        self.sent_data = []

    async def test_scheduler_packet_order(self):
        class MockScheduler:
            async def send(_self, data):
                self.sent_data.append(data)

        sched = PacketScheduler(MockScheduler())

        expected_data = []

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high1", priority=PacketScheduler.Priority.Highest)
            result.push_string("high1")
            expected_data.append(result.to_bytes())
            await sched.queue_message(result)

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high2", priority=PacketScheduler.Priority.Highest)
            result.push_string("high2")
            expected_data.append(result.to_bytes())
            await sched.queue_message(result)

        # Queue up 10 medium priority messages
        for _ in range(10):
            result = Message(5, source="medium", priority=PacketScheduler.Priority.Medium)
            result.push_string("medium")
            expected_data.append(result.to_bytes())
            await sched.queue_message(result)

        # Queue up 10 low priority messages
        for _ in range(10):
            result = Message(10, source="low", priority=PacketScheduler.Priority.Lowest)
            result.push_string("low")
            expected_data.append(result.to_bytes())
            await sched.queue_message(result)

        # Queue up 10 high priority messages
        for _ in range(10):
            result = Message(5, source="high3", priority=PacketScheduler.Priority.Highest)
            result.push_string("high3")
            expected_data.append(result.to_bytes())
            await sched.queue_message(result)

        # Queue up 10 medium priority messages
        for _ in range(10):
            result = Message(5, source="medium2", priority=PacketScheduler.Priority.Medium)
            result.push_string("medium2")
            expected_data.append(result.to_bytes())
            await sched.queue_message(result)

        # Start the scheduler
        sched.start()

        # Give the scheduler some time to send the packets
        await asyncio.sleep(0.1)

        # High priority data should be sent first in round robin
        for i in range(10):
            self.assertEqual(self.sent_data.pop(0), expected_data[0+i])
            self.assertEqual(self.sent_data.pop(0), expected_data[10+i])
            self.assertEqual(self.sent_data.pop(0), expected_data[40+i])

        # Next medium priority data should be sent in round robin
        for i in range(10):
            self.assertEqual(self.sent_data.pop(0), expected_data[20+i])
            self.assertEqual(self.sent_data.pop(0), expected_data[50+i])

        # Finally low priority data should be sent
        for i in range(10):
            self.assertEqual(self.sent_data.pop(0), expected_data[30+i])

        # Shut down the scheduler
        sched.stop()
