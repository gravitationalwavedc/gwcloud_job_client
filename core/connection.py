import asyncio

import websockets
import websockets.exceptions

from settings import settings
from utils.packet_scheduler import PacketScheduler
from .check_job_status import check_job_status_thread
from .handler import handle_message
from .messaging.message import Message


class JobController:

    def __init__(self, argv):
        self.argv = argv
        self.sock = None
        self.queue = asyncio.Queue()
        self.scheduler = None

    async def handle_message(self, message):
        """
        Handles an incoming websocket message

        :param message: The message to handle
        :return: Nothing
        """
        # Convert the raw message to a Message object
        msg = Message(data=message)

        # Handle the message
        await handle_message(self, msg)

    async def send_handler(self):
        """
        Handles sending messages from the queue to the server

        :return: Nothing
        """
        try:
            # Automatically handles sending messages added to the queue
            while True:
                # Wait for a message from the queue
                message = await self.queue.get()
                # Send the message
                await self.sock.send(message)
        except websockets.exceptions.ConnectionClosedOK:
            # Nothing to do, the websocket has been closed naturally
            pass
        except Exception:
            raise

    async def recv_handler(self):
        """
        Handles receiving messages from the client

        :return: Nothing
        """
        try:
            # Loop forever
            while True:
                # Wait for a message to arrive on the websocket
                message = await self.sock.recv()
                # Handle the message
                await self.handle_message(message)
        except websockets.exceptions.ConnectionClosedOK:
            # Nothing to do, the websocket has been closed naturally
            pass
        except Exception:
            raise

    async def run(self):
        """
        Called to create a websocket connection to the server and manage incoming messages
        :return: Nothing
        """

        self.sock = await websockets.connect(
            '{}?token={}'.format(settings.HPC_WEBSOCKET_SERVER, self.argv[1]),
            ping_interval=10,
            ping_timeout=10,
            close_timeout=1
        )

        self.scheduler = PacketScheduler(self.sock)

        # Create the consumer and producer tasks
        consumer_task = asyncio.ensure_future(self.recv_handler())
        producer_task = asyncio.ensure_future(self.send_handler())
        wait_closed_task = asyncio.ensure_future(self.sock.wait_closed())

        # Start the job status thread - this will run a job status check immediately
        asyncio.ensure_future(check_job_status_thread(self))

        # Wait for one of the tasks to finish
        done, pending = await asyncio.wait(
            [consumer_task, producer_task, wait_closed_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Kill the remaining tasks
        for task in pending:
            task.cancel()

    def execute(self):
        """
        Main entry point of the Job Controller - called by the daemon once it is initialised
        :return: Nothing
        """

        # Start the websocket connection
        asyncio.get_event_loop().run_until_complete(self.run())
