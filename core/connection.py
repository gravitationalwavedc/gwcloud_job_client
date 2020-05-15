import asyncio
import logging
import sys
import traceback
from asyncio import sleep

import websockets

from utils.status import JobStatus
from settings import settings
from utils.packet_scheduler import PacketScheduler
from .check_job_status import check_job_status_thread, check_all_jobs
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

        return

        if msg_id == Message.CANCEL_JOB:
            raise Exception("Not implemented")
            # Get the ui id of the job
            # ui_id = msg.pop_uint()
            #
            # # Get the job from the database
            # job = Job.objects.get(ui_id=ui_id)
            #
            # # Verify that this job exists
            # if not job:
            #     logging.info("Attempt to cancel a job with UI ID {} That does not exist in the database.".format(ui_id))
            #     return
            #
            # # Get the scheduler for this job
            # scheduler = self.scheduler_klass(self.settings, job.ui_id, job.job_id)
            #
            # # Ask the scheduler to cancel the job
            # scheduler.cancel()
            #
            # # wait a few seconds then check the status of the job again
            # await sleep(60)
            #
            # # Check the job status in case it was cancelled immediately
            # await self.check_job_status(job, True)

        elif msg_id == Message.DELETE_JOB:
            raise Exception("Not implemented")
            # # Get the ui id of the job
            # ui_id = msg.pop_uint()
            #
            # # Get the scheduler for this job
            # scheduler = self.scheduler_klass(self.settings, ui_id, None)
            #
            # try:
            #     # Ask the scheduler to delete the job data
            #     scheduler.delete_data()
            #
            #     # Mark the job deleted
            #     result = Message(Message.UPDATE_JOB)
            #     result.push_uint(ui_id)
            #     result.push_uint(JobStatus.DELETED)
            #     result.push_string("All Job Data successfully deleted.")
            #     await self.sock.send(result.to_bytes())
            # except Exception as Exp:
            #     # An exception occurred, log the exception to the log
            #     logging.error("Error while deleting job data")
            #     logging.error(type(Exp))
            #     logging.error(Exp.args)
            #     logging.error(Exp)
            #
            #     # Also log the stack trace
            #     exc_type, exc_value, exc_traceback = sys.exc_info()
            #     lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            #     logging.error(''.join('!! ' + line for line in lines))

        else:
            logging.info("Got unknown message id {}".format(msg_id))

    async def send_handler(self):
        """
        Handles sending messages from the queue to the server

        :return: Nothing
        """
        # Automatically handles sending messages added to the queue
        while True:
            # Wait for a message from the queue
            message = await self.queue.get()
            # Send the message
            await self.sock.send(message)

    async def recv_handler(self):
        """
        Handles receiving messages from the client

        :return: Nothing
        """
        # Loop forever
        while True:
            # Wait for a message to arrive on the websocket
            message = await self.sock.recv()
            # Handle the message
            await self.handle_message(message)

    async def run(self):
        """
        Called to create a websocket connection to the server and manage incoming messages
        :return: Nothing
        """

        self.sock = await websockets.connect(
            '{}?token={}'.format(settings.HPC_WEBSOCKET_SERVER, self.argv[1]))

        self.scheduler = PacketScheduler(self.sock)

        # Create the consumer and producer tasks
        consumer_task = asyncio.ensure_future(self.recv_handler())
        producer_task = asyncio.ensure_future(self.send_handler())

        # Start the job status thread - this will run a job status check immediately
        asyncio.ensure_future(check_job_status_thread(self))

        # Wait for one of the tasks to finish
        done, pending = await asyncio.wait(
            [consumer_task, producer_task],
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
