import asyncio
import importlib
import logging
import os
import pickle
import sys
import traceback
from asyncio import sleep
from threading import Thread

import websockets

from scheduler.status import JobStatus
from settings import settings
from utils.packet_scheduler import PacketScheduler
from .check_job_status import check_job_status_thread, check_all_jobs
from .db import get_job_by_ui_id, update_job, delete_job, get_all_jobs
from .handler import handle_message
from .messaging.message import Message
from .messaging.message_ids import UPDATE_JOB


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

        # Get the message id
        msg_id = msg.pop_uint()

        # Handle the message

        if msg_id == Message.HEARTBEAT_PING:
            result = Message(Message.HEARTBEAT_PONG)
            await self.send_assured_message(result, identifier)

        elif msg_id == Message.INITIATE_FILE_CONNECTION:
            # Create a new thread to handle a file connection
            Thread(target=create_file_connection, args=[msg.pop_string(), self.settings], daemon=True).start()

        elif msg_id == Message.SUBMIT_JOB:
            # Get the ui id of the job
            ui_id = msg.pop_uint()
            # Get the parameters for the job
            job_params = pickle.loads(msg.pop_bytes())

            # Check if this job has already been submitted by us
            job = await get_job_by_ui_id(ui_id)

            if job and job['job_id']:
                logging.info("Job with ui id {} has already been submitted, checking status...".format(ui_id))
                # If so, check the state of the job and notify the server of it's current state
                await self.check_job_status(job, True)




        elif msg_id == Message.TRANSMIT_ASSURED_RESPONSE_WEBSOCKET_MESSAGE:
            # Read the identifier
            response_identifier = msg.pop_string()
            # Handle the message
            await self.handle_message(msg.pop_bytes(), response_identifier)

        elif msg_id == Message.GET_FILE_TREE:
            # Get the UI ID of the job to get the files for
            ui_id = msg.pop_uint()
            # Get the path to find the get the files for
            path = msg.pop_string()
            # Check if we are getting a recursive tree
            is_recursive = msg.pop_bool()

            logging.info("Getting file list for job with ui id {}, path {}, recursive {}, identifier {}".format(
                ui_id, path, is_recursive, identifier))

            # Instantiate the scheduler
            scheduler = self.scheduler_klass(self.settings, ui_id, None)

            # Get the actual path on the filesystem for the files requested
            root_path = os.path.join(scheduler.get_working_directory(), path if len(path) and path[0] != os.sep else '')

            # Get the list of files requested
            file_list = []
            if is_recursive:
                # This is a recursive searh
                for root, dirnames, filenames in os.walk(root_path):
                    # Iterate over the directories
                    for item in dirnames:
                        # Construct the real path to this directory
                        real_file_name = os.path.join(root, item)
                        # Add the file entry
                        file_list.append({
                            # Remove the leading working directory
                            'path': real_file_name[len(scheduler.get_working_directory()):],
                            'is_dir': True,
                            'size': os.path.getsize(real_file_name)
                        })

                    for item in filenames:
                        # Construct the real path to this file
                        real_file_name = os.path.join(root, item)
                        # Add the file entry
                        try:
                            file_list.append({
                                # Remove the leading working directory
                                'path': real_file_name[len(scheduler.get_working_directory()):],
                                'is_dir': False,
                                'size': os.path.getsize(real_file_name)
                            })
                        except FileNotFoundError:
                            # Happens when trying to stat a symlink
                            pass
            else:
                # Not a recursive search
                for item in os.listdir(root_path):
                    # Construct the real path to this file/directory
                    real_file_name = os.path.join(root_path, item)
                    # Add the file entry
                    file_list.append({
                        # Remove the leading slash
                        'path': real_file_name[len(scheduler.get_working_directory()):],
                        'is_dir': os.path.isdir(real_file_name),
                        'size': os.path.getsize(real_file_name)
                    })

            # Build the return message
            result = Message(Message.GET_FILE_TREE)
            result.push_uint(len(file_list))
            for f in file_list:
                result.push_string(f['path'])
                result.push_bool(f['is_dir'])
                result.push_ulong(f['size'])

            # Send the message back to the server
            await self.send_assured_message(result, identifier)

        elif msg_id == Message.CANCEL_JOB:
            # Get the ui id of the job
            ui_id = msg.pop_uint()

            # Get the job from the database
            job = await get_job_by_ui_id(ui_id)

            # Verify that this job exists
            if not job:
                logging.info("Attempt to cancel a job with UI ID {} That does not exist in the database.".format(ui_id))
                return

            # Get the scheduler for this job
            scheduler = self.scheduler_klass(self.settings, job['ui_id'], job['job_id'])

            # Ask the scheduler to cancel the job
            scheduler.cancel()

            # wait a few seconds then check the status of the job again
            await sleep(5)

            # Check the job status in case it was cancelled immediately
            await self.check_job_status(job, True)

        elif msg_id == Message.DELETE_JOB:
            # Get the ui id of the job
            ui_id = msg.pop_uint()

            # Get the scheduler for this job
            scheduler = self.scheduler_klass(self.settings, ui_id, None)

            try:
                # Ask the scheduler to delete the job data
                scheduler.delete_data()

                # Mark the job deleted
                result = Message(Message.UPDATE_JOB)
                result.push_uint(ui_id)
                result.push_uint(JobStatus.DELETED)
                result.push_string("All Job Data successfully deleted.")
                await self.sock.send(result.to_bytes())
            except Exception as Exp:
                # An exception occurred, log the exception to the log
                logging.error("Error while deleting job data")
                logging.error(type(Exp))
                logging.error(Exp.args)
                logging.error(Exp)

                # Also log the stack trace
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logging.error(''.join('!! ' + line for line in lines))

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
            '{}/ws/?token={}'.format(settings.HPC_WEBSOCKET_SERVER, self.argv[1]))

        self.scheduler = PacketScheduler(self.sock)

        # Create the consumer and producer tasks
        consumer_task = asyncio.ensure_future(self.recv_handler())
        producer_task = asyncio.ensure_future(self.send_handler())

        # Start the job status thread
        asyncio.ensure_future(check_job_status_thread(self))

        # Run a job check
        await check_all_jobs(self)

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
