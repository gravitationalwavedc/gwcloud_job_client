import asyncio
import json
import logging
import os.path
import queue
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import patch

import aiohttp_xmlrpc.exceptions
import aiounittest
import psutil
import websockets
import websockets.exceptions

import client
from core.connection import JobController
from core.messaging.message import Message
from core.messaging.message_ids import SERVER_READY, SUBMIT_JOB
from utils.bundle.interface import check_or_start_bundle_server, run_bundle
from concurrent.futures import ThreadPoolExecutor

from utils.packet_scheduler import PacketScheduler

logging.getLogger("websockets").setLevel(logging.DEBUG)

class TestEndToEnd(unittest.TestCase):
    server_ready = threading.Event()
    server_terminate = threading.Event()
    cwd = os.path.dirname(os.path.abspath(__file__))
    python_path = None
    controller = JobController(['', 'not-real'])
    send_queue = queue.Queue()
    recv_queue = queue.Queue()

    @classmethod
    def setUpClass(cls):
        # Set up the venv and python symlinks
        bin_path = os.path.join(cls.cwd, 'test_bundle', 'venv', 'bin')

        activate_path = os.path.join(bin_path, 'activate')
        if os.path.exists(activate_path):
            os.remove(activate_path)

        cls.python_path = os.path.join(bin_path, 'python')
        if os.path.exists(cls.python_path):
            os.remove(cls.python_path)

        os.makedirs(bin_path, exist_ok=True)
        os.symlink(os.path.join(os.path.dirname(sys.executable), 'activate'), activate_path)
        os.symlink(os.path.join(os.path.dirname(sys.executable), 'python'), cls.python_path)

        cls.start_server()
        cls.connect_client()

        cls.cleanup()

    @classmethod
    def tearDownClass(cls):
        cls.server_terminate.set()

        # Kill any spawned python server processes
        for proc in psutil.process_iter():
            try:
                # Fetch process details as dict
                info = proc.as_dict(attrs=['pid', 'cmdline'])

                args = info['cmdline']
                has_python = any(filter(lambda x: 'python' in x, args))
                has_server = any(filter(lambda x: 'utils/bundle/server.py' in x, args))
                if has_python and has_server:
                    # Found a spawned server, terminate it
                    psutil.Process(info['pid']).terminate()
            except Exception:
                pass

        # Small delay to let the OS clean up
        time.sleep(0.5)

        cls.cleanup()

    @classmethod
    def cleanup(cls):
        if os.path.exists(os.path.join(cls.cwd, 'test_bundle', 'test_output')):
            os.remove(os.path.join(cls.cwd, 'test_bundle', 'test_output'))

        if os.path.exists(os.path.join(cls.cwd, 'test_bundle', 'test_extra.py')):
            os.remove(os.path.join(cls.cwd, 'test_bundle', 'test_extra.py'))

    @classmethod
    def server_thread(cls, loop):
        async def echo(websocket, path):
            async def send_handler():
                """
                Handles sending messages from the queue to the server

                :return: Nothing
                """
                try:
                    # Automatically handles sending messages added to the queue
                    while True:
                        while cls.send_queue.empty():
                            await asyncio.sleep(0.1)

                        while not cls.send_queue.empty():
                            # Wait for a message from the queue
                            message = cls.send_queue.get()

                            # Send the message
                            await websocket.send(message)
                except websockets.exceptions.ConnectionClosedOK:
                    # Nothing to do, the websocket has been closed naturally
                    pass
                except Exception:
                    raise

            async def recv_handler():
                """
                Handles receiving messages from the client

                :return: Nothing
                """
                try:
                    # Loop forever
                    while True:
                        # Wait for a message to arrive on the websocket
                        message = await websocket.recv()
                        # Handle the message
                        cls.recv_queue.put(message)
                except websockets.exceptions.ConnectionClosedOK:
                    # Nothing to do, the websocket has been closed naturally
                    pass
                except Exception:
                    raise

            consumer_task = asyncio.ensure_future(recv_handler())
            producer_task = asyncio.ensure_future(send_handler())

            msg = Message(SERVER_READY, source="system", priority=PacketScheduler.Priority.Highest)
            await websocket.send(msg.data)

            # Wait for one of the tasks to finish
            done, pending = await asyncio.wait(
                [consumer_task, producer_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Kill the remaining tasks
            for task in pending:
                task.cancel()

        async def main():
            async with websockets.serve(echo, "127.0.0.1", 8001):
                cls.server_ready.set()
                while not cls.server_terminate.is_set():
                    await asyncio.sleep(0.1)

        asyncio.set_event_loop(loop)
        asyncio.run(main())

    @classmethod
    def connect_client(cls):
        loop = asyncio.get_event_loop()
        def client_thread():
            asyncio.set_event_loop(loop)
            loop.run_until_complete(cls.controller.run())

        threading.Thread(target=client_thread).start()

    @classmethod
    def start_server(cls):
        loop = asyncio.get_event_loop()
        threading.Thread(target=cls.server_thread, args=(loop,)).start()
        cls.server_ready.wait()

    def setUp(self):
        from db.db.models import Job
        Job.objects.all().delete()

    @mock.patch('core.submit_job.get_bundle_path')
    def test_submit(self, get_bundle_path):
        get_bundle_path.return_value = self.cwd

        msg = Message(SUBMIT_JOB, source="system", priority=PacketScheduler.Priority.Highest)
        msg.push_uint(1234)
        msg.push_string("test_bundle")
        msg.push_string("params")
        self.send_queue.put(msg.data)

        msg = Message(data=self.recv_queue.get())
        print(msg.id)

        for i in range(100):
            msg = Message(SUBMIT_JOB, source="system", priority=PacketScheduler.Priority.Highest)
            msg.push_uint(1235+i)
            msg.push_string("test_bundle")
            msg.push_string("params")
            self.send_queue.put(msg.data)

        for _ in range(100):
            msg = Message(data=self.recv_queue.get())
            print(msg.id)

