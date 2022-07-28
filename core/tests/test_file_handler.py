import os
import random
import string
from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory, NamedTemporaryFile
from unittest.mock import patch

from asgiref.sync import sync_to_async
from django.test import TransactionTestCase

import client  # noqa
from core.file_handler import download_file, get_file_list
from core.messaging.message import Message
from core.messaging.message_ids import FILE_ERROR, FILE_DETAILS, FILE_CHUNK, FILE_LIST_ERROR, FILE_LIST
from db.db.models import Job


class TestScheduler:
    def __init__(self, queue):
        self.queue = queue

    async def queue_message(self, msg):
        self.queue.put(msg)

        if msg.callback:
            msg.callback()


class TestConnection:
    def __init__(self):
        self.queue = Queue()

        self.scheduler = TestScheduler(self.queue)


def transpose_message(msg):
    return Message(data=msg.data)


class TestFileHandler(TransactionTestCase):
    def setUp(self) -> None:
        self.con = TestConnection()

        self.temp_dir = TemporaryDirectory()
        self.temp_file = NamedTemporaryFile(dir=self.temp_dir.name)

        self.job = Job.objects.create(
            job_id=1234,
            bundle_hash='my_hash',
            working_directory=self.temp_dir.name
        )

        self.msg = Message(msg_id=0, source='', priority=0)

        self.file_data = bytearray(''.join(random.choice(string.printable) for _ in range(1024)), 'utf-8')

        with Path(self.temp_file.name).open('wb') as f:
            f.write(self.file_data)
            f.flush()

    async def test_download_file_job_not_exist(self):
        self.msg.push_int(self.job.job_id + 1000)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(Path(self.temp_file.name).name)
        await download_file(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'Job does not exist')

    async def test_download_file_job_submitting(self):
        self.job.submitting = True
        await sync_to_async(self.job.save)()

        self.msg.push_int(self.job.job_id)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(Path(self.temp_file.name).name)

        await download_file(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'Job is not submitted')

    async def test_download_file_job_outside_working_directory(self):
        self.job.working_directory = '/not/a/real/path'
        await sync_to_async(self.job.save)()

        self.msg.push_int(self.job.job_id)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string("../" + Path(self.temp_file.name).name)

        await download_file(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'File does not exist')

    async def test_download_file_job_file_not_exist(self):
        self.msg.push_int(self.job.job_id)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string("not_a_real_file")

        await download_file(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'File does not exist')

    async def test_download_file_job_file_is_a_directory(self):
        with TemporaryDirectory(dir=self.temp_dir.name) as nested_temp_dir:
            self.msg.push_int(self.job.job_id)
            self.msg.push_string('some_uuid')
            self.msg.push_string('some_bundle_hash')
            self.msg.push_string(Path(nested_temp_dir).name)

            await download_file(self.con, transpose_message(self.msg))

            self.msg = transpose_message(self.con.queue.get())
            self.assertEqual(self.msg.id, FILE_ERROR)
            self.assertEqual(self.msg.pop_string(), 'some_uuid')
            self.assertEqual(self.msg.pop_string(), 'File does not exist')

    async def test_download_file_job_success(self):
        self.msg.push_int(self.job.job_id)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(Path(self.temp_file.name).name)
        await download_file(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_DETAILS)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_ulong(), os.path.getsize(self.temp_file.name))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_CHUNK)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_bytes(), self.file_data)

    @patch("core.file_handler.run_bundle")
    async def test_download_file_no_job_outside_working_directory(self, run_bundle):
        run_bundle.return_value = self.temp_dir.name

        self.job.working_directory = '/not/a/real/path'
        await sync_to_async(self.job.save)()

        self.msg.push_int(0)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string("../" + Path(self.temp_file.name).name)

        await download_file(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'File does not exist')

    @patch("core.file_handler.run_bundle")
    async def test_download_file_no_job_file_not_exist(self, run_bundle):
        run_bundle.return_value = self.temp_dir.name

        self.msg.push_int(0)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string("not_a_real_file")

        await download_file(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'File does not exist')

    @patch("core.file_handler.run_bundle")
    async def test_download_file_no_job_file_is_a_directory(self, run_bundle):
        run_bundle.return_value = self.temp_dir.name

        with TemporaryDirectory(dir=self.temp_dir.name) as nested_temp_dir:
            self.msg.push_int(0)
            self.msg.push_string('some_uuid')
            self.msg.push_string('some_bundle_hash')
            self.msg.push_string(Path(nested_temp_dir).name)

            await download_file(self.con, transpose_message(self.msg))

            self.msg = transpose_message(self.con.queue.get())
            self.assertEqual(self.msg.id, FILE_ERROR)
            self.assertEqual(self.msg.pop_string(), 'some_uuid')
            self.assertEqual(self.msg.pop_string(), 'File does not exist')

    @patch("core.file_handler.run_bundle")
    async def test_download_file_no_job_success(self, run_bundle):
        run_bundle.return_value = self.temp_dir.name

        self.msg.push_int(0)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(Path(self.temp_file.name).name)
        await download_file(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_DETAILS)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_ulong(), os.path.getsize(self.temp_file.name))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_CHUNK)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_bytes(), self.file_data)

    async def test_get_file_list_job_not_exist(self):
        self.msg.push_int(self.job.job_id + 1000)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(self.temp_dir.name)
        self.msg.push_bool(True)
        await get_file_list(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_LIST_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'Job does not exist')

    async def test_get_file_list_job_submitting(self):
        self.job.submitting = True
        await sync_to_async(self.job.save)()

        self.msg.push_int(self.job.job_id)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(self.temp_dir.name)
        self.msg.push_bool(True)
        await get_file_list(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_LIST_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'Job is not submitted')

    async def test_get_file_list_job_outside_working_directory(self):
        self.job.working_directory = '/not/a/real/path'
        await sync_to_async(self.job.save)()

        self.msg.push_int(self.job.job_id)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string('../' + self.temp_dir.name)
        self.msg.push_bool(True)

        await get_file_list(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_LIST_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'Path does not exist')

    async def test_get_file_list_job_directory_not_exist(self):
        self.msg.push_int(self.job.job_id)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(self.temp_dir.name + '/not/real/')
        self.msg.push_bool(True)

        await get_file_list(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_LIST_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'Path does not exist')

    async def test_get_file_list_job_directory_is_a_file(self):
        with NamedTemporaryFile(dir=self.temp_dir.name) as nested_temp_file:
            self.msg.push_int(self.job.job_id)
            self.msg.push_string('some_uuid')
            self.msg.push_string('some_bundle_hash')
            self.msg.push_string(Path(nested_temp_file.name).name)
            self.msg.push_bool(True)

            await get_file_list(self.con, transpose_message(self.msg))

            self.msg = transpose_message(self.con.queue.get())
            self.assertEqual(self.msg.id, FILE_LIST_ERROR)
            self.assertEqual(self.msg.pop_string(), 'some_uuid')
            self.assertEqual(self.msg.pop_string(), 'Path does not exist')

    async def test_get_file_list_job_success(self):
        self.msg.push_int(self.job.job_id)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(self.temp_dir.name)
        self.msg.push_bool(True)
        await get_file_list(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_LIST)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')

        self.assertEqual(self.msg.pop_uint(), 1)
        self.assertEqual(self.msg.pop_string(), '/' + Path(self.temp_file.name).name)
        self.assertEqual(self.msg.pop_bool(), False)
        self.assertEqual(self.msg.pop_ulong(), os.path.getsize(self.temp_file.name))

    @patch("core.file_handler.run_bundle")
    async def test_get_file_list_no_job_outside_working_directory(self, run_bundle):
        run_bundle.return_value = self.temp_dir.name

        self.msg.push_int(0)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string('../not/a/real/path')
        self.msg.push_bool(True)

        await get_file_list(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_LIST_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'Path does not exist')

    @patch("core.file_handler.run_bundle")
    async def test_get_file_list_no_job_directory_not_exist(self, run_bundle):
        run_bundle.return_value = self.temp_dir.name
        self.msg.push_int(0)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string('not/a/real/path/')
        self.msg.push_bool(True)

        await get_file_list(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_LIST_ERROR)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')
        self.assertEqual(self.msg.pop_string(), 'Path does not exist')

    @patch("core.file_handler.run_bundle")
    async def test_get_file_list_no_job_directory_is_a_file(self, run_bundle):
        run_bundle.return_value = self.temp_dir.name

        with NamedTemporaryFile(dir=self.temp_dir.name) as nested_temp_file:
            self.msg.push_int(0)
            self.msg.push_string('some_uuid')
            self.msg.push_string('some_bundle_hash')
            self.msg.push_string(Path(nested_temp_file.name).name)
            self.msg.push_bool(True)

            await get_file_list(self.con, transpose_message(self.msg))

            self.msg = transpose_message(self.con.queue.get())
            self.assertEqual(self.msg.id, FILE_LIST_ERROR)
            self.assertEqual(self.msg.pop_string(), 'some_uuid')
            self.assertEqual(self.msg.pop_string(), 'Path does not exist')

    @patch("core.file_handler.run_bundle")
    async def test_get_file_list_no_job_success(self, run_bundle):
        run_bundle.return_value = self.temp_dir.name

        self.msg.push_int(0)
        self.msg.push_string('some_uuid')
        self.msg.push_string('some_bundle_hash')
        self.msg.push_string(self.temp_dir.name)
        self.msg.push_bool(True)
        await get_file_list(self.con, transpose_message(self.msg))

        self.msg = transpose_message(self.con.queue.get())
        self.assertEqual(self.msg.id, FILE_LIST)
        self.assertEqual(self.msg.pop_string(), 'some_uuid')

        self.assertEqual(self.msg.pop_uint(), 1)
        self.assertEqual(self.msg.pop_string(), '/' + Path(self.temp_file.name).name)
        self.assertEqual(self.msg.pop_bool(), False)
        self.assertEqual(self.msg.pop_ulong(), os.path.getsize(self.temp_file.name))
