import asyncio
import json
import os.path
import sys
import time
from datetime import datetime, timedelta
from unittest.mock import patch

import aiohttp_xmlrpc.exceptions
import aiounittest
import psutil

from utils.bundle.interface import check_or_start_bundle_server, run_bundle


class TestBundleInterface(aiounittest.AsyncTestCase):
    def setUp(self):
        self.cwd = os.path.dirname(os.path.abspath(__file__))

        # Set up the venv and python symlinks
        bin_path = os.path.join(self.cwd, 'test_bundle', 'venv', 'bin')

        activate_path = os.path.join(bin_path, 'activate')
        if os.path.exists(activate_path):
            os.remove(activate_path)

        self.python_path = os.path.join(bin_path, 'python')
        if os.path.exists(self.python_path):
            os.remove(self.python_path)

        os.makedirs(bin_path, exist_ok=True)
        os.symlink(os.path.join(os.path.dirname(sys.executable), 'activate'), activate_path)
        os.symlink(os.path.join(os.path.dirname(sys.executable), 'python'), self.python_path)

        self.cleanup()

    def tearDown(self):
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

        self.cleanup()

    def cleanup(self):
        if os.path.exists(os.path.join(self.cwd, 'test_bundle', 'test_output')):
            os.remove(os.path.join(self.cwd, 'test_bundle', 'test_output'))

        if os.path.exists(os.path.join(self.cwd, 'test_bundle', 'test_extra.py')):
            os.remove(os.path.join(self.cwd, 'test_bundle', 'test_extra.py'))

    @patch('asyncio.sleep')
    async def test_check_or_start_bundle_server_no_start(self, sleep_mock):
        # Check that a bundle that fails to start is reported correctly
        # Remove the symlink to the python interpreter so that the server never starts
        os.remove(self.python_path)

        with self.assertRaises(Exception) as e:
            await check_or_start_bundle_server(self.cwd, 'test_bundle')

        self.assertEqual(str(e.exception), "Unable to start the RPC server for bundle with hash test_bundle")

    async def call_bundle(self, output, bundle_function, bundle_path, bundle_hash, details, job_data):
        with open(os.path.join(self.cwd, 'test_bundle', 'test_output'), 'w') as f:
            json.dump(output, f)

        result = await run_bundle(bundle_function, bundle_path, bundle_hash, details, job_data)
        self.assertEqual(json.dumps(result['expected']), json.dumps(output))

        self.assertEqual(result['what'], bundle_function)
        self.assertDictEqual(result['details'], details)
        self.assertDictEqual(result['job_data'], job_data)

    async def test_bundle_functions(self):
        for fn in ['working_directory', 'submit', 'status', 'cancel', 'delete']:
            await self.call_bundle(1234, fn, self.cwd, 'test_bundle', {}, {})
            await self.call_bundle('1234', fn, self.cwd, 'test_bundle', {'test': 'hello'}, {'hello': 'test'})
            await self.call_bundle({'test': 'dict'}, fn, self.cwd, 'test_bundle', {'test': {'sub': 'dict'}}, {})

    async def test_bundle_reload(self):
        await self.call_bundle(1234, 'working_directory', self.cwd, 'test_bundle', {}, {})

        with open(os.path.join(self.cwd, 'test_bundle', 'test_extra.py'), 'w') as f:
            f.write("""
def working_directory(details, job_data):
    return "module_reloading_works"
            """)

        result = await run_bundle('working_directory', self.cwd, 'test_bundle', {}, {})
        self.assertEqual(result, "module_reloading_works")

        with open(os.path.join(self.cwd, 'test_bundle', 'test_extra.py'), 'w') as f:
            f.write("""
def working_directory(details, job_data):
    return "module_reloading_works2"
            """)

        result = await run_bundle('working_directory', self.cwd, 'test_bundle', {}, {})
        self.assertEqual(result, "module_reloading_works2")

    async def test_bundle_exception(self):
        with self.assertRaises(aiohttp_xmlrpc.exceptions.ApplicationError):
            await run_bundle('not_a_real_function', self.cwd, 'test_bundle', {}, {})

    async def test_bundle_non_blocking(self):
        # Here we're going to create an updated working directory function that has a 1 second delay in it. We'll
        # then call the bundle function 100 times and check that the time to complete is less than 10 seconds,
        # indicating that the calls ran in parallel

        with open(os.path.join(self.cwd, 'test_bundle', 'test_extra.py'), 'w') as f:
            f.write("""
def working_directory(details, job_data):
    import time
    time.sleep(1)
    return job_data['index']
                    """)

        tasks = [run_bundle('working_directory', self.cwd, 'test_bundle', {}, {'index': index}) for index in range(100)]

        now = datetime.now()

        results = await asyncio.gather(*tasks)

        self.assertEqual(len(results), len(tasks))

        for index, task in enumerate(results):
            self.assertEqual(task, index)

        duration = datetime.now() - now
        self.assertTrue(duration < timedelta(seconds=10))
