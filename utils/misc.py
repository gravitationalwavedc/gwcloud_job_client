import importlib
import inspect
import json
import logging
import os
import asyncio
from subprocess import list2cmdline

from settings import settings
from utils import shared_memory


def get_default_details():
    """
    Returns the default 'details' dictionary that is passed to the bundle.py file in each bundle

    :return: The default details dictionary
    """

    return {
        'cluster': settings.CLUSTER_NAME,
    }


def get_bundle_path():
    return os.path.join(os.path.dirname(__file__), "..", "bundles", "unpacked")


def get_bundle_loader_source(bundle_function, shm_name):
    """
    Constructs the python code that reads the data out of the shared memory region and then
    imports the bundle.py file and calls the specified function

    :param bundle_function: The name of the function to call from bundle.py
    :param shm_name: The name of the shared memory region to read data from

    :return: The python source
    """
    # Get the source code of the shared_memory module
    source = inspect.getsource(shared_memory)

    # Append the bundle loading code
    source += f"""
import sys
import struct
import json

shm = SharedMemory(name="{shm_name}")
data = shm.buf.tobytes()

data = data.decode("utf-8")

data = json.loads(data)

import bundle
result = bundle.{bundle_function}(data['details'], data['job_data'])

print(result)
    """

    return source


async def run_bundle(bundle_function, bundle_path, bundle_hash, details, job_data):
    """
    Calls a function from a specified bundle.py file with the provided data

    :param job_data: The job parameters to be passed
    :param details: Extra job details to be passed
    :param bundle_function: The function to call
    :param bundle_path: The path to the unpacked bundles
    :param bundle_hash: The hash of the bundle to call

    :return:
    """
    # Create a dict to serialize
    data = {
        'details': details,
        'job_data': job_data
    }

    data = json.dumps(data)

    # Create the shared memory object
    shm = shared_memory.SharedMemory(create=True, size=len(data))

    # Write the data in to the shared memory
    data = bytearray(data.encode('utf-8'))
    shm.buf[0:len(data)] = data

    # Get the loader source
    source = get_bundle_loader_source(bundle_function, shm.name)

    # Attempt to call the function from the bundle
    args = list2cmdline([os.path.join(bundle_path, bundle_hash, 'venv', 'bin', 'python'), '-c', source])
    p = await asyncio.create_subprocess_shell(
        args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=os.path.join(bundle_path, bundle_hash)
    )

    # Get the stdout and stderr output from the command
    out, err = await p.communicate()

    # Close the shared memory area
    shm.unlink()

    out = out.decode('utf-8')
    err = err.decode('utf-8')

    # Log the command and output
    logging.info("Running bundle command submit for " + bundle_hash)
    logging.info("Gave output:")
    logging.info("stdout: " + out)
    logging.info("stderr: " + err)

    # Get the last line of output
    lines = out.splitlines()
    result = out.splitlines()[-1] if len(lines) else None

    # Return the result and exit success
    return result, p.returncode == 0


def get_scheduler_instance():
    """
    Returns the class specified by the HPC_SCHEDULER_CLASS setting

    :return: The Class identified by HPC_SCHEDULER_CLASS
    """
    # Split the class path by full stops
    class_bits = settings.HPC_SCHEDULER_CLASS.split('.')

    # Import and return the class
    return getattr(importlib.import_module('.'.join(class_bits[:-1])), class_bits[-1])
