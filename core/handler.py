import asyncio
import importlib
import inspect
import json
import logging
import os
import subprocess

import utils.shared_memory
from core.db import is_bundle_queued, queue_bundle, queue_job, update_job, delete_job
from core.messaging.message import Message
from core.messaging.message_ids import *
from scheduler.status import JobStatus
from settings import settings
from utils.packet_scheduler import PacketScheduler

# Lock for synchronising bundle fetching
lock = asyncio.Lock()


def get_scheduler_instance():
    """
    Returns the class specified by the HPC_SCHEDULER_CLASS setting

    :return: The Class identified by HPC_SCHEDULER_CLASS
    """
    # Split the class path by full stops
    class_bits = settings.HPC_SCHEDULER_CLASS.split('.')

    # Import and return the class
    return getattr(importlib.import_module('.'.join(class_bits[:-1])), class_bits[-1])


def get_default_details():
    """
    Returns the default 'details' dictionary that is passed to the bundle.py file in each bundle

    :return: The default details dictionary
    """

    return {
        'cluster': settings.CLUSTER_NAME,
    }


def get_bundle_loader_source(bundle_function, shm_name):
    """
    Constructs the python code that reads the data out of the shared memory region and then
    imports the bundle.py file and calls the specified function

    :param bundle_function: The name of the function to call from bundle.py
    :param shm_name: The name of the shared memory region to read data from

    :return: The python source
    """
    # Get the source code of the shared_memory module
    source = inspect.getsource(utils.shared_memory)

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


def run_bundle(bundle_function, bundle_path, bundle_hash, details, job_data):
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
    shm = utils.shared_memory.SharedMemory(create=True, size=len(data))

    # Write the data in to the shared memory
    data = bytearray(data.encode('utf-8'))
    shm.buf[0:len(data)] = data

    # Get the loader source
    source = get_bundle_loader_source(bundle_function, shm.name)

    # Attempt to call the bundle to create the submission script
    args = [os.path.join(bundle_path, bundle_hash, 'venv', 'bin', 'python'), '-c', source]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         cwd=os.path.join(bundle_path, bundle_hash))

    # Wait for the process to finish
    p.wait()

    # Get the stdout and stderr output from the command
    out, err = p.communicate()

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


async def submit_job(con, msg):
    job_id = msg.pop_uint()
    bundle_hash = msg.pop_string()
    params = msg.pop_string()

    bundle_path = os.path.join(os.path.dirname(__file__), "..", "bundles", "unpacked")

    # Synchronise the bundle fetch if needed
    async with lock:
        # Check if the bundle exists or not
        if not os.path.exists(os.path.join(bundle_path, bundle_hash)):
            # The bundle does not exist, check if it's queued for delivery
            if not await is_bundle_queued(bundle_hash):
                logging.info(f"Requesting bundle {bundle_hash} from the server for job {job_id}")

                # Request the bundle from the server
                response = Message(REQUEST_BUNDLE, source="system", priority=PacketScheduler.Priority.Highest)
                response.push_uint(job_id)
                response.push_string(bundle_hash)
                con.scheduler.queue_message(response)

                # Mark the bundle as queued
                await queue_bundle(bundle_hash)

            logging.info(f"Queuing job {job_id} until the bundle arrives")

            # Queue the job to be submitted when the bundle has been sent
            await queue_job(job_id, bundle_hash, params)

            # Nothing more to do for now
            return

    # Submit the job and record that we have submitted the job
    logging.info(f"Submitting new job with ui id {job_id}")

    # Create a dict to store the data for this job
    details = get_default_details()
    details['job_id'] = job_id

    # Get the working directory
    working_directory, success = run_bundle("working_directory", bundle_path, bundle_hash, details, params)

    # Check for success
    if not success:
        raise Exception("Failed to run the working_directory function from bundle {}")

    # Instantiate the scheduler
    scheduler = get_scheduler_instance()(job_id, None, working_directory)

    # Create a new job object and save it
    job = {'job_id': job_id, 'slurm_id': None, 'status': JobStatus.SUBMITTING}
    await update_job(job)

    # Run the bundle.py submit
    script_path, success = run_bundle("submit", bundle_path, bundle_hash, details, params)

    # Check for success
    if not success:
        raise Exception("Failed to run the submit function from bundle {}")

    # script_path contains the path to the generated submission script

    # Submit the job
    scheduler_id = scheduler.submit(script_path)

    # Check if there was an issue with the job
    if not scheduler_id:
        logging.info("Job with ui id {} could not be submitted"
                     .format(job['job_id']))

        await delete_job(job)
        # Notify the server that the job is failed
        result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        result.push_uint(JobStatus.ERROR)
        result.push_string("Unable to submit job. Please check the logs as to why.")
        # Send the result
        con.connection.queue_message(result)
    else:
        # Update and save the job
        job['scheduler_id'] = scheduler_id
        await update_job(job)

        logging.info("Successfully submitted job with ui id {}, got scheduler id {}"
                     .format(job['job_id'], job['scheduler_id']))

        # Notify the server that the job is successfully submitted
        result = Message(SUBMIT_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        # Send the result
        con.scheduler.queue_message(result)


async def handle_message(con, msg):
    if msg.id == SERVER_READY:
        con.scheduler.server_ready()

        result = Message(2001, source="system", priority=PacketScheduler.Priority.Highest)
        result.push_string("Unable to submit job. Please check the logs as to why.")
        # Send the result

        con.scheduler.queue_message(result)
    elif msg.id == SUBMIT_JOB:
        await submit_job(con, msg)
    else:
        logging.error("Got unknown message ID from the server: " + str(msg.id) + " source: " + msg.source)
