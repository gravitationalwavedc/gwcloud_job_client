import asyncio
import importlib
import inspect
import json
import logging
import struct

import utils.shared_memory

from core.db import is_bundle_queued, queue_bundle, queue_job, update_job
from core.messaging.message import Message
from scheduler.status import JobStatus
from settings import settings
from utils.scheduler import Scheduler
import subprocess
import os

# Send by the server when it has finished setting up the connection and is ready for messages
SERVER_READY = 1000

# Submits a new job
SUBMIT_JOB = 2000

# Request a missing bundle from the server
REQUEST_BUNDLE = 3000


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
    logging.info("Running command " + " ".join(args))
    logging.info("Gave output:")
    logging.info("stdout: " + out)
    logging.info("stderr: " + err)

    # Get the last line of output
    result = out.splitlines()[-1]

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
                response = Message(REQUEST_BUNDLE, source="system", priority=Scheduler.Priority.Highest)
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

    # Instantiate the scheduler
    scheduler = get_scheduler_instance()(settings, job_id, None)

    # Create a new job object and save it
    job = {'job_id': job_id, 'slurm_id': None, 'status': JobStatus.SUBMITTING}
    await update_job(job)

    # Create a dict to store the data for this job
    details = get_default_details()
    details['job_id'] = job_id

    # Run the bundle.py submit
    result, success = run_bundle("submit", bundle_path, bundle_hash, details, params)

    # Check for success
    if not success:
        raise Exception("Failed to run the submit function from bundle {}")

    # result contains the path to the generated submission script

    # Submit the job
    job_id = scheduler._submit(job_params)

    # Check if there was an issue with the job
    if not job_id:
        logging.info("Job with ui id {} could not be submitted"
                     .format(job['ui_id'], job['job_id']))
        await delete_job(job)
        # Notify the server that the job is failed
        result = Message(Message.UPDATE_JOB)
        result.push_uint(ui_id)
        result.push_uint(JobStatus.ERROR)
        result.push_string("Unable to submit job. Please check the logs as to why.")
        # Send the result
        await self.sock.send(result.to_bytes())
    else:
        # Update and save the job
        job['job_id'] = job_id
        await update_job(job)

        logging.info("Successfully submitted job with ui id {}, got scheduler id {}"
                     .format(job['ui_id'], job['job_id']))

        # Notify the server that the job is successfully submitted
        result = Message(Message.SUBMIT_JOB)
        result.push_uint(ui_id)
        # Send the result
        await self.sock.send(result.to_bytes())

async def handle_message(con, msg):
    if msg.id == SERVER_READY:
        con.scheduler.server_ready()

        result = Message(2001, source="system", priority=Scheduler.Priority.Highest)
        result.push_string("Unable to submit job. Please check the logs as to why.")
        # Send the result

        con.scheduler.queue_message(result)
    elif msg.id == SUBMIT_JOB:
        await submit_job(con, msg)
    else:
        logging.error("Got unknown message ID from the server: " + str(msg.id) + " source: " + msg.source)