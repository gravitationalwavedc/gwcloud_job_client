# Lock for synchronising bundle fetching
import asyncio
import logging
import os

from core.db import is_bundle_queued, queue_bundle, queue_job, update_job, delete_job
from core.messaging.message import Message
from core.messaging.message_ids import REQUEST_BUNDLE, UPDATE_JOB
from scheduler.status import JobStatus
from settings import settings
from utils.misc import get_bundle_path, run_bundle, get_scheduler_instance
from utils.packet_scheduler import PacketScheduler

lock = asyncio.Lock()


def get_default_details():
    """
    Returns the default 'details' dictionary that is passed to the bundle.py file in each bundle

    :return: The default details dictionary
    """

    return {
        'cluster': settings.CLUSTER_NAME,
    }


async def submit_job(con, msg):
    job_id = msg.pop_uint()
    bundle_hash = msg.pop_string()
    params = msg.pop_string()

    bundle_path = get_bundle_path()

    logging.info("Attempting to submit new job with UI ID: {}".format(job_id))

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
    working_directory, success = await run_bundle("working_directory", bundle_path, bundle_hash, details, params)

    # Check for success
    if not success:
        raise Exception("Failed to run the working_directory function from bundle {}")

    # Instantiate the scheduler
    scheduler = get_scheduler_instance()(job_id, None, working_directory)

    # Create a new job object and save it
    job = {'job_id': job_id, 'scheduler_id': None, 'status': JobStatus.SUBMITTING, 'bundle_hash': bundle_hash}
    await update_job(job)

    # Run the bundle.py submit
    script_path, success = await run_bundle("submit", bundle_path, bundle_hash, details, params)

    # Check for success
    if not success:
        raise Exception("Failed to run the submit function from bundle {}")

    # script_path contains the path to the generated submission script

    # Submit the job
    scheduler_id = await scheduler.submit(script_path)

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
        con.scheduler.queue_message(result)
    else:
        # Update and save the job
        job['scheduler_id'] = scheduler_id
        await update_job(job)

        logging.info("Successfully submitted job with ui id {}, got scheduler id {}"
                     .format(job['job_id'], job['scheduler_id']))

        result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        result.push_uint(JobStatus.SUBMITTED)
        result.push_string("Job submitted successfully")
        # Send the result
        con.scheduler.queue_message(result)
