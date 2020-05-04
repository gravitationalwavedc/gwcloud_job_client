# Lock for synchronising bundle fetching
import asyncio
import logging
import os

from core.check_job_status import check_job_status
from core.db import is_bundle_queued, queue_bundle, queue_job, update_job, delete_job, get_job_by_ui_id
from core.messaging.message import Message
from core.messaging.message_ids import REQUEST_BUNDLE, UPDATE_JOB
from utils.status import JobStatus
from utils.misc import get_bundle_path, run_bundle, get_default_details
from utils.packet_scheduler import PacketScheduler

lock = asyncio.Lock()


async def submit_job(con, msg):
    job_id = msg.pop_uint()
    bundle_hash = msg.pop_string()
    params = msg.pop_string()

    bundle_path = get_bundle_path()

    # Check if this job has already been submitted
    job = await get_job_by_ui_id(job_id)

    # If the job is still waiting to be submitted - there is nothing more to do
    if job and 'submit_lock' in job and job['submit_lock']:
        logging.info(f"Job with {job_id} is being submitted, nothing to do")
        return

    if job and job['job_id']:
        logging.info(f"Job with job id {job_id} has already been submitted, checking status...")
        # If so, check the state of the job and notify the server of it's current state
        await check_job_status(con, job, True)
        return

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
                await con.scheduler.queue_message(response)

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

    # Create a new job object and save it
    job = {'job_id': job_id, 'scheduler_id': None, 'status': {}, 'bundle_hash': bundle_hash, 'submit_lock': True}
    await update_job(job)

    # Run the bundle.py submit
    scheduler_id = await run_bundle("submit", bundle_path, bundle_hash, details, params)

    job['submit_lock'] = False
    await update_job(job)

    # Check if there was an issue with the job
    if not scheduler_id:
        logging.info("Job with ui id {} could not be submitted"
                     .format(job['job_id']))

        await delete_job(job)
        # Notify the server that the job is failed
        result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        result.push_string("system")
        result.push_uint(JobStatus.ERROR)
        result.push_string("Unable to submit job. Please check the logs as to why.")
        # Send the result
        await con.scheduler.queue_message(result)
    else:
        # Update and save the job
        job['scheduler_id'] = scheduler_id
        await update_job(job)

        logging.info("Successfully submitted job with ui id {}, got scheduler id {}"
                     .format(job['job_id'], job['scheduler_id']))

        result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        result.push_string("system")
        result.push_uint(JobStatus.SUBMITTED)
        result.push_string("Job submitted successfully")
        # Send the result
        await con.scheduler.queue_message(result)
