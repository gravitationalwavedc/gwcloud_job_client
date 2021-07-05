# Lock for synchronising bundle fetching
import asyncio
import logging
import os

from asgiref.sync import sync_to_async

from core.check_job_status import check_job_status
from core.messaging.message import Message
from core.messaging.message_ids import REQUEST_BUNDLE, UPDATE_JOB
from db.db.models import Job
from utils.misc import get_bundle_path, run_bundle, get_default_details
from utils.packet_scheduler import PacketScheduler
from utils.status import JobStatus

lock = asyncio.Lock()


async def submit_job(con, msg):
    job_id = msg.pop_uint()
    bundle_hash = msg.pop_string()
    params = msg.pop_string()

    bundle_path = get_bundle_path()

    # The following fragment of code is a critical section, so we exclude access to any more than one thread. Without
    # this, it's possible that more than one job is entered in the database with the same job id
    async with lock:
        # Check if this job has already been submitted
        search = await sync_to_async(Job.objects.filter)(job_id=job_id)
        if await sync_to_async(search.exists)():
            job = await sync_to_async(search.first)()
        else:
            job = Job()

        # If the job is still waiting to be submitted - there is nothing more to do
        if job.submitting:
            logging.info(f"Job with {job_id} is being submitted, nothing to do")
            return

        if job.job_id:
            logging.info(f"Job with job id {job_id} has already been submitted, checking status...")
            # If so, check the state of the job and notify the server of it's current state
            await check_job_status(con, job, True)
            return

        logging.info("Attempting to submit new job with UI ID: {}".format(job_id))

        # Check if the bundle exists or not
        if not os.path.exists(os.path.join(bundle_path, bundle_hash)):
            # The bundle does not exist, check if it's queued for delivery
            if not await sync_to_async(
                    await sync_to_async(Job.objects.filter)(bundle_hash=bundle_hash, queued=True).exists)():
                logging.info(f"Requesting bundle {bundle_hash} from the server for job {job_id}")

                # Request the bundle from the server
                response = Message(REQUEST_BUNDLE, source="system", priority=PacketScheduler.Priority.Highest)
                response.push_uint(job_id)
                response.push_string(bundle_hash)
                await con.scheduler.queue_message(response)

            logging.info(f"Queuing job {job_id} until the bundle arrives")

            # Mark the job as queued
            job.queued = True
            job.job_id = job_id
            job.bundle_hash = bundle_hash
            job.params = params
            await sync_to_async(job.save)()

            # Nothing more to do for now
            return

        # Submit the job and record that we have submitted the job
        logging.info(f"Submitting new job with ui id {job_id}")

        # Create a dict to store the data for this job
        details = get_default_details()
        details['job_id'] = job_id

        # Create a new job object and save it
        job.job_id = job_id
        job.bundle_hash = bundle_hash
        job.submitting = True
        job.working_directory = ''
        await sync_to_async(job.save)()

    # The job is guarenteed to be in the database now, so we can finish the critical section

    # Get the working directory
    working_directory = await run_bundle("working_directory", bundle_path, bundle_hash, details, "")

    # Create a new job object and save it
    job.working_directory = working_directory
    await sync_to_async(job.save)()

    try:
        # Run the bundle.py submit
        scheduler_id = await run_bundle("submit", bundle_path, bundle_hash, details, params)
    except:
        scheduler_id = None

    # Check if there was an issue with the job
    if not scheduler_id:
        logging.info("Job with ui id {} could not be submitted"
                     .format(job.job_id))

        await sync_to_async(job.delete)()

        # Notify the server that the job is failed
        result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        result.push_string("system")
        result.push_uint(JobStatus.ERROR)
        result.push_string("Unable to submit job. Please check the logs as to why.")
        # Send the result
        await con.scheduler.queue_message(result)

        # Notify the server that the job is failed
        result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        result.push_string("_job_completion_")
        result.push_uint(JobStatus.ERROR)
        result.push_string("Unable to submit job. Please check the logs as to why.")
        # Send the result
        await con.scheduler.queue_message(result)
    else:
        # Update and save the job
        job.scheduler_id = scheduler_id
        job.submitting = False
        await sync_to_async(job.save)()

        logging.info("Successfully submitted job with ui id {}, got scheduler id {}"
                     .format(job.job_id, job.scheduler_id))

        result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        result.push_string("system")
        result.push_uint(JobStatus.SUBMITTED)
        result.push_string("Job submitted successfully")
        # Send the result
        await con.scheduler.queue_message(result)
