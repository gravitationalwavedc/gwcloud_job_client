import logging
from pathlib import Path
import sys
import traceback

from asgiref.sync import sync_to_async

from core.check_job_status import check_job_status
from core.messaging.message import Message
from core.messaging.message_ids import UPDATE_JOB
from db.db.models import Job
from utils.archive_job import archive_job
from utils.bundle.interface import run_bundle
from utils.misc import get_bundle_path, get_default_details
from utils.packet_scheduler import PacketScheduler
from utils.status import JobStatus


async def cancel_job(con, msg):
    # Get the job to cancel
    job_id = msg.pop_uint()

    try:
        # Get the job
        job = await sync_to_async(Job.objects.get)(
            running=True, 
            queued=False, 
            job_id=job_id,
            submitting=False
        )
    except:
        logging.info(f"Job does not exist {job_id}")
        
        # Notify the server that the job has been cancelled
        result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job_id)
        result.push_string("_job_completion_")
        result.push_uint(JobStatus.CANCELLED)
        result.push_string("Job has been cancelled")

        # Send the result
        await con.scheduler.queue_message(result)

        return

    try:
        # Force a status check
        await check_job_status(con, job)
        await sync_to_async(job.refresh_from_db)()

        # Check if the job is running after the status update
        if not job.running:
            # The job is no longer running - there is nothing to do
            logging.info(f"Job {job_id} is not running so cannot be cancelled, nothing to do.")
            return

        # The job is still running, so attempt to call the bundle to cancel the job

        # Get the bundle path
        bundle_path = get_bundle_path()

        # Create a dict to store the data for this job
        details = get_default_details()
        details['job_id'] = job.job_id
        details['scheduler_id'] = job.scheduler_id

        cancelled = await run_bundle("cancel", bundle_path, job.bundle_hash, details, "")
        if not cancelled:
            logging.info(f"Job {job_id} could not be cancelled by the bundle.")
            return

        # If the job was cancelled, we need to check the job status once more to update the server of any changes.
        # Once the final job status check has completed, we check if there is a job status already for cancelled, 
        # and add our own if not
        
        await sync_to_async(job.refresh_from_db)()
        await check_job_status(con, job)

        dbstatus = await sync_to_async(job.status.filter)(state=JobStatus.CANCELLED)
        if await sync_to_async(dbstatus.exists)():
            # We've already notified the server of the CANCELLED status if a state record exists
            return

        # Mark the job as no longer running
        job.running = False
        await sync_to_async(job.save)()

        # Tar up the job
        await archive_job(job)

        # Notify the server that the job has been cancelled
        result = Message(UPDATE_JOB, source=str(job.job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job.job_id)
        result.push_string("_job_completion_")
        result.push_uint(JobStatus.CANCELLED)
        result.push_string("Job has been cancelled")

        # Send the result
        await con.scheduler.queue_message(result)
    except Exception as Exp:
        # An exception occurred, log the exception to the log
        logging.error("Error in cancel job")
        logging.error(type(Exp))
        logging.error(Exp.args)
        logging.error(Exp)

        # Also log the stack trace
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logging.error(''.join('!! ' + line for line in lines))
