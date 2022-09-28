import logging
import sys
import traceback

from asgiref.sync import sync_to_async

from core.messaging.message import Message
from core.messaging.message_ids import UPDATE_JOB
from db.db.models import Job
from utils.bundle.interface import run_bundle
from utils.misc import get_bundle_path, get_default_details
from utils.packet_scheduler import PacketScheduler
from utils.status import JobStatus


async def delete_job(con, msg):
    # Get the job to delete
    job_id = msg.pop_uint()

    try:
        # Get the job, it must not be running or queued - indicating that it is completed
        job = await sync_to_async(Job.objects.get)(
            running=False, 
            queued=False, 
            job_id=job_id,
            submitting=False,
            deleted=False
        )
    except:
        logging.info(f"Job does not exist {job_id}, is currently running, or has already been deleted.")

        jobs = await sync_to_async(Job.objects.filter)(job_id=job_id)
        if not await sync_to_async(jobs.exists)():
            # If no job exists with the job id, mark it as deleted
            result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
            result.push_uint(job_id)
            result.push_string("_job_completion_")
            result.push_uint(JobStatus.DELETED)
            result.push_string("Job has been deleted")

            # Send the result
            await con.scheduler.queue_message(result)
            return

        job = await sync_to_async(jobs.first)()
        if job.deleted:
            # Job is already deleted, notify the server again
            result = Message(UPDATE_JOB, source=str(job_id), priority=PacketScheduler.Priority.Medium)
            result.push_uint(job_id)
            result.push_string("_job_completion_")
            result.push_uint(JobStatus.DELETED)
            result.push_string("Job has been deleted")

            # Send the result
            await con.scheduler.queue_message(result)

        return

    # If the job is currently deleting, do nothing
    if job.deleting:
        return

    # Mark the job as deleting
    job.deleting = True
    await sync_to_async(job.save)()

    try:
        # The job is not running and hasn't been deleted

        # Get the bundle path
        bundle_path = get_bundle_path()

        # Create a dict to store the data for this job
        details = get_default_details()
        details['job_id'] = job.job_id
        details['scheduler_id'] = job.scheduler_id

        deleted = await run_bundle("delete", bundle_path, job.bundle_hash, details, "")
        if not deleted:
            # If there was an issue with the bundle deleting the job, mark the job as not deleting
            # so that it can try again later
            job.deleting = False
            await sync_to_async(job.save)()

            logging.info(f"Job {job_id} could not be deleted by the bundle.")
            return

        # The job has been deleted so we can update the job and the server
        job.deleting = False
        job.deleted = True
        await sync_to_async(job.save)()

        result = Message(UPDATE_JOB, source=str(job.job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job.job_id)
        result.push_string("_job_completion_")
        result.push_uint(JobStatus.DELETED)
        result.push_string("Job has been deleted")

        # Send the result
        await con.scheduler.queue_message(result)
    except Exception as Exp:
        # If there was an exception, make sure that the job is not stuck in the deleting state
        job.deleting = False
        await sync_to_async(job.save)()

        # An exception occurred, log the exception to the log
        logging.error("Error in delete job")
        logging.error(type(Exp))
        logging.error(Exp.args)
        logging.error(Exp)

        # Also log the stack trace
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logging.error(''.join('!! ' + line for line in lines))
