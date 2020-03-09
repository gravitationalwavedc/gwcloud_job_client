import logging
import sys
import traceback
from asyncio import sleep

from core.db import delete_job, update_job, get_all_jobs
from core.messaging.message import Message
from core.messaging.message_ids import UPDATE_JOB
from core.submit_job import get_default_details
from scheduler.status import JobStatus
from utils.misc import run_bundle, get_bundle_path, get_scheduler_instance


async def check_job_status(con, job, force_notification=False):
    """
    Checks a job to see what it's current status is on the cluster. If the job state has changed since we last
    checked, then update the server. If force_notification is True, it will update the server even if the job
    status hasn't changed

    :param con: The connection to the server
    :param job: The job to check
    :param force_notification: If we should notify the server of the job status even if it hasn't changed
    :return: Nothing
    """
    # Check that this job actually has a local id to check
    if not job['job_id']:
        return

    # Get the bundle path
    bundle_path = get_bundle_path()

    # Create a dict to store the data for this job
    details = get_default_details()
    details['job_id'] = job['job_id']

    # Get the working directory
    working_directory, success = run_bundle("working_directory", bundle_path, job['bundle_hash'], details, "")

    # Check for success
    if not success:
        raise Exception("Failed to run the working_directory function from bundle {}")

    # Instantiate the scheduler
    scheduler = get_scheduler_instance()(job['job_id'], job['scheduler_id'], working_directory)

    # Get the status of the job
    status, info = await scheduler.status()

    # Check if the status has changed or not
    if job['status'] != status or force_notification:
        # Send the status to the server to assure receipt in case the job is to be deleted from our database
        result = Message(UPDATE_JOB)
        result.push_uint(job['ui_id'])
        result.push_uint(status)
        result.push_string(info)
        con.connection.queue_message(result)

        # Check if we should delete the job from the database
        if status > JobStatus.RUNNING:
            # Yes, the job is no longer running, remove it from the database
            await delete_job(job)
        else:
            # Update and save the job status
            job['status'] = status
            await update_job(job)


async def check_job_status_thread(self):
    """
    Thread entry point for checking the current status of running jobs

    :return: Nothing
    """
    while True:
        try:
            jobs = await get_all_jobs()
            logging.info("Jobs {}".format(str(jobs)))
            for job in jobs:
                await self.check_job_status(job)
        except Exception as Exp:
            # An exception occurred, log the exception to the log
            logging.error("Error in check job status")
            logging.error(type(Exp))
            logging.error(Exp.args)
            logging.error(Exp)

            # Also log the stack trace
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(''.join('!! ' + line for line in lines))

        await sleep(60)
