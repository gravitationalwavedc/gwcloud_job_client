import asyncio
import logging
import sys
import traceback
from asyncio import sleep

from asgiref.sync import sync_to_async

from core.messaging.message import Message
from core.messaging.message_ids import UPDATE_JOB
from db.db.models import Job, JobStatusModel
from utils.archive_job import archive_job
from utils.misc import run_bundle, get_bundle_path, get_default_details, sync_to_async_iterable
from utils.packet_scheduler import PacketScheduler
from utils.status import JobStatus


async def check_job_status(con, job, force_notification=False):
    """
    Checks a job to see what it's current status is on the cluster. If the job state has changed since we last
    checked, then update the server. If force_notification is True, it will update the server even if the job
    status hasn't changed

    :param con: The connection to the server
    :param job: The job object to check
    :param force_notification: If we should notify the server of the job status even if it hasn't changed
    :return: Nothing
    """
    # Get the bundle path
    bundle_path = get_bundle_path()

    # Create a dict to store the data for this job
    details = get_default_details()
    details['job_id'] = job.job_id
    details['scheduler_id'] = job.scheduler_id

    # Get the status of the job
    _status = await run_bundle("status", bundle_path, job.bundle_hash, details, "")

    # Check if the status has changed or not
    for stat in _status['status']:
        info = stat["info"]
        status = stat["status"]
        what = stat['what']

        dbstatus = await sync_to_async(job.status.filter)(what=what)

        # Prevent duplicates
        if await sync_to_async(dbstatus.count)() > 1:
            await sync_to_async(dbstatus.delete)()

        if force_notification \
                or not await sync_to_async(dbstatus.exists)() \
                or status != (await sync_to_async(dbstatus.first)()).state:

            # Check for a valid status - sometimes slurm returns an empty string
            if status is None:
                return

            if await sync_to_async(dbstatus.exists)():
                s = await sync_to_async(dbstatus.first)()
            else:
                s = JobStatusModel(job=job)

            # Update the database
            s.what = what
            s.state = status
            await sync_to_async(s.save)()

            # Send the status to the server
            result = Message(
                UPDATE_JOB,
                source=job.bundle_hash + "_" + str(job.job_id),
                priority=PacketScheduler.Priority.Medium
            )
            result.push_uint(job.job_id)
            result.push_string(what)
            result.push_uint(status)
            result.push_string(info)
            await con.scheduler.queue_message(result)

    job_error = False
    async for state in sync_to_async_iterable(job.status.all()):
        # Check if any of the jobs are in error state
        if state.state > JobStatus.RUNNING and state.state != JobStatus.COMPLETED:
            job_error = state.state

    job_complete = True
    async for state in sync_to_async_iterable(job.status.all()):
        # Check if all jobs are complete
        if state.state != JobStatus.COMPLETED:
            job_complete = False

    # Check if there was an error, or if all jobs have completed
    if job_error or (_status['complete'] and job_complete >= 1):
        job.running = False
        await sync_to_async(job.save)()

        # Tar up the job
        await archive_job(job)

        # Notify the server that the job has completed
        result = Message(UPDATE_JOB, source=str(job.job_id), priority=PacketScheduler.Priority.Medium)
        result.push_uint(job.job_id)
        result.push_string("_job_completion_")
        result.push_uint(job_error if job_error else JobStatus.COMPLETED)
        result.push_string("Job has completed")
        # Send the result
        await con.scheduler.queue_message(result)


async def check_all_jobs(con):
    try:
        jobs = await sync_to_async(Job.objects.filter)(running=True, queued=False, job_id__isnull=False,
                                                       submitting=False)
        logging.info("Jobs {}".format(str(await sync_to_async(jobs.__repr__)())))

        futures = []
        async for job in sync_to_async_iterable(jobs):
            futures.append(asyncio.ensure_future(check_job_status(con, job)))

        if len(futures):
            await asyncio.wait(futures)

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


async def check_job_status_thread(con):
    """
    Thread entry point for checking the current status of running jobs

    :return: Nothing
    """
    while True:
        await check_all_jobs(con)

        await sleep(60)

