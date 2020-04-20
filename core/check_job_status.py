import asyncio
import logging
import sys
import traceback
from asyncio import sleep

from core.db import delete_job, update_job, get_all_jobs
from core.messaging.message import Message
from core.messaging.message_ids import UPDATE_JOB
from utils.status import JobStatus
from utils.misc import run_bundle, get_bundle_path, get_default_details
from utils.packet_scheduler import PacketScheduler


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
    details['scheduler_id'] = job['scheduler_id']

    # Get the status of the job
    _status = await run_bundle("status", bundle_path, job['bundle_hash'], details, "")

    # Check if the status has changed or not
    for stat in _status['status']:
        info = stat["info"]
        status = stat["status"]
        what = stat['what']

        if what not in job['status'] or job['status'][what] != status or force_notification:
            # Update the database
            job['status'][what] = status

            # Send the status to the server
            result = Message(
                UPDATE_JOB,
                source=job['bundle_hash'] + "_" + str(job['job_id']),
                priority=PacketScheduler.Priority.Medium
            )
            result.push_uint(job['job_id'])
            result.push_string(what)
            result.push_uint(status)
            result.push_string(info)
            await con.scheduler.queue_message(result)

    # Update any changes in the database
    await update_job(job)

    job_error = False
    for state in job['status'].values():
        # Check if any of the jobs are in error state
        if state > JobStatus.RUNNING and state != JobStatus.COMPLETED:
            job_error = True

    job_complete = True
    for state in job['status'].values():
        # Check if all jobs are complete
        if state != JobStatus.COMPLETED:
            job_complete = False

    # Check if there was an error, or if all jobs have completed
    if job_error or (_status['complete'] and job_complete >= 1):
        await delete_job(job)


async def check_all_jobs(con):
    try:
        jobs = await get_all_jobs()
        logging.info("Jobs {}".format(str(jobs)))

        futures = []
        for job in jobs:
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
        await sleep(5)
