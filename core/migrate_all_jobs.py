import logging
import sys
import traceback

from asgiref.sync import sync_to_async

from core.messaging.message import Message
from core.messaging.message_ids import DB_JOB_SAVE
from db.db.models import Job
from utils.misc import sync_to_async_iterable
from utils.packet_scheduler import PacketScheduler


async def migrate_all_jobs(con):
    try:
        jobs = await sync_to_async(Job.objects.all)()
        logging.info("Migrating jobs to the server: {}".format(str(await sync_to_async(jobs.__repr__)())))

        db_request_id = 0
        async for job in sync_to_async_iterable(jobs):
            db_request_id += 1
            try:
                # Create the remote job
                msg = Message(
                    DB_JOB_SAVE,
                    source="database",
                    priority=PacketScheduler.Priority.Medium,
                    callback=lambda: print(f"Job {job} has been saved.")
                )
                msg.push_ulong(db_request_id)
                msg.push_ulong(0)
                msg.push_ulong(job.job_id)
                msg.push_ulong(job.scheduler_id)
                msg.push_bool(job.submitting)
                msg.push_uint(job.submitting_count)
                msg.push_string(job.bundle_hash)
                msg.push_string(job.working_directory)
                msg.push_bool(job.running)
                # Send the result
                await con.scheduler.queue_message(msg)
            except Exception as e:
                logging.info(f"Unable to save job {job} {e}")

    except Exception as Exp:
        # An exception occurred, log the exception to the log
        logging.error("Error in job migration")
        logging.error(type(Exp))
        logging.error(Exp.args)
        logging.error(Exp)

        # Also log the stack trace
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logging.error(''.join('!! ' + line for line in lines))
