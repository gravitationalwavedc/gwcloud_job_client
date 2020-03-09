import logging

from core.messaging.message_ids import *
from core.submit_job import submit_job


async def handle_message(con, msg):
    if msg.id == SERVER_READY:
        # The server is ready to accept packets - start the packet scheduler
        con.scheduler.server_ready()
    elif msg.id == SUBMIT_JOB:
        # Submit a job
        await submit_job(con, msg)
    else:
        logging.error("Got unknown message ID from the server: " + str(msg.id) + " source: " + msg.source)
