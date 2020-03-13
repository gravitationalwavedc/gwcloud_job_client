import asyncio
import logging

from core.file_handler import download_file, paused_file_transfers
from core.messaging.message_ids import *
from core.submit_job import submit_job


async def handle_message(con, msg):
    if msg.id == SERVER_READY:
        # The server is ready to accept packets - start the packet scheduler
        con.scheduler.server_ready()
    elif msg.id == SUBMIT_JOB:
        # Submit a job
        asyncio.ensure_future(submit_job(con, msg))
    elif msg.id == DOWNLOAD_FILE:
        asyncio.ensure_future(download_file(con, msg))
    elif msg.id == PAUSE_FILE_CHUNK_STREAM:
        uuid = msg.pop_string()
        logging.info(f"Paused download: {uuid}")
        paused_file_transfers[uuid] = asyncio.Event()
    elif msg.id == RESUME_FILE_CHUNK_STREAM:
        uuid = msg.pop_string()
        logging.info(f"Resumed download: {uuid}")
        if uuid in paused_file_transfers:
            paused_file_transfers[uuid].set()
            del paused_file_transfers[uuid]
    else:
        logging.error("Got unknown message ID from the server: " + str(msg.id) + " source: " + msg.source)
