import asyncio
import logging

from core.file_handler import download_file, paused_file_transfers, get_file_list
from core.messaging.message_ids import SERVER_READY, SUBMIT_JOB, DOWNLOAD_FILE, PAUSE_FILE_CHUNK_STREAM, \
    RESUME_FILE_CHUNK_STREAM, FILE_LIST, CANCEL_JOB, DELETE_JOB
from core.submit_job import submit_job
from core.cancel_job import cancel_job
from core.delete_job import delete_job


async def handle_message(con, msg):
    if msg.id == SERVER_READY:
        # The server is ready to accept packets - start the packet scheduler
        # This is sent after the server has created the cluster object after verifying the token is valid
        con.scheduler.start()
    elif msg.id == SUBMIT_JOB:
        # Submit a job
        asyncio.ensure_future(submit_job(con, msg))
    elif msg.id == DOWNLOAD_FILE:
        # Download a file
        asyncio.ensure_future(download_file(con, msg))
    elif msg.id == PAUSE_FILE_CHUNK_STREAM:
        # Pause a file download (Remote end's transmission buffer is above the "high" threshold)
        uuid = msg.pop_string()
        logging.info(f"Paused download: {uuid}")
        paused_file_transfers[uuid] = asyncio.Event()
    elif msg.id == RESUME_FILE_CHUNK_STREAM:
        # Resume a file download (Remote end's transmission buffer is below the "low" threshold)
        uuid = msg.pop_string()
        logging.info(f"Resumed download: {uuid}")
        if uuid in paused_file_transfers:
            paused_file_transfers[uuid].set()
            del paused_file_transfers[uuid]
    elif msg.id == FILE_LIST:
        # List all files in a directory
        asyncio.ensure_future(get_file_list(con, msg))
    elif msg.id == CANCEL_JOB:
        # Cancel a job
        asyncio.ensure_future(cancel_job(con, msg))
    elif msg.id == DELETE_JOB:
        # Delete a job
        asyncio.ensure_future(delete_job(con, msg))

    else:
        logging.error("Got unknown message ID from the server: " + str(msg.id) + " source: " + msg.source)
