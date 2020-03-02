import logging

from core.messaging.message import Message
from utils.scheduler import Scheduler

# Send by the server when it has finished setting up the connection and is ready for messages
SERVER_READY = 1000


async def handle_message(con, msg):
    if msg.id == SERVER_READY:
        con.scheduler.server_ready()

        result = Message(2000, source="system")
        result.push_string("Unable to submit job. Please check the logs as to why.")
        # Send the result

        con.scheduler.queue_message("system", result, Scheduler.Priority.Highest)

    else:
        logging.error("Got unknown message ID from the server: " + str(msg.id) + " source: " + msg.source)