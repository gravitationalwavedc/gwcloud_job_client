import asyncio
import logging
import os
import sys
import traceback
from asyncio import sleep

from core.messaging.message import Message
from core.messaging.message_ids import FILE_ERROR, FILE_DETAILS, FILE_CHUNK, FILE_LIST_ERROR, FILE_LIST
from utils.misc import run_bundle, get_bundle_path, get_default_details
from utils.packet_scheduler import PacketScheduler

# Set the chunk size to 64kb for now
CHUNK_SIZE = 1024*64

paused_file_transfers = {}


async def download_file(con, msg):
    # Get the job details
    job_id = msg.pop_uint()
    uuid = msg.pop_string()
    bundle_hash = msg.pop_string()
    file_path = msg.pop_string()

    # Create a dict to store the data for this job
    details = get_default_details()
    details['job_id'] = job_id

    # Get the working directory
    working_directory, success = await run_bundle("working_directory", get_bundle_path(), bundle_hash, details, "")

    # Check for success
    if not success:
        raise Exception(f"Failed to run the working_directory function from bundle {bundle_hash}")

    # Get the absolute path to the file
    file_path = os.path.abspath(os.path.join(working_directory, file_path))

    # Verify that this file really sits under the working directory
    if not file_path.startswith(working_directory):
        logging.info(f"File to download is outside the working directory {file_path}")
        # Report that the file doesn't exist
        result = Message(FILE_ERROR, source=str(uuid), priority=PacketScheduler.Priority.Highest)
        result.push_string(uuid)
        result.push_string("File does not exist")
        await con.scheduler.queue_message(result)
        return

    # Verify that the path exists
    if not os.path.exists(file_path):
        logging.info(f"File does not exist {file_path}")
        # Report that the file doesn't exist
        result = Message(FILE_ERROR, source=str(uuid), priority=PacketScheduler.Priority.Highest)
        result.push_string(uuid)
        result.push_string("File does not exist")
        await con.scheduler.queue_message(result)
        return

    # Verify that the path is not a directory
    if os.path.isdir(file_path):
        logging.info(f"File to download is directory {file_path}")
        # Report that the file doesn't exist
        result = Message(FILE_ERROR, source=str(uuid), priority=PacketScheduler.Priority.Highest)
        result.push_string(uuid)
        result.push_string("File does not exist")
        await con.scheduler.queue_message(result)
        return

    logging.info(f"Trying to download file {job_id} {uuid} {bundle_hash}")
    logging.info(f"Path {file_path}")

    # Get the file size
    file_size = os.path.getsize(file_path)

    # Send the file size to the server
    result = Message(FILE_DETAILS, source=str(uuid), priority=PacketScheduler.Priority.Highest)
    result.push_string(uuid)
    result.push_ulong(file_size)
    await con.scheduler.queue_message(result)

    try:
        # Open the file and stream it to the client
        with open(file_path, 'rb') as file:
            # Loop until all bytes of the file have been read
            packet_count = 0
            while file_size:
                # Check if the server has asked us to pause the stream
                # Wrapped in try in case python yields between if and await and uuid is removed
                try:
                    if uuid in paused_file_transfers:
                        await paused_file_transfers[uuid].wait()
                except:
                    pass

                # Read the next chunk and send it to the server
                if file_size > CHUNK_SIZE:
                    data = file.read(CHUNK_SIZE)
                else:
                    data = file.read(file_size)

                # Since we don't want to flood the packet scheduler (So that we can give the server a chance to
                # pause file transfers), we create an event that we wait for on every nth packet, and don't
                # transfer any more packets until the marked packet has been sent. There will always be some
                # amount of buffer overrun on the server, but at localhost speeds it's about 8Mb which is tolerable
                event = asyncio.Event()

                # Send the packet to the scheduler
                result = Message(FILE_CHUNK, source=str(uuid), priority=PacketScheduler.Priority.Lowest, callback=lambda: event.set())
                result.push_string(uuid)
                result.push_bytes(data)
                await con.scheduler.queue_message(result)

                # If this is the nth packet, wait for it to be sent before sending additional packets
                if packet_count % 10 == 0:
                    await event.wait()

                # Update counters
                file_size -= len(data)
                packet_count += 1

        logging.info(f"Finished file transfer for {uuid}")
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

        # Report that there was a file exception
        result = Message(FILE_ERROR, source=str(uuid), priority=PacketScheduler.Priority.Highest)
        result.push_string(uuid)
        result.push_string("Exception reading file")
        await con.scheduler.queue_message(result)

    finally:
        # Clean up the transfer pause event if one exists
        if uuid in paused_file_transfers:
            del paused_file_transfers[uuid]


async def get_file_list(con, msg):
    # Get the job details
    job_id = msg.pop_uint()
    uuid = msg.pop_string()
    bundle_hash = msg.pop_string()
    dir_path = msg.pop_string()
    is_recursive = msg.pop_bool()

    # Create a dict to store the data for this job
    details = get_default_details()
    details['job_id'] = job_id

    # Get the working directory
    working_directory, success = await run_bundle("working_directory", get_bundle_path(), bundle_hash, details, "")

    # Check for success
    if not success:
        raise Exception(f"Failed to run the working_directory function from bundle {bundle_hash}")

    # Get the absolute path to the file
    dir_path = os.path.abspath(os.path.join(working_directory, dir_path))

    # Verify that this file really sits under the working directory
    if not dir_path.startswith(working_directory):
        logging.info(f"Path to list files is outside the working directory {dir_path}")
        # Report that the file doesn't exist
        result = Message(FILE_LIST_ERROR, source=str(uuid), priority=PacketScheduler.Priority.Highest)
        result.push_string(uuid)
        result.push_string("File does not exist")
        await con.scheduler.queue_message(result)
        return

    # Verify that the path exists
    if not os.path.exists(dir_path):
        logging.info(f"Path to list files not exist {dir_path}")
        # Report that the file doesn't exist
        result = Message(FILE_LIST_ERROR, source=str(uuid), priority=PacketScheduler.Priority.Highest)
        result.push_string(uuid)
        result.push_string("File does not exist")
        await con.scheduler.queue_message(result)
        return

    # Verify that the path is not a directory
    if not os.path.isdir(dir_path):
        logging.info(f"Path to list files is not directory {dir_path}")
        # Report that the file doesn't exist
        result = Message(FILE_LIST_ERROR, source=str(uuid), priority=PacketScheduler.Priority.Highest)
        result.push_string(uuid)
        result.push_string("File does not exist")
        await con.scheduler.queue_message(result)
        return

    logging.info(f"Trying to get file list {job_id} {uuid} {bundle_hash}")
    logging.info(f"Path {dir_path}")

    # Get the list of files requested
    file_list = []
    if is_recursive:
        # This is a recursive searh
        for root, dirnames, filenames in os.walk(dir_path):
            # Iterate over the directories
            for item in dirnames:
                # Construct the real path to this directory
                real_file_name = os.path.join(root, item)
                # Add the file entry
                file_list.append({
                    # Remove the leading working directory
                    'path': real_file_name[len(working_directory):],
                    'is_dir': True,
                    'size': os.path.getsize(real_file_name)
                })

            for item in filenames:
                # Construct the real path to this file
                real_file_name = os.path.join(root, item)
                # Add the file entry
                try:
                    file_list.append({
                        # Remove the leading working directory
                        'path': real_file_name[len(working_directory):],
                        'is_dir': False,
                        'size': os.path.getsize(real_file_name)
                    })
                except FileNotFoundError:
                    # Happens when trying to stat a symlink
                    pass
    else:
        logging.info("getting files...")
        # Not a recursive search
        for item in os.listdir(dir_path):
            logging.info(f"file {item}")
            # Construct the real path to this file/directory
            real_file_name = os.path.join(dir_path, item)
            # Add the file entry
            file_list.append({
                # Remove the leading slash
                'path': real_file_name[len(working_directory):],
                'is_dir': os.path.isdir(real_file_name),
                'size': os.path.getsize(real_file_name),
            })

    # Build the return message
    result = Message(FILE_LIST, source=str(uuid), priority=PacketScheduler.Priority.Highest)
    result.push_string(uuid)
    result.push_uint(len(file_list))
    for f in file_list:
        result.push_string(f['path'])
        result.push_bool(f['is_dir'])
        result.push_ulong(f['size'])

    await con.scheduler.queue_message(result)
