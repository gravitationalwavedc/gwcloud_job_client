import asyncio
import os
import socket
from subprocess import list2cmdline

from utils.bundle.client import UnixStreamXMLRPCClient

BUNDLE_SOCKET_NAME = "bundle.sock"
bundle_start_lock = asyncio.Lock()


def get_bundle_socket_path(bundle_path, bundle_hash):
    """
    Returns the path to the bundle socket for the specified bundle hash
    """
    return os.path.join(bundle_path, bundle_hash, BUNDLE_SOCKET_NAME)


async def check_or_start_bundle_server(bundle_path, bundle_hash):
    """
    Verifies that the bundle with bundle_hash is running the RPC server, and starts it if not
    """
    working_directory = os.path.join(bundle_path, bundle_hash)

    # Try to connect to the bundle socket, if the socket can't connect assume the server is not running
    # and start the RPC server
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock_path = get_bundle_socket_path(bundle_path, bundle_hash)
    try:
        # Acquire the start lock so that we don't accidentally spawn many servers if called at the same time
        await bundle_start_lock.acquire()

        sock.connect(sock_path)
    except (FileNotFoundError, ConnectionRefusedError):
        # Remove the socket if it exists
        if os.path.exists(sock_path):
            os.remove(sock_path)

        # RPC server is not running for this bundle, so start it
        # Get the path to the server file
        server_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'server.py')

        # Activate the virtual environment then run the server
        args = list2cmdline(['.', os.path.join(working_directory, 'venv', 'bin', 'activate'), ";",
                             'python', server_file])

        p = await asyncio.create_subprocess_shell(
            args,
            cwd=os.path.join(bundle_path, bundle_hash)
        )

        # Clean up the process - we no longer care about it and this prevents the asyncio loops reporting errors
        # during test cleanup
        del p

        # Wait for the server to start up, but timeout after 60 seconds (Should be fairly quick)
        for _ in range(60):
            await asyncio.sleep(1)
            try:
                sock.connect(sock_path)

                # If the socket connected ok, then the remote server is running
                return
            except FileNotFoundError:
                pass

        # The server was unable to start for some reason
        raise Exception(f"Unable to start the RPC server for bundle with hash {bundle_hash}")
    finally:
        # Clean up the socket
        sock.close()

        # Release the lock
        bundle_start_lock.release()


async def run_bundle(bundle_function, bundle_path, bundle_hash, details, job_data):
    """
    Calls a function from a specified bundle.py file with the provided data

    :param job_data: The job parameters to be passed
    :param details: Extra job details to be passed
    :param bundle_function: The function to call
    :param bundle_path: The path to the unpacked bundles
    :param bundle_hash: The hash of the bundle to call

    :return:
    """
    # Make sure that the RPC server is running
    await check_or_start_bundle_server(bundle_path, bundle_hash)

    # Get a client connection to the bundle server
    client = UnixStreamXMLRPCClient(get_bundle_socket_path(bundle_path, bundle_hash))

    # Make the RPC and return teh result
    return getattr(client, bundle_function)(details, job_data)
