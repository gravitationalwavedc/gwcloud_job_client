import asyncio
import logging
import os
from subprocess import list2cmdline


async def archive_job(job):
    """
    Archives the output directory of a job in a tar.gz file
    Args:
        job: The job to archive

    Returns:
        Nothing
    """

    # Attempt to call the function from the bundle after sourcing the venv
    args = list2cmdline(['tar', '-cvf', 'archive.tar.gz', '.'])
    p = await asyncio.create_subprocess_shell(
        args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=job.working_directory
    )

    # Wait for the command to finish
    out, err = await p.communicate()

    logging.info(f"Archiving job {job.job_id} completed with code {p.returncode}\n\nStdout: {out}\n\nStderr: {err}\n")
