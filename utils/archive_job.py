import asyncio
import concurrent.futures
import logging
import subprocess


executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=4,
)


async def archive_job(job):
    """
    Archives the output directory of a job in a tar.gz file
    Args:
        job: The job to archive

    Returns:
        Nothing
    """

    def archive_job_impl():
        # Run the tar command in the jobs working directory to create a top level tar file of the job
        p = subprocess.Popen(
            ['tar', '-cvf', 'archive.tar.gz', '.'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=job.working_directory
        )

        # Wait for the command to finish
        out, err = p.communicate()

        return p.returncode, out, err

    # Run the archive function in its own thread to prevent blocking
    blocking_tasks = [
        asyncio.get_event_loop().run_in_executor(executor, archive_job_impl)
    ]

    completed, _ = await asyncio.wait(blocking_tasks)

    returncode, out, err = list(completed)[0].result()

    # Report the output
    logging.info(f"Archiving job {job.job_id} completed with code {returncode}\n\nStdout: {out}\n\nStderr: {err}\n")
