import asyncio
import pickle

# Create the Lock
lock = asyncio.Lock()
database_filename = 'db.pickle'


def _read_db():
    """
    Read the pickle and return the database

    :return: The read database, or an empty dict
    """
    try:
        with open('db.pickle', 'rb') as f:
            return pickle.load(f)
    except:
        return {}


def _write_db(db):
    """
    Writes the database file as a pickle

    :param db: The dict to write to the database file
    :return: Nothing
    """
    with open(database_filename, 'wb') as f:
        pickle.dump(db, f)


async def get_all_jobs():
    """
    Gets all job records for jobs in the database

    :return: An array of all current jobs in the database
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'jobs' not in db:
            # Create a new job array
            db['jobs'] = []

        # Return the jobs
        return db['jobs']


async def get_job_by_ui_id(ui_id):
    """
    Gets a job record if one exists for the provided ui id

    :param ui_id: The ui id of the job to look up
    :return: The job details if the job was found otherwise None
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Check if the job exists in the database
        if 'jobs' in db:
            for job in db['jobs']:
                if job['ui_id'] == ui_id:
                    # Found the job, return it
                    return job

        # No job matching the criteria was in the database
        return None


async def update_job(new_job):
    """
    Updates a job record in the database if one already exists, otherwise inserts the job in to the database

    :param new_job: The job to update
    :return: None
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'jobs' not in db:
            # Create a new job array
            db['jobs'] = []

        # Iterate over the jobs in the database
        found = False
        for job in db['jobs']:
            # Check if this job matches the job being updated
            if job['ui_id'] == new_job['ui_id']:
                # Found the job, update it
                found = True
                job.update(new_job)

        # If no record was found, insert the job
        if not found:
            db['jobs'].append(new_job)

        # Save the database
        _write_db(db)


async def delete_job(job):
    """
    Deletes a job record from the database

    :param job: The job to delete
    :return: None
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'jobs' not in db:
            return

        # Iterate over the jobs in the database
        for idx in range(len(db['jobs'])):
            # Check if this job matches the job being deleted
            if db['jobs'][idx]['ui_id'] == job['ui_id']:
                # Found the job, delete it
                del db['jobs'][idx]
                break

        # Save the database
        _write_db(db)


async def get_all_queued_jobs(bundle_hash):
    """
    Gets all job records for jobs in the database

    :param bundle_hash: The bundle to get all queued jobs for
    :return: An array of all current jobs in the database
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'queued_jobs' not in db:
            # Create a new job array
            db['queued_jobs'] = {bundle_hash: []}

        # Return the jobs
        return db['queued_jobs'][bundle_hash]


async def queue_job(job_id, bundle_hash, params):
    """
    Adds a job to the queue for a specific bundle

    :param params: The job parameters
    :param bundle_hash: The bundle we're queuing this job for
    :param job_id: The server ID for this job
    :return: None
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'queued_jobs' not in db:
            # Create a new job array
            db['queued_jobs'] = {}

        if bundle_hash not in db['queued_jobs']:
            db['queued_jobs'][bundle_hash] = []

        # If no record was found, insert the job
        db['queued_jobs'][bundle_hash].append({
            'id': job_id,
            'params': params
        })

        # Save the database
        _write_db(db)


async def pop_queued_job(bundle_hash):
    """
    Gets the first queued job in the specified bundle queue

    :param bundle_hash: The hash of the bundle to pop a job for
    :return: An object with id and params if a job existed, or None if there are no more jobs in the queue
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'queued_jobs' not in db:
            return

        if bundle_hash not in db['queued_jobs']:
            return

        # Get the jobs from the database for the specified bundle
        result = None
        jobs = db['queued_jobs'][bundle_hash]
        # Are there any jobs left in the queue?
        if not len(jobs):
            # No, delete the bundle queue from the database
            del db['queued_jobs'][bundle_hash]
        else:
            # Get the first job in the array
            result = jobs[0]
            # Remove the job from the queue
            db['queued_jobs'][bundle_hash] = jobs[1:]

        # Save the database
        _write_db(db)

        # Return the result
        return result


async def is_bundle_queued(bundle_hash):
    """
    Checks to see if we're currently waiting for a bundle to be delivered and unpacked by the server

    :param bundle_hash: The hash of the bundle to check
    :return: True or False if the bundle is queued
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Check if the bundle exists in the database
        if 'queued_bundles' in db:
            for bundle in db['queued_bundles']:
                if bundle == bundle_hash:
                    # Found the bundle, so it's still queued
                    return True

        # No job matching the criteria was in the database
        return False


async def queue_bundle(bundle_hash):
    """
    Markes a bundle as queued in the database

    :param bundle_hash: The bundle hash to mark queued
    :return: None
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a queued_bundles entry already
        if 'queued_bundles' not in db:
            # Create a new job array
            db['queued_bundles'] = []

        # Check if the specified bundle is already queued
        if bundle_hash not in db['queued_bundles']:
            # Nope, add the bundle to the database
            db['queued_bundles'].append(bundle_hash)

        # Save the database
        _write_db(db)
