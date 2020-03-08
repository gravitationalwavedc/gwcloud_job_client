import os


class Scheduler:
    """
    Base stub scheduler - this class should be inherited and extended
    """

    def __init__(self, job_id, scheduler_id, working_directory):
        """
        Initialises the job scheduler

        :param working_directory: The working directory for this job
        :param job_id: The UI ID of the job
        :param scheduler_id: The scheduler job id
        """
        self.job_id = job_id
        self.scheduler_id = scheduler_id
        self.working_directory = working_directory

    def submit(self, script):
        """
        Used to submit a job on the cluster

        :param script: The submission script for this job
        :return: An integer identifier for the submitted job
        """
        raise NotImplementedError()

    def status(self):
        """
        Get the status of a job

        :return: A tuple with JobStatus, additional info as a string
        """
        raise NotImplementedError()

    def cancel(self):
        """
        Cancel a running job

        :return: Nothing
        """
        raise NotImplementedError()

    def delete_data(self):
        """
        Delete all job data

        :return: Nothing
        """
        raise NotImplementedError()
