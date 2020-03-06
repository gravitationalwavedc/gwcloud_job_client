import os


class Scheduler:
    """
    Base stub scheduler - this class should be inherited and extended
    """

    def __init__(self, ui_id, job_id):
        """
        Initialises the job scheduler

        :param ui_id: The UI ID of the job
        :param job_id: The scheduler job id
        """
        self.ui_id = ui_id
        self.job_id = job_id

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

        :param job_id: The id of the job to cancel
        :return: Nothing
        """
        raise NotImplementedError()

    def delete_data(self):
        """
        Delete all job data

        :return: Nothing
        """
        raise NotImplementedError()
