"""
Module for high-level Airflow Job auditing.
"""

import logging

logger = logging.getLogger(__name__)

class AirflowJobAudit:
    """
    Manages high-level job state auditing for Airflow DAGs.
    """

    @staticmethod
    def set_job_started(job_id: str) -> None:
        """
        Logs the start of an Airflow job.
        """
        logger.info("AUDIT: Job %s has been marked as STARTED.", job_id)

    @staticmethod
    def set_job_status(job_id: str, status: str) -> None:
        """
        Logs the completion/failure of an Airflow job.
        """
        logger.info("AUDIT: Job %s has been marked as %s.", job_id, status)
