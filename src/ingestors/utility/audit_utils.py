
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

class AuditUtils:
    """
    Manages audit metadata and Job IDs across Airflow tasks.
    """

    @staticmethod
    def get_job_id(context: Dict[str, Any]) -> str:
        """
        Retrieves the native Airflow run_id to use as dw_pren_job_id.
        """
        dag_run = context.get("dag_run")
        if not dag_run:
            run_id = context.get("run_id")
            if not run_id:
                raise ValueError("Could not retrieve run_id from job context.")
            return run_id
        return dag_run.run_id
