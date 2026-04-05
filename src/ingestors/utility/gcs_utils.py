
"""
Utility for GCS operations.
"""
import logging
from typing import List
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)

class GCSFileHelper:
    """
    Helper to interact with GCS.
    """

    @staticmethod
    def get_file_list(conn_id: str, bucket_name: str, prefix: str) -> List[str]:
        """
        Returns full gs:// URIs for files matching the prefix.
        """
        logger.info("Fetching connection details for: %s", conn_id)

        hook = GCSHook(gcp_conn_id = conn_id)

        logger.info("Listing files in bucket: %s with prefix: %s", bucket_name, prefix)
        files = hook.list(
            bucket_name = bucket_name,
            prefix = prefix
        )

        if not files:
            return []

        file_uris = [f"gs://{bucket_name}/{f}" for f in files]
        logger.info("Found %s files.", len(file_uris))

        return file_uris
