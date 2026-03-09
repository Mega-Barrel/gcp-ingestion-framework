
"""
Utility for GCS operations.
"""
# import logging
# from typing import List
# from airflow.providers.google.cloud.hooks.gcs import GCSHook

# logger = logging.getLogger(__name__)

# class GCSFileHelper:
#     """
#     Helper to interact with GCS.
#     """

#     @staticmethod
#     def get_file_list(conn_id: str, bucket_name: str, prefix: str) -> List[str]:
#         """
#         Returns full gs:// URIs for files matching the prefix.
#         """
#         logger.info("Fetching connection details for: %s", conn_id)

#         hook = GCSHook(gcp_conn_id = conn_id)

#         logger.info("Listing files in bucket: %s with prefix: %s", bucket_name, prefix)
#         files = hook.list(
#             bucket_name = bucket_name,
#             prefix = prefix
#         )

#         if not files:
#             return []

#         file_uris = [f"gs://{bucket_name}/{f}" for f in files]
#         logger.info("Found %s files.", len(file_uris))

#         return file_uris

import logging
import json
from typing import List
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.oauth2 import service_account

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
        logger.info("Fetching connection for Astro 3.x: %s", conn_id)

        connection = BaseHook.get_connection(conn_id)
        try:
            key_info = connection.extra_dejson 
            if not key_info or "private_key" not in str(key_info):
                key_info = json.loads(connection.extra)
        except Exception as e:
            logger.error("Failed to extract credentials: %s", e)
            raise

        credentials = service_account.Credentials.from_service_account_info(key_info)
        project_id = key_info.get("project_id")

        hook = GCSHook(gcp_conn_id=conn_id)

        def manual_auth():
            logger.info("Injecting manual credentials for project: %s", project_id)
            return credentials, project_id

        hook.get_credentials_and_project_id = manual_auth

        logger.info("Listing files in bucket: %s", bucket_name)

        files = hook.list(bucket_name=bucket_name, prefix=prefix)

        if not files:
            return []

        file_uris = [f"gs://{bucket_name}/{f}" for f in files]
        logger.info("Successfully found %s files.", len(file_uris))

        return file_uris
