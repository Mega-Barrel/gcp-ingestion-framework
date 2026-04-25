"""
Base module providing shared utilities for all cloud ingestion processors.
"""

import logging
from typing import List

from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)

class BaseProcessor:
    """
    Base class providing common Google Cloud operations for ingestors.
    """

    def __init__(self, client: bigquery.Client, conn_id: str):
        """
        Initializes the BaseProcessor.

        Args:
            client (bigquery.Client): The BigQuery client instance.
            conn_id (str): The Airflow connection ID for GCP.
        """
        self.client = client
        self.conn_id = conn_id

    def get_existing_columns(self, table_id: str) -> List[str]:
        """
        Retrieves the list of column names for a specific BigQuery table.

        Args:
            table_id (str): Fully qualified table ID (project.dataset.table).

        Returns:
            List[str]: List of column names. Empty list if table not found.
        """
        try:
            table = self.client.get_table(table_id)
            return [field.name for field in table.schema]
        except Exception as exc:
            logger.warning("Could not fetch columns for %s: %s", table_id, exc)
            return []

    # def get_gcs_headers(self, uri: str, delimiter: str) -> List[str]:
    #     """
    #     Extracts headers from the first line of a GCS file.

    #     Args:
    #         uri (str): The gs:// path to the file.
    #         delimiter (str): The field separator (e.g., ',').

    #     Returns:
    #         List[str]: A list of header strings.
    #     """
    #     hook = GCSHook(gcp_conn_id = self.conn_id)
    #     bucket, blob = uri.replace("gs://", "").split("/", 1)
    #     # Download first 2KB only to save memory and time
    #     byte_data = hook.download(bucket, blob, start_byte = 0, end_byte = 2048)
    #     text_data = byte_data.decode("utf-8")
    #     return text_data.split("\n")[0].strip().split(delimiter)
