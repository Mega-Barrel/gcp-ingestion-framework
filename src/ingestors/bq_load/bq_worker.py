"""
Module for handling BigQuery Load operations from GCS.
This processor supports staging patterns, schema evolution, and audit logging.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List

from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from typing import cast

from ingestors.utility.audits.bq_audit import BQAuditManager
from ..utility.base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class BQLoadProcessor(BaseProcessor):
    """
    Handles the ingestion of CSV data from GCS into BigQuery tables.

    Attributes:
        config (dict): The DAG configuration dictionary.
        gcs_uris (list): List of GCS URIs to ingest.
        dw_pren_job_id (str): The unique job identifier from Airflow.
        dag_id (str): The ID of the running DAG.
        client (bigquery.Client): Initialized BigQuery client.
        auditor (BQAuditManager): Manager for logging job metadata.
    """

    def __init__(self, config: Dict, gcs_uris: List[str], job_id: str, dag_id: str):
        """
        Initializes the processor with configuration and cloud clients.
        """
        self.config = config
        self.gcs_uris = gcs_uris
        self.dw_pren_job_id = job_id
        self.dag_id = dag_id
        self.project_id = config.get("compute_project")
        self.conn_id = config.get("conn_id", "gcp-conn")

        self.hook = BigQueryHook(gcp_conn_id = self.conn_id)
        self.client = self.hook.get_client(project_id = self.project_id)

        audit_cfg = config.get("audit", {})
        audit_table = (
            f"{self.project_id}.{audit_cfg.get('audit_dataset')}."
            f"{audit_cfg.get('audit_table')}"
        )
        self.auditor = BQAuditManager(self.client, audit_table)

    def execute(self) -> bool:
        """
        Iterates through tasks in config and executes the BQ Load process.

        Returns:
            bool: True if all tasks succeeded, False otherwise.
        """
        results = []
        start_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        for task in self.config.get("tasks", []):
            task_name = task.get("task_name")
            target_cfg = task["targets"][0]["bigquery"]
            target_table = (
                f"{self.project_id}.{target_cfg['target_dataset']}."
                f"{target_cfg['target_table']}"
            )

            self.auditor.log_audit(
                dw_pren_job_id = self.dw_pren_job_id,
                dag_id = self.dag_id,
                job_name = task.get("job_name"),
                status = "RUNNING",
                target_table = target_table,
                start_ts = start_ts
            )

            try:
                rows = self._process_load(task, target_table)
                self.auditor.log_audit(
                    dw_pren_job_id = self.dw_pren_job_id,
                    dag_id = self.dag_id,
                    job_name = task.get("job_name"),
                    status = "COMPLETED",
                    target_table = target_table,
                    start_ts = start_ts,
                    records = rows
                )
                results.append(True)

            except Exception as exc:
                error_msg = str(exc).replace("'", "")[:500]
                self.auditor.log_audit(
                    dw_pren_job_id = self.dw_pren_job_id,
                    dag_id = self.dag_id,
                    job_name = task.get("job_name"),
                    status = "FAILED",
                    target_table = target_table,
                    start_ts = start_ts,
                    error_message = error_msg
                )
                logger.error("Task %s failed: %s", task_name, error_msg)
                results.append(False)

        return all(results)

    def _ensure_metadata_columns(self, table_id: str):
        """
        Ensures file_name and insert_ts exist in the target table.
        """
        existing = self.get_existing_columns(table_id)
        table = self.client.get_table(table_id)
        new_schema = list(table.schema)
        modified = False

        if "insert_ts" not in existing:
            new_schema.append(bigquery.SchemaField("insert_ts", "TIMESTAMP"))
            modified = True
        if "file_name" not in existing:
            new_schema.append(bigquery.SchemaField("file_name", "STRING"))
            modified = True

        if modified:
            table.schema = new_schema
            self.client.update_table(table, ["schema"])

    def _process_load(self, task: Dict, target_table: str) -> int:
        """
        Modified method to use External Table definition to access _FILE_NAME.
        """
        delim_cfg = task.get("delimited_file_props", {})
        target_cfg = task["targets"][0]["bigquery"]

        self._ensure_metadata_columns(target_table)

        target_schema = self.client.get_table(target_table).schema
        content_fields = [
            f.name for f in target_schema if f.name not in ["insert_ts", "file_name"]
        ]

        external_config = bigquery.ExternalConfig("CSV")
        external_config.source_uris = self.gcs_uris

        csv_options = cast(bigquery.CSVOptions, external_config.options)

        csv_options.skip_leading_rows = 1 if delim_cfg.get("has_header") else 0
        csv_options.field_delimiter = delim_cfg.get("delimiter", ",")
        csv_options.allow_jagged_rows = target_cfg.get("jagged_rows_allowed", True)

        external_config.schema = [
            bigquery.SchemaField(n, "STRING") for n in content_fields
        ]

        temp_ext_table_id = f"ext_{uuid.uuid4().hex[:8]}"

        job_config = bigquery.QueryJobConfig(
            table_definitions={temp_ext_table_id: external_config}
        )

        cols_sql = ", ".join([f"`{n}`" for n in content_fields])

        merge_sql = f"""
            INSERT INTO `{target_table}` ({cols_sql}, file_name, insert_ts)
            SELECT {cols_sql}, _FILE_NAME, CURRENT_TIMESTAMP()
            FROM `{temp_ext_table_id}`
        """

        logger.info("Executing load via External Table workaround for _FILE_NAME")
        query_job = self.client.query(merge_sql, job_config=job_config)
        query_job.result()

        inserted_rows = query_job.num_dml_affected_rows or 0
        return inserted_rows

def bq_load_ingestion_runner(config: Dict, gcs_uris: List[str], job_id: str, dag_id: str):
    """
    Function-based entry point for Airflow Tasks.
    """
    processor = BQLoadProcessor(config, gcs_uris, job_id, dag_id)
    return processor.execute()
