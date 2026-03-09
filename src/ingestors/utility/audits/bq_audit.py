"""
Module for managing BigQuery-based audit logging for ingestion pipelines.
"""

from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery


class BQAuditManager:
    """
    Handles logging of job statuses (RUNNING, COMPLETED, FAILED) to BigQuery.
    """

    def __init__(self, client: bigquery.Client, audit_table: str):
        """
        Initializes the Audit Manager.

        Args:
            client (bigquery.Client): BigQuery client for executing audit inserts.
            audit_table (str): Fully qualified audit table ID.
        """
        self.client = client
        self.audit_table = audit_table

    def log_audit(
        self,
        dw_pren_job_id: str,
        dag_id: str,
        job_name: str,
        status: str,
        target_table: str,
        start_ts: str,
        error_message: Optional[str] = None,
        records: Optional[int] = None,
    ) -> None:
        """
        Inserts an audit record into the centralized BigQuery audit table.
        """
        end_ts = (
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            if status != "RUNNING"
            else None
        )

        sql = f"""
            INSERT INTO `{self.audit_table}`
            (dw_pren_job_id, job_id, load_job_id, source_name, target_name, 
             tgt_rec_count, start_ts, end_ts, status, err_desc, bq_load)
            VALUES (@dw_id, @j_id, @l_id, 'GCS', @t_name, @cnt, 
                    TIMESTAMP(@s_ts), TIMESTAMP(@e_ts), @stat, @err, 'True')
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dw_id", "STRING", dw_pren_job_id),
                bigquery.ScalarQueryParameter("j_id", "STRING", job_name),
                bigquery.ScalarQueryParameter("l_id", "STRING", dag_id),
                bigquery.ScalarQueryParameter("t_name", "STRING", target_table),
                bigquery.ScalarQueryParameter("cnt", "STRING", records),
                bigquery.ScalarQueryParameter("s_ts", "TIMESTAMP", start_ts),
                bigquery.ScalarQueryParameter("e_ts", "TIMESTAMP", end_ts),
                bigquery.ScalarQueryParameter("stat", "STRING", status),
                bigquery.ScalarQueryParameter("err", "STRING", error_message),
            ]
        )
        self.client.query(sql, job_config=job_config).result()
