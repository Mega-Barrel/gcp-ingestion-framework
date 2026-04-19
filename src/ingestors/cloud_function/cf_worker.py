
import os
import logging
import uuid
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def trigger_cloud_run_job():
    project_id = os.environ["PROJECT_ID"]
    source_uri = os.environ["FILE_URI"]
    target_table = os.environ["TARGET_TABLE_ID"]

    file_path = "/".join(source_uri.split("/")[3:])
    file_name = file_path.split("/")[-1]

    dataset_id = target_table.split(".")[1]
    staging_table = f"{project_id}.{dataset_id}.staging_{uuid.uuid4().hex[:8]}"

    bq_client = bigquery.Client(project=project_id)

    try:
        logging.info(f"Starting direct BigQuery ELT ingestion for {source_uri}")

        target_table_def = bq_client.get_table(target_table)

        file_schema = target_table_def.schema[:-2]

        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.CSV,
            field_delimiter = "|",
            autodetect = False,
            schema = file_schema,
            skip_leading_rows = 1,
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        logging.info(f"Loading raw data into staging table: {staging_table}")
        load_job = bq_client.load_table_from_uri(
            source_uri, staging_table, job_config=job_config
        )
        load_job.result()

        loaded_rows = load_job.output_rows
        logging.info(f"DQ Check: Staging table successfully loaded with {loaded_rows} rows.")

        if loaded_rows == 0:
            raise ValueError(f"Data Quality Alert: Source file {file_name} is empty. Aborting insertion.")

        query = f"""
            INSERT INTO `{target_table}`
            SELECT 
                *,
                @file_name AS file_name,
                CURRENT_TIMESTAMP() AS insert_ts
            FROM `{staging_table}`
        """

        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_name", "STRING", file_name)
            ]
        )

        logging.info("Appending audit columns and moving to target partitioned table...")
        query_job = bq_client.query(query, job_config=query_config)
        query_job.result()

        logging.info(f"Successfully processed and loaded {loaded_rows} rows into {target_table}")

    except Exception as e:
        logging.error(f"Ingestion pipeline failed: {str(e)}")
        raise

    finally:
        logging.info("Cleaning up staging table...")
        bq_client.delete_table(staging_table, not_found_ok=True)

if __name__ == "__main__":
    trigger_cloud_run_job()
