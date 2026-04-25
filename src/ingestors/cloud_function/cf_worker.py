import os
import sys
import csv
import json
import logging
from datetime import datetime, timezone
from google.cloud import bigquery
from google.cloud import storage

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

def process_single_file(bq_client, storage_client, source_uri, file_props, target_props, project_id):
    delimiter = file_props.get("delimiter", ",")
    has_header = file_props.get("has_header", True)
    allow_jagged = target_props.get("jagged_rows_allowed", True)
    target_table_id = f"{project_id}.{target_props['target_dataset']}.{target_props['target_table']}"
    
    bucket_name = source_uri.split("/")[2]
    blob_name = "/".join(source_uri.split("/")[3:])
    file_name = blob_name.split("/")[-1]

    file_metrics = {
        "file_name": file_name,
        "processed_records": 0,
        "invalid_records": 0,
        "status": "SUCCESS",
        "error": None
    }

    local_path = f"/tmp/{file_name}"

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(local_path)

        rows_to_stream = []
        batch_size = 5000

        with open(local_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=delimiter)

            table = bq_client.get_table(target_table_id)
            schema_cols = [field.name for field in table.schema if field.name not in ['file_name', 'insert_ts']]
            expected_count = len(schema_cols)

            if has_header:
                next(reader)

            for row in reader:
                # DQ Check: Compare actual row length to BigQuery schema length
                if not allow_jagged and len(row) != expected_count:
                    file_metrics["invalid_records"] += 1
                    continue

                # Dynamic row-level timestamp
                row_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

                # Handle Jagged Rows (Padding/Truncating)
                if len(row) < len(schema_cols):
                    row += [None] * (len(schema_cols) - len(row))

                row_dict = dict(zip(schema_cols, row[:len(schema_cols)]))

                row_dict['file_name'] = file_name
                row_dict['insert_ts'] = row_ts

                rows_to_stream.append(row_dict)

                if len(rows_to_stream) >= batch_size:
                    errors = bq_client.insert_rows_json(target_table_id, rows_to_stream)
                    if errors:
                        raise RuntimeError(f"BQ Streaming error: {errors}")
                    file_metrics["processed_records"] += len(rows_to_stream)
                    rows_to_stream = []

            if rows_to_stream:
                errors = bq_client.insert_rows_json(target_table_id, rows_to_stream)
                if errors:
                    raise RuntimeError(f"BQ Streaming error: {errors}")
                file_metrics["processed_records"] += len(rows_to_stream)

    except Exception as e:
        file_metrics["status"] = "FAILED"
        file_metrics["error"] = str(e)
        logging.error(f"Error processing {file_name}: {str(e)}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

    return file_metrics

def main():
    project_id = os.environ.get("PROJECT_ID")
    source_uris = os.environ.get("FILE_URI", "").split(",")
    file_props = json.loads(os.environ.get("FILE_PROPS", "{}"))
    target_props = json.loads(os.environ.get("TARGET_PROPS", "{}"))

    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)

    results = []
    for uri in source_uris:
        if not uri.strip():
            continue
        logging.info(f"Triggering ingestion for: {uri}")
        report = process_single_file(bq_client, storage_client, uri, file_props, target_props, project_id)
        results.append(report)

    print(f"JOB_RESULT:{json.dumps(results)}")

    if any(r["status"] == "FAILED" for r in results):
        sys.exit(1)

if __name__ == "__main__":
    main()
