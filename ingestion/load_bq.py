"""
BQ Load Script
==============
Loads AHRF Parquet files from GCS into BigQuery.
One table per release year, truncate and reload on each run.

Pipeline: GCS Parquet → BigQuery raw layer

Usage:
    python ingestion/load_bq.py

Expects:
    - .env file in project root with GCS_BUCKET_NAME and GCP_PROJECT_ID
    - Parquet files already uploaded to GCS by ingest_ahrf.py
    - BigQuery dataset already exists
"""

import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_DATASET = "ahrf_raw"

RELEASES = ["2022_2023", "2023_2024", "2024_2025"]


def load_release(client: bigquery.Client, release: str) -> None:
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.ahrf_{release}"
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/raw/ahrf_{release}.parquet"

    print(f"Loading {gcs_uri} → {table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()

    table = client.get_table(table_id)
    print(f"  Done. Rows: {table.num_rows}, Columns: {len(table.schema)}")


def main():
    print("AHRF BQ Load")
    print(f"Project: {GCP_PROJECT_ID}")
    print(f"Bucket:  {GCS_BUCKET_NAME}")
    print(f"Dataset: {BQ_DATASET}")

    client = bigquery.Client(project=GCP_PROJECT_ID)

    for release in RELEASES:
        load_release(client, release)

    print("\nAll releases loaded.")


if __name__ == "__main__":
    main()
