"""
Load historical CSV from MinIO into a proper Iceberg table.
Run inside the bridge container which has pyiceberg + pyarrow.
"""
import csv
import io
import os
import time

import boto3
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

# ── Config (matches bridge env) ──────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse")
NAMESPACE = "accounting"
TABLE_NAME = "transactions_historical"
BUCKET = "warehouse"
CSV_KEY = "pipeline-v2-historical/historical_transactions.csv"

BATCH_SIZE = 5000

def main():
    print(f"Loading historical data from s3://{BUCKET}/{CSV_KEY}")

    # Read CSV from MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    obj = s3.get_object(Bucket=BUCKET, Key=CSV_KEY)
    csv_text = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    print(f"  Read {len(rows)} rows from CSV")

    # Connect to Iceberg catalog
    catalog = RestCatalog(
        name="rest",
        **{
            "uri": CATALOG_URI,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": "us-east-1",
        },
    )

    # Create namespace if needed
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"  Created namespace '{NAMESPACE}'")
    except Exception:
        print(f"  Namespace '{NAMESPACE}' already exists")

    # Define schema matching transactions_iceberg
    schema = pa.schema([
        ("transaction_id", pa.string()),
        ("amount", pa.float64()),
        ("account_name", pa.string()),
        ("transaction_type", pa.string()),
        ("transaction_timestamp", pa.string()),
        ("account_category", pa.string()),
        ("entry_type", pa.string()),
        ("status", pa.string()),
        ("ingestion_time", pa.string()),
    ])

    # Create or replace the historical table
    full_name = f"{NAMESPACE}.{TABLE_NAME}"
    try:
        catalog.drop_table(full_name)
        print(f"  Dropped existing table '{full_name}'")
    except Exception:
        pass

    table = catalog.create_table(full_name, schema=schema)
    print(f"  Created table '{full_name}'")

    # Load in batches
    total_written = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch_rows = rows[i : i + BATCH_SIZE]

        arrays = {
            "transaction_id": [r["transaction_id"] for r in batch_rows],
            "amount": [float(r["amount"]) for r in batch_rows],
            "account_name": [r["account_name"] for r in batch_rows],
            "transaction_type": [r["transaction_type"] for r in batch_rows],
            "transaction_timestamp": [r["transaction_timestamp"] for r in batch_rows],
            "account_category": [r["account_category"] for r in batch_rows],
            "entry_type": [r["entry_type"] for r in batch_rows],
            "status": [r["status"] for r in batch_rows],
            "ingestion_time": [r["ingestion_time"] for r in batch_rows],
        }

        arrow_table = pa.table(arrays, schema=schema)
        table.append(arrow_table)
        total_written += len(batch_rows)
        print(f"  Written {total_written}/{len(rows)} rows...")

    print(f"\nDone! {total_written} rows loaded into iceberg.{full_name}")


if __name__ == "__main__":
    main()
