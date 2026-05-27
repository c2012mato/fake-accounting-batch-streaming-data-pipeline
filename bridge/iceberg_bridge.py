"""
Kafka → Iceberg Bridge

Consumes accounting transactions from a Kafka topic and appends them as
batches to an Apache Iceberg table stored on MinIO (S3-compatible).

Iceberg is managed via pyiceberg with the REST catalog + S3FileIO backend.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import pyarrow as pa
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, Histogram, start_http_server
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    NestedField,
    StringType,
)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "iceberg-bridge")

MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY    = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY    = os.getenv("MINIO_SECRET_KEY", "password123")
ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
ICEBERG_WAREHOUSE   = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse")
ICEBERG_NAMESPACE   = os.getenv("ICEBERG_NAMESPACE", "accounting")
ICEBERG_TABLE       = os.getenv("ICEBERG_TABLE", "transactions_iceberg")

POLL_INTERVAL    = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "50"))
PROMETHEUS_PORT  = int(os.getenv("PROMETHEUS_PORT", "8001"))

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Prometheus metrics ────────────────────────────────────────────────────────
RECORDS_CONSUMED = Counter(
    "bridge_records_consumed_total",
    "Total records read from Kafka",
)
RECORDS_WRITTEN = Counter(
    "bridge_records_written_total",
    "Total records successfully committed to Iceberg",
)
WRITE_ERRORS = Counter(
    "bridge_write_errors_total",
    "Total failed Iceberg write attempts",
)
BATCH_SIZE_HIST = Histogram(
    "bridge_batch_size_records",
    "Number of records per Iceberg write batch",
    buckets=[1, 5, 10, 25, 50, 100, 200],
)
WRITE_LATENCY = Histogram(
    "bridge_write_duration_seconds",
    "Time spent committing a batch to Iceberg",
    buckets=[.005, .01, .025, .05, .1, .25, .5, 1, 2.5],
)

# ── Iceberg schema matching the transaction payload ────────────────────────────
ICEBERG_SCHEMA = Schema(
    NestedField(1,  "transaction_id",        StringType(),    required=False),
    NestedField(2,  "amount",                DoubleType(),    required=False),
    NestedField(3,  "account_name",          StringType(),    required=False),
    NestedField(4,  "transaction_type",      StringType(),    required=False),
    NestedField(5,  "transaction_timestamp", StringType(),    required=False),
    NestedField(6,  "account_category",      StringType(),    required=False),
    NestedField(7,  "entry_type",            StringType(),    required=False),
    NestedField(8,  "status",                StringType(),    required=False),
    NestedField(9,  "ingestion_time",        StringType(),    required=False),
)

ARROW_SCHEMA = pa.schema([
    pa.field("transaction_id",        pa.string()),
    pa.field("amount",                pa.float64()),
    pa.field("account_name",          pa.string()),
    pa.field("transaction_type",      pa.string()),
    pa.field("transaction_timestamp", pa.string()),
    pa.field("account_category",      pa.string()),
    pa.field("entry_type",            pa.string()),
    pa.field("status",                pa.string()),
    pa.field("ingestion_time",        pa.string()),
])


# ── Kafka readiness wait ───────────────────────────────────────────────────────

def wait_for_kafka(bootstrap_servers: str, retries: int = 40, delay: float = 5.0):
    servers = bootstrap_servers.split(",")
    for attempt in range(1, retries + 1):
        try:
            c = KafkaConsumer(bootstrap_servers=servers, request_timeout_ms=5000)
            c.close()
            log.info("Kafka ready after %d attempt(s).", attempt)
            return
        except NoBrokersAvailable:
            pass
        except Exception:
            pass
        log.info("Waiting for Kafka... (%d/%d)", attempt, retries)
        time.sleep(delay)
    raise RuntimeError(f"Kafka not reachable at {bootstrap_servers}")


# ── Iceberg catalog setup ─────────────────────────────────────────────────────

def init_catalog():
    return load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": ICEBERG_CATALOG_URI,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )


def get_or_create_table(catalog):
    namespace = (ICEBERG_NAMESPACE,)
    table_id  = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE}"

    try:
        catalog.create_namespace(namespace)
        log.info("Created Iceberg namespace: %s", ICEBERG_NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    try:
        table = catalog.load_table(table_id)
        log.info("Loaded existing Iceberg table: %s", table_id)
        return table
    except NoSuchTableError:
        log.info("Creating Iceberg table: %s", table_id)
        table = catalog.create_table(
            identifier=table_id,
            schema=ICEBERG_SCHEMA,
            properties={"write.parquet.compression-codec": "snappy"},
        )
        log.info("Iceberg table created: %s", table_id)
        return table


# ── Write helpers ─────────────────────────────────────────────────────────────

def records_to_arrow(records: list[dict]) -> pa.Table:
    cols = {field.name: [] for field in ARROW_SCHEMA}
    for rec in records:
        cols["transaction_id"].append(rec.get("transaction_id", ""))
        cols["amount"].append(float(rec.get("amount", 0.0)))
        cols["account_name"].append(rec.get("account_name", ""))
        cols["transaction_type"].append(rec.get("transaction_type", ""))
        cols["transaction_timestamp"].append(rec.get("transaction_timestamp", ""))
        cols["account_category"].append(rec.get("account_category", ""))
        cols["entry_type"].append(rec.get("entry_type", ""))
        cols["status"].append(rec.get("status", ""))
        cols["ingestion_time"].append(
            rec.get("ingestion_time", datetime.now(timezone.utc).isoformat())
        )
    return pa.table(cols, schema=ARROW_SCHEMA)


def write_batch(table, records: list[dict]):
    arrow_table = records_to_arrow(records)
    BATCH_SIZE_HIST.observe(len(records))
    with WRITE_LATENCY.time():
        table.append(arrow_table)
    RECORDS_WRITTEN.inc(len(records))
    log.info("Wrote %d records to Iceberg table.", len(records))


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log.info("=== Kafka → Iceberg Bridge starting ===")
    start_http_server(PROMETHEUS_PORT)
    log.info("Prometheus metrics available on :%d/metrics", PROMETHEUS_PORT)

    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    catalog = init_catalog()
    table   = get_or_create_table(catalog)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    total_written = 0
    log.info(
        "Bridge consuming topic='%s', group='%s', batch_size=%d, poll=%.1fs",
        KAFKA_TOPIC, KAFKA_GROUP_ID, BATCH_SIZE, POLL_INTERVAL,
    )

    while True:
        try:
            raw = consumer.poll(
                timeout_ms=int(POLL_INTERVAL * 1000),
                max_records=BATCH_SIZE,
            )
            records = [msg.value for msgs in raw.values() for msg in msgs]

            if records:
                RECORDS_CONSUMED.inc(len(records))
                write_batch(table, records)
                total_written += len(records)
                log.info("Total records written so far: %d", total_written)
            else:
                log.debug("No new messages, polling again in %.1fs...", POLL_INTERVAL)

        except Exception as exc:
            WRITE_ERRORS.inc()
            log.error("Error in bridge loop: %s", exc, exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    main()
