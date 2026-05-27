"""
Faker Transaction Producer → Apache Kafka

Generates synthetic accounting transactions using Faker and publishes them
to a Kafka topic every EMIT_INTERVAL_SECONDS.
Prometheus metrics are exposed on :8000/metrics.
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, Histogram, start_http_server

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "transactions")
EMIT_INTERVAL = float(os.getenv("EMIT_INTERVAL_SECONDS", "2"))
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Prometheus metrics ───────────────────────────────────────────────────────
transactions_produced = Counter(
    "transactions_produced_total",
    "Total number of transactions sent to Kafka",
)
transaction_errors = Counter(
    "transaction_errors_total",
    "Total number of producer errors",
)
emit_duration = Histogram(
    "transaction_emit_duration_seconds",
    "Time spent sending a transaction to Kafka",
)

# ── Faker setup ───────────────────────────────────────────────────────────────
fake = Faker()

TRANSACTION_TYPES = ["SALE", "REFUND", "PAYMENT", "FEE", "TRANSFER"]
ACCOUNT_CATEGORIES = ["REVENUE", "EXPENSE", "ASSET", "LIABILITY", "EQUITY"]
ENTRY_TYPES = ["DEBIT", "CREDIT"]
STATUSES = ["PENDING", "POSTED", "REVERSED"]


def generate_transaction() -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "amount": round(fake.pyfloat(min_value=10, max_value=10000, right_digits=2), 2),
        "account_name": fake.company(),
        "transaction_type": fake.random_element(TRANSACTION_TYPES),
        "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
        "account_category": fake.random_element(ACCOUNT_CATEGORIES),
        "entry_type": fake.random_element(ENTRY_TYPES),
        "status": fake.random_element(STATUSES),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
    }


def wait_for_kafka(bootstrap_servers: str, retries: int = 30, delay: float = 5.0):
    """Block until at least one Kafka broker is reachable."""
    servers = bootstrap_servers.split(",")
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(bootstrap_servers=servers, request_timeout_ms=5000)
            p.close()
            log.info("Kafka is reachable after %d attempt(s).", attempt)
            return
        except NoBrokersAvailable:
            pass
        except Exception:
            pass
        log.info("Waiting for Kafka... attempt %d/%d", attempt, retries)
        time.sleep(delay)
    raise RuntimeError(f"Kafka did not become available at {bootstrap_servers}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log.info("Starting Prometheus metrics server on port %d", PROMETHEUS_PORT)
    start_http_server(PROMETHEUS_PORT)

    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    log.info(
        "Producer started — sending to topic='%s' every %.1fs",
        KAFKA_TOPIC, EMIT_INTERVAL,
    )

    while True:
        tx = generate_transaction()
        start = time.perf_counter()
        try:
            producer.send(KAFKA_TOPIC, value=tx).get(timeout=10)
            elapsed = time.perf_counter() - start
            emit_duration.observe(elapsed)
            transactions_produced.inc()
            log.info(
                "Sent [%s] amount=%.2f type=%s status=%s (%.3fs)",
                tx["transaction_id"][:8],
                tx["amount"],
                tx["transaction_type"],
                tx["status"],
                elapsed,
            )
        except Exception as exc:
            transaction_errors.inc()
            log.error("Failed to send transaction: %s", exc)
            time.sleep(5)

        time.sleep(EMIT_INTERVAL)


if __name__ == "__main__":
    main()
