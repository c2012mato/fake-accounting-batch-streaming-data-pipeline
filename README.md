# Real-Time Accounting Data Pipeline  v2

Same streaming core as v1 — Faker → Kafka → Bridge → Iceberg → Trino — with two key upgrades:

- **Grafana** replaces Evidence for dashboards. Every panel queries Trino live on refresh; no `evidence sources` snapshot step required.
- **Prometheus** is added for ETL monitoring. The producer (`:8000`) and bridge (`:8001`) expose `/metrics`; Prometheus scrapes them every 15 s and Grafana visualises pipeline health alongside business data.

## Architecture

```
Faker Producer  ──► Kafka 7.7.0 (KRaft)  ──► Kafka→Iceberg Bridge
  :8000/metrics                                  :8001/metrics
       │                                               │
       └──────────► Prometheus :9090 ◄────────────────┘
                         │
                    Grafana :3000  ◄──── Trino :8889  ◄──── Iceberg (MinIO)
                    (live dashboards)   (query engine)       :9000 / :9001

Flink :18081  ──── optional batch SQL over Iceberg (manual trigger)
```

## Version Manifest

| Component | Version |
|---|---|
| Kafka (cp-kafka) | 7.7.0 (Kafka 3.7, KRaft) |
| MinIO | RELEASE.2025-09-07T16-13-09Z |
| Flink | 1.20.4-scala_2.12-java11 |
| Trino | 480 |
| Python | 3.12.9-slim |
| Prometheus | v2.55.1 |
| Grafana | 11.3.0 |

## Quick Start

```bash
cd real-time-accounting-data-pipeline_1/pipeline-v2
docker-compose up -d --build
```

First run downloads images and builds two custom images (~5 min). All services start in dependency order. No manual `evidence sources` step — Grafana queries Trino live on every panel refresh.

## Service URLs

| Service | URL | Auth |
|---|---|---|
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | open |
| **Producer metrics** | http://localhost:8000/metrics | open |
| **Bridge metrics** | http://localhost:8001/metrics | open |
| **MinIO Console** | http://localhost:9001 | admin / password123 |
| **Iceberg REST** | http://localhost:8181 | open |
| **Trino UI** | http://localhost:8889 | user: admin |
| **Flink UI** | http://localhost:18081 | open |

## Dashboards

Open **http://localhost:3000** (admin / admin). Two dashboards are pre-provisioned:

### Accounting Business Dashboard (Trino source, refreshes every 30 s)
- **KPI row** — Total Revenue, Total Expense, Net Profit, Transaction Count
- **Daily Revenue vs Expense** — time-series line chart, last 24 h by default
- **Amount by Category** — bar chart of posted amounts per account category
- **Transaction Type Mix** — bar chart of counts per type (SALE / REFUND / etc.)
- **Status Distribution** — bar chart of counts per status
- **Recent Transactions** — sortable table, last 100 rows

### ETL Pipeline Monitoring (Prometheus source, refreshes every 10 s)
- **Produce Rate** — messages/s from Faker to Kafka
- **Bridge Write Rate** — records/s committed to Iceberg
- **Pipeline Lag** — produce rate minus write rate (should hover near 0)
- **Write Latency p50 / p99** — Iceberg commit time histogram
- **Producer / Bridge Errors** — stat panels, threshold red at ≥ 1
- **Avg Batch Size** — mean records per Iceberg write
- **Total Produced** — all-time counter

## Prometheus Targets

http://localhost:9090/targets shows four scrape jobs:

| Job | Endpoint | Metrics |
|---|---|---|
| `faker-producer` | faker-producer:8000 | `transactions_produced_total`, `transaction_errors_total`, `transaction_emit_duration_seconds` |
| `bridge` | kafka-iceberg-bridge:8001 | `bridge_records_consumed_total`, `bridge_records_written_total`, `bridge_write_errors_total`, `bridge_batch_size_records`, `bridge_write_duration_seconds` |
| `trino` | trino:8080/v1/metrics | Trino JVM + query engine internals |
| `minio` | minio:9000/minio/v2/metrics/cluster | Storage, I/O, bucket stats |

## Data Setup

The pipeline uses two data sources:

### 1. Historical Data (Jan 1 — May 26, 2026)

**Auto-generated and loaded on first `docker-compose up`:**

```bash
# Generate synthetic historical transactions (~500 rows, ~150K total)
cd pipeline-v2/data
python generate_historical_data.py
# Output: historical_transactions.csv

# During docker-compose up, minio-init uploads this CSV to MinIO:
# s3://warehouse/pipeline-v2-historical/historical_transactions.csv
```

The CSV is then loaded into an Iceberg table (`iceberg.accounting.transactions_historical`) by the bridge container on first startup.

### 2. Live Streaming Data (May 27 onwards)

The Faker producer starts immediately and streams transactions to Kafka → Bridge → Iceberg table (`iceberg.accounting.transactions_iceberg`).

### 3. Unified View (Historical + Streaming)

After both sources exist, Trino combines them via a UNION view:

```sql
-- Automatically created if missing:
CREATE OR REPLACE VIEW iceberg.accounting.transactions_union AS
  SELECT * FROM iceberg.accounting.transactions_historical
  UNION ALL
  SELECT * FROM iceberg.accounting.transactions_iceberg
  -- with deduplication by transaction_id
```

**Grafana dashboards query this view**, so they show the complete historical + live picture.

**If the view doesn't exist**, create it manually:

```bash
docker exec trino trino --execute "CREATE OR REPLACE VIEW iceberg.accounting.transactions_union AS SELECT ... FROM iceberg.accounting.transactions_historical UNION ALL SELECT ... FROM iceberg.accounting.transactions_iceberg"
```

## Verifying data flow

```bash
# Faker producing
curl -s http://localhost:8000/metrics | grep transactions_produced_total

# Bridge writing
curl -s http://localhost:8001/metrics | grep bridge_records_written_total

# Trino sees both historical + streaming rows
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.accounting.transactions_union"
```

## Optional: Run Flink batch aggregation

```bash
MSYS_NO_PATHCONV=1 docker exec -it flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql-jobs/accounting-pipeline.sql
```

This writes `daily_summary` and `category_summary` Iceberg tables queryable at `iceberg.accounting.*` in Trino — and immediately visible in Grafana if you add a panel.

## Tuning

```yaml
# docker-compose.yml — faker service
EMIT_INTERVAL_SECONDS: "0.5"   # faster: 2 msg/s → 0.5 s interval

# docker-compose.yml — bridge service
POLL_INTERVAL_SECONDS: "2"
BATCH_SIZE: "100"
```

## Stopping

```bash
docker-compose down          # stop, keep volumes
docker-compose down -v       # stop + wipe all data
```

## Differences from v1

| | v1 (Evidence) | v2 (Grafana) |
|---|---|---|
| Dashboard freshness | Snapshot — stale until `evidence sources` re-run | Live — Trino queried on every 30 s refresh |
| ETL monitoring | None | Prometheus + Grafana ETL dashboard |
| Bridge metrics | None | 5 Prometheus metrics on :8001 |
| Extra services | — | Prometheus, Grafana |
| Port :3000 | Evidence | Grafana |
