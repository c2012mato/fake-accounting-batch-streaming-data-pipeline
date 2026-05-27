-- ============================================================
--  Flink 1.20.4 SQL Pipeline
--  Reads transactions_iceberg (written by bridge), builds
--  aggregated summary tables back into Iceberg for Trino.
--
--  Run:
--    docker exec -it flink-jobmanager \
--      /opt/flink/bin/sql-client.sh \
--      -f /opt/flink/sql-jobs/accounting-pipeline.sql
-- ============================================================

-- S3A / MinIO settings (Hadoop FileSystem layer)
SET 'fs.s3a.endpoint'          = 'http://minio:9000';
SET 'fs.s3a.access.key'        = 'admin';
SET 'fs.s3a.secret.key'        = 'password123';
SET 'fs.s3a.path.style.access' = 'true';
SET 'fs.s3a.impl'              = 'org.apache.hadoop.fs.s3a.S3AFileSystem';

-- Run in batch mode (one-shot aggregation over existing data)
SET 'execution.runtime-mode'           = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

-- ── 1. Iceberg catalog (Hadoop catalog on MinIO) ─────────────
CREATE CATALOG iceberg_catalog WITH (
  'type'            = 'iceberg',
  'catalog-type'    = 'hadoop',
  'warehouse'       = 's3a://warehouse',
  'property-version'= '1'
);

USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS accounting;
USE accounting;

-- ── 2. Daily summary table ───────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_summary (
  summary_date      STRING,
  account_category  STRING,
  transaction_type  STRING,
  total_amount      DOUBLE,
  tx_count          BIGINT,
  avg_amount        DOUBLE
) WITH (
  'connector'    = 'iceberg',
  'catalog-name' = 'iceberg_catalog',
  'database-name'= 'accounting',
  'table-name'   = 'daily_summary',
  'write.format.default' = 'parquet'
);

-- ── 3. Category / entry-type summary table ───────────────────
CREATE TABLE IF NOT EXISTS category_summary (
  account_category  STRING,
  entry_type        STRING,
  status            STRING,
  total_amount      DOUBLE,
  tx_count          BIGINT
) WITH (
  'connector'    = 'iceberg',
  'catalog-name' = 'iceberg_catalog',
  'database-name'= 'accounting',
  'table-name'   = 'category_summary',
  'write.format.default' = 'parquet'
);

-- ── 4. Populate daily_summary ────────────────────────────────
INSERT INTO daily_summary
SELECT
  SUBSTRING(transaction_timestamp, 1, 10)  AS summary_date,
  account_category,
  transaction_type,
  SUM(amount)                              AS total_amount,
  COUNT(*)                                 AS tx_count,
  AVG(amount)                              AS avg_amount
FROM transactions_iceberg
GROUP BY
  SUBSTRING(transaction_timestamp, 1, 10),
  account_category,
  transaction_type;

-- ── 5. Populate category_summary ────────────────────────────
INSERT INTO category_summary
SELECT
  account_category,
  entry_type,
  status,
  SUM(amount)  AS total_amount,
  COUNT(*)     AS tx_count
FROM transactions_iceberg
GROUP BY
  account_category,
  entry_type,
  status;
