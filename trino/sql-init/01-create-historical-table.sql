-- ════════════════════════════════════════════════════════════════
-- UNION view for historical + streaming data
-- ════════════════════════════════════════════════════════════════
-- Historical table (transactions_historical) is created by
-- data/load_historical.py via pyiceberg during docker-compose init.
-- This script creates the UNION view on top.

-- Drop view first (if exists) to allow recreation
DROP VIEW IF EXISTS iceberg.accounting.transactions_union;

-- Create UNION view combining historical + streaming
-- No overlap expected (historical ends May 26, live starts May 27)
-- but deduplicate by transaction_id just in case
CREATE OR REPLACE VIEW iceberg.accounting.transactions_union AS
SELECT
  transaction_id,
  amount,
  account_name,
  transaction_type,
  transaction_timestamp,
  account_category,
  entry_type,
  status,
  ingestion_time
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingestion_time ASC) AS rn
  FROM (
    SELECT * FROM iceberg.accounting.transactions_historical
    UNION ALL
    SELECT * FROM iceberg.accounting.transactions_iceberg
  )
)
WHERE rn = 1;
