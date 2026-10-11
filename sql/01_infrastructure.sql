/* ============================================================================
   01 — Warehouses, resource monitor, database, schemas
   Run as: ACCOUNTADMIN
   ----------------------------------------------------------------------------
   Two warehouses so that loading and querying cannot contend for compute.
   They share the same storage — that separation is the core of Snowflake's
   architecture.
   ========================================================================= */

USE ROLE ACCOUNTADMIN;

-- Load compute: COPY INTO, MERGE, scheduled tasks
CREATE WAREHOUSE IF NOT EXISTS LOAD_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60      -- billing is per-second after a 60s minimum;
  AUTO_RESUME         = TRUE    -- 60s balances cost against losing the local
  INITIALLY_SUSPENDED = TRUE    -- SSD cache on suspend
  COMMENT = 'Ingestion and transformation compute';

-- Query compute: analyst and BI reads
CREATE WAREHOUSE IF NOT EXISTS QUERY_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Analyst / BI read compute';

/* Resource monitor — hard cap on spend.
   SUSPEND lets running queries finish; SUSPEND_IMMEDIATE kills them. */
CREATE RESOURCE MONITOR IF NOT EXISTS MARKET_RM
  WITH CREDIT_QUOTA = 20
  FREQUENCY         = MONTHLY
  START_TIMESTAMP   = IMMEDIATELY
  TRIGGERS
    ON  75 PERCENT DO NOTIFY
    ON  90 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE LOAD_WH  SET RESOURCE_MONITOR = MARKET_RM;
ALTER WAREHOUSE QUERY_WH SET RESOURCE_MONITOR = MARKET_RM;

-- Three-layer warehouse
CREATE DATABASE IF NOT EXISTS MARKET_DW;
CREATE SCHEMA   IF NOT EXISTS MARKET_DW.RAW;      -- as-landed, append-only
CREATE SCHEMA   IF NOT EXISTS MARKET_DW.STAGING;  -- cleaned, deduplicated
CREATE SCHEMA   IF NOT EXISTS MARKET_DW.MARTS;    -- modelled for consumption
