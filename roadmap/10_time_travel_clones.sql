/* ============================================================================
   10 — Time Travel and zero-copy clones        [RUN, RESULTS NOT YET RECORDED]
   ----------------------------------------------------------------------------
   Time Travel : user-accessible, 0-90 days on Enterprise (1 day on Standard)
   Fail-safe   : a FURTHER 7 days, non-configurable, Snowflake Support only
   Both consume storage.
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE QUERY_WH;
USE SCHEMA    MARKET_DW.MARTS;

-- Break something on purpose
DELETE FROM MARTS.PRICE_FEATURES WHERE TICKER = 'AAPL';
SELECT COUNT(*) AS row_cnt FROM MARTS.PRICE_FEATURES WHERE TICKER = 'AAPL';

-- Three ways back
SELECT COUNT(*) FROM MARTS.PRICE_FEATURES AT(OFFSET => -300)
WHERE TICKER = 'AAPL';                              -- 5 minutes ago

SELECT COUNT(*) FROM MARTS.PRICE_FEATURES
  BEFORE(STATEMENT => '<query_id_of_the_delete>')
WHERE TICKER = 'AAPL';                              -- just before that stmt

-- Restore
INSERT INTO MARTS.PRICE_FEATURES
SELECT * FROM MARTS.PRICE_FEATURES
  BEFORE(STATEMENT => '<query_id_of_the_delete>')
WHERE TICKER = 'AAPL';

DROP TABLE MARTS.PRICE_FEATURES;
UNDROP TABLE MARTS.PRICE_FEATURES;

ALTER TABLE MARTS.PRICE_FEATURES SET DATA_RETENTION_TIME_IN_DAYS = 7;

/* ---------------------------------------------------------------------------
   Zero-copy clone.

   The clone references the SAME immutable micro-partitions — nothing is
   copied and no storage is billed at creation. Storage diverges only as
   either side is modified (copy-on-write), because micro-partitions are
   immutable: a write creates new ones and the clone keeps pointing at the old.
   ------------------------------------------------------------------------ */
CREATE OR REPLACE TABLE MARTS.PRICE_FEATURES_ASOF_20250630
  CLONE MARTS.PRICE_FEATURES;

-- Clone a whole schema at a point in time
CREATE SCHEMA MARKET_DW.MARTS_SNAPSHOT CLONE MARKET_DW.MARTS
  AT(OFFSET => -600);

/* Proof: near-zero ACTIVE_BYTES on the clone despite holding the full
   dataset. A table and its clones share a CLONE_GROUP_ID. */
SELECT TABLE_NAME, ACTIVE_BYTES, TIME_TRAVEL_BYTES, FAILSAFE_BYTES,
       CLONE_GROUP_ID
FROM MARKET_DW.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA IN ('MARTS','MARTS_SNAPSHOT');

/* ---------------------------------------------------------------------------
   TWO SEPARATE MECHANISMS, often conflated:

     No-lookahead comes from the TRADE_DATE filter and the backward-only
     window frames in 09_feature_mart.sql.

     The clone gives REPRODUCIBILITY — a named, frozen, storage-free copy of
     the mart as it existed at run time, so a backtest run today and re-run in
     six months returns identical results even though the mart has been
     rebuilt many times since.
   ------------------------------------------------------------------------ */
