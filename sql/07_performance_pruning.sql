/* ============================================================================
   07 — Partition pruning: measure, change one thing, measure again
   ----------------------------------------------------------------------------
   MEASURED RESULTS

     MARKET_DW.STAGING.PRICES_STG  (1.34M rows, written in shuffled order)
       before re-sort :  8 of  8 partitions, 10.79 MB scanned
       after  re-sort :  2 of  8 partitions,  1.80 MB scanned, 647 ms
       SYSTEM$CLUSTERING_INFORMATION before:
         total_partition_count 8, average_overlaps 7.0, average_depth 8.0

     TPCH_SF10.LINEITEM  (60M rows) — same query, two physical orderings
       ORDER BY RANDOM()     : 49 of 49 partitions, 264.34 MB
       ORDER BY L_SHIPDATE   :  3 of 50 partitions, 348 ms

   CONCLUSION
     The 4x improvement on PRICES_STG is real but worth milliseconds — the
     whole table is only 8 micro-partitions, so a full scan costs almost
     nothing and a clustering key would not earn its keep. The behaviour only
     matters at a scale where there is something to skip, which is why the
     experiment was repeated at 60M rows.

     Check total_partition_count BEFORE reaching for a clustering key.
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE QUERY_WH;
USE SCHEMA    MARKET_DW.STAGING;

-- Otherwise you measure the 24h result cache, not real I/O.
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

/* ---------------------------------------------------------------------------
   BEFORE — read Partitions scanned / Partitions total on the TableScan node
   in Query Profile (Snowsight > Query History > the query > Profile).
   ------------------------------------------------------------------------ */
SELECT TICKER, AVG(CLOSE) AS avg_close, COUNT(*) AS n
FROM STAGING.PRICES_STG
WHERE TICKER IN ('AAPL','MSFT','NVDA')
  AND TRADE_DATE BETWEEN '2024-01-01' AND '2024-06-30'
GROUP BY TICKER;
-- 8 of 8 partitions, 10.79 MB

/* average_depth is the number to watch: roughly "how many partitions must I
   open to find one value". Depth 1 is perfect; depth = total is worthless. */
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'MARKET_DW.STAGING.PRICES_STG', '(TICKER, TRADE_DATE)');
-- average_overlaps 7.0, average_depth 8.0 on 8 partitions — the worst case

/* ---------------------------------------------------------------------------
   THE CHANGE — a one-off physical re-sort via CTAS.

   Deliberately NOT  ALTER TABLE ... CLUSTER BY.  Automatic Clustering is a
   serverless background service that consumes credits continuously and can
   take hours to converge. For a table rebuilt in batch, a re-sort gives the
   same pruning benefit for a single, bounded cost. A clustering key is for
   large tables under ONGOING DML, where re-sorting isn't an option.
   ------------------------------------------------------------------------ */
CREATE OR REPLACE TABLE STAGING.PRICES_STG_CLUSTERED AS
SELECT * FROM STAGING.PRICES_STG
ORDER BY TICKER, TRADE_DATE;

/* ---------------------------------------------------------------------------
   AFTER — identical query, re-sorted table.
   ------------------------------------------------------------------------ */
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT TICKER, AVG(CLOSE) AS avg_close, COUNT(*) AS n
FROM STAGING.PRICES_STG_CLUSTERED
WHERE TICKER IN ('AAPL','MSFT','NVDA')
  AND TRADE_DATE BETWEEN '2024-01-01' AND '2024-06-30'
GROUP BY TICKER;
-- 2 of 8 partitions, 1.80 MB, 647 ms
-- Floor is 2 rather than 1 because the three tickers are alphabetically
-- scattered, so their partitions do not fully coincide.

SELECT SYSTEM$CLUSTERING_INFORMATION(
  'MARKET_DW.STAGING.PRICES_STG_CLUSTERED', '(TICKER, TRADE_DATE)');

/* ---------------------------------------------------------------------------
   THE LEADING-COLUMN RULE — pruning evaluates the physical sort order
   LEFT TO RIGHT.

   Sorting by (A, B) groups all of A together, and orders by B within each A:
     filter on A        -> prunes well
     filter on A and B  -> prunes best
     filter on B only   -> prunes badly

   This date-only query against the TICKER-led table should come back close to
   8 of 8 — barely better than unsorted — because every partition holds 2024
   dates for whichever tickers it contains.
   ------------------------------------------------------------------------ */
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT COUNT(*) AS row_cnt, AVG(CLOSE) AS avg_close
FROM STAGING.PRICES_STG_CLUSTERED
WHERE TRADE_DATE BETWEEN '2024-01-01' AND '2024-06-30';

/* ---------------------------------------------------------------------------
   SCALE VALIDATION — 60M rows, where pruning actually pays.
   TPCH sample data ships with every Snowflake account.
   ------------------------------------------------------------------------ */
CREATE OR REPLACE TABLE STAGING.LINEITEM_UNSORTED AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM
ORDER BY RANDOM();

CREATE OR REPLACE TABLE STAGING.LINEITEM_SORTED AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM
ORDER BY L_SHIPDATE;      -- date-only filter => the DATE must lead

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT COUNT(*), SUM(L_EXTENDEDPRICE)
FROM STAGING.LINEITEM_UNSORTED
WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1995-03-31';
-- 49 of 49 partitions, 264.34 MB

SELECT COUNT(*), SUM(L_EXTENDEDPRICE)
FROM STAGING.LINEITEM_SORTED
WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1995-03-31';
-- 3 of 50 partitions, 348 ms
-- Partition totals differ (49 vs 50) because sorting changes how rows pack.

/* Clean up. Set retention to 0 FIRST: a dropped table keeps billing through
   its Time Travel window and then 7 days of non-configurable Fail-safe. */
ALTER TABLE STAGING.LINEITEM_UNSORTED SET DATA_RETENTION_TIME_IN_DAYS = 0;
ALTER TABLE STAGING.LINEITEM_SORTED   SET DATA_RETENTION_TIME_IN_DAYS = 0;
DROP TABLE STAGING.LINEITEM_UNSORTED;
DROP TABLE STAGING.LINEITEM_SORTED;

/* ---------------------------------------------------------------------------
   Storage and spend.
   TABLE_STORAGE_METRICS splits ACTIVE_BYTES / TIME_TRAVEL_BYTES /
   FAILSAFE_BYTES — you pay for all three. (The column is ACTIVE_BYTES, not
   BYTES.) CLONE_GROUP_ID is shared by a table and its clones.
   ------------------------------------------------------------------------ */
SELECT TABLE_NAME, ACTIVE_BYTES, TIME_TRAVEL_BYTES, FAILSAFE_BYTES,
       CLONE_GROUP_ID
FROM MARKET_DW.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA IN ('RAW','STAGING','MARTS');

-- ACCOUNT_USAGE needs ACCOUNTADMIN and lags up to ~3h, but goes back further
-- than the real-time INFORMATION_SCHEMA equivalents.
SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;
