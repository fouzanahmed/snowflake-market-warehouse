/* ============================================================================
   05 — Idempotency: what actually stops a double load
   ----------------------------------------------------------------------------
   Two independent skip mechanisms, both discovered by breaking them.
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE LOAD_WH;
USE SCHEMA    MARKET_DW.RAW;

/* ---------------------------------------------------------------------------
   TEST 1 — Re-run the identical COPY.

   Result: "Copy executed with 0 files processed."

   Snowflake keeps load metadata per file, per table, for 64 DAYS, keyed on
   filename + ETag. Already-loaded files are skipped. A modified file with the
   same name DOES reload, because the ETag changed.
   ------------------------------------------------------------------------ */
COPY INTO RAW.PRICES_RAW
  (TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME,
   SRC_FILE, SRC_ROW)
FROM (SELECT $1, TO_DATE($2), $3, $4, $5, $6, $7, $8,
             METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
      FROM @STG_PRICES)
FILE_FORMAT = (FORMAT_NAME = FF_CSV_GZ)
ON_ERROR    = 'CONTINUE';

/* ---------------------------------------------------------------------------
   TEST 2 — A SEPARATE skip: PUT compares FILENAMES, not contents.

   Re-running the extract with a wider date range produced larger files with
   the same names. PUT reported SKIPPED for all ten (target_size = 0), so
   nothing reached the stage, so COPY then also found nothing new.

   The fix is not OVERWRITE=TRUE — that would reload all 1.3M rows to pick up
   a few thousand. Real pipelines land NEW FILES with NEW NAMES, usually
   date-partitioned:  s3://bucket/prices/year=2026/month=08/day=11/
   That makes both this skip and the 64-day expiry non-issues.
   ------------------------------------------------------------------------ */


/* ---------------------------------------------------------------------------
   TEST 3 — Break it on purpose with FORCE, then clean up.

   FORCE overrides the load metadata. This duplicates the table to 2,677,698
   rows, which is exactly the condition that makes a MERGE fail with
   "Duplicate row detected during DML action" — one target row matching two
   source rows, and Snowflake refusing to guess which wins.
   ------------------------------------------------------------------------ */
COPY INTO RAW.PRICES_RAW
  (TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME,
   SRC_FILE, SRC_ROW)
FROM (SELECT $1, TO_DATE($2), $3, $4, $5, $6, $7, $8,
             METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
      FROM @STG_PRICES)
FILE_FORMAT = (FORMAT_NAME = FF_CSV_GZ)
ON_ERROR    = 'CONTINUE'
FORCE       = TRUE;

SELECT COUNT(*) AS row_cnt FROM RAW.PRICES_RAW;   -- 2,677,698

-- Keep the most recently loaded version of each (ticker, date)
DELETE FROM RAW.PRICES_RAW
WHERE (TICKER, TRADE_DATE, LOAD_TS) IN (
  SELECT TICKER, TRADE_DATE, LOAD_TS
  FROM (
    SELECT TICKER, TRADE_DATE, LOAD_TS,
           ROW_NUMBER() OVER (PARTITION BY TICKER, TRADE_DATE
                              ORDER BY LOAD_TS DESC) AS rn
    FROM RAW.PRICES_RAW
  )
  WHERE rn > 1
);

SELECT COUNT(*) AS row_cnt FROM RAW.PRICES_RAW;   -- back to 1,338,849

/* ---------------------------------------------------------------------------
   Conclusion: COPY's load metadata is a 64-day convenience, not a guarantee.
   A long-lived pipeline should be idempotent BY DESIGN — via MERGE upserts on
   a natural key — rather than depending on it.
   ------------------------------------------------------------------------ */
