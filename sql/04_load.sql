/* ============================================================================
   04 — Upload and load
   ----------------------------------------------------------------------------
   Measured result on this dataset:
       1,338,849 rows across 10 files, 0 errors, 2.4 seconds on an XSMALL.
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE LOAD_WH;
USE SCHEMA    MARKET_DW.RAW;

/* ---------------------------------------------------------------------------
   STEP 1 — PUT the files onto the stage.

   PUT is client-side: it reads from the local filesystem, so it only works
   from SnowSQL or a driver, never from a Snowsight worksheet.

   AUTO_COMPRESS=FALSE because the extract is already gzipped — letting
   SnowSQL compress again produces .gz.gz and the load fails.

   Ten files, not one: COPY INTO parallelises across the warehouse's threads
   and the FILE is the unit of parallelism. An XSMALL has 8 threads, so a
   single large file would load single-threaded no matter how big the
   warehouse. Snowflake's guidance is ~100-250 MB compressed per file.
   ------------------------------------------------------------------------ */
-- PUT file://C:\path\to\data\prices_*.csv.gz @STG_PRICES AUTO_COMPRESS=FALSE;

LIST @STG_PRICES;

/* ---------------------------------------------------------------------------
   STEP 2 — Inspect the files ON the stage before loading them.

   $1, $2 ... are positional columns. Catching a column shift here is free;
   catching it after loading 1.3M rows is not.
   ------------------------------------------------------------------------ */
SELECT $1 AS ticker,
       $2 AS trade_date,
       $6 AS close,
       METADATA$FILENAME
FROM @STG_PRICES (FILE_FORMAT => FF_CSV_GZ)
LIMIT 10;

/* ---------------------------------------------------------------------------
   STEP 3 — Load, capturing lineage metadata.
   ------------------------------------------------------------------------ */
COPY INTO RAW.PRICES_RAW
  (TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME,
   SRC_FILE, SRC_ROW)
FROM (
  SELECT $1, TO_DATE($2), $3, $4, $5, $6, $7, $8,
         METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER
  FROM @STG_PRICES
)
FILE_FORMAT = (FORMAT_NAME = FF_CSV_GZ)
ON_ERROR    = 'CONTINUE';

/* ---------------------------------------------------------------------------
   STEP 4 — Verify. All three, for different reasons.
   ------------------------------------------------------------------------ */

-- Rows REJECTED by the last COPY. ON_ERROR='CONTINUE' drops bad rows
-- silently, so the table looks fine while quietly missing data. In
-- production this output would be persisted to a rejects table and alerted on.
SELECT * FROM TABLE(VALIDATE(RAW.PRICES_RAW, JOB_ID => '_last'));

-- Per-file load audit. ROW_COUNT vs ROW_PARSED shows anything skipped.
SELECT FILE_NAME, ROW_COUNT, ROW_PARSED, ERROR_COUNT, STATUS, LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'MARKET_DW.RAW.PRICES_RAW',
  START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP())));

-- Shape check. NOTE: "rows" is a reserved word in Snowflake (the window-frame
-- keyword), so it cannot be used as a bare column alias.
SELECT COUNT(*)              AS row_cnt,
       COUNT(DISTINCT TICKER) AS tickers,
       MIN(TRADE_DATE)        AS from_dt,
       MAX(TRADE_DATE)        AS to_dt
FROM RAW.PRICES_RAW;
-- 1338849 | 501 | 2015-01-02 | 2025-12-30


/* ---------------------------------------------------------------------------
   APPENDIX — external stage on S3, for reference.

   A storage integration means no AWS keys are ever stored in Snowflake; it is
   an IAM role assumption. After CREATE, run DESC INTEGRATION and copy
   STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into the IAM role's
   trust policy. The external ID is what prevents the confused-deputy problem.

   Not used in this build — the project runs on an internal stage.
   ------------------------------------------------------------------------ */
-- CREATE STORAGE INTEGRATION S3_MARKET_INT
--   TYPE                      = EXTERNAL_STAGE
--   STORAGE_PROVIDER          = 'S3'
--   ENABLED                   = TRUE
--   STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<account>:role/snowflake-s3-role'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/prices/');
--
-- CREATE STAGE STG_PRICES_S3
--   URL                 = 's3://<bucket>/prices/'
--   STORAGE_INTEGRATION = S3_MARKET_INT
--   FILE_FORMAT         = FF_CSV_GZ;
