/* ============================================================================
   08 — Incremental load: stream + task + MERGE [RUN, RESULTS NOT YET RECORDED]
   ----------------------------------------------------------------------------
   Replaces the batch INSERT in sql/06_staging.sql with an incremental path.
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE LOAD_WH;
USE SCHEMA    MARKET_DW.STAGING;

/* A stream is a BOOKMARK, not a queue — an offset into the table's version
   history plus change-tracking metadata. It stores no data.

   Reading a stream in a SELECT does NOT move the offset. Only consuming it
   inside a DML statement that COMMITS advances it. That means you can safely
   inspect a stream before running the pipeline.

   Raw is insert-only here, so APPEND_ONLY would be cheaper; STANDARD is used
   to keep METADATA$ACTION meaningful and the pattern general. */
CREATE OR REPLACE STREAM RAW.PRICES_RAW_STREAM
  ON TABLE RAW.PRICES_RAW
  SHOW_INITIAL_ROWS = TRUE;

SELECT COUNT(*) AS backlog FROM RAW.PRICES_RAW_STREAM;
SELECT SYSTEM$STREAM_HAS_DATA('MARKET_DW.RAW.PRICES_RAW_STREAM');

/* ---------------------------------------------------------------------------
   The task.

   WHEN is evaluated BEFORE the warehouse resumes. If false, the task logs
   SKIPPED and NOTHING IS BILLED — that is the mechanism behind "costs nothing
   on days with no new data", not just the outcome.

   The ROW_NUMBER dedupe is mandatory: Snowflake raises
   "Duplicate row detected during DML action" if one target row matches
   multiple source rows. See sql/05_idempotency_tests.sql for how that
   condition was reproduced on purpose.

   METADATA$ACTION filters the change type. A STANDARD stream represents an
   UPDATE as a DELETE row plus an INSERT row, both flagged
   METADATA$ISUPDATE = TRUE. */
CREATE OR REPLACE TASK STAGING.T_MERGE_PRICES
  WAREHOUSE = LOAD_WH
  SCHEDULE  = 'USING CRON 30 18 * * * Australia/Sydney'   -- after US close
  WHEN SYSTEM$STREAM_HAS_DATA('MARKET_DW.RAW.PRICES_RAW_STREAM')
AS
MERGE INTO STAGING.PRICES_STG t
USING (
  SELECT TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY TICKER, TRADE_DATE
                              ORDER BY LOAD_TS DESC) AS rn
    FROM MARKET_DW.RAW.PRICES_RAW_STREAM s
    WHERE METADATA$ACTION = 'INSERT'
  )
  WHERE rn = 1
) s
ON t.TICKER = s.TICKER AND t.TRADE_DATE = s.TRADE_DATE
WHEN MATCHED THEN UPDATE SET
  t.OPEN = s.OPEN, t.HIGH = s.HIGH, t.LOW = s.LOW, t.CLOSE = s.CLOSE,
  t.ADJ_CLOSE = s.ADJ_CLOSE, t.VOLUME = s.VOLUME,
  t.UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, UPDATED_AT)
VALUES
  (s.TICKER, s.TRADE_DATE, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.ADJ_CLOSE,
   s.VOLUME, CURRENT_TIMESTAMP());

-- Tasks are created SUSPENDED.
ALTER TASK STAGING.T_MERGE_PRICES RESUME;
EXECUTE TASK STAGING.T_MERGE_PRICES;   -- fire now, ignoring the schedule

/* Verify. STATE is SUCCEEDED / FAILED / SKIPPED — run twice with no new data
   and the second should be SKIPPED, which is the WHEN clause working. */
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  TASK_NAME    => 'T_MERGE_PRICES',
  RESULT_LIMIT => 20))
ORDER BY SCHEDULED_TIME DESC;

SELECT COUNT(*) AS row_cnt FROM STAGING.PRICES_STG;
SELECT SYSTEM$STREAM_HAS_DATA('MARKET_DW.RAW.PRICES_RAW_STREAM');  -- now FALSE


/* ---------------------------------------------------------------------------
   DAG extension — children use AFTER instead of SCHEDULE.

   Rules: exactly one root task carries the SCHEDULE; resume CHILDREN FIRST
   and the root LAST (resuming the root starts the clock); a child only runs
   if its parent SUCCEEDS; WHEN on the root gates the entire DAG; and a
   running DAG cannot be altered — suspend the root first.
   ------------------------------------------------------------------------ */
-- CREATE OR REPLACE TASK MARTS.T_BUILD_FEATURES
--   WAREHOUSE = LOAD_WH
--   AFTER STAGING.T_MERGE_PRICES
-- AS
--   CREATE OR REPLACE TABLE MARTS.PRICE_FEATURES AS <see 09_feature_mart.sql>;
--
-- ALTER TASK MARTS.T_BUILD_FEATURES  RESUME;   -- children first
-- ALTER TASK STAGING.T_MERGE_PRICES  RESUME;   -- root last

/* Worth knowing: DYNAMIC TABLES are the modern alternative — declare the end
   state in SQL and Snowflake manages the incremental refresh, collapsing
   stream + task + MERGE into one statement. Streams and tasks give more
   control; dynamic tables are far less code. */
