/* ============================================================================
   12 — Star schema and Slowly Changing Dimension Type 2     [NOT YET RUN]
   ----------------------------------------------------------------------------
   FACT       = what you measure. Numeric, many rows. Goes after SUM().
   DIMENSION  = what you slice by. Descriptive, few rows. Goes after GROUP BY.

   GRAIN of FCT_DAILY_PRICE: one row per ticker per trading day.

   Star (one fact, denormalised dimensions around it) is preferred over
   snowflake (further-normalised dimensions) because storage is cheap and
   joins are not.

   SURROGATE KEYS are generated integers; natural keys are real-world IDs
   (here, the ticker). Surrogates are needed because natural keys change, they
   join faster — and above all because SCD2 makes the natural key non-unique.
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE LOAD_WH;
USE SCHEMA    MARKET_DW.MARTS;

/* ---------------------------------------------------------------------------
   DIM_DATE — a conformed dimension: the same date dimension can serve every
   fact table in the warehouse.
   ------------------------------------------------------------------------ */
CREATE OR REPLACE TABLE MARTS.DIM_DATE AS
SELECT
  TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))        AS DATE_KEY,
  d                                        AS FULL_DATE,
  YEAR(d)                                  AS CAL_YEAR,
  QUARTER(d)                               AS CAL_QUARTER,
  MONTH(d)                                 AS CAL_MONTH,
  MONTHNAME(d)                             AS MONTH_NAME,
  DAYOFWEEK(d)                             AS DAY_OF_WEEK,
  DAYNAME(d)                               AS DAY_NAME,
  IFF(DAYOFWEEK(d) IN (0,6), FALSE, TRUE)  AS IS_WEEKDAY
FROM (
  SELECT DATEADD(day, SEQ4(), '2015-01-01'::DATE) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 4500))
)
WHERE d <= '2026-12-31';

/* ---------------------------------------------------------------------------
   DIM_TICKER as SCD TYPE 2.

   Type 1 would overwrite a changed sector, which retroactively rewrites every
   historical report. Type 2 closes the old row and inserts a new one, so
   history survives.

   VALID_TO uses 9999-12-31 rather than NULL so BETWEEN works without
   special-casing. IS_CURRENT is redundant but makes the common query trivial.
   ------------------------------------------------------------------------ */
CREATE OR REPLACE SEQUENCE MARTS.SEQ_TICKER_KEY START = 1 INCREMENT = 1;

CREATE OR REPLACE TABLE MARTS.DIM_TICKER (
  TICKER_KEY  NUMBER       NOT NULL,   -- surrogate
  TICKER      STRING       NOT NULL,   -- natural, NOT unique under SCD2
  COMPANY_NAME STRING,
  SECTOR      STRING,
  ROW_HASH    STRING,                  -- hash of tracked attributes
  VALID_FROM  DATE         NOT NULL,
  VALID_TO    DATE         NOT NULL,
  IS_CURRENT  BOOLEAN      NOT NULL
);

-- Seed (load real sectors from the Wikipedia table the extract already pulls)
INSERT INTO MARTS.DIM_TICKER
SELECT MARTS.SEQ_TICKER_KEY.NEXTVAL,
       s.TICKER, s.COMPANY_NAME, s.SECTOR,
       MD5(CONCAT_WS('|', s.COMPANY_NAME, s.SECTOR)),
       '2015-01-01'::DATE, '9999-12-31'::DATE, TRUE
FROM STAGING.TICKER_STG s;

/* ---------------------------------------------------------------------------
   THE SCD2 MERGE — TWO statements. This is the part people forget.

   Comparing every tracked column gets unwieldy, so production code hashes
   them and compares one value.
   ------------------------------------------------------------------------ */

-- 1) Expire rows whose tracked attributes changed
MERGE INTO MARTS.DIM_TICKER t
USING STAGING.TICKER_STG s
  ON t.TICKER = s.TICKER AND t.IS_CURRENT = TRUE
WHEN MATCHED
 AND t.ROW_HASH != MD5(CONCAT_WS('|', s.COMPANY_NAME, s.SECTOR))
THEN UPDATE SET
  t.VALID_TO   = CURRENT_DATE(),
  t.IS_CURRENT = FALSE;

-- 2) Insert new versions AND brand-new tickers.
-- Step 1 already flipped IS_CURRENT, so this LEFT JOIN ... IS NULL catches both.
INSERT INTO MARTS.DIM_TICKER
SELECT MARTS.SEQ_TICKER_KEY.NEXTVAL,
       s.TICKER, s.COMPANY_NAME, s.SECTOR,
       MD5(CONCAT_WS('|', s.COMPANY_NAME, s.SECTOR)),
       CURRENT_DATE(), '9999-12-31'::DATE, TRUE
FROM STAGING.TICKER_STG s
LEFT JOIN MARTS.DIM_TICKER t
  ON s.TICKER = t.TICKER AND t.IS_CURRENT = TRUE
WHERE t.TICKER IS NULL;

/* ---------------------------------------------------------------------------
   FCT_DAILY_PRICE — grain: one row per ticker per trading day.

   The fact stores the surrogate key that was CURRENT on the trade date, so
   joining back needs no date logic — that is what makes SCD2 work.

   Measure types: CLOSE and VOLUME are additive. A balance would be
   semi-additive (summable across entities, not across time). A percentage is
   NON-additive and must never be summed.
   ------------------------------------------------------------------------ */
CREATE OR REPLACE TABLE MARTS.FCT_DAILY_PRICE AS
SELECT
  d.DATE_KEY,
  dt.TICKER_KEY,
  p.OPEN, p.HIGH, p.LOW, p.CLOSE, p.ADJ_CLOSE, p.VOLUME
FROM STAGING.PRICES_STG_CLUSTERED p
JOIN MARTS.DIM_DATE   d  ON p.TRADE_DATE = d.FULL_DATE
JOIN MARTS.DIM_TICKER dt ON p.TICKER = dt.TICKER
                        AND p.TRADE_DATE BETWEEN dt.VALID_FROM AND dt.VALID_TO;

/* ---------------------------------------------------------------------------
   Query patterns
   ------------------------------------------------------------------------ */

-- Current state
SELECT * FROM MARTS.DIM_TICKER WHERE IS_CURRENT = TRUE;

-- As it was on a given date
SELECT * FROM MARTS.DIM_TICKER
WHERE '2020-06-01' BETWEEN VALID_FROM AND VALID_TO;

-- Star join: volume by sector by quarter, using the sector that applied AT THE TIME
SELECT d.CAL_YEAR, d.CAL_QUARTER, dt.SECTOR, SUM(f.VOLUME) AS total_volume
FROM MARTS.FCT_DAILY_PRICE f
JOIN MARTS.DIM_DATE   d  ON f.DATE_KEY   = d.DATE_KEY
JOIN MARTS.DIM_TICKER dt ON f.TICKER_KEY = dt.TICKER_KEY
GROUP BY 1, 2, 3
ORDER BY 1, 2, 4 DESC;
