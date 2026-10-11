/* ============================================================================
   09 — Feature mart, pure SQL window functions [RUN, RESULTS NOT YET RECORDED]
   ----------------------------------------------------------------------------
   ROWS vs RANGE matters here. ROWS counts PHYSICAL rows; RANGE counts logical
   values of the ORDER BY column. Market data has weekend and holiday gaps, so
   a 20-ROW moving average and a 20-day RANGE average are different things.
   ROWS is correct — 20 TRADING days is what a 20-day MA means.

   No frame uses FOLLOWING. Every frame ends at CURRENT ROW, so absence of
   lookahead is a property of the schema rather than something to remember.
   NEXT_CLOSE uses LEAD and is the LABEL, not a feature — it must never enter
   the model's X matrix.
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE LOAD_WH;
USE SCHEMA    MARKET_DW.MARTS;

CREATE OR REPLACE TABLE MARTS.PRICE_FEATURES AS
WITH base AS (
  SELECT TICKER, TRADE_DATE, CLOSE, VOLUME,
         LAG(CLOSE, 1) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE)
           AS PREV_CLOSE
  FROM STAGING.PRICES_STG_CLUSTERED
),
ret AS (
  -- DIV0 returns 0 instead of erroring on divide-by-zero
  SELECT *, DIV0(CLOSE - PREV_CLOSE, PREV_CLOSE) AS DAILY_RETURN
  FROM base
),
gl AS (
  SELECT *,
         GREATEST(CLOSE - PREV_CLOSE, 0) AS GAIN,
         GREATEST(PREV_CLOSE - CLOSE, 0) AS LOSS
  FROM ret
)
SELECT
  TICKER,
  TRADE_DATE,
  CLOSE,
  VOLUME,
  DAILY_RETURN,

  AVG(CLOSE) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE
                   ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS MA_20,
  AVG(CLOSE) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE
                   ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS MA_50,

  -- SQRT(252) annualises a daily stdev: ~252 trading days per year
  STDDEV_SAMP(DAILY_RETURN) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE
                   ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252)
                   AS VOL_20D_ANNUALISED,

  LAG(DAILY_RETURN, 1) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE)
    AS RET_LAG_1,
  LAG(DAILY_RETURN, 5) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE)
    AS RET_LAG_5,

  /* SMA variant of RSI. Wilder's original uses recursive exponential
     smoothing, which a plain window function cannot express — it needs a
     recursive CTE, a UDF, or Snowpark. This approximation is deliberate. */
  100 - (100 / (1 + DIV0(
      AVG(GAIN) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE
                      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
      AVG(LOSS) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE
                      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
  ))) AS RSI_14,

  SUM(VOLUME) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    AS VOLUME_SUM_20,

  -- LABEL. Not a feature.
  LEAD(CLOSE, 1) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE) AS NEXT_CLOSE
FROM gl;


/* QUALIFY filters on a window function without a wrapping subquery —
   Snowflake-flavoured and much tidier than the standard workaround. */
SELECT TICKER, TRADE_DATE, DAILY_RETURN
FROM MARTS.PRICE_FEATURES
WHERE TRADE_DATE >= '2025-01-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY TICKER
                           ORDER BY DAILY_RETURN DESC) <= 3;
