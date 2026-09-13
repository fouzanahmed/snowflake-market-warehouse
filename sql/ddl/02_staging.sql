-- Staging layer: deduplicated, typed, one row per (ticker, date).
-- A table rather than a view so it can carry its own clustering key --
-- useful once RAW grows past a few million rows and query pruning matters.

CREATE SCHEMA IF NOT EXISTS MARKET_DW.STAGING;

CREATE OR REPLACE TABLE MARKET_DW.STAGING.STG_PRICES
CLUSTER BY (TICKER, DATE)
AS
SELECT
    TICKER,
    DATE,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    ADJ_CLOSE,
    VOLUME
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TICKER, DATE
            ORDER BY LOADED_AT DESC
        ) AS RN
    FROM MARKET_DW.RAW.PRICES
    WHERE TICKER IS NOT NULL
      AND DATE IS NOT NULL
      AND CLOSE IS NOT NULL
)
WHERE RN = 1;
