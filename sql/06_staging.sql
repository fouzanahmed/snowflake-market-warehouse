/* ============================================================================
   06 — Staging layer
   ----------------------------------------------------------------------------
   Grain: ONE ROW PER TICKER PER TRADING DAY.
   That sentence decides everything else — it is why (TICKER, TRADE_DATE) is
   the natural key, why it is what we merge on, and why a duplicate at that
   grain breaks every average downstream.
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE LOAD_WH;
USE SCHEMA    MARKET_DW.STAGING;

CREATE OR REPLACE TABLE STAGING.PRICES_STG (
  TICKER     STRING NOT NULL,
  TRADE_DATE DATE   NOT NULL,
  OPEN       NUMBER(18,6),
  HIGH       NUMBER(18,6),
  LOW        NUMBER(18,6),
  CLOSE      NUMBER(18,6),
  ADJ_CLOSE  NUMBER(18,6),
  VOLUME     NUMBER(38,0),
  UPDATED_AT TIMESTAMP_NTZ,
  CONSTRAINT PK_PRICES PRIMARY KEY (TICKER, TRADE_DATE)
);

/* NOTE: Snowflake does NOT enforce PRIMARY KEY, UNIQUE or FOREIGN KEY. They
   are metadata for tooling and the optimiser. NOT NULL is the only enforced
   constraint. That is precisely why uniqueness has to be handled in the load
   logic — and why declaring a PK has no effect on physical layout or pruning.

   Batch rebuild from raw, deduplicated to the grain.
   For the incremental stream + task + MERGE version, see roadmap/. */
INSERT INTO STAGING.PRICES_STG
  (TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, UPDATED_AT)
SELECT TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME,
       CURRENT_TIMESTAMP()
FROM (
  SELECT r.*,
         ROW_NUMBER() OVER (PARTITION BY TICKER, TRADE_DATE
                            ORDER BY LOAD_TS DESC) AS rn
  FROM MARKET_DW.RAW.PRICES_RAW r
)
WHERE rn = 1;

SELECT COUNT(*) AS row_cnt FROM STAGING.PRICES_STG;
