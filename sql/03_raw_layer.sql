/* ============================================================================
   03 — File format, internal stage, raw table
   ========================================================================= */

USE ROLE      MARKET_ENGINEER;
USE WAREHOUSE LOAD_WH;
USE SCHEMA    MARKET_DW.RAW;

CREATE FILE FORMAT IF NOT EXISTS FF_CSV_GZ
  TYPE                         = CSV
  FIELD_DELIMITER              = ','
  SKIP_HEADER                  = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('', 'NULL', 'null', 'NaN')
  EMPTY_FIELD_AS_NULL          = TRUE
  COMPRESSION                  = GZIP
  DATE_FORMAT                  = 'YYYY-MM-DD';

CREATE STAGE IF NOT EXISTS STG_PRICES
  FILE_FORMAT = FF_CSV_GZ
  COMMENT     = 'Internal named stage for daily price extracts';

/* Raw is append-only and stays exactly as it arrived, so it can always be
   reprocessed. SRC_FILE and SRC_ROW carry lineage: any row can be traced back
   to the file and line it came from. */
CREATE OR REPLACE TABLE RAW.PRICES_RAW (
  TICKER     STRING,
  TRADE_DATE DATE,
  OPEN       NUMBER(18,6),
  HIGH       NUMBER(18,6),
  LOW        NUMBER(18,6),
  CLOSE      NUMBER(18,6),
  ADJ_CLOSE  NUMBER(18,6),
  VOLUME     NUMBER(38,0),
  SRC_FILE   STRING,
  SRC_ROW    NUMBER,
  LOAD_TS    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
