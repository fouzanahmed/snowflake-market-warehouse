-- Loads the gzipped CSVs produced by ingestion/fetch_prices.py into the raw
-- table via an S3 external stage. Replace the stage URL/credentials with
-- your own -- these are placeholders, not real values.

CREATE FILE FORMAT IF NOT EXISTS MARKET_DW.RAW.CSV_GZ
    TYPE = CSV
    SKIP_HEADER = 1
    COMPRESSION = GZIP
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE STAGE IF NOT EXISTS MARKET_DW.RAW.PRICES_STAGE
    URL = 's3://YOUR_BUCKET/market-dw/prices/'
    CREDENTIALS = (AWS_KEY_ID = 'YOUR_KEY_ID' AWS_SECRET_KEY = 'YOUR_SECRET_KEY')
    FILE_FORMAT = MARKET_DW.RAW.CSV_GZ;

-- Upload with the SnowSQL CLI first, e.g.:
--   PUT file://data/prices_*.csv.gz @MARKET_DW.RAW.PRICES_STAGE;

COPY INTO MARKET_DW.RAW.PRICES (TICKER, DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
FROM @MARKET_DW.RAW.PRICES_STAGE
FILE_FORMAT = (FORMAT_NAME = MARKET_DW.RAW.CSV_GZ)
ON_ERROR = 'ABORT_STATEMENT';
