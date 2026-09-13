-- Raw layer: land the CSV data exactly as it arrives, minimal typing.
-- Loaded via sql/copy_into_example.sql.

CREATE DATABASE IF NOT EXISTS MARKET_DW;
CREATE SCHEMA IF NOT EXISTS MARKET_DW.RAW;

CREATE TABLE IF NOT EXISTS MARKET_DW.RAW.PRICES (
    TICKER      VARCHAR(16)     NOT NULL,
    DATE        DATE            NOT NULL,
    OPEN        FLOAT,
    HIGH        FLOAT,
    LOW         FLOAT,
    CLOSE       FLOAT,
    ADJ_CLOSE   FLOAT,
    VOLUME      NUMBER(20, 0),
    LOADED_AT   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
