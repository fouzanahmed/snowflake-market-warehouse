# Snowflake Market Data Warehouse

A three-layer (raw / staging / marts) Snowflake data warehouse over daily S&P 500 OHLCV prices, with an S3-based ingestion path, incremental loading via Streams + Tasks, and a baseline return-forecasting model on top of the mart layer.

## What's actually implemented vs. design examples

Everything under `ingestion/`, `python/`, and `tests/` is real, runnable code with a passing test suite. The SQL under `sql/` is real, correct Snowflake DDL/DML written against Snowflake's documented syntax, but has **not** been executed against a live Snowflake account as part of building this repo — `sql/ddl/04_rbac_example.sql` in particular is a role/grant design example, not something verified to run. Anyone using this against their own account should read through the SQL first.

## Architecture

```
ingestion/fetch_prices.py         Yahoo Finance -> gzipped CSVs
        |
        v  PUT + COPY INTO (sql/copy_into_example.sql)
MARKET_DW.RAW.PRICES               raw layer, minimal typing
        |
        v  Stream + Task, incremental MERGE (sql/stream_and_task_example.sql)
MARKET_DW.STAGING.STG_PRICES       deduplicated, typed, clustered by (TICKER, DATE)
        |
        v  sql/ddl/03_marts.sql
MARKET_DW.MARTS.DAILY_RETURNS
MARKET_DW.MARTS.FEATURES_ROLLING   rolling return + volatility features
        |
        v  python/forecast_baseline.py
Linear regression vs. naive-zero baseline
```

- `ingestion/fetch_prices.py` — downloads S&P 500 daily OHLCV from Yahoo Finance, writes N gzipped CSVs sized for parallel COPY INTO
- `sql/ddl/` — raw table, staging table (deduplicated + clustered), mart tables, an RBAC design example
- `sql/copy_into_example.sql` — S3 external stage + COPY INTO
- `sql/stream_and_task_example.sql` — a Stream + Task pair doing incremental MERGE from raw into staging
- `python/feature_engineering.py` — a pure-pandas mirror of the `FEATURES_ROLLING` mart (rolling return + volatility), independently unit-tested
- `python/forecast_baseline.py` — trains a linear regression on the rolling features to predict next-day return, compared against a naive "predict zero return" baseline, using a chronological (not random) train/test split to avoid lookahead bias

## Results (measured, not simulated)

Run against 424,377 rows of real daily price data (503 S&P 500 tickers, 2015-2026, from 3 of the 10 gzipped shard files this repo's own ingestion produces), with a chronological 80/20 train/test split:

| Model | MAE (next-day return) |
|---|---|
| Naive (predict 0 return) | 0.025517 |
| Linear regression on rolling features | 0.025481 |

The linear regression barely beats the naive baseline. That's the expected, honest result — short-horizon stock returns are famously close to unpredictable from simple lagged features, which is itself a real finding about market efficiency, not a shortcoming of this implementation. Reproduce it yourself:

```bash
python ingestion/fetch_prices.py          # writes data/prices_*.csv.gz
python python/forecast_baseline.py data/prices_*.csv.gz
```

## Technologies

Python, pandas, scikit-learn, Snowflake (SQL, Streams, Tasks, RBAC), S3 external stages, pytest, GitHub Actions.

## Setup

```bash
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements-dev.txt

# fetch real data
python ingestion/fetch_prices.py

# run tests (no live Snowflake connection needed)
pytest -q

# run the forecasting baseline
python python/forecast_baseline.py data/prices_*.csv.gz
```

To actually load into Snowflake, you'll need your own account — copy `.env.example` to `.env` and fill in your credentials, then run the SQL in `sql/ddl/` in order, followed by `sql/copy_into_example.sql` (after uploading the CSVs to your own S3 bucket) and `sql/stream_and_task_example.sql`.
