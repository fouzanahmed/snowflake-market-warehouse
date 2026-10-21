# Market Data Warehouse on Snowflake

A layered Snowflake warehouse over **11 years of daily price data for 500 S&P 500
tickers — 1,338,849 rows** — built to learn the platform's ingestion, cost and
performance behaviour hands-on rather than from documentation.

Personal project, built on a Snowflake Enterprise trial account.

---

## What's measured

Every number below was read off Snowflake's own output, not estimated.

| | |
|---|---|
| Rows loaded | **1,338,849** |
| Tickers | **501** (of 503 constituents; 2 had no price history) |
| Date range | **2015-01-02 → 2025-12-30** |
| Load time | **2.4 s** on an XSMALL warehouse, 10 files, **0 errors** |
| Micro-partitions | **8** total |
| Pruning before re-sort | **8 / 8** partitions, **10.79 MB** scanned |
| Pruning after re-sort | **2 / 8** partitions, **1.80 MB**, 647 ms |
| `average_depth` before | **8.0** on 8 partitions (worst case) |

---

## The main finding

Re-sorting the table on `(ticker, trade_date)` cut partitions scanned 4× and
bytes scanned 6×. But the whole table is only **eight micro-partitions** — 1.34M
rows of narrow numeric data compresses that far — so a full scan costs
milliseconds either way and **a clustering key would not have earned its keep.**

Rather than report a ratio that didn't mean anything at that scale, the
experiment was repeated on TPCH at 60M rows:

| `LINEITEM` (60M rows), same date-range query | Partitions scanned |
|---|---|
| `ORDER BY RANDOM()` | **49 / 49** — 264 MB |
| `ORDER BY L_SHIPDATE` | **3 / 50** — 348 ms |

Two things came out of that:

**Pruning evaluates the physical sort order left to right.** The leading column
has to match your most selective predicate. Sorting by `(ticker, trade_date)`
helps queries filtering on ticker, or on ticker *and* date — but a **date-only**
filter still touches nearly every partition, because each one holds that date
range for some subset of tickers. The TPCH test sorts on `L_SHIPDATE` alone
precisely because the filter is date-only.

**Check `total_partition_count` before reaching for a clustering key.** Below a
certain size there is nothing to skip, and the optimisation is cost without
benefit.

---

## Design decisions

**Ten files, not one.** `COPY INTO` parallelises across the warehouse's threads
and the *file* is the unit of parallelism — an XSMALL has 8 threads, so a single
file loads single-threaded no matter how large the warehouse. The files
completed out of order in the load output, which is the visible signature of
parallel execution. File count and warehouse size have to be matched: scaling to
a SMALL gives 16 threads, and with only 10 files 6 would sit idle while the bill
doubled.

**Rows shuffled before splitting.** Deliberate — it destroys natural clustering
so the pruning comparison has a real before state. It also mirrors how
multi-source ingestion tends to look in practice.

**A one-off CTAS re-sort rather than `ALTER TABLE ... CLUSTER BY`.** Automatic
Clustering is a serverless background service that consumes credits
continuously and can take hours to converge. For a table rebuilt in batch, a
physical re-sort buys the same pruning for a single bounded cost. A clustering
key is for large tables under ongoing DML, where re-sorting isn't an option.

**Two warehouses.** `LOAD_WH` and `QUERY_WH` share the same storage but isolate
compute, so ingestion can't contend with reads. Both XSMALL with 60-second
auto-suspend — long enough to keep the local SSD cache warm between queries,
short enough not to idle.

**A resource monitor with staged triggers.** Notify at 75%, `SUSPEND` at 90%
(running queries finish), `SUSPEND_IMMEDIATE` at 100% (they don't).

**Lineage columns on the raw table.** `METADATA$FILENAME` and
`METADATA$FILE_ROW_NUMBER` are captured into `SRC_FILE` and `SRC_ROW`, so any
row traces back to the file and line it came from.

**Internal stage, not S3.** The storage-integration DDL is in
`sql/04_load.sql` as an appendix — the point being that an integration is an
IAM role assumption, so no AWS keys are ever stored in Snowflake.

---

## Idempotency, tested by breaking it

Re-running the identical `COPY INTO` returns *"Copy executed with 0 files
processed"*. Snowflake keeps load metadata per file, per table, for **64 days**,
keyed on filename + ETag.

Two things that only surface by trying:

`PUT` is a **separate** skip mechanism and compares **filenames, not contents**.
Re-running the extract over a wider date range produced larger files with the
same names — all ten were skipped, so nothing reached the stage and the
subsequent `COPY` also found nothing. The fix isn't `OVERWRITE=TRUE` (that
reloads 1.3M rows to pick up a few thousand); real pipelines land new files
under new, usually date-partitioned, names.

Forcing a reload with `FORCE = TRUE` duplicated the table to **2,677,698 rows** —
which is exactly the condition that makes a `MERGE` fail with *"Duplicate row
detected during DML action"*: one target row matching two source rows, and
Snowflake refusing to guess which wins. Cleaned up with a `ROW_NUMBER` dedupe on
load timestamp.

The conclusion: COPY's load metadata is a 64-day convenience, not a guarantee. A
long-lived pipeline should be idempotent *by design*, through upserts on a
natural key.

---

## Repository layout

```
sql/        Executed, with measured results in the header comments
  01_infrastructure.sql       Warehouses, resource monitor, database, schemas
  02_rbac.sql                 Engineer / analyst roles, future grants
  03_raw_layer.sql            File format, internal stage, raw table
  04_load.sql                 PUT, stage inspection, COPY INTO, verification
  05_idempotency_tests.sql    The 64-day metadata, the PUT skip, FORCE
  06_staging.sql              Staging table, grain, batch rebuild
  07_performance_pruning.sql  Query Profile, clustering info, TPCH at scale

roadmap/    Later layers, each marked with its status in the header
  08_streams_tasks_merge.sql  Stream, scheduled task, MERGE upsert      run
  09_feature_mart.sql         Window-function feature layer            run
  10_time_travel_clones.sql   Time Travel recovery, zero-copy clones   run
  11_masking.sql              Dynamic data masking                     not yet run
  12_star_schema_scd2.sql     Dimensional model, SCD Type 2            not yet run

extract/
  fetch_prices.py             Constituent list + OHLCV download, file splitting
```

The split is deliberate. Everything in `sql/` was run, with its real output
recorded in the header comments. `08`–`10` in `roadmap/` have been run too, but
their measured output isn't written up yet; `11` and `12` are design work that
hasn't been executed. A file moves into `sql/` once its results are pasted in.

---

## Running it

```bash
pip install -r requirements.txt
python extract/fetch_prices.py          # ~5-10 min, writes data/*.csv.gz
```

Then, in a Snowsight worksheet, run `sql/01` through `sql/03`.

`PUT` is client-side and cannot run in a browser worksheet, so from SnowSQL:

```sql
snowsql -a <org>-<account> -u <user>

USE ROLE MARKET_ENGINEER; USE WAREHOUSE LOAD_WH; USE SCHEMA MARKET_DW.RAW;
PUT file://<abs-path>/data/prices_*.csv.gz @STG_PRICES AUTO_COMPRESS=FALSE;
```

Back in Snowsight, run `sql/04` onward. For `sql/07`, open **Query History → the
query → Profile** and read `Partitions scanned` / `Partitions total` on the
TableScan node.

**Enterprise edition** is needed for masking policies and 90-day Time Travel;
Standard caps Time Travel at 1 day. Edition is fixed at signup.

---

## Notes for anyone running this on Windows

Two failures that cost real time, both now handled in the script:

**A silent fallback.** `pandas.read_html` needs an HTML parser (`lxml`,
`html5lib`, `beautifulsoup4`). Without one it raises, the script falls back to a
hardcoded 50-ticker list, and the run *looks successful* — the row count is the
only signal. A silent fallback is worse than a crash, so the script now logs the
failure loudly and exits 1 without downloading anything, and the parsers are
pinned in `requirements.txt`. To build from the 50-ticker list anyway (say,
because Wikipedia changed its table layout), opt in explicitly:

```bash
python extract/fetch_prices.py --allow-fallback
```

**`OperationalError('database is locked')`.** yfinance caches timezone data in
SQLite; with `threads=True` parallel workers contend for the file and tickers
drop out at random. The script now points the cache at a local directory and
downloads single-threaded — slower, but complete.

Two symbols in the constituent list had no price history under those tickers
(recent index additions), so they're logged and skipped rather than failing the
batch.

---

## Things I'd do differently

**External stage on S3 with a storage integration**, so no credentials live in
Snowflake at all. Skipped here for time on a trial account.

**Dynamic tables instead of streams + tasks.** You declare the end state and
Snowflake manages the incremental refresh — roughly a third of the code. Streams
and tasks gave more control and were better for learning the mechanics, but the
declarative version is what I'd reach for now.

**A rejects table wired to `VALIDATE`.** `ON_ERROR = 'CONTINUE'` drops bad rows
silently, so the table looks healthy while quietly missing data. That output
should be persisted and alerted on, not discovered later.
