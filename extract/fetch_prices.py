"""
fetch_prices.py - build the load files for the Snowflake Market Data Warehouse.

Pulls S&P 500 constituents from Wikipedia, downloads daily OHLCV from Yahoo
Finance, and writes N gzipped CSVs ready for PUT + COPY INTO. Also writes a
small ticker/sector file used by the dimensional model.

    pip install -r requirements.txt
    python extract/fetch_prices.py

Measured output (2015-01-01 to 2025-12-31):
    1,338,849 rows | 501 tickers | 10 files | ~38 MB gzipped

Two design choices worth knowing:

  1. Output is split into N files, not one. COPY INTO parallelises across
     files using the warehouse's threads (an XSMALL has 8). A single large
     file loads single-threaded. Snowflake's guidance is roughly
     100-250 MB compressed per file.

  2. Rows are SHUFFLED before splitting. This deliberately destroys natural
     clustering so that the pruning comparison in sql/07 shows a real
     before/after. It also mirrors how multi-source ingestion tends to look.

Windows notes (both of these cost real debugging time):

  - Wikipedia's table needs an HTML parser (lxml / html5lib / bs4). Without
    one, pd.read_html raises and the script falls back to a hardcoded list of
    50 tickers -- a SILENT degradation that looks like success. The row count
    is the only signal. A silent fallback is worse than a crash.

  - yfinance caches timezone data in SQLite. With threads=True, parallel
    workers contend for the file and tickers fail with
    OperationalError('database is locked'). Pointing the cache at a local
    directory and running single-threaded is slower but complete.
"""

import sys
import time
from contextlib import suppress
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
START = "2015-01-01"
END = "2025-12-31"
BATCH_SIZE = 40  # tickers per yfinance call
MAX_ATTEMPTS = 4  # per batch, including the first try
BACKOFF_BASE_S = 2.0  # waits 2s, 4s, 8s between attempts
N_OUTPUT_FILES = 10
OUT_DIR = Path("data")
FILE_PREFIX = "prices"
TZ_CACHE = "./yf_cache"

WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Fallback so a Wikipedia layout change never blocks the build entirely.
# If you end up here you'll get ~50 tickers, not ~500 -- check the log.
FALLBACK_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "JPM",
    "V", "UNH", "XOM", "JNJ", "WMT", "MA", "PG", "AVGO", "HD", "CVX", "LLY",
    "ABBV", "MRK", "PEP", "KO", "COST", "ADBE", "CSCO", "TMO", "MCD", "ACN",
    "CRM", "ABT", "NFLX", "AMD", "LIN", "DHR", "TXN", "NEE", "PM", "WFC",
    "DIS", "VZ", "INTC", "CMCSA", "BMY", "RTX", "COP", "UPS", "QCOM", "HON",
]


def get_constituents() -> pd.DataFrame:
    """S&P 500 constituents with sector, plus a hardcoded fallback."""
    try:
        resp = requests.get(
            WIKI_SP500,
            headers={"User-Agent": "Mozilla/5.0 (compatible; data-eng-practice)"},
            timeout=30,
        )
        resp.raise_for_status()
        table = pd.read_html(StringIO(resp.text))[0]

        df = pd.DataFrame({
            # BRK.B -> BRK-B : Yahoo uses hyphens for share classes
            "TICKER": table["Symbol"].astype(str).str.replace(".", "-", regex=False),
            "COMPANY_NAME": table["Security"].astype(str),
            "SECTOR": table["GICS Sector"].astype(str),
        }).drop_duplicates(subset=["TICKER"]).sort_values("TICKER")

        print(f"Fetched {len(df)} constituents from Wikipedia")
        return df.reset_index(drop=True)

    except Exception as exc:  # noqa: BLE001
        print(f"!! Wikipedia fetch failed ({exc})")
        print(f"!! Falling back to {len(FALLBACK_TICKERS)} hardcoded tickers.")
        print("!! Install an HTML parser to fix: pip install lxml html5lib beautifulsoup4")
        return pd.DataFrame({
            "TICKER": FALLBACK_TICKERS,
            "COMPANY_NAME": [None] * len(FALLBACK_TICKERS),
            "SECTOR": [None] * len(FALLBACK_TICKERS),
        })


def tidy_batch(raw: pd.DataFrame, batch: list) -> pd.DataFrame:
    """
    yfinance with group_by='ticker' returns MultiIndex columns (ticker, field).
    Unpack to long format without relying on .stack(), whose signature has
    shifted across pandas versions.
    """
    frames = []
    multi = isinstance(raw.columns, pd.MultiIndex)

    for tkr in batch:
        try:
            sub = raw[tkr].copy() if multi else raw.copy()
        except KeyError:
            continue

        sub = sub.dropna(how="all")
        if sub.empty:
            continue

        sub = sub.reset_index()
        sub.insert(0, "TICKER", tkr)
        frames.append(sub)

    return pd.concat(frames, ignore_index=True) if frames else None


def download_batch(batch: list) -> pd.DataFrame | None:
    """
    yf.download with retry and exponential backoff. yfinance often swallows
    a transient Yahoo error and returns an empty frame instead of raising, so
    an empty result is retried too. Returns None if every attempt fails.
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            raw = yf.download(
                batch,
                start=START,
                end=END,
                auto_adjust=False,   # keeps a separate 'Adj Close' column
                group_by="ticker",
                threads=False,       # avoids the SQLite lock on Windows
                progress=False,
            )
            if raw is not None and not raw.empty:
                return raw
            reason = "returned nothing"
        except Exception as exc:  # noqa: BLE001
            reason = f"failed: {exc}"

        if attempt < MAX_ATTEMPTS:
            wait = BACKOFF_BASE_S * 2 ** (attempt - 1)
            print(f"  attempt {attempt}/{MAX_ATTEMPTS} {reason}; retrying in {wait:.0f}s")
            time.sleep(wait)
        else:
            print(f"  attempt {attempt}/{MAX_ATTEMPTS} {reason}; giving up")

    return None


def report_failed_batches(failed_batches: list) -> None:
    """List batches that never succeeded, so a partial run can't pass as complete."""
    if not failed_batches:
        return
    n_tickers = sum(len(batch) for _, batch in failed_batches)
    print(f"\n!! {len(failed_batches)} batch(es) failed after {MAX_ATTEMPTS} attempts "
          f"-- {n_tickers} ticker(s) missing from the output:")
    for i, batch in failed_batches:
        print(f"!!   batch at {i}: {', '.join(batch)}")
    print("!! Re-run the script to fetch them.")


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)

    # Keep the tz cache local and off the shared default path
    with suppress(AttributeError):  # older yfinance has no set_tz_cache_location
        yf.set_tz_cache_location(TZ_CACHE)

    constituents = get_constituents()
    tickers = constituents["TICKER"].tolist()

    chunks = []
    failed_batches = []
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        raw = download_batch(batch)
        if raw is None:
            print(f"  batch at {i} failed after {MAX_ATTEMPTS} attempts")
            failed_batches.append((i, batch))
            continue

        tidied = tidy_batch(raw, batch)
        if tidied is not None:
            chunks.append(tidied)

        print(f"  {min(i + BATCH_SIZE, len(tickers))}/{len(tickers)} tickers")

    if not chunks:
        print("No data downloaded. Check network / yfinance version.")
        report_failed_batches(failed_batches)
        return 1

    df = pd.concat(chunks, ignore_index=True)

    # Normalise column names: 'Adj Close' -> ADJ_CLOSE, 'Date' -> DATE
    df.columns = [str(c).upper().replace(" ", "_") for c in df.columns]
    if "DATE" not in df.columns and "DATETIME" in df.columns:
        df = df.rename(columns={"DATETIME": "DATE"})
    if "ADJ_CLOSE" not in df.columns:
        df["ADJ_CLOSE"] = df["CLOSE"]

    wanted = ["TICKER", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        print(f"Missing expected columns: {missing}")
        print(f"Got: {list(df.columns)}")
        return 1

    df = df[wanted].dropna(subset=["CLOSE"])
    df["DATE"] = pd.to_datetime(df["DATE"], utc=True, errors="coerce").dt.date
    df = df.dropna(subset=["DATE"])
    df = df.drop_duplicates(subset=["TICKER", "DATE"], keep="last")

    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"]:
        df[col] = df[col].astype(float).round(6)
    df["VOLUME"] = df["VOLUME"].fillna(0).astype("int64")

    # Report tickers with no price history rather than failing the batch
    # (tickers from failed batches are reported separately, below)
    got = set(df["TICKER"].unique())
    in_failed_batches = {t for _, batch in failed_batches for t in batch}
    dropped = [t for t in tickers if t not in got and t not in in_failed_batches]
    if dropped:
        print(f"\nNo price history for {len(dropped)} ticker(s): {', '.join(dropped)}")
        print("(usually recent index additions whose symbol has no back-series)")

    # Shuffle, then split -- see module docstring for why
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)

    # Split row positions rather than the frame itself: on recent numpy/pandas,
    # np.array_split(df, n) returns bare ndarrays, which have no .to_csv.
    print()
    for k, rows in enumerate(np.array_split(np.arange(len(df)), N_OUTPUT_FILES)):
        part = df.iloc[rows]
        path = OUT_DIR / f"{FILE_PREFIX}_{k:02d}.csv.gz"
        part.to_csv(path, index=False, compression="gzip")
        print(f"  wrote {path}  ({len(part):,} rows)")

    # Ticker dimension source, for the star schema
    dim_path = OUT_DIR / "tickers.csv.gz"
    constituents[constituents["TICKER"].isin(got)].to_csv(
        dim_path, index=False, compression="gzip")
    print(f"  wrote {dim_path}  ({len(got):,} tickers)")

    print(
        f"\nDone. {len(df):,} rows | {df['TICKER'].nunique()} tickers | "
        f"{df['DATE'].min()} to {df['DATE'].max()}"
    )
    report_failed_batches(failed_batches)
    return 0


if __name__ == "__main__":
    sys.exit(main())
