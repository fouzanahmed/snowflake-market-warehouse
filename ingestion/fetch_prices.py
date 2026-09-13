"""
fetch_prices.py -- build the load files for the Snowflake Market Data Warehouse.

Pulls S&P 500 constituents, downloads daily OHLCV from Yahoo Finance,
and writes N gzipped CSVs ready for PUT + COPY INTO (see
sql/copy_into_example.sql).

    pip install -r ../requirements.txt
    python fetch_prices.py

Two design choices worth knowing:

  1. Output is split into N files, not one. COPY INTO parallelises across
     files using the warehouse's threads (an XSMALL has 8). A single large
     file loads single-threaded. Snowflake's guidance is roughly
     100-250 MB compressed per file.

  2. Rows are SHUFFLED before splitting. This deliberately destroys natural
     clustering so a clustering-key / pruning demo shows a real before/after.
     It also mirrors how multi-source ingestion actually looks.
"""

import sys
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
START = "2021-01-01"
END = "2026-01-01"
N_OUTPUT_FILES = 10
OUT_DIR = Path("data")
FILE_PREFIX = "prices"
BATCH_SIZE = 40

WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Fallback so a fresh clone isn't blocked by a Wikipedia layout change.
FALLBACK_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "JPM",
    "V", "UNH", "XOM", "JNJ", "WMT", "MA", "PG", "AVGO", "HD", "CVX", "LLY",
    "ABBV", "MRK", "PEP", "KO", "COST", "ADBE", "CSCO", "TMO", "MCD", "ACN",
    "CRM", "ABT", "NFLX", "AMD", "LIN", "DHR", "TXN", "NEE", "PM", "WFC",
    "DIS", "VZ", "INTC", "CMCSA", "BMY", "RTX", "COP", "UPS", "QCOM", "HON",
]


def get_tickers() -> list[str]:
    """S&P 500 constituents, with a hardcoded fallback."""
    try:
        resp = requests.get(
            WIKI_SP500,
            headers={"User-Agent": "Mozilla/5.0 (compatible; data-eng-practice)"},
            timeout=30,
        )
        resp.raise_for_status()
        table = pd.read_html(StringIO(resp.text))[0]
        # BRK.B -> BRK-B : Yahoo uses hyphens for share classes
        tickers = table["Symbol"].astype(str).str.replace(".", "-", regex=False)
        tickers = sorted(set(tickers.tolist()))
        print(f"Fetched {len(tickers)} tickers from Wikipedia")
        return tickers
    except Exception as exc:  # noqa: BLE001
        print(f"Wikipedia fetch failed ({exc}); using {len(FALLBACK_TICKERS)} fallback tickers")
        return FALLBACK_TICKERS


def tidy_batch(raw: pd.DataFrame, batch: list[str]) -> pd.DataFrame | None:
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


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    tickers = get_tickers()

    chunks = []
    yf.set_tz_cache_location("./yf_cache")
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        try:
            raw = yf.download(
                batch,
                start=START,
                end=END,
                auto_adjust=False,
                group_by="ticker",
                threads=False,
                progress=False,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  batch {i} failed: {exc}")
            continue

        if raw is None or raw.empty:
            print(f"  batch {i} returned nothing")
            continue

        tidied = tidy_batch(raw, batch)
        if tidied is not None:
            chunks.append(tidied)

        print(f"  {min(i + BATCH_SIZE, len(tickers))}/{len(tickers)} tickers")

    if not chunks:
        print("No data downloaded. Check your network / yfinance version.")
        return 1

    df = pd.concat(chunks, ignore_index=True)

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

    df = df[wanted]
    df = df.dropna(subset=["CLOSE"])
    df["DATE"] = pd.to_datetime(df["DATE"], utc=True, errors="coerce").dt.date
    df = df.dropna(subset=["DATE"])
    df = df.drop_duplicates(subset=["TICKER", "DATE"], keep="last")

    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"]:
        df[col] = df[col].astype(float).round(6)
    df["VOLUME"] = df["VOLUME"].fillna(0).astype("int64")

    # Shuffle, then split -- see module docstring for why
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)

    for k, part in enumerate(np.array_split(df, N_OUTPUT_FILES)):
        path = OUT_DIR / f"{FILE_PREFIX}_{k:02d}.csv.gz"
        part.to_csv(path, index=False, compression="gzip")
        print(f"  wrote {path}  ({len(part):,} rows)")

    print(
        f"\nDone. {len(df):,} rows | {df['TICKER'].nunique()} tickers | "
        f"{df['DATE'].min()} to {df['DATE'].max()}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
