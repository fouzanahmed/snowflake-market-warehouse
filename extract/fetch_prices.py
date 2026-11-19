"""
fetch_prices.py - build the load files for the Snowflake Market Data Warehouse.

Pulls S&P 500 constituents from Wikipedia, downloads daily OHLCV from Yahoo
Finance, and writes N gzipped CSVs ready for PUT + COPY INTO. Also writes a
small ticker/sector file used by the dimensional model.

    pip install -r requirements.txt
    python extract/fetch_prices.py [--allow-fallback]

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
    is the only signal. A silent fallback is worse than a crash, so the
    script now exits 1 instead, unless --allow-fallback is passed.

  - yfinance caches timezone data in SQLite. With threads=True, parallel
    workers contend for the file and tickers fail with
    OperationalError('database is locked'). Pointing the cache at a local
    directory and running single-threaded is slower but complete.
"""

import argparse
import hashlib
import json
import logging
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
MANIFEST_NAME = "manifest.json"  # outside the prices_*.csv.gz glob, so never PUT
TZ_CACHE = "./yf_cache"

WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(message)s"

log = logging.getLogger(__name__)

# Opt-in fallback (--allow-fallback) so a Wikipedia layout change need not
# block the build entirely. You'll get ~50 tickers, not ~500 -- check the log.
FALLBACK_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "JPM",
    "V", "UNH", "XOM", "JNJ", "WMT", "MA", "PG", "AVGO", "HD", "CVX", "LLY",
    "ABBV", "MRK", "PEP", "KO", "COST", "ADBE", "CSCO", "TMO", "MCD", "ACN",
    "CRM", "ABT", "NFLX", "AMD", "LIN", "DHR", "TXN", "NEE", "PM", "WFC",
    "DIS", "VZ", "INTC", "CMCSA", "BMY", "RTX", "COP", "UPS", "QCOM", "HON",
]


def get_constituents(allow_fallback: bool = False) -> pd.DataFrame | None:
    """
    S&P 500 constituents with sector. If the Wikipedia fetch fails, returns
    the hardcoded fallback only when allow_fallback is set; otherwise None,
    so the run stops instead of silently building a ~50-ticker dataset.
    """
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

        log.info(f"Fetched {len(df)} constituents from Wikipedia")
        return df.reset_index(drop=True)

    except Exception as exc:  # noqa: BLE001
        log.warning(f"Wikipedia fetch failed ({exc})")
        log.warning("Install an HTML parser to fix: pip install lxml html5lib beautifulsoup4")
        if not allow_fallback:
            log.error(f"Not falling back to the {len(FALLBACK_TICKERS)} hardcoded tickers. "
                      "Pass --allow-fallback to build from them anyway.")
            return None
        log.warning(f"Falling back to {len(FALLBACK_TICKERS)} hardcoded tickers "
                    "(--allow-fallback).")
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
            log.warning(f"attempt {attempt}/{MAX_ATTEMPTS} {reason}; retrying in {wait:.0f}s")
            time.sleep(wait)
        else:
            log.error(f"attempt {attempt}/{MAX_ATTEMPTS} {reason}; giving up")

    return None


def report_failed_batches(failed_batches: list) -> None:
    """List batches that never succeeded, so a partial run can't pass as complete."""
    if not failed_batches:
        return
    n_tickers = sum(len(batch) for _, batch in failed_batches)
    log.error(f"{len(failed_batches)} batch(es) failed after {MAX_ATTEMPTS} attempts "
              f"-- {n_tickers} ticker(s) missing from the output:")
    for i, batch in failed_batches:
        log.error(f"  batch at {i}: {', '.join(batch)}")
    log.error("Re-run the script to fetch them.")


PRICE_COLUMNS = ["TICKER", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]


def clean_prices(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Normalise the concatenated tidy_batch output into the load-file schema
    (PRICE_COLUMNS). Returns a new frame and leaves the input untouched;
    returns None, after logging what was there instead, if a required
    column is missing.
    """
    # Normalise column names: 'Adj Close' -> ADJ_CLOSE, 'Date' -> DATE
    df = df.copy()
    df.columns = [str(c).upper().replace(" ", "_") for c in df.columns]
    if "DATE" not in df.columns and "DATETIME" in df.columns:
        df = df.rename(columns={"DATETIME": "DATE"})
    if "ADJ_CLOSE" not in df.columns and "CLOSE" in df.columns:
        df["ADJ_CLOSE"] = df["CLOSE"]

    missing = [c for c in PRICE_COLUMNS if c not in df.columns]
    if missing:
        log.error(f"Missing expected columns: {missing}")
        log.error(f"Got: {list(df.columns)}")
        return None

    df = df[PRICE_COLUMNS].dropna(subset=["CLOSE"])
    df["DATE"] = pd.to_datetime(df["DATE"], utc=True, errors="coerce").dt.date
    df = df.dropna(subset=["DATE"])
    df = df.drop_duplicates(subset=["TICKER", "DATE"], keep="last")

    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"]:
        df[col] = df[col].astype(float).round(6)
    df["VOLUME"] = df["VOLUME"].fillna(0).astype("int64")
    return df


def remove_stale_parts(out_dir: Path) -> list[Path]:
    """
    Delete FILE_PREFIX_*.csv.gz left in out_dir by an earlier run. Without
    this, lowering N_OUTPUT_FILES leaves the old higher-numbered parts behind,
    and the README's `PUT .../data/prices_*.csv.gz` glob uploads them too --
    loading stale rows into RAW alongside the new ones. Returns what it removed.
    """
    removed = []
    for path in sorted(out_dir.glob(f"{FILE_PREFIX}_*.csv.gz")):
        path.unlink()
        log.info(f"removed stale {path}")
        removed.append(path)
    return removed


def sha256_file(path: Path) -> str:
    """Hex sha256 of a file's bytes, read in chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def write_manifest(out_dir: Path, parts: list[tuple[Path, int]],
                   df: pd.DataFrame) -> Path:
    """
    Write out_dir/MANIFEST_NAME: each part's file name, row count and sha256,
    plus the totals from the 'Done.' log line. A record to reconcile against
    COPY INTO's per-file rows_loaded. The name sits outside the
    FILE_PREFIX_*.csv.gz glob, so remove_stale_parts leaves it alone (it is
    simply overwritten) and the README's PUT never uploads it.
    """
    manifest = {
        "files": [
            {"file": path.name, "rows": rows, "sha256": sha256_file(path)}
            for path, rows in parts
        ],
        "total_rows": len(df),
        "tickers": int(df["TICKER"].nunique()),
        "date_min": str(df["DATE"].min()),
        "date_max": str(df["DATE"].max()),
    }
    path = out_dir / MANIFEST_NAME
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    log.info(f"wrote {path}  ({len(parts)} files)")
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the load files for the Snowflake Market Data Warehouse.")
    parser.add_argument(
        "--allow-fallback",
        action="store_true",
        help=f"if the Wikipedia constituent fetch fails, build from the "
             f"{len(FALLBACK_TICKERS)} hardcoded tickers instead of exiting 1",
    )
    return parser.parse_args(argv)


def main(allow_fallback: bool = False) -> int:
    OUT_DIR.mkdir(exist_ok=True)

    # Keep the tz cache local and off the shared default path
    with suppress(AttributeError):  # older yfinance has no set_tz_cache_location
        yf.set_tz_cache_location(TZ_CACHE)

    constituents = get_constituents(allow_fallback=allow_fallback)
    if constituents is None:
        return 1
    tickers = constituents["TICKER"].tolist()

    chunks = []
    failed_batches = []
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        raw = download_batch(batch)
        if raw is None:
            log.error(f"batch at {i} failed after {MAX_ATTEMPTS} attempts")
            failed_batches.append((i, batch))
            continue

        tidied = tidy_batch(raw, batch)
        if tidied is not None:
            chunks.append(tidied)

        log.info(f"{min(i + BATCH_SIZE, len(tickers))}/{len(tickers)} tickers")

    if not chunks:
        log.error("No data downloaded. Check network / yfinance version.")
        report_failed_batches(failed_batches)
        return 1

    df = clean_prices(pd.concat(chunks, ignore_index=True))
    if df is None:
        return 1

    # Report tickers with no price history rather than failing the batch
    # (tickers from failed batches are reported separately, below)
    got = set(df["TICKER"].unique())
    in_failed_batches = {t for _, batch in failed_batches for t in batch}
    dropped = [t for t in tickers if t not in got and t not in in_failed_batches]
    if dropped:
        log.warning(f"No price history for {len(dropped)} ticker(s): {', '.join(dropped)}")
        log.warning("(usually recent index additions whose symbol has no back-series)")

    # Shuffle, then split -- see module docstring for why
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)

    # Clear old parts only now, once there is data to replace them with, so a
    # failed run leaves the previous output intact.
    remove_stale_parts(OUT_DIR)

    # Split row positions rather than the frame itself: on recent numpy/pandas,
    # np.array_split(df, n) returns bare ndarrays, which have no .to_csv.
    written = []
    for k, rows in enumerate(np.array_split(np.arange(len(df)), N_OUTPUT_FILES)):
        part = df.iloc[rows]
        path = OUT_DIR / f"{FILE_PREFIX}_{k:02d}.csv.gz"
        part.to_csv(path, index=False, compression="gzip")
        log.info(f"wrote {path}  ({len(part):,} rows)")
        written.append((path, len(part)))

    write_manifest(OUT_DIR, written, df)

    # Ticker dimension source, for the star schema
    dim_path = OUT_DIR / "tickers.csv.gz"
    constituents[constituents["TICKER"].isin(got)].to_csv(
        dim_path, index=False, compression="gzip")
    log.info(f"wrote {dim_path}  ({len(got):,} tickers)")

    log.info(
        f"Done. {len(df):,} rows | {df['TICKER'].nunique()} tickers | "
        f"{df['DATE'].min()} to {df['DATE'].max()}"
    )
    report_failed_batches(failed_batches)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    sys.exit(main(allow_fallback=parse_args().allow_fallback))
