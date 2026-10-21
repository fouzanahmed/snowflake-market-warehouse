"""
Unit tests for extract/fetch_prices.py.

No network: Wikipedia and yfinance are replaced with small synthetic frames
shaped like what yfinance returns with group_by='ticker'.

    pip install -r requirements-dev.txt
    pytest
"""

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "extract" / "fetch_prices.py"
_spec = importlib.util.spec_from_file_location("fetch_prices", _SCRIPT)
fp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fp)

FIELDS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
WANTED = ["TICKER", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]


def make_raw(tickers, n_days=5, empty=()):
    """A yfinance-style frame: DatetimeIndex 'Date', columns (ticker, field)."""
    dates = pd.date_range("2024-01-02", periods=n_days, freq="B", name="Date")
    cols = pd.MultiIndex.from_product([tickers, FIELDS])
    data = np.arange(n_days * len(cols), dtype=float).reshape(n_days, len(cols)) + 1
    raw = pd.DataFrame(data, index=dates, columns=cols)
    for tkr in empty:
        raw[tkr] = np.nan
    return raw


# ---------------------------------------------------------------------------
# tidy_batch
# ---------------------------------------------------------------------------
def test_tidy_batch_reshapes_multiindex_to_long():
    raw = make_raw(["AAA", "BBB"], n_days=4)
    out = fp.tidy_batch(raw, ["AAA", "BBB"])

    assert list(out.columns) == ["TICKER", "Date", *FIELDS]
    assert len(out) == 8
    assert out["TICKER"].value_counts().to_dict() == {"AAA": 4, "BBB": 4}
    # Values come from that ticker's own columns, not a neighbour's
    bbb = out[out["TICKER"] == "BBB"].reset_index(drop=True)
    assert bbb["Close"].tolist() == raw[("BBB", "Close")].tolist()
    assert bbb["Date"].tolist() == raw.index.tolist()


def test_tidy_batch_drops_all_nan_rows_but_keeps_partial_rows():
    raw = make_raw(["AAA"], n_days=3)
    raw.iloc[0] = np.nan                        # whole row missing -> dropped
    raw.loc[raw.index[1], ("AAA", "Open")] = np.nan  # one field missing -> kept
    out = fp.tidy_batch(raw, ["AAA"])

    assert len(out) == 2
    assert out["Date"].tolist() == raw.index[1:].tolist()


def test_tidy_batch_skips_missing_and_empty_tickers():
    raw = make_raw(["AAA", "BBB"], empty=["BBB"])
    out = fp.tidy_batch(raw, ["AAA", "BBB", "NOT_RETURNED"])

    assert set(out["TICKER"]) == {"AAA"}


def test_tidy_batch_returns_none_when_nothing_usable():
    raw = make_raw(["AAA"], empty=["AAA"])
    assert fp.tidy_batch(raw, ["AAA", "NOT_RETURNED"]) is None


def test_tidy_batch_flat_columns_single_ticker():
    raw = make_raw(["AAA"], n_days=3)["AAA"]    # flat columns, as for 1 ticker
    out = fp.tidy_batch(raw, ["AAA"])

    assert list(out.columns) == ["TICKER", "Date", *FIELDS]
    assert len(out) == 3
    assert set(out["TICKER"]) == {"AAA"}


# ---------------------------------------------------------------------------
# download_batch: retry with exponential backoff
# ---------------------------------------------------------------------------
@pytest.fixture
def sleeps(monkeypatch):
    """Record backoff waits instead of sleeping."""
    waits = []
    monkeypatch.setattr(fp.time, "sleep", waits.append)
    return waits


def flaky_download(outcomes):
    """A yf.download stand-in that plays back outcomes, one per call."""
    calls = []

    def _download(batch, **_kwargs):
        calls.append(list(batch))
        outcome = outcomes[len(calls) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return _download, calls


def test_download_batch_retries_then_succeeds(monkeypatch, sleeps):
    good = make_raw(["AAA"])
    download, calls = flaky_download([RuntimeError("503"), pd.DataFrame(), good])
    monkeypatch.setattr(fp.yf, "download", download)

    assert fp.download_batch(["AAA"]) is good
    assert len(calls) == 3
    assert sleeps == [fp.BACKOFF_BASE_S, fp.BACKOFF_BASE_S * 2]


def test_download_batch_gives_up_after_max_attempts(monkeypatch, sleeps):
    download, calls = flaky_download([RuntimeError("503")] * fp.MAX_ATTEMPTS)
    monkeypatch.setattr(fp.yf, "download", download)

    assert fp.download_batch(["AAA"]) is None
    assert len(calls) == fp.MAX_ATTEMPTS
    # Exponential, and no wait after the final attempt
    assert sleeps == [fp.BACKOFF_BASE_S * 2 ** k for k in range(fp.MAX_ATTEMPTS - 1)]


def test_download_batch_no_wait_on_first_success(monkeypatch, sleeps):
    download, calls = flaky_download([make_raw(["AAA"])])
    monkeypatch.setattr(fp.yf, "download", download)

    assert fp.download_batch(["AAA"]) is not None
    assert len(calls) == 1
    assert sleeps == []


# ---------------------------------------------------------------------------
# main(): shuffle + split into N_OUTPUT_FILES
# ---------------------------------------------------------------------------
TICKERS = ["AAA", "BBB", "CCC", "NODATA"]
N_DAYS = 23


@pytest.fixture
def run_main(tmp_path, monkeypatch):
    """Run main() offline, writing into tmp_path. Returns the output dir."""
    def fake_download(batch, **_kwargs):
        return make_raw(batch, n_days=N_DAYS, empty=["NODATA"])

    constituents = pd.DataFrame({
        "TICKER": TICKERS,
        "COMPANY_NAME": [f"{t} Inc" for t in TICKERS],
        "SECTOR": ["Tech"] * len(TICKERS),
    })
    monkeypatch.setattr(fp, "get_constituents", lambda **_kwargs: constituents)
    monkeypatch.setattr(fp.yf, "download", fake_download)
    monkeypatch.setattr(fp.yf, "set_tz_cache_location", lambda _path: None)
    monkeypatch.setattr(fp, "BATCH_SIZE", 2)    # exercise more than one batch

    def _run(out_dir):
        monkeypatch.setattr(fp, "OUT_DIR", out_dir)
        assert fp.main() == 0
        return out_dir

    return lambda name="data": _run(tmp_path / name)


def read_parts(out_dir):
    paths = sorted(out_dir.glob(f"{fp.FILE_PREFIX}_*.csv.gz"))
    return paths, [pd.read_csv(p) for p in paths]


def test_main_writes_n_output_files(run_main):
    paths, _ = read_parts(run_main())

    assert [p.name for p in paths] == [
        f"{fp.FILE_PREFIX}_{k:02d}.csv.gz" for k in range(fp.N_OUTPUT_FILES)
    ]


def test_main_split_preserves_every_row_exactly_once(run_main):
    _, parts = read_parts(run_main())
    combined = pd.concat(parts, ignore_index=True)

    expected_rows = (len(TICKERS) - 1) * N_DAYS  # NODATA has no history
    assert len(combined) == expected_rows
    assert not combined.duplicated(subset=["TICKER", "DATE"]).any()
    assert set(combined["TICKER"]) == {"AAA", "BBB", "CCC"}
    # Files are balanced: sizes differ by at most one row
    sizes = [len(p) for p in parts]
    assert max(sizes) - min(sizes) <= 1


def test_main_output_schema(run_main):
    _, parts = read_parts(run_main())

    for part in parts:
        assert list(part.columns) == WANTED
    assert parts[0]["DATE"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()


def test_main_rows_are_shuffled_across_files(run_main):
    _, parts = read_parts(run_main())
    combined = pd.concat(parts, ignore_index=True)
    in_order = combined.sort_values(["TICKER", "DATE"]).reset_index(drop=True)

    assert not combined.equals(in_order)
    # Each file mixes tickers rather than holding one ticker's contiguous range
    assert all(part["TICKER"].nunique() > 1 for part in parts)


def test_main_shuffle_is_deterministic(run_main):
    first, _ = read_parts(run_main("run1"))
    second, _ = read_parts(run_main("run2"))

    for a, b in zip(first, second, strict=True):
        pd.testing.assert_frame_equal(pd.read_csv(a), pd.read_csv(b))


def test_main_ticker_dimension_excludes_tickers_without_history(run_main):
    dim = pd.read_csv(run_main() / "tickers.csv.gz")

    assert dim["TICKER"].tolist() == ["AAA", "BBB", "CCC"]


def test_main_lists_batches_that_still_failed(run_main, monkeypatch, sleeps, capsys):
    def download(batch, **_kwargs):
        if "CCC" in batch:                      # second batch: CCC, NODATA
            raise RuntimeError("503")
        return make_raw(batch, n_days=N_DAYS)

    monkeypatch.setattr(fp.yf, "download", download)
    _, parts = read_parts(run_main())
    out = capsys.readouterr().out

    assert set(pd.concat(parts)["TICKER"]) == {"AAA", "BBB"}
    assert len(sleeps) == fp.MAX_ATTEMPTS - 1
    assert "1 batch(es) failed" in out
    assert "batch at 2: CCC, NODATA" in out
    # Failed-batch tickers aren't misreported as having no price history
    assert "No price history" not in out


def test_main_all_batches_failed_returns_1_and_lists_them(
        tmp_path, monkeypatch, sleeps, capsys):
    download, _ = flaky_download([RuntimeError("503")] * fp.MAX_ATTEMPTS * 2)
    monkeypatch.setattr(fp.yf, "download", download)
    monkeypatch.setattr(fp, "get_constituents",
                        lambda **_kwargs: pd.DataFrame({"TICKER": TICKERS}))
    monkeypatch.setattr(fp.yf, "set_tz_cache_location", lambda _path: None)
    monkeypatch.setattr(fp, "BATCH_SIZE", 2)
    monkeypatch.setattr(fp, "OUT_DIR", tmp_path / "data")

    assert fp.main() == 1
    out = capsys.readouterr().out
    assert "2 batch(es) failed" in out
    assert "batch at 0: AAA, BBB" in out
    assert "batch at 2: CCC, NODATA" in out


# ---------------------------------------------------------------------------
# Constituent fallback: exit 1 unless --allow-fallback
# ---------------------------------------------------------------------------
@pytest.fixture
def wikipedia_down(monkeypatch):
    def _fail(*_args, **_kwargs):
        raise ImportError("Missing optional dependency 'lxml'")

    monkeypatch.setattr(fp.requests, "get", _fail)


def test_get_constituents_refuses_fallback_by_default(wikipedia_down, capsys):
    assert fp.get_constituents() is None
    assert "--allow-fallback" in capsys.readouterr().out


def test_get_constituents_falls_back_when_allowed(wikipedia_down):
    df = fp.get_constituents(allow_fallback=True)

    assert df["TICKER"].tolist() == fp.FALLBACK_TICKERS


def test_main_exits_1_on_fallback_without_downloading(
        wikipedia_down, tmp_path, monkeypatch):
    def download(*_args, **_kwargs):
        raise AssertionError("should not download")

    monkeypatch.setattr(fp.yf, "download", download)
    monkeypatch.setattr(fp.yf, "set_tz_cache_location", lambda _path: None)
    monkeypatch.setattr(fp, "OUT_DIR", tmp_path / "data")

    assert fp.main() == 1
    assert not list((tmp_path / "data").glob("*.csv.gz"))


def test_main_allow_fallback_builds_from_fallback_tickers(
        wikipedia_down, tmp_path, monkeypatch):
    monkeypatch.setattr(fp, "FALLBACK_TICKERS", ["AAA", "BBB"])
    monkeypatch.setattr(fp.yf, "download",
                        lambda batch, **_kwargs: make_raw(batch, n_days=N_DAYS))
    monkeypatch.setattr(fp.yf, "set_tz_cache_location", lambda _path: None)
    monkeypatch.setattr(fp, "OUT_DIR", tmp_path / "data")

    assert fp.main(allow_fallback=True) == 0
    _, parts = read_parts(tmp_path / "data")
    assert set(pd.concat(parts)["TICKER"]) == {"AAA", "BBB"}


def test_parse_args_allow_fallback_flag():
    assert fp.parse_args([]).allow_fallback is False
    assert fp.parse_args(["--allow-fallback"]).allow_fallback is True
