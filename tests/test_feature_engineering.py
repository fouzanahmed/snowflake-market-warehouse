import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))

from feature_engineering import add_daily_return, add_rolling_features, build_features


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "TICKER": ["AAA"] * 5 + ["BBB"] * 5,
        "DATE": pd.to_datetime(
            ["2024-01-0" + str(i) for i in range(1, 6)] * 2
        ),
        "ADJ_CLOSE": [100, 102, 101, 105, 110, 50, 49, 51, 52, 53],
    })


def test_add_daily_return_first_row_is_nan(sample_df):
    out = add_daily_return(sample_df)
    first_aaa = out[out["TICKER"] == "AAA"].iloc[0]
    assert pd.isna(first_aaa["DAILY_RETURN"])


def test_add_daily_return_value(sample_df):
    out = add_daily_return(sample_df)
    aaa = out[out["TICKER"] == "AAA"].reset_index(drop=True)
    # 102/100 - 1 = 0.02
    assert aaa.loc[1, "DAILY_RETURN"] == pytest.approx(0.02)


def test_rolling_features_columns_present(sample_df):
    out = add_rolling_features(sample_df, window=3)
    assert "ROLLING_3D_AVG_RETURN" in out.columns
    assert "ROLLING_3D_VOLATILITY" in out.columns


def test_rolling_features_isolated_per_ticker(sample_df):
    out = add_rolling_features(sample_df, window=20)
    # BBB's rolling average should not be influenced by AAA's returns
    aaa_avg = out[out["TICKER"] == "AAA"]["ROLLING_20D_AVG_RETURN"].dropna()
    bbb_avg = out[out["TICKER"] == "BBB"]["ROLLING_20D_AVG_RETURN"].dropna()
    assert not aaa_avg.equals(bbb_avg)


def test_build_features_drops_first_row_per_ticker(sample_df):
    out = build_features(sample_df)
    # 5 rows per ticker minus 1 (no return on first day) = 4 per ticker
    assert len(out[out["TICKER"] == "AAA"]) == 4
    assert len(out[out["TICKER"] == "BBB"]) == 4
    assert out["DAILY_RETURN"].isna().sum() == 0


# --- Edge cases: short history, single-row tickers, duplicate (TICKER, DATE) rows ---


def test_add_rolling_features_ticker_shorter_than_window():
    # Only 3 rows for a window of 20 (e.g. a recent IPO with little price history).
    df = pd.DataFrame({
        "TICKER": ["CCC"] * 3,
        "DATE": pd.to_datetime(["2024-02-01", "2024-02-02", "2024-02-03"]),
        "ADJ_CLOSE": [10.0, 11.0, 9.0],
    })
    out = add_rolling_features(df, window=20).reset_index(drop=True)
    assert len(out) == 3
    # First row has no return yet, so rolling stats are undefined.
    assert pd.isna(out.loc[0, "ROLLING_20D_AVG_RETURN"])
    assert pd.isna(out.loc[0, "ROLLING_20D_VOLATILITY"])
    # min_periods=1 means later rows still get a value despite < window observations.
    assert out.loc[1, "ROLLING_20D_AVG_RETURN"] == pytest.approx(0.1)
    # Volatility needs at least 2 return observations (std is undefined for n=1).
    assert pd.isna(out.loc[1, "ROLLING_20D_VOLATILITY"])
    assert not pd.isna(out.loc[2, "ROLLING_20D_VOLATILITY"])


def test_add_rolling_features_single_row_ticker_has_no_return():
    # A brand-new ticker with exactly one observed price has no return at all.
    df = pd.DataFrame({
        "TICKER": ["SOLO"],
        "DATE": pd.to_datetime(["2024-03-01"]),
        "ADJ_CLOSE": [42.0],
    })
    out = add_rolling_features(df, window=20)
    assert pd.isna(out.iloc[0]["DAILY_RETURN"])
    assert pd.isna(out.iloc[0]["ROLLING_20D_AVG_RETURN"])
    assert pd.isna(out.iloc[0]["ROLLING_20D_VOLATILITY"])


def test_build_features_drops_single_row_ticker_entirely():
    # build_features drops rows with no DAILY_RETURN, so a single-row ticker
    # disappears completely from the output rather than producing a NaN row.
    df = pd.DataFrame({
        "TICKER": ["SOLO", "AAA", "AAA"],
        "DATE": pd.to_datetime(["2024-03-01", "2024-01-01", "2024-01-02"]),
        "ADJ_CLOSE": [42.0, 100.0, 105.0],
    })
    out = build_features(df)
    assert "SOLO" not in out["TICKER"].values
    assert len(out[out["TICKER"] == "AAA"]) == 1


def test_add_daily_return_duplicate_ticker_date_rows_not_deduped():
    # Bad source data can produce two rows for the same (TICKER, DATE).
    df = pd.DataFrame({
        "TICKER": ["AAA", "AAA", "AAA"],
        "DATE": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02"]),
        "ADJ_CLOSE": [100.0, 102.0, 101.0],
    })
    out = add_daily_return(df)
    # Duplicates are preserved, not collapsed.
    assert len(out) == 3
    # Only the very first row for the ticker has an undefined return; the
    # duplicate date's second row gets a return computed against the first.
    assert out["DAILY_RETURN"].isna().sum() == 1


def test_build_features_duplicate_ticker_date_rows_not_deduped():
    df = pd.DataFrame({
        "TICKER": ["AAA", "AAA", "AAA"],
        "DATE": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02"]),
        "ADJ_CLOSE": [100.0, 102.0, 101.0],
    })
    out = build_features(df)
    # build_features only drops rows with an undefined return; it does not
    # deduplicate on (TICKER, DATE), so the duplicate date can still appear once.
    assert len(out) == 2
    assert (out["DATE"] == pd.Timestamp("2024-01-01")).sum() == 1
