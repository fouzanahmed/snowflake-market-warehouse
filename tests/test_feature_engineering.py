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
