import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))

from forecast_baseline import evaluate, make_target


@pytest.fixture
def synthetic_prices():
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=200, freq="D")
    frames = []
    for ticker in ["AAA", "BBB", "CCC"]:
        prices = 100 * np.cumprod(1 + rng.normal(0, 0.01, size=len(dates)))
        frames.append(pd.DataFrame({"TICKER": ticker, "DATE": dates, "ADJ_CLOSE": prices}))
    return pd.concat(frames, ignore_index=True)


def test_make_target_shifts_forward(synthetic_prices):
    from feature_engineering import build_features
    features = build_features(synthetic_prices)
    labeled = make_target(features)
    # every row should have a forward-looking target, none from the last date per ticker
    assert "NEXT_DAY_RETURN" in labeled.columns
    assert labeled["NEXT_DAY_RETURN"].isna().sum() == 0


def test_evaluate_returns_expected_keys(synthetic_prices):
    result = evaluate(synthetic_prices, window=10)
    assert set(result.keys()) == {"n_train", "n_test", "linreg_mae", "naive_zero_mae"}
    assert result["n_train"] > 0
    assert result["n_test"] > 0
    assert result["linreg_mae"] >= 0
    assert result["naive_zero_mae"] >= 0


def test_evaluate_raises_on_insufficient_data():
    tiny = pd.DataFrame({
        "TICKER": ["AAA"],
        "DATE": pd.to_datetime(["2024-01-01"]),
        "ADJ_CLOSE": [100.0],
    })
    with pytest.raises(ValueError):
        evaluate(tiny)
