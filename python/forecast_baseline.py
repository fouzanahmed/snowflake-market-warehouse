"""
Baseline next-day-return forecaster: linear regression on rolling features
vs. a naive "predict zero return" baseline. This is a baseline model for
comparison, not a production forecasting system -- see README for what
that would take (walk-forward validation, transaction costs, etc.).

    python forecast_baseline.py path/to/prices.csv.gz [path2.csv.gz ...]
"""

import sys

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

from feature_engineering import build_features


def make_target(df: pd.DataFrame) -> pd.DataFrame:
    """NEXT_DAY_RETURN = next row's DAILY_RETURN, per ticker."""
    out = df.sort_values(["TICKER", "DATE"]).copy()
    out["NEXT_DAY_RETURN"] = out.groupby("TICKER")["DAILY_RETURN"].shift(-1)
    return out.dropna(subset=["NEXT_DAY_RETURN"])


def evaluate(df: pd.DataFrame, window: int = 20, test_frac: float = 0.2) -> dict:
    """Chronological (not random) train/test split per the DATE column, so
    the test set is strictly later in time than the train set -- avoids the
    lookahead bias a random split would introduce."""
    features = build_features(df, window=window)
    labeled = make_target(features)

    feature_cols = [f"ROLLING_{window}D_AVG_RETURN", f"ROLLING_{window}D_VOLATILITY"]
    labeled = labeled.dropna(subset=feature_cols)

    cutoff = labeled["DATE"].quantile(1 - test_frac)
    train = labeled[labeled["DATE"] <= cutoff]
    test = labeled[labeled["DATE"] > cutoff]

    if len(train) == 0 or len(test) == 0:
        raise ValueError("Not enough data for a train/test split -- widen the date range.")

    X_train, y_train = train[feature_cols], train["NEXT_DAY_RETURN"]
    X_test, y_test = test[feature_cols], test["NEXT_DAY_RETURN"]

    model = LinearRegression()
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    naive_preds = np.zeros_like(y_test)

    return {
        "n_train": len(train),
        "n_test": len(test),
        "linreg_mae": mean_absolute_error(y_test, preds),
        "naive_zero_mae": mean_absolute_error(y_test, naive_preds),
    }


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 1

    frames = [pd.read_csv(p, parse_dates=["DATE"]) for p in sys.argv[1:]]
    df = pd.concat(frames, ignore_index=True)

    result = evaluate(df)
    print(f"Train rows: {result['n_train']:,}  Test rows: {result['n_test']:,}")
    print(f"Naive (predict 0 return) MAE: {result['naive_zero_mae']:.6f}")
    print(f"Linear regression MAE:        {result['linreg_mae']:.6f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
