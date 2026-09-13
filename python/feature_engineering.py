"""
Pure-pandas mirror of sql/ddl/03_marts.sql's FEATURES_ROLLING table.

Kept independent of any live Snowflake connection so it's unit-testable
with a synthetic DataFrame, and reusable if you want to compute features
client-side before an initial load.
"""

import pandas as pd


def add_daily_return(df: pd.DataFrame) -> pd.DataFrame:
    """Adds DAILY_RETURN = pct change of ADJ_CLOSE, per TICKER, ordered by DATE."""
    out = df.sort_values(["TICKER", "DATE"]).copy()
    out["DAILY_RETURN"] = out.groupby("TICKER")["ADJ_CLOSE"].pct_change()
    return out


def add_rolling_features(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """Adds rolling mean return and rolling volatility (stddev of return)
    per TICKER, over the trailing `window` rows (inclusive of the current row,
    matching the SQL's ROWS BETWEEN N PRECEDING AND CURRENT ROW)."""
    if "DAILY_RETURN" not in df.columns:
        df = add_daily_return(df)

    out = df.copy()
    grouped = out.groupby("TICKER")["DAILY_RETURN"]
    out[f"ROLLING_{window}D_AVG_RETURN"] = grouped.transform(
        lambda s: s.rolling(window=window, min_periods=1).mean()
    )
    out[f"ROLLING_{window}D_VOLATILITY"] = grouped.transform(
        lambda s: s.rolling(window=window, min_periods=1).std()
    )
    return out


def build_features(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """End-to-end: daily return + rolling features, dropping the first
    per-ticker row (no return defined) to match the SQL mart's WHERE clause."""
    out = add_rolling_features(df, window=window)
    return out.dropna(subset=["DAILY_RETURN"]).reset_index(drop=True)
