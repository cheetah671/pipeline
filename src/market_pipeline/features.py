from __future__ import annotations

import numpy as np
import pandas as pd

from .config import FeatureConfig
from .native_features import order_book_imbalance


FEATURE_COLUMNS = [
    "vwap",
    "realized_volatility",
    "order_book_imbalance",
    "mid_price",
    "future_return",
    "target",
]


def _compute_symbol_features(frame: pd.DataFrame, config: FeatureConfig) -> pd.DataFrame:
    ordered = frame.sort_values("timestamp").copy()
    mid_price = (ordered["bid_price"] + ordered["ask_price"]) / 2.0
    ordered["mid_price"] = mid_price
    ordered["notional"] = ordered["price"] * ordered["size"]
    ordered["vwap"] = (
        ordered["notional"].rolling(config.rolling_window, min_periods=1).sum()
        / ordered["size"].rolling(config.rolling_window, min_periods=1).sum()
    )
    returns = mid_price.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    ordered["realized_volatility"] = (
        returns.rolling(config.rolling_window, min_periods=2).std().fillna(0.0)
    )
    ordered["order_book_imbalance"] = order_book_imbalance(ordered["bid_size"].to_numpy(), ordered["ask_size"].to_numpy())
    ordered["future_mid_price"] = ordered["mid_price"].shift(-config.horizon)
    ordered["future_return"] = (
        ordered["future_mid_price"] / ordered["mid_price"] - 1.0
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    ordered["target"] = (ordered["future_return"] > 0).astype(int)
    return ordered


def compute_features(frame: pd.DataFrame, config: FeatureConfig | None = None) -> pd.DataFrame:
    config = config or FeatureConfig()
    if frame.empty:
        return frame.copy()
    groups = []
    for symbol, symbol_frame in frame.groupby("symbol", sort=False):
        groups.append(_compute_symbol_features(symbol_frame, config))
    result = pd.concat(groups, ignore_index=True)
    return result.sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def compute_features_distributed(frame: pd.DataFrame, config: FeatureConfig | None = None) -> pd.DataFrame:
    config = config or FeatureConfig()
    try:
        from dask import delayed, compute
    except ImportError:
        return compute_features(frame, config)

    delayed_frames = [delayed(_compute_symbol_features)(symbol_frame, config) for _, symbol_frame in frame.groupby("symbol", sort=False)]
    computed = compute(*delayed_frames)
    return pd.concat(computed, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def feature_payload(row: pd.Series) -> dict[str, float]:
    return {
        "vwap": float(row["vwap"]),
        "realized_volatility": float(row["realized_volatility"]),
        "order_book_imbalance": float(row["order_book_imbalance"]),
        "mid_price": float(row["mid_price"]),
    }
