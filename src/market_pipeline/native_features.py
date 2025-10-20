from __future__ import annotations

import numpy as np

try:
    import market_pipeline_native as native_extension
except ImportError:  # pragma: no cover
    native_extension = None


def order_book_imbalance(bid_size: np.ndarray, ask_size: np.ndarray) -> np.ndarray:
    bid = np.asarray(bid_size, dtype=np.float64)
    ask = np.asarray(ask_size, dtype=np.float64)
    if native_extension is not None:
        return native_extension.order_book_imbalance(bid, ask)
    total_depth = bid + ask
    return np.where(total_depth > 0, (bid - ask) / total_depth, 0.0)
