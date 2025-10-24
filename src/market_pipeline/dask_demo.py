from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from .config import FeatureConfig
from .data import MarketDataGenerator
from .features import compute_features, _compute_symbol_features


def _chaos_partition_worker(
    symbol_frame: pd.DataFrame,
    config: FeatureConfig,
    marker_path: Path,
    inject_symbol: str,
) -> pd.DataFrame:
    if symbol_frame.empty:
        return symbol_frame

    symbol = str(symbol_frame.iloc[0]["symbol"])
    if symbol == inject_symbol and not marker_path.exists():
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_path.write_text("triggered", encoding="utf-8")
        os._exit(1)

    return _compute_symbol_features(symbol_frame, config)


def run_dask_worker_failure_demo(
    total_events: int = 50000,
    inject_symbol: str = "AAPL",
    marker_path: Path | None = None,
) -> dict[str, object]:
    try:
        from dask.distributed import Client, LocalCluster
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install dask[complete] to run the worker failure demo") from exc

    chaos_marker = marker_path or Path("/tmp/market_pipeline_dask_chaos.flag")
    if chaos_marker.exists():
        chaos_marker.unlink()

    generator = MarketDataGenerator()
    frame = generator.generate_frame(total_events)
    config = FeatureConfig()

    cluster = LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        dashboard_address=None,
        silence_logs=False,
    )
    client = Client(cluster)
    try:
        futures = []
        for symbol, symbol_frame in frame.groupby("symbol", sort=False):
            futures.append(
                client.submit(
                    _chaos_partition_worker,
                    symbol_frame,
                    config,
                    chaos_marker,
                    inject_symbol,
                    retries=1,
                )
            )

        computed = client.gather(futures)
        result = pd.concat(computed, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        return {
            "rows": int(len(result)),
            "workers": len(cluster.workers),
            "failed_once": chaos_marker.exists(),
            "symbols": sorted(result["symbol"].unique().tolist()),
        }
    finally:
        client.close()
        cluster.close()
