from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def write_partitioned_features(frame: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    writable = frame.copy()
    writable["date"] = pd.to_datetime(writable["timestamp"], utc=True).dt.date.astype(str)
    writable.to_parquet(output_dir, partition_cols=["date", "symbol"], index=False)
    return output_dir


def read_features(output_dir: Path) -> pd.DataFrame:
    return duckdb.query(
        f"SELECT * FROM read_parquet('{output_dir.as_posix()}/**/*.parquet')"
    ).to_df()


def query_point_in_time_features(output_dir: Path, symbol: str, timestamp: str) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM read_parquet('{output_dir.as_posix()}/**/*.parquet')
    WHERE symbol = '{symbol}'
      AND timestamp <= TIMESTAMP '{timestamp}'
    ORDER BY timestamp DESC
    LIMIT 1
    """
    return duckdb.query(query).to_df()
