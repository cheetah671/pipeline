from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelinePaths:
    project_root: Path = Path(__file__).resolve().parents[2]
    data_dir: Path = project_root / "data"
    raw_dir: Path = data_dir / "raw"
    processed_dir: Path = data_dir / "processed"
    models_dir: Path = project_root / "models"
    benchmarks_dir: Path = project_root / "benchmarks"


@dataclass(frozen=True)
class StreamConfig:
    symbols: tuple[str, ...] = ("AAPL", "MSFT", "NVDA", "BTC-USD", "ETH-USD")
    events_per_second: int = 1000
    batch_size: int = 1024
    seed: int = 7


@dataclass(frozen=True)
class FeatureConfig:
    rolling_window: int = 20
    horizon: int = 5


DEFAULT_PATHS = PipelinePaths()
