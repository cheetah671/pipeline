from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

from .config import FeatureConfig, StreamConfig
from .data import MarketDataGenerator
from .features import compute_features, compute_features_distributed
from .modeling import train_model
from .native_features import order_book_imbalance


@dataclass(frozen=True)
class BenchmarkResult:
    rows: int
    seconds: float
    rows_per_second: float


def _rate(rows: int, seconds: float) -> BenchmarkResult:
    return BenchmarkResult(rows=rows, seconds=seconds, rows_per_second=rows / max(seconds, 1e-9))


def benchmark_features(total_events: int = 100000, distributed: bool = False) -> BenchmarkResult:
    generator = MarketDataGenerator(StreamConfig())
    frame = generator.generate_frame(total_events)
    start = perf_counter()
    if distributed:
        compute_features_distributed(frame, FeatureConfig())
    else:
        compute_features(frame, FeatureConfig())
    elapsed = perf_counter() - start
    return _rate(total_events, elapsed)


def benchmark_training(total_events: int = 100000) -> BenchmarkResult:
    generator = MarketDataGenerator(StreamConfig())
    frame = compute_features(generator.generate_frame(total_events), FeatureConfig())
    start = perf_counter()
    train_model(frame)
    elapsed = perf_counter() - start
    return _rate(total_events, elapsed)


def benchmark_order_book_imbalance(total_events: int = 100000) -> BenchmarkResult:
    generator = MarketDataGenerator(StreamConfig())
    frame = generator.generate_frame(total_events)
    start = perf_counter()
    order_book_imbalance(frame["bid_size"].to_numpy(), frame["ask_size"].to_numpy())
    elapsed = perf_counter() - start
    return _rate(total_events, elapsed)
