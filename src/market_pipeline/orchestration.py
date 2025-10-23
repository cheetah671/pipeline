from __future__ import annotations

from pathlib import Path

from .config import DEFAULT_PATHS, FeatureConfig, StreamConfig
from .data import MarketDataGenerator
from .features import compute_features_distributed
from .modeling import save_model, train_model
from .storage import write_partitioned_features


class PipelineOrchestrator:
    def __init__(self, raw_dir: Path | None = None, processed_dir: Path | None = None) -> None:
        self.raw_dir = raw_dir or DEFAULT_PATHS.raw_dir
        self.processed_dir = processed_dir or DEFAULT_PATHS.processed_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def run_daily_batch(self, total_events: int = 10000) -> dict[str, object]:
        generator = MarketDataGenerator(StreamConfig())
        raw_frame = generator.generate_frame(total_events)
        raw_path = self.raw_dir / "trades.parquet"
        raw_frame.to_parquet(raw_path, index=False)
        feature_frame = compute_features_distributed(raw_frame, FeatureConfig())
        feature_path = write_partitioned_features(feature_frame, self.processed_dir)
        model, metrics = train_model(feature_frame)
        model_path = save_model(model)
        return {
            "raw_path": str(raw_path),
            "feature_path": str(feature_path),
            "model_path": str(model_path),
            "metrics": metrics,
            "rows": int(len(feature_frame)),
        }


try:
    from prefect import flow, task
except ImportError:  # pragma: no cover
    flow = None
    task = None


if flow is not None:
    @task(retries=2, retry_delay_seconds=5)
    def generate_features_task(total_events: int) -> dict[str, object]:
        orchestrator = PipelineOrchestrator()
        return orchestrator.run_daily_batch(total_events)

    @flow(name="market-pipeline")
    def market_pipeline_flow(total_events: int = 10000) -> dict[str, object]:
        return generate_features_task(total_events)
else:
    def market_pipeline_flow(total_events: int = 10000) -> dict[str, object]:
        orchestrator = PipelineOrchestrator()
        return orchestrator.run_daily_batch(total_events)
