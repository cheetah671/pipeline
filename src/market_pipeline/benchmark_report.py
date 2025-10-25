from __future__ import annotations

from pathlib import Path

from .benchmark import benchmark_features, benchmark_training


def write_benchmark_report(output_path: Path, total_events: int = 100000, distributed: bool = False) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    feature_result = benchmark_features(total_events, distributed)
    training_result = benchmark_training(total_events)
    lines = [
        "# Benchmark Report",
        "",
        "| Component | Rows | Seconds | Rows/sec |",
        "| --- | ---: | ---: | ---: |",
        f"| Feature computation | {feature_result.rows} | {feature_result.seconds:.4f} | {feature_result.rows_per_second:.2f} |",
        f"| Training | {training_result.rows} | {training_result.seconds:.4f} | {training_result.rows_per_second:.2f} |",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path
