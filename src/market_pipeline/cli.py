from __future__ import annotations

import argparse
from pathlib import Path


from .config import DEFAULT_PATHS, FeatureConfig, StreamConfig
from .data import MarketDataGenerator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Distributed market data pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="Generate synthetic market data")
    generate_parser.add_argument("--events", type=int, default=10000)
    generate_parser.add_argument("--output", type=Path, default=DEFAULT_PATHS.raw_dir / "generated.parquet")

    features_parser = subparsers.add_parser("features", help="Compute rolling features")
    features_parser.add_argument("--events", type=int, default=10000)
    features_parser.add_argument("--distributed", action="store_true")
    features_parser.add_argument("--output-dir", type=Path, default=DEFAULT_PATHS.processed_dir)

    train_parser = subparsers.add_parser("train", help="Train the direction model")
    train_parser.add_argument("--events", type=int, default=10000)

    query_parser = subparsers.add_parser("query", help="Query point-in-time features")
    query_parser.add_argument("symbol")
    query_parser.add_argument("timestamp")
    query_parser.add_argument("--feature-dir", type=Path, default=DEFAULT_PATHS.processed_dir)

    run_parser = subparsers.add_parser("run", help="Run the end-to-end batch pipeline")
    run_parser.add_argument("--events", type=int, default=10000)

    serve_parser = subparsers.add_parser("serve", help="Start the inference API")
    serve_parser.add_argument("--model-path", type=Path, default=Path("models/price_direction_model.joblib"))
    serve_parser.add_argument("--feature-dir", type=Path, default=DEFAULT_PATHS.processed_dir)
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=8000)

    prefect_parser = subparsers.add_parser("flow", help="Run the orchestration flow")
    prefect_parser.add_argument("--events", type=int, default=10000)

    benchmark_parser = subparsers.add_parser("benchmark", help="Measure feature and training throughput")
    benchmark_parser.add_argument("--events", type=int, default=100000)
    benchmark_parser.add_argument("--distributed", action="store_true")

    stream_parser = subparsers.add_parser("stream", help="Replay synthetic events to a sink")
    stream_parser.add_argument("--events", type=int, default=1000)
    stream_parser.add_argument("--rate", type=int, default=1000)
    stream_parser.add_argument("--sink", choices=["console", "file", "kafka"], default="console")
    stream_parser.add_argument("--output", type=Path, default=DEFAULT_PATHS.raw_dir / "live_replay.jsonl")
    stream_parser.add_argument("--topic", default="trades")
    stream_parser.add_argument("--bootstrap-servers", default="localhost:9092")

    consume_parser = subparsers.add_parser("consume", help="Consume a local replay file")
    consume_parser.add_argument("--source", choices=["file", "kafka"], default="file")
    consume_parser.add_argument("--input", type=Path, default=DEFAULT_PATHS.raw_dir / "live_replay.jsonl")
    consume_parser.add_argument("--topic", default="trades")
    consume_parser.add_argument("--bootstrap-servers", default="localhost:9092")

    report_parser = subparsers.add_parser("report", help="Write benchmark report markdown")
    report_parser.add_argument("--output", type=Path, default=DEFAULT_PATHS.benchmarks_dir / "REPORT.md")
    report_parser.add_argument("--events", type=int, default=100000)
    report_parser.add_argument("--distributed", action="store_true")

    subparsers.add_parser("retry-demo", help="Show retry behavior with an injected failure")

    chaos_parser = subparsers.add_parser("chaos-demo", help="Kill a Dask worker mid-job and recover")
    chaos_parser.add_argument("--events", type=int, default=50000)
    chaos_parser.add_argument("--symbol", default="AAPL")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    generator = MarketDataGenerator(StreamConfig())
    feature_config = FeatureConfig()

    if args.command == "generate":
        frame = generator.generate_frame(args.events)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(args.output, index=False)
        print(f"wrote {len(frame)} rows to {args.output}")

    elif args.command == "features":
        from .features import compute_features, compute_features_distributed
        from .storage import write_partitioned_features

        frame = generator.generate_frame(args.events)
        features = compute_features_distributed(frame, feature_config) if args.distributed else compute_features(frame, feature_config)
        feature_path = write_partitioned_features(features, args.output_dir)
        print(f"wrote features to {feature_path}")

    elif args.command == "train":
        from .features import compute_features
        from .modeling import train_model, save_model

        frame = compute_features(generator.generate_frame(args.events), feature_config)
        model, metrics = train_model(frame)
        path = save_model(model)
        print({"model_path": str(path), "metrics": metrics})

    elif args.command == "query":
        from .storage import query_point_in_time_features

        frame = query_point_in_time_features(args.feature_dir, args.symbol, args.timestamp)
        print(frame.to_dict(orient="records"))

    elif args.command == "run":
        from .orchestration import PipelineOrchestrator

        orchestrator = PipelineOrchestrator()
        print(orchestrator.run_daily_batch(args.events))

    elif args.command == "serve":
        import uvicorn
        from .api import AppState, create_app

        state = AppState(model_path=args.model_path, feature_store_path=args.feature_dir)
        uvicorn.run(create_app(state), host=args.host, port=args.port)

    elif args.command == "flow":
        from .orchestration import market_pipeline_flow

        print(market_pipeline_flow(args.events))

    elif args.command == "benchmark":
        from .benchmark import benchmark_features, benchmark_training, benchmark_order_book_imbalance

        features = benchmark_features(args.events, args.distributed)
        training = benchmark_training(args.events)
        imbalance = benchmark_order_book_imbalance(args.events)
        print({"features": features.__dict__, "training": training.__dict__, "order_book_imbalance": imbalance.__dict__})

    elif args.command == "stream":
        from .demo import console_replay, replay_to_local_file, replay_to_kafka

        if args.sink == "console":
            console_replay(args.events, args.rate)
        elif args.sink == "file":
            replay_to_local_file(args.output, args.events, args.rate)
        else:
            replay_to_kafka(args.topic, args.events, args.rate, args.bootstrap_servers)

    elif args.command == "consume":
        from .demo import log_local_file_events, consume_from_kafka

        if args.source == "file":
            print({"consumed": log_local_file_events(args.input)})
        else:
            print({"consumed": consume_from_kafka(args.topic, args.bootstrap_servers)})

    elif args.command == "report":
        from .benchmark_report import write_benchmark_report

        path = write_benchmark_report(args.output, args.events, args.distributed)
        print({"report_path": str(path)})

    elif args.command == "retry-demo":
        from .resilience import run_with_retries

        state = {"count": 0}

        def flaky_operation() -> str:
            state["count"] += 1
            if state["count"] < 2:
                raise RuntimeError("injected failure")
            return "recovered"

        result, retry_result = run_with_retries(flaky_operation, retries=2)
        print({"result": result, "attempts": retry_result.attempts, "succeeded": retry_result.succeeded})

    elif args.command == "chaos-demo":
        from .dask_demo import run_dask_worker_failure_demo

        print(run_dask_worker_failure_demo(args.events, args.symbol))


if __name__ == "__main__":
    main()
