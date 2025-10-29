# Distributed Market Data Pipeline & Feature Store

This repository is a compact, end-to-end demo for building a market-data feature pipeline with an emphasis on: ingest/replay, distributed feature computation, point-in-time feature storage, lightweight model training/serving, and performance optimization (native hot paths).

Key highlights

- Synthetic market-data generator (tick + top-of-book) and replay utilities.
- Streaming sink abstraction supporting console, local file, and Kafka/Redpanda.
- Symbol-partitioned feature computation with a Dask-capable path and pandas fallback.
- Partitioned Parquet feature store with DuckDB point-in-time queries for lookups and validation.
- Simple scikit-learn training pipeline and FastAPI inference service with async batching.
- Resilience demos: worker-failure recovery (Dask), retry helpers, and orchestrator hooks.
- Native hot-path implemented with pybind11 for the order-book imbalance calculation (NumPy fallback available).
- Docker Compose and Kubernetes manifests to exercise local integration and horizontal scaling.

Repository layout (quick links)

- `src/market_pipeline/data.py` — synthetic tick data generation.
- `src/market_pipeline/features.py` — feature computations (VWAP, realized volatility, imbalance).
- `src/market_pipeline/native_features.py` & `cpp/order_book_imbalance.cpp` — native imbalance hot-path.
- `src/market_pipeline/storage.py` — write/read partitioned Parquet + DuckDB queries.
- `src/market_pipeline/modeling.py` — training, saving, and loading model artifacts.
- `src/market_pipeline/api.py` — FastAPI app for inference and feature lookup.
- `src/market_pipeline/benchmark.py` & `benchmarks/REPORT.md` — benchmark harness and recorded micro-results.
- `src/market_pipeline/cli.py` — command-line interface (lazy-loads heavy modules for faster startup).
- `docker-compose.yml`, `k8s/deployment.yaml`, `k8s/hpa.yaml` — local integration manifests.

Quickstart (local, development)

1. Create a Python environment and install dependencies:

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

2. Build the native hot-path (optional but recommended for imbalance benchmark):

```bash
python -m pip install pybind11
python setup.py build_ext --inplace
```

3. Generate data, compute features, and train the model (small example):

```bash
python -m market_pipeline generate --events 10000
python -m market_pipeline features --events 10000 --distributed
python -m market_pipeline train --events 10000
```

4. Start the inference API (after training):

```bash
python -m market_pipeline serve --host 0.0.0.0 --port 8000
```

Useful commands

- Replay events to console/file/kafka:
	- `python -m market_pipeline stream --sink console --events 1000 --rate 1000`
	- `python -m market_pipeline stream --sink file --events 1000 --output data/raw/live_replay.jsonl`
- Run micro-benchmarks (feature, training, native imbalance):
	- `python -m market_pipeline benchmark --events 2000 --distributed`
- Produce a markdown benchmark report:
	- `python -m market_pipeline report --output benchmarks/REPORT.md --events 2000 --distributed`

Architecture overview

1. Ingestion: `MarketDataGenerator` produces synthetic trades + top-of-book messages; `stream`/`replay` commands send events to sinks (file or Kafka).
2. Feature computation: symbol-first partitioning allows easy parallelism across workers (Dask-friendly). Features are written as partitioned Parquet by date/symbol.
3. Feature store & lookups: DuckDB provides fast point-in-time queries against the partitioned Parquet store for training and inference validation.
4. Modeling & serving: a small scikit-learn pipeline trains a direction classifier; `api.py` provides async-batched inference via FastAPI.
5. Optimization: compute-heavy hot-path (order-book imbalance) is implemented in C++ via pybind11; code falls back to NumPy if the extension is not present.

Benchmarks and results

See `benchmarks/REPORT.md` for the micro-benchmark numbers collected during local runs. The repository includes a `benchmark` CLI helper that measures feature computation throughput, training throughput, and the native imbalance path. Re-run the benchmark at larger scales to collect production-like numbers.

Running with Docker / Kubernetes (local integration)

- `docker-compose.yml` composes a local Redpanda (Kafka-compatible) broker plus the replayer and consumer services for an end-to-end demo.
- `k8s/deployment.yaml` and `k8s/hpa.yaml` provide a simple deployment + horizontal autoscaler example to exercise scaling the feature compute or API.

Notes, caveats, and next steps

- The included benchmarks are microbenchmarks (2k–100k events) to validate correctness and relative performance; for meaningful production numbers run at larger scales and on representative hardware.
- The CLI intentionally lazy-loads heavy modules (DuckDB, Dask) to allow `python -m market_pipeline` to be responsive in environments where those dependencies are optional.
- To fully validate resilience and scaling, run the `chaos-demo` and the Docker Compose or Kubernetes manifests locally.

Contributing and tests

- Add reproducible benchmark cases under `benchmarks/` and record `REPORT.md` outputs.
- Tests are not included in this demo — use the modular functions in `src/market_pipeline/` to add unit and integration tests.

If you want, I can run a larger benchmark (100k / 1M rows) next and update `benchmarks/REPORT.md` with the numbers.

