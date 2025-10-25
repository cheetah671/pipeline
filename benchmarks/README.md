# Benchmarks

Track throughput and latency numbers here after running the pipeline on your local machine.

## Suggested table

| Component | Language | Input Size | Throughput | p99 Latency | Notes |
| --- | --- | ---: | ---: | ---: | --- |
| Feature computation | Python/Dask | 10M rows | fill in | fill in | Parallel by symbol |
| Hot path (order-book imbalance) | C++/pybind11 | 10M rows | fill in | fill in | Native vectorized loop |
| Inference API | Python/FastAPI | 1k req | fill in | fill in | Async batching enabled |
| Hot path rewrite | C++/Go | 10M rows | fill in | fill in | Before/after comparison |

## How to fill this in

1. Build the extension with `python setup.py build_ext --inplace`.
2. Run `python -m market_pipeline benchmark --events 100000 --distributed`.
3. Copy the reported rows/sec values into the table above.

## Recorded results (local run)

The following micro-benchmarks were collected by building the native extension and running the benchmark harness with a 2,000-event workload on the developer machine.

| Component | Language | Input Size | Throughput (rows/sec) | Notes |
| --- | --- | ---: | ---: | --- |
| Feature computation | Python/Dask | 2,000 rows | 116,179.99 | Distributed symbol-parallel run (microbenchmark) |
| Training | Python/scikit-learn | 2,000 rows | 124,657.84 | Single-process training loop |
| Hot path (order-book imbalance) | C++/pybind11 | 2,000 rows | 36,660,251.03 | Native hot-path measured vs NumPy fallback |

See `REPORT.md` for details and the exact commands used.
