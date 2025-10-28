# Benchmark Report

Summary of a local micro-benchmark run used to validate the pipeline and the native hot-path.

Commands executed
- `python setup.py build_ext --inplace` (built `market_pipeline_native` via pybind11)
- Ran the benchmark harness (2,000 events) using the packaged benchmark functions.

Environment
- Python interpreter: /home/arnav-agnihotri/miniconda3/envs/autograder/bin/python (CPython 3.12)
- Note: measured on the developer's local machine; results will vary by CPU, memory, and environment.

Results (2,000 events)

| Component | Input Size | Seconds | Rows/sec |
| --- | ---: | ---: | ---: |
| Feature computation (distributed) | 2,000 | 0.01721 | 116,179.99 |
| Training (single-process) | 2,000 | 0.01604 | 124,657.84 |
| Order-book imbalance (native hot-path) | 2,000 | 0.0000546 | 36,660,251.03 |

Notes
- These are microbenchmark numbers intended to validate the implementation and the native C++ hot-path. They are not representative of large-scale production workloads.
- To reproduce:
  1. Ensure `pybind11` is installed in the build environment.
  2. Run `python setup.py build_ext --inplace` from the repo root.
  3. Run the benchmark harness, e.g. `python -m market_pipeline benchmark --events 2000 --distributed` (or call the benchmark functions directly).

If you want, I can run larger-sized benchmarks (e.g., 100k / 1M rows) and commit the updated numbers, but those runs will take longer and may require more memory/time.
