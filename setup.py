from __future__ import annotations

from pathlib import Path

from setuptools import Extension, setup

try:
    import pybind11
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("pybind11 is required to build the native extension") from exc


this_dir = Path(__file__).resolve().parent

ext_modules = [
    Extension(
        "market_pipeline_native",
        [str(this_dir / "cpp" / "order_book_imbalance.cpp")],
        include_dirs=[pybind11.get_include()],
        language="c++",
        extra_compile_args=["-O3", "-std=c++17"],
    )
]

setup(ext_modules=ext_modules)
