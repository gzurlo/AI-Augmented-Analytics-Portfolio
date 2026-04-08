"""
analytics — Core analytics layer.

Exposes the pipeline benchmarker, SQL query library, and R bridge.
"""

from analytics.pipeline import baseline_pipeline, optimized_pipeline, run_benchmark
from analytics.sql_queries import SQLQueryLibrary
from analytics.r_bridge import run_r_analysis

__all__ = [
    "baseline_pipeline",
    "optimized_pipeline",
    "run_benchmark",
    "SQLQueryLibrary",
    "run_r_analysis",
]
