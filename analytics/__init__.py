"""analytics — Pipeline benchmarking, SQL library, and R bridge."""

from analytics.pipeline import baseline_pipeline, optimized_pipeline, benchmark
from analytics.sql_queries import (
    hourly_revenue, top_routes, monthly_growth,
    payment_breakdown, detect_anomalies,
)
from analytics.r_bridge import run_r_regression

__all__ = [
    "baseline_pipeline", "optimized_pipeline", "benchmark",
    "hourly_revenue", "top_routes", "monthly_growth",
    "payment_breakdown", "detect_anomalies",
    "run_r_regression",
]
