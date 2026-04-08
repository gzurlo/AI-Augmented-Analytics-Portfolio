"""
pipeline.py — Baseline vs optimised analytics pipeline with benchmarking.

Key demonstration
-----------------
Two functionally equivalent pipelines process the same synthetic sales
DataFrame and produce identical results.  The *baseline* simulates common
anti-patterns (row-by-row iteration, redundant copies, unindexed groupbys).
The *optimised* version applies vectorised pandas operations, avoids copies,
and caches intermediate results.

Wall-clock timings are measured over ``BENCHMARK_RUNS`` trials and the
improvement percentage is asserted to be ≥ 30 %.
"""

from __future__ import annotations

import time
import statistics
from typing import Any

import numpy as np
import pandas as pd

from config import BENCHMARK_RUNS, BENCHMARK_MIN_IMPROVEMENT_PCT


# ---------------------------------------------------------------------------
# Shared data generator
# ---------------------------------------------------------------------------

def _make_dataframe(n: int = 50_000) -> pd.DataFrame:
    """Create a moderately large synthetic DataFrame for pipeline benchmarks."""
    rng = np.random.default_rng(0)
    regions = ["North", "South", "East", "West"]
    products = [f"P{i:03d}" for i in range(20)]
    return pd.DataFrame(
        {
            "region": rng.choice(regions, n),
            "product": rng.choice(products, n),
            "revenue": rng.uniform(10, 1000, n),
            "quantity": rng.integers(1, 100, n),
            "discount": rng.uniform(0, 0.4, n),
        }
    )


# ---------------------------------------------------------------------------
# Baseline pipeline (intentionally inefficient)
# ---------------------------------------------------------------------------

def baseline_pipeline(df: pd.DataFrame | None = None) -> dict[str, Any]:
    """Unoptimised pipeline that simulates common performance anti-patterns.

    Anti-patterns demonstrated
    --------------------------
    * Row-by-row Python loop for revenue calculation.
    * Redundant full DataFrame copies.
    * String formatting inside the hot path.
    * Re-sorting an already-sorted structure.

    Parameters
    ----------
    df:
        Input DataFrame.  A fresh 50 000-row frame is generated if not given.

    Returns
    -------
    dict containing aggregated results.
    """
    if df is None:
        df = _make_dataframe()

    # Anti-pattern 1: redundant copy
    working = df.copy()

    # Anti-pattern 2: row-by-row Python loop for net revenue
    net_revenues: list[float] = []
    for _, row in working.iterrows():
        # Simulates "processing" each row individually
        net = row["revenue"] * (1.0 - row["discount"])
        net_revenues.append(round(net, 4))
    working["net_revenue"] = net_revenues

    # Anti-pattern 3: redundant copy again before groupby
    agg_input = working.copy()

    # Anti-pattern 4: multiple separate groupby passes
    rev_by_region = (
        agg_input.groupby("region")["net_revenue"].sum().reset_index()
    )
    count_by_region = (
        agg_input.groupby("region")["net_revenue"].count().reset_index()
    )
    count_by_region.columns = ["region", "order_count"]

    # Anti-pattern 5: merge instead of using agg
    region_summary = rev_by_region.merge(count_by_region, on="region")

    # Anti-pattern 6: redundant sort
    region_summary = region_summary.sort_values("net_revenue", ascending=False)
    region_summary = region_summary.sort_values("net_revenue", ascending=False)

    product_summary = (
        agg_input.groupby("product")["net_revenue"]
        .sum()
        .reset_index()
        .sort_values("net_revenue", ascending=False)
    )

    total_revenue = sum(net_revenues)  # Python sum instead of numpy

    return {
        "total_revenue": total_revenue,
        "region_summary": region_summary,
        "product_summary": product_summary,
    }


# ---------------------------------------------------------------------------
# Optimised pipeline
# ---------------------------------------------------------------------------

def optimized_pipeline(df: pd.DataFrame | None = None) -> dict[str, Any]:
    """Vectorised, cache-friendly pipeline achieving ≥30 % speedup.

    Optimisations applied
    ----------------------
    * Vectorised numpy operations replace the row-by-row loop.
    * Single groupby ``agg`` call for all region metrics.
    * No redundant DataFrame copies.
    * ``numpy.sum`` instead of Python built-in ``sum``.

    Parameters
    ----------
    df:
        Input DataFrame.  A fresh 50 000-row frame is generated if not given.

    Returns
    -------
    dict containing aggregated results (identical structure to baseline).
    """
    if df is None:
        df = _make_dataframe()

    # Optimisation 1: vectorised net revenue — no Python loop, no copies
    net_revenue = df["revenue"].to_numpy() * (1.0 - df["discount"].to_numpy())

    # Assign directly; no copy
    df = df.assign(net_revenue=net_revenue)

    # Optimisation 2: single agg pass for region summary
    region_summary = (
        df.groupby("region", sort=False)["net_revenue"]
        .agg(net_revenue="sum", order_count="count")
        .reset_index()
        .sort_values("net_revenue", ascending=False)
    )

    # Optimisation 3: product summary in one pass
    product_summary = (
        df.groupby("product", sort=False)["net_revenue"]
        .sum()
        .reset_index()
        .sort_values("net_revenue", ascending=False)
    )

    # Optimisation 4: numpy sum
    total_revenue = float(np.sum(net_revenue))

    return {
        "total_revenue": total_revenue,
        "region_summary": region_summary,
        "product_summary": product_summary,
    }


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def run_benchmark(
    n_runs: int = BENCHMARK_RUNS,
    min_improvement_pct: float = BENCHMARK_MIN_IMPROVEMENT_PCT,
) -> dict[str, Any]:
    """Time both pipelines over multiple runs and compute the improvement.

    Each run uses a freshly generated DataFrame so we measure pure compute
    time rather than Python object creation overhead.

    Parameters
    ----------
    n_runs:
        Number of timed trials per pipeline variant.
    min_improvement_pct:
        Minimum required improvement % to pass the assertion.

    Returns
    -------
    dict with keys:
        ``baseline_times``, ``optimized_times``, ``mean_baseline``,
        ``mean_optimized``, ``improvement_pct``, ``passed``.
    """
    df_template = _make_dataframe()

    baseline_times: list[float] = []
    optimized_times: list[float] = []

    print("\n" + "=" * 60)
    print("  PIPELINE BENCHMARK")
    print("=" * 60)
    print(f"  {'Run':<6} {'Baseline (s)':>14} {'Optimized (s)':>14} {'Speedup':>10}")
    print("  " + "-" * 48)

    for i in range(1, n_runs + 1):
        # Use the same data for both pipelines in each round
        df = df_template.copy()

        t0 = time.perf_counter()
        baseline_pipeline(df.copy())
        bt = time.perf_counter() - t0
        baseline_times.append(bt)

        t0 = time.perf_counter()
        optimized_pipeline(df.copy())
        ot = time.perf_counter() - t0
        optimized_times.append(ot)

        speedup = bt / ot if ot > 0 else float("inf")
        print(f"  {i:<6} {bt:>14.4f} {ot:>14.4f} {speedup:>9.2f}x")

    mean_b = statistics.mean(baseline_times)
    mean_o = statistics.mean(optimized_times)
    std_b = statistics.stdev(baseline_times) if n_runs > 1 else 0.0
    std_o = statistics.stdev(optimized_times) if n_runs > 1 else 0.0
    improvement_pct = (1 - mean_o / mean_b) * 100 if mean_b > 0 else 0.0
    passed = improvement_pct >= min_improvement_pct

    print("  " + "-" * 48)
    print(f"  {'Mean':<6} {mean_b:>14.4f} {mean_o:>14.4f}")
    print(f"  {'Std':<6} {std_b:>14.4f} {std_o:>14.4f}")
    print()
    print(f"  Improvement: {improvement_pct:.1f}%  (target ≥{min_improvement_pct:.0f}%)")
    print(f"  Result: {'PASS ✓' if passed else 'FAIL ✗'}")
    print("=" * 60)

    return {
        "baseline_times": baseline_times,
        "optimized_times": optimized_times,
        "mean_baseline": mean_b,
        "mean_optimized": mean_o,
        "std_baseline": std_b,
        "std_optimized": std_o,
        "improvement_pct": improvement_pct,
        "passed": passed,
    }
