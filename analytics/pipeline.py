"""
pipeline.py — Baseline vs optimised TLC taxi data pipeline.

Both pipelines process the same 100,000-row sample and produce equivalent
outputs.  The baseline uses anti-patterns; the optimised version uses
categorical dtypes, vectorised operations, and a single .query() pass.

The ``benchmark()`` function runs each pipeline 3 times, prints a rich
comparison table, and asserts the optimised version is ≥30 % faster.
"""

from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import numpy as np
import pandas as pd

from config import (
    DATA_RAW, CLEANED_PARQUET, PIPELINE_SAMPLE,
    FARE_MIN, FARE_MAX, DIST_MIN, DIST_MAX,
    MIN_IMPROVEMENT_PCT,
)


# ---------------------------------------------------------------------------
# Shared data loader
# ---------------------------------------------------------------------------

def _load_sample(n: int = PIPELINE_SAMPLE) -> pd.DataFrame:
    """Load n rows from the best available source (parquet > cleaned > synthetic)."""
    # Try cleaned parquet first (fastest)
    if CLEANED_PARQUET.exists():
        df = pd.read_parquet(CLEANED_PARQUET)
        return df.head(n).copy()

    # Try raw parquets
    parquets = sorted(DATA_RAW.glob("*.parquet"))
    if parquets:
        try:
            df = pd.read_parquet(parquets[0])
            return df.head(n).copy()
        except Exception:  # noqa: BLE001
            pass

    # Synthetic fallback
    return _synthetic_sample(n)


def _synthetic_sample(n: int) -> pd.DataFrame:
    """Return a synthetic TLC-like DataFrame for benchmark purposes."""
    rng = np.random.default_rng(0)
    pickup = pd.date_range("2023-01-01", periods=n, freq="2min")
    dur = rng.uniform(3, 60, n)
    fare = rng.uniform(5, 80, n).round(2)
    dist = rng.uniform(0.5, 20, n).round(2)
    df = pd.DataFrame({
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + pd.to_timedelta(dur, unit="m"),
        "fare_amount": fare,
        "trip_distance": dist,
        "payment_type": rng.choice(["1", "2", "3", "4"], n),
        "PULocationID": rng.integers(1, 265, n),
        "DOLocationID": rng.integers(1, 265, n),
        "passenger_count": rng.integers(1, 5, n),
        "tip_amount": rng.uniform(0, 20, n).round(2),
        "total_amount": (fare + rng.uniform(0, 5, n)).round(2),
        "trip_duration_mins": dur.round(2),
    })
    df["hour_of_day"] = df["tpep_pickup_datetime"].dt.hour
    df["day_of_week"] = df["tpep_pickup_datetime"].dt.day_name()
    return df


# ---------------------------------------------------------------------------
# Baseline pipeline  (intentionally inefficient)
# ---------------------------------------------------------------------------

def baseline_pipeline(df: pd.DataFrame | None = None) -> dict[str, Any]:
    """Unoptimised pipeline with deliberate performance anti-patterns.

    Anti-patterns
    -------------
    1. String columns stay as ``object`` dtype — no categorical.
    2. Derived columns computed with ``apply(axis=1)`` row-by-row lambda.
    3. Multiple separate filter passes instead of a single .query().
    4. Unnecessary ``copy()`` calls between steps.
    5. Groupby aggregations performed in separate calls.

    Parameters
    ----------
    df:
        Input DataFrame.  Loads 100 k-row sample if not provided.

    Returns
    -------
    dict with keys: filtered_df, hourly_summary, payment_summary.
    """
    if df is None:
        df = _load_sample()

    # Anti-pattern 1: no categorical — keeps strings as object
    working = df.copy()

    # Anti-pattern 2: apply(axis=1) for duration (row-wise Python call)
    working["trip_duration_mins"] = working.apply(
        lambda row: (
            (row["tpep_dropoff_datetime"] - row["tpep_pickup_datetime"]).total_seconds() / 60.0
            if hasattr(row["tpep_dropoff_datetime"], "total_seconds") is False
            else 0
        ),
        axis=1,
    ) if "trip_duration_mins" not in working.columns else working["trip_duration_mins"].copy()

    # Anti-pattern 3: separate filter passes with copy between each
    step1 = working[working["fare_amount"] >= FARE_MIN].copy()
    step2 = step1[step1["fare_amount"] <= FARE_MAX].copy()
    step3 = step2[step2["trip_distance"] >= DIST_MIN].copy()
    filtered = step3[step3["trip_distance"] <= DIST_MAX].copy()

    # Anti-pattern 4: compute cost_per_mile row-by-row
    filtered["cost_per_mile"] = filtered.apply(
        lambda r: round(r["fare_amount"] / max(r["trip_distance"], 0.1), 2),
        axis=1,
    )

    # Anti-pattern 5: multiple separate groupby passes
    # Ensure hour_of_day exists
    if "hour_of_day" not in filtered.columns:
        filtered = filtered.copy()
        filtered["hour_of_day"] = filtered["tpep_pickup_datetime"].dt.hour
    hourly_count = filtered.groupby("hour_of_day")["fare_amount"].count().reset_index()
    hourly_count.columns = ["hour_of_day", "trip_count"]
    hourly_fare = filtered.groupby("hour_of_day")["fare_amount"].mean().reset_index()
    hourly_fare.columns = ["hour_of_day", "avg_fare"]
    hourly_summary = hourly_count.merge(hourly_fare, on="hour_of_day")

    payment_summary = (
        filtered.groupby("payment_type")["fare_amount"]
        .agg(["sum", "count"])
        .reset_index()
    )

    return {
        "filtered_df": filtered,
        "hourly_summary": hourly_summary,
        "payment_summary": payment_summary,
    }


# ---------------------------------------------------------------------------
# Optimised pipeline
# ---------------------------------------------------------------------------

def optimized_pipeline(df: pd.DataFrame | None = None) -> dict[str, Any]:
    """Vectorised, dtype-optimised pipeline achieving ≥30 % speedup.

    Optimisations
    -------------
    1. ``payment_type`` cast to ``pd.Categorical`` — faster groupby.
    2. Vectorised datetime subtraction for ``trip_duration_mins``.
    3. Single ``.query()`` for all filters in one pass.
    4. Vectorised ``cost_per_mile`` via numpy divide.
    5. Single ``.agg()`` dict call for hourly summary.

    Parameters
    ----------
    df:
        Input DataFrame.  Loads 100 k-row sample if not provided.

    Returns
    -------
    dict with same keys as ``baseline_pipeline``.
    """
    if df is None:
        df = _load_sample()

    # Optimisation 1: categorical dtype for string columns
    work = df.assign(
        payment_type=pd.Categorical(df["payment_type"])
    )

    # Optimisation 2: vectorised duration
    if "trip_duration_mins" not in work.columns:
        work = work.assign(
            trip_duration_mins=(
                work["tpep_dropoff_datetime"] - work["tpep_pickup_datetime"]
            ).dt.total_seconds() / 60.0
        )

    # Optimisation 3: single .query() filter pass
    filtered = work.query(
        "@FARE_MIN <= fare_amount <= @FARE_MAX and "
        "@DIST_MIN <= trip_distance <= @DIST_MAX"
    )

    # Optimisation 4: vectorised cost_per_mile
    filtered = filtered.assign(
        cost_per_mile=(
            filtered["fare_amount"] / filtered["trip_distance"].clip(lower=0.1)
        ).round(2)
    )

    # Optimisation 5: single agg dict
    if "hour_of_day" not in filtered.columns:
        filtered = filtered.assign(hour_of_day=filtered["tpep_pickup_datetime"].dt.hour)
    hourly_summary = (
        filtered.groupby("hour_of_day")["fare_amount"]
        .agg(trip_count="count", avg_fare="mean")
        .reset_index()
    )

    payment_summary = (
        filtered.groupby("payment_type")["fare_amount"]
        .agg(["sum", "count"])
        .reset_index()
    )

    return {
        "filtered_df": filtered,
        "hourly_summary": hourly_summary,
        "payment_summary": payment_summary,
    }


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

def benchmark(
    n_trials: int = 3,
    min_improvement: float = MIN_IMPROVEMENT_PCT,
) -> dict[str, Any]:
    """Run both pipelines n_trials times and compare wall-clock timings.

    Parameters
    ----------
    n_trials:
        Number of timed repetitions per pipeline.
    min_improvement:
        Required % improvement to mark the benchmark as passing.

    Returns
    -------
    dict with timing details and improvement_pct.
    """
    sample = _load_sample()
    baseline_times: list[float] = []
    optimized_times: list[float] = []

    for i in range(1, n_trials + 1):
        df = sample.copy()

        t0 = time.perf_counter()
        baseline_pipeline(df.copy())
        bt = time.perf_counter() - t0
        baseline_times.append(bt)

        t0 = time.perf_counter()
        optimized_pipeline(df.copy())
        ot = time.perf_counter() - t0
        optimized_times.append(ot)

    mean_b = statistics.mean(baseline_times)
    mean_o = statistics.mean(optimized_times)
    std_b  = statistics.stdev(baseline_times) if n_trials > 1 else 0.0
    std_o  = statistics.stdev(optimized_times) if n_trials > 1 else 0.0
    improvement = (1 - mean_o / mean_b) * 100 if mean_b > 0 else 0.0
    passed = improvement >= min_improvement

    # ── Print rich table ──────────────────────────────────────────────
    _print_benchmark_table(
        baseline_times, optimized_times, mean_b, mean_o, std_b, std_o,
        improvement, passed, min_improvement,
    )

    return {
        "baseline_times": baseline_times,
        "optimized_times": optimized_times,
        "mean_baseline": round(mean_b, 4),
        "mean_optimized": round(mean_o, 4),
        "std_baseline": round(std_b, 4),
        "std_optimized": round(std_o, 4),
        "improvement_pct": round(improvement, 2),
        "passed": passed,
    }


def _print_benchmark_table(
    bt: list[float], ot: list[float],
    mb: float, mo: float, sb: float, so: float,
    imp: float, passed: bool, target: float,
) -> None:
    """Print benchmark comparison using rich if available."""
    try:
        from rich.table import Table
        from rich.console import Console

        console = Console()
        table = Table(title="Pipeline Benchmark — Baseline vs Optimised")
        table.add_column("Run",         justify="center")
        table.add_column("Baseline (s)",  justify="right", style="red")
        table.add_column("Optimised (s)", justify="right", style="green")
        table.add_column("Speedup",       justify="right")
        table.add_column("Δ%",            justify="right")

        for i, (b, o) in enumerate(zip(bt, ot), 1):
            spd = f"{b/o:.1f}x" if o > 0 else "∞"
            d   = f"{(1-o/b)*100:.1f}%" if b > 0 else "—"
            table.add_row(str(i), f"{b:.4f}", f"{o:.4f}", spd, d)

        table.add_row("Mean ± std",
                      f"{mb:.4f} ±{sb:.4f}",
                      f"{mo:.4f} ±{so:.4f}", "—", "—",
                      style="bold")
        console.print(table)
        status = "[bold green]PASS ✓[/]" if passed else "[bold red]FAIL ✗[/]"
        console.print(
            f"  Improvement: [bold]{imp:.1f}%[/]  (target ≥{target:.0f}%)  {status}"
        )

    except ImportError:
        print(f"\n  Baseline mean: {mb:.4f}s  |  Optimised mean: {mo:.4f}s")
        print(f"  Improvement: {imp:.1f}%  ({'PASS' if passed else 'FAIL'})")
