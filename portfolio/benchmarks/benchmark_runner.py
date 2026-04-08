"""
benchmark_runner.py — Run baseline vs optimised pipeline benchmarks and save
JSON results.

Usage
-----
::

    python -m benchmarks.benchmark_runner           # runs from portfolio/
    # or
    from benchmarks.benchmark_runner import run_and_save
    result = run_and_save()
"""

from __future__ import annotations

import json
import logging
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analytics.pipeline import baseline_pipeline, optimized_pipeline, _make_dataframe
from config import BENCHMARKS_DIR, BENCHMARK_RUNS, BENCHMARK_MIN_IMPROVEMENT_PCT

logger = logging.getLogger(__name__)


def run_and_save(
    n_runs: int = BENCHMARK_RUNS,
    min_improvement_pct: float = BENCHMARK_MIN_IMPROVEMENT_PCT,
) -> dict[str, Any]:
    """Time both pipeline variants, print a comparison table, and save JSON.

    Parameters
    ----------
    n_runs:
        Number of timed trials per variant.
    min_improvement_pct:
        Required improvement percentage to mark the benchmark as passed.

    Returns
    -------
    Result dict (same structure as saved JSON).
    """
    df_template = _make_dataframe()
    baseline_times: list[float] = []
    optimized_times: list[float] = []

    print("\n" + "=" * 66)
    print("  BENCHMARK RUNNER  —  Baseline vs Optimised Pipeline")
    print("=" * 66)
    print(f"  {'Run':<6} {'Baseline (s)':>14} {'Optimized (s)':>14} {'Speedup':>10} {'Δ%':>8}")
    print("  " + "-" * 56)

    for i in range(1, n_runs + 1):
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
        delta_pct = (1 - ot / bt) * 100 if bt > 0 else 0.0
        print(
            f"  {i:<6} {bt:>14.4f} {ot:>14.4f} {speedup:>9.2f}x {delta_pct:>7.1f}%"
        )

    mean_b = statistics.mean(baseline_times)
    mean_o = statistics.mean(optimized_times)
    std_b = statistics.stdev(baseline_times) if n_runs > 1 else 0.0
    std_o = statistics.stdev(optimized_times) if n_runs > 1 else 0.0
    improvement_pct = (1 - mean_o / mean_b) * 100 if mean_b > 0 else 0.0
    mean_speedup = mean_b / mean_o if mean_o > 0 else float("inf")
    passed = improvement_pct >= min_improvement_pct

    print("  " + "-" * 56)
    print(f"  {'Mean':<6} {mean_b:>14.4f} {mean_o:>14.4f}")
    print(f"  {'Std':<6} {std_b:>14.4f} {std_o:>14.4f}")
    print()
    print(f"  Mean speedup : {mean_speedup:.2f}x")
    print(f"  Improvement  : {improvement_pct:.1f}%  (target ≥{min_improvement_pct:.0f}%)")
    print(f"  Result       : {'PASS ✓' if passed else 'FAIL ✗'}")
    print("=" * 66)

    result: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "n_runs": n_runs,
        "baseline_times": [round(t, 6) for t in baseline_times],
        "optimized_times": [round(t, 6) for t in optimized_times],
        "mean_baseline": round(mean_b, 6),
        "mean_optimized": round(mean_o, 6),
        "std_baseline": round(std_b, 6),
        "std_optimized": round(std_o, 6),
        "improvement_pct": round(improvement_pct, 2),
        "mean_speedup": round(mean_speedup, 4),
        "target_improvement_pct": min_improvement_pct,
        "passed": passed,
    }

    _save_result(result)
    return result


def _save_result(result: dict[str, Any]) -> Path:
    """Persist benchmark results as a timestamped JSON file."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    BENCHMARKS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = BENCHMARKS_DIR / f"benchmark_{ts}.json"
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    logger.info("Benchmark result saved to %s", out_path)
    print(f"\n  Results saved → {out_path}")
    return out_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    run_and_save()
