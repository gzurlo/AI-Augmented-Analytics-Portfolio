"""
benchmark_runner.py — Time baseline vs optimised pipelines and save JSON results.

Usage
-----
    python benchmarks/benchmark_runner.py
    # or
    from benchmarks.benchmark_runner import run_benchmarks
    result = run_benchmarks()
"""

from __future__ import annotations

import json
import logging
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from analytics.pipeline import baseline_pipeline, optimized_pipeline, _load_sample
from config import BENCHMARKS_RESULTS, N_BENCHMARK_TRIALS, MIN_IMPROVEMENT_PCT

logger = logging.getLogger(__name__)


def run_benchmarks(
    n_trials: int = N_BENCHMARK_TRIALS,
    min_improvement: float = MIN_IMPROVEMENT_PCT,
) -> dict[str, Any]:
    """Time both pipeline variants, print a rich comparison table, and save JSON.

    Parameters
    ----------
    n_trials:
        Number of timed repetitions per pipeline.
    min_improvement:
        Required improvement % to mark as passing.

    Returns
    -------
    dict with timing details and improvement_pct.
    """
    sample = _load_sample()
    baseline_times:  list[float] = []
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
    speedup = mean_b / mean_o if mean_o > 0 else float("inf")
    passed  = improvement >= min_improvement

    _print_table(
        baseline_times, optimized_times, mean_b, mean_o,
        std_b, std_o, improvement, speedup, passed, min_improvement,
    )

    result: dict[str, Any] = {
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "n_trials":          n_trials,
        "baseline_times":    [round(t, 6) for t in baseline_times],
        "optimized_times":   [round(t, 6) for t in optimized_times],
        "mean_baseline":     round(mean_b, 4),
        "mean_optimized":    round(mean_o, 4),
        "std_baseline":      round(std_b, 4),
        "std_optimized":     round(std_o, 4),
        "improvement_pct":   round(improvement, 2),
        "mean_speedup":      round(speedup, 2),
        "target_pct":        min_improvement,
        "passed":            passed,
    }

    _save(result)
    return result


def _print_table(
    bt: list[float], ot: list[float],
    mb: float, mo: float, sb: float, so: float,
    improvement: float, speedup: float,
    passed: bool, target: float,
) -> None:
    """Print a rich Table; fall back to plain text if rich is absent."""
    try:
        from rich.table import Table
        from rich.console import Console

        console = Console()
        t = Table(title="Benchmark — Baseline vs Optimised Pipeline")
        t.add_column("Trial", justify="center")
        t.add_column("Baseline (s)",  justify="right", style="red")
        t.add_column("Optimised (s)", justify="right", style="green")
        t.add_column("Speedup",       justify="right")
        t.add_column("Improvement",   justify="right")

        for i, (b, o) in enumerate(zip(bt, ot), 1):
            spd = f"{b/o:.1f}x" if o > 0 else "∞"
            d   = f"{(1-o/b)*100:.1f}%" if b > 0 else "—"
            t.add_row(str(i), f"{b:.4f}", f"{o:.4f}", spd, d)

        t.add_row(
            "Mean ± std",
            f"{mb:.4f} ±{sb:.4f}",
            f"{mo:.4f} ±{so:.4f}",
            f"{speedup:.1f}x",
            f"[bold]{improvement:.1f}%[/bold]",
            style="bold",
        )
        console.print(t)
        status = "[bold green]PASS ✓[/]" if passed else "[bold red]FAIL ✗[/]"
        console.print(
            f"  Improvement: [bold]{improvement:.1f}%[/]  "
            f"(target ≥{target:.0f}%)  {status}"
        )

    except ImportError:
        print(f"\n  Baseline mean : {mb:.4f}s ±{sb:.4f}")
        print(f"  Optimised mean: {mo:.4f}s ±{so:.4f}")
        print(f"  Improvement   : {improvement:.1f}%  (target ≥{target:.0f}%)")
        print(f"  Result        : {'PASS' if passed else 'FAIL'}")


def _save(result: dict[str, Any]) -> Path:
    """Persist benchmark JSON to benchmarks/results/."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    BENCHMARKS_RESULTS.mkdir(parents=True, exist_ok=True)
    path = BENCHMARKS_RESULTS / f"benchmark_{ts}.json"
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"  Benchmark result saved → {path}")
    return path


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    run_benchmarks()
