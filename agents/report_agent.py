"""
report_agent.py — Report compilation agent.

Writes a timestamped Markdown report to benchmarks/results/ containing:
* Dataset statistics from the real TLC data
* All five SQL analysis results as Markdown tables
* Efficiency gain metrics from the benchmark
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
from config import BENCHMARKS_RESULTS

logger = logging.getLogger(__name__)


def _df_to_md(df: pd.DataFrame, max_rows: int = 15) -> str:
    """Render a DataFrame as a Markdown table string."""
    df = df.head(max_rows)
    cols = " | ".join(str(c) for c in df.columns)
    sep  = " | ".join("---" for _ in df.columns)
    rows = "\n".join(
        " | ".join(str(v) for v in row) for row in df.itertuples(index=False)
    )
    return f"| {cols} |\n| {sep} |\n{rows}"


class ReportAgent:
    """Agent that writes Markdown and JSON reports from analysis results.

    Parameters
    ----------
    output_dir:
        Directory for report files (defaults to benchmarks/results/).
    """

    def __init__(self, output_dir: Path = BENCHMARKS_RESULTS) -> None:
        self.output_dir = output_dir

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Async entry point.

        Parameters
        ----------
        payload:
            Keys expected: ``analysis_results``, ``benchmark``, ``data_stats``.

        Returns
        -------
        dict with report_path, duration_s.
        """
        t0 = time.perf_counter()
        loop = asyncio.get_event_loop()
        report_path = await loop.run_in_executor(None, self._write, payload)
        return {
            "report_path": str(report_path),
            "duration_s": round(time.perf_counter() - t0, 3),
        }

    def _write(self, payload: dict[str, Any]) -> Path:
        """Serialise the full report to disk."""
        ts_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        ts_human = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        analysis: dict[str, Any] = payload.get("analysis_results", {}).get("results", {})
        benchmark: dict[str, Any] = payload.get("benchmark", {})
        data_stats: dict[str, Any] = payload.get("data_stats", {})

        lines: list[str] = [
            "# TLC Yellow Taxi — Analytics Report",
            "",
            f"**Generated:** {ts_human}",
            "",
            "---",
            "",
            "## Dataset Statistics",
            "",
            f"| Metric | Value |",
            f"| --- | --- |",
            f"| Rows analysed | {data_stats.get('rows', 'N/A'):,} |" if isinstance(data_stats.get('rows'), int) else f"| Rows analysed | {data_stats.get('rows', 'N/A')} |",
            f"| Columns | {len(data_stats.get('columns', []))} |",
            f"| Source | {data_stats.get('source', 'N/A')} |",
            "",
        ]

        section_map = {
            "avg_fare_by_hour":        "Average Fare by Hour of Day",
            "top_10_pickup_hours":     "Top 10 Busiest Pickup Hours",
            "avg_duration_by_dow":     "Average Trip Duration by Day of Week",
            "revenue_by_payment_type": "Revenue by Payment Type",
            "fare_anomalies":          "Fare Anomalies (z-score > 3)",
        }
        for key, title in section_map.items():
            df = analysis.get(key)
            if df is not None and not df.empty:
                lines += [f"## {title}", "", _df_to_md(df), ""]

        if benchmark:
            imp = benchmark.get("improvement_pct", 0)
            lines += [
                "## Pipeline Efficiency Benchmark",
                "",
                f"| Metric | Value |",
                f"| --- | --- |",
                f"| Mean baseline time | {benchmark.get('mean_baseline', 0):.4f}s |",
                f"| Mean optimized time | {benchmark.get('mean_optimized', 0):.4f}s |",
                f"| Improvement | **{imp:.1f}%** {'✓' if imp >= 30 else '✗'} |",
                "",
            ]

        self.output_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.output_dir / f"report_{ts_str}.md"
        report_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("ReportAgent: wrote %s", report_path)
        return report_path
