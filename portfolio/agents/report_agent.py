"""
report_agent.py — Report compilation agent.

Responsibilities
----------------
* Accept the structured analysis results from AnalysisAgent.
* Write a human-readable Markdown report.
* Write a machine-readable JSON report.
* Embed efficiency-gain metrics gathered by the Orchestrator.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from config import REPORT_PATH, REPORT_JSON_PATH

logger = logging.getLogger(__name__)


def _df_to_md(df: pd.DataFrame, max_rows: int = 10) -> str:
    """Render a DataFrame as a plain Markdown table string."""
    df = df.head(max_rows)
    headers = " | ".join(str(c) for c in df.columns)
    sep = " | ".join("---" for _ in df.columns)
    rows = "\n".join(
        " | ".join(str(v) for v in row) for row in df.itertuples(index=False)
    )
    return f"| {headers} |\n| {sep} |\n{rows}"


class ReportAgent:
    """Agent that compiles Markdown and JSON reports from analysis results.

    Parameters
    ----------
    report_path:
        Destination for the Markdown report.
    json_path:
        Destination for the JSON report.
    """

    def __init__(
        self,
        report_path: Path = REPORT_PATH,
        json_path: Path = REPORT_JSON_PATH,
    ) -> None:
        self.report_path = report_path
        self.json_path = json_path

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Entry point called by the Orchestrator.

        Parameters
        ----------
        payload:
            Must contain keys:
            * ``analysis``          – result dict from AnalysisAgent
            * ``benchmark``         – dict with baseline/optimized timing
            * ``data_agent_rows``   – int row count from DataAgent

        Returns
        -------
        dict with keys:
            ``report_path``  – path to Markdown report
            ``json_path``    – path to JSON report
            ``duration_s``   – wall-clock seconds
        """
        start = time.perf_counter()
        analysis: dict[str, Any] = payload["analysis"]
        benchmark: dict[str, Any] = payload.get("benchmark", {})
        row_count: int = payload.get("data_agent_rows", 0)

        logger.info("ReportAgent: writing reports")
        await asyncio.get_event_loop().run_in_executor(
            None, self._write_reports, analysis, benchmark, row_count
        )

        duration = time.perf_counter() - start
        logger.info("ReportAgent: done in %.3fs", duration)
        return {
            "report_path": str(self.report_path),
            "json_path": str(self.json_path),
            "duration_s": duration,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _write_reports(
        self,
        analysis: dict[str, Any],
        benchmark: dict[str, Any],
        row_count: int,
    ) -> None:
        """Serialise both report formats to disk."""
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        stats = analysis.get("summary_stats", {})

        # ---- Markdown -------------------------------------------------------
        md_lines: list[str] = [
            "# Analytics Report",
            "",
            f"**Generated:** {ts}",
            f"**Records analysed:** {row_count:,}",
            "",
            "---",
            "",
            "## Summary KPIs",
            "",
            f"| Metric | Value |",
            f"| --- | --- |",
            f"| Total Orders | {int(stats.get('total_orders', 0)):,} |",
            f"| Total Revenue | ${stats.get('total_revenue', 0):,.2f} |",
            f"| Avg Order Value | ${stats.get('avg_order_value', 0):,.2f} |",
            f"| Max Order Value | ${stats.get('max_order_value', 0):,.2f} |",
            f"| Unique Customers | {int(stats.get('unique_customers', 0)):,} |",
            f"| Unique Products | {int(stats.get('unique_products', 0)):,} |",
            "",
        ]

        for section, key in [
            ("Revenue by Region", "revenue_by_region"),
            ("Top Products", "top_products"),
            ("Revenue by Channel", "revenue_by_channel"),
            ("Monthly Revenue Trend (first 12 months)", "monthly_trend"),
            ("Revenue Anomalies (top 20)", "anomalies"),
        ]:
            df = analysis.get(key)
            if df is not None and not df.empty:
                md_lines += [f"## {section}", "", _df_to_md(df), ""]

        if benchmark:
            improvement = benchmark.get("improvement_pct", 0.0)
            md_lines += [
                "## Pipeline Efficiency Benchmark",
                "",
                f"| Run | Baseline (s) | Optimized (s) |",
                f"| --- | --- | --- |",
            ]
            for i, (b, o) in enumerate(
                zip(
                    benchmark.get("baseline_times", []),
                    benchmark.get("optimized_times", []),
                ),
                start=1,
            ):
                md_lines.append(f"| {i} | {b:.4f} | {o:.4f} |")
            md_lines += [
                "",
                f"**Mean baseline:** {benchmark.get('mean_baseline', 0):.4f}s",
                f"**Mean optimized:** {benchmark.get('mean_optimized', 0):.4f}s",
                f"**Improvement: {improvement:.1f}%** "
                f"({'✓ ≥30%' if improvement >= 30 else '✗ <30%'})",
                "",
            ]

        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.report_path.write_text("\n".join(md_lines), encoding="utf-8")
        logger.debug("ReportAgent: Markdown written to %s", self.report_path)

        # ---- JSON -----------------------------------------------------------
        def _serialise(obj: Any) -> Any:
            if isinstance(obj, pd.DataFrame):
                return obj.to_dict(orient="records")
            if isinstance(obj, float):
                return round(obj, 4)
            return obj

        json_payload: dict[str, Any] = {
            "generated_at": ts,
            "row_count": row_count,
            "summary_stats": {
                k: round(v, 4) if isinstance(v, float) else v
                for k, v in stats.items()
            },
            "revenue_by_region": _serialise(analysis.get("revenue_by_region")),
            "top_products": _serialise(analysis.get("top_products")),
            "revenue_by_channel": _serialise(analysis.get("revenue_by_channel")),
            "monthly_trend": _serialise(analysis.get("monthly_trend")),
            "benchmark": benchmark,
        }

        self.json_path.parent.mkdir(parents=True, exist_ok=True)
        self.json_path.write_text(
            json.dumps(json_payload, indent=2, default=str), encoding="utf-8"
        )
        logger.debug("ReportAgent: JSON written to %s", self.json_path)
