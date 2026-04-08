"""
orchestrator.py — Async master orchestrator with task queue and rich timing table.

Pattern
-------
Maintains a sequential task queue ["ingest", "analyze", "report"] where each
stage's output feeds the next.  Times every stage and renders a summary table
with the ``rich`` library.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from agents.data_agent import DataAgent
from agents.analysis_agent import AnalysisAgent
from agents.report_agent import ReportAgent

logger = logging.getLogger(__name__)


class Orchestrator:
    """Master coordinator for the TLC taxi analytics pipeline.

    Runs three agents in sequence (data → analysis → report), collects
    per-agent timings, and prints a summary table via ``rich``.

    Parameters
    ----------
    benchmark:
        Optional benchmark result dict to embed in the report.
    """

    def __init__(self, benchmark: dict[str, Any] | None = None) -> None:
        self.benchmark = benchmark or {}
        self._timings: dict[str, float] = {}
        self._results: dict[str, Any] = {}

    async def run(self) -> dict[str, Any]:
        """Execute the full pipeline end-to-end.

        Returns
        -------
        dict with keys: data, analysis, report, timings, total_duration_s.
        """
        pipeline_start = time.perf_counter()

        # Stage 1 — data ingestion
        data_result = await self._timed("ingest", DataAgent().run())

        # Stage 2 — analysis (depends on stage 1 DataFrame)
        analysis_agent = AnalysisAgent(df=data_result.get("dataframe"))
        analysis_result = await self._timed("analyze", analysis_agent.run())

        # Stage 3 — report (depends on stages 1 & 2)
        report_payload = {
            "analysis_results": analysis_result,
            "benchmark": self.benchmark,
            "data_stats": {
                "rows": data_result.get("rows"),
                "columns": data_result.get("columns"),
                "source": data_result.get("source"),
            },
        }
        report_result = await self._timed("report", ReportAgent().run(report_payload))

        total = round(time.perf_counter() - pipeline_start, 3)
        self._timings["total"] = total

        self._print_timing_table()

        return {
            "data": data_result,
            "analysis": analysis_result,
            "report": report_result,
            "timings": self._timings,
            "total_duration_s": total,
        }

    async def _timed(self, name: str, coro) -> Any:
        """Await a coroutine, record its wall-clock time."""
        t0 = time.perf_counter()
        result = await coro
        self._timings[name] = round(time.perf_counter() - t0, 3)
        return result

    def _print_timing_table(self) -> None:
        """Print a per-agent timing table using rich if available."""
        try:
            from rich.table import Table
            from rich.console import Console

            console = Console()
            table = Table(title="Orchestrator — Agent Timing", show_header=True)
            table.add_column("Stage", style="cyan")
            table.add_column("Duration (s)", justify="right", style="green")
            table.add_column("Share", justify="right")

            total = self._timings.get("total", 1)
            for stage, t in self._timings.items():
                if stage == "total":
                    continue
                pct = f"{t / total * 100:.1f}%"
                table.add_row(stage, f"{t:.3f}", pct)
            table.add_row("─── total ───", f"{total:.3f}", "100%", style="bold")
            console.print(table)

        except ImportError:
            print("\n  ORCHESTRATOR TIMING")
            print("  " + "-" * 30)
            for stage, t in self._timings.items():
                print(f"  {stage:<12}  {t:.3f}s")
            print("  " + "-" * 30)
