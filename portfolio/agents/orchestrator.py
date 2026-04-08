"""
orchestrator.py — Master agent that delegates tasks to specialist sub-agents.

Architecture
------------
The Orchestrator maintains a typed task queue.  Each task is a dict with at
minimum a ``"type"`` key that maps to one of the three sub-agents:

    "data"     → DataAgent
    "analysis" → AnalysisAgent
    "report"   → ReportAgent

Tasks that have no inter-dependencies are dispatched concurrently via
``asyncio.gather``; tasks that require the output of a prior task are
chained sequentially inside ``run_pipeline()``.

The Orchestrator logs per-agent wall-clock time so that the benchmark can
verify the efficiency gain end-to-end.

Multi-agent pattern
-------------------
Each agent is a stateless object with a single ``async run(payload)``
coroutine.  The Orchestrator wires them together without knowing their
internals, making it trivial to swap, test, or scale any agent independently.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from agents.data_agent import DataAgent
from agents.analysis_agent import AnalysisAgent
from agents.report_agent import ReportAgent

logger = logging.getLogger(__name__)


class Orchestrator:
    """Master coordinator for the multi-agent analytics pipeline.

    Parameters
    ----------
    data_agent:
        Instance of DataAgent (injected for testability).
    analysis_agent:
        Instance of AnalysisAgent.
    report_agent:
        Instance of ReportAgent.
    """

    def __init__(
        self,
        data_agent: DataAgent | None = None,
        analysis_agent: AnalysisAgent | None = None,
        report_agent: ReportAgent | None = None,
    ) -> None:
        self.data_agent = data_agent or DataAgent()
        self.analysis_agent = analysis_agent or AnalysisAgent()
        self.report_agent = report_agent or ReportAgent()

        # Timing records: agent_name → seconds
        self.timing: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run_pipeline(
        self, benchmark: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Execute the full analytics pipeline end-to-end.

        Stages
        ------
        1. DataAgent  — ingest & clean (independent; runs first)
        2. AnalysisAgent — SQL analytics (depends on DataAgent output)
        3. ReportAgent — write reports (depends on both prior stages)

        Stages 1–2 could be parallelised when using multiple data sources;
        here they are chained because AnalysisAgent needs the DataFrame.

        Parameters
        ----------
        benchmark:
            Optional dict of benchmark timing results to embed in the report.

        Returns
        -------
        dict summarising outputs and per-agent timing.
        """
        pipeline_start = time.perf_counter()
        logger.info("Orchestrator: pipeline start")

        # Stage 1: data ingestion
        data_result = await self._dispatch("data", self.data_agent, {})

        # Stage 2: analysis (depends on stage 1)
        analysis_result = await self._dispatch(
            "analysis",
            self.analysis_agent,
            {"dataframe": data_result["dataframe"]},
        )

        # Stage 3: report (depends on stages 1 & 2)
        report_result = await self._dispatch(
            "report",
            self.report_agent,
            {
                "analysis": analysis_result,
                "benchmark": benchmark or {},
                "data_agent_rows": data_result["rows"],
            },
        )

        total = time.perf_counter() - pipeline_start
        self.timing["total_pipeline"] = total

        self._print_timing_table()

        return {
            "data": data_result,
            "analysis": analysis_result,
            "report": report_result,
            "timing": self.timing,
        }

    async def run_concurrent_demo(self) -> list[dict[str, Any]]:
        """Demonstrate concurrent dispatch by running three independent
        DataAgent tasks in parallel — useful for showcasing asyncio.gather.

        Returns
        -------
        List of results from three concurrent DataAgent runs.
        """
        logger.info("Orchestrator: running concurrent DataAgent demo")
        tasks = [
            self.data_agent.run({"force_regenerate": False})
            for _ in range(3)
        ]
        results = await asyncio.gather(*tasks)
        return list(results)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _dispatch(
        self,
        name: str,
        agent: DataAgent | AnalysisAgent | ReportAgent,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Run a single agent, record its timing, and return its result.

        Parameters
        ----------
        name:
            Logical name for logging / timing dict key.
        agent:
            The agent instance to run.
        payload:
            Input payload forwarded to ``agent.run()``.
        """
        logger.info("Orchestrator: dispatching → %s", name)
        t0 = time.perf_counter()
        result = await agent.run(payload)
        elapsed = time.perf_counter() - t0
        self.timing[name] = elapsed
        logger.info("Orchestrator: ← %s completed in %.3fs", name, elapsed)
        return result

    def _print_timing_table(self) -> None:
        """Pretty-print a per-agent timing breakdown to stdout."""
        print("\n" + "=" * 50)
        print("  ORCHESTRATOR TIMING BREAKDOWN")
        print("=" * 50)
        for name, seconds in self.timing.items():
            bar = "#" * min(int(seconds * 20), 40)
            print(f"  {name:<22} {seconds:7.3f}s  {bar}")
        print("=" * 50)
