"""
demo.py — End-to-end demonstration of the AI-Augmented Analytics Portfolio.

Run with:
    cd portfolio/
    python demo.py

Every module is exercised in sequence.  R and Java integrations produce
informative messages rather than crashing if those runtimes are absent.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

# Ensure the portfolio directory is on sys.path when running as a script
_HERE = Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s  %(name)s — %(message)s",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _section(title: str) -> None:
    """Print a prominent section header."""
    width = 66
    print("\n" + "╔" + "═" * (width - 2) + "╗")
    print("║  " + title.ljust(width - 4) + "║")
    print("╚" + "═" * (width - 2) + "╝")


# ---------------------------------------------------------------------------
# Section 1 — Multi-agent orchestrator
# ---------------------------------------------------------------------------

async def _run_orchestrator() -> dict:
    """Run the full multi-agent pipeline and return the benchmark-ready result."""
    from agents.orchestrator import Orchestrator
    from benchmarks.benchmark_runner import run_and_save

    _section("1 / 6  —  MULTI-AGENT ORCHESTRATOR")
    print("  Running DataAgent → AnalysisAgent → ReportAgent …\n")

    # Run benchmark first so we can embed it in the report
    benchmark = run_and_save(n_runs=3)

    orch = Orchestrator()
    pipeline_result = await orch.run_pipeline(benchmark=benchmark)

    rpt = pipeline_result["report"]
    print(f"\n  Markdown report : {rpt['report_path']}")
    print(f"  JSON report     : {rpt['json_path']}")
    return pipeline_result


# ---------------------------------------------------------------------------
# Section 2 — SQL query library
# ---------------------------------------------------------------------------

def _run_sql_analytics(df) -> None:
    """Demonstrate the SQLQueryLibrary on the DataAgent's DataFrame."""
    from analytics.sql_queries import SQLQueryLibrary

    _section("2 / 6  —  SQL QUERY LIBRARY")
    with SQLQueryLibrary.from_dataframe(df) as lib:

        print("\n  ── Total Revenue ──────────────────────────")
        print(lib.total_revenue().to_string(index=False))

        print("\n  ── Top 5 Customers ────────────────────────")
        print(lib.top_n_customers(n=5).to_string(index=False))

        print("\n  ── Monthly Growth Rate (first 6 months) ───")
        mgr = lib.monthly_growth_rate()
        print(mgr.head(6).to_string(index=False))

        print("\n  ── Cohort Retention (first 6 cohorts) ─────")
        cr = lib.cohort_retention()
        print(cr.head(6).to_string(index=False))

        print("\n  ── Revenue Anomalies (top 5) ───────────────")
        anom = lib.revenue_anomalies(std_multiplier=2.0)
        print(anom[["order_id", "region", "product", "revenue"]].head(5).to_string(index=False))

        print("\n  ── Revenue by Product × Region (top 5) ────")
        prx = lib.revenue_by_product_region()
        print(prx.head(5).to_string(index=False))


# ---------------------------------------------------------------------------
# Section 3 — R bridge
# ---------------------------------------------------------------------------

def _run_r_bridge(df) -> None:
    """Run linear regression via rpy2 / NumPy fallback."""
    from analytics.r_bridge import run_r_analysis

    _section("3 / 6  —  R BRIDGE  (rpy2 / NumPy fallback)")
    result = run_r_analysis(df)
    print(f"\n  Method    : {result['method']}")
    print(f"  Intercept : {result['intercept']:.4f}")
    print(f"  Slope     : {result['slope']:.4f}")
    print(f"  R²        : {result['r_squared']:.4f}")


# ---------------------------------------------------------------------------
# Section 4 — A* pathfinding
# ---------------------------------------------------------------------------

def _run_pathfinding() -> None:
    """Solve and visualise a 20×20 maze with A*."""
    from pathfinding.visualizer import demo_maze
    from config import DATA_DIR

    _section("4 / 6  —  A* PATHFINDING  (20×20 maze)")
    save_png = DATA_DIR / "astar_maze.png"
    result = demo_maze(save_png=save_png)

    if result["path"]:
        print(f"\n  Path cost   : {result['cost']:.1f}")
        print(f"  Path length : {len(result['path'])} steps")
        print(f"  Nodes visited: {len(result['visited'])}")
    else:
        print("\n  No path found (try reducing wall probability in config.py)")


# ---------------------------------------------------------------------------
# Section 5 — Web scraper + multimodal dataset
# ---------------------------------------------------------------------------

async def _run_scraper() -> None:
    """Scrape quotes.toscrape.com and persist a multimodal JSONL dataset."""
    from scraper.scraper import AsyncScraper
    from scraper.multimodal_parser import MultimodalParser
    from scraper.dataset_builder import DatasetBuilder
    from config import SCRAPER_BASE_URL, MULTIMODAL_DATASET_PATH

    _section("5 / 6  —  ASYNC WEB SCRAPER  (quotes.toscrape.com)")
    print("  Scraping up to 3 pages …  (respects 0.5 s rate limit)\n")

    scraper = AsyncScraper(max_pages=3)
    pages = await scraper.scrape()

    parser = MultimodalParser(base_url=SCRAPER_BASE_URL)
    records = [parser.parse(p) for p in pages if not p.get("error")]

    # Clear previous dataset to avoid duplicates across demo runs
    if MULTIMODAL_DATASET_PATH.exists():
        MULTIMODAL_DATASET_PATH.unlink()

    builder = DatasetBuilder(append=False)
    written = builder.save(records)

    print(f"  Pages fetched   : {len(pages)}")
    print(f"  Records parsed  : {len(records)}")
    print(f"  Dataset written : {MULTIMODAL_DATASET_PATH}  ({written} records)")

    for i, rec in enumerate(records, 1):
        words = rec.get("metadata", {}).get("word_count", "?")
        imgs  = len(rec.get("images", []))
        print(f"    Page {i}: {rec['url']}  |  {words} words  |  {imgs} images")


# ---------------------------------------------------------------------------
# Section 6 — Java interop
# ---------------------------------------------------------------------------

def _run_java_bridge() -> None:
    """Compile DataProcessor.java and normalise a sample array."""
    from java_interop.java_bridge import JavaBridge, JavaNotFoundError

    _section("6 / 6  —  JAVA INTEROP  (batch normalisation)")
    data = [10.0, 20.0, 30.0, 40.0, 50.0, 15.0, 25.0, 35.0]

    try:
        bridge = JavaBridge()
        result = bridge.batch_normalize(data)

        print(f"\n  Input  : {data}")
        print(f"  Mean   : {result['mean']}")
        print(f"  StdDev : {result['std_dev']}")
        print(f"  Min    : {result['min']}")
        print(f"  Max    : {result['max']}")
        normed = [round(v, 4) for v in result["normalized"]]
        print(f"  Z-scores: {normed}")

    except JavaNotFoundError as exc:
        print(f"\n  [SKIPPED]  {exc}")
    except Exception as exc:  # noqa: BLE001
        print(f"\n  [ERROR]  Java bridge failed: {exc}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Orchestrate the full end-to-end demo."""
    print("\n" + "█" * 66)
    print("  AI-AUGMENTED ANALYTICS PORTFOLIO  —  End-to-End Demo")
    print("█" * 66)

    # ── Section 1: orchestrator + benchmark ─────────────────────────────────
    pipeline_result = await _run_orchestrator()
    df = pipeline_result["data"]["dataframe"]

    # ── Section 2: SQL analytics ─────────────────────────────────────────────
    _run_sql_analytics(df)

    # ── Section 3: R bridge ──────────────────────────────────────────────────
    _run_r_bridge(df)

    # ── Section 4: A* pathfinding ────────────────────────────────────────────
    _run_pathfinding()

    # ── Section 5: web scraper ───────────────────────────────────────────────
    await _run_scraper()

    # ── Section 6: Java interop ──────────────────────────────────────────────
    _run_java_bridge()

    # ── Done ─────────────────────────────────────────────────────────────────
    print("\n" + "█" * 66)
    print("  DEMO COMPLETE")
    print("█" * 66 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
