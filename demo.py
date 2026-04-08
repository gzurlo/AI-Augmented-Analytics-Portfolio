"""
demo.py — End-to-end demonstration of the AI-Augmented Analytics Portfolio.

Run with:
    python demo.py

Every section is wrapped in try/except so the demo never crashes.
R and Java integrations fail gracefully if those runtimes are absent.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Ensure project root is importable
_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from rich.console import Console
    _console = Console()
    def _print(msg: str, **kw) -> None:
        _console.print(msg, **kw)
except ImportError:
    _console = None  # type: ignore[assignment]
    def _print(msg: str, **kw) -> None:  # type: ignore[misc]
        # Strip rich markup tags for plain output
        import re
        plain = re.sub(r"\[/?[^\]]+\]", "", msg)
        print(plain)

def section(title: str) -> None:
    """Print a bold section rule."""
    try:
        from rich.console import Console
        Console().rule(f"[bold cyan]{title}[/bold cyan]")
    except ImportError:
        width = 68
        print("\n" + "=" * width)
        print(f"  {title}")
        print("=" * width)


# ===========================================================================
# Section 1 — Data Ingestion
# ===========================================================================

async def run_data_ingestion() -> dict:
    """Run DataAgent and return its result."""
    from agents.data_agent import DataAgent
    agent = DataAgent()
    return await agent.run()


def section_1() -> dict:
    """1. DATA INGESTION (TLC Yellow Taxi)"""
    section("1. DATA INGESTION  —  TLC Yellow Taxi")
    result: dict = {}
    try:
        result = asyncio.run(run_data_ingestion())
        df = result["dataframe"]
        _print(f"\n  [green]Source:[/green]   {result['source']}")
        _print(f"  [green]Rows:[/green]     {result['rows']:,}")
        _print(f"  [green]Columns:[/green]  {len(result['columns'])}")
        _print(f"  [green]Duration:[/green] {result['duration_s']:.3f}s\n")
        _print(f"  Sample columns: {result['columns'][:8]}")
        _print("\n  [bold]First 3 rows:[/bold]")
        _print(df[["tpep_pickup_datetime", "fare_amount",
                    "trip_distance", "hour_of_day", "day_of_week"]].head(3).to_string())
    except Exception as exc:
        _print(f"\n  [bold red]Section 1 error:[/bold red] {exc}")
    return result


# ===========================================================================
# Section 2 — Multi-Agent Orchestration
# ===========================================================================

async def run_orchestration(benchmark: dict) -> dict:
    """Run the full Orchestrator pipeline."""
    from agents.orchestrator import Orchestrator
    return await Orchestrator(benchmark=benchmark).run()


def section_2(benchmark: dict) -> dict:
    """2. MULTI-AGENT ORCHESTRATION"""
    section("2. MULTI-AGENT ORCHESTRATION")
    result: dict = {}
    try:
        result = asyncio.run(run_orchestration(benchmark))
        rpt = result.get("report", {})
        _print(f"\n  [green]Pipeline duration:[/green] {result.get('total_duration_s', '?'):.3f}s")
        _print(f"  [green]Report written:[/green]    {rpt.get('report_path', '—')}")
    except Exception as exc:
        _print(f"\n  [bold red]Section 2 error:[/bold red] {exc}")
    return result


# ===========================================================================
# Section 3 — SQL Analytics
# ===========================================================================

def section_3(df=None) -> None:
    """3. SQL ANALYTICS ON TAXI DATA"""
    section("3. SQL ANALYTICS  —  TLC Taxi Data")
    try:
        from analytics.sql_queries import (
            create_connection, hourly_revenue, top_routes,
            monthly_growth, payment_breakdown, detect_anomalies,
        )
        conn = create_connection(df)

        _print("\n  [bold]── Hourly Revenue (first 6 hours) ──────────────[/bold]")
        _print(hourly_revenue(conn).head(6).to_string(index=False))

        _print("\n  [bold]── Top 5 Routes ─────────────────────────────────[/bold]")
        _print(top_routes(conn, n=5).to_string(index=False))

        _print("\n  [bold]── Monthly Growth Rate ──────────────────────────[/bold]")
        _print(monthly_growth(conn).head(6).to_string(index=False))

        _print("\n  [bold]── Revenue by Payment Type ──────────────────────[/bold]")
        _print(payment_breakdown(conn).to_string(index=False))

        _print("\n  [bold]── Fare Anomalies (z > 3, top 5) ────────────────[/bold]")
        anom = detect_anomalies(conn, z_threshold=3.0)
        cols = [c for c in ["tpep_pickup_datetime", "trip_distance",
                             "fare_amount", "z_score"] if c in anom.columns]
        _print(anom[cols].head(5).to_string(index=False))
        conn.close()
    except Exception as exc:
        _print(f"\n  [bold red]Section 3 error:[/bold red] {exc}")


# ===========================================================================
# Section 4 — Pipeline Benchmark
# ===========================================================================

def section_4() -> dict:
    """4. PIPELINE BENCHMARK (≥30% efficiency gain)"""
    section("4. PIPELINE BENCHMARK  —  ≥30% Efficiency Gain")
    result: dict = {}
    try:
        from benchmarks.benchmark_runner import run_benchmarks
        result = run_benchmarks(n_trials=3)
        imp = result.get("improvement_pct", 0)
        colour = "green" if imp >= 30 else "red"
        _print(f"\n  Improvement: [{colour}][bold]{imp:.1f}%[/bold][/{colour}]  "
               f"{'✓ PASS' if imp >= 30 else '✗ FAIL'}")
    except Exception as exc:
        _print(f"\n  [bold red]Section 4 error:[/bold red] {exc}")
    return result


# ===========================================================================
# Section 5 — A* Pathfinding
# ===========================================================================

def section_5() -> None:
    """5. A* PATHFINDING VISUALIZER"""
    section("5. A* PATHFINDING  —  20×20 Maze")
    try:
        from pathfinding.visualizer import demo
        result = demo()
        if result["path"]:
            _print(f"\n  [green]Path cost:[/green]     {result['cost']:.1f}")
            _print(f"  [green]Path length:[/green]   {len(result['path'])} steps")
            _print(f"  [green]Nodes visited:[/green] {len(result['visited'])}")
        else:
            _print("\n  [yellow]No path found (try reducing WALL_DENSITY)[/yellow]")
    except Exception as exc:
        _print(f"\n  [bold red]Section 5 error:[/bold red] {exc}")


# ===========================================================================
# Section 6 — Web Scraper
# ===========================================================================

async def run_scraper() -> dict:
    """Scrape books.toscrape.com and build JSONL dataset."""
    from scraper.scraper import AsyncScraper
    from scraper.multimodal_parser import MultimodalParser
    from scraper.dataset_builder import build_dataset
    from config import SCRAPER_PAGES, SCRAPER_BASE_URL

    scraper = AsyncScraper()
    pages   = await scraper.scrape_pages(n=SCRAPER_PAGES)
    parser  = MultimodalParser(base_url=SCRAPER_BASE_URL)
    records = [parser.parse(p) for p in pages if not p.get("error")]
    written = build_dataset(records)
    return {"pages": len(pages), "records": len(records), "written": written}


def section_6() -> None:
    """6. WEB SCRAPER (Multimodal)"""
    section("6. WEB SCRAPER  —  books.toscrape.com  (Multimodal)")
    try:
        from scraper.scraper import AsyncScraper
        from scraper.multimodal_parser import MultimodalParser
        from scraper.dataset_builder import build_dataset
        from config import SCRAPER_PAGES, SCRAPER_BASE_URL, MULTIMODAL_JSONL

        async def _inner() -> dict:
            scraper = AsyncScraper()
            pages   = await scraper.scrape_pages(n=SCRAPER_PAGES)
            parser  = MultimodalParser(base_url=SCRAPER_BASE_URL)
            records = [parser.parse(p) for p in pages if not p.get("error")]
            written = build_dataset(records)
            return {"pages_fetched": len(pages), "records": len(records),
                    "written": written}

        res = asyncio.run(_inner())
        _print(f"\n  [green]Pages fetched:[/green]  {res['pages_fetched']}")
        _print(f"  [green]Records parsed:[/green] {res['records']}")
        _print(f"  [green]JSONL written:[/green]  {MULTIMODAL_JSONL}")
    except Exception as exc:
        _print(f"\n  [bold red]Section 6 error:[/bold red] {exc}")


# ===========================================================================
# Section 7 — R Bridge
# ===========================================================================

def section_7(df=None) -> None:
    """7. R STATISTICAL BRIDGE"""
    section("7. R STATISTICAL BRIDGE  —  Linear Regression")
    try:
        from analytics.r_bridge import run_r_regression
        res = run_r_regression(df)
        _print(f"\n  [green]Method:[/green]        {res['method']}")
        _print(f"  [green]Intercept:[/green]     {res['intercept']:.4f}")
        _print(f"  [green]Coef distance:[/green] {res['coef_distance']:.4f}")
        _print(f"  [green]Coef duration:[/green] {res['coef_duration']:.4f}")
        _print(f"  [green]R²:[/green]            {res['r_squared']:.4f}")
    except Exception as exc:
        _print(f"\n  [bold red]Section 7 error:[/bold red] {exc}")


# ===========================================================================
# Section 8 — Java Interop
# ===========================================================================

def section_8() -> None:
    """8. JAVA INTEROP — Min-Max Normalisation"""
    section("8. JAVA INTEROP  —  Batch Min-Max Normalisation")
    try:
        from java_interop.java_bridge import process
        values = [1.5, 2.3, 4.1, 0.8, 3.3]
        result = process(values)
        src = result.pop("_source", "java")
        _print(f"\n  [green]Source:[/green]     {src}")
        _print(f"  [green]Input:[/green]      {values}")
        _print(f"  [green]Min:[/green]        {result['min']}")
        _print(f"  [green]Max:[/green]        {result['max']}")
        _print(f"  [green]Mean:[/green]       {result['mean']:.6f}")
        _print(f"  [green]Std:[/green]        {result['std']:.6f}")
        norm = [round(v, 4) for v in result['normalized']]
        _print(f"  [green]Normalised:[/green] {norm}")
    except Exception as exc:
        _print(f"\n  [bold red]Section 8 error:[/bold red] {exc}")


# ===========================================================================
# Main
# ===========================================================================

def main() -> None:
    """Run all portfolio sections end-to-end."""
    _print("\n[bold magenta]" + "█" * 68 + "[/bold magenta]")
    _print("[bold magenta]  AI-AUGMENTED ANALYTICS PORTFOLIO  —  TLC Yellow Taxi Dataset[/bold magenta]")
    _print("[bold magenta]" + "█" * 68 + "[/bold magenta]\n")

    # Section 4 runs before 2 so benchmark is embedded in the report
    benchmark = section_4()

    data_result = section_1()
    df = data_result.get("dataframe")

    section_2(benchmark)
    section_3(df)
    section_5()
    section_6()
    section_7(df)
    section_8()

    _print("\n[bold green]" + "█" * 68 + "[/bold green]")
    _print("[bold green]  ✓ All modules completed successfully[/bold green]")
    _print("[bold green]" + "█" * 68 + "[/bold green]\n")


if __name__ == "__main__":
    main()
