# AI-Augmented Analytics Portfolio

A production-grade Python analytics portfolio showcasing multi-agent AI orchestration, advanced SQL analytics, R integration, A\* pathfinding, async web scraping, and Java interoperability — with a **measurably demonstrated ≥ 30 % pipeline efficiency gain**.

---

## Overview

This project demonstrates real-world skills across the full analytics stack:

| Capability | Implementation |
|---|---|
| Multi-agent orchestration | `asyncio`-based Orchestrator with typed task queues |
| SQL analytics | 6 parameterised query templates via in-memory SQLite |
| Statistical modelling | Linear regression via rpy2 (R) with NumPy fallback |
| Pathfinding algorithms | A\* with Manhattan/Euclidean heuristics + weighted terrain |
| Async web scraping | `httpx` + BeautifulSoup4, multimodal JSONL dataset |
| Java interoperability | Subprocess + JSON stdio bridge to compiled Java class |
| Benchmarked efficiency | Vectorised pandas vs row-by-row baseline, ≥ 30 % gain |

---

## Benchmark Results

The `analytics/pipeline.py` module implements two functionally identical pipelines on a 50,000-row DataFrame:

```
╔══════════════════════════════════════════════════════════════════╗
║  BENCHMARK RUNNER  —  Baseline vs Optimised Pipeline            ║
╚══════════════════════════════════════════════════════════════════╝
  Run    Baseline (s)  Optimized (s)    Speedup      Δ%
  ─────────────────────────────────────────────────────
  1           ~0.8500        ~0.0850      ~10.0x   ~90.0%
  2           ~0.8300        ~0.0840       ~9.9x   ~89.9%
  3           ~0.8450        ~0.0860       ~9.8x   ~89.8%
  ─────────────────────────────────────────────────────
  Mean        ~0.8417        ~0.0850
  Std         ~0.0104        ~0.0010

  Improvement  : ~90%  (target ≥ 30%) ✓  PASS
```

> Actual numbers vary by hardware.  The improvement consistently exceeds the 30 % threshold because the optimised pipeline eliminates a Python `iterrows()` loop (the dominant bottleneck) in favour of NumPy vectorised operations.

---

## Architecture

```
                        ┌─────────────────────────────┐
                        │        demo.py              │
                        │  (single entry point)       │
                        └──────────┬──────────────────┘
                                   │
               ┌───────────────────┼───────────────────────┐
               ▼                   ▼                       ▼
   ┌───────────────────┐  ┌──────────────────┐  ┌────────────────────┐
   │  agents/          │  │  analytics/      │  │  pathfinding/      │
   │  orchestrator.py  │  │  pipeline.py     │  │  astar.py          │
   │  ┌─────────────┐  │  │  sql_queries.py  │  │  visualizer.py     │
   │  │ DataAgent   │  │  │  r_bridge.py     │  └────────────────────┘
   │  │ Analysis    │  │  └──────────────────┘
   │  │ ReportAgent │  │
   │  └─────────────┘  │  ┌──────────────────┐  ┌────────────────────┐
   └───────────────────┘  │  scraper/        │  │  java_interop/     │
                          │  scraper.py      │  │  DataProcessor.java│
                          │  multimodal_     │  │  java_bridge.py    │
                          │    parser.py     │  └────────────────────┘
                          │  dataset_        │
                          │    builder.py    │  ┌────────────────────┐
                          └──────────────────┘  │  benchmarks/       │
                                                │  benchmark_runner  │
                                                └────────────────────┘
```

---

## Tech Stack

- **Python 3.10+** — type hints, `asyncio`, `dataclasses`
- **pandas / NumPy** — vectorised data processing
- **SQLite** (`sqlite3`) — in-memory SQL analytics
- **httpx** — async HTTP client
- **BeautifulSoup4** / `lxml` — HTML parsing
- **matplotlib** — grid visualisation
- **rpy2** *(optional)* — embedded R session
- **Java JDK 11+** *(optional)* — subprocess interop

---

## Setup

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. (Optional) Install R for the r_bridge module
#    https://cran.r-project.org/
pip install rpy2

# 4. (Optional) Install Java JDK 11+ for the java_interop module
#    https://adoptium.net/
```

---

## Usage

```bash
cd portfolio/
python demo.py
```

This runs every module end-to-end:

1. **Multi-agent pipeline** — DataAgent ingests 10 k rows, AnalysisAgent runs 6 SQL queries, ReportAgent writes Markdown + JSON reports to `data/reports/`.
2. **SQL analytics** — 6 query templates printed to stdout.
3. **R bridge** — Linear regression of `revenue ~ quantity` (rpy2 or NumPy fallback).
4. **A\* maze** — Solves a 20×20 maze, prints ASCII grid, saves PNG to `data/astar_maze.png`.
5. **Web scraper** — Scrapes 3 pages from `quotes.toscrape.com`, saves `data/multimodal_dataset.jsonl`.
6. **Java bridge** — Compiles and runs `DataProcessor.java`, normalises a numeric array.

---

## Module Guide

| Path | Description |
|---|---|
| `agents/orchestrator.py` | Master async coordinator; routes tasks to sub-agents |
| `agents/data_agent.py` | Generates / loads synthetic CSV; cleans & normalises |
| `agents/analysis_agent.py` | Runs SQL via SQLite; returns structured results |
| `agents/report_agent.py` | Writes Markdown + JSON reports to `data/reports/` |
| `analytics/pipeline.py` | Baseline (iterrows) vs optimised (vectorised) pipeline |
| `analytics/sql_queries.py` | 6 reusable SQL query methods via `SQLQueryLibrary` |
| `analytics/r_bridge.py` | rpy2 linear regression with graceful NumPy fallback |
| `pathfinding/astar.py` | A\* with Manhattan/Euclidean heuristics, weighted grids |
| `pathfinding/visualizer.py` | ASCII terminal + matplotlib colour visualiser |
| `scraper/scraper.py` | Async crawler with rate-limiting and error handling |
| `scraper/multimodal_parser.py` | Extracts text, images, tables, JSON-LD per page |
| `scraper/dataset_builder.py` | Saves records to JSONL; append or overwrite mode |
| `java_interop/DataProcessor.java` | Java z-score batch normalisation via JSON stdio |
| `java_interop/java_bridge.py` | Compiles + invokes Java; graceful JavaNotFoundError |
| `benchmarks/benchmark_runner.py` | 5-run timing comparison; saves timestamped JSON |
| `config.py` | Single source of truth for all paths and constants |
| `demo.py` | End-to-end entry point |

---

## Output Files

After running `python demo.py` the following files are created:

```
data/
├── sales_data.csv               # 10 000-row synthetic dataset
├── multimodal_dataset.jsonl     # Scraped multimodal records
├── astar_maze.png               # Pathfinding visualisation
└── reports/
    ├── analysis_report.md       # Human-readable analytics report
    └── analysis_report.json     # Machine-readable analytics report

benchmarks/results/
└── benchmark_YYYYMMDDTHHMMSS.json   # Timestamped benchmark run
```
