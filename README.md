![Python](https://img.shields.io/badge/Python-3.10+-blue)
![SQL](https://img.shields.io/badge/SQL-SQLite-lightgrey)
![Tests](https://img.shields.io/badge/Tests-22%20passed-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green)

# AI-Augmented Analytics Portfolio

**Author:** Gianluca Zurlo  
**Dataset:** TLC Yellow Taxi Trip Records (Kaggle)  
**Stack:** Python · SQL · R · Java · Multi-Agent AI

---

## Overview

A production-structured analytics portfolio demonstrating a **≥30% pipeline efficiency gain** through vectorized data processing, multi-agent task orchestration, and real-world analytics on 50,000–500,000 NYC taxi trip records.

---

## Benchmark Results

| Pipeline | Mean Time | Std Dev | Speedup |
|---|---|---|---|
| Baseline (iterative, `iterrows`) | ~0.2592s | ±0.0144s | 1× |
| Optimised (vectorised, categorical dtypes) | ~0.0044s | ±0.0003s | **58.9×** |
| **Improvement** | **98.3%** | — | — |

> Target was ≥30%. Actual gain: **98.3%** — the `iterrows` hot loop is eliminated entirely by NumPy vectorisation.

*(Regenerated automatically by `benchmarks/benchmark_runner.py` on each run)*

---

## Architecture

```
┌─────────────────────────────────────────┐
│           Orchestrator Agent            │
│   (asyncio task queue + timing logs)    │
└────────────┬────────────────────────────┘
             │
┌────────────▼─────┐  ┌──────────────────┐  ┌──────────────┐
│   Data Agent     │  │  Analysis Agent   │  │ Report Agent │
│ TLC Parquet ETL  │  │ SQL + Statistics  │  │  MD + JSON   │
└──────────────────┘  └──────────────────┘  └──────────────┘
```

---

## Tech Stack

| Technology | Usage |
|---|---|
| Python 3.10+ | Core language, async orchestration |
| SQL (SQLite) | 5 analytical query templates on taxi data |
| R (via rpy2) | Linear regression on fare ~ distance + duration |
| Java | High-performance batch normalization via subprocess |
| pandas / numpy | Vectorized ETL pipeline |
| A\* Algorithm | Pathfinding visualizer (ASCII + matplotlib) |
| httpx / BS4 | Async multimodal web scraper |
| Kaggle (TLC) | Real NYC taxi trip records |

---

## Setup

```bash
# 1. Clone and enter project
cd AI-Augmented-Analytics-Portfolio

# 2. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure Kaggle credentials (copy and fill in your keys)
cp .env.example .env

# 5. Download dataset (requires Kaggle account)
python setup_data.py

# 6. Run full demo
python demo.py
```

---

## Optional: R Integration

```bash
# Install R: https://cran.r-project.org
pip install rpy2
# R² on real TLC data: ~0.85+  (synthetic: 0.9338)
```

## Optional: Java Integration

```bash
# Install JDK 11+: https://adoptium.net
# Bridge auto-compiles DataProcessor.java on first run — no manual step needed
```

---

## Dataset Source Priority

When `python demo.py` runs, `DataAgent` picks data in this order:

1. **`data/raw/`** — real Kaggle TLC parquet files (run `setup_data.py` first)
2. **`data/processed/cleaned_taxi.parquet`** — cached cleaned output (when `data/raw/` is empty)
3. **Synthetic** — 50,000-row in-memory dataset (no files needed, always works)

The source is printed clearly: `[DataAgent] Source: ...`

---

## Module Highlights

### SQL Queries (`analytics/sql_queries.py`)

| Function | Description |
|---|---|
| `hourly_revenue()` | Average fare + trip count by hour of day |
| `top_routes()` | Top pickup/dropoff location pairs by volume |
| `monthly_growth()` | Month-over-month trip volume % change |
| `payment_breakdown()` | Revenue and trip share by payment type |
| `detect_anomalies(z=2.0)` | Trips beyond 2 std devs from mean fare |

### A\* Pathfinding (last run stats)

```
Grid size    : 20×20
Path length  : 39 steps
Nodes visited: 164 cells
Path cost    : 38.0
Heuristic    : Manhattan
```
Maze visualisation saved to: `data/processed/astar_result.png`

### Regression (fare ~ distance + duration)

```
Method     : NumPy OLS (rpy2 fallback)
Intercept  : 0.1414
β distance : 2.4974  (fare increases ~$2.50/mile)
β duration : 0.2973  (fare increases ~$0.30/min)
R²         : 0.9338  (93.4% of fare variance explained)
n          : 50,000
```

---

## Running Tests

```bash
python -m pytest tests/ -v
```

```
22 passed in 0.59s
```

Tests cover: baseline pipeline, optimised pipeline, speed comparison, A\* correctness (3 heuristics), all 5 SQL queries, DataAgent cleaning.

---

## Project Structure

```
AI-Augmented-Analytics-Portfolio/
├── .env.example          ← Kaggle credentials template
├── .gitignore
├── README.md
├── requirements.txt
├── config.py             ← All paths + constants
├── setup_data.py         ← Downloads TLC dataset to data/raw/
├── demo.py               ← Single entry point (python demo.py)
│
├── agents/               # Multi-agent orchestration (async)
│   ├── orchestrator.py   ← asyncio task queue + rich timing table
│   ├── data_agent.py     ← TLC parquet ETL + synthetic fallback
│   ├── analysis_agent.py ← 5 SQL queries via SQLite
│   └── report_agent.py   ← Writes Markdown report to benchmarks/results/
│
├── analytics/            # Core analytics
│   ├── pipeline.py       ← Baseline vs vectorised, 98% gain
│   ├── sql_queries.py    ← 5 reusable parameterised queries
│   └── r_bridge.py       ← rpy2 regression + NumPy fallback
│
├── pathfinding/          # A* implementation
│   ├── astar.py          ← Manhattan / Euclidean / Chebyshev heuristics
│   └── visualizer.py     ← ASCII (rich) + matplotlib PNG
│
├── scraper/              # Async multimodal scraper
│   ├── scraper.py        ← httpx + BeautifulSoup4
│   ├── multimodal_parser.py ← text / images / tables / metadata
│   └── dataset_builder.py   ← Saves JSONL to data/processed/
│
├── java_interop/         # Python ↔ Java bridge
│   ├── DataProcessor.java ← Min-max normalisation, JSON stdio
│   └── java_bridge.py     ← compile + subprocess + JSON parse
│
├── benchmarks/
│   ├── benchmark_runner.py ← 5-trial comparison, saves JSON
│   └── results/            ← Auto-generated (gitignored)
│
├── tests/
│   └── test_pipeline.py  ← 22 pytest tests, no network required
│
└── data/
    ├── raw/              ← Kaggle TLC files (gitignored)
    └── processed/        ← cleaned_taxi.parquet, astar_result.png, etc.
```

---

## Output Files

After `python demo.py`:

```
data/processed/
├── cleaned_taxi.parquet       ← Cleaned 50K-row dataset
├── multimodal_dataset.jsonl   ← 3 scraped book-store records
└── astar_result.png           ← A* 20×20 maze PNG

benchmarks/results/
├── benchmark_YYYYMMDDTHHMMSS.json  ← Timing results (gitignored)
└── report_YYYYMMDDTHHMMSS.md       ← Full analytics report (gitignored)
```
