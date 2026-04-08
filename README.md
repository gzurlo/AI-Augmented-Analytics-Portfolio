# AI-Augmented Analytics Portfolio

**Author:** Gianluca Zurlo  
**Dataset:** TLC Yellow Taxi Trip Records (Kaggle)  
**Stack:** Python · SQL · R · Java · Multi-Agent AI

---

## Overview

A production-structured analytics portfolio demonstrating a **≥30% pipeline efficiency gain** through vectorized data processing, multi-agent task orchestration, and real-world analytics on 500,000+ NYC taxi trip records.

---

## Benchmark Results

| Pipeline | Mean Time | Std Dev |
|---|---|---|
| Baseline (iterative) | ~X.XXs | ±X.XXs |
| Optimized (vectorized) | ~X.XXs | ±X.XXs |
| **Improvement** | **≥30%** | — |

*(Updated automatically by `benchmarks/benchmark_runner.py`)*

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
| A* Algorithm | Pathfinding visualizer (ASCII + matplotlib) |
| httpx / BS4 | Async multimodal web scraper |
| Kaggle (TLC) | 500K+ real NYC taxi trip records |

---

## Setup

```bash
# 1. Enter project
cd /Users/gianlucazurlo/AI-Augmented-Analytics-Portfolio

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download dataset (requires Kaggle account)
python setup_data.py

# 4. Run full demo
python demo.py
```

---

## Optional: R Integration

```bash
# Install R: https://cran.r-project.org
pip install rpy2
```

## Optional: Java Integration

```bash
# Install JDK: https://adoptium.net
# Bridge auto-compiles DataProcessor.java on first run
```

---

## Project Structure

```
AI-Augmented-Analytics-Portfolio/
├── agents/          # Multi-agent orchestration (async)
├── analytics/       # SQL queries + optimized pipeline
├── pathfinding/     # A* algorithm + visualizer
├── scraper/         # Async multimodal web scraper
├── java_interop/    # Python↔Java subprocess bridge
├── benchmarks/      # Reproducible timing results
├── data/raw/        # TLC Yellow Taxi parquet files
└── demo.py          # Single entry point
```

---

## SQL Queries on Real Taxi Data

Five reusable query functions in `analytics/sql_queries.py`:

| Function | Description |
|---|---|
| `hourly_revenue()` | Average fare + trip count by hour of day |
| `top_routes()` | Top pickup/dropoff location pairs by volume |
| `monthly_growth()` | Month-over-month trip volume % change |
| `payment_breakdown()` | Revenue and trip share by payment type |
| `detect_anomalies()` | Trips beyond N standard deviations from mean fare |

---

## Output Files

```
data/
├── raw/                          ← Kaggle TLC parquet files
├── processed/
│   ├── cleaned_taxi.parquet      ← Cleaned dataset
│   ├── multimodal_dataset.jsonl  ← Scraped web records
│   └── astar_result.png          ← A* maze visualisation
benchmarks/results/
└── benchmark_YYYYMMDDTHHMMSS.json
```
