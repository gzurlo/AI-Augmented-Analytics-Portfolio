"""
config.py — Central configuration for the AI-Augmented Analytics Portfolio.

All paths, constants, and dataset settings live here.
Every directory is created on import so no module needs to mkdir manually.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Root paths
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path("/Users/gianlucazurlo/AI-Augmented-Analytics-Portfolio")

DATA_RAW: Path = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED: Path = PROJECT_ROOT / "data" / "processed"
BENCHMARKS_RESULTS: Path = PROJECT_ROOT / "benchmarks" / "results"

# Ensure all runtime directories exist
for _d in (DATA_RAW, DATA_PROCESSED, BENCHMARKS_RESULTS):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------
DATASET_NAME: str = "marcbrandner/tlc-trip-record-data-yellow-taxi"
CLEANED_PARQUET: Path = DATA_PROCESSED / "cleaned_taxi.parquet"
MULTIMODAL_JSONL: Path = DATA_PROCESSED / "multimodal_dataset.jsonl"
ASTAR_PNG: Path = DATA_PROCESSED / "astar_result.png"

# ---------------------------------------------------------------------------
# Ingestion limits
# ---------------------------------------------------------------------------
MAX_ROWS: int = 500_000          # DataAgent hard cap
PIPELINE_SAMPLE: int = 100_000   # benchmark pipeline sample size

# ---------------------------------------------------------------------------
# Fare / distance filters
# ---------------------------------------------------------------------------
FARE_MIN: float = 1.0
FARE_MAX: float = 500.0
DIST_MIN: float = 0.1
DIST_MAX: float = 100.0

# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------
SCRAPER_BASE_URL: str = "https://books.toscrape.com"
SCRAPER_PAGES: int = 3
SCRAPER_DELAY: float = 0.5

# ---------------------------------------------------------------------------
# Pathfinding
# ---------------------------------------------------------------------------
GRID_SIZE: int = 20
WALL_DENSITY: float = 0.20
GRID_SEED: int = 42

# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
N_BENCHMARK_TRIALS: int = 5
MIN_IMPROVEMENT_PCT: float = 30.0

# ---------------------------------------------------------------------------
# Java
# ---------------------------------------------------------------------------
JAVA_SOURCE: Path = PROJECT_ROOT / "java_interop" / "DataProcessor.java"
JAVA_CLASS_DIR: Path = PROJECT_ROOT / "java_interop"
