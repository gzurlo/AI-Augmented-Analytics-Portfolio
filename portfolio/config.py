"""
Central configuration for the AI-Augmented Analytics Portfolio.

All paths, model names, database settings, and tunable constants live here
so that every other module can import from a single source of truth.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Base paths
# ---------------------------------------------------------------------------
BASE_DIR: Path = Path(__file__).parent
DATA_DIR: Path = BASE_DIR / "data"
BENCHMARKS_DIR: Path = BASE_DIR / "benchmarks" / "results"
REPORTS_DIR: Path = BASE_DIR / "data" / "reports"

# Ensure runtime directories exist
for _dir in (DATA_DIR, BENCHMARKS_DIR, REPORTS_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Dataset settings
# ---------------------------------------------------------------------------
SYNTHETIC_DATASET_PATH: Path = DATA_DIR / "sales_data.csv"
MULTIMODAL_DATASET_PATH: Path = DATA_DIR / "multimodal_dataset.jsonl"
NUM_SYNTHETIC_ROWS: int = 10_000

# ---------------------------------------------------------------------------
# Database settings
# ---------------------------------------------------------------------------
DB_PATH: str = ":memory:"  # SQLite in-memory by default

# ---------------------------------------------------------------------------
# Scraper settings
# ---------------------------------------------------------------------------
SCRAPER_BASE_URL: str = "https://quotes.toscrape.com"
SCRAPER_MAX_PAGES: int = 3
SCRAPER_DELAY_SECONDS: float = 0.5
SCRAPER_TIMEOUT_SECONDS: float = 10.0
SCRAPER_USER_AGENT: str = (
    "Mozilla/5.0 (compatible; PortfolioBot/1.0; educational-use)"
)

# ---------------------------------------------------------------------------
# A* pathfinding settings
# ---------------------------------------------------------------------------
ASTAR_GRID_ROWS: int = 20
ASTAR_GRID_COLS: int = 20
ASTAR_WALL_PROBABILITY: float = 0.25
ASTAR_RANDOM_SEED: int = 42

# ---------------------------------------------------------------------------
# Java interop settings
# ---------------------------------------------------------------------------
JAVA_SOURCE: Path = BASE_DIR / "java_interop" / "DataProcessor.java"
JAVA_CLASS_DIR: Path = BASE_DIR / "java_interop"
JAVA_CLASS_NAME: str = "DataProcessor"

# ---------------------------------------------------------------------------
# Benchmark settings
# ---------------------------------------------------------------------------
BENCHMARK_RUNS: int = 5
BENCHMARK_MIN_IMPROVEMENT_PCT: float = 30.0

# ---------------------------------------------------------------------------
# Agent / orchestration settings
# ---------------------------------------------------------------------------
AGENT_LOG_LEVEL: str = "INFO"
REPORT_PATH: Path = REPORTS_DIR / "analysis_report.md"
REPORT_JSON_PATH: Path = REPORTS_DIR / "analysis_report.json"
