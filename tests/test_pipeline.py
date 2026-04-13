"""
tests/test_pipeline.py — Unit and integration tests for the core portfolio modules.

Run with:
    python -m pytest tests/ -v

All tests are self-contained: they use synthetic data and make no network
requests, so they pass without the Kaggle dataset or Java/R installed.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

# Ensure project root is importable from the tests/ directory
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture(scope="module")
def synthetic_df():
    """Return a small synthetic taxi DataFrame for all tests in this module."""
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(0)
    n = 2_000
    pickup  = pd.date_range("2023-01-01", periods=n, freq="5min")
    dur     = rng.uniform(5, 60, n)
    dist    = rng.uniform(0.5, 20, n).round(2)
    fare    = (dist * 2.5 + dur * 0.3 + rng.normal(0, 4, n)).clip(3, 200).round(2)
    df = pd.DataFrame({
        "tpep_pickup_datetime":  pickup,
        "tpep_dropoff_datetime": pickup + pd.to_timedelta(dur, unit="m"),
        "fare_amount":           fare,
        "trip_distance":         dist,
        "trip_duration_mins":    dur.round(2),
        "payment_type":          rng.choice(["1", "2", "3", "4"], n),
        "PULocationID":          rng.integers(1, 265, n),
        "DOLocationID":          rng.integers(1, 265, n),
        "passenger_count":       rng.integers(1, 5, n),
        "hour_of_day":           pickup.hour,
        "day_of_week":           pickup.day_name(),
    })
    return df


@pytest.fixture(scope="module")
def sql_conn(synthetic_df):
    """Return an in-memory SQLite connection loaded with synthetic_df."""
    from analytics.sql_queries import create_connection
    conn = create_connection(synthetic_df)
    yield conn
    conn.close()


# ===========================================================================
# Pipeline tests
# ===========================================================================

class TestBaselinePipeline:
    """Tests for the unoptimised (baseline) pipeline."""

    def test_runs_without_error(self, synthetic_df):
        """baseline_pipeline() completes and returns expected keys."""
        from analytics.pipeline import baseline_pipeline
        result = baseline_pipeline(synthetic_df.copy())
        assert isinstance(result, dict)
        assert "filtered_df" in result
        assert "hourly_summary" in result
        assert "payment_summary" in result

    def test_returns_non_empty_dataframes(self, synthetic_df):
        """All output DataFrames must have at least one row."""
        import pandas as pd
        from analytics.pipeline import baseline_pipeline
        result = baseline_pipeline(synthetic_df.copy())
        for key in ("filtered_df", "hourly_summary", "payment_summary"):
            assert isinstance(result[key], pd.DataFrame), f"{key} is not a DataFrame"
            assert len(result[key]) > 0, f"{key} is empty"

    def test_filtered_df_respects_fare_bounds(self, synthetic_df):
        """filtered_df must contain only rows within the fare filter bounds."""
        from analytics.pipeline import baseline_pipeline
        from config import FARE_MIN, FARE_MAX
        result = baseline_pipeline(synthetic_df.copy())
        df = result["filtered_df"]
        assert df["fare_amount"].min() >= FARE_MIN
        assert df["fare_amount"].max() <= FARE_MAX


class TestOptimizedPipeline:
    """Tests for the vectorised (optimised) pipeline."""

    def test_runs_without_error(self, synthetic_df):
        """optimized_pipeline() completes and returns expected keys."""
        from analytics.pipeline import optimized_pipeline
        result = optimized_pipeline(synthetic_df.copy())
        assert isinstance(result, dict)
        assert "filtered_df" in result
        assert "hourly_summary" in result
        assert "payment_summary" in result

    def test_returns_non_empty_dataframes(self, synthetic_df):
        """All output DataFrames must have at least one row."""
        import pandas as pd
        from analytics.pipeline import optimized_pipeline
        result = optimized_pipeline(synthetic_df.copy())
        for key in ("filtered_df", "hourly_summary", "payment_summary"):
            assert isinstance(result[key], pd.DataFrame), f"{key} is not a DataFrame"
            assert len(result[key]) > 0, f"{key} is empty"

    def test_filtered_df_respects_fare_bounds(self, synthetic_df):
        """filtered_df must contain only rows within the fare filter bounds."""
        from analytics.pipeline import optimized_pipeline
        from config import FARE_MIN, FARE_MAX
        result = optimized_pipeline(synthetic_df.copy())
        df = result["filtered_df"]
        assert df["fare_amount"].min() >= FARE_MIN
        assert df["fare_amount"].max() <= FARE_MAX


class TestOptimizedFasterThanBaseline:
    """Confirm the optimised pipeline is meaningfully faster than the baseline."""

    def test_optimized_is_faster(self, synthetic_df):
        """Optimised pipeline must be at least 30% faster than baseline on 5k rows."""
        import numpy as np
        import pandas as pd
        from analytics.pipeline import baseline_pipeline, optimized_pipeline

        # Use a 5 000-row sample so the test is quick but the ratio is clear
        rng = np.random.default_rng(1)
        n = 5_000
        pickup  = pd.date_range("2023-01-01", periods=n, freq="5min")
        dur     = rng.uniform(5, 60, n)
        dist    = rng.uniform(0.5, 20, n).round(2)
        fare    = (dist * 2.5 + dur * 0.3 + rng.normal(0, 4, n)).clip(3, 200).round(2)
        df = pd.DataFrame({
            "tpep_pickup_datetime":  pickup,
            "tpep_dropoff_datetime": pickup + pd.to_timedelta(dur, unit="m"),
            "fare_amount":           fare,
            "trip_distance":         dist,
            "trip_duration_mins":    dur.round(2),
            "payment_type":          rng.choice(["1", "2", "3", "4"], n),
            "PULocationID":          rng.integers(1, 265, n),
            "DOLocationID":          rng.integers(1, 265, n),
            "passenger_count":       rng.integers(1, 5, n),
            "hour_of_day":           pickup.hour,
            "day_of_week":           pickup.day_name(),
        })

        # Time each pipeline (single run; enough to detect order-of-magnitude diff)
        t0 = time.perf_counter()
        baseline_pipeline(df.copy())
        baseline_t = time.perf_counter() - t0

        t0 = time.perf_counter()
        optimized_pipeline(df.copy())
        optimized_t = time.perf_counter() - t0

        improvement = (1 - optimized_t / baseline_t) * 100 if baseline_t > 0 else 0.0
        assert improvement >= 30.0, (
            f"Optimised pipeline was only {improvement:.1f}% faster "
            f"(baseline={baseline_t:.4f}s, optimised={optimized_t:.4f}s). "
            "Expected ≥30% improvement."
        )


# ===========================================================================
# A* pathfinding tests
# ===========================================================================

class TestAStar:
    """Tests for the A* pathfinding implementation."""

    def test_finds_path_on_default_grid(self):
        """A* must find a valid path from (0,0) to (19,19) on the default grid."""
        from pathfinding.astar import Grid, AStar
        from config import GRID_SIZE, WALL_DENSITY, GRID_SEED

        grid = Grid(
            width=GRID_SIZE, height=GRID_SIZE,
            wall_density=WALL_DENSITY, seed=GRID_SEED,
        )
        solver = AStar(grid, heuristic="manhattan")
        path, visited, cost = solver.solve((0, 0), (GRID_SIZE - 1, GRID_SIZE - 1))

        assert len(path) > 0, "A* returned an empty path — no route found"
        assert path[0] == (0, 0), "Path must start at (0,0)"
        assert path[-1] == (GRID_SIZE - 1, GRID_SIZE - 1), "Path must end at (19,19)"

    def test_path_contains_only_passable_cells(self):
        """Every cell in the returned path must be passable (not a wall)."""
        from pathfinding.astar import Grid, AStar
        from config import GRID_SIZE, WALL_DENSITY, GRID_SEED

        grid = Grid(
            width=GRID_SIZE, height=GRID_SIZE,
            wall_density=WALL_DENSITY, seed=GRID_SEED,
        )
        solver = AStar(grid)
        path, _, _ = solver.solve((0, 0), (GRID_SIZE - 1, GRID_SIZE - 1))

        for r, c in path:
            assert not grid.is_wall(r, c), f"Path passes through wall at ({r},{c})"

    def test_path_is_connected(self):
        """Adjacent cells in the path must be exactly one step apart."""
        from pathfinding.astar import Grid, AStar
        from config import GRID_SIZE, WALL_DENSITY, GRID_SEED

        grid = Grid(
            width=GRID_SIZE, height=GRID_SIZE,
            wall_density=WALL_DENSITY, seed=GRID_SEED,
        )
        solver = AStar(grid)
        path, _, _ = solver.solve((0, 0), (GRID_SIZE - 1, GRID_SIZE - 1))

        for (r1, c1), (r2, c2) in zip(path, path[1:]):
            manhattan = abs(r1 - r2) + abs(c1 - c2)
            assert manhattan == 1, (
                f"Non-adjacent cells in path: ({r1},{c1}) → ({r2},{c2})"
            )

    def test_euclidean_heuristic_also_finds_path(self):
        """Euclidean heuristic variant must also solve the maze."""
        from pathfinding.astar import Grid, AStar
        from config import GRID_SIZE, WALL_DENSITY, GRID_SEED

        grid = Grid(
            width=GRID_SIZE, height=GRID_SIZE,
            wall_density=WALL_DENSITY, seed=GRID_SEED,
        )
        solver = AStar(grid, heuristic="euclidean")
        path, _, _ = solver.solve((0, 0), (GRID_SIZE - 1, GRID_SIZE - 1))
        assert len(path) > 0

    def test_chebyshev_heuristic_also_finds_path(self):
        """Chebyshev heuristic variant must also solve the maze."""
        from pathfinding.astar import Grid, AStar
        from config import GRID_SIZE, WALL_DENSITY, GRID_SEED

        grid = Grid(
            width=GRID_SIZE, height=GRID_SIZE,
            wall_density=WALL_DENSITY, seed=GRID_SEED,
        )
        solver = AStar(grid, heuristic="chebyshev")
        path, _, _ = solver.solve((0, 0), (GRID_SIZE - 1, GRID_SIZE - 1))
        assert len(path) > 0

    def test_visited_nodes_count(self):
        """Visited node count must be positive and ≤ total passable cells."""
        from pathfinding.astar import Grid, AStar
        from config import GRID_SIZE, WALL_DENSITY, GRID_SEED
        import numpy as np

        grid = Grid(
            width=GRID_SIZE, height=GRID_SIZE,
            wall_density=WALL_DENSITY, seed=GRID_SEED,
        )
        total_passable = int(np.sum(~grid._walls))
        solver = AStar(grid)
        _, visited, _ = solver.solve((0, 0), (GRID_SIZE - 1, GRID_SIZE - 1))

        assert len(visited) > 0
        assert len(visited) <= total_passable


# ===========================================================================
# SQL query tests
# ===========================================================================

class TestSQLQueries:
    """Tests for the five SQL query library functions."""

    def test_hourly_revenue_non_empty(self, sql_conn):
        """hourly_revenue() must return at least one row."""
        from analytics.sql_queries import hourly_revenue
        df = hourly_revenue(sql_conn)
        assert len(df) > 0
        assert "hour_of_day" in df.columns
        assert "avg_fare" in df.columns

    def test_top_routes_respects_n(self, sql_conn):
        """top_routes(n=5) must return at most 5 rows."""
        from analytics.sql_queries import top_routes
        df = top_routes(sql_conn, n=5)
        assert len(df) <= 5
        assert len(df) > 0

    def test_monthly_growth_has_growth_col(self, sql_conn):
        """monthly_growth() must include a growth_pct column."""
        from analytics.sql_queries import monthly_growth
        df = monthly_growth(sql_conn)
        assert "growth_pct" in df.columns
        assert len(df) > 0

    def test_payment_breakdown_sums_to_100(self, sql_conn):
        """pct_trips in payment_breakdown must sum to ~100%."""
        from analytics.sql_queries import payment_breakdown
        df = payment_breakdown(sql_conn)
        assert len(df) > 0
        total_pct = df["pct_trips"].sum()
        assert abs(total_pct - 100.0) < 1.0, f"pct_trips sums to {total_pct}, expected ~100"

    def test_detect_anomalies_returns_results_at_z2(self, sql_conn):
        """detect_anomalies(z_threshold=2) must return at least one row."""
        from analytics.sql_queries import detect_anomalies
        df = detect_anomalies(sql_conn, z_threshold=2.0)
        assert len(df) > 0, "No anomalies detected at z>2 — check fare distribution"
        assert "z_score" in df.columns

    def test_anomalies_ordered_descending(self, sql_conn):
        """Anomaly rows must be ordered by fare_amount descending."""
        from analytics.sql_queries import detect_anomalies
        df = detect_anomalies(sql_conn, z_threshold=2.0)
        if len(df) > 1:
            assert df["fare_amount"].is_monotonic_decreasing or \
                   (df["fare_amount"].diff().dropna() <= 0).all(), \
                   "Anomalies are not sorted by fare_amount descending"


# ===========================================================================
# DataAgent smoke test
# ===========================================================================

class TestDataAgent:
    """Smoke tests for DataAgent (uses synthetic fallback — no Kaggle needed)."""

    def test_returns_required_keys(self):
        """DataAgent.run() must return dataframe, rows, columns, source, duration_s."""
        import asyncio
        from agents.data_agent import DataAgent
        result = asyncio.run(DataAgent().run())
        for key in ("dataframe", "rows", "columns", "source", "duration_s"):
            assert key in result, f"Missing key: {key}"

    def test_dataframe_has_required_columns(self):
        """Cleaned DataFrame must contain the derived columns added by _add_features."""
        import asyncio
        from agents.data_agent import DataAgent
        result = asyncio.run(DataAgent(force_refresh=True).run())
        df = result["dataframe"]
        for col in ("trip_duration_mins", "cost_per_mile", "hour_of_day", "day_of_week"):
            assert col in df.columns, f"Missing derived column: {col}"

    def test_no_nulls_in_key_columns(self):
        """Key columns must have no nulls after cleaning."""
        import asyncio
        from agents.data_agent import DataAgent
        result = asyncio.run(DataAgent(force_refresh=True).run())
        df = result["dataframe"]
        for col in ("fare_amount", "trip_distance", "trip_duration_mins"):
            null_count = df[col].isna().sum()
            assert null_count == 0, f"{col} has {null_count} nulls after cleaning"
