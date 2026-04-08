"""
data_agent.py — TLC Yellow Taxi data ingestion and cleaning agent.

Responsibilities
----------------
* Load real parquet files from data/raw/ (up to MAX_ROWS rows).
* Clean: drop nulls in key columns, remove fare/distance outliers.
* Add derived columns: trip_duration_mins, cost_per_mile, hour_of_day, day_of_week.
* Save cleaned DataFrame to data/processed/cleaned_taxi.parquet.
* Fall back to synthetic taxi-like data if no parquet files found.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
import sys

# Ensure project root is on path when module is imported standalone
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import numpy as np
import pandas as pd

from config import (
    DATA_RAW, CLEANED_PARQUET, MAX_ROWS,
    FARE_MIN, FARE_MAX, DIST_MIN, DIST_MAX,
)

logger = logging.getLogger(__name__)


class DataAgent:
    """Agent responsible for ingesting and cleaning TLC taxi trip data.

    Parameters
    ----------
    max_rows:
        Maximum number of rows to load (sliced after reading).
    force_refresh:
        Re-run cleaning even if cleaned_taxi.parquet already exists.
    """

    def __init__(self, max_rows: int = MAX_ROWS, force_refresh: bool = False) -> None:
        self.max_rows = max_rows
        self.force_refresh = force_refresh

    async def run(self) -> dict:
        """Async entry point. Loads, cleans, and saves the dataset.

        Returns
        -------
        dict with keys: dataframe, rows, columns, duration_s, source.
        """
        t0 = time.perf_counter()
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._run_sync)
        result["duration_s"] = round(time.perf_counter() - t0, 3)
        return result

    def _run_sync(self) -> dict:
        """Synchronous implementation called from the async wrapper."""
        if not self.force_refresh and CLEANED_PARQUET.exists():
            logger.info("DataAgent: loading cached cleaned parquet")
            df = pd.read_parquet(CLEANED_PARQUET)
            return {
                "dataframe": df,
                "rows": len(df),
                "columns": list(df.columns),
                "source": "cache",
            }

        df, source = self._load_raw()
        df = self._clean(df)
        df = self._add_features(df)

        CLEANED_PARQUET.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(CLEANED_PARQUET, index=False)
        logger.info("DataAgent: saved %d rows to %s", len(df), CLEANED_PARQUET)

        return {
            "dataframe": df,
            "rows": len(df),
            "columns": list(df.columns),
            "source": source,
        }

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def _load_raw(self) -> tuple[pd.DataFrame, str]:
        """Load raw parquet files from data/raw/. Fall back to synthetic data."""
        parquets = sorted(DATA_RAW.glob("*.parquet"))
        csvs = sorted(DATA_RAW.glob("*.csv"))

        if parquets:
            logger.info("DataAgent: reading %d parquet file(s)", len(parquets))
            frames = []
            rows_left = self.max_rows
            for p in parquets:
                if rows_left <= 0:
                    break
                try:
                    chunk = pd.read_parquet(p)
                    frames.append(chunk.head(rows_left))
                    rows_left -= len(frames[-1])
                except Exception as exc:  # noqa: BLE001
                    logger.warning("DataAgent: could not read %s — %s", p.name, exc)
            if frames:
                return pd.concat(frames, ignore_index=True), "parquet"

        if csvs:
            logger.info("DataAgent: reading CSV %s", csvs[0].name)
            try:
                df = pd.read_csv(csvs[0], nrows=self.max_rows, low_memory=False)
                return df, "csv"
            except Exception as exc:  # noqa: BLE001
                logger.warning("DataAgent: CSV read failed — %s", exc)

        logger.warning("DataAgent: no raw files found — generating synthetic data")
        return self._synthetic_fallback(), "synthetic"

    # ------------------------------------------------------------------
    # Cleaning
    # ------------------------------------------------------------------

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardise column names, drop nulls, and filter outliers."""
        # Normalise column names
        df.columns = [c.strip().lower() for c in df.columns]

        # Identify pickup/dropoff datetime columns (handle naming variations)
        pickup_col = self._find_col(df, ["tpep_pickup_datetime", "pickup_datetime", "lpep_pickup_datetime"])
        dropoff_col = self._find_col(df, ["tpep_dropoff_datetime", "dropoff_datetime", "lpep_dropoff_datetime"])
        fare_col = self._find_col(df, ["fare_amount", "fare"])
        dist_col = self._find_col(df, ["trip_distance", "distance"])

        if pickup_col:
            df = df.rename(columns={pickup_col: "tpep_pickup_datetime"})
        if dropoff_col:
            df = df.rename(columns={dropoff_col: "tpep_dropoff_datetime"})
        if fare_col and fare_col != "fare_amount":
            df = df.rename(columns={fare_col: "fare_amount"})
        if dist_col and dist_col != "trip_distance":
            df = df.rename(columns={dist_col: "trip_distance"})

        # Ensure required columns exist
        required = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "trip_distance"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.warning("DataAgent: missing columns %s — using synthetic", missing)
            df = self._synthetic_fallback()
            df.columns = [c.strip().lower() for c in df.columns]

        # Parse datetimes
        for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Numeric coercion
        for col in ("fare_amount", "trip_distance"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        before = len(df)
        df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                                "fare_amount", "trip_distance"])

        # Outlier filters
        df = df[df["fare_amount"].between(FARE_MIN, FARE_MAX)]
        df = df[df["trip_distance"].between(DIST_MIN, DIST_MAX)]

        after = len(df)
        logger.info("DataAgent: cleaned %d → %d rows", before, after)
        return df.reset_index(drop=True)

    def _add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived columns used in analytics queries."""
        dt_diff = (
            df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        ).dt.total_seconds() / 60.0
        df = df.assign(
            trip_duration_mins=dt_diff.clip(lower=0.5, upper=300),
            cost_per_mile=(df["fare_amount"] / df["trip_distance"].clip(lower=0.1)).round(2),
            hour_of_day=df["tpep_pickup_datetime"].dt.hour,
            day_of_week=df["tpep_pickup_datetime"].dt.day_name(),
        )
        # Ensure payment_type column exists (used in SQL queries)
        if "payment_type" not in df.columns:
            rng = np.random.default_rng(0)
            df["payment_type"] = rng.choice([1, 2, 3, 4], size=len(df))
        df["payment_type"] = df["payment_type"].astype(str)
        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
        """Return the first candidate column name present in df."""
        for c in candidates:
            if c in df.columns:
                return c
        return None

    @staticmethod
    def _synthetic_fallback() -> pd.DataFrame:
        """Generate a realistic synthetic TLC taxi dataset."""
        rng = np.random.default_rng(99)
        n = 50_000

        pickup = pd.date_range("2023-01-01", periods=n, freq="2min")
        duration_mins = rng.uniform(3, 60, n)
        dropoff = pickup + pd.to_timedelta(duration_mins, unit="m")

        dist = rng.uniform(0.5, 20, n).round(2)
        # Realistic fare correlated with distance + duration
        fare = (dist * 2.5 + duration_mins * 0.3
                + rng.normal(0, 4, n)).clip(3, 150).round(2)

        return pd.DataFrame({
            "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": dropoff,
            "fare_amount": fare,
            "trip_distance": dist,
            "passenger_count": rng.integers(1, 5, n),
            "payment_type": rng.choice(["1", "2", "3", "4"], n),
            "tip_amount": rng.uniform(0, 20, n).round(2),
            "total_amount": (fare + rng.uniform(0, 5, n)).round(2),
            "PULocationID": rng.integers(1, 265, n),
            "DOLocationID": rng.integers(1, 265, n),
        })
