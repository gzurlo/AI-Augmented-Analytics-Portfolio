"""
analysis_agent.py — SQL analytics agent for TLC taxi trip data.

Runs five parameterised SQL queries against the cleaned taxi DataFrame using
an in-memory SQLite connection.  Returns all results as a structured dict.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
from config import CLEANED_PARQUET

logger = logging.getLogger(__name__)


class AnalysisAgent:
    """Agent that runs SQL analytics on the cleaned TLC taxi dataset.

    Parameters
    ----------
    df:
        Optional pre-loaded DataFrame.  If None, loads from cleaned parquet.
    """

    def __init__(self, df: pd.DataFrame | None = None) -> None:
        self._df = df

    async def run(self) -> dict[str, Any]:
        """Async entry point: runs all five SQL queries.

        Returns
        -------
        dict with keys: results (dict of DataFrames), duration_s, rows_analysed.
        """
        t0 = time.perf_counter()
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, self._run_sync)
        results["duration_s"] = round(time.perf_counter() - t0, 3)
        return results

    def _run_sync(self) -> dict[str, Any]:
        """Load data and execute all queries."""
        df = self._load_df()
        conn = sqlite3.connect(":memory:")

        # Convert datetime cols to strings for SQLite compatibility
        df_sql = df.copy()
        for col in df_sql.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
            df_sql[col] = df_sql[col].astype(str)

        df_sql.to_sql("taxi", conn, if_exists="replace", index=False)

        queries: dict[str, pd.DataFrame] = {
            "avg_fare_by_hour":         self._avg_fare_by_hour(conn),
            "top_10_pickup_hours":      self._top_10_pickup_hours(conn),
            "avg_duration_by_dow":      self._avg_duration_by_dow(conn),
            "revenue_by_payment_type":  self._revenue_by_payment_type(conn),
            "fare_anomalies":           self._fare_anomalies(conn),
        }
        conn.close()

        return {
            "results": queries,
            "rows_analysed": len(df),
        }

    # ------------------------------------------------------------------
    # Query implementations
    # ------------------------------------------------------------------

    def _avg_fare_by_hour(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """Average fare and trip count grouped by hour of day."""
        return pd.read_sql_query(
            """
            SELECT
                hour_of_day,
                ROUND(AVG(fare_amount), 2)   AS avg_fare,
                COUNT(*)                      AS trip_count,
                ROUND(AVG(trip_distance), 2) AS avg_distance
            FROM taxi
            GROUP BY hour_of_day
            ORDER BY hour_of_day
            """,
            conn,
        )

    def _top_10_pickup_hours(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """Top 10 busiest hours by trip volume."""
        return pd.read_sql_query(
            """
            SELECT
                hour_of_day,
                COUNT(*)                    AS trip_count,
                ROUND(SUM(fare_amount), 2)  AS total_revenue,
                ROUND(AVG(fare_amount), 2)  AS avg_fare
            FROM taxi
            GROUP BY hour_of_day
            ORDER BY trip_count DESC
            LIMIT 10
            """,
            conn,
        )

    def _avg_duration_by_dow(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """Average trip duration in minutes grouped by day of week."""
        return pd.read_sql_query(
            """
            SELECT
                day_of_week,
                ROUND(AVG(trip_duration_mins), 2) AS avg_duration_mins,
                ROUND(AVG(fare_amount), 2)         AS avg_fare,
                COUNT(*)                            AS trip_count
            FROM taxi
            GROUP BY day_of_week
            ORDER BY avg_duration_mins DESC
            """,
            conn,
        )

    def _revenue_by_payment_type(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """Revenue and trip share broken down by payment_type.

        TLC codes: 1=Credit card, 2=Cash, 3=No charge, 4=Dispute.
        """
        return pd.read_sql_query(
            """
            SELECT
                payment_type,
                COUNT(*)                          AS trip_count,
                ROUND(SUM(fare_amount), 2)         AS total_revenue,
                ROUND(AVG(fare_amount), 2)         AS avg_fare,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_trips
            FROM taxi
            GROUP BY payment_type
            ORDER BY total_revenue DESC
            """,
            conn,
        )

    def _fare_anomalies(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """Trips where fare_amount exceeds mean + 3 standard deviations."""
        return pd.read_sql_query(
            """
            WITH stats AS (
                SELECT
                    AVG(fare_amount)                                  AS mean_fare,
                    SQRT(
                        AVG(fare_amount * fare_amount)
                        - AVG(fare_amount) * AVG(fare_amount)
                    )                                                 AS std_fare
                FROM taxi
            )
            SELECT
                t.tpep_pickup_datetime,
                t.trip_distance,
                t.fare_amount,
                t.trip_duration_mins,
                t.payment_type,
                ROUND((t.fare_amount - s.mean_fare) / s.std_fare, 2) AS z_score
            FROM taxi t, stats s
            WHERE t.fare_amount > s.mean_fare + 3 * s.std_fare
            ORDER BY t.fare_amount DESC
            LIMIT 20
            """,
            conn,
        )

    # ------------------------------------------------------------------
    # Loader
    # ------------------------------------------------------------------

    def _load_df(self) -> pd.DataFrame:
        """Return injected DataFrame or load from cleaned parquet."""
        if self._df is not None:
            return self._df
        if CLEANED_PARQUET.exists():
            return pd.read_parquet(CLEANED_PARQUET)
        raise FileNotFoundError(
            f"Cleaned parquet not found at {CLEANED_PARQUET}. "
            "Run DataAgent first."
        )
