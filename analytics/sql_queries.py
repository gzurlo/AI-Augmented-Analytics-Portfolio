"""
sql_queries.py — Reusable SQL query functions for TLC taxi analytics.

Each function takes a ``sqlite3.Connection`` (pre-loaded with a ``taxi`` table)
and returns a pandas DataFrame.  Use ``create_connection()`` to build one.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
from config import CLEANED_PARQUET


def create_connection(df: pd.DataFrame | None = None) -> sqlite3.Connection:
    """Create an in-memory SQLite connection loaded with taxi trip data.

    Parameters
    ----------
    df:
        DataFrame to load.  If None, reads from cleaned_taxi.parquet.

    Returns
    -------
    Live ``sqlite3.Connection`` with table ``taxi``.
    """
    if df is None:
        if not CLEANED_PARQUET.exists():
            raise FileNotFoundError(
                f"Cleaned parquet not found: {CLEANED_PARQUET}\n"
                "Run DataAgent (or demo.py) first."
            )
        df = pd.read_parquet(CLEANED_PARQUET)

    # Convert datetime columns to strings for SQLite
    df_sql = df.copy()
    for col in df_sql.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df_sql[col] = df_sql[col].astype(str)

    conn = sqlite3.connect(":memory:")
    df_sql.to_sql("taxi", conn, if_exists="replace", index=False)
    return conn


def hourly_revenue(conn: sqlite3.Connection) -> pd.DataFrame:
    """Average fare and total trips per hour of day.

    Parameters
    ----------
    conn:
        SQLite connection with a ``taxi`` table.

    Returns
    -------
    DataFrame with columns hour_of_day, avg_fare, trip_count, total_revenue.
    """
    return pd.read_sql_query(
        """
        SELECT
            hour_of_day,
            ROUND(AVG(fare_amount), 2)  AS avg_fare,
            COUNT(*)                     AS trip_count,
            ROUND(SUM(fare_amount), 2)   AS total_revenue
        FROM taxi
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """,
        conn,
    )


def top_routes(conn: sqlite3.Connection, n: int = 10) -> pd.DataFrame:
    """Top pickup/dropoff location pairs by trip volume.

    Parameters
    ----------
    conn:
        SQLite connection.
    n:
        Number of top routes to return.

    Returns
    -------
    DataFrame with columns PULocationID, DOLocationID, trip_count, avg_fare.
    """
    return pd.read_sql_query(
        f"""
        SELECT
            PULocationID,
            DOLocationID,
            COUNT(*)                    AS trip_count,
            ROUND(AVG(fare_amount), 2)  AS avg_fare,
            ROUND(AVG(trip_distance), 2) AS avg_distance
        FROM taxi
        GROUP BY PULocationID, DOLocationID
        ORDER BY trip_count DESC
        LIMIT {int(n)}
        """,
        conn,
    )


def monthly_growth(conn: sqlite3.Connection) -> pd.DataFrame:
    """Month-over-month trip volume percentage change.

    Uses a self-join on the derived ``year_month`` column.

    Returns
    -------
    DataFrame with year_month, trip_count, prev_count, growth_pct.
    """
    return pd.read_sql_query(
        """
        WITH monthly AS (
            SELECT
                SUBSTR(tpep_pickup_datetime, 1, 7) AS year_month,
                COUNT(*)                             AS trip_count,
                ROUND(SUM(fare_amount), 2)           AS total_revenue
            FROM taxi
            GROUP BY year_month
        ),
        with_prev AS (
            SELECT
                m.year_month,
                m.trip_count,
                m.total_revenue,
                p.trip_count AS prev_count
            FROM monthly m
            LEFT JOIN monthly p
                ON p.year_month = (
                    SELECT MAX(year_month)
                    FROM monthly
                    WHERE year_month < m.year_month
                )
        )
        SELECT
            year_month,
            trip_count,
            total_revenue,
            prev_count,
            CASE
                WHEN prev_count IS NULL OR prev_count = 0 THEN NULL
                ELSE ROUND((trip_count - prev_count) * 100.0 / prev_count, 2)
            END AS growth_pct
        FROM with_prev
        ORDER BY year_month
        """,
        conn,
    )


def payment_breakdown(conn: sqlite3.Connection) -> pd.DataFrame:
    """Revenue and trip share by payment type.

    TLC codes: 1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute.

    Returns
    -------
    DataFrame with payment_type, trip_count, total_revenue, avg_fare, pct_trips.
    """
    return pd.read_sql_query(
        """
        SELECT
            payment_type,
            COUNT(*)                                                          AS trip_count,
            ROUND(SUM(fare_amount), 2)                                        AS total_revenue,
            ROUND(AVG(fare_amount), 2)                                        AS avg_fare,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)               AS pct_trips
        FROM taxi
        GROUP BY payment_type
        ORDER BY total_revenue DESC
        """,
        conn,
    )


def detect_anomalies(
    conn: sqlite3.Connection,
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """Return trips whose fare_amount exceeds z_threshold standard deviations.

    Parameters
    ----------
    conn:
        SQLite connection.
    z_threshold:
        Number of standard deviations above the mean to flag as anomalous.

    Returns
    -------
    DataFrame of anomalous trips ordered by fare_amount descending.
    """
    return pd.read_sql_query(
        f"""
        WITH stats AS (
            SELECT
                AVG(fare_amount) AS mean_fare,
                SQRT(
                    AVG(fare_amount * fare_amount)
                    - AVG(fare_amount) * AVG(fare_amount)
                ) AS std_fare
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
        WHERE t.fare_amount > s.mean_fare + {float(z_threshold)} * s.std_fare
        ORDER BY t.fare_amount DESC
        LIMIT 25
        """,
        conn,
    )
