"""
analysis_agent.py — SQL + statistical analysis agent.

Responsibilities
----------------
* Load a pandas DataFrame from the previous DataAgent result.
* Run parameterized SQL queries via an in-memory SQLite database.
* Compute revenue aggregations, top-product rankings, and monthly trends.
* Return a structured result dict ready for the ReportAgent.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class AnalysisAgent:
    """Agent that runs SQL analytics on a tidy DataFrame.

    This agent registers the DataFrame as a virtual SQLite table, executes
    a battery of pre-defined queries, and bundles the results.

    Parameters
    ----------
    top_n:
        How many top products / customers to surface in ranking queries.
    """

    def __init__(self, top_n: int = 10) -> None:
        self.top_n = top_n

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Entry point called by the Orchestrator.

        Parameters
        ----------
        payload:
            Must contain key ``dataframe`` (a pandas DataFrame).

        Returns
        -------
        dict with keys:
            ``revenue_by_region``  – DataFrame
            ``top_products``       – DataFrame
            ``monthly_trend``      – DataFrame
            ``summary_stats``      – dict of scalar KPIs
            ``duration_s``         – wall-clock seconds
        """
        start = time.perf_counter()
        df: pd.DataFrame = payload["dataframe"]

        logger.info("AnalysisAgent: running SQL analytics on %d rows", len(df))

        results = await asyncio.get_event_loop().run_in_executor(
            None, self._run_analysis, df
        )

        duration = time.perf_counter() - start
        results["duration_s"] = duration
        logger.info("AnalysisAgent: completed in %.3fs", duration)
        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_analysis(self, df: pd.DataFrame) -> dict[str, Any]:
        """Execute all queries inside a single in-memory SQLite connection."""
        conn = sqlite3.connect(":memory:")
        df.to_sql("sales", conn, if_exists="replace", index=False)

        results: dict[str, Any] = {}

        # 1. Revenue by region
        results["revenue_by_region"] = pd.read_sql_query(
            """
            SELECT
                region,
                COUNT(*)            AS order_count,
                SUM(revenue)        AS total_revenue,
                AVG(revenue)        AS avg_order_value,
                SUM(quantity)       AS total_units
            FROM sales
            GROUP BY region
            ORDER BY total_revenue DESC
            """,
            conn,
        )

        # 2. Top-N products by revenue
        results["top_products"] = pd.read_sql_query(
            f"""
            SELECT
                product,
                COUNT(*)            AS order_count,
                SUM(revenue)        AS total_revenue,
                AVG(discount_pct)   AS avg_discount
            FROM sales
            GROUP BY product
            ORDER BY total_revenue DESC
            LIMIT {self.top_n}
            """,
            conn,
        )

        # 3. Monthly revenue trend
        results["monthly_trend"] = pd.read_sql_query(
            """
            SELECT
                year_month,
                SUM(revenue)  AS monthly_revenue,
                COUNT(*)      AS order_count
            FROM sales
            GROUP BY year_month
            ORDER BY year_month
            """,
            conn,
        )

        # 4. Revenue by channel
        results["revenue_by_channel"] = pd.read_sql_query(
            """
            SELECT
                channel,
                SUM(revenue)  AS total_revenue,
                COUNT(*)      AS order_count
            FROM sales
            GROUP BY channel
            ORDER BY total_revenue DESC
            """,
            conn,
        )

        # 5. Summary scalar KPIs
        kpi_row = pd.read_sql_query(
            """
            SELECT
                COUNT(*)               AS total_orders,
                SUM(revenue)           AS total_revenue,
                AVG(revenue)           AS avg_order_value,
                MAX(revenue)           AS max_order_value,
                MIN(revenue)           AS min_order_value,
                COUNT(DISTINCT customer_id) AS unique_customers,
                COUNT(DISTINCT product)     AS unique_products
            FROM sales
            """,
            conn,
        )
        results["summary_stats"] = kpi_row.iloc[0].to_dict()

        # 6. Anomaly detection — orders whose revenue > mean + 2*stddev
        results["anomalies"] = pd.read_sql_query(
            """
            WITH stats AS (
                SELECT
                    AVG(revenue)                        AS mean_rev,
                    AVG(revenue * revenue) - AVG(revenue) * AVG(revenue) AS var_rev
                FROM sales
            )
            SELECT s.*
            FROM sales s, stats
            WHERE s.revenue > stats.mean_rev + 2 * SQRT(stats.var_rev)
            ORDER BY s.revenue DESC
            LIMIT 20
            """,
            conn,
        )

        conn.close()
        return results
