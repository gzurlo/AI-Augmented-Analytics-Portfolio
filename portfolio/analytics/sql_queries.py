"""
sql_queries.py — Reusable, parameterised SQL query library backed by SQLite.

Usage
-----
::

    from analytics.sql_queries import SQLQueryLibrary

    lib = SQLQueryLibrary.from_dataframe(df)
    print(lib.total_revenue())
    print(lib.top_n_customers(n=5))
    lib.close()

Each public method returns a pandas DataFrame so callers never need to write
raw SQL or manage cursors.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd


class SQLQueryLibrary:
    """Provides five+ reusable parameterised SQL queries on a sales dataset.

    The library opens an in-memory SQLite database, loads a DataFrame into it,
    and exposes named query methods.  Call ``close()`` when done.

    Parameters
    ----------
    conn:
        A live ``sqlite3.Connection``.  Use ``from_dataframe()`` to build one.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, table: str = "sales") -> "SQLQueryLibrary":
        """Load a DataFrame into an in-memory SQLite database.

        Parameters
        ----------
        df:
            Source DataFrame.  Must contain columns: ``revenue``,
            ``customer_id``, ``year_month``, ``product``, ``region``.
        table:
            Name to register the table under.

        Returns
        -------
        A ready-to-use ``SQLQueryLibrary`` instance.
        """
        conn = sqlite3.connect(":memory:")
        df.to_sql(table, conn, if_exists="replace", index=False)
        return cls(conn)

    def close(self) -> None:
        """Close the underlying database connection."""
        self._conn.close()

    # ------------------------------------------------------------------
    # Query 1 — Total revenue
    # ------------------------------------------------------------------

    def total_revenue(self) -> pd.DataFrame:
        """Return the sum of revenue across the entire dataset.

        Returns
        -------
        Single-row DataFrame with columns ``total_revenue``, ``order_count``,
        ``avg_order_value``.
        """
        return pd.read_sql_query(
            """
            SELECT
                SUM(revenue)  AS total_revenue,
                COUNT(*)      AS order_count,
                AVG(revenue)  AS avg_order_value
            FROM sales
            """,
            self._conn,
        )

    # ------------------------------------------------------------------
    # Query 2 — Top-N customers
    # ------------------------------------------------------------------

    def top_n_customers(self, n: int = 10) -> pd.DataFrame:
        """Return the top-N customers ranked by total spend.

        Parameters
        ----------
        n:
            Number of customers to return.

        Returns
        -------
        DataFrame with columns ``customer_id``, ``total_revenue``,
        ``order_count``, ``avg_order_value``.
        """
        return pd.read_sql_query(
            f"""
            SELECT
                customer_id,
                SUM(revenue)   AS total_revenue,
                COUNT(*)       AS order_count,
                AVG(revenue)   AS avg_order_value
            FROM sales
            GROUP BY customer_id
            ORDER BY total_revenue DESC
            LIMIT {int(n)}
            """,
            self._conn,
        )

    # ------------------------------------------------------------------
    # Query 3 — Monthly growth rate
    # ------------------------------------------------------------------

    def monthly_growth_rate(self) -> pd.DataFrame:
        """Compute month-over-month revenue growth as a percentage.

        Uses a window-function–style self-join to derive the prior-period
        revenue, then calculates ``(current - prior) / prior * 100``.

        Returns
        -------
        DataFrame with columns ``year_month``, ``revenue``, ``prev_revenue``,
        ``growth_pct``.
        """
        return pd.read_sql_query(
            """
            WITH monthly AS (
                SELECT
                    year_month,
                    SUM(revenue) AS revenue
                FROM sales
                GROUP BY year_month
            ),
            with_prev AS (
                SELECT
                    m.year_month,
                    m.revenue,
                    p.revenue AS prev_revenue
                FROM monthly m
                LEFT JOIN monthly p
                    ON p.year_month = (
                        SELECT MAX(year_month) FROM monthly WHERE year_month < m.year_month
                    )
            )
            SELECT
                year_month,
                ROUND(revenue, 2)       AS revenue,
                ROUND(prev_revenue, 2)  AS prev_revenue,
                CASE
                    WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN NULL
                    ELSE ROUND((revenue - prev_revenue) * 100.0 / prev_revenue, 2)
                END AS growth_pct
            FROM with_prev
            ORDER BY year_month
            """,
            self._conn,
        )

    # ------------------------------------------------------------------
    # Query 4 — Cohort retention (simplified)
    # ------------------------------------------------------------------

    def cohort_retention(self) -> pd.DataFrame:
        """Compute a simplified cohort retention table.

        Cohort = first purchase month of a customer.
        Retention = how many customers from a cohort are still active N months later.

        Returns
        -------
        DataFrame with columns ``cohort_month``, ``active_customers``,
        ``cohort_size``, ``retention_pct``.
        """
        return pd.read_sql_query(
            """
            WITH first_purchase AS (
                SELECT
                    customer_id,
                    MIN(year_month) AS cohort_month
                FROM sales
                GROUP BY customer_id
            ),
            cohort_activity AS (
                SELECT
                    fp.cohort_month,
                    COUNT(DISTINCT s.customer_id) AS active_customers
                FROM first_purchase fp
                JOIN sales s ON s.customer_id = fp.customer_id
                GROUP BY fp.cohort_month
            ),
            cohort_size AS (
                SELECT cohort_month, COUNT(*) AS cohort_size
                FROM first_purchase
                GROUP BY cohort_month
            )
            SELECT
                ca.cohort_month,
                ca.active_customers,
                cs.cohort_size,
                ROUND(ca.active_customers * 100.0 / cs.cohort_size, 1) AS retention_pct
            FROM cohort_activity ca
            JOIN cohort_size cs ON cs.cohort_month = ca.cohort_month
            ORDER BY ca.cohort_month
            """,
            self._conn,
        )

    # ------------------------------------------------------------------
    # Query 5 — Anomaly detection (stddev filter)
    # ------------------------------------------------------------------

    def revenue_anomalies(self, std_multiplier: float = 2.0) -> pd.DataFrame:
        """Return orders whose revenue deviates more than N standard deviations
        from the mean (upper tail only — unusually large orders).

        Parameters
        ----------
        std_multiplier:
            Number of standard deviations above the mean to use as the
            anomaly threshold.

        Returns
        -------
        DataFrame of anomalous rows sorted by revenue descending.
        """
        threshold_query = f"""
            WITH stats AS (
                SELECT
                    AVG(revenue) AS mean_rev,
                    AVG(revenue * revenue) - AVG(revenue) * AVG(revenue) AS var_rev
                FROM sales
            )
            SELECT s.*
            FROM sales s, stats
            WHERE s.revenue > stats.mean_rev + {float(std_multiplier)} * SQRT(stats.var_rev)
            ORDER BY s.revenue DESC
        """
        return pd.read_sql_query(threshold_query, self._conn)

    # ------------------------------------------------------------------
    # Query 6 — Revenue by product × region (pivot helper)
    # ------------------------------------------------------------------

    def revenue_by_product_region(self) -> pd.DataFrame:
        """Cross-tabulate revenue by product and region.

        Returns
        -------
        DataFrame with ``product`` and one column per region containing total
        revenue, sorted by grand total descending.
        """
        raw = pd.read_sql_query(
            """
            SELECT product, region, SUM(revenue) AS revenue
            FROM sales
            GROUP BY product, region
            """,
            self._conn,
        )
        pivot = raw.pivot_table(
            index="product", columns="region", values="revenue", fill_value=0
        ).reset_index()
        pivot.columns.name = None
        pivot["grand_total"] = pivot.iloc[:, 1:].sum(axis=1)
        return pivot.sort_values("grand_total", ascending=False)

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "SQLQueryLibrary":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
