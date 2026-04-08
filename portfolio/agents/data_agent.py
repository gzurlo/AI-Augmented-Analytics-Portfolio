"""
data_agent.py — Data ingestion and cleaning agent.

Responsibilities
----------------
* Generate (or load) a synthetic sales/analytics CSV dataset.
* Clean nulls, normalize column names, deduplicate rows.
* Return a tidy pandas DataFrame ready for downstream analysis.

Design pattern
--------------
Each agent exposes a single async ``run()`` coroutine that accepts a plain
dict payload and returns a plain dict result.  This keeps the Orchestrator's
dispatch logic generic and makes individual agents easy to test in isolation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from config import SYNTHETIC_DATASET_PATH, NUM_SYNTHETIC_ROWS

logger = logging.getLogger(__name__)


class DataAgent:
    """Agent responsible for data ingestion and cleaning.

    Parameters
    ----------
    dataset_path:
        Where to read / write the synthetic CSV.  Defaults to the value in
        ``config.py``.
    num_rows:
        How many synthetic rows to generate when the file does not exist.
    """

    def __init__(
        self,
        dataset_path: Path = SYNTHETIC_DATASET_PATH,
        num_rows: int = NUM_SYNTHETIC_ROWS,
    ) -> None:
        self.dataset_path = dataset_path
        self.num_rows = num_rows

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Entry point called by the Orchestrator.

        Parameters
        ----------
        payload:
            Accepts an optional ``force_regenerate`` boolean key.

        Returns
        -------
        dict with keys:
            ``dataframe``  – cleaned pandas DataFrame
            ``rows``       – row count after cleaning
            ``duration_s`` – wall-clock seconds spent
        """
        start = time.perf_counter()
        force = payload.get("force_regenerate", False)

        logger.info("DataAgent: loading dataset (force=%s)", force)
        df = await asyncio.get_event_loop().run_in_executor(
            None, self._load_or_generate, force
        )
        df = self._clean(df)

        duration = time.perf_counter() - start
        logger.info(
            "DataAgent: finished — %d rows in %.3fs", len(df), duration
        )
        return {"dataframe": df, "rows": len(df), "duration_s": duration}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_or_generate(self, force: bool) -> pd.DataFrame:
        """Load CSV from disk or generate a synthetic dataset."""
        if not force and self.dataset_path.exists():
            logger.debug("DataAgent: reading existing CSV from %s", self.dataset_path)
            return pd.read_csv(self.dataset_path)

        logger.debug("DataAgent: generating synthetic dataset (%d rows)", self.num_rows)
        df = self._generate_synthetic()
        self.dataset_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.dataset_path, index=False)
        return df

    def _generate_synthetic(self) -> pd.DataFrame:
        """Create a realistic-looking sales dataset with deliberate dirty data."""
        rng = np.random.default_rng(seed=42)
        n = self.num_rows

        regions = ["North", "South", "East", "West", "Central"]
        products = [
            "Widget-A", "Widget-B", "Gadget-X", "Gadget-Y",
            "Service-Pro", "Service-Lite", "Bundle-1", "Bundle-2",
        ]
        channels = ["Online", "Retail", "Partner", "Direct"]

        dates = pd.date_range("2022-01-01", periods=n, freq="h")
        # Randomly sample dates so they are not strictly sequential.
        # Use positional indices then map back to timestamps to stay compatible
        # with all pandas versions (avoids int64 epoch overflow on pandas 2.x).
        idx = rng.integers(0, len(dates), size=n)
        dates = dates[idx]

        df = pd.DataFrame(
            {
                "order_id": [f"ORD-{i:06d}" for i in range(n)],
                "date": dates,
                "region": rng.choice(regions, size=n),
                "product": rng.choice(products, size=n),
                "channel": rng.choice(channels, size=n),
                "quantity": rng.integers(1, 50, size=n).astype(float),
                "unit_price": np.round(rng.uniform(9.99, 999.99, size=n), 2),
                "discount_pct": np.round(rng.uniform(0, 0.4, size=n), 3),
                "customer_id": [
                    f"CUST-{rng.integers(1, 1000):04d}" for _ in range(n)
                ],
            }
        )
        df["revenue"] = np.round(
            df["quantity"] * df["unit_price"] * (1 - df["discount_pct"]), 2
        )

        # Inject ~2 % nulls to simulate real-world messiness
        null_mask = rng.random(n) < 0.02
        df.loc[null_mask, "revenue"] = np.nan
        df.loc[rng.random(n) < 0.01, "region"] = None

        # Inject ~0.5 % duplicates
        dup_idx = rng.choice(n, size=int(n * 0.005), replace=False)
        df = pd.concat([df, df.iloc[dup_idx]], ignore_index=True)

        return df

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names, drop nulls and duplicates."""
        # Normalize column names: lowercase, strip whitespace
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        before = len(df)
        df = df.drop_duplicates(subset=["order_id"])
        df = df.dropna(subset=["revenue", "region"])

        # Ensure numeric dtypes
        for col in ("quantity", "unit_price", "discount_pct", "revenue"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["revenue"])

        # Parse dates
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        # Derived columns useful for analysis
        df["year_month"] = df["date"].dt.to_period("M").astype(str)

        after = len(df)
        logger.debug(
            "DataAgent: cleaned %d → %d rows (removed %d)", before, after, before - after
        )
        return df.reset_index(drop=True)
