"""
r_bridge.py — Python ↔ R bridge for statistical modelling on taxi data.

Fits a multiple linear regression:
    fare_amount ~ trip_distance + trip_duration_mins

via rpy2 (embedded R).  Falls back to a pure NumPy OLS when rpy2/R is absent.

Setup
-----
1. Install R   : https://cran.r-project.org
2. Install rpy2: pip install rpy2
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import numpy as np
import pandas as pd
from config import CLEANED_PARQUET

logger = logging.getLogger(__name__)


def run_r_regression(
    df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Run multiple linear regression fare_amount ~ trip_distance + trip_duration_mins.

    Parameters
    ----------
    df:
        Input DataFrame.  Loads cleaned_taxi.parquet if None.

    Returns
    -------
    dict with keys: method, intercept, coef_distance, coef_duration,
    r_squared, summary_text, available.
    """
    if df is None:
        if CLEANED_PARQUET.exists():
            df = pd.read_parquet(CLEANED_PARQUET)
        else:
            # Create minimal synthetic data for the bridge to work standalone
            rng = np.random.default_rng(0)
            n = 5000
            dist = rng.uniform(0.5, 20, n)
            dur  = rng.uniform(3, 60, n)
            df = pd.DataFrame({
                "fare_amount":        dist * 2.5 + dur * 0.3 + rng.normal(0, 2, n),
                "trip_distance":      dist,
                "trip_duration_mins": dur,
            })

    try:
        return _rpy2_regression(df)
    except ImportError:
        logger.warning(
            "rpy2 not installed — falling back to NumPy OLS.\n"
            "Install R: https://cran.r-project.org\n"
            "Then: pip install rpy2"
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("rpy2 raised an error (%s) — falling back to NumPy OLS.", exc)

    return _numpy_regression(df)


# ---------------------------------------------------------------------------
# rpy2 implementation
# ---------------------------------------------------------------------------

def _rpy2_regression(df: pd.DataFrame) -> dict[str, Any]:
    """Use rpy2 to fit the model inside an embedded R session."""
    import rpy2.robjects as ro  # type: ignore[import]
    from rpy2.robjects import pandas2ri  # type: ignore[import]
    from rpy2.robjects.packages import importr  # type: ignore[import]

    pandas2ri.activate()
    stats = importr("stats")
    base  = importr("base")

    cols = ["fare_amount", "trip_distance", "trip_duration_mins"]
    clean = df[cols].dropna().sample(min(50_000, len(df)), random_state=42)
    r_df  = pandas2ri.py2rpy(clean)

    formula = ro.Formula("fare_amount ~ trip_distance + trip_duration_mins")
    model   = stats.lm(formula, data=r_df)
    summ    = base.summary(model)

    coefs     = dict(zip(summ.rx2("coefficients").rownames, summ.rx2("coefficients")))
    r_squared = float(summ.rx2("r.squared")[0])

    summary_text = str(base.capture_output(base.print_(summ))[0])
    print("\n--- R Linear Regression Summary ---")
    print(summary_text)

    return {
        "method":          "rpy2",
        "intercept":       float(coefs.get("(Intercept)", 0.0)),
        "coef_distance":   float(coefs.get("trip_distance", 0.0)),
        "coef_duration":   float(coefs.get("trip_duration_mins", 0.0)),
        "r_squared":       r_squared,
        "summary_text":    summary_text,
        "available":       True,
    }


# ---------------------------------------------------------------------------
# NumPy fallback
# ---------------------------------------------------------------------------

def _numpy_regression(df: pd.DataFrame) -> dict[str, Any]:
    """Pure-Python multiple OLS via NumPy when rpy2 is unavailable."""
    cols  = ["fare_amount", "trip_distance", "trip_duration_mins"]
    clean = df[cols].dropna()

    y = clean["fare_amount"].to_numpy(dtype=float)
    X = np.column_stack([
        np.ones(len(clean)),
        clean["trip_distance"].to_numpy(dtype=float),
        clean["trip_duration_mins"].to_numpy(dtype=float),
    ])

    coeffs, *_ = np.linalg.lstsq(X, y, rcond=None)
    intercept, coef_dist, coef_dur = (
        float(coeffs[0]), float(coeffs[1]), float(coeffs[2])
    )

    y_pred   = X @ coeffs
    ss_res   = float(np.sum((y - y_pred) ** 2))
    ss_tot   = float(np.sum((y - y.mean()) ** 2))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    summary_text = (
        "Multiple Linear Regression: fare_amount ~ trip_distance + trip_duration_mins\n"
        f"  (NumPy OLS — install rpy2+R for full R output)\n\n"
        f"  Intercept        : {intercept:>10.4f}\n"
        f"  trip_distance    : {coef_dist:>10.4f}\n"
        f"  trip_duration_mins: {coef_dur:>9.4f}\n"
        f"  R²               : {r_squared:>10.4f}\n"
        f"  n                : {len(clean):>10,}"
    )

    print("\n--- Linear Regression (NumPy fallback) ---")
    print(summary_text)

    return {
        "method":          "numpy_fallback",
        "intercept":       intercept,
        "coef_distance":   coef_dist,
        "coef_duration":   coef_dur,
        "r_squared":       r_squared,
        "summary_text":    summary_text,
        "available":       False,
    }
