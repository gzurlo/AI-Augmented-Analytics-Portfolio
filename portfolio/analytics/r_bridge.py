"""
r_bridge.py — Python ↔ R bridge using rpy2.

What this module does
---------------------
* Fits a simple linear regression (``lm``) of revenue ~ quantity on the
  sales DataFrame using R's base stats package.
* Prints the R summary output (coefficients, R², p-values) as text.
* Falls back gracefully with an informative message when rpy2 / R is absent.

Setup instructions
------------------
1. Install R:          https://cran.r-project.org/
2. Install rpy2:       ``pip install rpy2``
3. (Optional) ggplot2: ``Rscript -e "install.packages('ggplot2')"``

The bridge is deliberately kept to the standard ``stats`` package so it works
without any additional R package installation.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def run_r_analysis(df: pd.DataFrame) -> dict[str, Any]:
    """Run a linear regression and descriptive statistics in R via rpy2.

    Falls back to a pure-Python NumPy OLS implementation when rpy2 / R is
    not available, so the rest of the demo always succeeds.

    Parameters
    ----------
    df:
        Must contain numeric columns ``revenue`` and ``quantity``.

    Returns
    -------
    dict with keys:
        ``method``           – "rpy2" or "numpy_fallback"
        ``intercept``        – float
        ``slope``            – float (coefficient on quantity)
        ``r_squared``        – float
        ``summary_text``     – str (R-style summary or formatted table)
        ``available``        – bool (True if rpy2 ran successfully)
    """
    try:
        return _run_with_rpy2(df)
    except ImportError:
        logger.warning(
            "rpy2 is not installed.  Install with: pip install rpy2\n"
            "Falling back to NumPy OLS."
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("R/rpy2 raised an error (%s).  Using NumPy fallback.", exc)

    return _run_numpy_fallback(df)


# ---------------------------------------------------------------------------
# rpy2 implementation
# ---------------------------------------------------------------------------

def _run_with_rpy2(df: pd.DataFrame) -> dict[str, Any]:
    """Execute linear regression inside an embedded R session."""
    import rpy2.robjects as ro  # type: ignore[import]
    from rpy2.robjects import pandas2ri  # type: ignore[import]
    from rpy2.robjects.packages import importr  # type: ignore[import]

    pandas2ri.activate()

    base = importr("base")
    stats = importr("stats")

    # Transfer the DataFrame into R
    r_df = pandas2ri.py2rpy(df[["revenue", "quantity"]].dropna())

    # Fit the model: revenue ~ quantity
    formula = ro.Formula("revenue ~ quantity")
    model = stats.lm(formula, data=r_df)
    summ = base.summary(model)

    # Extract coefficients
    coefs = dict(zip(summ.rx2("coefficients").rownames, summ.rx2("coefficients")))
    intercept = float(coefs.get("(Intercept)", 0.0))
    slope = float(coefs.get("quantity", 0.0))
    r_squared = float(summ.rx2("r.squared")[0])

    # Get full summary text
    old_warn = ro.r("options(warn=-1)")  # noqa: F841
    summary_text = str(base.capture_output(base.print_(summ))[0])

    print("\n--- R Linear Regression Summary ---")
    print(summary_text)

    return {
        "method": "rpy2",
        "intercept": intercept,
        "slope": slope,
        "r_squared": r_squared,
        "summary_text": summary_text,
        "available": True,
    }


# ---------------------------------------------------------------------------
# NumPy fallback implementation
# ---------------------------------------------------------------------------

def _run_numpy_fallback(df: pd.DataFrame) -> dict[str, Any]:
    """Pure-Python OLS using NumPy when R is not available."""
    clean = df[["revenue", "quantity"]].dropna()
    x = clean["quantity"].to_numpy(dtype=float)
    y = clean["revenue"].to_numpy(dtype=float)

    # OLS: [intercept, slope] via normal equations
    x_mat = np.column_stack([np.ones_like(x), x])
    coeffs, *_ = np.linalg.lstsq(x_mat, y, rcond=None)
    intercept, slope = float(coeffs[0]), float(coeffs[1])

    y_pred = intercept + slope * x
    ss_res = float(np.sum((y - y_pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    summary_text = (
        f"Linear Regression: revenue ~ quantity  (NumPy OLS fallback)\n"
        f"  Intercept : {intercept:>12.4f}\n"
        f"  Slope     : {slope:>12.4f}\n"
        f"  R²        : {r_squared:>12.4f}\n"
        f"  n         : {len(x):>12,}\n"
        f"\n  (Install rpy2 + R for the full R summary output)"
    )

    print("\n--- Linear Regression Summary (NumPy fallback) ---")
    print(summary_text)

    return {
        "method": "numpy_fallback",
        "intercept": intercept,
        "slope": slope,
        "r_squared": r_squared,
        "summary_text": summary_text,
        "available": False,
    }
