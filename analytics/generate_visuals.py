"""
analytics/generate_visuals.py — Generate static portfolio charts from TLC taxi data.

Charts saved to images/ (project root):
    fare_distribution.png
    trip_demand_by_hour.png
    revenue_by_hour.png
    payment_type_breakdown.png
    top_routes.png
    fare_prediction_actual_vs_predicted.png

Run:
    python analytics/generate_visuals.py

All charts fall back to synthetic data automatically if cleaned_taxi.parquet
is absent — no Kaggle credentials required.
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

import matplotlib
matplotlib.use("Agg")          # non-interactive backend; must precede pyplot import

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

# ── Project root (this file lives in analytics/, root is one level up) ────────
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import CLEANED_PARQUET
from analytics.sql_queries import create_connection, hourly_revenue, payment_breakdown
from analytics.r_bridge import run_r_regression

# ── Output directory ───────────────────────────────────────────────────────────
IMAGES_DIR = _ROOT / "images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

# ── Colour palette (consistent across all charts) ─────────────────────────────
BLUE       = "#1565C0"
LIGHT_BLUE = "#90CAF9"
AMBER      = "#E65100"
GREEN      = "#2E7D32"
RED        = "#C62828"
GREY       = "#9E9E9E"

PAYMENT_LABELS = {
    "1": "Credit Card",
    "2": "Cash",
    "3": "No Charge",
    "4": "Dispute",
}
PAYMENT_COLORS = {
    "Credit Card": BLUE,
    "Cash":        AMBER,
    "No Charge":   GREEN,
    "Dispute":     RED,
}

# ── Seaborn / Matplotlib defaults ─────────────────────────────────────────────
sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams.update({
    "figure.dpi":        150,
    "axes.titlesize":    13,
    "axes.labelsize":    11,
    "axes.titleweight":  "bold",
    "savefig.dpi":       150,
    "savefig.facecolor": "white",
    "savefig.bbox":      "tight",
})


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_data() -> pd.DataFrame:
    """Return cleaned TLC parquet, or a synthetic fallback if unavailable."""
    if CLEANED_PARQUET.exists():
        df = pd.read_parquet(CLEANED_PARQUET)
        print(f"  Source : {CLEANED_PARQUET.name}  ({len(df):,} rows)")
        return df

    print("  Source : synthetic fallback (run setup_data.py to use real TLC data)")
    import asyncio
    from agents.data_agent import DataAgent
    result = asyncio.run(DataAgent().run())
    return result["dataframe"]


def _save(fig: plt.Figure, name: str) -> Path:
    """Save *fig* to images/<name>, close it, and print its size."""
    out = IMAGES_DIR / name
    fig.savefig(out)
    plt.close(fig)
    print(f"  ✓  images/{name}  ({out.stat().st_size // 1024} KB)")
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Chart 1 — Fare Distribution
# ─────────────────────────────────────────────────────────────────────────────

def plot_fare_distribution(df: pd.DataFrame) -> Path:
    """Histogram (with mean/median markers) + ECDF side-by-side."""
    fares  = df["fare_amount"]
    mean_  = fares.mean()
    med_   = fares.median()

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("NYC Yellow Taxi — Fare Distribution", fontsize=15, fontweight="bold")

    # ── Left: histogram ───────────────────────────────────────────────────────
    axes[0].hist(fares, bins=50, color=BLUE, edgecolor="white", lw=0.4, alpha=0.85)
    axes[0].axvline(mean_, color=AMBER, lw=2.0, ls="--", label=f"Mean   ${mean_:.2f}")
    axes[0].axvline(med_,  color=GREEN, lw=2.0, ls=":",  label=f"Median ${med_:.2f}")
    axes[0].set_title("Distribution of Fare Amounts")
    axes[0].set_xlabel("Fare ($)")
    axes[0].set_ylabel("Number of Trips")
    axes[0].xaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.0f}"))
    axes[0].legend()

    # ── Right: ECDF ───────────────────────────────────────────────────────────
    sorted_f = np.sort(fares)
    pct      = np.arange(1, len(sorted_f) + 1) / len(sorted_f) * 100
    axes[1].plot(sorted_f, pct, color=BLUE, lw=2)
    for q, label, c in [
        (0.25, "Q1",       GREY),
        (0.50, "Median",   AMBER),
        (0.75, "Q3",       GREEN),
        (0.95, "95th pct", RED),
    ]:
        val = float(fares.quantile(q))
        axes[1].axvline(val, color=c, ls="--", lw=1.4, label=f"{label}  ${val:.0f}")
    axes[1].set_title("Cumulative Distribution of Fares")
    axes[1].set_xlabel("Fare ($)")
    axes[1].set_ylabel("Cumulative % of Trips")
    axes[1].xaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.0f}"))
    axes[1].yaxis.set_major_formatter(mticker.PercentFormatter())
    axes[1].legend(fontsize=9)

    plt.tight_layout()
    return _save(fig, "fare_distribution.png")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 2 — Trip Demand by Hour
# ─────────────────────────────────────────────────────────────────────────────

def plot_trip_demand_by_hour(hourly: pd.DataFrame) -> Path:
    """Bar chart of trip volume across all 24 hours; peak/trough highlighted."""
    peak_idx   = int(hourly["trip_count"].idxmax())
    trough_idx = int(hourly["trip_count"].idxmin())
    peak_row   = hourly.loc[peak_idx]
    trough_row = hourly.loc[trough_idx]

    colors = [
        BLUE if i == peak_idx else (RED if i == trough_idx else LIGHT_BLUE)
        for i in hourly.index
    ]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(
        hourly["hour_of_day"], hourly["trip_count"],
        color=colors, edgecolor="white", lw=0.4, width=0.75,
    )
    ax.set_title("NYC Yellow Taxi — Trip Demand by Hour of Day", pad=12)
    ax.set_xlabel("Hour of Day  (0 = midnight)")
    ax.set_ylabel("Number of Trips")
    ax.set_xticks(range(24))
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))

    # Annotations
    peak_h = int(peak_row["hour_of_day"])
    peak_v = int(peak_row["trip_count"])
    trough_h = int(trough_row["hour_of_day"])
    trough_v = int(trough_row["trip_count"])
    y_range  = hourly["trip_count"].max() - hourly["trip_count"].min()
    nudge    = max(y_range * 0.15, 10)   # proportional offset so label never overlaps

    ax.annotate(
        f"Peak\n{peak_h}:00  ({peak_v:,})",
        xy=(peak_h, peak_v),
        xytext=(peak_h + 1.8, peak_v + nudge),
        fontsize=9, color=BLUE, fontweight="bold",
        arrowprops=dict(arrowstyle="->", color=BLUE, lw=1.2),
    )
    ax.annotate(
        f"Trough\n{trough_h}:00  ({trough_v:,})",
        xy=(trough_h, trough_v),
        xytext=(trough_h + 1.8, trough_v + nudge),
        fontsize=9, color=RED, fontweight="bold",
        arrowprops=dict(arrowstyle="->", color=RED, lw=1.2),
    )

    from matplotlib.patches import Patch
    ax.legend(
        handles=[
            Patch(color=BLUE,       label="Peak hour"),
            Patch(color=LIGHT_BLUE, label="Other hours"),
            Patch(color=RED,        label="Trough hour"),
        ],
        loc="lower right",
    )

    plt.tight_layout()
    return _save(fig, "trip_demand_by_hour.png")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 3 — Revenue by Hour
# ─────────────────────────────────────────────────────────────────────────────

def plot_revenue_by_hour(hourly: pd.DataFrame) -> Path:
    """Dual-axis: total revenue bars (left axis) + avg fare line (right axis)."""
    fig, ax1 = plt.subplots(figsize=(12, 5))

    ax1.bar(
        hourly["hour_of_day"], hourly["total_revenue"],
        color=GREEN, edgecolor="white", lw=0.4, width=0.75, alpha=0.85,
        label="Total Revenue",
    )
    ax1.set_title("NYC Yellow Taxi — Revenue and Average Fare by Hour of Day", pad=12)
    ax1.set_xlabel("Hour of Day  (0 = midnight)")
    ax1.set_ylabel("Total Revenue ($)", color=GREEN)
    ax1.tick_params(axis="y", labelcolor=GREEN)
    ax1.set_xticks(range(24))
    ax1.yaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.0f}"))

    ax2 = ax1.twinx()
    ax2.plot(
        hourly["hour_of_day"], hourly["avg_fare"],
        color=AMBER, lw=2.5, marker="o", ms=5, label="Avg Fare",
    )
    ax2.set_ylabel("Average Fare ($)", color=AMBER)
    ax2.tick_params(axis="y", labelcolor=AMBER)
    ax2.yaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.2f}"))

    # Unified legend
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="lower right")

    plt.tight_layout()
    return _save(fig, "revenue_by_hour.png")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 4 — Payment Type Breakdown
# ─────────────────────────────────────────────────────────────────────────────

def plot_payment_type_breakdown(pay: pd.DataFrame) -> Path:
    """3-panel horizontal bar: trip share · avg fare · revenue share."""
    pay = pay.copy()
    pay["payment_label"] = (
        pay["payment_type"].astype(str)
        .map(PAYMENT_LABELS)
        .fillna(pay["payment_type"])
    )
    pay["pct_revenue"] = (
        pay["total_revenue"] / pay["total_revenue"].sum() * 100
    ).round(1)
    # Sort ascending so the longest bar is at the top of the horizontal chart
    pay = pay.sort_values("trip_count", ascending=True).reset_index(drop=True)

    labels  = pay["payment_label"].tolist()
    palette = [PAYMENT_COLORS.get(lbl, GREY) for lbl in labels]
    y_pos   = range(len(labels))

    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    fig.suptitle("NYC Yellow Taxi — Payment Type Analysis", fontsize=14, fontweight="bold")

    def _panel(ax, values, title, xlabel, fmt_fn):
        bars = ax.barh(y_pos, values, color=palette, edgecolor="white", lw=0.4, height=0.6)
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(labels)
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_xlim(0, max(values) * 1.22)
        for bar, val in zip(bars, values):
            ax.text(
                bar.get_width() + max(values) * 0.01,
                bar.get_y() + bar.get_height() / 2,
                fmt_fn(val),
                va="center", fontsize=9,
            )

    _panel(axes[0], pay["pct_trips"].tolist(),   "Share of Trips",    "% of Trips",    lambda v: f"{v:.1f}%")
    _panel(axes[1], pay["avg_fare"].tolist(),     "Average Fare",      "Avg Fare ($)",  lambda v: f"${v:.2f}")
    _panel(axes[2], pay["pct_revenue"].tolist(),  "Share of Revenue",  "% of Revenue",  lambda v: f"{v:.1f}%")

    plt.tight_layout()
    return _save(fig, "payment_type_breakdown.png")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 5 — Top Pickup Zones (routes)
# ─────────────────────────────────────────────────────────────────────────────

def plot_top_routes(df: pd.DataFrame) -> Path:
    """Side-by-side: top 12 pickup zones by volume vs. top 12 by avg fare."""
    zone_stats = (
        df.groupby("pulocationid")
        .agg(trips=("fare_amount", "count"), avg_fare=("fare_amount", "mean"))
        .reset_index()
        .round({"avg_fare": 2})
    )

    top_vol  = zone_stats.nlargest(12, "trips").sort_values("trips")
    top_fare = zone_stats.nlargest(12, "avg_fare").sort_values("avg_fare")
    label    = lambda z: f"Zone {int(z)}"

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("NYC Yellow Taxi — Top Pickup Zones", fontsize=14, fontweight="bold")

    # Left: by volume
    axes[0].barh(
        [label(z) for z in top_vol["pulocationid"]],
        top_vol["trips"],
        color=BLUE, edgecolor="white", lw=0.4, height=0.7,
    )
    axes[0].set_title("By Trip Volume  (Top 12)")
    axes[0].set_xlabel("Number of Trips")
    axes[0].xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
    for i, (_, row) in enumerate(top_vol.iterrows()):
        axes[0].text(
            row["trips"] + top_vol["trips"].max() * 0.01, i,
            f"{int(row['trips']):,}", va="center", fontsize=9,
        )
    axes[0].set_xlim(0, top_vol["trips"].max() * 1.18)

    # Right: by avg fare
    axes[1].barh(
        [label(z) for z in top_fare["pulocationid"]],
        top_fare["avg_fare"],
        color=AMBER, edgecolor="white", lw=0.4, height=0.7,
    )
    axes[1].set_title("By Average Fare  (Top 12)")
    axes[1].set_xlabel("Average Fare ($)")
    axes[1].xaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.2f}"))
    for i, (_, row) in enumerate(top_fare.iterrows()):
        axes[1].text(
            row["avg_fare"] + top_fare["avg_fare"].max() * 0.01, i,
            f"${row['avg_fare']:.2f}", va="center", fontsize=9,
        )
    axes[1].set_xlim(0, top_fare["avg_fare"].max() * 1.18)

    plt.tight_layout()
    return _save(fig, "top_routes.png")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 6 — Fare Prediction: Actual vs Predicted + Residuals
# ─────────────────────────────────────────────────────────────────────────────

def plot_fare_prediction(df: pd.DataFrame) -> Path | None:
    """Actual vs predicted scatter + residuals diagnostics from OLS regression."""
    try:
        res = run_r_regression(df)
    except Exception as exc:
        print(f"    Skipping (regression failed): {exc}")
        return None

    sample = df[["trip_distance", "trip_duration_mins", "fare_amount"]].dropna()
    y      = sample["fare_amount"].to_numpy(dtype=float)
    X      = np.column_stack([
        np.ones(len(sample)),
        sample["trip_distance"].to_numpy(dtype=float),
        sample["trip_duration_mins"].to_numpy(dtype=float),
    ])
    coeffs = np.array([res["intercept"], res["coef_distance"], res["coef_duration"]])
    y_pred = X @ coeffs
    resid  = y - y_pred
    sigma  = resid.std()

    # Sub-sample for readability without losing chart density
    rng = np.random.default_rng(42)
    idx = rng.choice(len(y), size=min(8_000, len(y)), replace=False)
    r2  = res["r_squared"]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(
        f"Fare Prediction — OLS Regression  "
        f"(β_dist = ${res['coef_distance']:.2f}/mi  ·  "
        f"β_dur = ${res['coef_duration']:.2f}/min  ·  "
        f"R² = {r2:.3f})",
        fontsize=11, fontweight="bold",
    )

    # Left: Actual vs Predicted
    lim = (y.min() - 2, y.max() + 2)
    axes[0].scatter(y[idx], y_pred[idx], alpha=0.20, s=6, color=BLUE, label="Trips")
    axes[0].plot(lim, lim, color=RED, lw=1.5, ls="--", label="Perfect fit")
    axes[0].set_xlim(lim)
    axes[0].set_ylim(lim)
    axes[0].set_title("Actual vs Predicted Fare")
    axes[0].set_xlabel("Actual Fare ($)")
    axes[0].set_ylabel("Predicted Fare ($)")
    axes[0].xaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.0f}"))
    axes[0].yaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.0f}"))
    axes[0].legend()

    # Right: Residuals
    axes[1].scatter(y_pred[idx], resid[idx], alpha=0.20, s=6, color=AMBER)
    axes[1].axhline(0,         color=RED,  lw=1.5, ls="--", label="Zero residual")
    axes[1].axhline( 2 * sigma, color=GREY, lw=1.2, ls=":",  label=f"±2σ  (${2*sigma:.2f})")
    axes[1].axhline(-2 * sigma, color=GREY, lw=1.2, ls=":")
    axes[1].set_title("Residuals vs Predicted Fare")
    axes[1].set_xlabel("Predicted Fare ($)")
    axes[1].set_ylabel("Residual  (Actual − Predicted)  ($)")
    axes[1].xaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.0f}"))
    axes[1].legend()

    plt.tight_layout()
    return _save(fig, "fare_prediction_actual_vs_predicted.png")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "─" * 60)
    print("  generate_visuals.py — Portfolio Chart Generator")
    print("─" * 60)
    print(f"  Output : images/  (project root)\n")

    # ── Load data + run shared SQL queries once ────────────────────────────────
    df      = _load_data()
    conn    = create_connection(df)
    hourly  = hourly_revenue(conn)
    pay_df  = payment_breakdown(conn)
    conn.close()
    print()

    # ── Run each chart function, catching failures individually ───────────────
    chart_fns = [
        ("Fare Distribution",          lambda: plot_fare_distribution(df)),
        ("Trip Demand by Hour",        lambda: plot_trip_demand_by_hour(hourly)),
        ("Revenue by Hour",            lambda: plot_revenue_by_hour(hourly)),
        ("Payment Type Breakdown",     lambda: plot_payment_type_breakdown(pay_df)),
        ("Top Routes (Pickup Zones)",  lambda: plot_top_routes(df)),
        ("Fare Prediction",            lambda: plot_fare_prediction(df)),
    ]

    saved, failed = 0, 0
    for label, fn in chart_fns:
        print(f"  [{label}]")
        try:
            result = fn()
            if result is not None:
                saved += 1
        except Exception:
            failed += 1
            print("    ✗  Unexpected error:")
            traceback.print_exc()
        print()

    print("─" * 60)
    print(f"  {saved} chart(s) saved  ·  {failed} failed")
    print("─" * 60 + "\n")


if __name__ == "__main__":
    main()
