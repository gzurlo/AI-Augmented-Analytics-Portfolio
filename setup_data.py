"""
setup_data.py — Download the TLC Yellow Taxi dataset and stage it to data/raw/.

Usage
-----
    python setup_data.py

Requirements
------------
    pip install kagglehub

The script is idempotent: if files already exist in data/raw/ it prints their
sizes and skips the download.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

# Ensure project root is importable from this script
_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import DATA_RAW, DATASET_NAME


def main() -> None:
    """Download the TLC dataset and copy files to data/raw/."""
    # ── Check for existing files ─────────────────────────────────────────────
    existing = list(DATA_RAW.glob("*.parquet")) + list(DATA_RAW.glob("*.csv"))
    if existing:
        print(f"data/raw/ already contains {len(existing)} file(s) — skipping download.\n")
        _print_file_summary(existing)
        return

    # ── Check kagglehub is available ─────────────────────────────────────────
    try:
        import kagglehub  # type: ignore[import]
    except ImportError:
        print("kagglehub is not installed.")
        print("Run: pip install kagglehub")
        print("Then re-run: python setup_data.py")
        sys.exit(0)

    # ── Download ──────────────────────────────────────────────────────────────
    print(f"Downloading dataset: {DATASET_NAME}")
    print("(This may take a few minutes on first run — files are cached locally)\n")

    try:
        cache_path = kagglehub.dataset_download(DATASET_NAME)
    except Exception as exc:  # noqa: BLE001
        print(f"Download failed: {exc}")
        print("\nTroubleshooting:")
        print("  1. Set up Kaggle API credentials: https://github.com/Kaggle/kagglehub")
        print("  2. Accept dataset terms at: https://www.kaggle.com/datasets/" + DATASET_NAME)
        sys.exit(1)

    print(f"Path to dataset files: {cache_path}\n")

    # ── Copy to data/raw/ ─────────────────────────────────────────────────────
    cache = Path(cache_path)
    copied: list[Path] = []

    for pattern in ("*.parquet", "*.csv"):
        for src in sorted(cache.rglob(pattern)):
            dst = DATA_RAW / src.name
            if not dst.exists():
                shutil.copy2(src, dst)
                copied.append(dst)
            else:
                copied.append(dst)

    if not copied:
        print(f"WARNING: No .parquet or .csv files found under {cache_path}")
        print("Check the dataset structure manually.")
        sys.exit(1)

    print(f"Staged {len(copied)} file(s) to data/raw/\n")
    _print_file_summary(copied)


def _print_file_summary(files: list[Path]) -> None:
    """Print name, size, and estimated row count for each file."""
    try:
        import pandas as pd
    except ImportError:
        pd = None  # type: ignore[assignment]

    print(f"{'File':<45} {'Size':>10}  {'Est. rows':>12}")
    print("-" * 72)

    for f in sorted(files):
        size_mb = f.stat().st_size / (1024 * 1024)
        row_info = "—"
        if pd is not None:
            try:
                if f.suffix == ".parquet":
                    meta = pd.read_parquet(f, columns=[]).shape
                    row_info = f"{meta[0]:,}"
                elif f.suffix == ".csv":
                    # Count rows cheaply
                    with f.open("rb") as fh:
                        row_info = f"{sum(1 for _ in fh) - 1:,}"
            except Exception:  # noqa: BLE001
                row_info = "?"
        print(f"  {f.name:<43} {size_mb:>8.1f} MB  {row_info:>12}")

    print()


if __name__ == "__main__":
    main()
