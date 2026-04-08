"""
java_bridge.py — Call DataProcessor.java from Python via subprocess + JSON.

If ``javac`` / ``java`` are absent, prints a clear message and returns a
mock result so demo.py can continue without crashing.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import JAVA_SOURCE, JAVA_CLASS_DIR

logger = logging.getLogger(__name__)

JAVA_INSTALL_URL = "https://adoptium.net"


def compile_if_needed() -> bool:
    """Compile DataProcessor.java if the .class file is missing.

    Returns
    -------
    True if compilation succeeded (or was already done), False if javac
    is unavailable.
    """
    class_file = JAVA_CLASS_DIR / "DataProcessor.class"
    if class_file.exists():
        return True

    javac = shutil.which("javac")
    if javac is None:
        print(
            f"Java JDK required. Install from {JAVA_INSTALL_URL}\n"
            "Then re-run demo.py."
        )
        return False

    result = subprocess.run(
        [javac, str(JAVA_SOURCE), "-d", str(JAVA_CLASS_DIR)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("javac failed:\n%s", result.stderr)
        return False

    logger.info("DataProcessor.java compiled successfully.")
    return True


def process(values: list[float]) -> dict[str, Any]:
    """Normalise a numeric array using the compiled Java DataProcessor.

    Compiles on first call if needed.  Returns a mock result if Java is
    not available.

    Parameters
    ----------
    values:
        List of floats to normalise (min-max).

    Returns
    -------
    dict with keys: min, max, mean, std, input_size, normalized.
    """
    if not compile_if_needed():
        return _mock_result(values)

    java = shutil.which("java")
    if java is None:
        print(f"'java' not found. Install from {JAVA_INSTALL_URL}")
        return _mock_result(values)

    payload = json.dumps({"values": values})

    try:
        result = subprocess.run(
            [java, "-cp", str(JAVA_CLASS_DIR), "DataProcessor"],
            input=payload,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        logger.error("Java process timed out.")
        return _mock_result(values)

    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip()
        logger.error("Java process failed: %s", err)
        return _mock_result(values)

    stdout = result.stdout.strip()
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        logger.error("Java output is not valid JSON: %r", stdout)
        return _mock_result(values)


def _mock_result(values: list[float]) -> dict[str, Any]:
    """Pure-Python fallback that mirrors the Java output format."""
    import numpy as np
    arr = np.array(values, dtype=float)
    mn, mx, mean, std = float(arr.min()), float(arr.max()), float(arr.mean()), float(arr.std())
    rng  = mx - mn
    norm = ((arr - mn) / rng).tolist() if rng > 0 else [0.0] * len(values)
    return {
        "min": mn, "max": mx, "mean": mean, "std": std,
        "input_size": len(values),
        "normalized": [round(v, 6) for v in norm],
        "_source": "python_fallback",
    }
