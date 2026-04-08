"""
java_bridge.py — Call a compiled Java class from Python via subprocess + JSON.

Protocol
--------
* Python serialises a payload dict to JSON and writes it to the Java process'
  stdin.
* The Java process writes a JSON result to stdout.
* Python reads and deserialises that result back into a Python dict.

Graceful fallback
-----------------
If ``java`` or ``javac`` is not on the PATH, the bridge raises a descriptive
``JavaNotFoundError`` that the demo catches and reports without crashing.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

from config import JAVA_SOURCE, JAVA_CLASS_DIR, JAVA_CLASS_NAME

logger = logging.getLogger(__name__)


class JavaNotFoundError(RuntimeError):
    """Raised when the ``java`` or ``javac`` executable cannot be found."""


class JavaBridge:
    """Compile and invoke a Java class via subprocess, exchanging JSON data.

    Parameters
    ----------
    source_file:
        Path to the ``.java`` source file to compile.
    class_dir:
        Directory that receives the compiled ``.class`` file.
    class_name:
        Fully-qualified Java class name (no package prefix needed here).
    """

    def __init__(
        self,
        source_file: Path = JAVA_SOURCE,
        class_dir: Path = JAVA_CLASS_DIR,
        class_name: str = JAVA_CLASS_NAME,
    ) -> None:
        self.source_file = source_file
        self.class_dir = class_dir
        self.class_name = class_name
        self._compiled = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def call(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Send *payload* to the Java process and return the parsed result.

        Compiles the Java source on the first call if not already compiled.

        Parameters
        ----------
        payload:
            Python dict to serialise as JSON and pass via stdin.

        Returns
        -------
        Python dict parsed from the Java process' stdout.

        Raises
        ------
        JavaNotFoundError
            If ``javac`` / ``java`` are not available on the PATH.
        RuntimeError
            If the Java process exits with a non-zero status.
        """
        self._ensure_compiled()
        return self._invoke(payload)

    def batch_normalize(self, data: list[float]) -> dict[str, Any]:
        """Convenience wrapper: run z-score normalisation on a numeric list.

        Parameters
        ----------
        data:
            List of floats to normalise.

        Returns
        -------
        Result dict from ``DataProcessor`` with keys:
        ``mean``, ``std_dev``, ``min``, ``max``, ``normalized``.
        """
        return self.call({"operation": "batch_normalize", "data": data})

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _ensure_compiled(self) -> None:
        """Compile the Java source file if the .class file is absent."""
        class_file = self.class_dir / f"{self.class_name}.class"
        if self._compiled or class_file.exists():
            self._compiled = True
            return

        javac = self._find_executable("javac")
        logger.info("JavaBridge: compiling %s", self.source_file)

        result = subprocess.run(
            [javac, str(self.source_file), "-d", str(self.class_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"javac failed (exit {result.returncode}):\n{result.stderr}"
            )
        self._compiled = True
        logger.info("JavaBridge: compilation successful")

    def _invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Run the compiled Java class and return the parsed output."""
        java = self._find_executable("java")
        stdin_data = json.dumps(payload)

        logger.debug("JavaBridge: stdin → %s", stdin_data)
        result = subprocess.run(
            [java, "-cp", str(self.class_dir), self.class_name],
            input=stdin_data,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            err = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(
                f"Java process exited with code {result.returncode}: {err}"
            )

        stdout = result.stdout.strip()
        logger.debug("JavaBridge: stdout ← %s", stdout)

        try:
            return json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Java output is not valid JSON: {exc}\nRaw output: {stdout!r}"
            ) from exc

    @staticmethod
    def _find_executable(name: str) -> str:
        """Return the path to ``name`` or raise ``JavaNotFoundError``."""
        import shutil
        path = shutil.which(name)
        if path is None:
            raise JavaNotFoundError(
                f"'{name}' not found on PATH.\n"
                "Please install Java (JDK 11+): https://adoptium.net/\n"
                "The Java interop section of the demo will be skipped."
            )
        return path
