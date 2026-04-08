"""
dataset_builder.py — Persist multimodal records to a JSONL dataset.

Each line of ``data/multimodal_dataset.jsonl`` contains one JSON object with
keys: ``url``, ``text``, ``images``, ``tables``, ``metadata``.

The builder appends by default so incremental crawls accumulate records
without duplicating previously saved pages.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Sequence

from config import MULTIMODAL_DATASET_PATH

logger = logging.getLogger(__name__)


class DatasetBuilder:
    """Persists a list of multimodal page records to a JSONL file.

    Parameters
    ----------
    output_path:
        Destination ``.jsonl`` file.
    append:
        If True (default), append to an existing file rather than overwriting.
    """

    def __init__(
        self,
        output_path: Path = MULTIMODAL_DATASET_PATH,
        append: bool = True,
    ) -> None:
        self.output_path = output_path
        self.append = append

    def save(self, records: Sequence[dict[str, Any]]) -> int:
        """Write records to disk in JSONL format.

        Each record is serialised to a single JSON line.  Non-serialisable
        values (e.g. pandas DataFrames inside ``tables``) are converted to
        their ``list`` representation first.

        Parameters
        ----------
        records:
            Iterable of multimodal record dicts.

        Returns
        -------
        Number of records written.
        """
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if self.append else "w"

        written = 0
        with self.output_path.open(mode, encoding="utf-8") as fh:
            for record in records:
                line = self._serialise_record(record)
                fh.write(line + "\n")
                written += 1

        logger.info(
            "DatasetBuilder: wrote %d records to %s", written, self.output_path
        )
        return written

    def load(self) -> list[dict[str, Any]]:
        """Load and parse all records from the JSONL file.

        Returns
        -------
        List of record dicts; empty list if the file does not exist.
        """
        if not self.output_path.exists():
            return []
        records: list[dict[str, Any]] = []
        with self.output_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        logger.warning("Skipping malformed JSONL line: %s", exc)
        return records

    @property
    def record_count(self) -> int:
        """Return the number of records currently in the dataset file."""
        if not self.output_path.exists():
            return 0
        count = 0
        with self.output_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    count += 1
        return count

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _serialise_record(record: dict[str, Any]) -> str:
        """Convert a multimodal record to a JSON string.

        Handles pandas DataFrames and other non-standard types that may
        appear inside ``tables``.
        """
        def default_encoder(obj: Any) -> Any:
            # pandas DataFrame → list of row dicts
            try:
                import pandas as pd
                if isinstance(obj, pd.DataFrame):
                    return obj.to_dict(orient="records")
            except ImportError:
                pass
            # Fallback: convert to string
            return str(obj)

        return json.dumps(record, ensure_ascii=False, default=default_encoder)
