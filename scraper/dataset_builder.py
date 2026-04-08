"""
dataset_builder.py — Persist multimodal records to a JSONL file.

Each line of the output file is one JSON record with keys:
    url, text, images, tables (as list of dicts), metadata.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Sequence

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import MULTIMODAL_JSONL

logger = logging.getLogger(__name__)


def build_dataset(
    records: Sequence[dict[str, Any]],
    output_path: Path = MULTIMODAL_JSONL,
) -> int:
    """Serialise multimodal records to a JSONL file.

    Parameters
    ----------
    records:
        Iterable of multimodal record dicts (as returned by ``MultimodalParser``).
    output_path:
        Destination ``.jsonl`` file.

    Returns
    -------
    Number of records written.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output_path.open("w", encoding="utf-8") as fh:
        for rec in records:
            line = _serialise(rec)
            fh.write(line + "\n")
            written += 1
    logger.info("DatasetBuilder: wrote %d records to %s", written, output_path)
    return written


def _serialise(record: dict[str, Any]) -> str:
    """Convert a record to a JSON string, handling non-standard types."""
    def _default(obj: Any) -> Any:
        try:
            import pandas as pd
            if isinstance(obj, pd.DataFrame):
                return obj.to_dict(orient="records")
        except ImportError:
            pass
        return str(obj)

    return json.dumps(record, ensure_ascii=False, default=_default)
