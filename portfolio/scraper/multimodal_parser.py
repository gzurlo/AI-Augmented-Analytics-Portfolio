"""
multimodal_parser.py — Extracts text, images, tables, and JSON-LD from HTML.

For each scraped page this parser produces a unified *multimodal record* dict:

::

    {
        "url":    str,
        "text":   str,            # clean body text
        "images": [{"src": str, "alt": str}, ...],
        "tables": [list[dict], ...],   # each table as list-of-row-dicts
        "json_ld": [dict, ...],        # structured data blocks if present
        "metadata": {
            "title":       str,
            "description": str,
            "h1s":         [str, ...],
            "word_count":  int,
        },
    }
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup, Tag
    import pandas as pd
    _PARSER_AVAILABLE = True
except ImportError:
    _PARSER_AVAILABLE = False
    logger.warning(
        "beautifulsoup4 or pandas not installed. "
        "Run: pip install beautifulsoup4 lxml pandas"
    )


class MultimodalParser:
    """Extract multimodal content from a raw HTML string.

    All extraction methods are tolerant of missing elements — each returns an
    empty value rather than raising an exception.

    Parameters
    ----------
    base_url:
        Used to resolve relative image/link URLs.
    """

    def __init__(self, base_url: str = "") -> None:
        self.base_url = base_url.rstrip("/")

    def parse(self, page: dict[str, Any]) -> dict[str, Any]:
        """Parse a page dict (as returned by ``AsyncScraper``) into a
        multimodal record.

        Parameters
        ----------
        page:
            Must contain at minimum keys ``url`` and ``html``.

        Returns
        -------
        Multimodal record dict.
        """
        url = page.get("url", "")
        html = page.get("html", "")

        if not _PARSER_AVAILABLE or not html:
            return {
                "url": url,
                "text": "",
                "images": [],
                "tables": [],
                "json_ld": [],
                "metadata": {},
            }

        soup = BeautifulSoup(html, "lxml")

        return {
            "url": url,
            "text": self._extract_text(soup),
            "images": self._extract_images(soup),
            "tables": self._extract_tables(soup),
            "json_ld": self._extract_json_ld(soup),
            "metadata": self._extract_metadata(soup),
        }

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _extract_text(self, soup: "BeautifulSoup") -> str:
        """Return clean visible text from the page body."""
        # Remove script / style nodes before extracting text
        for tag in soup(["script", "style", "noscript", "head"]):
            tag.decompose()
        raw = soup.get_text(separator=" ", strip=True)
        # Collapse whitespace
        return re.sub(r"\s{2,}", " ", raw).strip()

    def _extract_images(self, soup: "BeautifulSoup") -> list[dict[str, str]]:
        """Return a list of ``{src, alt}`` dicts for all ``<img>`` tags."""
        images: list[dict[str, str]] = []
        for img in soup.find_all("img"):
            src = img.get("src", "")
            if src and not src.startswith("data:"):
                if src.startswith("/"):
                    src = self.base_url + src
                images.append({"src": src, "alt": img.get("alt", "").strip()})
        return images

    def _extract_tables(self, soup: "BeautifulSoup") -> list[list[dict[str, str]]]:
        """Parse all HTML tables and return them as lists of row dicts."""
        tables: list[list[dict[str, str]]] = []
        for table_tag in soup.find_all("table"):
            try:
                rows = table_tag.find_all("tr")
                if not rows:
                    continue
                # Use first row as header
                headers = [
                    th.get_text(strip=True)
                    for th in (rows[0].find_all("th") or rows[0].find_all("td"))
                ]
                if not headers:
                    continue
                table_records: list[dict[str, str]] = []
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cells) == len(headers):
                        table_records.append(dict(zip(headers, cells)))
                if table_records:
                    tables.append(table_records)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Table parse error: %s", exc)
        return tables

    def _extract_json_ld(self, soup: "BeautifulSoup") -> list[dict[str, Any]]:
        """Find and parse all ``<script type="application/ld+json">`` blocks."""
        blocks: list[dict[str, Any]] = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, list):
                    blocks.extend(data)
                elif isinstance(data, dict):
                    blocks.append(data)
            except json.JSONDecodeError:
                pass
        return blocks

    def _extract_metadata(self, soup: "BeautifulSoup") -> dict[str, Any]:
        """Extract title, meta description, H1 headings, and word count."""
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""

        desc_tag = soup.find("meta", attrs={"name": "description"})
        description = ""
        if desc_tag and isinstance(desc_tag, Tag):
            description = desc_tag.get("content", "")  # type: ignore[assignment]

        h1s = [h.get_text(strip=True) for h in soup.find_all("h1")]

        body_text = soup.get_text(separator=" ", strip=True)
        word_count = len(body_text.split())

        return {
            "title": title,
            "description": str(description),
            "h1s": h1s,
            "word_count": word_count,
        }
