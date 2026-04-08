"""
multimodal_parser.py — Extract text, images, tables, and metadata from HTML.

Returns a unified multimodal record per page:
    {
      "url":       str,
      "text":      str,
      "images":    [{"src": str, "alt": str}, ...],
      "tables":    [list[dict], ...],
      "metadata":  {"title": str, "description": str}
    }
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup
    import pandas as pd
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    logger.warning("beautifulsoup4 or pandas not installed.")


class MultimodalParser:
    """Parse HTML into a structured multimodal record.

    Parameters
    ----------
    base_url:
        Used to resolve relative image URLs.
    """

    def __init__(self, base_url: str = "") -> None:
        self.base_url = base_url.rstrip("/")

    def parse(self, page: dict[str, Any]) -> dict[str, Any]:
        """Parse a raw page dict from ``AsyncScraper`` into a multimodal record.

        Parameters
        ----------
        page:
            Dict with at least ``url`` and ``html`` keys.

        Returns
        -------
        Multimodal record dict.
        """
        url  = page.get("url", "")
        html = page.get("html", "")
        base: dict[str, Any] = {
            "url": url, "text": "", "images": [], "tables": [], "metadata": {}
        }

        if not _AVAILABLE or not html:
            return base

        soup = BeautifulSoup(html, "lxml")
        return {
            "url":      url,
            "text":     self._text(soup),
            "images":   self._images(soup),
            "tables":   self._tables(soup),
            "metadata": self._metadata(soup),
        }

    # ------------------------------------------------------------------

    def _text(self, soup: "BeautifulSoup") -> str:
        """Extract clean visible body text."""
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        raw = soup.get_text(separator=" ", strip=True)
        return re.sub(r"\s{2,}", " ", raw).strip()

    def _images(self, soup: "BeautifulSoup") -> list[dict[str, str]]:
        """Return list of {src, alt} for all <img> tags."""
        images = []
        for img in soup.find_all("img"):
            src = img.get("src", "")
            if not src or src.startswith("data:"):
                continue
            if src.startswith("/"):
                src = self.base_url + src
            elif not src.startswith("http"):
                src = self.base_url + "/" + src.lstrip("/")
            images.append({"src": src, "alt": img.get("alt", "").strip()})
        return images

    def _tables(self, soup: "BeautifulSoup") -> list[list[dict[str, str]]]:
        """Parse all <table> elements and return them as lists of row-dicts."""
        tables = []
        for tbl in soup.find_all("table"):
            try:
                rows   = tbl.find_all("tr")
                if not rows:
                    continue
                headers = [th.get_text(strip=True)
                           for th in (rows[0].find_all("th") or rows[0].find_all("td"))]
                if not headers:
                    continue
                records = []
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cells) == len(headers):
                        records.append(dict(zip(headers, cells)))
                if records:
                    tables.append(records)
            except Exception:  # noqa: BLE001
                pass
        return tables

    def _metadata(self, soup: "BeautifulSoup") -> dict[str, str]:
        """Extract page title and meta description."""
        title = ""
        t = soup.find("title")
        if t:
            title = t.get_text(strip=True)

        desc = ""
        m = soup.find("meta", attrs={"name": "description"})
        if m:
            desc = str(m.get("content", ""))

        return {"title": title, "description": desc}
