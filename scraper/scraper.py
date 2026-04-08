"""
scraper.py — Async web scraper targeting https://books.toscrape.com.

Respects a 0.5-second delay between requests.  Handles timeouts and HTTP
errors gracefully by returning an error record rather than raising.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

try:
    import httpx
    from bs4 import BeautifulSoup
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    logger.warning("httpx or beautifulsoup4 not installed. Run: pip install httpx beautifulsoup4 lxml")

BASE_URL = "https://books.toscrape.com"
USER_AGENT = "Mozilla/5.0 (compatible; PortfolioBot/1.0; +educational-use)"


class AsyncScraper:
    """Async HTTP scraper for books.toscrape.com.

    Parameters
    ----------
    base_url:
        Root URL to crawl from.
    delay:
        Seconds to wait between requests.
    timeout:
        Per-request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        delay: float = 0.5,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.delay    = delay
        self.timeout  = timeout

    async def scrape_pages(self, n: int = 5) -> list[dict[str, Any]]:
        """Fetch n pages starting from the base URL.

        Parameters
        ----------
        n:
            Maximum number of pages to retrieve.

        Returns
        -------
        List of dicts with keys: url, html, status, error, fetch_time_s.
        """
        if not _AVAILABLE:
            logger.error("Scraper dependencies missing — returning empty list.")
            return []

        pages: list[dict[str, Any]] = []
        next_path = "/"

        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=self.timeout,
            follow_redirects=True,
        ) as client:
            for page_num in range(1, n + 1):
                url = urljoin(self.base_url, next_path)
                logger.info("Scraper: page %d → %s", page_num, url)

                record = await self._fetch(client, url)
                pages.append(record)

                if record["error"]:
                    break

                next_path = self._next_link(record["html"])
                if not next_path:
                    break

                if page_num < n:
                    await asyncio.sleep(self.delay)

        return pages

    async def _fetch(self, client: "httpx.AsyncClient", url: str) -> dict[str, Any]:
        """Fetch a single URL."""
        t0 = time.perf_counter()
        base: dict[str, Any] = {
            "url": url, "html": "", "status": 0,
            "error": None, "fetch_time_s": 0.0,
        }
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            elapsed = round(time.perf_counter() - t0, 3)
            return {**base, "url": str(resp.url), "html": resp.text,
                    "status": resp.status_code, "fetch_time_s": elapsed}
        except httpx.TimeoutException:
            return {**base, "error": "timeout"}
        except httpx.HTTPStatusError as exc:
            return {**base, "status": exc.response.status_code,
                    "error": f"HTTP {exc.response.status_code}"}
        except Exception as exc:  # noqa: BLE001
            return {**base, "error": str(exc)}

    @staticmethod
    def _next_link(html: str) -> str | None:
        """Extract the 'next' pagination href."""
        if not html:
            return None
        try:
            soup = BeautifulSoup(html, "lxml")
            tag = soup.select_one("li.next > a")
            if tag and tag.get("href"):
                href = str(tag["href"])
                # books.toscrape returns relative paths like "catalogue/page-2.html"
                if not href.startswith("/"):
                    href = "/catalogue/" + href.lstrip("catalogue/")
                return href
        except Exception:  # noqa: BLE001
            pass
        return None
