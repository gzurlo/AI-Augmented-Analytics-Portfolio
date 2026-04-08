"""
scraper.py — Async web scraper using httpx + BeautifulSoup4.

Target site: https://quotes.toscrape.com  (public scraping sandbox)

Features
--------
* Fully async (``asyncio`` + ``httpx.AsyncClient``).
* Configurable rate-limiting (default 0.5 s between requests).
* Graceful error handling: timeouts, HTTP errors, network failures all
  produce an error record rather than crashing the pipeline.
* Returns a list of structured page dicts ready for the multimodal parser.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any
from urllib.parse import urljoin

from config import (
    SCRAPER_BASE_URL,
    SCRAPER_MAX_PAGES,
    SCRAPER_DELAY_SECONDS,
    SCRAPER_TIMEOUT_SECONDS,
    SCRAPER_USER_AGENT,
)

logger = logging.getLogger(__name__)

try:
    import httpx
    from bs4 import BeautifulSoup
    _SCRAPER_AVAILABLE = True
except ImportError:
    _SCRAPER_AVAILABLE = False
    logger.warning(
        "httpx or beautifulsoup4 not installed. "
        "Run: pip install httpx beautifulsoup4 lxml"
    )


class AsyncScraper:
    """Async HTTP scraper that crawls a configurable number of pages.

    Parameters
    ----------
    base_url:
        Root URL to start crawling from.
    max_pages:
        Maximum number of pages to fetch.
    delay:
        Seconds to wait between requests (rate-limiting).
    timeout:
        Per-request timeout in seconds.
    user_agent:
        ``User-Agent`` header sent with every request.
    """

    def __init__(
        self,
        base_url: str = SCRAPER_BASE_URL,
        max_pages: int = SCRAPER_MAX_PAGES,
        delay: float = SCRAPER_DELAY_SECONDS,
        timeout: float = SCRAPER_TIMEOUT_SECONDS,
        user_agent: str = SCRAPER_USER_AGENT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.max_pages = max_pages
        self.delay = delay
        self.timeout = timeout
        self.user_agent = user_agent

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape up to ``max_pages`` pages starting from ``base_url``.

        Returns
        -------
        list of page dicts, each containing:
            ``url``         – final URL after redirects
            ``status``      – HTTP status code (0 on network error)
            ``html``        – raw HTML string
            ``error``       – error message or None
        """
        if not _SCRAPER_AVAILABLE:
            logger.error("Scraper dependencies missing — returning empty list.")
            return []

        pages: list[dict[str, Any]] = []
        next_path = "/"

        async with httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout,
            follow_redirects=True,
        ) as client:
            for page_num in range(1, self.max_pages + 1):
                url = urljoin(self.base_url, next_path)
                logger.info("Scraper: fetching page %d — %s", page_num, url)

                page_data = await self._fetch_page(client, url)
                pages.append(page_data)

                if page_data.get("error"):
                    logger.warning(
                        "Scraper: error on page %d: %s",
                        page_num,
                        page_data["error"],
                    )
                    break

                # Discover the "Next" link for pagination
                next_path = self._extract_next_link(page_data["html"])
                if not next_path:
                    logger.info("Scraper: no more pages found after page %d", page_num)
                    break

                if page_num < self.max_pages:
                    await asyncio.sleep(self.delay)

        logger.info("Scraper: collected %d pages", len(pages))
        return pages

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_page(
        self, client: "httpx.AsyncClient", url: str
    ) -> dict[str, Any]:
        """Fetch a single URL and return a page dict."""
        t0 = time.perf_counter()
        try:
            response = await client.get(url)
            response.raise_for_status()
            elapsed = time.perf_counter() - t0
            logger.debug(
                "Scraper: %s → HTTP %d in %.2fs", url, response.status_code, elapsed
            )
            return {
                "url": str(response.url),
                "status": response.status_code,
                "html": response.text,
                "error": None,
                "fetch_time_s": round(elapsed, 3),
            }
        except httpx.TimeoutException:
            return {"url": url, "status": 0, "html": "", "error": "timeout", "fetch_time_s": 0}
        except httpx.HTTPStatusError as exc:
            return {
                "url": url,
                "status": exc.response.status_code,
                "html": "",
                "error": f"HTTP {exc.response.status_code}",
                "fetch_time_s": 0,
            }
        except Exception as exc:  # noqa: BLE001
            return {"url": url, "status": 0, "html": "", "error": str(exc), "fetch_time_s": 0}

    @staticmethod
    def _extract_next_link(html: str) -> str | None:
        """Parse the HTML and return the href of the 'Next' pagination link."""
        if not html:
            return None
        try:
            soup = BeautifulSoup(html, "lxml")
            next_tag = soup.select_one("li.next > a")
            if next_tag and next_tag.get("href"):
                return str(next_tag["href"])
        except Exception:  # noqa: BLE001
            pass
        return None
