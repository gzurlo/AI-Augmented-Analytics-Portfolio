"""scraper — Async multimodal web scraper targeting books.toscrape.com."""

from scraper.scraper import AsyncScraper
from scraper.multimodal_parser import MultimodalParser
from scraper.dataset_builder import build_dataset

__all__ = ["AsyncScraper", "MultimodalParser", "build_dataset"]
