"""
Shared utilities: rate limiting, checkpointing, logging, CSV I/O, URL caching.
"""

import csv
import logging
import os
import time
from pathlib import Path

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load YAML configuration. Also loads .env if present."""
    from dotenv import load_dotenv
    load_dotenv()
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class RateLimiter:
    """Token-bucket rate limiter with exponential backoff."""

    def __init__(self, min_delay: float = 0.1, backoff_factor: float = 2,
                 backoff_max: float = 360):
        self.min_delay = min_delay
        self.backoff_factor = backoff_factor
        self.backoff_max = backoff_max
        self.last_request_time: float = 0
        self._current_delay = min_delay

    def wait(self):
        """Wait the appropriate amount before next request."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self._current_delay:
            time.sleep(self._current_delay - elapsed)
        self.last_request_time = time.time()

    def backoff(self):
        """Increase delay after a rate-limit response."""
        self._current_delay = min(
            self._current_delay * self.backoff_factor,
            self.backoff_max
        )
        logger.warning(f"Rate limited. Backing off to {self._current_delay:.1f}s")

    def reset(self):
        """Reset delay to minimum after successful request."""
        self._current_delay = self.min_delay


class ProgressTracker:
    """CSV-based checkpoint tracker for resumable scraping."""

    def __init__(self, progress_file: str):
        self.progress_file = progress_file
        self._completed: set[str] = set()
        self._load()

    def _load(self):
        """Load previously completed URLs from checkpoint file."""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self._completed.add(row.get("url", ""))
            logger.info(f"Loaded {len(self._completed)} completed URLs from checkpoint")

    def is_done(self, url: str) -> bool:
        """Check if a URL has already been scraped."""
        return url in self._completed

    def mark_done(self, row: dict):
        """Append a completed row to the checkpoint file."""
        write_header = not os.path.exists(self.progress_file)
        os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)

        with open(self.progress_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            if write_header:
                writer.writeheader()
            writer.writerow(row)

        self._completed.add(row.get("url", ""))


def append_csv(filepath: str, rows: list[dict]):
    """Append rows (list of dicts) to a CSV file, creating it if needed."""
    if not rows:
        return

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    write_header = not os.path.exists(filepath)
    fieldnames = list(rows[0].keys())

    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


class URLCache:
    """CSV-backed cache for URL lookups, keyed by (candidate, state, year, source).

    Avoids re-querying APIs for candidates whose URLs were already found in a
    previous run. Each source writes its own cache file under data/url_cache/.
    """

    def __init__(self, cache_dir: str, source_name: str, ttl_days: int = 90):
        self.cache_dir = cache_dir
        self.source_name = source_name
        self.ttl_days = ttl_days
        self.cache_path = os.path.join(cache_dir, f"{source_name}.csv")
        self._cache: dict[tuple, str] = {}
        self._load()

    def _load(self):
        """Load cached URLs from disk."""
        if not os.path.exists(self.cache_path):
            return
        try:
            df = pd.read_csv(self.cache_path, dtype=str).fillna("")
            now = time.time()
            for _, row in df.iterrows():
                # Skip expired entries
                cached_at = float(row.get("cached_at", 0))
                if self.ttl_days > 0 and (now - cached_at) > self.ttl_days * 86400:
                    continue
                key = (row["candidate"], row["state"], str(row["year"]))
                self._cache[key] = row.get("url", "")
            logger.info(f"URLCache[{self.source_name}]: loaded {len(self._cache)} entries")
        except Exception as e:
            logger.warning(f"URLCache[{self.source_name}]: failed to load cache: {e}")

    def get(self, candidate: str, state: str, year: int) -> str | None:
        """Return cached URL or None if not cached."""
        key = (candidate, state, str(year))
        if key in self._cache:
            return self._cache[key]
        return None

    def put(self, candidate: str, state: str, year: int, url: str):
        """Store a URL in the cache and append to disk."""
        key = (candidate, state, str(year))
        self._cache[key] = url
        os.makedirs(self.cache_dir, exist_ok=True)

        write_header = not os.path.exists(self.cache_path)
        with open(self.cache_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["candidate", "state", "year", "url", "cached_at"]
            )
            if write_header:
                writer.writeheader()
            writer.writerow({
                "candidate": candidate,
                "state": state,
                "year": str(year),
                "url": url,
                "cached_at": str(time.time()),
            })


def setup_logging(level: str = "INFO"):
    """Configure logging for the project."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
