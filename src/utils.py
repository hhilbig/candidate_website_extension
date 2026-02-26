"""
Shared utilities: rate limiting, checkpointing, logging, CSV I/O.
"""

import csv
import logging
import os
import time
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load YAML configuration."""
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


def setup_logging(level: str = "INFO"):
    """Configure logging for the project."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
