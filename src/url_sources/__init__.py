"""
Multi-source waterfall for candidate website URL discovery.

Each source implements the URLSource protocol and is tried in priority order.
Candidates that already have a URL are skipped by subsequent sources.
"""

import logging
from typing import Protocol

import pandas as pd

logger = logging.getLogger(__name__)


class URLSource(Protocol):
    """Protocol that each URL source must implement."""

    @property
    def name(self) -> str: ...

    def is_available(self, config: dict) -> bool:
        """Return True if this source can be used (e.g., API key is set)."""
        ...

    def fill_urls(self, roster: pd.DataFrame, config: dict) -> pd.DataFrame:
        """Fill website_url for candidates that still have empty URLs.

        Should only modify rows where website_url == "".
        Returns the modified roster.
        """
        ...


def run_waterfall(roster: pd.DataFrame, config: dict,
                  sources: list[URLSource]) -> pd.DataFrame:
    """Run URL sources in priority order, each filling in still-missing URLs.

    Args:
        roster: DataFrame with at least 'candidate', 'state', 'year', 'website_url'.
        config: Full config dict.
        sources: Ordered list of URLSource instances.

    Returns:
        Roster with website_url filled in where possible.
    """
    total = len(roster)
    initially_missing = (roster["website_url"] == "").sum()
    logger.info(f"URL waterfall: {initially_missing}/{total} candidates need URLs")

    for source in sources:
        missing_before = (roster["website_url"] == "").sum()
        if missing_before == 0:
            logger.info("All candidates have URLs — skipping remaining sources")
            break

        if not source.is_available(config):
            logger.warning(f"[{source.name}] Skipped — not available (missing API key?)")
            continue

        logger.info(f"[{source.name}] Trying {missing_before} candidates...")
        try:
            roster = source.fill_urls(roster, config)
        except Exception:
            logger.exception(f"[{source.name}] Failed with error")
            continue

        missing_after = (roster["website_url"] == "").sum()
        found = missing_before - missing_after
        logger.info(f"[{source.name}] Found {found} URLs ({missing_after} still missing)")

    final_missing = (roster["website_url"] == "").sum()
    final_found = initially_missing - final_missing
    logger.info(
        f"URL waterfall complete: {final_found}/{initially_missing} URLs found, "
        f"{final_missing} still missing"
    )
    return roster


def build_default_sources() -> list:
    """Build the default ordered list of URL sources."""
    from .openfec import OpenFECSource
    from .wikidata import WikidataSource

    return [
        OpenFECSource(),
        WikidataSource(),
    ]
