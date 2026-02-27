"""
OpenFEC API source for candidate campaign website URLs.

Uses the /v1/committee/{id}/ endpoint to look up principal campaign committees
(cand_pcc) and extract the website field. One API call per committee.

API key: free at https://api.open.fec.gov/signup/
Env var: OPENFEC_API_KEY (falls back to DEMO_KEY for testing)
Rate limit: 1000 requests/hour with registered key.
"""

import logging
import os
import time

import pandas as pd
import requests

from ..utils import RateLimiter, URLCache

logger = logging.getLogger(__name__)

OPENFEC_BASE = "https://api.open.fec.gov/v1"
MAX_RETRIES = 3


class OpenFECSource:
    name = "openfec"

    def is_available(self, config: dict) -> bool:
        # Available with any key, including DEMO_KEY
        return True

    def fill_urls(self, roster: pd.DataFrame, config: dict) -> pd.DataFrame:
        api_key = os.environ.get("OPENFEC_API_KEY", "DEMO_KEY")
        if api_key == "DEMO_KEY":
            logger.warning("Using DEMO_KEY for OpenFEC — rate limits are strict (40/hr)")

        cache_dir = os.path.join(
            config.get("output", {}).get("base_dir", "data"), "url_cache"
        )
        cache = URLCache(cache_dir, self.name)

        src_config = config.get("url_sources", {}).get("openfec", {})
        rate_limit = src_config.get("rate_limit_seconds", 3.6)
        limiter = RateLimiter(min_delay=rate_limit)

        missing = roster[roster["website_url"] == ""].index
        n_found = 0
        n_cached = 0

        # Check cache first
        uncached_indices = []
        for idx in missing:
            row = roster.loc[idx]
            cached_url = cache.get(row["candidate"], row["state"], row["year"])
            if cached_url is not None:
                if cached_url:  # Non-empty cached URL
                    roster.at[idx, "website_url"] = cached_url
                    n_found += 1
                n_cached += 1
            else:
                uncached_indices.append(idx)

        if n_cached:
            logger.info(f"[openfec] {n_cached} cache hits ({n_found} with URLs)")

        # Group uncached candidates by their principal campaign committee ID
        pcc_to_indices: dict[str, list] = {}
        for idx in uncached_indices:
            row = roster.loc[idx]
            pcc = row.get("cand_pcc", "")
            if pd.notna(pcc) and pcc.strip():
                pcc_to_indices.setdefault(pcc.strip(), []).append(idx)
            else:
                # No PCC — cache as empty so we don't retry
                cache.put(row["candidate"], row["state"], row["year"], "")

        if not pcc_to_indices:
            logger.info("[openfec] No candidates with principal campaign committee IDs")
            return roster

        logger.info(f"[openfec] Querying {len(pcc_to_indices)} committees...")
        session = requests.Session()
        n_queried = 0

        for pcc, indices in pcc_to_indices.items():
            n_queried += 1
            if n_queried % 100 == 0:
                logger.info(f"[openfec] Progress: {n_queried}/{len(pcc_to_indices)} committees")

            website = _query_committee(session, api_key, pcc, limiter)

            for idx in indices:
                row = roster.loc[idx]
                cache.put(row["candidate"], row["state"], row["year"], website)
                if website:
                    roster.at[idx, "website_url"] = website
                    n_found += 1

        session.close()
        logger.info(f"[openfec] Found {n_found} URLs total")
        return roster


def _query_committee(session: requests.Session, api_key: str, pcc: str,
                     limiter: RateLimiter) -> str:
    """Query a single committee and return its website URL (empty string if none)."""
    for attempt in range(MAX_RETRIES):
        limiter.wait()
        try:
            response = session.get(
                f"{OPENFEC_BASE}/committee/{pcc}/",
                params={"api_key": api_key},
                timeout=30,
            )

            if response.status_code == 429:
                logger.warning(f"[openfec] Rate limited (attempt {attempt + 1}), backing off")
                limiter.backoff()
                continue

            if response.status_code == 404:
                return ""

            response.raise_for_status()
            data = response.json()
            limiter.reset()

        except requests.RequestException as e:
            logger.warning(f"[openfec] API error for {pcc}: {e}")
            if attempt < MAX_RETRIES - 1:
                limiter.backoff()
            continue

        results = data.get("results", [])
        if results:
            website = (results[0].get("website", "") or "").strip()
            website = _normalize_url(website)
            return website
        return ""

    logger.warning(f"[openfec] Failed after {MAX_RETRIES} retries for {pcc}")
    return ""


def _normalize_url(url: str) -> str:
    """Clean up FEC website URLs: lowercase, fix doubled schemes, add scheme."""
    if not url:
        return ""
    url = url.strip().lower()
    # Fix doubled schemes like "https://https://example.com"
    for prefix in ("https://https://", "https://http://", "http://https://", "http://http://"):
        if url.startswith(prefix):
            url = "https://" + url[len(prefix):]
            break
    # Add scheme if missing
    if not url.startswith("http"):
        url = "https://" + url
    return url
