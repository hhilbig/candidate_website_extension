#!/usr/bin/env python3
"""
Wayback Machine scraper for U.S. candidate websites.

Queries the CDX API for archived snapshots, fetches pages, extracts visible text.
Supports House, Senate, and Governor races.

Usage:
    python -m src.scrape_wayback --office house --year 2022
    python -m src.scrape_wayback --office senate --year 2020 --threads 4
    python -m src.scrape_wayback --roster data/rosters/roster_house_2022.csv
"""

import argparse
import csv
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from .extract_text import (
    extract_frame_content,
    extract_visible_text,
    get_subpage_urls,
    is_wayback_page,
    strip_wayback_toolbar,
)
from .utils import (
    ProgressTracker,
    RateLimiter,
    append_csv,
    load_config,
    setup_logging,
)

logger = logging.getLogger(__name__)


# ── CDX API ──────────────────────────────────────────────────────────

CDX_API = "https://web.archive.org/cdx/search/cdx"


def query_cdx(url: str, start_date: str, end_date: str,
               config: dict) -> list[dict]:
    """
    Query Wayback Machine CDX API for snapshots of a URL.

    Args:
        url: Original candidate website URL.
        start_date: YYYYMMDD start of window.
        end_date: YYYYMMDD end of window.
        config: Wayback config dict.

    Returns:
        List of snapshot dicts with timestamp, original URL, wayback URL.
    """
    params = {
        "url": url,
        "matchType": "prefix",
        "from": start_date,
        "to": end_date,
        "output": "json",
        "fl": "timestamp,original,statuscode,mimetype",
        "filter": ["statuscode:200", "mimetype:text/html"],
        "collapse": "timestamp:6",  # One snapshot per month
        "limit": 10000,
    }

    session = _make_session(config)
    max_retries = config.get("max_retries", 3)
    timeout = (config.get("timeout_connect", 30), config.get("timeout_read", 120))

    for attempt in range(max_retries):
        try:
            response = session.get(CDX_API, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if not data or len(data) < 2:
                return []

            headers = data[0]
            snapshots = []
            for row in data[1:]:
                record = dict(zip(headers, row))
                snapshots.append({
                    "timestamp": record["timestamp"],
                    "original_url": record["original"],
                    "wayback_url": f"https://web.archive.org/web/{record['timestamp']}/{record['original']}",
                })
            return snapshots

        except requests.RequestException as e:
            logger.warning(f"CDX query failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 10
                time.sleep(wait)
            else:
                logger.error(f"CDX query failed after {max_retries} attempts for {url}")
                return []

    return []


# ── Page fetching ────────────────────────────────────────────────────

def _make_session(config: dict) -> requests.Session:
    """Create a requests session with retry adapter."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=5)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": config.get("user_agent", "CandidateWebsiteExtension/1.0 (Academic Research)")
    })
    return session


def fetch_page(url: str, session: requests.Session,
               rate_limiter: RateLimiter) -> Optional[BeautifulSoup]:
    """
    Fetch a Wayback Machine page and return parsed soup.

    Returns None for PDFs, non-Wayback pages, or on error.
    """
    skip_ext = [".pdf", ".jpg", ".png", ".gif", ".mp3", ".mp4", ".zip"]
    if any(url.lower().endswith(ext) for ext in skip_ext):
        return None

    rate_limiter.wait()

    try:
        response = session.get(url, allow_redirects=True, timeout=(30, 90))
        response.raise_for_status()

        if not is_wayback_page(response.text):
            return None

        clean_html = strip_wayback_toolbar(response.text)
        rate_limiter.reset()
        return BeautifulSoup(clean_html, "html.parser")

    except requests.exceptions.TooManyRedirects:
        return None
    except requests.exceptions.InvalidSchema:
        return None
    except requests.RequestException as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None


def scrape_snapshot(wayback_url: str, session: requests.Session,
                    rate_limiter: RateLimiter,
                    config: dict) -> list[dict]:
    """
    Scrape a single snapshot: home page + subpages.

    Returns list of dicts with snap_url and snap_content.
    """
    scrape_cfg = config.get("scraping", {})
    separator = scrape_cfg.get("text_separator", "#+#")
    max_subpages = scrape_cfg.get("max_subpages", 50)
    exclude_domains = scrape_cfg.get("exclude_domains", [])

    results = []
    urls_explored = set()

    # Fetch home page
    soup = fetch_page(wayback_url, session, rate_limiter)
    if soup is None:
        return [{"snap_url": wayback_url, "snap_content": ""}]

    def _fetch_fn(url):
        return fetch_page(url, session, rate_limiter)

    text, subpage_urls = extract_frame_content(soup, wayback_url, separator, _fetch_fn)
    results.append({"snap_url": wayback_url, "snap_content": text})
    urls_explored.add(wayback_url)

    # Scrape subpages
    subpage_urls = [u for u in subpage_urls if u not in urls_explored][:max_subpages]
    for sub_url in subpage_urls:
        sub_soup = fetch_page(sub_url, session, rate_limiter)
        if sub_soup is None:
            continue

        sub_text = extract_visible_text(sub_soup, separator)
        if "too many requests" in sub_text.lower():
            logger.warning("Rate limited by Wayback. Backing off.")
            rate_limiter.backoff()
            time.sleep(rate_limiter._current_delay)
            sub_soup = fetch_page(sub_url, session, rate_limiter)
            if sub_soup:
                sub_text = extract_visible_text(sub_soup, separator)

        results.append({"snap_url": sub_url, "snap_content": sub_text})
        urls_explored.add(sub_url)

    # Deduplicate by content
    seen_content = set()
    deduped = []
    for r in results:
        if r["snap_content"] not in seen_content:
            seen_content.add(r["snap_content"])
            deduped.append(r)

    return deduped


# ── Candidate processing ────────────────────────────────────────────

def process_candidate(candidate: dict, config: dict,
                      progress: ProgressTracker) -> int:
    """
    Scrape all snapshots for a single candidate.

    Args:
        candidate: Dict with candidate, state, district, office, year, party, website_url.
        config: Full config dict.
        progress: ProgressTracker for resumability.

    Returns:
        Number of snapshots scraped.
    """
    wb_config = config.get("wayback", {})
    out_config = config.get("output", {})

    name = candidate["candidate"]
    office = candidate["office"]
    year = int(candidate["year"])
    state = candidate["state"]
    website_url = candidate["website_url"]

    # Election-year window: Jan 1 to Dec 31
    start_date = f"{year}0101"
    end_date = f"{year}1231"

    logger.info(f"Querying CDX for {name} ({state}, {office} {year}): {website_url}")
    snapshots = query_cdx(website_url, start_date, end_date, wb_config)

    if not snapshots:
        logger.info(f"No snapshots found for {name}")
        return 0

    session = _make_session(wb_config)
    rate_limiter = RateLimiter(
        min_delay=wb_config.get("rate_limit_seconds", 0.1),
        backoff_factor=wb_config.get("backoff_factor", 2),
        backoff_max=wb_config.get("backoff_max_seconds", 360),
    )

    output_dir = os.path.join(out_config.get("snapshots_dir", "data/snapshots"), office, str(year))
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{name} ({state}).csv")

    n_scraped = 0
    for snap in snapshots:
        wb_url = snap["wayback_url"]
        if progress.is_done(wb_url):
            continue

        try:
            pages = scrape_snapshot(wb_url, session, rate_limiter, config)

            rows = []
            for page in pages:
                content = page["snap_content"]
                rows.append({
                    "candidate": name,
                    "state": state,
                    "district": candidate.get("district", ""),
                    "office": office,
                    "year": year,
                    "party": candidate.get("party", ""),
                    "stage": candidate.get("stage", 2),
                    "date": snap["timestamp"],
                    "urlkey": website_url,
                    "snap_url": page["snap_url"],
                    "data_source": "wayback_cdx",
                    "n_tags": 0,
                    "n_clean_tags": 0,
                    "text_snap_content": content,
                    "n_char": len(content),
                    "n_words": len(content.split()),
                })

            if rows:
                append_csv(output_file, rows)
                n_scraped += 1

            progress.mark_done({
                "url": wb_url,
                "candidate": name,
                "state": state,
                "office": office,
                "year": year,
                "scrape_complete": 1,
                "scrape_error": 0,
            })

        except Exception as e:
            logger.error(f"Error scraping {name} snapshot {wb_url}: {e}")
            progress.mark_done({
                "url": wb_url,
                "candidate": name,
                "state": state,
                "office": office,
                "year": year,
                "scrape_complete": 0,
                "scrape_error": 1,
            })

    session.close()
    return n_scraped


def run_scrape(roster_path: str, config: dict, threads: int = 8):
    """
    Scrape all candidates in a roster file.

    Args:
        roster_path: Path to candidate roster CSV.
        config: Full config dict.
        threads: Number of parallel threads.
    """
    roster = pd.read_csv(roster_path)
    logger.info(f"Loaded roster with {len(roster)} candidates from {roster_path}")

    out_config = config.get("output", {})
    progress_dir = out_config.get("progress_dir", "data/progress")
    os.makedirs(progress_dir, exist_ok=True)

    # Derive progress file name from roster
    roster_stem = os.path.splitext(os.path.basename(roster_path))[0]
    progress_file = os.path.join(progress_dir, f"progress_{roster_stem}.csv")
    progress = ProgressTracker(progress_file)

    candidates = roster.to_dict("records")
    total_scraped = 0

    if threads == 1:
        for cand in tqdm(candidates, desc="Scraping candidates"):
            total_scraped += process_candidate(cand, config, progress)
    else:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {
                executor.submit(process_candidate, cand, config, progress): cand
                for cand in candidates
            }
            for future in tqdm(as_completed(futures), total=len(futures),
                               desc="Scraping candidates"):
                try:
                    total_scraped += future.result()
                except Exception as e:
                    cand = futures[future]
                    logger.error(f"Thread error for {cand.get('candidate', '?')}: {e}")

    logger.info(f"Scraping complete. {total_scraped} snapshots saved.")


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrape U.S. candidate websites from the Wayback Machine."
    )
    parser.add_argument("--roster", type=str,
                        help="Path to candidate roster CSV")
    parser.add_argument("--office", type=str, choices=["house", "senate", "governor"],
                        help="Office type (used to find default roster)")
    parser.add_argument("--year", type=int,
                        help="Election year (used to find default roster)")
    parser.add_argument("--threads", type=int, default=None,
                        help="Number of parallel threads (overrides config)")
    parser.add_argument("--config", type=str, default="config/config.yaml",
                        help="Path to config YAML")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()
    setup_logging(args.log_level)
    config = load_config(args.config)

    # Determine roster path
    if args.roster:
        roster_path = args.roster
    elif args.office and args.year:
        roster_dir = config.get("output", {}).get("roster_dir", "data/rosters")
        roster_path = os.path.join(roster_dir, f"roster_{args.office}_{args.year}.csv")
    else:
        parser.error("Provide either --roster or both --office and --year")

    if not os.path.exists(roster_path):
        logger.error(f"Roster file not found: {roster_path}")
        return

    threads = args.threads or config.get("scraping", {}).get("threads", 8)
    run_scrape(roster_path, config, threads)


if __name__ == "__main__":
    main()
