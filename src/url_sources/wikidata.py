"""
Wikidata SPARQL source for candidate website URLs.

Queries Wikidata for US congress members with P856 (official website).
Matches by name. No API key needed.

This is the lowest-priority source — it mainly catches incumbent members
of Congress who have well-maintained Wikidata entries.
"""

import logging

import pandas as pd
import requests

from ..utils import URLCache

logger = logging.getLogger(__name__)

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = "CandidateWebsiteExtension/1.0 (Academic Research)"

# SPARQL query: entity IDs + websites for US Congress members.
# Labels are resolved separately via the Wikidata API to avoid
# the SERVICE wikibase:label clause, which causes timeouts.
SPARQL_QUERY = """
SELECT ?person ?website WHERE {
  {
    ?person wdt:P39 wd:Q13218630 .  # member of US House
  } UNION {
    ?person wdt:P39 wd:Q4416090 .   # member of US Senate
  }
  ?person wdt:P856 ?website .
}
"""


class WikidataSource:
    name = "wikidata"

    def is_available(self, config: dict) -> bool:
        return True  # No API key needed

    def fill_urls(self, roster: pd.DataFrame, config: dict) -> pd.DataFrame:
        cache_dir = config.get("output", {}).get("base_dir", "data") + "/url_cache"
        cache = URLCache(cache_dir, self.name)

        missing = roster[roster["website_url"] == ""].index
        if len(missing) == 0:
            return roster

        # Check cache first
        uncached_indices = []
        n_found = 0
        n_cached = 0

        for idx in missing:
            row = roster.loc[idx]
            cached_url = cache.get(row["candidate"], row["state"], row["year"])
            if cached_url is not None:
                if cached_url:
                    roster.at[idx, "website_url"] = cached_url
                    n_found += 1
                n_cached += 1
            else:
                uncached_indices.append(idx)

        if n_cached:
            logger.info(f"[wikidata] {n_cached} cache hits ({n_found} with URLs)")

        if not uncached_indices:
            return roster

        # Fetch Wikidata results (single bulk query)
        wikidata_map = _fetch_wikidata_websites()
        if not wikidata_map:
            logger.warning("[wikidata] SPARQL query returned no results")
            # Cache all as empty so we don't retry
            for idx in uncached_indices:
                row = roster.loc[idx]
                cache.put(row["candidate"], row["state"], row["year"], "")
            return roster

        logger.info(f"[wikidata] {len(wikidata_map)} congress members with websites")

        for idx in uncached_indices:
            row = roster.loc[idx]
            candidate = row["candidate"]
            state = row["state"]
            year = row["year"]

            website = _match_candidate(candidate, state, wikidata_map)
            cache.put(candidate, state, year, website)
            if website:
                roster.at[idx, "website_url"] = website
                n_found += 1
                logger.debug(f"[wikidata] {candidate} ({state}): {website}")

        logger.info(f"[wikidata] Found {n_found} URLs total")
        return roster


def _fetch_wikidata_websites() -> dict[str, list[dict]]:
    """Fetch congress members with websites from Wikidata.

    Two-step approach to avoid SPARQL label-service timeouts:
    1. SPARQL query returns entity IDs + website URLs (~1-2s)
    2. Wikidata API resolves entity IDs to English labels in batches (~10s)

    Returns a dict of lowercase last name → list of {name, website}.
    """
    # Step 1: SPARQL query for entity IDs + websites
    try:
        response = requests.get(
            WIKIDATA_SPARQL,
            params={"query": SPARQL_QUERY, "format": "json"},
            headers={"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"},
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"[wikidata] SPARQL query failed: {e}")
        return {}

    # Collect entity ID → list of websites
    entity_websites: dict[str, list[str]] = {}
    for binding in data.get("results", {}).get("bindings", []):
        qid = binding.get("person", {}).get("value", "").split("/")[-1]
        website = binding.get("website", {}).get("value", "")
        if qid and website:
            entity_websites.setdefault(qid, []).append(website)

    if not entity_websites:
        return {}

    logger.info(f"[wikidata] SPARQL returned {len(entity_websites)} entities with websites")

    # Step 2: Resolve entity IDs to names via Wikidata API (50 per batch)
    entity_names: dict[str, str] = {}
    qids = list(entity_websites.keys())
    batch_size = 50
    for i in range(0, len(qids), batch_size):
        batch = qids[i : i + batch_size]
        try:
            resp = requests.get(
                WIKIDATA_API,
                params={
                    "action": "wbgetentities",
                    "ids": "|".join(batch),
                    "props": "labels",
                    "languages": "en",
                    "format": "json",
                },
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            resp.raise_for_status()
            for qid, entity in resp.json().get("entities", {}).items():
                label = entity.get("labels", {}).get("en", {}).get("value", "")
                if label:
                    entity_names[qid] = label
        except (requests.RequestException, ValueError) as e:
            logger.warning(f"[wikidata] Label batch failed: {e}")

    logger.info(f"[wikidata] Resolved {len(entity_names)} entity names")

    # Build last-name index
    results: dict[str, list[dict]] = {}
    for qid, name in entity_names.items():
        websites = entity_websites.get(qid, [])
        last_name = name.split()[-1].lower() if name.split() else ""
        if last_name and websites:
            for website in websites:
                results.setdefault(last_name, []).append({
                    "name": name,
                    "website": website,
                })

    return results


def _match_candidate(candidate: str, state: str,
                     wikidata_map: dict[str, list[dict]]) -> str:
    """Match a roster candidate to a Wikidata result by name."""
    parts = candidate.split()
    if not parts:
        return ""

    last_name = parts[-1].lower()
    first_name = parts[0].lower()

    candidates_with_name = wikidata_map.get(last_name, [])
    if not candidates_with_name:
        return ""

    for entry in candidates_with_name:
        wiki_name = entry["name"].lower()

        # Match first name (at least first 3 chars)
        first_match = (
            first_name[:3] in wiki_name
            or wiki_name.split()[0][:3] == first_name[:3]
        )

        if first_match:
            return entry["website"]

    return ""
