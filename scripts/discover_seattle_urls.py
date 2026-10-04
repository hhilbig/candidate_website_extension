#!/usr/bin/env python3
"""Discover mayoral campaign sites from Seattle's official campaign portal."""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import unicodedata
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_mayoral_url_discovery import EVIDENCE_COLUMNS, fetch_text_with_curl, write_csv_atomic
from src.mayoral_discovery import canonical_http_url, is_candidate_campaign_host


BASE_URL = "https://web6.seattle.gov/ethics/elections/campaigns.aspx"
CONTACT_URL = "https://web6.seattle.gov/ethics/elections/returnCampaignContactInfo.ashx"


def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    return " ".join(re.findall(r"[a-z]+", value.casefold()))


def mayor_campaign_ids(html: str, year: int) -> list[int]:
    soup = BeautifulSoup(html, "html.parser")
    mayor_header = next(
        (strong for strong in soup.find_all("strong") if normalize_name(strong.get_text()) == "mayor"),
        None,
    )
    if mayor_header is None:
        return []
    header_container = mayor_header.find_parent(class_="tabLeftColumn")
    ids = []
    node = header_container.next_sibling if header_container else None
    pattern = re.compile(rf"jsLoadDashboard\('{year}','campaign',(\d+),'collapsed'\)")
    while node is not None:
        if getattr(node, "name", None) == "span" and "tabLeftColumn" in (node.get("class") or []):
            break
        if hasattr(node, "find_all"):
            candidates = [node] if getattr(node, "name", None) == "span" else node.find_all("span")
            for candidate in candidates:
                match = pattern.search(candidate.get("onclick", ""))
                if match:
                    ids.append(int(match.group(1)))
        node = node.next_sibling
    return list(dict.fromkeys(ids))


def extract_campaign_contact(html: str) -> tuple[str, list[tuple[str, str]]]:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("div[style*='font-size: 1em']")
    if content is None:
        return "", []
    strong = content.find("strong")
    candidate_name = strong.get_text(" ", strip=True) if strong else ""
    links = []
    for anchor in content.find_all("a", href=True):
        url = canonical_http_url(anchor.get("href", ""))
        if url and is_candidate_campaign_host(url):
            links.append((url, anchor.get_text(" ", strip=True)))
    return candidate_name, links


def best_candidate_match(name: str, candidates: pd.DataFrame) -> tuple[pd.Series | None, float]:
    normalized = normalize_name(name)
    source_tokens = normalized.split()
    normalized_targets = [normalize_name(value) for value in candidates["full_name"]]
    source_pair = (source_tokens[0][0], source_tokens[-1]) if source_tokens else ("", "")
    pair_matches = sum(
        bool(tokens) and (tokens[0][0], tokens[-1]) == source_pair
        for tokens in (target.split() for target in normalized_targets)
    )
    scores = []
    for target in normalized_targets:
        target_tokens = target.split()
        score = SequenceMatcher(None, normalized, target).ratio()
        if (
            source_tokens and target_tokens
            and source_tokens[-1] == target_tokens[-1]
            and source_tokens[0][0] == target_tokens[0][0]
            and pair_matches == 1
        ):
            score = max(score, 0.90)
        scores.append(score)
    if not scores:
        return None, 0
    best = max(range(len(scores)), key=scores.__getitem__)
    return candidates.iloc[best], scores[best]


def normalize_reported_url(value: str) -> str:
    if pd.isna(value) or not str(value).strip():
        return ""
    value = str(value).strip()
    if not value.startswith(("http://", "https://")):
        value = "http://" + value
    url = canonical_http_url(value)
    return url if url and is_candidate_campaign_host(url) else ""


def parse_contact_csv(text: str) -> pd.DataFrame:
    rows = list(csv.reader(io.StringIO(text)))
    if not rows or "strContestType" not in rows[0]:
        raise ValueError("Seattle contact file has no recognized header")
    parsed = []
    for number, row in enumerate(rows[1:], start=2):
        if len(row) < 8:
            continue
        participation = next(
            (index for index in range(len(row) - 1, 7, -1) if row[index] in {"No", "Yes", "Was"}),
            None,
        )
        web = row[participation - 1] if participation is not None and participation > 8 else ""
        parsed.append({
            "intCampaignID": row[0], "strContestType": row[1],
            "c30CampaignName": row[5], "c200Web": web,
            "source_line_number": number, "source_field_count": len(row),
        })
    frame = pd.DataFrame(parsed)
    return frame[frame["strContestType"].str.casefold().eq("mayor")].copy()


def discover_seattle(
    universe: pd.DataFrame, timeout: int = 20, delay_seconds: float = 1
) -> tuple[pd.DataFrame, pd.DataFrame]:
    seattle = universe[
        universe["in_discovery_scope"] & universe["geo_name"].eq("seattle")
    ].copy()
    evidence, status = [], []
    for year, candidates in seattle.groupby("year"):
        contact_url = f"{CONTACT_URL}?yearElection={int(year)}"
        try:
            contacts = parse_contact_csv(fetch_text_with_curl(contact_url, timeout, attempts=2))
        except Exception as error:
            status.append({"year": year, "source_status": "error", "campaign_id": "", "candidate_name": "", "matched_candidate_year_id": "", "match_score": pd.NA, "source_profile_url": contact_url, "error": f"{type(error).__name__}: {error}"[:500]})
            continue
        if contacts.empty:
            status.append({"year": year, "source_status": "no_mayor_campaign_records", "campaign_id": "", "candidate_name": "", "matched_candidate_year_id": "", "match_score": pd.NA, "source_profile_url": contact_url, "error": ""})
        for contact in contacts.itertuples(index=False):
            campaign_id = contact.intCampaignID
            profile_url = f"{BASE_URL}?IDNum={campaign_id}&cycle={int(year)}&leftmenu=collapsed&type=campaign"
            candidate_name = str(contact.c30CampaignName)
            row_status = {"year": year, "source_status": "not_attempted", "campaign_id": campaign_id, "candidate_name": candidate_name, "matched_candidate_year_id": "", "match_score": pd.NA, "source_profile_url": profile_url, "error": ""}
            try:
                matched, score = best_candidate_match(candidate_name, candidates)
                row_status.update({"candidate_name": candidate_name, "match_score": score})
                if matched is None or score < 0.86:
                    row_status["source_status"] = "candidate_match_unresolved"
                else:
                    row_status["matched_candidate_year_id"] = matched["candidate_year_id"]
                    row_status["source_status"] = "profile_checked"
                    url = normalize_reported_url(contact.c200Web)
                    if url:
                        evidence.append({
                            "candidate_year_id": matched["candidate_year_id"],
                            "ledb_candid": matched["ledb_candid"], "full_name": matched["full_name"],
                            "geo_name": matched["geo_name"], "state_abb": matched["state_abb"],
                            "year": matched["year"],
                            "candidate_source": "American Local Government Elections Database",
                            "candidate_source_doi": "10.17605/OSF.IO/MV5E6",
                            "url_source": "Seattle Ethics and Elections campaign contact page",
                            "source_profile_url": profile_url, "source_snapshot_timestamp": "",
                            "retrieved_on": date.today().isoformat(), "evidence_url": url,
                            "evidence_text": f"{candidate_name}: c200Web={contact.c200Web}"[:500],
                            "discovery_method": "seattle_official_campaign_contact_csv",
                            "confidence": "high",
                        })
            except Exception as error:
                row_status["source_status"] = "error"
                row_status["error"] = f"{type(error).__name__}: {error}"[:500]
            status.append(row_status)
        time.sleep(delay_seconds)
    return pd.DataFrame(evidence).reindex(columns=EVIDENCE_COLUMNS), pd.DataFrame(status)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/municipal_discovery/shard_0")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--delay-seconds", type=float, default=1)
    args = parser.parse_args()
    directory = Path(args.input_dir)
    evidence, status = discover_seattle(
        pd.read_csv(directory / "mayoral_candidate_universe.csv", low_memory=False),
        timeout=args.timeout,
        delay_seconds=args.delay_seconds,
    )
    write_csv_atomic(evidence, directory / "mayoral_url_evidence_seattle.csv", EVIDENCE_COLUMNS)
    status.to_csv(directory / "mayoral_source_status_seattle.csv", index=False)
    print({"evidence_rows": len(evidence), "matched_candidate_years": evidence["candidate_year_id"].nunique() if not evidence.empty else 0, "status_rows": len(status)})


if __name__ == "__main__":
    main()
