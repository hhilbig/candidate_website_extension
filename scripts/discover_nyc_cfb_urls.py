#!/usr/bin/env python3
"""Discover NYC mayoral campaign sites from official CFB general-election guides."""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_mayoral_url_discovery import EVIDENCE_COLUMNS, fetch_text_with_curl, write_csv_atomic
from src.mayoral_discovery import canonical_http_url, is_candidate_campaign_host, normalize_person_name


GUIDE_SEEDS = {
    2005: "https://www.nyccfb.info/public/voter-guide/general_2005/cd_profile/M_Ferrer_AY.aspx",
    2009: "https://www.nyccfb.info/public/voter-guide/general_2009/intro_mayor.aspx",
    2013: "https://www.nyccfb.info/public/voter-guide/general_2013/districts_map/cd13.aspx",
}
STATUS_COLUMNS = [
    "year", "source_profile_url", "source_status", "source_candidate_name",
    "matched_candidate_year_id", "match_score", "campaign_urls_found", "error",
]


def repair_text(value: str) -> str:
    value = str(value)
    try:
        if "Ã" in value or "Â" in value:
            return value.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    return value


def mayor_profile_links(html: str, source_url: str, year: int) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(rf"/general_{year}/cd_profile/M_", re.IGNORECASE)
    links = []
    for anchor in soup.find_all("a", href=True):
        url = urljoin(source_url, anchor["href"].strip())
        if pattern.search(urlparse(url).path):
            parsed = urlparse(url)
            links.append(urlunparse(parsed._replace(fragment="")))
    if pattern.search(urlparse(source_url).path):
        links.append(source_url)
    return list(dict.fromkeys(links))


def profile_candidate_name(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    if "Candidate Profile:" in title:
        return title.split("Candidate Profile:", 1)[1].strip()
    return title.rsplit(":", 1)[-1].strip() if ":" in title else ""


def profile_campaign_urls(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    found = {}
    for paragraph in soup.find_all(["p", "li"]):
        text = " ".join(paragraph.get_text(" ", strip=True).split())
        if not re.search(r"\b(web\s*site|website)\s*:", text, re.IGNORECASE):
            continue
        anchors = paragraph.find_all("a", href=True)
        raw_values = [anchor["href"].strip() for anchor in anchors]
        if not raw_values:
            raw_values = [re.sub(r"^.*?\b(?:web\s*site|website)\s*:\s*", "", text, flags=re.I)]
        for raw in raw_values:
            if raw and not raw.startswith(("http://", "https://")):
                raw = "http://" + raw
            url = canonical_http_url(raw)
            if url and is_candidate_campaign_host(url):
                found[url] = text[:500]
    for anchor in soup.find_all("a", href=True):
        if "web site" not in str(anchor.get("alt", "")).casefold():
            continue
        url = canonical_http_url(anchor["href"].strip())
        if url and is_candidate_campaign_host(url):
            found[url] = "Candidate-supplied Web Site link"
    return sorted(found.items())


def match_candidate(name: str, candidates: pd.DataFrame) -> tuple[pd.Series | None, float]:
    source = normalize_person_name(repair_text(name))
    source_tokens = source.split()
    suffixes = {"jr", "sr", "ii", "iii", "iv"}
    if source_tokens and source_tokens[-1] in suffixes:
        source_tokens = source_tokens[:-1]
    surname_matches = 0
    if source_tokens:
        surname_matches = sum(
            bool(tokens) and (tokens[-2] if tokens[-1] in suffixes and len(tokens) > 1 else tokens[-1]) == source_tokens[-1]
            for tokens in (
                normalize_person_name(repair_text(target)).split()
                for target in candidates.full_name
            )
        )
    scores = []
    for target in candidates.full_name:
        normalized = normalize_person_name(repair_text(target))
        score = SequenceMatcher(None, source, normalized).ratio()
        target_tokens = normalized.split()
        if target_tokens and target_tokens[-1] in suffixes:
            target_tokens = target_tokens[:-1]
        if source_tokens and target_tokens and source_tokens[0] == target_tokens[0] and source_tokens[-1] == target_tokens[-1]:
            score = max(score, 0.95)
        elif source_tokens and target_tokens and source_tokens[-1] == target_tokens[-1] and surname_matches == 1:
            score = max(score, 0.90)
        scores.append(score)
    if not scores:
        return None, 0
    best = max(range(len(scores)), key=scores.__getitem__)
    return candidates.iloc[best], scores[best]


def discover_nyc_cfb(
    universe: pd.DataFrame, timeout: int = 20, delay_seconds: float = 0.5
) -> tuple[pd.DataFrame, pd.DataFrame]:
    nyc = universe[
        universe.in_discovery_scope & universe.geo_name.eq("new york")
    ].drop_duplicates("candidate_year_id")
    evidence, status = [], []
    for year, seed_url in GUIDE_SEEDS.items():
        candidates = nyc[nyc.year.eq(year)]
        if candidates.empty:
            continue
        try:
            seed_html = fetch_text_with_curl(seed_url, timeout, attempts=2)
            profile_urls = mayor_profile_links(seed_html, seed_url, year)
        except Exception as error:
            status.append({"year":year,"source_profile_url":seed_url,"source_status":"error",
                           "source_candidate_name":"","matched_candidate_year_id":"","match_score":pd.NA,
                           "campaign_urls_found":0,"error":f"{type(error).__name__}: {error}"[:500]})
            continue
        for profile_url in profile_urls:
            row_status = {"year":year,"source_profile_url":profile_url,"source_status":"not_attempted",
                          "source_candidate_name":"","matched_candidate_year_id":"","match_score":pd.NA,
                          "campaign_urls_found":0,"error":""}
            try:
                html = seed_html if profile_url == seed_url else fetch_text_with_curl(profile_url, timeout, attempts=2)
                source_name = profile_candidate_name(html)
                matched, score = match_candidate(source_name, candidates)
                row_status.update({"source_candidate_name":source_name,"match_score":score})
                if matched is None or score < 0.86:
                    row_status["source_status"] = "candidate_match_unresolved"
                else:
                    row_status["source_status"] = "profile_checked"
                    row_status["matched_candidate_year_id"] = matched.candidate_year_id
                    urls = profile_campaign_urls(html)
                    row_status["campaign_urls_found"] = len(urls)
                    for url, label in urls:
                        evidence.append({
                            "candidate_year_id":matched.candidate_year_id,"ledb_candid":matched.ledb_candid,
                            "full_name":matched.full_name,"geo_name":matched.geo_name,"state_abb":matched.state_abb,
                            "year":matched.year,"candidate_source":"American Local Government Elections Database",
                            "candidate_source_doi":"10.17605/OSF.IO/MV5E6",
                            "url_source":"NYC Campaign Finance Board official general-election voter guide",
                            "source_profile_url":profile_url,"source_snapshot_timestamp":"",
                            "retrieved_on":date.today().isoformat(),"evidence_url":url,
                            "evidence_text":f"{source_name}: {label}"[:500],
                            "discovery_method":"nyc_cfb_candidate_supplied_campaign_website",
                            "confidence":"high",
                        })
            except Exception as error:
                row_status["source_status"] = "error"
                row_status["error"] = f"{type(error).__name__}: {error}"[:500]
            status.append(row_status)
            time.sleep(delay_seconds)
    return pd.DataFrame(evidence).reindex(columns=EVIDENCE_COLUMNS), pd.DataFrame(status).reindex(columns=STATUS_COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/municipal_discovery/shard_0")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--delay-seconds", type=float, default=0.5)
    args = parser.parse_args()
    directory = Path(args.input_dir)
    evidence, status = discover_nyc_cfb(
        pd.read_csv(directory / "mayoral_candidate_universe.csv", low_memory=False),
        timeout=args.timeout, delay_seconds=args.delay_seconds,
    )
    write_csv_atomic(evidence, directory / "mayoral_url_evidence_nyc_cfb.csv", EVIDENCE_COLUMNS)
    write_csv_atomic(status, directory / "mayoral_source_status_nyc_cfb.csv", STATUS_COLUMNS)
    print({"evidence_rows":len(evidence),"candidate_years":evidence.candidate_year_id.nunique() if len(evidence) else 0,"status_rows":len(status)})


if __name__ == "__main__":
    main()
