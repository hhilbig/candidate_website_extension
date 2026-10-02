"""Propose identity links from verified mayoral finalists to the legacy roster.

A finalist with no exact-name link is matched to a legacy roster candidate in the
same city, year and election month when the surname is equal after
normalization and the first names are compatible (same initial, or a quoted
nickname equals the other first name). Only a unique match is accepted; the
rest go to a review file. The build reads accepted links from
data/municipal_discovery/top100_coverage/linkage/identity_links.csv.
"""
from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path

import pandas as pd

BASE = Path("data/municipal_discovery/top100_coverage")
ROSTER = Path("data/municipal_discovery/exhaustive_source_expansion_20260908/ownership_repair_20260910/combined/candidate_roster.csv")
URLS = Path("quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_complete/candidate_urls_with_capture_reviews.csv")
SUFFIX = {"jr", "sr", "ii", "iii", "iv", "dr", "mr", "mrs", "ms", "hon"}


def fix_text(value: str) -> str:
    """Repair UTF-8 read as Latin-1 (for example 'jesÃºs') and strip accents."""
    text = str(value)
    try:
        text = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def tokens(name: str) -> tuple[list[str], list[str]]:
    text = fix_text(name).casefold()
    nicks = [re.sub(r"[^a-z]", "", n) for n in re.findall(r"['\"“”‘’]([^'\"“”‘’]+)['\"“”‘’]", text)]
    text = re.sub(r"['\"“”‘’][^'\"“”‘’]+['\"“”‘’]", " ", text)
    words = [re.sub(r"[^a-z\-]", "", w) for w in text.replace(",", " ").split()]
    words = [w for w in words if w and w.strip("-") and w not in SUFFIX]
    return words, [n for n in nicks if n]


def surname(name: str) -> str:
    words, _ = tokens(name)
    return words[-1].replace("-", "") if words else ""


def first_names(name: str) -> set[str]:
    words, nicks = tokens(name)
    return {w for w in words[:-1] if len(w) > 1} | set(nicks)


NICKNAMES = [
    {"robert", "bob", "rob", "bobby"}, {"james", "jim", "jimmy"}, {"william", "bill", "will", "billy"},
    {"richard", "rick", "dick", "rich"}, {"michael", "mike"}, {"thomas", "tom"}, {"joseph", "joe"},
    {"steven", "stephen", "steve"}, {"edward", "ed", "eddie", "ted"}, {"elizabeth", "liz", "beth"},
    {"kathleen", "kathy", "kate"}, {"katherine", "kathy", "kate", "katie"}, {"daniel", "dan", "danny"},
    {"anthony", "tony"}, {"charles", "charlie", "chuck"}, {"gregory", "greg"}, {"matthew", "matt"},
    {"christopher", "chris"}, {"jonathan", "jon"}, {"timothy", "tim"}, {"joshua", "josh"},
    {"frederick", "fred"}, {"patricia", "pat"}, {"patrick", "pat"}, {"ronald", "ron"}, {"donald", "don"},
    {"lawrence", "larry"}, {"samuel", "sam"}, {"victoria", "vicki"}, {"jennifer", "jen", "jenny"},
]


def _expand(names: set[str]) -> set[str]:
    out = set(names)
    for group in NICKNAMES:
        if out & group:
            out |= group
    return out


def compatible(a: str, b: str) -> bool:
    fa, fb = _expand(first_names(a)), _expand(first_names(b))
    if not fa or not fb:
        return True
    if fa & fb:
        return True
    initials_a = {w[0] for w in tokens(a)[0][:-1]}
    initials_b = {w[0] for w in tokens(b)[0][:-1]}
    return bool(initials_a & initials_b)


def main() -> int:
    cities = pd.read_csv(BASE / "inputs/cities_2020_top100.csv", dtype=str)
    fips = {str(x).zfill(7): cid for x, cid in zip(cities.roster_place_fips, cities.city_id)}
    roster = pd.read_csv(ROSTER, dtype=str, low_memory=False)
    roster["city_id"] = roster.place_fips.map(lambda x: fips.get(str(x).split(".")[0].zfill(7)))
    roster["yr"] = roster.year.astype(float).astype(int)
    roster["mo"] = pd.to_numeric(roster.month, errors="coerce")
    with_url = set(pd.read_csv(URLS, dtype=str, low_memory=False).candidate_year_id)
    coverage = pd.read_csv(BASE / "outputs/candidate_coverage.csv", dtype=str, low_memory=False)
    todo = coverage[(coverage.identity_link_basis == "unresolved_or_new_candidate")]
    accepted, review = [], []
    for _, f in todo.iterrows():
        city, year, month = str(f.city_id).zfill(7), int(f.year), int(f.election_date[5:7])
        pool = roster[(roster.city_id == city) & (roster.yr == year) & (roster.mo == month)]
        same = pool[pool.full_name.map(surname) == surname(f.candidate_name)]
        good = same[same.full_name.map(lambda n: compatible(n, f.candidate_name))]
        row = dict(candidate_election_id=f.candidate_election_id, election_id=f.election_id, candidate_name=f.candidate_name,
                   city_id=city, election_date=f.election_date)
        if len(good) == 1:
            g = good.iloc[0]
            accepted.append(dict(row, candidate_year_id=g.candidate_year_id, roster_full_name=g.full_name,
                                 link_basis="reviewed_name_variant_same_city_year_month",
                                 roster_has_url_rows=g.candidate_year_id in with_url))
        elif len(same) or len(good) > 1:
            review.append(dict(row, reason=f"{len(same)} same-surname roster rows, {len(good)} with compatible first names",
                               roster_candidates="; ".join(f"{a}={b}" for a, b in zip(same.candidate_year_id, same.full_name))))
    out = BASE / "linkage"
    out.mkdir(exist_ok=True)
    pd.DataFrame(accepted).to_csv(out / "identity_links.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    pd.DataFrame(review).to_csv(out / "identity_links_review.csv", index=False)
    print(f"accepted {len(accepted)} (with URL rows {sum(a['roster_has_url_rows'] for a in accepted)}), review {len(review)}, unmatched {len(todo) - len(accepted) - len(review)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
