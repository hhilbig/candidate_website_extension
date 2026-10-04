#!/usr/bin/env python3
"""Create a deterministic review queue for suspicious website captures.

The audit combines hard cross-file/geography checks with conservative content
signals.  Hard invariant failures are labelled ``error``.  Signals that need a
human to inspect the archived pages are labelled ``review_flag``.  This script
never changes the corpus, the candidate universe, or an adjudication file.

Examples
--------
    python scripts/audit_capture_validity.py build/release_candidate
    python scripts/audit_capture_validity.py build/release_candidate \
        --output-dir build/capture_validity_audit --fail-on-error
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import unicodedata
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse

import pandas as pd


QUEUE_COLUMNS = [
    "finding_class",
    "category",
    "signal",
    "proposed_action",
    "review_status",
    "candidate_cycle_id",
    "candidate",
    "state",
    "district",
    "office",
    "year",
    "n_raw_rows",
    "n_text_rows",
    "n_distinct_domains",
    "evidence",
]

REQUIRED_CROSSWALK_COLUMNS = {
    "candidate_cycle_id",
    "candidate",
    "state",
    "district",
    "office",
    "year",
}
REQUIRED_RAW_COLUMNS = {"candidate_cycle_id", "text_snap_content"}
OPTIONAL_RAW_COLUMNS = {
    "state", "district", "office", "year", "urlkey", "page_type", "date"
}

STATE_NAMES = {
    "AL": "alabama",
    "AK": "alaska",
    "AZ": "arizona",
    "AR": "arkansas",
    "CA": "california",
    "CO": "colorado",
    "CT": "connecticut",
    "DE": "delaware",
    "FL": "florida",
    "GA": "georgia",
    "HI": "hawaii",
    "ID": "idaho",
    "IL": "illinois",
    "IN": "indiana",
    "IA": "iowa",
    "KS": "kansas",
    "KY": "kentucky",
    "LA": "louisiana",
    "ME": "maine",
    "MD": "maryland",
    "MA": "massachusetts",
    "MI": "michigan",
    "MN": "minnesota",
    "MS": "mississippi",
    "MO": "missouri",
    "MT": "montana",
    "NE": "nebraska",
    "NV": "nevada",
    "NH": "new hampshire",
    "NJ": "new jersey",
    "NM": "new mexico",
    "NY": "new york",
    "NC": "north carolina",
    "ND": "north dakota",
    "OH": "ohio",
    "OK": "oklahoma",
    "OR": "oregon",
    "PA": "pennsylvania",
    "RI": "rhode island",
    "SC": "south carolina",
    "SD": "south dakota",
    "TN": "tennessee",
    "TX": "texas",
    "UT": "utah",
    "VT": "vermont",
    "VA": "virginia",
    "WA": "washington",
    "WV": "west virginia",
    "WI": "wisconsin",
    "WY": "wyoming",
    "DC": "district of columbia",
    "AS": "american samoa",
    "GU": "guam",
    "MP": "northern mariana islands",
    "PR": "puerto rico",
    "VI": "virgin islands",
}
STATE_BY_NAME = {name: code for code, name in STATE_NAMES.items()}
STATE_TOKENS = {code: tuple(name.split()) for code, name in STATE_NAMES.items()}

FOREIGN_CONTENT_MARKERS = (
    "cape town", "south africa", "indonesia", "jakarta", "bahasa indonesia",
    "united kingdom", "australia", "new zealand", "singapore", "malaysia", "philippines",
)
THIRD_PARTY_CONTENT_MARKERS = ("ballotpedia", "wikipedia", "vote smart", "votesmart", "opensecrets")
SPAM_CONTENT_MARKERS = (
    "domain is for sale", "domain for sale", "buy this domain", "parked domain",
    "online casino", "sportsbook", "online betting", "slot game", "online slot", "poker bonus",
)
STATE_LEGISLATIVE_MARKERS = (
    "state representative", "state house", "state senate",
    "house of representatives district", "senate district",
)
THIRD_PARTY_DOMAINS = {
    "ballotpedia.org",
    "facebook.com",
    "instagram.com",
    "opensecrets.org",
    "twitter.com",
    "votesmart.org",
    "wikipedia.org",
    "x.com",
    "youtube.com",
}
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "phd", "md"}
_CONTENT_FLAG_CACHE: OrderedDict[
    bytes, tuple[bool, bool, bool, bool, bool]
] = OrderedDict()
_CONTENT_FLAG_CACHE_SIZE = 50_000
CJK_RE = re.compile(r"[\u3400-\u9fff]")
RUNAWAY_PAGES_PER_DAY = 100
EXTREME_RAW_ROWS = 10_000
EXTREME_SNAPSHOT_TIMESTAMPS = 2_000


def _plain(value: Any) -> str:
    """Return a stable, accent-insensitive lowercase representation."""
    if value is None or value is pd.NA:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", text).strip().lower()


def _display(value: Any) -> str:
    """Convert nullable values to a stable CSV-friendly string."""
    text = _plain(value)
    return "" if not text else str(value).strip()


def _content(value: Any) -> str:
    """Normalize corpus text cheaply; content rules use ASCII marker strings."""
    if value is None or value is pd.NA:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).lower()


def _contains_any(text: str, markers: Iterable[str]) -> bool:
    return any(marker in text for marker in markers)


def _content_flags(value: Any) -> tuple[bool, bool, bool, bool, bool]:
    """Evaluate corpus-wide markers once per repeated text body.

    Exact pages recur across snapshots.  A bounded digest cache avoids repeatedly
    lowercasing and scanning those long bodies while retaining bounded memory.
    """
    if value is None or value is pd.NA:
        return False, False, False, False, False
    raw = str(value)
    if not raw:
        return False, False, False, False, False
    digest = hashlib.blake2b(raw.encode("utf-8"), digest_size=16).digest()
    cached = _CONTENT_FLAG_CACHE.get(digest)
    if cached is not None:
        _CONTENT_FLAG_CACHE.move_to_end(digest)
        return cached
    text = raw.lower()
    # The compiled search runs in C and avoids a Python character loop for the
    # overwhelmingly ASCII corpus. Only candidate CJK pages need an exact count.
    cjk = len(CJK_RE.findall(raw)) if CJK_RE.search(raw) else 0
    flags = (
        _contains_any(text, FOREIGN_CONTENT_MARKERS),
        _contains_any(text, THIRD_PARTY_CONTENT_MARKERS),
        _contains_any(text, SPAM_CONTENT_MARKERS),
        _contains_any(text, STATE_LEGISLATIVE_MARKERS),
        cjk >= 40 and cjk / max(len(raw), 1) >= 0.25,
    )
    _CONTENT_FLAG_CACHE[digest] = flags
    if len(_CONTENT_FLAG_CACHE) > _CONTENT_FLAG_CACHE_SIZE:
        _CONTENT_FLAG_CACHE.popitem(last=False)
    return flags


def _district(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) else number


def _district_key(value: Any) -> str:
    number = _district(value)
    if number is None:
        return ""
    return str(int(number)) if number.is_integer() else str(number)


def _year_key(value: Any) -> str:
    number = _district(value)
    if number is None:
        return ""
    return str(int(number)) if number.is_integer() else str(number)


def _name_parts(candidate: Any) -> tuple[str, str]:
    parts = re.findall(r"[a-z]+", _plain(candidate))
    while parts and parts[-1] in SUFFIXES:
        parts.pop()
    if not parts:
        return "", ""
    return parts[0], parts[-1]


@lru_cache(maxsize=16_384)
def _cached_host(raw: str) -> str:
    parsed = urlparse(raw if "://" in raw else f"//{raw}")
    host = (parsed.hostname or "").lower().strip(".")
    return re.sub(r"^www\d*\.", "", host)


def _marker_excerpt(value: Any, markers: Iterable[str] = ()) -> str:
    """Return a compact evidence excerpt around the first matching marker."""
    text = re.sub(r"\s+", " ", "" if value is None else str(value)).strip()
    if not text:
        return ""
    lowered = text.lower()
    positions = [lowered.find(marker) for marker in markers if marker in lowered]
    center = min(positions) if positions else 0
    start = max(0, center - 70)
    end = min(len(text), center + 150)
    excerpt = text[start:end]
    return ("…" if start else "") + excerpt + ("…" if end < len(text) else "")


def _host(value: Any) -> str:
    raw = _plain(value)
    if not raw:
        return ""
    return _cached_host(raw)


def _domain_matches(host: str, domains: Iterable[str]) -> bool:
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)


def _word_pattern(token: str) -> re.Pattern[str] | None:
    return re.compile(rf"\b{re.escape(token)}\b") if token else None


@dataclass
class CandidateStats:
    candidate_cycle_id: str
    candidate: str = ""
    state: str = ""
    district: Any = ""
    office: str = ""
    year: Any = ""
    n_raw_rows: int = 0
    n_text_rows: int = 0
    n_homepage_text_rows: int = 0
    n_first_last_rows: int = 0
    n_surname_rows: int = 0
    n_foreign_rows: int = 0
    n_third_party_rows: int = 0
    n_spam_rows: int = 0
    n_state_legislative_rows: int = 0
    n_high_cjk_rows: int = 0
    total_text_chars: int = 0
    max_text_chars: int = 0
    domains: set[str] = field(default_factory=set)
    timestamps: set[str] = field(default_factory=set)
    snapshot_days: set[str] = field(default_factory=set)
    pages_by_day: Counter[str] = field(default_factory=Counter)
    state_rows: Counter[str] = field(default_factory=Counter)
    raw_states: Counter[str] = field(default_factory=Counter)
    raw_districts: Counter[str] = field(default_factory=Counter)
    raw_offices: Counter[str] = field(default_factory=Counter)
    raw_years: Counter[str] = field(default_factory=Counter)
    content_excerpts: dict[str, str] = field(default_factory=dict)
    _first_pattern: re.Pattern[str] | None = field(init=False, repr=False)
    _surname_pattern: re.Pattern[str] | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        first, surname = _name_parts(self.candidate)
        self._first_pattern = _word_pattern(first)
        self._surname_pattern = _word_pattern(surname)

    def add_raw_row(self, row: dict[str, Any]) -> None:
        self.n_raw_rows += 1
        raw_values = (
            (self.raw_states, _plain(row.get("state")).upper()),
            (self.raw_districts, _district_key(row.get("district"))),
            (self.raw_offices, _plain(row.get("office"))),
            (self.raw_years, _year_key(row.get("year"))),
        )
        for counter, value in raw_values:
            if value:
                counter[value] += 1

        timestamp = re.match(r"^(\d{8})(\d{6})?", _display(row.get("date")))
        if timestamp:
            full = timestamp.group(1) + (timestamp.group(2) or "")
            self.timestamps.add(full)
            self.snapshot_days.add(timestamp.group(1))
            self.pages_by_day[timestamp.group(1)] += 1

        host = _host(row.get("urlkey"))
        if host:
            self.domains.add(host)

        raw_text = row.get("text_snap_content")
        if raw_text is None or raw_text is pd.NA or not str(raw_text):
            return
        self.n_text_rows += 1
        text_chars = len(str(raw_text))
        self.total_text_chars += text_chars
        self.max_text_chars = max(self.max_text_chars, text_chars)
        is_homepage = _plain(row.get("page_type")) == "homepage"
        if is_homepage:
            self.n_homepage_text_rows += 1
            homepage_text = _content(raw_text)
            first_match = bool(self._first_pattern and self._first_pattern.search(homepage_text))
            surname_match = bool(self._surname_pattern and self._surname_pattern.search(homepage_text))
            self.n_surname_rows += int(surname_match)
            self.n_first_last_rows += int(first_match and surname_match)

        foreign, third_party, spam, state_legislative, high_cjk = _content_flags(raw_text)
        self.n_foreign_rows += int(foreign)
        self.n_third_party_rows += int(third_party)
        self.n_spam_rows += int(spam)
        self.n_state_legislative_rows += int(state_legislative)
        self.n_high_cjk_rows += int(high_cjk)
        excerpt_specs = (
            (foreign, "foreign_location_content_dominates", FOREIGN_CONTENT_MARKERS),
            (third_party, "third_party_content_dominates", THIRD_PARTY_CONTENT_MARKERS),
            (spam, "parked_or_spam_content_dominates", SPAM_CONTENT_MARKERS),
            (state_legislative, "state_legislative_content_dominates", STATE_LEGISLATIVE_MARKERS),
            (high_cjk, "high_cjk_content_dominates", ()),
        )
        for matched, signal, markers in excerpt_specs:
            if matched:
                excerpt = _marker_excerpt(raw_text, markers)
                previous = self.content_excerpts.get(signal)
                if excerpt and (previous is None or excerpt < previous):
                    self.content_excerpts[signal] = excerpt
        # Full state scanning is only necessary for official congressional
        # domains, where a state mismatch is strong identity evidence.  Running
        # a 56-name alternation over every long article in the corpus is both
        # slow and too permissive for general campaign sites that discuss other
        # states.  Tokenize each official-domain page once instead.
        if host.endswith(".house.gov") or host.endswith(".senate.gov"):
            text = _content(raw_text)
            words = set(re.findall(r"[a-z]+", text))
            for code, tokens in STATE_TOKENS.items():
                if all(token in words for token in tokens):
                    if len(tokens) == 1 or " ".join(tokens) in text:
                        self.state_rows[code] += 1


def _crosswalk_records(crosswalk: pd.DataFrame) -> tuple[dict[str, CandidateStats], list[str]]:
    missing = REQUIRED_CROSSWALK_COLUMNS - set(crosswalk.columns)
    if missing:
        raise ValueError(f"crosswalk is missing required columns: {sorted(missing)}")

    cw = crosswalk.copy()
    cw["candidate_cycle_id"] = cw["candidate_cycle_id"].map(_display)
    duplicates = sorted(cw.loc[cw.candidate_cycle_id.duplicated(keep=False), "candidate_cycle_id"].unique())
    records: dict[str, CandidateStats] = {}
    for row in cw.drop_duplicates("candidate_cycle_id", keep="first").to_dict("records"):
        candidate_id = row["candidate_cycle_id"]
        records[candidate_id] = CandidateStats(
            candidate_cycle_id=candidate_id,
            candidate=_display(row.get("candidate")),
            state=_plain(row.get("state")).upper(),
            district=row.get("district"),
            office=_plain(row.get("office")),
            year=row.get("year"),
        )
    return records, duplicates


def _iter_raw_records(raw: pd.DataFrame | str | Path) -> Iterator[dict[str, Any]]:
    if isinstance(raw, pd.DataFrame):
        missing = REQUIRED_RAW_COLUMNS - set(raw.columns)
        if missing:
            raise ValueError(f"raw corpus is missing required columns: {sorted(missing)}")
        yield from raw.to_dict("records")
        return

    import pyarrow.parquet as pq

    path = Path(raw)
    parquet = pq.ParquetFile(path)
    available = set(parquet.schema.names)
    missing = REQUIRED_RAW_COLUMNS - available
    if missing:
        raise ValueError(f"raw corpus is missing required columns: {sorted(missing)}")
    columns = sorted(REQUIRED_RAW_COLUMNS | (OPTIONAL_RAW_COLUMNS & available))
    for batch in parquet.iter_batches(columns=columns, batch_size=20_000):
        yield from batch.to_pylist()


def _finding(
    stats: CandidateStats,
    finding_class: str,
    category: str,
    signal: str,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    return {
        "finding_class": finding_class,
        "category": category,
        "signal": signal,
        "proposed_action": "manual_review",
        "review_status": "pending",
        "candidate_cycle_id": stats.candidate_cycle_id,
        "candidate": stats.candidate,
        "state": stats.state,
        "district": _district_key(stats.district),
        "office": stats.office,
        "year": _year_key(stats.year),
        "n_raw_rows": stats.n_raw_rows,
        "n_text_rows": stats.n_text_rows,
        "n_distinct_domains": len(stats.domains),
        "evidence": json.dumps(evidence, sort_keys=True, separators=(",", ":")),
    }


def _share(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _candidate_findings(stats: CandidateStats) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    district = _district(stats.district)

    if stats.office not in {"house", "senate"}:
        findings.append(_finding(
            stats, "error", "geography", "office_not_federal",
            {"observed_office": stats.office},
        ))
    elif stats.office == "house" and (
        district is None or district < 0 or district > 53 or not district.is_integer()
    ):
        findings.append(_finding(
            stats, "error", "geography", "house_district_out_of_range",
            {"allowed": "integer 0 through 53", "observed_district": _display(stats.district)},
        ))
    elif stats.office == "senate" and district not in {None, 0.0}:
        findings.append(_finding(
            stats, "error", "geography", "senate_district_nonzero",
            {"allowed": "missing or 0", "observed_district": _display(stats.district)},
        ))

    if stats.state not in STATE_NAMES:
        findings.append(_finding(
            stats, "error", "geography", "state_code_invalid",
            {"observed_state": stats.state},
        ))

    expected = {
        "state": {stats.state} if stats.state else set(),
        "district": {_district_key(stats.district)} if _district_key(stats.district) else set(),
        "office": {stats.office} if stats.office else set(),
        "year": {_year_key(stats.year)} if _year_key(stats.year) else set(),
    }
    observed_counters = {
        "state": stats.raw_states,
        "district": stats.raw_districts,
        "office": stats.raw_offices,
        "year": stats.raw_years,
    }
    for field_name in ("state", "district", "office", "year"):
        # Senate district 0 and missing both mean that the office is statewide.
        # The separate senate_district_nonzero rule still catches a substantive
        # district value in the crosswalk.
        if field_name == "district" and stats.office == "senate":
            continue
        observed = set(observed_counters[field_name])
        if observed and observed != expected[field_name]:
            findings.append(_finding(
                stats, "error", "cross_file", f"raw_crosswalk_{field_name}_mismatch",
                {
                    "crosswalk": sorted(expected[field_name]),
                    "raw_counts": {
                        key: int(observed_counters[field_name][key])
                        for key in sorted(observed_counters[field_name])
                    },
                },
            ))

    if stats.n_raw_rows == 0:
        findings.append(_finding(
            stats, "error", "cross_file", "crosswalk_candidate_missing_raw",
            {"raw_rows": 0},
        ))
        return findings

    max_pages_one_day = max(stats.pages_by_day.values(), default=0)
    if max_pages_one_day > RUNAWAY_PAGES_PER_DAY:
        findings.append(_finding(
            stats, "review_flag", "volume", "runaway_snapshot_day",
            {
                "threshold_pages": RUNAWAY_PAGES_PER_DAY,
                "max_pages_one_day": max_pages_one_day,
                "n_snapshot_days": len(stats.snapshot_days),
            },
        ))
    if stats.n_raw_rows > EXTREME_RAW_ROWS:
        findings.append(_finding(
            stats, "review_flag", "volume", "extreme_raw_page_count",
            {"threshold_rows": EXTREME_RAW_ROWS, "observed_rows": stats.n_raw_rows},
        ))
    if len(stats.timestamps) > EXTREME_SNAPSHOT_TIMESTAMPS:
        findings.append(_finding(
            stats, "review_flag", "volume", "extreme_snapshot_count",
            {
                "threshold_timestamps": EXTREME_SNAPSHOT_TIMESTAMPS,
                "observed_timestamps": len(stats.timestamps),
            },
        ))

    if stats.n_homepage_text_rows >= 2:
        surname_share = _share(stats.n_surname_rows, stats.n_homepage_text_rows)
        full_name_share = _share(stats.n_first_last_rows, stats.n_homepage_text_rows)
        if surname_share == 0 and full_name_share == 0:
            findings.append(_finding(
                stats, "review_flag", "identity", "candidate_name_low_support",
                {
                    "homepage_text_rows": stats.n_homepage_text_rows,
                    "first_and_last_name_rows": stats.n_first_last_rows,
                    "first_and_last_name_homepage_share": full_name_share,
                    "surname_rows": stats.n_surname_rows,
                    "surname_homepage_share": surname_share,
                },
            ))

    content_signals = [
        ("foreign_location_content_dominates", "geography", stats.n_foreign_rows),
        ("third_party_content_dominates", "domain", stats.n_third_party_rows),
        ("parked_or_spam_content_dominates", "domain", stats.n_spam_rows),
        ("state_legislative_content_dominates", "geography", stats.n_state_legislative_rows),
        ("high_cjk_content_dominates", "domain", stats.n_high_cjk_rows),
    ]
    for signal, category, count in content_signals:
        minimum = 2 if signal == "state_legislative_content_dominates" else 3
        share = _share(count, stats.n_text_rows)
        if count >= minimum and share >= 0.5:
            findings.append(_finding(
                stats, "review_flag", category, signal,
                {
                    "matching_text_rows": count,
                    "matching_text_share": share,
                    "evidence_excerpt": stats.content_excerpts.get(signal, ""),
                },
            ))

    expected_state_rows = stats.state_rows.get(stats.state, 0)
    other_state_rows = [(count, code) for code, count in stats.state_rows.items() if code != stats.state]
    state_mismatch = False
    if other_state_rows:
        dominant_count, dominant_state = sorted(other_state_rows, key=lambda pair: (-pair[0], pair[1]))[0]
        dominant_share = _share(dominant_count, stats.n_text_rows)
        expected_share = _share(expected_state_rows, stats.n_text_rows)
        if dominant_count >= 3 and dominant_share >= 0.8 and expected_share <= 0.1:
            state_mismatch = True
            findings.append(_finding(
                stats, "review_flag", "geography", "text_state_mismatch",
                {
                    "dominant_other_state": dominant_state,
                    "dominant_other_state_rows": dominant_count,
                    "dominant_other_state_share": dominant_share,
                    "expected_state": stats.state,
                    "expected_state_rows": expected_state_rows,
                    "expected_state_share": expected_share,
                },
            ))

    official_domains = sorted(
        host for host in stats.domains
        if host.endswith(".house.gov") or host.endswith(".senate.gov")
    )
    if state_mismatch and official_domains:
        findings.append(_finding(
            stats, "review_flag", "domain", "official_domain_geography_mismatch",
            {"domains": official_domains, "expected_state": stats.state},
        ))

    third_party_domains = sorted(host for host in stats.domains if _domain_matches(host, THIRD_PARTY_DOMAINS))
    if third_party_domains:
        findings.append(_finding(
            stats, "review_flag", "domain", "third_party_capture_domain",
            {"domains": third_party_domains},
        ))
    return findings


def audit_capture_validity(
    raw: pd.DataFrame | str | Path,
    crosswalk: pd.DataFrame,
) -> pd.DataFrame:
    """Return the review queue without mutating either input.

    Each row is one independently reviewable signal.  A candidate can therefore
    appear more than once.  The stable sort makes the CSV byte-identical when
    the same inputs are audited again.
    """
    stats_by_id, duplicate_ids = _crosswalk_records(crosswalk)
    unknown: dict[str, CandidateStats] = {}

    for row in _iter_raw_records(raw):
        candidate_id = _display(row.get("candidate_cycle_id"))
        if candidate_id in stats_by_id:
            stats = stats_by_id[candidate_id]
        else:
            stats = unknown.setdefault(candidate_id, CandidateStats(candidate_cycle_id=candidate_id))
        stats.add_raw_row(row)

    findings: list[dict[str, Any]] = []
    for candidate_id in duplicate_ids:
        stats = stats_by_id[candidate_id]
        findings.append(_finding(
            stats, "error", "cross_file", "crosswalk_duplicate_candidate_cycle_id",
            {"candidate_cycle_id": candidate_id},
        ))
    for candidate_id in sorted(stats_by_id):
        findings.extend(_candidate_findings(stats_by_id[candidate_id]))

    shared_domains: dict[tuple[str, str], list[str]] = {}
    for candidate_id, stats in stats_by_id.items():
        year = _year_key(stats.year)
        for domain in stats.domains:
            shared_domains.setdefault((year, domain), []).append(candidate_id)
    for (year, domain), candidate_ids in sorted(shared_domains.items()):
        unique_ids = sorted(set(candidate_ids))
        if len(unique_ids) < 2:
            continue
        for candidate_id in unique_ids:
            findings.append(_finding(
                stats_by_id[candidate_id],
                "review_flag",
                "domain",
                "shared_year_domain",
                {
                    "year": year,
                    "normalized_full_host": domain,
                    "candidate_cycle_ids": unique_ids,
                    "n_candidate_cycle_ids": len(unique_ids),
                    "normalization_rule": "lowercase host; strip leading www or wwwN",
                },
            ))
    for candidate_id in sorted(unknown):
        stats = unknown[candidate_id]
        findings.append(_finding(
            stats, "error", "cross_file", "raw_candidate_missing_crosswalk",
            {"candidate_cycle_id": candidate_id},
        ))

    queue = pd.DataFrame(findings, columns=QUEUE_COLUMNS)
    if queue.empty:
        return queue
    queue["_class_order"] = queue.finding_class.map({"error": 0, "review_flag": 1}).fillna(2)
    queue = queue.sort_values(
        ["_class_order", "candidate_cycle_id", "category", "signal"],
        kind="mergesort",
    ).drop(columns="_class_order").reset_index(drop=True)
    return queue


def enrich_queue_with_decisions(queue: pd.DataFrame, decisions: pd.DataFrame) -> pd.DataFrame:
    """Mark queue rows covered by an external, already-reviewed decision map.

    The helper only annotates a copy of the queue.  It does not filter raw rows,
    modify the decision map, or write files.  Decisions may provide
    ``candidate_cycle_id`` directly or the pair ``cand_id`` and ``year``.
    """
    out = queue.copy(deep=True)
    if decisions.empty:
        return out
    if "action" not in decisions:
        raise ValueError("decisions is missing required column: action")

    reviewed = decisions.copy()
    if "candidate_cycle_id" not in reviewed:
        missing = {"cand_id", "year"} - set(reviewed.columns)
        if missing:
            raise ValueError(
                "decisions requires candidate_cycle_id or cand_id and year; "
                f"missing {sorted(missing)}"
            )
        years = pd.to_numeric(reviewed["year"], errors="raise").astype(int).astype(str)
        reviewed["candidate_cycle_id"] = reviewed["cand_id"].map(_display) + "-" + years
    reviewed["candidate_cycle_id"] = reviewed["candidate_cycle_id"].map(_display)
    reviewed["action"] = reviewed["action"].map(_display)

    conflicting = reviewed.groupby("candidate_cycle_id").action.nunique().loc[lambda x: x > 1]
    if not conflicting.empty:
        raise ValueError(
            "conflicting decisions for candidate_cycle_id: "
            + ", ".join(sorted(conflicting.index))
        )
    action_by_id = (
        reviewed.drop_duplicates("candidate_cycle_id", keep="last")
        .set_index("candidate_cycle_id")["action"]
    )
    mapped = out.candidate_cycle_id.map(action_by_id)
    decided = mapped.notna() & mapped.ne("")
    out.loc[decided, "proposed_action"] = mapped.loc[decided]
    out.loc[decided, "review_status"] = "approved"
    return out


def audit_report(
    queue: pd.DataFrame,
    *,
    raw_row_count: int,
    crosswalk_candidate_count: int,
) -> dict[str, Any]:
    """Build a machine-readable deterministic summary of a review queue."""
    if queue.empty:
        by_class: dict[str, int] = {}
        by_signal: dict[str, int] = {}
        candidates_with_findings = 0
    else:
        by_class = {str(k): int(v) for k, v in sorted(queue.finding_class.value_counts().items())}
        by_signal = {str(k): int(v) for k, v in sorted(queue.signal.value_counts().items())}
        candidates_with_findings = int(queue.candidate_cycle_id.nunique())
    return {
        "schema_version": "1.0",
        "crosswalk_candidate_count": int(crosswalk_candidate_count),
        "raw_row_count": int(raw_row_count),
        "findings_total": int(len(queue)),
        "candidates_with_findings": candidates_with_findings,
        "counts_by_finding_class": by_class,
        "counts_by_signal": by_signal,
        "counts_by_review_status": {
            str(k): int(v)
            for k, v in sorted(queue.review_status.value_counts().items())
        } if not queue.empty else {},
        "counts_by_proposed_action": {
            str(k): int(v)
            for k, v in sorted(queue.proposed_action.value_counts().items())
        } if not queue.empty else {},
        "automatic_exclusions_applied": 0,
        "required_next_step": "Human review; record any decision in the adjudication ledger separately.",
    }


def _parquet_row_count(path: Path) -> int:
    import pyarrow.parquet as pq

    return int(pq.ParquetFile(path).metadata.num_rows)


def write_audit_outputs(
    queue: pd.DataFrame,
    report: dict[str, Any],
    output_dir: str | Path,
) -> tuple[Path, Path]:
    """Write a queue CSV and JSON report without touching release inputs."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    queue_path = output / "capture_validity_review_queue.csv"
    report_path = output / "capture_validity_report.json"
    queue.to_csv(queue_path, index=False, lineterminator="\n")
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return queue_path, report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "release_dir",
        type=Path,
        help="Directory containing raw_corpus.parquet and candidate_crosswalk.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("build/capture_validity_audit"),
        help="Destination for the review queue and report (default: %(default)s)",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Return exit status 1 when hard invariant errors are present",
    )
    parser.add_argument(
        "--decisions",
        action="append",
        type=Path,
        default=[],
        help=("optional reviewed adjudication CSV used only to annotate the "
              "queue; may be supplied more than once"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_path = args.release_dir / "raw_corpus.parquet"
    crosswalk_path = args.release_dir / "candidate_crosswalk.csv"
    if not raw_path.is_file():
        raise FileNotFoundError(raw_path)
    if not crosswalk_path.is_file():
        raise FileNotFoundError(crosswalk_path)

    crosswalk = pd.read_csv(crosswalk_path, low_memory=False)
    queue = audit_capture_validity(raw_path, crosswalk)
    for decision_path in args.decisions:
        if not decision_path.is_file():
            raise FileNotFoundError(decision_path)
        queue = enrich_queue_with_decisions(
            queue, pd.read_csv(decision_path, dtype=str)
        )
    report = audit_report(
        queue,
        raw_row_count=_parquet_row_count(raw_path),
        crosswalk_candidate_count=crosswalk.candidate_cycle_id.nunique(),
    )
    queue_path, report_path = write_audit_outputs(queue, report, args.output_dir)
    print(f"review queue: {queue_path} ({len(queue):,} findings)")
    print(f"audit report: {report_path}")
    print("No candidates were excluded and no adjudication files were changed.")
    has_error = not queue.empty and queue.finding_class.eq("error").any()
    return int(args.fail_on_error and has_error)


if __name__ == "__main__":
    raise SystemExit(main())
