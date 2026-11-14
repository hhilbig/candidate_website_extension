#!/usr/bin/env python3
"""Build the rectangular candidate universe and candidate-year provenance.

The universe begins with FEC records, not successful URL or archive records.
It covers House candidates in 2018--2024 and Senate candidates in 2002--2024,
keeps same-election-year Democratic/Republican/DFL candidacies, restricts the
Senate to state-years with a general election, applies reviewed universe
adjudications, and adds reviewed ballot exceptions. Historical URL, capture,
and ballot evidence is then left-joined by ``candidate_cycle_id``.

The provenance product has one row per universe row. Its collection fields are
evidence bounded: a released successful page does not reveal whether all CDX
records were available, why another attempt failed, or whether a legacy cap
bound. Those fields therefore remain ``unknown`` unless a future source records
the missing evidence explicitly.
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Mapping
from urllib.parse import urlsplit, urlunsplit

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.coverage_vs_returns import (  # noqa: E402
    ballot as load_ballot,
    keys as ballot_keys,
    reverse_match,
)
from scripts.icpsr_replicate_coding import election_day  # noqa: E402
from src.build_candidate_roster import PARTY_MAP  # noqa: E402
from src.name_utils import clean_name  # noqa: E402
from src.release_adjudication import apply_universe_adjudication  # noqa: E402
from src.scrape_wayback import _clean_campaign_url  # noqa: E402

HOUSE_YEARS = frozenset(range(2018, 2025, 2))
SENATE_YEARS = frozenset(range(2002, 2025, 2))
VALID_MEASURE_STATUSES = frozenset({
    "observed",
    "observed_zero",
    "insufficient_clean_text",
    "scope_not_observed",
    "not_computed",
})
UNKNOWN_EVIDENCE = "unknown"
COLLECTION_METHOD_UNKNOWN = "legacy_mixed_unknown"
WAYBACK_PREFIX_RE = re.compile(
    r"^https?://web\.archive\.org/web/\d+(?:[a-z_]+)?/(.+)$",
    flags=re.IGNORECASE,
)
DATE_RE = re.compile(r"^(\d{8})(\d{6})?")


def _string(value) -> str:
    """Return a stripped string, treating pandas missing values as blank."""
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _bool_or_missing(value):
    """Normalize common CSV boolean encodings without treating NaN as true."""
    if value is None or pd.isna(value) or _string(value) == "":
        return pd.NA
    if isinstance(value, bool):
        return value
    lowered = _string(value).lower()
    if lowered in {"true", "t", "1", "yes"}:
        return True
    if lowered in {"false", "f", "0", "no"}:
        return False
    raise ValueError(f"unrecognized boolean value: {value!r}")


def normalize_website_url(value) -> str | None:
    """Return one deterministic accepted campaign URL, or ``None``.

    Validation is deliberately identical to the scraper. The normalized value
    lowercases the scheme and host, removes a leading ``www.``, discards a URL
    fragment, and removes a trailing slash from non-root paths. The historical
    ``website_url`` itself is never rewritten.
    """
    accepted = _clean_campaign_url(value)
    if not accepted:
        return None
    parsed_value = accepted if "://" in accepted else f"https://{accepted}"
    try:
        parsed = urlsplit(parsed_value)
        port_value = parsed.port
    except ValueError:
        return None
    host = (parsed.hostname or "").lower().strip(".")
    host = re.sub(r"^www\d*\.", "", host)
    if not host:
        return None
    port = f":{port_value}" if port_value else ""
    path = parsed.path or ""
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), host + port, path,
                       parsed.query, ""))


def normalize_website_domain(value) -> str | None:
    """Return the normalized host used in ``website_cycle_id``.

    This is a host normalization, not an inferred registrable-domain lookup.
    It strips only a leading ``www`` label and therefore does not merge distinct
    subdomains without evidence that they are the same campaign website.
    """
    normalized = normalize_website_url(value)
    if not normalized:
        return None
    return (urlsplit(normalized).hostname or "").lower() or None


def website_cycle_id(value, year: int) -> str | None:
    """Return ``normalized-host-year`` without inventing a person identifier."""
    domain = normalize_website_domain(value)
    return f"{domain}-{int(year)}" if domain else None


def _normalize_page_url(value) -> str | None:
    """Normalize an observed original page URL for provenance counts."""
    raw = _string(value)
    if not raw:
        return None
    match = WAYBACK_PREFIX_RE.match(raw)
    if match:
        raw = match.group(1)
    parsed_value = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlsplit(parsed_value)
    except ValueError:
        return None
    host = (parsed.hostname or "").lower().strip(".")
    if not host:
        return None
    host = re.sub(r"^www\d*\.", "", host)
    try:
        port_value = parsed.port
    except ValueError:
        # Some legacy captures contain impossible numeric ports. The hostname
        # and path still identify the observed page; omit only the invalid port.
        port_value = None
    port = f":{port_value}" if port_value else ""
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), host + port, path,
                       parsed.query, ""))


def _read_senate_races(path: Path) -> set[tuple[int, str]]:
    data = pd.read_csv(path, low_memory=False, encoding="latin-1")
    data.columns = [str(c).strip() for c in data.columns]
    general = data[data["stage"].astype(str).str.upper().eq("GEN")]
    return set(zip(general["year"].astype(int),
                   general["state_po"].astype(str).str.upper()))


def load_fec_records(fec_dir: Path) -> pd.DataFrame:
    """Load cached FEC files and retain their file-cycle provenance."""
    frames = []
    for path in sorted(fec_dir.glob("cn*.csv")):
        match = re.fullmatch(r"cn(\d{4})", path.stem)
        if not match:
            continue
        frame = pd.read_csv(path, dtype=str)
        frame["file_year"] = int(match.group(1))
        frames.append(frame)
    if not frames:
        raise FileNotFoundError(f"no cnYYYY.csv files found under {fec_dir}")
    return pd.concat(frames, ignore_index=True)


def select_fec_population(
    fec_records: pd.DataFrame,
    senate_races: set[tuple[int, str]],
) -> pd.DataFrame:
    """Select the approved same-election-year federal FEC population."""
    required = {
        "cand_id", "cand_name", "cand_pty_affiliation", "cand_election_yr",
        "cand_office_st", "cand_office", "cand_office_district",
        "cand_ici", "cand_status", "file_year",
    }
    missing = required - set(fec_records)
    if missing:
        raise ValueError(f"FEC input missing columns: {sorted(missing)}")

    rows = fec_records.copy()
    rows["file_year"] = pd.to_numeric(rows["file_year"], errors="raise").astype(int)
    rows["cand_election_yr"] = rows["cand_election_yr"].astype("string").str.strip()
    rows = rows[rows["cand_election_yr"].eq(rows["file_year"].astype(str))]
    rows = rows[rows["cand_pty_affiliation"].isin(PARTY_MAP)]
    rows = rows[rows["cand_office"].isin(["H", "S"])].copy()

    rows["year"] = rows["file_year"]
    rows["office"] = rows["cand_office"].map({"H": "house", "S": "senate"})
    rows["state"] = rows["cand_office_st"].astype("string").str.strip().str.upper()
    rows["party"] = rows["cand_pty_affiliation"].map(PARTY_MAP)
    scope = ((rows["office"].eq("house") & rows["year"].isin(HOUSE_YEARS))
             | (rows["office"].eq("senate") & rows["year"].isin(SENATE_YEARS)))
    rows = rows[scope].copy()

    race_key = list(zip(rows["year"], rows["state"]))
    rows = rows[~rows["office"].eq("senate")
                | pd.Series([key in senate_races for key in race_key],
                            index=rows.index)].copy()

    rows["cand_id"] = rows["cand_id"].astype("string").str.strip()
    rows = rows[rows["cand_id"].ne("")].copy()
    rows["candidate_cycle_id"] = rows["cand_id"] + "-" + rows["year"].astype(str)
    rows = (rows.sort_values(["candidate_cycle_id", "file_year"])
            .drop_duplicates("candidate_cycle_id", keep="first"))

    rows["candidate"] = rows["cand_name"].fillna("").map(clean_name)
    rows["fec_raw_name"] = rows["cand_name"].fillna("")
    rows["source_cand_id"] = rows["cand_id"]
    rows["source_district"] = rows["cand_office_district"].astype("string")
    rows["district"] = pd.to_numeric(rows["cand_office_district"], errors="coerce")
    rows["geography_status"] = "valid_house_district"
    senate = rows["office"].eq("senate")
    rows.loc[senate, "district"] = pd.NA
    rows.loc[senate, "geography_status"] = "senate_not_applicable"
    missing_house = rows["office"].eq("house") & rows["district"].isna()
    rows.loc[missing_house, "geography_status"] = "missing_house_district"
    invalid_house = (rows["office"].eq("house") & rows["district"].notna()
                     & ~(rows["district"].eq(0) | rows["district"].between(1, 53)))
    rows.loc[invalid_house, "district"] = pd.NA
    rows.loc[invalid_house, "geography_status"] = "invalid_house_district"
    rows["universe_source"] = "same_year_fec"
    return rows


def _add_missing_ballot_exceptions(
    population: pd.DataFrame,
    fec_records: pd.DataFrame,
    identity_overrides: pd.DataFrame,
) -> pd.DataFrame:
    """Add reviewed ballot exceptions absent from the same-year FEC set."""
    if identity_overrides.empty:
        return population
    decisions = identity_overrides.copy()
    decisions["year"] = pd.to_numeric(decisions["year"], errors="raise").astype(int)
    decisions["office"] = decisions["office"].astype("string").str.lower().str.strip()
    decisions["state"] = decisions["state"].astype("string").str.upper().str.strip()
    decisions = decisions[decisions["action"].eq("retain_ballot_override")]
    present = set(population["candidate_cycle_id"])
    extras: list[dict] = []
    for decision in decisions.sort_values(["year", "canonical_cand_id"]).to_dict("records"):
        canonical = _string(decision["canonical_cand_id"])
        cycle_id = f"{canonical}-{int(decision['year'])}"
        if cycle_id in present:
            continue
        source = _string(decision["source_cand_id"])
        candidates = fec_records[
            fec_records["cand_id"].astype("string").str.strip().eq(source)
        ].copy()
        candidates = candidates[
            candidates["cand_office"].map({"H": "house", "S": "senate"})
            .eq(decision["office"])
        ]
        if candidates.empty:
            raise ValueError(f"no FEC metadata found for ballot exception {cycle_id}")
        candidates["_distance"] = (
            pd.to_numeric(candidates["file_year"], errors="coerce")
            .sub(int(decision["year"])).abs()
        )
        source_row = candidates.sort_values(["_distance", "file_year"]).iloc[0]
        party = PARTY_MAP.get(source_row["cand_pty_affiliation"])
        if party is None:
            raise ValueError(f"ballot exception is not D/R/DFL: {cycle_id}")
        raw_district = source_row["cand_office_district"]
        district = pd.to_numeric(pd.Series([raw_district]), errors="coerce").iloc[0]
        if decision["office"] == "senate":
            district = pd.NA
            geo_status = "senate_not_applicable"
        elif pd.isna(district):
            district = pd.NA
            geo_status = "missing_house_district"
        elif not (district == 0 or 1 <= district <= 53):
            district = pd.NA
            geo_status = "invalid_house_district"
        else:
            geo_status = "valid_house_district"
        extras.append({
            "cand_id": canonical,
            "candidate_cycle_id": cycle_id,
            "candidate": clean_name(source_row["cand_name"]),
            "fec_raw_name": _string(source_row["cand_name"]),
            "state": decision["state"],
            "source_district": raw_district,
            "district": district,
            "geography_status": geo_status,
            "office": decision["office"],
            "year": int(decision["year"]),
            "party": party,
            "cand_election_yr": source_row["cand_election_yr"],
            "cand_status": source_row["cand_status"],
            "cand_ici": source_row["cand_ici"],
            "source_cand_id": source,
            "universe_source": "ballot_override",
        })
        present.add(cycle_id)
    if not extras:
        return population
    extra = pd.DataFrame(extras)
    for column in population.columns:
        if column not in extra:
            extra[column] = pd.NA
    return pd.concat([population, extra[population.columns]], ignore_index=True)


def _prepare_historical_roster(historical_roster: pd.DataFrame) -> pd.DataFrame:
    """Prepare one ID-keyed historical attempt record without name matching."""
    if historical_roster.empty:
        return pd.DataFrame(columns=["candidate_cycle_id"])
    old = historical_roster.copy()
    if "candidate_cycle_id" not in old:
        if not {"cand_id", "year"}.issubset(old):
            raise ValueError("historical roster needs candidate_cycle_id or cand_id + year")
        old["candidate_cycle_id"] = (old["cand_id"].astype("string").str.strip()
                                     + "-" + old["year"].astype(int).astype(str))
    if old["candidate_cycle_id"].duplicated().any():
        duplicate_ids = sorted(old.loc[old["candidate_cycle_id"].duplicated(False),
                                       "candidate_cycle_id"].astype(str).unique())
        raise ValueError(f"duplicate historical roster IDs: {duplicate_ids[:10]}")
    keep = [
        "candidate_cycle_id", "website_url", "url_status", "has_url",
        "captured", "capture_status", "on_ballot", "general_votes", "stage",
        "data_source", "url_source", "url_discovery_status",
        "url_validation_status",
    ]
    old = old[[column for column in keep if column in old]].copy()
    rename = {column: f"historical_{column}" for column in old
              if column != "candidate_cycle_id"}
    return old.rename(columns=rename)


def _match_ballot_outcomes(population: pd.DataFrame) -> pd.DataFrame:
    """Match the full universe to local MIT general-election returns."""
    source = load_ballot()
    work = population[[
        "candidate_cycle_id", "year", "office", "state", "fec_raw_name",
        "district",
    ]].copy()
    work["state_po"] = work["state"]
    work["district"] = pd.to_numeric(work["district"], errors="coerce")
    work.loc[work["office"].eq("senate"), "district"] = 0
    # Impossible FEC source districts deliberately remain in the population but
    # cannot be assigned to a federal House contest.
    work["district"] = work["district"].fillna(-1).astype(int)
    work = ballot_keys(work, "fec_raw_name", comma=True)
    result = reverse_match(work, source)
    return pd.DataFrame({
        "candidate_cycle_id": population["candidate_cycle_id"].to_numpy(),
        "on_ballot": result["on_ballot"].astype(bool).to_numpy(),
        "general_votes": pd.to_numeric(result["general_votes"], errors="coerce").to_numpy(),
    })


def _capture_decision_map(decisions: pd.DataFrame) -> dict[str, tuple[str, str]]:
    if decisions.empty:
        return {}
    required = {"year", "cand_id", "action", "reason"}
    missing = required - set(decisions)
    if missing:
        raise ValueError(f"capture adjudication missing columns: {sorted(missing)}")
    data = decisions.copy()
    data["year"] = pd.to_numeric(data["year"], errors="raise").astype(int)
    data["cand_id"] = data["cand_id"].astype("string").str.strip()
    data["candidate_cycle_id"] = data["cand_id"] + "-" + data["year"].astype(str)
    if data["candidate_cycle_id"].duplicated().any():
        raise ValueError("duplicate capture adjudications")
    unknown = sorted(set(data["action"].dropna()) - {"exclude", "retain"})
    if unknown:
        raise ValueError(f"unknown capture adjudication actions: {unknown}")
    return dict(zip(data["candidate_cycle_id"],
                    zip(data["action"], data["reason"].fillna(""))))


def _capture_date_decision_map(
    decisions: pd.DataFrame,
) -> dict[str, tuple[str, str | None, str | None, str]]:
    """Validate approved partial-date rules and key them by candidacy-year."""
    if decisions.empty:
        return {}
    required = {
        "year", "cand_id", "action", "min_snap_day", "max_snap_day", "reason",
    }
    missing = required - set(decisions)
    if missing:
        raise ValueError(
            f"capture-date adjudication missing columns: {sorted(missing)}"
        )
    data = decisions.copy()
    data["year"] = pd.to_numeric(data["year"], errors="raise").astype(int)
    data["cand_id"] = data["cand_id"].astype("string").str.strip()
    data["candidate_cycle_id"] = data["cand_id"] + "-" + data["year"].astype(str)
    if data["candidate_cycle_id"].duplicated().any():
        raise ValueError("duplicate capture-date adjudications")
    unknown = sorted(set(data["action"].dropna()) - {"retain_date_range"})
    if unknown:
        raise ValueError(f"unknown capture-date adjudication actions: {unknown}")

    def day(value) -> str | None:
        raw = _string(value).split(".")[0]
        if not raw:
            return None
        if not re.fullmatch(r"\d{8}", raw):
            raise ValueError(f"capture-date boundary is not YYYYMMDD: {value!r}")
        return raw

    output = {}
    for row in data.to_dict("records"):
        minimum, maximum = day(row["min_snap_day"]), day(row["max_snap_day"])
        if minimum and maximum and minimum > maximum:
            raise ValueError(
                f"capture-date minimum exceeds maximum for "
                f"{row['candidate_cycle_id']}"
            )
        output[row["candidate_cycle_id"]] = (
            row["action"], minimum, maximum, _string(row["reason"])
        )
    return output


def build_candidate_universe(
    fec_records: pd.DataFrame,
    senate_races: set[tuple[int, str]],
    identity_overrides: pd.DataFrame,
    universe_adjudication: pd.DataFrame,
    capture_adjudication: pd.DataFrame,
    historical_roster: pd.DataFrame,
    observed_candidate_ids: set[str],
    ballot_outcomes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build the full population and left-join historical evidence by FEC ID."""
    population = select_fec_population(fec_records, senate_races)
    population = _add_missing_ballot_exceptions(
        population, fec_records, identity_overrides
    )
    population = apply_universe_adjudication(population, universe_adjudication)
    population = population.sort_values("candidate_cycle_id").reset_index(drop=True)
    if population["candidate_cycle_id"].duplicated().any():
        raise ValueError("candidate_cycle_id is not unique in the population")

    universe_ids = set(population["candidate_cycle_id"])
    missing_observed = sorted(set(observed_candidate_ids) - universe_ids)
    if missing_observed:
        raise ValueError(
            "observed release candidates are outside the approved universe: "
            f"{missing_observed[:20]}"
        )

    historical = _prepare_historical_roster(historical_roster)
    population = population.merge(
        historical, on="candidate_cycle_id", how="left", validate="one_to_one"
    )
    population["historical_attempt_record"] = population[
        "historical_website_url"
    ].notna() if "historical_website_url" in population else False
    if not historical.empty:
        attempted = set(historical["candidate_cycle_id"])
        population["historical_attempt_record"] = population[
            "candidate_cycle_id"
        ].isin(attempted)

    website = (population["historical_website_url"]
               if "historical_website_url" in population
               else pd.Series(pd.NA, index=population.index))
    population["website_url"] = website.astype("string")
    population.loc[~population["historical_attempt_record"], "website_url"] = pd.NA

    historical_status = pd.Series(pd.NA, index=population.index, dtype="string")
    if "historical_url_status" in population:
        historical_status = population["historical_url_status"].astype("string")
    if "historical_url_validation_status" in population:
        historical_status = population[
            "historical_url_validation_status"
        ].astype("string").fillna(historical_status)
    validation = pd.Series("not_evaluated_no_url_record", index=population.index,
                           dtype="string")
    attempted = population["historical_attempt_record"]
    nonblank = population["website_url"].fillna("").str.strip().ne("")
    validation.loc[attempted & ~nonblank] = "missing_or_blank"
    accepted_status = historical_status.isin(["valid", "valid_manual_override"])
    inferred_valid = population["website_url"].map(
        lambda value: _clean_campaign_url(value) is not None
    )
    validation.loc[attempted & nonblank] = "invalid_or_placeholder"
    validation.loc[attempted & nonblank & inferred_valid] = "valid"
    validation.loc[attempted & accepted_status] = historical_status.loc[
        attempted & accepted_status
    ]
    population["url_validation_status"] = validation
    population["has_url"] = validation.isin(["valid", "valid_manual_override"])

    discovery = pd.Series("legacy_no_url_record", index=population.index,
                          dtype="string")
    discovery.loc[attempted & ~nonblank] = "legacy_blank_url_record"
    discovery.loc[attempted & nonblank] = "legacy_url_recorded"
    if "historical_url_discovery_status" in population:
        recorded_discovery = population[
            "historical_url_discovery_status"
        ].astype("string")
        discovery = recorded_discovery.fillna(discovery)
    population["url_discovery_status"] = discovery
    population["url_source"] = pd.Series(pd.NA, index=population.index,
                                          dtype="string")
    population.loc[attempted & nonblank, "url_source"] = "legacy_unknown"
    if "historical_url_source" in population:
        recorded_source = population["historical_url_source"].astype("string")
        known_source = recorded_source.notna() & recorded_source.ne("")
        population.loc[known_source, "url_source"] = recorded_source.loc[known_source]

    population["normalized_website_url"] = population["website_url"].map(
        normalize_website_url
    ).astype("string")
    population.loc[~population["has_url"], "normalized_website_url"] = pd.NA
    population["website_domain"] = population["website_url"].map(
        normalize_website_domain
    ).astype("string")
    population.loc[~population["has_url"], "website_domain"] = pd.NA
    population["website_cycle_id"] = [
        website_cycle_id(url, year) if valid else None
        for url, year, valid in zip(population["website_url"], population["year"],
                                    population["has_url"])
    ]

    if ballot_outcomes is None:
        ballot_outcomes = _match_ballot_outcomes(population)
    required_ballot = {"candidate_cycle_id", "on_ballot", "general_votes"}
    if not required_ballot.issubset(ballot_outcomes):
        raise ValueError("ballot outcomes need candidate_cycle_id, on_ballot, general_votes")
    if ballot_outcomes["candidate_cycle_id"].duplicated().any():
        raise ValueError("duplicate ballot outcomes")
    population = population.merge(
        ballot_outcomes[["candidate_cycle_id", "on_ballot", "general_votes"]],
        on="candidate_cycle_id", how="left",
        validate="one_to_one",
    )
    population["on_ballot"] = population["on_ballot"].fillna(False).astype(bool)
    population["general_votes"] = pd.to_numeric(population["general_votes"],
                                                errors="coerce")

    # Retain already reviewed historical ballot outcomes where they exist.
    if "historical_on_ballot" in population:
        historical_ballot = population["historical_on_ballot"].map(_bool_or_missing)
        known = historical_ballot.notna()
        population.loc[known, "on_ballot"] = historical_ballot.loc[known].astype(bool)
        if "historical_general_votes" in population:
            historical_votes = pd.to_numeric(
                population["historical_general_votes"], errors="coerce"
            )
            population.loc[known, "general_votes"] = historical_votes.loc[known]
    population.loc[~population["on_ballot"], "general_votes"] = pd.NA
    if population.loc[population["on_ballot"], "general_votes"].isna().any():
        bad = population.loc[
            population["on_ballot"] & population["general_votes"].isna(),
            "candidate_cycle_id",
        ].tolist()
        raise ValueError(f"ballot candidates without vote totals: {bad[:20]}")

    decision_map = _capture_decision_map(capture_adjudication)
    invalid = {key for key, (action, _) in decision_map.items()
               if action == "exclude"}
    population["captured"] = population["candidate_cycle_id"].isin(
        observed_candidate_ids
    )
    population["capture_status"] = "not_captured"
    population.loc[population["captured"], "capture_status"] = "captured"
    population.loc[population["candidate_cycle_id"].isin(invalid),
                   "capture_status"] = "invalid_capture_excluded"
    population.loc[population["candidate_cycle_id"].isin(invalid), "captured"] = False

    if not population["has_url"].eq(
        population["url_validation_status"].isin(["valid", "valid_manual_override"])
    ).all():
        raise AssertionError("has_url disagrees with accepted URL states")
    if population["candidate_cycle_id"].duplicated().any():
        raise AssertionError("universe key is not unique after evidence joins")

    public_columns = [
        "candidate_cycle_id", "cand_id", "source_cand_id", "cand_election_yr",
        "cand_status", "cand_ici", "candidate", "fec_raw_name", "state",
        "source_district", "district", "geography_status", "office", "year",
        "party", "universe_source", "on_ballot", "general_votes",
        "historical_attempt_record", "website_url", "normalized_website_url",
        "website_domain", "website_cycle_id", "url_discovery_status",
        "url_validation_status", "url_source", "has_url", "captured",
        "capture_status",
    ]
    return population[public_columns].sort_values(
        ["office", "year", "state", "district", "candidate_cycle_id"],
        na_position="last",
    ).reset_index(drop=True)


@dataclass
class _RawAccumulator:
    """Streaming evidence accumulator for one candidate-year."""

    timestamps: set[str] = field(default_factory=set)
    days: set[str] = field(default_factory=set)
    page_urls: set[str] = field(default_factory=set)
    text_hashes_distinct: set[str] = field(default_factory=set)
    text_hashes_all: list[str] = field(default_factory=list)
    page_types: set[str] = field(default_factory=set)
    hosts: set[str] = field(default_factory=set)
    day_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    n_rows: int = 0
    n_homepage_rows: int = 0
    n_preelection_rows: int = 0


def _iter_dataframe_records(frame: pd.DataFrame) -> Iterator[Mapping]:
    for record in frame.to_dict("records"):
        yield record


def _summarize_raw_records(records: Iterable[Mapping]) -> pd.DataFrame:
    accumulators: dict[str, _RawAccumulator] = defaultdict(_RawAccumulator)
    for row in records:
        cycle_id = _string(row.get("candidate_cycle_id"))
        if not cycle_id:
            raise ValueError("raw page without candidate_cycle_id")
        acc = accumulators[cycle_id]
        acc.n_rows += 1

        date_value = _string(row.get("date"))
        date_match = DATE_RE.match(date_value)
        day = date_match.group(1) if date_match else ""
        timestamp = (date_match.group(1) + (date_match.group(2) or "")
                     if date_match else "")
        if timestamp:
            acc.timestamps.add(timestamp)
        if day:
            acc.days.add(day)
            acc.day_counts[day] += 1

        page_url = (_normalize_page_url(row.get("snap_url"))
                    or _normalize_page_url(row.get("urlkey")))
        if page_url:
            acc.page_urls.add(page_url)
            host = (urlsplit(page_url).hostname or "").lower()
            if host:
                acc.hosts.add(host)

        page_type = _string(row.get("page_type"))
        if page_type:
            acc.page_types.add(page_type)
        if page_type == "homepage":
            acc.n_homepage_rows += 1

        text = "" if pd.isna(row.get("text_snap_content")) else str(
            row.get("text_snap_content")
        )
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        acc.text_hashes_all.append(text_hash)
        acc.text_hashes_distinct.add(text_hash)

        raw_year = row.get("year")
        if day and raw_year is not None and not pd.isna(raw_year):
            if int(day) <= election_day(int(raw_year)):
                acc.n_preelection_rows += 1

    output = []
    for cycle_id, acc in sorted(accumulators.items()):
        sorted_timestamps = sorted(acc.timestamps)
        sorted_days = sorted(acc.days)
        first_day = sorted_days[0] if sorted_days else None
        last_day = sorted_days[-1] if sorted_days else None
        span = ((datetime.strptime(last_day, "%Y%m%d")
                 - datetime.strptime(first_day, "%Y%m%d")).days
                if first_day and last_day else pd.NA)
        max_pages = max(acc.day_counts.values(), default=0)
        runaway_pages = sum(count for count in acc.day_counts.values()
                            if count > 100)
        page_digest = hashlib.sha256(
            "\n".join(sorted(acc.text_hashes_all)).encode("ascii")
        ).hexdigest()
        output.append({
            "candidate_cycle_id": cycle_id,
            "n_observed_snapshot_timestamps": len(acc.timestamps),
            "n_observed_snapshot_days": len(acc.days),
            "first_capture_timestamp": (sorted_timestamps[0]
                                        if sorted_timestamps else pd.NA),
            "last_capture_timestamp": (sorted_timestamps[-1]
                                       if sorted_timestamps else pd.NA),
            "first_capture_day": first_day if first_day else pd.NA,
            "last_capture_day": last_day if last_day else pd.NA,
            "capture_span_days": span,
            "n_raw_page_rows": acc.n_rows,
            "n_distinct_page_urls": len(acc.page_urls),
            "n_distinct_page_texts": len(acc.text_hashes_distinct),
            "page_text_digest_sha256": page_digest,
            "n_page_types_observed": len(acc.page_types),
            "page_types_observed": ";".join(sorted(acc.page_types)),
            "n_source_hosts": len(acc.hosts),
            "source_hosts": ";".join(sorted(acc.hosts)),
            "max_raw_pages_one_day": max_pages,
            "n_runaway_days": sum(count > 100 for count in acc.day_counts.values()),
            "share_raw_pages_on_runaway_days": (
                runaway_pages / acc.n_rows if acc.n_rows else pd.NA
            ),
            "n_homepage_raw_page_rows": acc.n_homepage_rows,
            "n_preelection_raw_page_rows": acc.n_preelection_rows,
        })
    return pd.DataFrame(output)


def summarize_raw_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    """Summarize a raw-page dataframe; primarily useful for focused tests."""
    return _summarize_raw_records(_iter_dataframe_records(frame))


def summarize_raw_parquet(path: Path) -> pd.DataFrame:
    """Stream the raw corpus and compute one evidence row per candidacy-year."""
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    wanted = [
        "candidate_cycle_id", "year", "date", "urlkey", "snap_url",
        "page_type", "text_snap_content",
    ]
    missing = set(wanted) - set(parquet.schema_arrow.names)
    if missing:
        raise ValueError(f"raw corpus missing provenance columns: {sorted(missing)}")

    def records() -> Iterator[Mapping]:
        for batch in parquet.iter_batches(columns=wanted, batch_size=65_536):
            data = batch.to_pydict()
            for values in zip(*(data[column] for column in wanted)):
                yield dict(zip(wanted, values))

    return _summarize_raw_records(records())


def _family_status(values: pd.Series, unsupported: str) -> str:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return unsupported
    return "observed_zero" if numeric.eq(0).all() else "observed"


def build_candidate_year_provenance(
    universe: pd.DataFrame,
    raw_summary: pd.DataFrame,
    panel: pd.DataFrame,
    coded_panel: pd.DataFrame,
    capture_adjudication: pd.DataFrame,
    capture_date_adjudication: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Join source evidence and assign candidate/measure observability states."""
    if universe["candidate_cycle_id"].duplicated().any():
        raise ValueError("universe key is not unique")
    for label, frame in (("raw summary", raw_summary), ("panel", panel),
                         ("coded panel", coded_panel)):
        if not frame.empty and frame["candidate_cycle_id"].duplicated().any():
            raise ValueError(f"{label} key is not unique")
    universe_ids = set(universe["candidate_cycle_id"])
    for label, frame in (("raw summary", raw_summary), ("panel", panel),
                         ("coded panel", coded_panel)):
        outside = sorted(set(frame.get("candidate_cycle_id", [])) - universe_ids)
        if outside:
            raise ValueError(f"{label} candidates outside universe: {outside[:20]}")
    decision_map = _capture_decision_map(capture_adjudication)
    invalid_decision_ids = {
        key for key, (action, _) in decision_map.items() if action == "exclude"
    }
    raw_ids = set(raw_summary.get("candidate_cycle_id", []))
    invalid_raw = sorted(raw_ids & invalid_decision_ids)
    if invalid_raw:
        raise ValueError(
            f"invalid captures remain in substantive raw data: {invalid_raw}"
        )
    marked_captured_ids = set(
        universe.loc[universe["captured"].astype(bool), "candidate_cycle_id"]
    )
    if raw_ids != marked_captured_ids:
        missing_raw = sorted(marked_captured_ids - raw_ids)
        unmarked_raw = sorted(raw_ids - marked_captured_ids)
        raise ValueError(
            "raw evidence disagrees with universe captured status: "
            f"missing raw={missing_raw[:20]}, unmarked raw={unmarked_raw[:20]}"
        )

    raw = raw_summary.copy()
    panel_columns = [column for column in (
        "candidate_cycle_id", "sel_date", "text_quality"
    ) if column in panel]
    selected = panel[panel_columns].copy()
    selected = selected.rename(columns={
        "sel_date": "selected_panel_timestamp",
        "text_quality": "selected_text_quality",
    })
    coded = coded_panel.copy()
    coded_columns = [column for column in coded if (
        column == "candidate_cycle_id"
        or column in {"icpsr_n_char", "icpsr_n_char_home",
                      "icpsr_n_char_preelec"}
        or column.startswith("icpsr_topic_") and not column.endswith("_home")
    )]
    coded = coded[coded_columns]

    out = universe.copy()
    if not raw.empty:
        out = out.merge(raw, on="candidate_cycle_id", how="left", validate="one_to_one")
    if not selected.empty:
        out = out.merge(selected, on="candidate_cycle_id", how="left",
                        validate="one_to_one")
    if not coded.empty:
        out = out.merge(coded, on="candidate_cycle_id", how="left",
                        validate="one_to_one")

    count_columns = [
        "n_observed_snapshot_timestamps", "n_observed_snapshot_days",
        "n_raw_page_rows", "n_distinct_page_urls", "n_distinct_page_texts",
        "n_page_types_observed", "n_source_hosts", "max_raw_pages_one_day",
        "n_runaway_days", "n_homepage_raw_page_rows",
        "n_preelection_raw_page_rows",
    ]
    for column in count_columns:
        if column not in out:
            out[column] = 0
        out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0).astype(int)

    if "share_raw_pages_on_runaway_days" not in out:
        out["share_raw_pages_on_runaway_days"] = pd.NA
    out.loc[out["n_raw_page_rows"].eq(0),
            "share_raw_pages_on_runaway_days"] = pd.NA
    if "selected_panel_timestamp" not in out:
        out["selected_panel_timestamp"] = pd.NA
    if "selected_text_quality" not in out:
        out["selected_text_quality"] = pd.NA
    out["selected_panel_rule"] = pd.Series(pd.NA, index=out.index, dtype="string")
    has_selection = out["selected_panel_timestamp"].notna()
    out.loc[has_selection, "selected_panel_rule"] = (
        "longest_text_snapshot_latest_timestamp_tie"
    )

    out["capture_adjudication_status"] = "not_reviewed"
    out["capture_adjudication_reason"] = pd.Series(
        pd.NA, index=out.index, dtype="string"
    )
    for index, cycle_id in out["candidate_cycle_id"].items():
        decision = decision_map.get(cycle_id)
        if not decision:
            continue
        action, reason = decision
        out.at[index, "capture_adjudication_status"] = (
            "excluded_invalid_capture" if action == "exclude"
            else "retained_after_review"
        )
        out.at[index, "capture_adjudication_reason"] = reason

    date_decision_map = _capture_date_decision_map(
        capture_date_adjudication
        if capture_date_adjudication is not None else pd.DataFrame()
    )
    date_targets_outside = sorted(
        set(date_decision_map) - set(out["candidate_cycle_id"])
    )
    if date_targets_outside:
        raise ValueError(
            "capture-date adjudications outside the public universe: "
            f"{date_targets_outside}"
        )
    out["capture_date_adjudication_status"] = "not_reviewed"
    out["capture_date_min_snap_day"] = pd.Series(
        pd.NA, index=out.index, dtype="string"
    )
    out["capture_date_max_snap_day"] = pd.Series(
        pd.NA, index=out.index, dtype="string"
    )
    out["capture_date_adjudication_reason"] = pd.Series(
        pd.NA, index=out.index, dtype="string"
    )
    for index, cycle_id in out["candidate_cycle_id"].items():
        decision = date_decision_map.get(cycle_id)
        if not decision:
            continue
        action, minimum, maximum, reason = decision
        if action != "retain_date_range":
            raise AssertionError(f"unhandled capture-date action: {action}")
        out.at[index, "capture_date_adjudication_status"] = (
            "retained_approved_date_range"
        )
        out.at[index, "capture_date_min_snap_day"] = minimum
        out.at[index, "capture_date_max_snap_day"] = maximum
        out.at[index, "capture_date_adjudication_reason"] = reason
        if out.at[index, "n_raw_page_rows"] == 0:
            raise ValueError(
                f"date-adjudicated candidate has no retained raw pages: {cycle_id}"
            )
        first_day = _string(out.at[index, "first_capture_day"])
        last_day = _string(out.at[index, "last_capture_day"])
        if ((minimum and (not first_day or first_day < minimum))
                or (maximum and (not last_day or last_day > maximum))):
            raise ValueError(
                f"raw capture days violate approved date range for {cycle_id}: "
                f"observed {first_day or 'missing'}--{last_day or 'missing'}, "
                f"approved {minimum or 'open'}--{maximum or 'open'}"
            )

    invalid = out["capture_adjudication_status"].eq("excluded_invalid_capture")
    if out.loc[invalid, "n_raw_page_rows"].gt(0).any():
        bad = out.loc[invalid & out["n_raw_page_rows"].gt(0),
                      "candidate_cycle_id"].tolist()
        raise ValueError(f"invalid captures remain in substantive raw data: {bad}")

    quality = out["selected_text_quality"].astype("string")
    observed = out["n_raw_page_rows"].gt(0)
    status = pd.Series(pd.NA, index=out.index, dtype="string")
    status.loc[invalid] = "invalid_capture_excluded"
    status.loc[observed & quality.eq("usable") & ~invalid] = "captured_usable_text"
    status.loc[observed & quality.eq("thin") & ~invalid] = "captured_thin_text"
    status.loc[observed & ~(quality.isin(["usable", "thin"])) & ~invalid] = (
        "captured_empty_text"
    )
    remaining = status.isna()
    status.loc[remaining & out["url_discovery_status"].eq("legacy_no_url_record")] = (
        "legacy_no_url_record"
    )
    status.loc[remaining & out["has_url"]] = (
        "valid_url_no_capture_unknown_reason"
    )
    status.loc[status.isna()] = "invalid_or_placeholder_url"
    out["observability_status"] = status

    all_values = out.get("icpsr_n_char", pd.Series(pd.NA, index=out.index))
    home_values = out.get("icpsr_n_char_home", pd.Series(pd.NA, index=out.index))
    pre_values = out.get("icpsr_n_char_preelec", pd.Series(pd.NA, index=out.index))
    topic_columns = [column for column in out
                     if column.startswith("icpsr_topic_")
                     and not column.endswith("_home")]
    for family in (
        "all_text_measure_status", "homepage_measure_status",
        "preelection_measure_status", "topic_measure_status",
    ):
        out[family] = "not_computed"

    for index in out.index:
        if invalid.at[index] or not observed.at[index]:
            continue
        out.at[index, "all_text_measure_status"] = _family_status(
            pd.Series([all_values.at[index]]), "insufficient_clean_text"
        )
        home_unsupported = (
            "scope_not_observed" if out.at[index, "n_homepage_raw_page_rows"] == 0
            else "insufficient_clean_text"
        )
        out.at[index, "homepage_measure_status"] = _family_status(
            pd.Series([home_values.at[index]]), home_unsupported
        )
        pre_unsupported = (
            "scope_not_observed" if out.at[index, "n_preelection_raw_page_rows"] == 0
            else "insufficient_clean_text"
        )
        out.at[index, "preelection_measure_status"] = _family_status(
            pd.Series([pre_values.at[index]]), pre_unsupported
        )
        if topic_columns:
            topic_unsupported = (
                "insufficient_clean_text"
                if out.at[index, "all_text_measure_status"]
                == "insufficient_clean_text" else "not_computed"
            )
            out.at[index, "topic_measure_status"] = _family_status(
                out.loc[index, topic_columns], topic_unsupported
            )

    for family in (
        "all_text_measure_status", "homepage_measure_status",
        "preelection_measure_status", "topic_measure_status",
    ):
        if not set(out[family]).issubset(VALID_MEASURE_STATUSES):
            raise AssertionError(f"unknown values in {family}")

    out["collection_method_version"] = COLLECTION_METHOD_UNKNOWN
    out["cdx_availability_status"] = UNKNOWN_EVIDENCE
    out["collection_failure_reason"] = UNKNOWN_EVIDENCE
    out["snapshot_cap_binding_status"] = UNKNOWN_EVIDENCE
    out["provenance_evidence_scope"] = "released_successful_pages_and_legacy_roster"

    # Derived numeric values live in the coded panel, not in this compact
    # provenance table. Only their family-level availability states are kept.
    out = out.drop(columns=[column for column in coded_columns
                            if column != "candidate_cycle_id"], errors="ignore")
    if len(out) != len(universe) or out["candidate_cycle_id"].duplicated().any():
        raise AssertionError("provenance is not one-to-one with the universe")
    return out.sort_values(
        ["office", "year", "state", "district", "candidate_cycle_id"],
        na_position="last",
    ).reset_index(drop=True)


def build_outputs(
    release_dir: Path,
    fec_dir: Path,
    senate_returns: Path,
    identity_overrides_path: Path,
    universe_adjudication_path: Path,
    capture_adjudication_path: Path,
    capture_date_adjudication_path: Path | None,
    historical_roster_path: Path,
    out_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load authoritative inputs, build both products, and write CSV outputs."""
    raw_path = release_dir / "raw_corpus.parquet"
    panel_path = release_dir / "panel_candidate_year.csv"
    coded_path = release_dir / "panel_icpsr_compat.csv"
    required_paths = (raw_path, panel_path, coded_path, historical_roster_path,
                      identity_overrides_path, universe_adjudication_path,
                      capture_adjudication_path, senate_returns)
    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(path)
    if (capture_date_adjudication_path is not None
            and not capture_date_adjudication_path.exists()):
        raise FileNotFoundError(capture_date_adjudication_path)

    raw_summary = summarize_raw_parquet(raw_path)
    observed_ids = set(raw_summary["candidate_cycle_id"])
    fec_records = load_fec_records(fec_dir)
    universe = build_candidate_universe(
        fec_records=fec_records,
        senate_races=_read_senate_races(senate_returns),
        identity_overrides=pd.read_csv(identity_overrides_path, dtype=str),
        universe_adjudication=pd.read_csv(universe_adjudication_path, dtype=str),
        capture_adjudication=pd.read_csv(capture_adjudication_path, dtype=str),
        historical_roster=pd.read_csv(historical_roster_path, low_memory=False,
                                      dtype={"cand_id": str}),
        observed_candidate_ids=observed_ids,
    )
    panel = pd.read_csv(panel_path, low_memory=False)
    coded = pd.read_csv(coded_path, low_memory=False)
    provenance = build_candidate_year_provenance(
        universe=universe,
        raw_summary=raw_summary,
        panel=panel,
        coded_panel=coded,
        capture_adjudication=pd.read_csv(capture_adjudication_path, dtype=str),
        capture_date_adjudication=(
            pd.read_csv(capture_date_adjudication_path, dtype=str)
            if capture_date_adjudication_path is not None else pd.DataFrame()
        ),
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    universe.to_csv(out_dir / "candidate_year_universe.csv", index=False)
    provenance.to_csv(out_dir / "candidate_year_provenance.csv", index=False)
    return universe, provenance


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True,
                        help="Corrected staged release containing raw and panels")
    parser.add_argument("--fec-dir", type=Path,
                        default=REPO / "data/fec_cache")
    parser.add_argument("--senate-returns", type=Path,
                        default=REPO / "data/external/1976-2024-senate-state.csv")
    parser.add_argument("--identity-overrides", type=Path,
                        default=REPO / "config/candidate_identity_overrides.csv")
    parser.add_argument("--universe-adjudication", type=Path,
                        default=REPO / "config/candidate_universe_adjudication.csv")
    parser.add_argument("--capture-adjudication", type=Path,
                        default=REPO / "config/capture_adjudication.csv")
    parser.add_argument("--capture-date-adjudication", type=Path,
                        default=REPO / "config/capture_date_adjudication.csv")
    parser.add_argument("--historical-roster", type=Path, default=None,
                        help="Pre-expansion roster with historical URL evidence")
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()
    historical = args.historical_roster or args.release_dir / "release_roster.csv"
    out_dir = args.out_dir or args.release_dir
    universe, provenance = build_outputs(
        release_dir=args.release_dir,
        fec_dir=args.fec_dir,
        senate_returns=args.senate_returns,
        identity_overrides_path=args.identity_overrides,
        universe_adjudication_path=args.universe_adjudication,
        capture_adjudication_path=args.capture_adjudication,
        capture_date_adjudication_path=args.capture_date_adjudication,
        historical_roster_path=historical,
        out_dir=out_dir,
    )
    invalid_geography = int(universe["geography_status"].eq(
        "invalid_house_district"
    ).sum())
    print(f"candidate universe: {len(universe):,} rows; "
          f"{universe['captured'].sum():,} captured; "
          f"{invalid_geography:,} retained invalid source districts")
    print("observability statuses:")
    print(provenance["observability_status"].value_counts().sort_index().to_string())
    print(f"wrote {out_dir / 'candidate_year_universe.csv'}")
    print(f"wrote {out_dir / 'candidate_year_provenance.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
