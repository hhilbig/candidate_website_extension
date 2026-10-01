"""Utilities for building a mayoral candidate universe and extracting URL evidence."""

from __future__ import annotations

import re
import hashlib
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup


SOURCE_ENCODING = "latin-1"
EXPECTED_SOURCE_SHA256 = {
    "ledb_candidatelevel.csv": "86cc4f80b2c9048967b18c74c46b9f932d52fbc561e0c89a86441f0186f35357",
    "cities_constituency_data.csv": "1fa62cce2bfad05d1d438a89d02f6889a0c879833f8c8e977f996fc20b107078",
}
POPULATION_SUPPLEMENTS = {
    "1517000": {
        "population_2020": 350_964,
        "population_source": "U.S. Census Bureau, 2020 Decennial Census",
        "population_source_url": "https://data.census.gov/profile/Urban_Honolulu_CDP,_Hawaii?g=1600000US1571550",
        "population_note": "Urban Honolulu CDP proxy for legacy Honolulu place code; threshold classification only",
    },
    "1836000": {
        "population_2020": 887_642,
        "population_source": "U.S. Census Bureau QuickFacts, 2020 Decennial Census",
        "population_source_url": "https://www.census.gov/quickfacts/fact/table/indianapoliscitybalanceindiana/POP010220",
        "population_note": "Indianapolis city (balance) proxy for legacy consolidated-city code; threshold classification only",
    },
    "3420260": {
        "population_2020": 107_588,
        "population_source": "U.S. Census Bureau QuickFacts, 2020 Decennial Census",
        "population_source_url": "https://www.census.gov/quickfacts/fact/table/edisontownshipmiddlesexcountynewjersey/POP010220",
        "population_note": "Edison township population matched to source jurisdiction name; threshold classification only",
    },
}
EXCLUDED_HOSTS = {
    "ballotpedia.org",
    "facebook.com",
    "google.com",
    "instagram.com",
    "linkedin.com",
    "twitter.com",
    "wikipedia.org",
    "x.com",
    "youtube.com",
}
NON_CAMPAIGN_HOST_SUFFIXES = (
    ".gov",
    "actblue.com",
    "anedot.com",
    "ballotpedia.org",
    "eventbrite.com",
    "medium.com",
    "nytimes.com",
    "secure.winred.com",
    "washingtonpost.com",
)
DIRECT_CAMPAIGN_LINK_LABEL = re.compile(
    r"\b(campaign\s+website|official\s+(?:campaign\s+)?website|website|homepage|home\s+page)\b",
    re.IGNORECASE,
)


def file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_known_source_file(path: str) -> str:
    """Verify published ALGEL inputs when the filename identifies a known file."""
    observed = file_sha256(path)
    expected = EXPECTED_SOURCE_SHA256.get(Path(path).name)
    if expected and observed != expected:
        raise ValueError(
            f"Source checksum mismatch for {Path(path).name}: expected {expected}, got {observed}"
        )
    return observed


def normalize_place_fips(values: pd.Series) -> pd.Series:
    """Normalize Census state+place FIPS to seven-character strings."""
    cleaned = values.astype("string").str.replace(r"\.0$", "", regex=True)
    return cleaned.str.zfill(7)


def apply_stage_annotations(candidates: pd.DataFrame, annotation_path: Path) -> pd.DataFrame:
    """Overlay only source-provenanced contest-stage adjudications."""
    candidates = candidates.copy()
    candidates["election_stage_source_url"] = ""
    candidates["election_stage_note"] = ""
    if not annotation_path.exists():
        return candidates
    annotations = pd.read_json(annotation_path)
    key_columns = ["geo_name", "state_abb", "year", "month"]
    value_columns = [
        "election_stage_status", "election_stage_source_url", "election_stage_note"
    ]
    missing_stage = set(key_columns + value_columns) - set(annotations.columns)
    if missing_stage:
        raise ValueError(f"Stage annotation file missing columns: {sorted(missing_stage)}")
    if annotations.duplicated(key_columns).any():
        raise ValueError("Stage annotation file has duplicate contest keys")
    candidates = candidates.merge(
        annotations[key_columns + value_columns],
        on=key_columns,
        how="left",
        suffixes=("", "_curated"),
        validate="many_to_one",
    )
    for column in value_columns:
        curated = f"{column}_curated"
        candidates[column] = candidates[curated].fillna(candidates[column])
        candidates = candidates.drop(columns=curated)
    return candidates


def build_mayoral_universe(
    candidate_path: str,
    city_path: str,
    start_year: int = 2000,
    end_year: int = 2021,
    min_population: int = 100_000,
    stage_annotations_path: str | None = None,
) -> pd.DataFrame:
    """Build the discovery universe without dropping unmatched jurisdictions.

    The source has no explicit primary/general/runoff field. ``contest_n_candidates``
    therefore describes the recorded contest, and ``target_contested`` marks only
    whether at least two distinct candidates appear in that contest.
    """
    candidates = pd.read_csv(
        candidate_path,
        encoding=SOURCE_ENCODING,
        low_memory=False,
        dtype={"fips": "string"},
    )
    cities = pd.read_csv(
        city_path,
        encoding=SOURCE_ENCODING,
        dtype={"fips": "string"},
    )

    required_candidates = {
        "ledb_candid",
        "full_name",
        "fips",
        "geo_name",
        "state_abb",
        "office_consolidated",
        "year",
        "month",
        "contest",
        "votes",
        "vote_share",
        "ballotpedia_url",
    }
    required_cities = {"fips", "population_2020", "geo_type"}
    missing_candidates = required_candidates - set(candidates.columns)
    missing_cities = required_cities - set(cities.columns)
    if missing_candidates:
        raise ValueError(f"Candidate file missing columns: {sorted(missing_candidates)}")
    if missing_cities:
        raise ValueError(f"City file missing columns: {sorted(missing_cities)}")

    candidates = candidates[candidates["office_consolidated"].eq("Mayor")].copy()
    candidates["place_fips"] = normalize_place_fips(candidates["fips"])
    cities["place_fips"] = normalize_place_fips(cities["fips"])
    city_lookup = cities[["place_fips", "population_2020", "geo_type"]].drop_duplicates(
        "place_fips"
    )
    candidates = candidates.merge(
        city_lookup,
        on="place_fips",
        how="left",
        validate="many_to_one",
    )
    candidates["population_source"] = "ALGEL cities constituency data"
    candidates["population_source_url"] = "https://doi.org/10.17605/OSF.IO/MV5E6"
    candidates["population_note"] = ""
    for place_fips, supplement in POPULATION_SUPPLEMENTS.items():
        use = candidates["place_fips"].eq(place_fips) & candidates["population_2020"].isna()
        for column, value in supplement.items():
            candidates.loc[use, column] = value

    contest_counts = candidates.groupby("contest")["ledb_candid"].transform("nunique")
    contest_winners = candidates["winner"].eq("win").groupby(candidates["contest"]).transform("sum")
    candidates["contest_n_candidates"] = contest_counts.astype("Int64")
    candidates["contest_n_winners_observed"] = contest_winners.astype("Int64")
    candidates["contest_validation_status"] = "valid_single_winner"
    candidates.loc[
        candidates["contest_n_winners_observed"].ne(1), "contest_validation_status"
    ] = "anomalous_winner_count"
    candidates["population_status"] = candidates["population_2020"].apply(
        lambda value: "missing_population" if pd.isna(value) else "observed"
    )
    supplemented = candidates["place_fips"].isin(POPULATION_SUPPLEMENTS)
    candidates.loc[supplemented, "population_status"] = "supplemented_proxy"
    candidates["target_year"] = candidates["year"].between(start_year, end_year)
    candidates["target_population"] = candidates["population_2020"].ge(min_population)
    candidates["target_contested"] = candidates["contest_n_candidates"].ge(2)
    candidates["direct_election_status"] = "observed_candidate_vote_returns"
    candidates["election_stage_status"] = (
        "single_recorded_mayoral_contest_per_city_year_stage_not_explicit"
    )
    annotation_path = Path(stage_annotations_path) if stage_annotations_path else (
        Path(__file__).resolve().parents[1] / "config" / "curated_mayoral_contest_stages.json"
    )
    candidates = apply_stage_annotations(candidates, annotation_path)
    candidates["in_discovery_scope"] = (
        candidates["target_year"]
        & candidates["target_population"]
        & candidates["target_contested"]
    )
    candidates["stage_scope_status"] = "stage_unresolved"
    candidates.loc[
        candidates["election_stage_status"].eq("verified_general_election"),
        "stage_scope_status",
    ] = "verified_general"
    candidates.loc[
        candidates["election_stage_status"].str.startswith("verified_")
        & candidates["election_stage_status"].ne("verified_general_election"),
        "stage_scope_status",
    ] = "verified_non_general_or_special"
    candidates["in_verified_general_scope"] = (
        candidates["in_discovery_scope"]
        & candidates["stage_scope_status"].eq("verified_general")
    )
    candidates["candidate_contest_id"] = (
        candidates["contest"].astype("string")
        + "__"
        + candidates["ledb_candid"].astype("string")
    )
    candidates["candidate_year_id"] = (
        candidates["ledb_candid"].astype("string")
        + "__"
        + candidates["year"].astype("Int64").astype("string")
    )
    candidates["source_dataset"] = "American Local Government Elections Database"
    candidates["source_doi"] = "10.17605/OSF.IO/MV5E6"

    columns = [
        "candidate_contest_id",
        "candidate_year_id",
        "ledb_candid",
        "full_name",
        "firstname",
        "lastname",
        "place_fips",
        "geo_name",
        "state_abb",
        "year",
        "month",
        "contest",
        "contest_n_candidates",
        "contest_n_winners_observed",
        "contest_validation_status",
        "votes",
        "vote_share",
        "winner",
        "incumbent",
        "population_2020",
        "population_status",
        "population_source",
        "population_source_url",
        "population_note",
        "ballotpedia_url",
        "target_year",
        "target_population",
        "target_contested",
        "direct_election_status",
        "election_stage_status",
        "election_stage_source_url",
        "election_stage_note",
        "stage_scope_status",
        "in_verified_general_scope",
        "in_discovery_scope",
        "source_dataset",
        "source_doi",
    ]
    return candidates[columns].sort_values(
        ["in_discovery_scope", "year", "state_abb", "geo_name", "contest", "full_name"],
        ascending=[False, True, True, True, True, True],
    ).reset_index(drop=True)


def unwrap_wayback_url(url: str) -> str:
    """Return the original target from Internet Archive wrapper URLs.

    Both the public Wayback host and Archive-It's Wayback host embed the
    original URL verbatim in the wrapper path.  Extracting it before any host
    or site-root normalization prevents ``web.archive.org`` from becoming the
    apparent campaign domain.  URL path and query case are intentionally left
    untouched.
    """
    if not isinstance(url, str):
        return ""
    value = url.strip().replace("&amp;", "&")
    parsed = urlparse(value)
    wrapper_host = (parsed.hostname or "").casefold()
    if wrapper_host not in {"web.archive.org", "wayback.archive-it.org"}:
        return value
    target_match = re.search(r"/(https?://.*)$", parsed.path, flags=re.IGNORECASE)
    if target_match is None:
        return value
    target = target_match.group(1)
    if parsed.query:
        target = f"{target}?{parsed.query}"
    if parsed.fragment:
        target = f"{target}#{parsed.fragment}"
    return target


def canonical_http_url(url: str) -> str:
    """Return a clean HTTP(S) target URL or an empty string."""
    if not isinstance(url, str):
        return ""
    url = unwrap_wayback_url(url)
    if not url.startswith(("http://", "https://")):
        return ""
    parsed = urlparse(url)
    if not parsed.netloc:
        return ""
    return parsed._replace(fragment="").geturl()


def hostname(url: str) -> str:
    host = urlparse(canonical_http_url(url)).hostname or ""
    return host.lower().removeprefix("www.")


def site_root_url(url: str) -> str:
    """Normalize a source-backed page URL to its campaign-site origin."""
    clean = canonical_http_url(url)
    if not clean:
        return ""
    parsed = urlparse(clean)
    return parsed._replace(path="/", params="", query="", fragment="").geturl()


def is_candidate_campaign_host(url: str) -> bool:
    host = hostname(url)
    if not host or host in EXCLUDED_HOSTS:
        return False
    if any(host == suffix.removeprefix(".") or host.endswith(suffix) for suffix in NON_CAMPAIGN_HOST_SUFFIXES):
        return False
    return True


def _section_nodes(heading):
    level = int(heading.name[1])
    node = heading.next_sibling
    while node is not None:
        if getattr(node, "name", None) in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            if int(node.name[1]) <= level:
                break
        yield node
        node = node.next_sibling


def normalize_person_name(value: str) -> str:
    """Normalize a person name for conservative cross-source matching."""
    ascii_value = unicodedata.normalize("NFKD", str(value)).encode(
        "ascii", "ignore"
    ).decode()
    return " ".join(re.findall(r"[a-z]+", ascii_value.casefold()))


def extract_ballotpedia_election_page_campaign_links(
    html: str, candidate_names: list[str]
) -> list[dict[str, str]]:
    """Attribute campaign-site citations on an election page to candidates.

    A claim is accepted only when a ``Campaign website`` heading sits under a
    heading whose normalized text exactly matches one supplied candidate name.
    This deliberately sacrifices recall to prevent cross-candidate attribution.
    """
    soup = BeautifulSoup(html, "html.parser")
    names = {normalize_person_name(name): name for name in candidate_names}
    evidence: dict[tuple[str, str], dict[str, str]] = {}
    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        heading_text = " ".join(heading.get_text(" ", strip=True).casefold().split())
        if heading_text != "campaign website":
            continue
        candidate_name = ""
        candidate_heading = heading.find_previous(
            lambda tag: getattr(tag, "name", "") in {"h2", "h3"}
        )
        if candidate_heading is not None:
            candidate_name = names.get(
                normalize_person_name(candidate_heading.get_text(" ", strip=True)), ""
            )
        if not candidate_name:
            continue
        reference_ids: set[str] = set()
        for node in _section_nodes(heading):
            if not hasattr(node, "find_all"):
                continue
            for anchor in node.find_all("a", href=True):
                href = anchor.get("href", "")
                if href.startswith("#cite_note-"):
                    reference_ids.add(href[1:])
                    continue
                url = canonical_http_url(href)
                label = anchor.get_text(" ", strip=True)
                if url and is_candidate_campaign_host(url) and DIRECT_CAMPAIGN_LINK_LABEL.search(label):
                    evidence[(candidate_name, url)] = {
                        "full_name": candidate_name, "evidence_url": url,
                        "evidence_text": label[:500],
                        "discovery_method": "ballotpedia_election_page_campaign_section_direct",
                        "confidence": "high",
                    }
        for reference_id in reference_ids:
            note = soup.find(id=reference_id)
            if note is None:
                continue
            note_text = note.get_text(" ", strip=True)
            for anchor in note.find_all("a", href=True):
                url = canonical_http_url(anchor.get("href", ""))
                if url and is_candidate_campaign_host(url):
                    evidence[(candidate_name, url)] = {
                        "full_name": candidate_name, "evidence_url": url,
                        "evidence_text": note_text[:500],
                        "discovery_method": "ballotpedia_election_page_campaign_section_citation",
                        "confidence": "high",
                    }
    return sorted(evidence.values(), key=lambda item: (item["full_name"], item["evidence_url"]))


def extract_ballotpedia_election_stage(html: str, year: int, month: int) -> dict[str, str]:
    """Extract one uniquely date-matched stage from Ballotpedia result sections."""
    soup = BeautifulSoup(html, "html.parser")
    matches = []
    for heading in soup.select("h5.votebox-header-election-type"):
        label = heading.get_text(" ", strip=True)
        result_text = heading.find_next("p", class_="results_text")
        if result_text is None:
            continue
        text = result_text.get_text(" ", strip=True)
        date_match = re.search(rf"\bon ([A-Z][a-z]+ \d{{1,2}}, {int(year)})\b", text)
        if not date_match:
            continue
        try:
            election_date = datetime.strptime(date_match.group(1), "%B %d, %Y")
        except ValueError:
            continue
        if election_date.month != int(month):
            continue
        lowered = label.casefold()
        if "runoff" in lowered:
            stage = "verified_runoff_election"
        elif "primary" in lowered:
            stage = "verified_primary_election"
        elif "general election" in lowered:
            stage = "verified_general_election"
        elif "special" in lowered:
            stage = "verified_special_election"
        else:
            continue
        matches.append((stage, label, date_match.group(1)))
    unique = list(dict.fromkeys(matches))
    if len(unique) != 1:
        return {}
    stage, label, election_date = unique[0]
    return {
        "source_election_stage": stage,
        "source_election_stage_label": label,
        "source_election_date": election_date,
        "source_election_stage_confidence": "high_date_matched_secondary_source",
    }


def extract_ballotpedia_campaign_links(html: str) -> list[dict[str, str]]:
    """Extract campaign-site claims from archived Ballotpedia HTML.

    Only URLs directly inside a ``Campaign website`` section, or citations
    referenced by that section, are accepted. Generic external links and news
    citations elsewhere on the profile are intentionally excluded.
    """
    soup = BeautifulSoup(html, "html.parser")
    evidence: dict[str, dict[str, str]] = {}

    headings = []
    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        text = " ".join(heading.get_text(" ", strip=True).casefold().split())
        if text == "campaign website" or text.endswith(" campaign website"):
            headings.append(heading)

    for heading in headings:
        section = list(_section_nodes(heading))
        reference_ids: set[str] = set()
        for node in section:
            if not hasattr(node, "find_all"):
                continue
            for anchor in node.find_all("a", href=True):
                href = anchor.get("href", "")
                if href.startswith("#cite_note-"):
                    reference_ids.add(href[1:])
                    continue
                url = canonical_http_url(href)
                label = anchor.get_text(" ", strip=True)
                if (
                    url
                    and is_candidate_campaign_host(url)
                    and DIRECT_CAMPAIGN_LINK_LABEL.search(label)
                ):
                    evidence[url] = {
                        "evidence_url": url,
                        "evidence_text": label[:500],
                        "discovery_method": "ballotpedia_campaign_section_direct",
                        "confidence": "high",
                    }

        for reference_id in reference_ids:
            note = soup.find(id=reference_id)
            if note is None:
                continue
            for anchor in note.find_all("a", href=True):
                url = canonical_http_url(anchor.get("href", ""))
                if url and is_candidate_campaign_host(url):
                    evidence[url] = {
                        "evidence_url": url,
                        "evidence_text": note.get_text(" ", strip=True)[:500],
                        "discovery_method": "ballotpedia_campaign_section_citation",
                        "confidence": "high",
                    }

    return sorted(evidence.values(), key=lambda item: item["evidence_url"])


def choose_near_election_snapshots(
    timestamps: list[str], year: int, month: int | float | None, limit: int = 3
) -> list[str]:
    """Choose unique archive timestamps closest to the recorded election month."""
    clean: list[tuple[float, str]] = []
    if month is None or pd.isna(month):
        raise ValueError("Election month is required for temporal archive selection")
    election_month = int(month)
    if not 1 <= election_month <= 12:
        raise ValueError(f"Invalid election month: {month}")
    target = datetime(year, election_month, 15)
    for timestamp in set(timestamps):
        try:
            captured = datetime.strptime(timestamp[:14], "%Y%m%d%H%M%S")
        except (TypeError, ValueError):
            continue
        clean.append((abs((captured - target).total_seconds()), timestamp))
    return [timestamp for _, timestamp in sorted(clean)[:limit]]
