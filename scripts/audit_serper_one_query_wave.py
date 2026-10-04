#!/usr/bin/env python3
"""Offline contract and audit for the bounded 480-candidate Serper wave.

This module never sends a request or fetches a result page.  It freezes the
one-query design, classifies already-saved organic results, and verifies that
the acquisition outputs preserve query, result, and URL-claim provenance.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import unquote, urlparse

import pandas as pd

from src.mayoral_discovery import canonical_http_url, unwrap_wayback_url


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_VERSION = "serper_one_query_wave_contract_v1"
QUERY_TEMPLATE_VERSION = "identity_alias_mayor_place_year_campaign_or_website_v1"
EXPECTED_SAMPLE_ROWS = 480
EXPECTED_SAMPLE_SHA256 = (
    "646e2688005ca7c4f0f20ed027aeb7709351adbdfa9864af26e143f1caf3eb19"
)
MAX_SOURCE_PAGES_PER_CANDIDATE = 3
UNIT_SEPARATOR = "\x1f"
SEARCH_PROVIDER = "serper"
SEARCH_GL = "us"
SEARCH_HL = "en"
SEARCH_RESULT_COUNT = 10

REQUEST_STATUSES = {"found", "no_result", "request_error", "provider_error"}
RESULT_PAGE_ROLES = {
    "direct_candidate_site",
    "credible_source_page",
    "excluded_url",
    "identity_unresolved",
    "context_unresolved",
    "temporal_conflict_review",
    "office_mismatch_review",
}
COVERAGE_URL_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}

SOCIAL_HOSTS = {
    "bsky.app",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "threads.net",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}
FUNDRAISING_HOSTS = {
    "actblue.com",
    "anedot.com",
    "donorbox.org",
    "gofundme.com",
    "paypal.com",
    "secure.winred.com",
    "winred.com",
}
EVIDENCE_PROFILE_HOSTS = {
    "ballotready.org",
    "ballotpedia.org",
    "justfacts.votesmart.org",
    "vote411.org",
    "votesmart.org",
    "wikipedia.org",
}
SEARCH_OR_CACHE_HOSTS = {
    "bing.com",
    "google.com",
    "search.yahoo.com",
    "webcache.googleusercontent.com",
}
SHARED_CAMPAIGN_HOSTS = {
    "campaignsitebuilder.com",
    "nationbuilder.com",
    "sites.google.com",
    "substack.com",
    "weebly.com",
    "wixsite.com",
    "wordpress.com",
}
OTHER_OFFICE_TERMS = {
    "attorney general",
    "congress",
    "county commissioner",
    "governor",
    "president",
    "school board",
    "secretary of state",
    "senate",
    "state representative",
}
ELECTION_CONTEXT_TERMS = {
    "ballot",
    "campaign",
    "candidate",
    "election",
    "filing",
    "race",
    "voter guide",
    "website",
}
CAMPAIGN_URL_MARKERS = ("formayor", "for-mayor", "vote", "elect", "campaign")

NICKNAME_RE = re.compile(r"(?:(?<=\s)|^)(['\"])([^'\"]{1,50})\1(?=\s|$)")
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
MOJIBAKE_MARKERS = ("Ã", "Â", "â\x80", "\ufffd")


class SearchWaveContractError(ValueError):
    """Raised when a frozen search artifact violates the offline contract."""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(*parts: object) -> str:
    payload = UNIT_SEPARATOR.join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{stable_hash(prefix, *parts)}"


def _tokens(value: object) -> list[str]:
    normalized = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    return re.findall(r"[a-z0-9]+", normalized.casefold())


def _host(url: str) -> str:
    host = (urlparse(url).hostname or "").casefold()
    return host.removeprefix("www.")


def _host_is(host: str, domains: Iterable[str]) -> bool:
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    token = str(value).strip().casefold()
    if token in {"true", "1", "yes", "y"}:
        return True
    if token in {"false", "0", "no", "n", ""}:
        return False
    raise SearchWaveContractError(f"Cannot parse boolean value: {value!r}")


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise SearchWaveContractError(f"{label} is missing columns: {missing}")


def repair_query_identity_text(full_name: object) -> tuple[str, str]:
    """Decode common UTF-8-as-Latin-1 damage without changing the frozen roster."""
    raw = str(full_name)
    repaired = raw
    had_mojibake = any(marker in repaired for marker in MOJIBAKE_MARKERS)
    if had_mojibake:
        try:
            repaired = repaired.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError) as error:
            raise SearchWaveContractError(
                f"Could not deterministically repair candidate query name {raw!r}"
            ) from error
    repaired = repaired.replace("\u00a0", " ")
    repaired = repaired.translate(str.maketrans({"“": '"', "”": '"', "‘": "'", "’": "'"}))
    repaired = " ".join(repaired.split())
    if any(marker in repaired for marker in MOJIBAKE_MARKERS):
        raise SearchWaveContractError(f"Mojibake remains in candidate query name {raw!r}")
    if had_mojibake:
        status = "mojibake_repaired"
    elif repaired != raw:
        status = "query_punctuation_normalized"
    else:
        status = "unchanged"
    return repaired, status


def name_aliases(full_name: object) -> list[str]:
    """Return deterministic legal/nickname search aliases.

    Quoted nicknames are removed from the legal-name phrase and paired with the
    surname in a separate phrase.  Apostrophes inside names (O'Hara, Jacq'ue)
    are not treated as nickname delimiters.
    """
    value, _ = repair_query_identity_text(full_name)
    value = " ".join(value.strip().split())
    nickname_matches = list(NICKNAME_RE.finditer(value))
    legal = NICKNAME_RE.sub(" ", value)
    legal = " ".join(legal.replace('"', "").split())
    aliases: list[str] = []
    if legal:
        aliases.append(legal)
    legal_tokens = legal.split()
    surname = legal_tokens[-1] if legal_tokens else ""
    if len(legal_tokens) > 2:
        aliases.append(f"{legal_tokens[0]} {surname}")
    for match in nickname_matches:
        nickname = " ".join(match.group(2).split())
        if nickname and surname:
            aliases.append(f"{nickname} {surname}")
    return list(dict.fromkeys(alias for alias in aliases if alias))


def identity_alias_token_sets(full_name: object) -> list[tuple[str, ...]]:
    aliases = name_aliases(full_name)
    token_sets: list[tuple[str, ...]] = []
    for alias in aliases:
        tokens = _tokens(alias)
        if len(tokens) >= 2:
            token_sets.append(tuple(tokens))
            if len(tokens) > 2:
                token_sets.append((tokens[0], tokens[-1]))
    return list(dict.fromkeys(token_sets))


def build_one_query(row: Mapping[str, object]) -> str:
    aliases = name_aliases(row["full_name"])
    if not aliases:
        raise SearchWaveContractError("Candidate name produced no search alias")
    quoted = [f'"{alias}"' for alias in aliases]
    identity = quoted[0] if len(quoted) == 1 else f"({' OR '.join(quoted)})"
    city = str(row["geo_name"]).replace('"', "").strip()
    state = str(row["state_abb"]).strip()
    year = int(row["year"])
    return f'{identity} mayor "{city}" {state} {year} (campaign OR website)'


QUERY_MANIFEST_COLUMNS = [
    "search_contract_version",
    "query_template_version",
    "query_attempt_id",
    "candidate_year_id",
    "candidate_contest_id",
    "contest",
    "full_name",
    "query_name",
    "query_name_repair_status",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "sample_order",
    "query",
    "query_sha256",
    "provider",
    "provider_gl",
    "provider_hl",
    "requested_result_count",
    "sample_sha256",
]


def build_expected_query_manifest(sample: pd.DataFrame, sample_sha256: str) -> pd.DataFrame:
    required = {
        "candidate_year_id",
        "candidate_contest_id",
        "contest",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
        "month",
        "sample_order",
    }
    _require_columns(sample, required, "candidate sample")
    if len(sample) != EXPECTED_SAMPLE_ROWS:
        raise SearchWaveContractError(
            f"Candidate sample has {len(sample)} rows; expected {EXPECTED_SAMPLE_ROWS}"
        )
    if sample["candidate_year_id"].isna().any() or sample["candidate_year_id"].duplicated().any():
        raise SearchWaveContractError("Candidate sample keys must be nonmissing and unique")
    rows: list[dict[str, object]] = []
    for row in sample.sort_values("sample_order").to_dict("records"):
        query_name, query_name_repair_status = repair_query_identity_text(row["full_name"])
        query = build_one_query(row)
        query_sha256 = stable_hash(query)
        query_attempt_id = stable_id(
            "query",
            CONTRACT_VERSION,
            sample_sha256,
            row["candidate_year_id"],
            row["candidate_contest_id"],
            QUERY_TEMPLATE_VERSION,
            query_sha256,
            SEARCH_PROVIDER,
            SEARCH_GL,
            SEARCH_HL,
            SEARCH_RESULT_COUNT,
        )
        rows.append(
            {
                "search_contract_version": CONTRACT_VERSION,
                "query_template_version": QUERY_TEMPLATE_VERSION,
                "query_attempt_id": query_attempt_id,
                "candidate_year_id": row["candidate_year_id"],
                "candidate_contest_id": row["candidate_contest_id"],
                "contest": row["contest"],
                "full_name": row["full_name"],
                "query_name": query_name,
                "query_name_repair_status": query_name_repair_status,
                "geo_name": row["geo_name"],
                "state_abb": row["state_abb"],
                "year": int(row["year"]),
                "month": row["month"],
                "sample_order": int(row["sample_order"]),
                "query": query,
                "query_sha256": query_sha256,
                "provider": SEARCH_PROVIDER,
                "provider_gl": SEARCH_GL,
                "provider_hl": SEARCH_HL,
                "requested_result_count": SEARCH_RESULT_COUNT,
                "sample_sha256": sample_sha256,
            }
        )
    return pd.DataFrame(rows, columns=QUERY_MANIFEST_COLUMNS)


def exact_normalized_url(raw_url: object) -> str:
    value = html.unescape(str(raw_url).strip())
    if value.casefold().startswith("www."):
        value = f"http://{value}"
    return canonical_http_url(value)


def candidate_identity_status(full_name: object, text: object) -> tuple[str, str]:
    text_tokens = set(_tokens(text))
    aliases = identity_alias_token_sets(full_name)
    for alias in aliases:
        if set(alias).issubset(text_tokens):
            return "identity_supported", "first_or_nickname_and_surname"
    surnames = {alias[-1] for alias in aliases if alias}
    if surnames.intersection(text_tokens):
        return "identity_unresolved", "surname_only"
    return "identity_not_supported", "no_name_alias"


def classify_candidate_url(
    raw_url: object,
    candidate: Mapping[str, object],
    surrounding_text: object = "",
) -> dict[str, object]:
    raw = html.unescape(str(raw_url).strip())
    exact = exact_normalized_url(raw)
    if not exact:
        return {
            "exact_normalized_url": "",
            "url_host": "",
            "url_type": "invalid_url",
            "url_exclusion_reason": "invalid_or_non_http_url",
            "identity_attribution_status": "identity_not_supported",
            "identity_match_method": "no_name_alias",
            "eligible_campaign_lead": False,
        }
    host = _host(exact)
    combined = f"{surrounding_text} {unquote(exact)}"
    identity_status, identity_method = candidate_identity_status(
        candidate["full_name"], combined
    )
    if _host_is(host, SOCIAL_HOSTS):
        url_type, exclusion = "social_profile", "social_platform"
    elif _host_is(host, FUNDRAISING_HOSTS):
        url_type, exclusion = "fundraising_page", "fundraising_platform"
    elif host.endswith(".gov") or host.endswith(".mil"):
        url_type, exclusion = "officeholder_profile", "government_or_officeholder_site"
    elif _host_is(host, EVIDENCE_PROFILE_HOSTS):
        url_type, exclusion = "evidence_profile", "third_party_profile_not_candidate_owned"
    elif _host_is(host, SEARCH_OR_CACHE_HOSTS):
        url_type, exclusion = "search_or_cache_url", "search_or_cache_url"
    else:
        compact_url = "".join(_tokens(f"{host}{urlparse(exact).path}"))
        alias_sets = identity_alias_token_sets(candidate["full_name"])
        surnames = {alias[-1] for alias in alias_sets if alias}
        first_names = {alias[0] for alias in alias_sets if alias}
        surname_signal = any(surname in compact_url for surname in surnames)
        first_signal = any(first in compact_url for first in first_names if len(first) >= 3)
        campaign_signal = any(marker in compact_url for marker in CAMPAIGN_URL_MARKERS)
        city_signal = "".join(_tokens(candidate.get("geo_name", ""))) in compact_url
        if identity_status == "identity_supported" and surname_signal and (
            first_signal or campaign_signal or city_signal
        ):
            if _host_is(host, SHARED_CAMPAIGN_HOSTS):
                url_type = "shared_host_path"
            else:
                url_type = "custom_domain"
            exclusion = ""
        else:
            url_type, exclusion = "unresolved_web_url", "candidate_ownership_unresolved"
    return {
        "exact_normalized_url": exact,
        "url_host": host,
        "url_type": url_type,
        "url_exclusion_reason": exclusion,
        "identity_attribution_status": identity_status,
        "identity_match_method": identity_method,
        "eligible_campaign_lead": bool(
            identity_status == "identity_supported" and url_type in COVERAGE_URL_TYPES
        ),
    }


def classify_result_page(
    result: Mapping[str, object], candidate: Mapping[str, object]
) -> dict[str, object]:
    raw_url = result.get("raw_result_url", result.get("url", ""))
    title = result.get("result_title", result.get("title", ""))
    snippet = result.get("result_snippet", result.get("snippet", ""))
    rank = result.get("organic_rank", result.get("result_rank", result.get("rank", "")))
    exact = exact_normalized_url(raw_url)
    combined = f"{title} {snippet} {unquote(str(raw_url))}"
    folded = " ".join(_tokens(combined))
    text_tokens = set(_tokens(combined))
    identity_status, identity_method = candidate_identity_status(
        candidate["full_name"], combined
    )
    city_tokens = set(_tokens(candidate.get("geo_name", "")))
    city_match = bool(city_tokens and city_tokens.issubset(text_tokens))
    mayor_match = "mayor" in text_tokens or "mayoral" in text_tokens
    target_year = str(int(candidate["year"]))
    mentioned_years = sorted(set(YEAR_RE.findall(combined)))
    year_match = target_year in mentioned_years
    temporal_conflict = bool(mentioned_years and not year_match)
    office_mismatch = not mayor_match and any(term in folded for term in OTHER_OFFICE_TERMS)
    election_context = any(term in folded for term in ELECTION_CONTEXT_TERMS)
    candidate_url = classify_candidate_url(raw_url, candidate, f"{title} {snippet}")
    host = candidate_url["url_host"]

    page_exclusion = ""
    if not exact:
        role, page_exclusion = "excluded_url", "invalid_or_non_http_url"
    elif _host_is(host, SEARCH_OR_CACHE_HOSTS):
        role, page_exclusion = "excluded_url", "search_or_cache_url"
    elif _host_is(host, SOCIAL_HOSTS):
        role, page_exclusion = "excluded_url", "social_platform"
    elif _host_is(host, FUNDRAISING_HOSTS):
        role, page_exclusion = "excluded_url", "fundraising_platform"
    elif candidate_url["eligible_campaign_lead"]:
        role, page_exclusion = "direct_candidate_site", "direct_site_does_not_need_source_fetch"
    elif identity_status != "identity_supported":
        role = "identity_unresolved"
        page_exclusion = "surname_only" if identity_status == "identity_unresolved" else "candidate_not_named"
    elif office_mismatch:
        role, page_exclusion = "office_mismatch_review", "other_office_without_mayoral_context"
    elif temporal_conflict:
        role, page_exclusion = "temporal_conflict_review", "target_year_absent_other_year_present"
    elif mayor_match and city_match and (year_match or election_context):
        role = "credible_source_page"
    else:
        role, page_exclusion = "context_unresolved", "missing_mayoral_city_or_election_context"

    eligible_for_page_fetch = role == "credible_source_page"
    return {
        "organic_rank": int(rank) if str(rank).strip() else pd.NA,
        "raw_result_url": str(raw_url),
        "exact_normalized_result_url": exact,
        "result_host": host,
        "candidate_identity_status": identity_status,
        "identity_match_method": identity_method,
        "mayoral_context_match": mayor_match,
        "city_context_match": city_match,
        "exact_year_context": year_match,
        "mentioned_years": " | ".join(mentioned_years),
        "temporal_conflict": temporal_conflict,
        "office_mismatch_signal": office_mismatch,
        "candidate_url_type": candidate_url["url_type"],
        "result_page_role": role,
        "eligible_for_page_fetch": eligible_for_page_fetch,
        "page_fetch_exclusion_reason": page_exclusion,
        "classification_reason": role if not page_exclusion else f"{role}:{page_exclusion}",
    }


def make_result_claim_id(row: Mapping[str, object]) -> str:
    return stable_id(
        "result",
        row["query_attempt_id"],
        row["organic_rank"],
        row["raw_result_url"],
        row.get("result_title", ""),
        row.get("result_snippet", ""),
    )


def attach_result_claim_ids(results: pd.DataFrame) -> pd.DataFrame:
    required = {
        "query_attempt_id",
        "organic_rank",
        "raw_result_url",
        "result_title",
        "result_snippet",
    }
    _require_columns(results, required, "ranked result queue")
    output = results.copy()
    output["result_claim_id"] = [make_result_claim_id(row) for row in output.to_dict("records")]
    if output["result_claim_id"].duplicated().any():
        raise SearchWaveContractError("Ranked result queue contains duplicate result claims")
    return output


def rank_result_pages(classified: pd.DataFrame) -> pd.DataFrame:
    required = {
        "candidate_year_id",
        "organic_rank",
        "result_host",
        "eligible_for_page_fetch",
        "exact_year_context",
    }
    _require_columns(classified, required, "classified result queue")
    output = classified.copy()
    output["selected_for_page_fetch"] = False
    output["page_fetch_selection_rank"] = pd.NA
    output["page_fetch_selection_reason"] = output.apply(
        lambda row: "not_eligible:" + str(row.get("page_fetch_exclusion_reason", ""))
        if not _as_bool(row["eligible_for_page_fetch"])
        else "eligible_beyond_candidate_cap",
        axis=1,
    )
    output["_source_priority"] = output["result_host"].map(
        lambda host: 0
        if str(host).endswith(".gov")
        else 1
        if _host_is(str(host), EVIDENCE_PROFILE_HOSTS)
        else 2
    )
    eligible = output.loc[output["eligible_for_page_fetch"].map(_as_bool)].copy()
    eligible = eligible.sort_values(
        [
            "candidate_year_id",
            "exact_year_context",
            "_source_priority",
            "organic_rank",
            "result_claim_id" if "result_claim_id" in eligible.columns else "raw_result_url",
        ],
        ascending=[True, False, True, True, True],
    )
    eligible["_selection_rank"] = eligible.groupby("candidate_year_id").cumcount() + 1
    for index, selection_rank in eligible["_selection_rank"].items():
        output.at[index, "page_fetch_selection_rank"] = int(selection_rank)
        if selection_rank <= MAX_SOURCE_PAGES_PER_CANDIDATE:
            output.at[index, "selected_for_page_fetch"] = True
            output.at[index, "page_fetch_selection_reason"] = "selected_top_three_credible_pages"
    return output.drop(columns="_source_priority")


def attach_url_claim_ids(leads: pd.DataFrame) -> pd.DataFrame:
    required = {
        "result_claim_id",
        "query_attempt_id",
        "candidate_year_id",
        "exact_normalized_url",
        "extraction_method",
    }
    _require_columns(leads, required, "URL leads")
    output = leads.copy()
    if "discovery_candidate_year_id" not in output.columns:
        output["discovery_candidate_year_id"] = output["candidate_year_id"]
    output["url_claim_id"] = [
        stable_id(
            "urlclaim",
            row["result_claim_id"],
            row["candidate_year_id"],
            row["exact_normalized_url"],
            row["extraction_method"],
        )
        for row in output.to_dict("records")
    ]
    output["url_entity_id"] = [
        stable_id("urlentity", row["candidate_year_id"], row["exact_normalized_url"])
        for row in output.to_dict("records")
    ]
    if output["url_claim_id"].duplicated().any():
        raise SearchWaveContractError(
            "Duplicate URL-claim IDs found; rerun duplicates must be rejected, not silently collapsed"
        )
    return output


def build_url_entity_index(leads: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        leads,
        {"url_claim_id", "url_entity_id", "candidate_year_id", "exact_normalized_url"},
        "URL leads",
    )
    return (
        leads.groupby(
            ["url_entity_id", "candidate_year_id", "exact_normalized_url"],
            as_index=False,
            dropna=False,
        )
        .agg(
            independent_claim_count=("url_claim_id", "nunique"),
            url_claim_ids=("url_claim_id", lambda x: " | ".join(sorted(set(map(str, x))))),
        )
    )


def audit_filter_stage(
    stage: str,
    before_rows: int,
    after_rows: int,
    reason_counts: Mapping[str, int] | None = None,
    parent_rows_retained: bool = False,
) -> dict[str, object]:
    if after_rows > before_rows or min(before_rows, after_rows) < 0:
        raise SearchWaveContractError(f"Invalid filter counts for {stage}")
    removed = before_rows - after_rows
    fraction = removed / before_rows if before_rows else 0.0
    reasons = {str(key): int(value) for key, value in (reason_counts or {}).items()}
    explicitly_accounted = sum(reasons.values()) == removed
    if fraction > 0.20 and not (explicitly_accounted or parent_rows_retained):
        raise SearchWaveContractError(
            f"{stage} removes {removed}/{before_rows} rows ({fraction:.1%}) without an exact reason audit"
        )
    return {
        "stage": stage,
        "before_rows": int(before_rows),
        "after_rows": int(after_rows),
        "removed_rows": int(removed),
        "removed_fraction": fraction,
        "reason_counts": reasons,
        "parent_rows_retained": bool(parent_rows_retained),
        "explicit_audit_present": bool(explicitly_accounted or parent_rows_retained or fraction <= 0.20),
    }


def _validate_query_manifest(sample: pd.DataFrame, manifest: pd.DataFrame, sample_hash: str) -> None:
    if "screen_sha256" in manifest.columns:
        required = {
            "contract_version",
            "query_version",
            "query_attempt_id",
            "query_attempt_hash",
            "candidate_year_id",
            "candidate_contest_id",
            "contest",
            "full_name",
            "query_name",
            "query_name_repaired",
            "query_name_repair_method",
            "geo_name",
            "state_abb",
            "year",
            "month",
            "sample_order",
            "query",
            "query_sha256",
            "provider",
            "country",
            "language",
            "results_requested",
            "screen_sha256",
        }
        _require_columns(manifest, required, "query manifest")
        if len(manifest) != EXPECTED_SAMPLE_ROWS:
            raise SearchWaveContractError(
                f"Query manifest has {len(manifest)} rows; expected {EXPECTED_SAMPLE_ROWS}"
            )
        if manifest["query_attempt_id"].duplicated().any() or manifest[
            "candidate_year_id"
        ].duplicated().any():
            raise SearchWaveContractError(
                "Runner query manifest must contain one unique attempt per candidate"
            )
        sample_lookup = sample.set_index("candidate_year_id", drop=False)
        expected_keys = set(sample_lookup.index.astype(str))
        observed_keys = set(manifest["candidate_year_id"].astype(str))
        if expected_keys != observed_keys:
            raise SearchWaveContractError(
                f"Runner query-manifest candidate keys differ from frozen screen: "
                f"{len(expected_keys-observed_keys)} missing, {len(observed_keys-expected_keys)} unexpected"
            )
        for row in manifest.to_dict("records"):
            candidate_id = str(row["candidate_year_id"])
            source = sample_lookup.loc[candidate_id]
            for column in (
                "candidate_contest_id",
                "contest",
                "full_name",
                "geo_name",
                "state_abb",
                "year",
                "sample_order",
            ):
                if str(row[column]) != str(source[column]):
                    raise SearchWaveContractError(
                        f"Runner query context mismatch for {candidate_id}.{column}"
                    )
            query_name, repair_status = repair_query_identity_text(source["full_name"])
            observed_query_name, _ = repair_query_identity_text(row["query_name"])
            if observed_query_name != query_name:
                raise SearchWaveContractError(
                    f"Runner query-name repair mismatch for {candidate_id}"
                )
            repaired_flag = _as_bool(row["query_name_repaired"])
            if repaired_flag != (repair_status == "mojibake_repaired"):
                raise SearchWaveContractError(
                    f"Runner query-name repair flag mismatch for {candidate_id}"
                )
            query = str(row["query"])
            if any(marker in query for marker in MOJIBAKE_MARKERS):
                raise SearchWaveContractError(f"Mojibake remains in query for {candidate_id}")
            required_query_parts = [
                " mayor ",
                f'"{str(source["geo_name"])}"',
                str(source["state_abb"]),
                str(int(source["year"])),
                "(campaign OR website)",
            ]
            if not all(part in query for part in required_query_parts):
                raise SearchWaveContractError(
                    f"Runner query omits an approved identity/place/year/recall component for {candidate_id}"
                )
            repaired_identity, _ = repair_query_identity_text(source["full_name"])
            nickname_matches = list(NICKNAME_RE.finditer(repaired_identity))
            legal_alias = " ".join(NICKNAME_RE.sub(" ", repaired_identity).split())
            surname = legal_alias.split()[-1]
            required_aliases = [legal_alias] + [
                f"{' '.join(match.group(2).split())} {surname}"
                for match in nickname_matches
            ]
            if not all(f'"{alias}"' in query for alias in required_aliases):
                raise SearchWaveContractError(
                    f"Runner query omits a legal/nickname identity alias for {candidate_id}"
                )
            query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
            if str(row["query_sha256"]) != query_hash:
                raise SearchWaveContractError(f"Runner query hash mismatch for {candidate_id}")
            if (
                str(row["provider"]) != SEARCH_PROVIDER
                or str(row["country"]) != SEARCH_GL
                or str(row["language"]) != SEARCH_HL
                or int(row["results_requested"]) != SEARCH_RESULT_COUNT
                or str(row["screen_sha256"]) != sample_hash
            ):
                raise SearchWaveContractError(
                    f"Runner request parameters drift for {candidate_id}"
                )
            attempt_hash = stable_hash(
                sample_hash,
                row["candidate_year_id"],
                row["candidate_contest_id"],
                query,
                row["query_version"],
                row["provider"],
                row["country"],
                row["language"],
                int(row["results_requested"]),
            )
            if str(row["query_attempt_hash"]) != attempt_hash or str(
                row["query_attempt_id"]
            ) != f"serper_query_{attempt_hash[:24]}":
                raise SearchWaveContractError(
                    f"Runner query-attempt key mismatch for {candidate_id}"
                )
        return
    _require_columns(manifest, QUERY_MANIFEST_COLUMNS, "query manifest")
    expected = build_expected_query_manifest(sample, sample_hash)
    if len(manifest) != EXPECTED_SAMPLE_ROWS:
        raise SearchWaveContractError(
            f"Query manifest has {len(manifest)} rows; expected {EXPECTED_SAMPLE_ROWS}"
        )
    if manifest["query_attempt_id"].duplicated().any():
        raise SearchWaveContractError("Query manifest has duplicate query-attempt IDs")
    if manifest["candidate_year_id"].duplicated().any():
        raise SearchWaveContractError("The first wave must have exactly one query per candidate")
    compare_columns = QUERY_MANIFEST_COLUMNS
    observed = manifest[compare_columns].copy().sort_values("candidate_year_id").reset_index(drop=True)
    expected = expected[compare_columns].sort_values("candidate_year_id").reset_index(drop=True)
    for column in ("year", "sample_order"):
        observed[column] = pd.to_numeric(observed[column], errors="raise").astype(int)
        expected[column] = pd.to_numeric(expected[column], errors="raise").astype(int)
    for column in compare_columns:
        if column == "month":
            left = observed[column].astype("string").fillna("<NA>")
            right = expected[column].astype("string").fillna("<NA>")
        else:
            left = observed[column].astype(str)
            right = expected[column].astype(str)
        mismatch = left.ne(right)
        if mismatch.any():
            example = observed.loc[mismatch, ["candidate_year_id", column]].head(3).to_dict("records")
            raise SearchWaveContractError(
                f"Query manifest differs from frozen contract in {column}: {example}"
            )


def _validate_request_ledger(manifest: pd.DataFrame, ledger: pd.DataFrame) -> dict[str, object]:
    required = {
        "query_attempt_id",
        "candidate_year_id",
        "request_status",
        "terminal_for_run",
        "retryable",
        "complete",
        "result_count",
        "error",
    }
    _require_columns(ledger, required, "request ledger")
    if ledger["query_attempt_id"].duplicated().any():
        raise SearchWaveContractError("Request ledger must contain one current row per query attempt")
    expected_keys = set(manifest["query_attempt_id"].astype(str))
    observed_keys = set(ledger["query_attempt_id"].astype(str))
    missing = sorted(expected_keys - observed_keys)
    unexpected = sorted(observed_keys - expected_keys)
    if missing or unexpected:
        raise SearchWaveContractError(
            f"Request-ledger key mismatch: {len(missing)} missing, {len(unexpected)} unexpected"
        )
    joined = ledger.merge(
        manifest[["query_attempt_id", "candidate_year_id"]],
        on="query_attempt_id",
        how="left",
        suffixes=("", "_manifest"),
        validate="one_to_one",
    )
    if joined["candidate_year_id"].astype(str).ne(joined["candidate_year_id_manifest"].astype(str)).any():
        raise SearchWaveContractError("Request ledger carries candidate context from a different query")
    unknown = sorted(set(joined["request_status"].astype(str)) - REQUEST_STATUSES)
    if unknown:
        raise SearchWaveContractError(f"Unknown request statuses: {unknown}")
    for row in joined.to_dict("records"):
        status = str(row["request_status"])
        terminal = _as_bool(row["terminal_for_run"])
        retryable = _as_bool(row["retryable"])
        complete = _as_bool(row["complete"])
        count = int(row["result_count"])
        expected_flags = {
            "found": (True, False, True),
            "no_result": (True, False, True),
            "request_error": (True, True, False),
            "provider_error": (True, False, False),
        }[status]
        if (terminal, retryable, complete) != expected_flags:
            raise SearchWaveContractError(
                f"Invalid terminal/retry/complete flags for {status} ({row['query_attempt_id']})"
            )
        if status == "found" and count <= 0:
            raise SearchWaveContractError("found request must have at least one organic result")
        if status != "found" and count != 0:
            raise SearchWaveContractError(f"{status} request must have result_count=0")
        if status in {"request_error", "provider_error"} and not str(row.get("error", "")).strip():
            raise SearchWaveContractError(f"{status} request must preserve an error message")
    statuses = Counter(joined["request_status"].astype(str))
    return {
        "rows": len(joined),
        "key_agreement": True,
        "status_counts": dict(sorted(statuses.items())),
        "retryable_rows": int(joined["retryable"].map(_as_bool).sum()),
        "complete_rows": int(joined["complete"].map(_as_bool).sum()),
    }


def _validate_raw_responses(
    manifest: pd.DataFrame,
    ledger: pd.DataFrame,
    raw_responses: list[Mapping[str, object]],
) -> dict[str, object]:
    required = {
        "query_attempt_id",
        "candidate_year_id",
        "request_status",
        "raw_response_sha256",
        "raw_response_body",
        "raw_response",
    }
    for index, record in enumerate(raw_responses):
        missing = sorted(required - set(record))
        if missing:
            raise SearchWaveContractError(
                f"Raw response {index} is missing fields: {missing}"
            )
    keys = [str(record["query_attempt_id"]) for record in raw_responses]
    if len(keys) != len(set(keys)):
        raise SearchWaveContractError("Raw response JSONL has duplicate query-attempt keys")
    ledger_keys = set(ledger["query_attempt_id"].astype(str))
    raw_keys = set(keys)
    if ledger_keys != raw_keys:
        raise SearchWaveContractError(
            f"Raw-response/ledger key mismatch: {len(ledger_keys-raw_keys)} missing, "
            f"{len(raw_keys-ledger_keys)} unexpected"
        )
    manifest_context = manifest.set_index("query_attempt_id")["candidate_year_id"].astype(str)
    ledger_lookup = ledger.set_index("query_attempt_id")
    organic_count = 0
    for record in raw_responses:
        attempt_id = str(record["query_attempt_id"])
        if str(record["candidate_year_id"]) != manifest_context.loc[attempt_id]:
            raise SearchWaveContractError("Raw response carries mismatched candidate context")
        ledger_row = ledger_lookup.loc[attempt_id]
        if str(record["request_status"]) != str(ledger_row["request_status"]):
            raise SearchWaveContractError("Raw response and request ledger disagree on status")
        if str(record["raw_response_sha256"]) != str(ledger_row["raw_response_sha256"]):
            raise SearchWaveContractError("Raw response and request ledger disagree on response hash")
        body = str(record["raw_response_body"])
        if str(record["request_status"]) in {"found", "no_result"} and not body:
            raise SearchWaveContractError(
                f"Successful provider response body is missing for {attempt_id}"
            )
        observed_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        if observed_hash != str(record["raw_response_sha256"]):
            raise SearchWaveContractError(
                f"Raw response body hash mismatch for {attempt_id}"
            )
        payload = record["raw_response"]
        organic = payload.get("organic", []) if isinstance(payload, Mapping) else []
        if not isinstance(organic, list):
            organic = []
        organic_count += len(organic)
        if len(organic) != int(ledger_row["result_count"]):
            raise SearchWaveContractError(
                f"Raw organic count disagrees with ledger for {attempt_id}"
            )
    return {
        "rows": len(raw_responses),
        "exact_ledger_key_agreement": True,
        "full_response_body_hashes_verified": True,
        "organic_results": organic_count,
    }


def _validate_result_queue(
    manifest: pd.DataFrame, ledger: pd.DataFrame, queue: pd.DataFrame
) -> dict[str, object]:
    required = {
        "query_attempt_id",
        "result_claim_id",
        "candidate_year_id",
        "organic_rank",
        "raw_result_url",
        "result_title",
        "result_snippet",
        "candidate_identity_status",
        "result_page_role",
        "eligible_for_page_fetch",
        "selected_for_page_fetch",
        "page_fetch_selection_reason",
        "classification_reason",
    }
    _require_columns(queue, required, "ranked result-page queue")
    if queue["result_claim_id"].duplicated().any():
        raise SearchWaveContractError("Ranked result-page queue has duplicate result claims")
    if queue.duplicated(["query_attempt_id", "organic_rank"]).any():
        raise SearchWaveContractError("Organic rank must be unique within each query attempt")
    expected_attempts = set(manifest["query_attempt_id"].astype(str))
    unexpected = set(queue["query_attempt_id"].astype(str)) - expected_attempts
    if unexpected:
        raise SearchWaveContractError(f"Result queue has {len(unexpected)} unexpected query attempts")
    provenance = queue.merge(
        manifest[["query_attempt_id", "candidate_year_id"]],
        on="query_attempt_id",
        how="left",
        suffixes=("", "_manifest"),
        validate="many_to_one",
    )
    if provenance["candidate_year_id"].astype(str).ne(
        provenance["candidate_year_id_manifest"].astype(str)
    ).any():
        raise SearchWaveContractError("Result queue misattributes a result to another candidate")
    if "raw_result_sha256" in queue.columns:
        recomputed_ids = [
            "serper_result_"
            + stable_hash(
                row["contract_version"],
                row["query_attempt_id"],
                row["organic_rank"],
                row["raw_result_url"],
                row["raw_result_sha256"],
            )[:24]
            for row in queue.to_dict("records")
        ]
        if "classifier_version" not in queue.columns or queue[
            "classifier_version"
        ].fillna("").astype(str).str.strip().eq("").any():
            raise SearchWaveContractError(
                "Runner result classifications must carry a nonblank classifier version"
            )
    else:
        recomputed_ids = [make_result_claim_id(row) for row in queue.to_dict("records")]
    if list(queue["result_claim_id"].astype(str)) != recomputed_ids:
        raise SearchWaveContractError("Result-claim IDs do not match raw query/rank/result evidence")
    unknown_roles = sorted(set(queue["result_page_role"].astype(str)) - RESULT_PAGE_ROLES)
    if unknown_roles:
        raise SearchWaveContractError(f"Unknown result-page roles: {unknown_roles}")
    blank_reason = queue["classification_reason"].fillna("").astype(str).str.strip().eq("")
    if blank_reason.any():
        raise SearchWaveContractError("Every organic result must have an explicit classification reason")
    selected = queue["selected_for_page_fetch"].map(_as_bool)
    eligible = queue["eligible_for_page_fetch"].map(_as_bool)
    if (selected & ~eligible).any():
        raise SearchWaveContractError("An ineligible result page was selected for fetch")
    if queue.assign(_selected=selected).groupby("candidate_year_id")["_selected"].sum().gt(
        MAX_SOURCE_PAGES_PER_CANDIDATE
    ).any():
        raise SearchWaveContractError("More than three source pages selected for a candidate")
    beyond_cap = eligible & ~selected
    if beyond_cap.any() and not queue.loc[beyond_cap, "page_fetch_selection_reason"].astype(str).eq(
        "eligible_beyond_candidate_cap"
    ).all():
        raise SearchWaveContractError("Credible pages beyond the cap must remain reason-coded")
    ledger_counts = ledger.set_index("query_attempt_id")["result_count"].astype(int)
    queue_counts = queue.groupby("query_attempt_id").size()
    for attempt_id, expected_count in ledger_counts.items():
        observed_count = int(queue_counts.get(attempt_id, 0))
        if observed_count != int(expected_count):
            raise SearchWaveContractError(
                f"Organic-result count mismatch for {attempt_id}: ledger={expected_count}, queue={observed_count}"
            )
    reason_counts = Counter(
        queue.loc[~eligible, "classification_reason"].fillna("missing_reason").astype(str)
    )
    filter_audit = audit_filter_stage(
        "organic_results_to_fetch_eligible",
        len(queue),
        int(eligible.sum()),
        reason_counts,
        parent_rows_retained=True,
    )
    return {
        "rows": len(queue),
        "result_count_agreement": True,
        "role_counts": dict(sorted(Counter(queue["result_page_role"].astype(str)).items())),
        "eligible_source_pages": int(eligible.sum()),
        "selected_source_pages": int(selected.sum()),
        "credible_beyond_cap_retained": int(beyond_cap.sum()),
        "filter_audit": filter_audit,
    }


def _validate_url_leads(queue: pd.DataFrame, leads: pd.DataFrame) -> dict[str, object]:
    required = {
        "url_claim_id",
        "url_entity_id",
        "result_claim_id",
        "query_attempt_id",
        "discovery_candidate_year_id",
        "candidate_year_id",
        "raw_url",
        "exact_normalized_url",
        "url_type",
        "identity_attribution_status",
        "eligible_for_temporal_validation",
        "extraction_method",
    }
    if leads.empty and "discovery_candidate_year_id" not in leads.columns:
        leads = leads.copy()
        leads["discovery_candidate_year_id"] = pd.Series(dtype=str)
    _require_columns(leads, required, "URL leads")
    if leads["url_claim_id"].duplicated().any():
        raise SearchWaveContractError("URL leads contain duplicate claim IDs")
    queue_context = queue[
        ["result_claim_id", "query_attempt_id", "candidate_year_id", "result_page_role", "raw_result_url"]
    ]
    joined = leads.merge(
        queue_context,
        on="result_claim_id",
        how="left",
        suffixes=("", "_result"),
        validate="many_to_one",
        indicator=True,
    )
    if joined["_merge"].ne("both").any():
        raise SearchWaveContractError("URL lead lacks an originating organic result claim")
    if joined["query_attempt_id"].astype(str).ne(
        joined["query_attempt_id_result"].astype(str)
    ).any():
        raise SearchWaveContractError(
            "URL lead carries inconsistent query_attempt_id provenance"
        )
    if joined["discovery_candidate_year_id"].astype(str).ne(
        joined["candidate_year_id_result"].astype(str)
    ).any():
        raise SearchWaveContractError(
            "URL lead carries inconsistent discovery-candidate provenance"
        )
    runner_contract = bool(
        len(leads)
        and "contract_version" in leads.columns
        and leads["contract_version"].astype(str).str.startswith("mayoral-search-").all()
    )
    if runner_contract:
        expected_entity_ids = [
            "url_entity_"
            + stable_hash(row["candidate_year_id"], row["exact_normalized_url"])[:24]
            for row in leads.to_dict("records")
        ]
        if not leads["url_claim_id"].astype(str).str.fullmatch(
            r"search_url_claim_[0-9a-f]{24}"
        ).all():
            raise SearchWaveContractError("Runner URL-claim IDs violate the frozen format")
        if "parser_version" not in leads.columns or leads["parser_version"].fillna(
            ""
        ).astype(str).str.strip().eq("").any():
            raise SearchWaveContractError("Runner URL leads lack parser-version provenance")
        if "review_status" in leads.columns and not leads["review_status"].astype(str).eq(
            "pending"
        ).all():
            raise SearchWaveContractError("Runner URL leads must remain pending")
        if "evidence_grade" in leads.columns and not leads["evidence_grade"].astype(str).eq(
            "D"
        ).all():
            raise SearchWaveContractError("Runner search leads must remain grade D")
        if "strict_coverage_eligible" in leads.columns and leads[
            "strict_coverage_eligible"
        ].map(_as_bool).any():
            raise SearchWaveContractError("Runner search leads cannot create strict coverage")
    else:
        expected_claim_ids = [
            stable_id(
                "urlclaim",
                row["result_claim_id"],
                row["candidate_year_id"],
                row["exact_normalized_url"],
                row["extraction_method"],
            )
            for row in leads.to_dict("records")
        ]
        expected_entity_ids = [
            stable_id("urlentity", row["candidate_year_id"], row["exact_normalized_url"])
            for row in leads.to_dict("records")
        ]
        if list(leads["url_claim_id"].astype(str)) != expected_claim_ids:
            raise SearchWaveContractError("URL-claim IDs do not preserve result-level provenance")
    if list(leads["url_entity_id"].astype(str)) != expected_entity_ids:
        raise SearchWaveContractError("URL-entity IDs do not match candidate and exact URL")
    eligible = leads["eligible_for_temporal_validation"].map(_as_bool)
    invalid_gate = eligible & (
        leads["identity_attribution_status"].astype(str).ne("identity_supported")
        | ~leads["url_type"].astype(str).isin(COVERAGE_URL_TYPES)
    )
    if invalid_gate.any():
        raise SearchWaveContractError(
            "Temporal validation includes a non-coverage URL type or unresolved identity"
        )
    # Every direct result is an independent claim, even when several results
    # identify the same candidate URL.
    direct = queue.loc[queue["result_page_role"].eq("direct_candidate_site")]
    direct_claims = set(direct["result_claim_id"].astype(str))
    lead_direct_claims = set(
        leads.loc[leads["result_claim_id"].astype(str).isin(direct_claims), "result_claim_id"].astype(str)
    )
    missing_direct = direct_claims - lead_direct_claims
    if missing_direct:
        raise SearchWaveContractError(
            f"{len(missing_direct)} direct candidate-site result claims were silently dropped"
        )
    entities = build_url_entity_index(leads) if len(leads) else pd.DataFrame()
    return {
        "rows": len(leads),
        "entity_rows": len(entities),
        "independent_claims_preserved": True,
        "duplicate_entity_claims": int((entities["independent_claim_count"] > 1).sum())
        if len(entities)
        else 0,
        "temporal_validation_eligible": int(eligible.sum()),
    }


def _normalize_runner_artifacts(
    request_ledger: pd.DataFrame,
    result_queue: pd.DataFrame,
    url_leads: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Map the acquisition runner's public columns into this audit contract."""
    ledger = request_ledger.copy()
    if "results_returned" in ledger.columns:
        ledger = ledger.rename(
            columns={"results_returned": "result_count", "complete_for_wave": "complete"}
        )
    queue = result_queue.copy()
    if "result_rank" in queue.columns:
        queue = queue.rename(
            columns={
                "result_rank": "organic_rank",
                "result_url": "raw_result_url",
                "candidate_attribution_status": "candidate_identity_status",
                "page_selection_reason": "page_fetch_selection_reason",
            }
        )
        role_map = {
            "direct_candidate_site": "direct_candidate_site",
            "credible_source_page": "credible_source_page",
            "credible_source_page_temporal_conflict": "temporal_conflict_review",
            "excluded_url": "excluded_url",
            "identity_unresolved": "identity_unresolved",
            "noise": "context_unresolved",
        }
        queue["result_page_role"] = queue["result_classification"].map(role_map)
        unknown = queue["result_page_role"].isna() if len(queue) else pd.Series(dtype=bool)
        if len(queue) and unknown.any():
            bad = sorted(set(queue.loc[unknown, "result_classification"].astype(str)))
            raise SearchWaveContractError(f"Runner result classifications are unknown: {bad}")
        queue["classification_reason"] = (
            queue["result_classification"].fillna("").astype(str)
            + ":"
            + queue.get("candidate_attribution_reason", "").fillna("").astype(str)
            + ":"
            + queue.get("url_exclusion_reason", "").fillna("").astype(str)
        )
    leads = url_leads.copy()
    if "strict_coverage_eligible" in leads.columns:
        leads = leads.rename(
            columns={"candidate_attribution_status": "identity_attribution_status"}
        )
        leads["eligible_for_temporal_validation"] = (
            leads["identity_attribution_status"].astype(str).eq("identity_supported")
            & leads["url_type"].astype(str).isin(COVERAGE_URL_TYPES)
        )
    return ledger, queue, leads


def audit_wave_outputs(
    sample: pd.DataFrame,
    sample_hash: str,
    query_manifest: pd.DataFrame,
    request_ledger: pd.DataFrame,
    result_queue: pd.DataFrame,
    url_leads: pd.DataFrame,
    raw_responses: list[Mapping[str, object]] | None = None,
) -> dict[str, object]:
    _validate_query_manifest(sample, query_manifest, sample_hash)
    normalized_ledger, normalized_queue, normalized_leads = _normalize_runner_artifacts(
        request_ledger, result_queue, url_leads
    )
    ledger_audit = _validate_request_ledger(query_manifest, normalized_ledger)
    raw_audit = (
        _validate_raw_responses(query_manifest, normalized_ledger, raw_responses)
        if raw_responses is not None
        else {"not_supplied": True}
    )
    result_audit = _validate_result_queue(
        query_manifest, normalized_ledger, normalized_queue
    )
    lead_audit = _validate_url_leads(normalized_queue, normalized_leads)
    request_filter = audit_filter_stage(
        "planned_queries_to_terminal_request_rows",
        len(query_manifest),
        len(normalized_ledger),
        {},
    )
    raw_filter = audit_filter_stage(
        "saved_organic_results_to_classified_result_rows",
        int(normalized_ledger["result_count"].astype(int).sum()),
        len(normalized_queue),
        {},
    )
    return {
        "contract_version": CONTRACT_VERSION,
        "pass": True,
        "sample_rows": len(sample),
        "sample_sha256": sample_hash,
        "query_attempt_rows": len(query_manifest),
        "one_query_per_candidate": True,
        "request_ledger": ledger_audit,
        "raw_responses": raw_audit,
        "ranked_result_page_queue": result_audit,
        "url_leads": lead_audit,
        "filter_audits": [request_filter, raw_filter, result_audit["filter_audit"]],
        "ready_for_downstream_fetch": ledger_audit["retryable_rows"] == 0
        and ledger_audit["complete_rows"] == EXPECTED_SAMPLE_ROWS,
    }


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def audit_prior_pipeline(repo_root: Path = REPO_ROOT) -> dict[str, object]:
    """Measure observed accounting failures in the frozen 120-case pilot."""
    from scripts.discover_mayoral_search_urls import build_lead_queue, result_evidence

    pilot = repo_root / "data/municipal_discovery/search_pilot"
    sample_path = (
        repo_root
        / "data/municipal_discovery/screening_2010_2021/candidate_screening_sample_2010_2021.csv"
    )
    manifest_path = pilot / "query_manifest.csv"
    results_path = pilot / "search_results.jsonl"
    queue_path = pilot / "search_lead_review.csv"
    expansion_path = pilot / "source_page_expansion.csv"
    unified_path = pilot / "search_evidence_review.csv"
    manifest = pd.read_csv(manifest_path, low_memory=False)
    records = _read_jsonl(results_path)
    queue = pd.read_csv(queue_path, low_memory=False)
    expansion = pd.read_csv(expansion_path, low_memory=False)
    unified = pd.read_csv(unified_path, low_memory=False)
    sample = pd.read_csv(sample_path, low_memory=False)

    manifest_keys = set(zip(manifest["candidate_year_id"], manifest["query_variant"]))
    result_keys = {
        (record["candidate_year_id"], record["query_variant"]) for record in records
    }
    missing_attempts = sorted(manifest_keys - result_keys)
    unexpected_attempts = sorted(result_keys - manifest_keys)
    raw_result_count = sum(len(record.get("results", [])) for record in records)
    stored_classification_count = sum(
        bool(str(item.get("evidence_type", "")).strip())
        for record in records
        for item in record.get("results", [])
    )
    stored_evidence_counts = Counter(
        str(item.get("evidence_type", "missing"))
        for record in records
        for item in record.get("results", [])
    )
    rescored_evidence_counts = Counter(
        result_evidence(item, record)["evidence_type"]
        for record in records
        for item in record.get("results", [])
    )

    flattened_claims: list[dict[str, object]] = []
    for record in records:
        for item in record.get("results", []):
            rescored = {**item, **result_evidence(item, record)}
            evidence_type = rescored.get("evidence_type")
            if evidence_type == "candidate_domain":
                proposed = [canonical_http_url(str(rescored.get("url", ""))) or rescored.get("url", "")]
            elif evidence_type == "embedded_domain_evidence":
                proposed = rescored.get("embedded_urls", [])
            elif evidence_type == "source_evidence":
                proposed = [""]
            else:
                proposed = []
            for proposed_url in proposed:
                flattened_claims.append(
                    {
                        "candidate_year_id": record["candidate_year_id"],
                        "candidate_contest_id": record.get("candidate_contest_id", ""),
                        "query_variant": record["query_variant"],
                        "result_rank": rescored.get("rank", ""),
                        "proposed_campaign_url": proposed_url,
                        "evidence_source_url": rescored.get("url", ""),
                    }
                )
    rebuilt_queue = build_lead_queue(records)
    legacy_dedup_keys = [
        "candidate_year_id",
        "candidate_contest_id",
        "proposed_campaign_url",
        "evidence_source_url",
    ]
    flattened = pd.DataFrame(flattened_claims)
    collapsed_claims = (
        len(flattened) - len(flattened.drop_duplicates(legacy_dedup_keys))
        if len(flattened)
        else 0
    )

    source_keys = ["candidate_year_id", "candidate_contest_id", "evidence_source_url"]
    eligible_source = queue.loc[
        queue["evidence_type"].eq("source_evidence")
        & ~queue["office_mismatch_signal"].fillna(False).astype(bool),
        source_keys,
    ].drop_duplicates()
    expanded_source = expansion[source_keys].drop_duplicates()
    source_merge = eligible_source.merge(
        expanded_source, on=source_keys, how="outer", indicator=True
    )
    missing_expansion = int(source_merge["_merge"].eq("left_only").sum())
    missing_expansion_keys = source_merge.loc[
        source_merge["_merge"].eq("left_only"), source_keys
    ]
    missing_expansion_rows = eligible_source.merge(
        queue[source_keys + ["temporal_conflict"]],
        on=source_keys,
        how="left",
        validate="one_to_one",
    ).merge(
        missing_expansion_keys,
        on=source_keys,
        how="inner",
        validate="one_to_one",
    )
    missing_expansion_temporal_conflicts = int(
        missing_expansion_rows["temporal_conflict"].fillna(False).astype(bool).sum()
    )

    compare_keys = [
        "candidate_year_id",
        "query_variant",
        "result_rank",
        "evidence_source_url",
    ]
    case_compare = rebuilt_queue.merge(
        queue,
        on=compare_keys,
        how="inner",
        suffixes=("_rebuilt", "_stored"),
    )
    rebuilt_url = case_compare["proposed_campaign_url_rebuilt"].fillna("").astype(str)
    stored_url = case_compare["proposed_campaign_url_stored"].fillna("").astype(str)
    case_loss = rebuilt_url.str.casefold().eq(stored_url.str.casefold()) & rebuilt_url.ne(stored_url)
    case_examples = case_compare.loc[
        case_loss,
        ["candidate_year_id", "proposed_campaign_url_rebuilt", "proposed_campaign_url_stored"],
    ].to_dict("records")

    nickname_rows = int(sample["full_name"].astype(str).map(lambda value: bool(NICKNAME_RE.search(value))).sum())
    mojibake_rows = int(
        sample["full_name"].astype(str).map(
            lambda value: any(marker in value for marker in MOJIBAKE_MARKERS)
        ).sum()
    )
    missing_month_rows = int(sample["month"].isna().sum())
    raw_to_queue_drop = audit_filter_stage(
        "raw_organic_results_to_review_queue",
        raw_result_count,
        len(queue),
        {"not_exported_to_review_queue": raw_result_count - len(queue)},
    )
    source_expansion_drop = audit_filter_stage(
        "eligible_source_pages_to_expansion_attempts",
        len(eligible_source),
        len(eligible_source) - missing_expansion,
        {"missing_terminal_expansion_attempt": missing_expansion},
    )
    findings = [
        {
            "severity": "high",
            "issue": "manifest_attempts_without_terminal_rows",
            "evidence": f"{len(missing_attempts)} of {len(manifest)} query rows have no search-results record",
            "required_fix": "one current terminal-for-run ledger row for every frozen query_attempt_id",
        },
        {
            "severity": "high",
            "issue": "resume_key_omits_request_identity",
            "evidence": "legacy completion key contains candidate_year_id and query_variant only",
            "required_fix": "hash sample, candidate/contest, exact query/version, provider, gl, hl, and num into query_attempt_id",
        },
        {
            "severity": "high",
            "issue": "eligible_source_pages_without_attempt_rows",
            "evidence": (
                f"{missing_expansion} of {len(eligible_source)} eligible source-page keys are absent "
                f"from expansion output; all {missing_expansion_temporal_conflicts} are retained "
                "date-conflict leads"
            ),
            "required_fix": "preserve a terminal fetch-attempt row for every selected result claim",
        },
        {
            "severity": "high",
            "issue": "candidate_attribution_too_weak",
            "evidence": "legacy direct-domain and evidence rules can pass surname plus city/mayor context",
            "required_fix": "require legal-first-or-nickname plus surname for candidate attribution",
        },
        {
            "severity": "high",
            "issue": "shared_host_paths_are_screened_by_hostname_only",
            "evidence": "legacy candidate-shaped-domain logic ignores identity and campaign signals in URL paths",
            "required_fix": "classify exact custom-domain, candidate-subdomain, and shared-host-path leads separately",
        },
        {
            "severity": "moderate",
            "issue": "independent_claims_collapsed",
            "evidence": f"legacy lead deduplication collapses {collapsed_claims} result-level claim(s) in the frozen pilot",
            "required_fix": "retain result_claim_id and url_claim_id; group only through a separate url_entity_id",
        },
        {
            "severity": "moderate",
            "issue": "result_classification_has_no_versioned_audit",
            "evidence": (
                f"the same frozen flattened results contain "
                f"{stored_evidence_counts.get('source_evidence', 0)} stored source_evidence labels "
                f"but current rescoring yields {rescored_evidence_counts.get('source_evidence', 0)}"
            ),
            "required_fix": "retain and reason-code every organic result row under a frozen classifier version",
        },
        {
            "severity": "moderate",
            "issue": "full_provider_response_not_frozen",
            "evidence": "legacy JSONL stores selected organic fields, not the complete provider response and response hash",
            "required_fix": "freeze the complete raw response and SHA-256 before flattening organic rows",
        },
        {
            "severity": "moderate",
            "issue": "candidate_url_and_evidence_page_exclusions_are_conflated",
            "evidence": "legacy host exclusion is used for candidate URLs but does not express evidence-page eligibility independently",
            "required_fix": "separate candidate coverage URL type from source-page fetch eligibility",
        },
        {
            "severity": "moderate",
            "issue": "quoted_nickname_query_failure",
            "evidence": f"{nickname_rows} of 480 frozen names contain quoted nicknames that the old exact-phrase sanitizer combines with legal names",
            "required_fix": "use one legal-name/nickname OR group in the single approved query",
        },
        {
            "severity": "moderate",
            "issue": "query_requires_campaign_and_website",
            "evidence": "legacy primary query includes both ungrouped terms, narrowing source-page recall",
            "required_fix": "use (campaign OR website) while retaining candidate, mayor, city, state, and year",
        },
        {
            "severity": "moderate",
            "issue": "query_identity_text_contains_mojibake",
            "evidence": f"{mojibake_rows} of 480 frozen candidate names contain recoverable encoding damage",
            "required_fix": "preserve the raw name, deterministically repair query text, flag the repair, and fail if damage remains",
        },
        {
            "severity": "moderate",
            "issue": "exact_url_case_not_fully_preserved",
            "evidence": f"{len(case_examples)} case-only URL change(s) observed between raw-result reconstruction and stored queue",
            "required_fix": "preserve raw URL and exact normalized URL separately; never lowercase path/query",
        },
    ]
    return {
        "audit_version": CONTRACT_VERSION,
        "scope": "offline audit of frozen Serper/search pilot for the approved 480-case one-query wave",
        "network_calls": 0,
        "input_hashes": {
            str(path.relative_to(repo_root)): file_sha256(path)
            for path in (manifest_path, results_path, queue_path, expansion_path, unified_path, sample_path)
        },
        "observed_counts": {
            "pilot_query_manifest_rows": len(manifest),
            "pilot_candidate_years": int(manifest["candidate_year_id"].nunique()),
            "pilot_search_result_records": len(records),
            "pilot_missing_terminal_query_rows": len(missing_attempts),
            "pilot_unexpected_query_rows": len(unexpected_attempts),
            "pilot_raw_organic_results": raw_result_count,
            "pilot_results_with_stored_classification": stored_classification_count,
            "pilot_stored_source_evidence_results": int(
                stored_evidence_counts.get("source_evidence", 0)
            ),
            "pilot_current_rescored_source_evidence_results": int(
                rescored_evidence_counts.get("source_evidence", 0)
            ),
            "pilot_review_queue_rows": len(queue),
            "pilot_pre_dedup_current_claim_rows": len(flattened),
            "pilot_post_dedup_current_claim_rows": len(rebuilt_queue),
            "pilot_independent_claims_collapsed": collapsed_claims,
            "pilot_eligible_source_page_keys": len(eligible_source),
            "pilot_expanded_source_page_keys": len(expanded_source),
            "pilot_missing_source_page_attempts": missing_expansion,
            "pilot_missing_source_page_attempts_with_temporal_conflict": (
                missing_expansion_temporal_conflicts
            ),
            "pilot_unified_evidence_rows": len(unified),
            "frozen_480_quoted_nickname_rows": nickname_rows,
            "frozen_480_mojibake_name_rows": mojibake_rows,
            "frozen_480_missing_election_month_rows": missing_month_rows,
            "exact_url_case_loss_rows": len(case_examples),
        },
        "missing_query_attempts": [
            {"candidate_year_id": key[0], "query_variant": key[1]} for key in missing_attempts
        ],
        "exact_url_case_loss_examples": case_examples,
        "filter_audits": [raw_to_queue_drop, source_expansion_drop],
        "findings": findings,
        "required_wave_invariants": {
            "query_attempts": EXPECTED_SAMPLE_ROWS,
            "one_query_per_candidate": True,
            "query_template_version": QUERY_TEMPLATE_VERSION,
            "query_attempt_id_inputs": [
                "sample_sha256",
                "candidate_year_id",
                "candidate_contest_id",
                "exact_query",
                "query_template_version",
                "provider",
                "provider_gl",
                "provider_hl",
                "requested_result_count",
            ],
            "provider": SEARCH_PROVIDER,
            "provider_gl": SEARCH_GL,
            "provider_hl": SEARCH_HL,
            "requested_result_count": SEARCH_RESULT_COUNT,
            "max_selected_source_pages_per_candidate": MAX_SOURCE_PAGES_PER_CANDIDATE,
            "raw_results_retained": True,
            "result_classification_required": True,
            "independent_url_claims_retained": True,
            "request_error_retryable": True,
            "filters_over_20_percent_require_exact_reason_counts": True,
        },
    }


def write_json(payload: Mapping[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prior = subparsers.add_parser("prior", help="audit the frozen prior search pilot")
    prior.add_argument(
        "--output",
        type=Path,
        default=Path(
            "quality_reports/mayoral_url_discovery_2026-08-25/"
            "serper_one_query_wave_contract/prior_pipeline_audit.json"
        ),
    )
    wave = subparsers.add_parser("wave", help="audit saved outputs from the 480-case wave")
    wave.add_argument(
        "--sample",
        type=Path,
        default=Path(
            "data/municipal_discovery/screening_2010_2021/"
            "candidate_screening_sample_2010_2021.csv"
        ),
    )
    wave.add_argument(
        "--query-manifest",
        type=Path,
        default=Path(
            "data/municipal_discovery/screening_2010_2021/"
            "search_source_expansion/query_manifest.csv"
        ),
    )
    wave.add_argument(
        "--request-ledger",
        type=Path,
        default=Path(
            "data/municipal_discovery/screening_2010_2021/"
            "search_source_expansion/request_ledger.csv"
        ),
    )
    wave.add_argument(
        "--raw-responses",
        type=Path,
        default=Path(
            "data/municipal_discovery/screening_2010_2021/"
            "search_source_expansion/raw_responses.jsonl"
        ),
    )
    wave.add_argument(
        "--result-queue",
        type=Path,
        default=Path(
            "data/municipal_discovery/screening_2010_2021/"
            "search_source_expansion/ranked_result_page_queue.csv"
        ),
    )
    wave.add_argument(
        "--url-leads",
        type=Path,
        default=Path(
            "data/municipal_discovery/screening_2010_2021/"
            "search_source_expansion/url_leads.csv"
        ),
    )
    wave.add_argument(
        "--output",
        type=Path,
        default=Path(
            "quality_reports/mayoral_url_discovery_2026-08-25/"
            "serper_one_query_wave_contract/wave_output_audit.json"
        ),
    )
    args = parser.parse_args()
    if args.command == "prior":
        payload = audit_prior_pipeline(REPO_ROOT)
    else:
        observed_hash = file_sha256(args.sample)
        if observed_hash != EXPECTED_SAMPLE_SHA256:
            raise SearchWaveContractError(
                f"Frozen 480-case sample hash changed: {observed_hash}"
            )
        payload = audit_wave_outputs(
            pd.read_csv(args.sample, low_memory=False),
            observed_hash,
            pd.read_csv(args.query_manifest, low_memory=False),
            pd.read_csv(args.request_ledger, low_memory=False),
            pd.read_csv(args.result_queue, low_memory=False),
            pd.read_csv(args.url_leads, low_memory=False),
            _read_jsonl(args.raw_responses),
        )
    write_json(payload, args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
