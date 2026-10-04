#!/usr/bin/env python3
"""Extract auditable URL leads from the bounded OpenAI web-search pilot."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
from pathlib import Path
from typing import Mapping

import pandas as pd

from scripts.extract_municipal_mayoral_urls import (
    classify_url,
    normalize_exact_url,
    registrable_domain,
    site_root_url,
)
from scripts.run_mayoral_search_source_expansion_2010_2021 import URL_LEAD_COLUMNS


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/openai_search_residual_pilot/production"
)
DEFAULT_SCREEN = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/candidate_screening_sample_2010_2021.csv"
)
DEFAULT_SERPER_LEADS = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/search_source_expansion/url_leads.csv"
)
CONTRACT_VERSION = "openai-web-search-residual-leads-v1"
WRONG_OFFICE_PATTERN = re.compile(
    r"(?:^|[^a-z])(congress|senate|senator|governor|assembly|statehouse|state-house)(?:[^a-z]|$)",
    re.IGNORECASE,
)
KNOWN_NON_CANDIDATE_DOMAINS = {
    "ballotready.org", "ballotpedia.org", "justfacts.votesmart.org", "lwv.org",
    "ourcampaigns.com", "vote411.org", "votesmart.org", "wikipedia.org",
}

OUTPUT_COLUMNS = [
    "assistant_output_id", "attempt_id", "candidate_year_id", "parse_status",
    "candidate_label", "campaign_url_count", "evidence_url_count", "notes",
    "output_text_sha256", "output_text",
]
CLAIM_COLUMNS = [
    "url_claim_id", "assistant_output_id", "attempt_id", "candidate_year_id",
    "candidate_contest_id", "contest", "full_name", "geo_name", "state_abb",
    "year", "month", "period", "claim_kind", "claim_position", "raw_url",
    "exact_normalized_url", "site_root_url", "registrable_domain", "url_type",
    "item_metadata_json", "wrong_office_flag", "wrong_office_reason",
    "known_non_candidate_domain", "candidate_site_exclusion_reason",
    "eligible_candidate_site_lead", "in_serper_candidate_domain",
    "evidence_grade", "review_status", "acceptance", "parser_version",
]
TARGET_COLUMNS = [
    "target_id", "assistant_output_id", "attempt_id", "candidate_year_id", "candidate_contest_id", "contest",
    "full_name", "geo_name", "state_abb", "year", "month", "period",
    "registrable_domain", "site_root_url", "primary_exact_url", "url_type",
    "campaign_claim_count", "campaign_claim_ids_json", "campaign_urls_json",
    "evidence_urls_json", "assistant_notes", "in_serper_candidate_domain",
    "candidate_attribution_status", "temporal_validation_status",
    "evidence_grade", "review_status", "acceptance", "parser_version",
]


class OpenAILeadExtractionError(RuntimeError):
    pass


def _text(value: object) -> str:
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return ""
    return str(value).strip()


def _stable_id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(_text(value) for value in values)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False)
    temporary.replace(path)


def _atomic_json(value: Mapping[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(value, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(text)
    temporary.replace(path)


def _message_text(response: Mapping[str, object]) -> str:
    texts: list[str] = []
    for item in response.get("output", []):
        if isinstance(item, Mapping) and item.get("type") == "message":
            for content in item.get("content", []):
                if isinstance(content, Mapping) and content.get("type") == "output_text":
                    texts.append(_text(content.get("text")))
    return "\n".join(texts).strip()


def _parse_json_output(text: str) -> dict[str, object]:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I | re.S)
    value = json.loads(cleaned)
    if not isinstance(value, dict):
        raise OpenAILeadExtractionError("assistant output is not a JSON object")
    for field in ("campaign_urls", "evidence_urls"):
        if not isinstance(value.get(field, []), list):
            raise OpenAILeadExtractionError(f"{field} is not a list")
    return value


def _url_item(item: object) -> tuple[str, str]:
    if isinstance(item, str):
        return item.strip(), "{}"
    if isinstance(item, Mapping):
        raw_url = _text(item.get("url"))
        metadata = {str(key): value for key, value in item.items() if key != "url"}
        return raw_url, json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "", json.dumps({"unparsed_value": item}, ensure_ascii=False, sort_keys=True, default=str)


def _wrong_office(exact_url: str) -> tuple[bool, str]:
    domain = registrable_domain(exact_url)
    compact_domain = re.sub(r"[^a-z]", "", domain.casefold())
    for marker, office in (
        ("forcongress", "congress"),
        ("forsenate", "senate"),
        ("forsenator", "senator"),
        ("forgovernor", "governor"),
        ("forassembly", "assembly"),
        ("forstatehouse", "statehouse"),
    ):
        if marker in compact_domain:
            return True, f"wrong_office_url_token:{office}"
    token_text = re.sub(r"[^a-z]+", "-", f"{domain} {exact_url}".casefold())
    match = WRONG_OFFICE_PATTERN.search(token_text.replace("-", " "))
    return (True, f"wrong_office_url_token:{match.group(1).casefold()}") if match else (False, "")


def extract_leads(
    raw_wrappers: list[dict[str, object]],
    manifest: pd.DataFrame,
    screen: pd.DataFrame,
    serper_leads: pd.DataFrame,
    *,
    expected_rows: int = 100,
    contract_version: str = CONTRACT_VERSION,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    if len(raw_wrappers) != expected_rows or len(manifest) != expected_rows:
        raise OpenAILeadExtractionError(
            f"input must contain exactly {expected_rows} raw responses and manifest rows"
        )
    if manifest["attempt_id"].duplicated().any() or screen["candidate_year_id"].duplicated().any():
        raise OpenAILeadExtractionError("manifest or screen keys are duplicated")
    raw_by_attempt = {_text(row.get("attempt_id")): row for row in raw_wrappers}
    if set(raw_by_attempt) != set(manifest["attempt_id"].map(_text)):
        raise OpenAILeadExtractionError("raw response and manifest attempt keys differ")
    context = screen.set_index("candidate_year_id", drop=False)
    serper_keys = set(
        zip(
            serper_leads.get("candidate_year_id", pd.Series(dtype=str)).map(_text),
            serper_leads.get("registrable_domain", pd.Series(dtype=str)).map(_text),
        )
    )
    output_rows: list[dict[str, object]] = []
    claim_rows: list[dict[str, object]] = []
    parsed_by_candidate: dict[str, dict[str, object]] = {}
    for manifest_row in manifest.sort_values("sample_order").to_dict("records"):
        attempt_id = _text(manifest_row["attempt_id"])
        candidate_id = _text(manifest_row["candidate_year_id"])
        wrapper = raw_by_attempt[attempt_id]
        raw_response_text = _text(wrapper.get("raw_response_text"))
        parse_status = "parsed"
        parse_error = ""
        try:
            response = json.loads(raw_response_text)
            if not isinstance(response, Mapping):
                raise OpenAILeadExtractionError("provider response is not a JSON object")
            text = _message_text(response)
            parsed = _parse_json_output(text)
        except (json.JSONDecodeError, OpenAILeadExtractionError) as error:
            response = {}
            text = ""
            parsed = {"campaign_urls": [], "evidence_urls": []}
            parse_status = "parse_error"
            parse_error = f"{type(error).__name__}: {error}"[:1000]
        parsed_by_candidate[candidate_id] = parsed
        output_id = _stable_id("openai_assistant_output", attempt_id, hashlib.sha256(text.encode()).hexdigest())
        output_rows.append({
            "assistant_output_id": output_id,
            "attempt_id": attempt_id,
            "candidate_year_id": candidate_id,
            "parse_status": parse_status,
            "candidate_label": _text(parsed.get("candidate")),
            "campaign_url_count": len(parsed.get("campaign_urls", [])),
            "evidence_url_count": len(parsed.get("evidence_urls", [])),
            "notes": _text(parsed.get("notes")) if parse_status == "parsed" else parse_error,
            "output_text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "output_text": text,
        })
        if candidate_id not in context.index:
            raise OpenAILeadExtractionError(f"candidate absent from screen: {candidate_id}")
        candidate = context.loc[candidate_id]
        for claim_kind in ("campaign_url", "evidence_url"):
            items = parsed.get(f"{claim_kind}s", [])
            for position, item in enumerate(items, start=1):
                raw_url, item_metadata = _url_item(item)
                exact = normalize_exact_url(raw_url)
                wrong, wrong_reason = _wrong_office(exact) if exact else (False, "")
                url_type = classify_url(exact) if exact else "invalid_url"
                domain = registrable_domain(exact) if exact else ""
                eligible_type = url_type in {"custom_domain", "candidate_subdomain", "shared_host_path"}
                known_non_candidate = domain in KNOWN_NON_CANDIDATE_DOMAINS
                if wrong:
                    exclusion_reason = wrong_reason
                elif known_non_candidate:
                    exclusion_reason = "known_election_or_profile_domain"
                elif not exact:
                    exclusion_reason = "invalid_url"
                elif not eligible_type:
                    exclusion_reason = f"ineligible_url_type:{url_type}"
                elif claim_kind != "campaign_url":
                    exclusion_reason = "evidence_url_not_candidate_site_claim"
                else:
                    exclusion_reason = ""
                eligible = (
                    claim_kind == "campaign_url" and bool(exact) and eligible_type
                    and not wrong and not known_non_candidate
                )
                claim_rows.append({
                    "url_claim_id": _stable_id("openai_url_claim", attempt_id, claim_kind, position, raw_url),
                    "assistant_output_id": output_id,
                    "attempt_id": attempt_id,
                    "candidate_year_id": candidate_id,
                    "candidate_contest_id": candidate["candidate_contest_id"],
                    "contest": candidate.get("contest", ""),
                    "full_name": candidate["full_name"],
                    "geo_name": candidate["geo_name"],
                    "state_abb": candidate["state_abb"],
                    "year": candidate["year"],
                    "month": candidate["month"],
                    "period": candidate["period"],
                    "claim_kind": claim_kind,
                    "claim_position": position,
                    "raw_url": raw_url,
                    "exact_normalized_url": exact,
                    "site_root_url": site_root_url(exact) if exact else "",
                    "registrable_domain": domain,
                    "url_type": url_type,
                    "item_metadata_json": item_metadata,
                    "wrong_office_flag": wrong,
                    "wrong_office_reason": wrong_reason,
                    "known_non_candidate_domain": known_non_candidate,
                    "candidate_site_exclusion_reason": exclusion_reason,
                    "eligible_candidate_site_lead": eligible,
                    "in_serper_candidate_domain": (candidate_id, domain) in serper_keys,
                    "evidence_grade": "D",
                    "review_status": "pending",
                    "acceptance": "",
                    "parser_version": contract_version,
                })
    outputs = pd.DataFrame(output_rows).reindex(columns=OUTPUT_COLUMNS)
    claims = pd.DataFrame(claim_rows).reindex(columns=CLAIM_COLUMNS)
    if outputs["assistant_output_id"].duplicated().any() or claims["url_claim_id"].duplicated().any():
        raise OpenAILeadExtractionError("output or URL claim IDs are duplicated")
    eligible = claims.loc[claims["eligible_candidate_site_lead"]].copy()
    target_rows: list[dict[str, object]] = []
    for (candidate_id, domain), group in eligible.groupby(["candidate_year_id", "registrable_domain"], sort=True):
        first = group.iloc[0]
        parsed = parsed_by_candidate[_text(candidate_id)]
        evidence_urls = []
        for item in parsed.get("evidence_urls", []):
            raw_url, _ = _url_item(item)
            if raw_url:
                evidence_urls.append(raw_url)
        exact_urls = list(dict.fromkeys(group["exact_normalized_url"].map(_text)))
        claim_ids = list(group["url_claim_id"].map(_text))
        target_rows.append({
            "target_id": _stable_id("openai_site_target", candidate_id, domain),
            "assistant_output_id": first["assistant_output_id"],
            "attempt_id": first["attempt_id"],
            "candidate_year_id": candidate_id,
            "candidate_contest_id": first["candidate_contest_id"],
            "contest": first["contest"],
            "full_name": first["full_name"],
            "geo_name": first["geo_name"],
            "state_abb": first["state_abb"],
            "year": first["year"],
            "month": first["month"],
            "period": first["period"],
            "registrable_domain": domain,
            "site_root_url": site_root_url(exact_urls[0]),
            "primary_exact_url": exact_urls[0],
            "url_type": first["url_type"],
            "campaign_claim_count": len(group),
            "campaign_claim_ids_json": json.dumps(claim_ids, separators=(",", ":")),
            "campaign_urls_json": json.dumps(exact_urls, ensure_ascii=False, separators=(",", ":")),
            "evidence_urls_json": json.dumps(list(dict.fromkeys(evidence_urls)), ensure_ascii=False, separators=(",", ":")),
            "assistant_notes": _text(parsed.get("notes")),
            "in_serper_candidate_domain": bool(group["in_serper_candidate_domain"].any()),
            "candidate_attribution_status": "model_explicit_candidate_url_pending_independent_validation",
            "temporal_validation_status": "not_attempted",
            "evidence_grade": "D",
            "review_status": "pending",
            "acceptance": "",
            "parser_version": contract_version,
        })
    targets = pd.DataFrame(target_rows).reindex(columns=TARGET_COLUMNS)
    audit = {
        "contract_version": contract_version,
        "raw_response_rows": len(raw_wrappers),
        "assistant_outputs": len(outputs),
        "assistant_outputs_parsed": int(outputs["parse_status"].eq("parsed").sum()),
        "assistant_outputs_parse_error": int(outputs["parse_status"].eq("parse_error").sum()),
        "campaign_url_claims": int(claims["claim_kind"].eq("campaign_url").sum()),
        "campaign_url_candidates": int(claims.loc[claims["claim_kind"].eq("campaign_url"), "candidate_year_id"].nunique()),
        "evidence_url_claims": int(claims["claim_kind"].eq("evidence_url").sum()),
        "evidence_url_candidates": int(claims.loc[claims["claim_kind"].eq("evidence_url"), "candidate_year_id"].nunique()),
        "invalid_url_claims": int(claims["exact_normalized_url"].eq("").sum()),
        "wrong_office_campaign_claims": int(((claims["claim_kind"] == "campaign_url") & claims["wrong_office_flag"]).sum()),
        "known_non_candidate_campaign_claims": int(
            ((claims["claim_kind"] == "campaign_url") & claims["known_non_candidate_domain"]).sum()
        ),
        "eligible_campaign_claims": len(eligible),
        "candidate_domain_targets": len(targets),
        "target_candidates": int(targets["candidate_year_id"].nunique()) if len(targets) else 0,
        "serper_candidate_domain_overlap": int(targets["in_serper_candidate_domain"].sum()) if len(targets) else 0,
        "strict_urls": 0,
        "interpretation": "All extracted URLs are grade-D leads pending independent identity and temporal validation.",
    }
    return outputs, claims, targets, audit


def validation_leads_from_targets(
    targets: pd.DataFrame,
    *,
    contract_version: str = CONTRACT_VERSION,
) -> pd.DataFrame:
    """Translate target-level leads into the existing temporal-validator schema."""
    rows: list[dict[str, object]] = []
    for target in targets.to_dict("records"):
        candidate_id = _text(target["candidate_year_id"])
        rows.append({
            "contract_version": contract_version,
            "url_claim_id": target["target_id"],
            "url_entity_id": _stable_id("openai_url_entity", candidate_id, target["primary_exact_url"]),
            "result_claim_id": target["assistant_output_id"],
            "query_attempt_id": target["attempt_id"],
            "page_link_id": "",
            "page_id": "",
            "source_page_url": "",
            "discovery_candidate_year_id": candidate_id,
            "discovery_candidate_contest_id": target["candidate_contest_id"],
            "candidate_year_id": candidate_id,
            "candidate_contest_id": target["candidate_contest_id"],
            "contest": target["contest"],
            "full_name": target["full_name"],
            "geo_name": target["geo_name"],
            "state_abb": target["state_abb"],
            "year": target["year"],
            "month": target["month"],
            "in_candidate_screen": True,
            "raw_url": target["primary_exact_url"],
            "exact_normalized_url": target["primary_exact_url"],
            "site_root_url": target["site_root_url"],
            "registrable_domain": target["registrable_domain"],
            "url_type": target["url_type"],
            "candidate_attribution_status": "identity_supported",
            "candidate_attribution_reason": "openai_structured_output_explicitly_attributes_campaign_url_to_manifest_candidate",
            "matched_candidate_ids_json": json.dumps([candidate_id], separators=(",", ":")),
            "source_locator": "openai_responses_output:campaign_urls",
            "extraction_method": "openai_model_explicit_campaign_url",
            "evidence_text": _text(target["assistant_notes"])[:3000],
            "evidence_grade": "D",
            "strict_coverage_eligible": False,
            "review_status": "pending",
            "acceptance": "",
            "parser_version": contract_version,
        })
    result = pd.DataFrame(rows).reindex(columns=URL_LEAD_COLUMNS)
    if result["url_claim_id"].duplicated().any():
        raise OpenAILeadExtractionError("validation lead IDs are duplicated")
    return result


def build(
    input_dir: Path,
    screen_path: Path,
    serper_path: Path,
    *,
    expected_rows: int = 100,
    contract_version: str = CONTRACT_VERSION,
) -> dict[str, object]:
    wrappers = [json.loads(line) for line in (input_dir / "raw_responses.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    manifest = pd.read_csv(input_dir / "candidate_manifest.csv", keep_default_na=False)
    screen = pd.read_csv(screen_path, keep_default_na=False, low_memory=False)
    serper = pd.read_csv(serper_path, keep_default_na=False, low_memory=False)
    outputs, claims, targets, audit = extract_leads(
        wrappers,
        manifest,
        screen,
        serper,
        expected_rows=expected_rows,
        contract_version=contract_version,
    )
    validation_leads = validation_leads_from_targets(
        targets, contract_version=contract_version
    )
    output_dir = input_dir / "extracted"
    _atomic_csv(outputs, output_dir / "assistant_outputs.csv")
    _atomic_csv(claims, output_dir / "url_claims.csv")
    _atomic_csv(targets, output_dir / "candidate_site_targets.csv")
    _atomic_csv(validation_leads, output_dir / "validation_leads.csv")
    _atomic_csv(pd.DataFrame(columns=["page_link_id", "anchor_text"]), output_dir / "empty_page_links.csv")
    audit["validation_lead_rows"] = len(validation_leads)
    _atomic_json(audit, output_dir / "audit.json")
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--screen", type=Path, default=DEFAULT_SCREEN)
    parser.add_argument("--serper-leads", type=Path, default=DEFAULT_SERPER_LEADS)
    parser.add_argument("--expected-rows", type=int, default=100)
    parser.add_argument("--contract-version", default=CONTRACT_VERSION)
    args = parser.parse_args()
    print(json.dumps(
        build(
            args.input_dir,
            args.screen,
            args.serper_leads,
            expected_rows=args.expected_rows,
            contract_version=args.contract_version,
        ),
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
