#!/usr/bin/env python3
"""Build a compact, deterministic audit of new mayoral URL leads."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
from math import sqrt
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlsplit

csv.field_size_limit(sys.maxsize)

VERSION = "mayoral-new-url-lead-audit-v1"
CHANGE_TYPES = (
    "new_candidate_url_lead",
    "additional_site_for_known_candidate",
    "alternative_exact_url_for_known_site",
)
CAMPAIGN_TERMS = {"campaign", "elect", "election", "for mayor", "mayor", "vote"}
OTHER_OFFICE_TERMS = {"congress", "governor", "senate", "sheriff", "school board", "secretary of state"}
ARTICLE_PATH_TERMS = {"article", "news", "politics", "story", "stories", "election-results"}
KNOWN_NONCAMPAIGN_DOMAINS = {
    "ballotpedia.org",
    "facebook.com",
    "indystar.com",
    "linkedin.com",
    "medium.com",
    "nytimes.com",
    "twitter.com",
    "wikipedia.org",
    "youtube.com",
}


def _open_csv(path: str):
    if path == "-":
        return io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", newline="")
    return Path(path).open(newline="", encoding="utf-8")


def _json_list(value: str) -> list[str]:
    parsed = json.loads(value or "[]")
    if not isinstance(parsed, list) or any(not isinstance(item, str) for item in parsed):
        raise ValueError("Expected a JSON list of strings")
    return parsed


def _truth(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _tokens(value: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", (value or "").lower()))


def _domain(url: str) -> str:
    return (urlsplit(url).hostname or "").lower().removeprefix("www.")


def _known_noncampaign(domain: str) -> bool:
    return any(domain == item or domain.endswith("." + item) for item in KNOWN_NONCAMPAIGN_DOMAINS)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _unique_join(values) -> str:
    return " || ".join(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))


def _year(value: str) -> int | None:
    match = re.search(r"(?:19|20)\d{2}", value or "")
    return int(match.group()) if match else None


def _structural_fields(lead: dict, claims: list[dict], exact_candidates: dict, domain_candidates: dict) -> dict:
    full_name = lead["full_name"].lower()
    name_tokens = [token for token in _tokens(full_name) if len(token) >= 3]
    last_name = name_tokens[-1] if name_tokens else ""
    city_tokens = [token for token in _tokens(lead["geo_name"]) if len(token) >= 4]
    url = lead["exact_normalized_url"]
    split = urlsplit(url)
    target_text = " ".join((split.netloc, split.path, split.query)).lower()
    evidence_text = _unique_join(
        value
        for claim in claims
        for value in (
            claim.get("anchor_text", ""),
            claim.get("surrounding_context", ""),
            claim.get("heading_context", ""),
            claim.get("preceding_text", ""),
            claim.get("following_text", ""),
            claim.get("source_title", ""),
            claim.get("attribution_reason", ""),
        )
    ).lower()
    source_urls = [claim.get("source_url", "") for claim in claims if claim.get("source_url")]
    source_years = [_year(claim.get("source_publication_date", "") or claim.get("source_date", "")) for claim in claims]
    source_years = [year for year in source_years if year is not None]
    election_year = int(float(lead["year"]))
    domain = _domain(url)
    path_parts = [part for part in split.path.lower().split("/") if part]
    flags = []
    candidate_in_target = bool(last_name and last_name in target_text)
    candidate_in_evidence = bool(name_tokens and all(token in evidence_text for token in name_tokens))
    campaign_in_target = any(term.replace(" ", "") in re.sub(r"[^a-z0-9]", "", target_text) for term in CAMPAIGN_TERMS)
    campaign_in_evidence = any(term in evidence_text for term in CAMPAIGN_TERMS)
    city_in_evidence = any(token in evidence_text for token in city_tokens)
    other_office = any(term in target_text or term in evidence_text for term in OTHER_OFFICE_TERMS)
    article_shaped = len(path_parts) >= 3 and any(term in path_parts or term in split.path.lower() for term in ARTICLE_PATH_TERMS)
    exact_reuse = len(exact_candidates[url]) > 1
    domain_reuse = len(domain_candidates[domain]) > 1
    source_same_url = any(url.rstrip("/").lower() == source.rstrip("/").lower() for source in source_urls)
    source_far = bool(source_years) and min(abs(year - election_year) for year in source_years) > 1
    checks = (
        ("known_noncampaign_domain", _known_noncampaign(domain)),
        ("article_shaped_target", article_shaped),
        ("other_office_language", other_office),
        ("exact_url_reused_across_candidates", exact_reuse),
        ("source_equals_target", source_same_url),
        ("source_date_outside_one_year", source_far),
        ("candidate_absent_from_target_and_evidence", not candidate_in_target and not candidate_in_evidence),
        ("campaign_absent_from_target_and_evidence", not campaign_in_target and not campaign_in_evidence),
    )
    flags.extend(name for name, present in checks if present)
    high = {"known_noncampaign_domain", "article_shaped_target", "other_office_language", "exact_url_reused_across_candidates"}
    risk = "high" if high.intersection(flags) else "medium" if flags else "low"
    return {
        "audit_version": VERSION,
        "candidate_name_in_target": candidate_in_target,
        "candidate_name_in_evidence": candidate_in_evidence,
        "campaign_language_in_target": campaign_in_target,
        "campaign_language_in_evidence": campaign_in_evidence,
        "city_in_evidence": city_in_evidence,
        "known_noncampaign_domain": _known_noncampaign(domain),
        "article_shaped_target": article_shaped,
        "other_office_language": other_office,
        "exact_url_candidate_count": len(exact_candidates[url]),
        "domain_candidate_count": len(domain_candidates[domain]),
        "source_equals_target": source_same_url,
        "source_years_json": json.dumps(sorted(set(source_years))),
        "source_date_outside_one_year": source_far,
        "structural_risk": risk,
        "structural_flags": ";".join(flags),
        "evidence_claim_count": len(claims),
        "evidence_anchor_text": _unique_join(claim.get("anchor_text", "") for claim in claims),
        "evidence_context": _unique_join(claim.get("surrounding_context", "") for claim in claims),
        "evidence_source_titles": _unique_join(claim.get("source_title", "") for claim in claims),
        "evidence_source_urls": _unique_join(source_urls),
        "evidence_methods": _unique_join(claim.get("extraction_method", "") for claim in claims),
        "evidence_attribution_reasons": _unique_join(claim.get("attribution_reason", "") for claim in claims),
    }


def build_audit(new_urls_path: Path, source_claims_path: str, output_dir: Path, sample_per_type: int = 20) -> dict:
    with new_urls_path.open(newline="", encoding="utf-8") as handle:
        leads = list(csv.DictReader(handle))
    keys = [(row["candidate_year_id"], row["exact_normalized_url"]) for row in leads]
    if not leads or len(keys) != len(set(keys)):
        raise ValueError("New URL rows must have unique candidate-year and exact-URL keys")
    claim_to_lead = {}
    for row in leads:
        for claim_id in _json_list(row["source_expansion_claim_ids_json"]):
            if claim_id in claim_to_lead:
                raise ValueError(f"Source claim belongs to multiple lead rows: {claim_id}")
            claim_to_lead[claim_id] = row["candidate_url_id"]
    selected_claims = defaultdict(list)
    seen_claims = Counter()
    with _open_csv(source_claims_path) as handle:
        for row in csv.DictReader(handle):
            claim_id = row.get("claim_id", "")
            if claim_id in claim_to_lead:
                selected_claims[claim_to_lead[claim_id]].append(row)
                seen_claims[claim_id] += 1
    missing = sorted(set(claim_to_lead) - set(seen_claims))
    duplicates = sorted(claim_id for claim_id, count in seen_claims.items() if count != 1)
    if missing or duplicates:
        raise ValueError(f"Claim linkage failed: missing={len(missing)}, nonunique={len(duplicates)}")
    exact_candidates = defaultdict(set)
    domain_candidates = defaultdict(set)
    for row in leads:
        exact_candidates[row["exact_normalized_url"]].add(row["candidate_year_id"])
        domain_candidates[_domain(row["exact_normalized_url"])].add(row["candidate_year_id"])
    evidence_rows = []
    for lead in leads:
        claims = selected_claims[lead["candidate_url_id"]]
        evidence_rows.append({**lead, **_structural_fields(lead, claims, exact_candidates, domain_candidates)})
    by_type = defaultdict(list)
    for row in evidence_rows:
        by_type[row["summary_change_type"]].append(row)
    unexpected = sorted(set(by_type) - set(CHANGE_TYPES))
    if unexpected:
        raise ValueError(f"Unexpected change types: {unexpected}")
    sample = []
    for change_type in CHANGE_TYPES:
        ranked = sorted(by_type[change_type], key=lambda row: hashlib.sha256(row["candidate_url_id"].encode()).hexdigest())
        if len(ranked) < sample_per_type:
            raise ValueError(f"Too few rows for {change_type}: {len(ranked)}")
        sample.extend(ranked[:sample_per_type])
    output_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = output_dir / "lead_evidence.csv"
    sample_path = output_dir / "audit_sample.csv"
    evidence_fields = list(dict.fromkeys(key for row in evidence_rows for key in row))
    decision_fields = ["audit_decision", "audit_evidence_grade", "audit_reason", "audit_checked_source", "audit_checked_archive"]
    with evidence_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=evidence_fields)
        writer.writeheader()
        writer.writerows(evidence_rows)
    with sample_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[*evidence_fields, *decision_fields])
        writer.writeheader()
        writer.writerows(sample)
    audit = {
        "version": VERSION,
        "input": {"path": str(new_urls_path), "rows": len(leads), "sha256": _sha256(new_urls_path)},
        "unique_lead_keys": len(set(keys)),
        "expected_claim_ids": len(claim_to_lead),
        "matched_claim_ids": len(seen_claims),
        "matched_claim_rows": sum(seen_claims.values()),
        "change_type_counts": dict(Counter(row["summary_change_type"] for row in evidence_rows)),
        "risk_counts": dict(Counter(row["structural_risk"] for row in evidence_rows)),
        "flag_counts": dict(Counter(flag for row in evidence_rows for flag in row["structural_flags"].split(";") if flag)),
        "sample": {
            "method": "SHA-256 rank within change type",
            "rows": len(sample),
            "per_change_type": sample_per_type,
            "sha256": _sha256(sample_path),
        },
        "outputs": {
            "lead_evidence.csv": {"rows": len(evidence_rows), "sha256": _sha256(evidence_path)},
            "audit_sample.csv": {"rows": len(sample), "sha256": _sha256(sample_path)},
        },
    }
    (output_dir / "audit_build.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    return audit


def summarize_decisions(sample_path: Path, decisions_path: Path, build_path: Path, output_path: Path) -> dict:
    with sample_path.open(newline="", encoding="utf-8") as handle:
        sample = list(csv.DictReader(handle))
    with decisions_path.open(newline="", encoding="utf-8") as handle:
        decisions = list(csv.DictReader(handle))
    sample_by_id = {row["candidate_url_id"]: row for row in sample}
    decision_by_id = {row["candidate_url_id"]: row for row in decisions}
    if len(sample_by_id) != len(sample) or len(decision_by_id) != len(decisions):
        raise ValueError("Sample and decision IDs must be unique")
    if set(sample_by_id) != set(decision_by_id):
        raise ValueError("Decision IDs must agree exactly with the frozen sample")
    build = json.loads(build_path.read_text())
    allowed = {"accept", "reject", "unresolved", "corrected_url_needed"}
    if any(row["audit_decision"] not in allowed for row in decisions):
        raise ValueError("Unknown audit decision")
    joined = [{**sample_by_id[candidate_url_id], **decision_by_id[candidate_url_id]} for candidate_url_id in sample_by_id]
    strata = {}
    overall = 0.0
    variance = 0.0
    total_population = sum(build["change_type_counts"].values())
    for change_type in CHANGE_TYPES:
        rows = [row for row in joined if row["summary_change_type"] == change_type]
        accepted = sum(row["audit_decision"] == "accept" for row in rows)
        recoverable = sum(row["audit_decision"] == "corrected_url_needed" for row in rows)
        population = build["change_type_counts"][change_type]
        n = len(rows)
        p = accepted / n
        weight = population / total_population
        overall += weight * p
        if n > 1:
            variance += weight**2 * p * (1 - p) / (n - 1) * (1 - n / population)
        strata[change_type] = {
            "population_rows": population,
            "sample_rows": n,
            "accepted_exact_urls": accepted,
            "recoverable_with_corrected_url": recoverable,
            "rejected": sum(row["audit_decision"] == "reject" for row in rows),
            "unresolved": sum(row["audit_decision"] == "unresolved" for row in rows),
            "accepted_exact_url_fraction": p,
        }
    se = sqrt(max(variance, 0.0))
    result = {
        "version": VERSION,
        "sample_sha256_before_decisions": build["sample"]["sha256"],
        "decision_file_sha256": _sha256(decisions_path),
        "sample_rows": len(joined),
        "decision_counts": dict(Counter(row["audit_decision"] for row in joined)),
        "evidence_grade_counts": dict(Counter(row["audit_evidence_grade"] for row in joined)),
        "strata": strata,
        "stratified_exact_url_precision": overall,
        "stratified_standard_error": se,
        "stratified_approximate_95_percent_interval": [max(0.0, overall - 1.96 * se), min(1.0, overall + 1.96 * se)],
        "interpretation": "Audit decisions estimate whether the exact proposed URL is a candidate campaign website. corrected_url_needed is excluded from exact-URL precision. Decisions are agent audit judgments, not human approvals or release changes.",
    }
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--new-urls", type=Path, required=True)
    parser.add_argument("--source-claims", required=True, help="CSV path or - for standard input")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sample-per-type", type=int, default=20)
    parser.add_argument("--decisions", type=Path)
    parser.add_argument("--decision-summary", type=Path)
    args = parser.parse_args()
    result = build_audit(args.new_urls, args.source_claims, args.output_dir, args.sample_per_type)
    if args.decisions:
        if not args.decision_summary:
            parser.error("--decision-summary is required with --decisions")
        result = summarize_decisions(args.output_dir / "audit_sample.csv", args.decisions, args.output_dir / "audit_build.json", args.decision_summary)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
