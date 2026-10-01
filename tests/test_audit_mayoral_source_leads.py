import csv
import json

import pytest

from scripts.audit_mayoral_source_leads import build_audit, summarize_decisions


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fixture_rows(n=2):
    leads, claims = [], []
    for change in ("new_candidate_url_lead", "additional_site_for_known_candidate", "alternative_exact_url_for_known_site"):
        for index in range(n):
            claim = f"{change}_{index}"
            candidate = f"candidate_{change}_{index}"
            leads.append({
                "candidate_year_id": candidate,
                "candidate_url_id": f"url_{change}_{index}",
                "full_name": f"Ann Person{index}",
                "geo_name": "Sample City",
                "year": "2017",
                "exact_normalized_url": f"https://person{index}-{change}.org/",
                "source_expansion_claim_ids_json": json.dumps([claim]),
                "summary_change_type": change,
            })
            claims.append({
                "claim_id": claim,
                "anchor_text": f"Ann Person{index} for Mayor",
                "surrounding_context": f"Ann Person{index} is running for mayor of Sample City.",
                "source_url": "https://news.example/election",
                "source_publication_date": "2017-03-01",
                "extraction_method": "anchor",
            })
    return leads, claims


def test_builds_deterministic_stratified_sample_and_exact_claim_linkage(tmp_path):
    leads, claims = fixture_rows()
    new_urls = tmp_path / "new.csv"
    source_claims = tmp_path / "claims.csv"
    write_csv(new_urls, leads)
    write_csv(source_claims, claims)
    first = build_audit(new_urls, str(source_claims), tmp_path / "one", sample_per_type=1)
    second = build_audit(new_urls, str(source_claims), tmp_path / "two", sample_per_type=1)
    assert first["unique_lead_keys"] == 6
    assert first["matched_claim_ids"] == 6
    assert first["sample"]["rows"] == 3
    assert first["sample"]["sha256"] == second["sample"]["sha256"]


def test_duplicate_lead_key_fails(tmp_path):
    leads, claims = fixture_rows()
    leads.append(dict(leads[0]))
    new_urls = tmp_path / "new.csv"
    source_claims = tmp_path / "claims.csv"
    write_csv(new_urls, leads)
    write_csv(source_claims, claims)
    with pytest.raises(ValueError, match="unique"):
        build_audit(new_urls, str(source_claims), tmp_path / "out", sample_per_type=1)


def test_missing_or_duplicate_claim_fails(tmp_path):
    leads, claims = fixture_rows()
    new_urls = tmp_path / "new.csv"
    source_claims = tmp_path / "claims.csv"
    write_csv(new_urls, leads)
    write_csv(source_claims, claims[:-1])
    with pytest.raises(ValueError, match="Claim linkage failed"):
        build_audit(new_urls, str(source_claims), tmp_path / "missing", sample_per_type=1)
    write_csv(source_claims, [*claims, dict(claims[0])])
    with pytest.raises(ValueError, match="Claim linkage failed"):
        build_audit(new_urls, str(source_claims), tmp_path / "duplicate", sample_per_type=1)


def test_decision_summary_requires_exact_sample_and_weights_strata(tmp_path):
    leads, claims = fixture_rows()
    new_urls = tmp_path / "new.csv"
    source_claims = tmp_path / "claims.csv"
    output = tmp_path / "audit"
    write_csv(new_urls, leads)
    write_csv(source_claims, claims)
    build_audit(new_urls, str(source_claims), output, sample_per_type=1)
    with (output / "audit_sample.csv").open(newline="", encoding="utf-8") as handle:
        sample = list(csv.DictReader(handle))
    decisions = [{"candidate_url_id": row["candidate_url_id"], "audit_decision": "accept", "audit_evidence_grade": "C"} for row in sample]
    decisions_path = tmp_path / "decisions.csv"
    write_csv(decisions_path, decisions)
    result = summarize_decisions(output / "audit_sample.csv", decisions_path, output / "audit_build.json", output / "summary.json")
    assert result["stratified_exact_url_precision"] == 1
    assert result["decision_counts"] == {"accept": 3}
    decisions.pop()
    write_csv(decisions_path, decisions)
    with pytest.raises(ValueError, match="agree exactly"):
        summarize_decisions(output / "audit_sample.csv", decisions_path, output / "audit_build.json", output / "summary.json")
