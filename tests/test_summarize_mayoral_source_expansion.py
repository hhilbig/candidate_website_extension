import csv
import hashlib
import json
from pathlib import Path

import pytest

from scripts.summarize_mayoral_source_expansion import summarize


def write_csv(path, rows, fields=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = fields or list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def freeze(run):
    roster = []
    for cid, name in [("1__2013", "Ann Able"), ("2__2013", "Bob Baker"), ("3__2005", "Cal Cole")]:
        roster.append({"candidate_year_id": cid, "candidate_contest_id": cid + "_contest", "year": cid[-4:], "month": "5", "contest": "contest" + cid[-4:], "full_name": name, "active_2010_2021": cid != "3__2005", "has_url_lead": cid == "1__2013", "candidate_url_count": int(cid == "1__2013"), "historical_url_identified": cid == "1__2013", "human_accepted": cid == "1__2013", "archive_capture_available": "true" if cid == "1__2013" else "unknown", "discovery_status": "historical_url_identified" if cid == "1__2013" else "unresolved"})
    write_csv(run / "roster/candidate_roster.csv", roster)
    url = {"candidate_url_id": "old_url", "candidate_year_id": "1__2013", "exact_normalized_url": "https://AnnAble.com/", "raw_url": "https://AnnAble.com/", "site_root_url": "https://AnnAble.com/", "registrable_domain": "annable.com", "url_type": "custom_domain", "source_claim_count": "1", "reconciliation_claim_ids_json": '["old_claim"]', "source_ids_json": '["old_source"]', "source_urls_json": '["https://oldnews.org/ann"]', "model_cited_source_urls_json": "[]", "human_accepted": True, "historical_url_identified": True, "strict_exact_url": True, "strict_custom_domain": True, "archive_capture_available": "true", "archive_capture_within_headline_window": "true", "downloaded_content_available": "true", "review_required": False, "sampled_audit_accepted": False}
    write_csv(run / "roster/candidate_urls.csv", [url])
    write_csv(run / "roster/combined_claims.csv", [{**url, "reconciliation_claim_id": "old_claim", "source_id": "old_source", "disposition": "candidate_url_lead", "review_decision": "human_accepted", "eligible_url_lead": True, "evidence_grade": "A"}])
    write_csv(run / "roster/claim_dispositions.csv", [{"candidate_year_id": "1__2013", "exact_normalized_url": "https://AnnAble.com/", "review_decision": "human_accepted", "disposition": "candidate_url_lead"}])


def expanded(claim_id="new_claim", candidate="2__2013", url="https://BobBaker.com/Campaign?View=Full", **extra):
    extra = {"candidate_site_ownership_supported": True, "candidate_site_ownership_reason": "explicit_candidate_website_context", **extra}
    return {"claim_id": claim_id, "candidate_year_id": candidate, "raw_url": url, "exact_normalized_url": url, "url_type": "custom_domain", "registrable_domain": "bobbaker.com", "source_id": "news1", "source_url": "https://news.org/election2013", "source_content_sha256": "a" * 64, "source_content_path": "raw/source.gz", "surrounding_context": "Bob Baker lists his campaign website.", "source_locator": "anchor:3", "attribution_status": "identity_supported", "temporal_status": "not_assessed", **extra}


def test_new_candidates_alternative_urls_and_extra_source_claims(tmp_path):
    freeze(tmp_path)
    rows = [expanded(), expanded(claim_id="independent", source_id="news2"), expanded(claim_id="known", candidate="1__2013", url="https://AnnAble.com/"), expanded(claim_id="path", candidate="1__2013", url="https://AnnAble.com/About"), expanded(claim_id="newsite", candidate="1__2013", url="https://electann.com/")]
    write_csv(tmp_path / "expanded_url_claims.csv", rows)
    audit = summarize(tmp_path)
    assert audit["new_active_candidate_years_with_source_attributed_leads"] == 1
    assert audit["active_candidate_years_with_url_leads"] == 2
    assert audit["new_active_exact_url_rows"] == 3
    assert audit["new_active_sites_for_known_candidates"] == 1
    assert audit["new_active_exact_url_change_counts"] == {"additional_site_for_known_candidate": 1, "alternative_exact_url_for_known_site": 1, "new_candidate_url_lead": 1}
    urls = read_csv(tmp_path / "combined/candidate_urls.csv")
    assert len(urls) == 4
    bob = next(row for row in urls if row["candidate_year_id"] == "2__2013")
    assert bob["exact_normalized_url"] == "https://BobBaker.com/Campaign?View=Full"
    assert bob["source_expansion_claim_count"] == "2"
    assert json.loads(bob["source_expansion_claim_ids_json"]) == ["independent", "new_claim"]
    assert bob["historical_url_identified"] == "False"
    assert len(read_csv(tmp_path / "combined/source_claims.csv")) == 6
    assert audit["baseline_historical_url_candidate_years_active"] == 1


def test_legacy_claim_without_ownership_cannot_add_coverage(tmp_path):
    freeze(tmp_path)
    row = expanded()
    del row["candidate_site_ownership_supported"]
    del row["candidate_site_ownership_reason"]
    write_csv(tmp_path / "expanded_url_claims.csv", [row])
    audit = summarize(tmp_path)
    assert audit["new_active_exact_url_rows"] == 0
    assert read_csv(tmp_path / "combined/review_claims.csv")[0]["summary_disposition"] == "identity_or_ownership_unresolved"


def test_repair_verification_freezes_sample_and_checks_original_decisions(tmp_path):
    from scripts.audit_mayoral_source_leads import build_audit
    from scripts.verify_mayoral_ownership_repair import verify
    freeze(tmp_path)
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded()])
    summarize(tmp_path)
    prior = tmp_path / "prior"
    build_audit(tmp_path / "combined/new_candidate_urls.csv", str(tmp_path / "expanded_url_claims.csv"), prior, sample_per_type=0)
    row = read_csv(prior / "lead_evidence.csv")[0]
    write_csv(prior / "audit_decisions.csv", [{"candidate_url_id": row["candidate_url_id"], "audit_decision": "accept"}])
    result = verify(tmp_path, prior, tmp_path / "repair_audit", rebuild=False)
    assert result["original_sample_mismatches"] == []
    assert result["fresh_sample_rows"] == 0
    assert result["acquisition_tables_before"] == result["acquisition_tables_after"]


def test_uncertain_ownership_stays_in_review_after_reference_classification(tmp_path):
    freeze(tmp_path)
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded(
        attribution_status="reference_lead", url_type="source_reference",
        candidate_site_ownership_supported=False,
        candidate_site_ownership_reason="undated_personal_site_requires_campaign_review",
    )])
    summarize(tmp_path)
    assert read_csv(tmp_path / "combined/new_candidate_urls.csv") == []
    assert read_csv(tmp_path / "combined/review_claims.csv")[0]["summary_disposition"] == "identity_or_ownership_unresolved"


def test_rejections_persist_across_scheme_and_www_aliases(tmp_path):
    freeze(tmp_path)
    write_csv(tmp_path / "roster/claim_dispositions.csv", [{"candidate_year_id": "2__2013", "exact_normalized_url": "http://bobbaker.com/", "review_decision": "human_rejected", "disposition": "human_rejected_url"}])
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded(url="https://www.BobBaker.com/")])
    audit = summarize(tmp_path)
    assert audit["new_active_candidate_years_with_source_attributed_leads"] == 0
    review = read_csv(tmp_path / "combined/review_claims.csv")
    assert review[0]["summary_disposition"] == "prior_human_rejection"
    assert review[0]["raw_url"] == "https://www.BobBaker.com/"


def test_date_conflict_keeps_review_and_cannot_count_as_addition(tmp_path):
    freeze(tmp_path)
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded(temporal_status="date_conflict_review")])
    audit = summarize(tmp_path)
    assert audit["new_active_exact_url_rows"] == 0
    assert audit["expanded_claim_dispositions"] == {"date_conflict_review": 1}
    assert read_csv(tmp_path / "combined/review_claims.csv")[0]["surrounding_context"] == "Bob Baker lists his campaign website."


def test_wrong_office_is_review_even_when_source_year_matches(tmp_path):
    freeze(tmp_path)
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded(wrong_office_flag=True, wrong_office_reason="gubernatorial_campaign", source_publication_date="2013-05-01", source_date="2013-05-01", temporal_status="not_assessed")])
    audit = summarize(tmp_path)
    assert audit["new_active_exact_url_rows"] == 0
    assert audit["new_active_candidate_years_with_source_attributed_leads"] == 0
    assert audit["expanded_claim_dispositions"] == {"wrong_office_review": 1}
    review = read_csv(tmp_path / "combined/review_claims.csv")[0]
    assert review["wrong_office_reason"] == "gubernatorial_campaign"
    assert review["source_date"] == "2013-05-01"


def test_inherited_candidate_flags_never_approve_new_claims(tmp_path):
    freeze(tmp_path)
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded(human_accepted=True, historical_url_identified=True, strict_exact_url=True, strict_custom_domain=True, archive_capture_available=True, downloaded_content_available=True, evidence_grade="A", review_status="accepted")])
    audit = summarize(tmp_path)
    claim = read_csv(tmp_path / "combined/source_claims.csv")[1]
    assert claim["human_accepted"] == "False"
    assert claim["input_candidate_human_accepted"] == "True"
    assert claim["historical_url_identified"] == "False"
    assert claim["strict_exact_url"] == "False"
    assert claim["archive_capture_available"] == "unknown"
    assert claim["downloaded_content_available"] == "unknown"
    assert claim["source_content_sha256"] == "a" * 64
    assert claim["evidence_grade"] == "D"
    assert claim["input_claim_evidence_grade"] == "A"
    assert claim["review_status"] == "pending"
    assert claim["input_claim_review_status"] == "accepted"
    assert audit["new_strict_or_human_approved_claims"] == 0


def test_reference_shape_unknown_and_social_claims_remain_visible(tmp_path):
    freeze(tmp_path)
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded(attribution_status="domain_shape_only"), expanded(claim_id="social", url_type="social_profile"), expanded(claim_id="unknown", candidate="outside"), expanded(claim_id="reference", attribution_status="reference_lead")])
    audit = summarize(tmp_path)
    assert audit["expanded_eligible_claim_rows"] == 0
    assert audit["unknown_expanded_candidate_keys"] == ["outside"]
    assert len(read_csv(tmp_path / "combined/source_claims.csv")) == 5
    assert len(read_csv(tmp_path / "combined/review_claims.csv")) == 3


def test_empty_expansion_and_hashes_and_no_frozen_changes(tmp_path):
    freeze(tmp_path)
    baseline_path = tmp_path / "roster/candidate_roster.csv"
    baseline = baseline_path.read_bytes()
    audit = summarize(tmp_path)
    assert audit["expanded_source_claim_rows"] == 0
    assert audit["active_candidate_years_with_url_leads"] == 1
    assert read_csv(tmp_path / "combined/new_candidate_urls.csv") == []
    assert baseline_path.read_bytes() == baseline
    for name, record in audit["outputs"].items():
        assert hashlib.sha256((tmp_path / "combined" / name).read_bytes()).hexdigest() == record["sha256"]


def test_duplicate_claim_id_fails_before_replacing_outputs(tmp_path):
    freeze(tmp_path)
    summarize(tmp_path)
    old = (tmp_path / "combined/source_claims.csv").read_bytes()
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded(), expanded()])
    with pytest.raises(ValueError, match="Duplicate expanded"):
        summarize(tmp_path)
    assert (tmp_path / "combined/source_claims.csv").read_bytes() == old


def test_separate_missing_source_state_and_rebuild_removes_stale_urls(tmp_path):
    freeze(tmp_path)
    (tmp_path / "audit.json").write_text(json.dumps({"stage": "source_pass_incomplete", "never_attempted_sources": 20, "retryable_sources": 3, "source_status_counts": {"fetched": 5, "request_error": 3}}))
    write_csv(tmp_path / "expanded_url_claims.csv", [expanded()])
    audit = summarize(tmp_path)
    assert audit["source_run_state"]["never_attempted_sources"] == 20
    write_csv(tmp_path / "expanded_url_claims.csv", [], ["claim_id", "candidate_year_id", "exact_normalized_url"])
    audit = summarize(tmp_path)
    assert audit["new_active_exact_url_rows"] == 0
    assert len(read_csv(tmp_path / "combined/candidate_urls.csv")) == 1


def exclusion_file(run, candidate="1__2013", url="https://AnnAble.com/", **extra):
    row = dict(candidate_year_id=candidate, candidate_url_id="old_url", exact_normalized_url=url,
               attribution_exclusion_version="v1", attribution_exclusion_reason="Explicit other candidate",
               attribution_exclusion_review_file="review.json", attribution_exclusion_reviewer="reviewer",
               attribution_exclusion_model="gpt-5.6-sol", attribution_exclusion_capture_sha256="a" * 64,
               attribution_exclusion_capture_timestamp="20130501000000",
               attribution_exclusion_evidence_path="remote/capture.gz", **extra)
    path = run / "exclusions.csv"
    write_csv(path, [row])
    return path


def unaccept_baseline(run):
    for filename in ["candidate_urls.csv", "combined_claims.csv"]:
        path = run / "roster" / filename
        rows = read_csv(path)
        for row in rows:
            row.update(human_accepted=False, sampled_audit_accepted=False)
            if "review_decision" in row:
                row["review_decision"] = "pending"
        write_csv(path, rows)


def test_attribution_exclusion_preserves_claims_blocks_readdition_and_exact_aliases(tmp_path):
    freeze(tmp_path)
    unaccept_baseline(tmp_path)
    write_csv(tmp_path / "expanded_url_claims.csv", [
        expanded(candidate="1__2013", url="https://AnnAble.com/"),
        expanded(claim_id="alias", candidate="1__2013", url="http://www.AnnAble.com/"),
        expanded(claim_id="other", candidate="2__2013", url="https://AnnAble.com/"),
    ])
    exclusions = exclusion_file(tmp_path)
    output = tmp_path / "rebuilt"
    audit = summarize(tmp_path, exclusions, output)
    urls = read_csv(output / "candidate_urls.csv")
    assert {(r["candidate_year_id"], r["exact_normalized_url"]) for r in urls} == {
        ("1__2013", "http://www.AnnAble.com/"), ("2__2013", "https://AnnAble.com/")}
    claims = read_csv(output / "source_claims.csv")
    assert len(claims) == 4
    rejected = [r for r in claims if r["summary_disposition"] == "ai_candidate_attribution_rejected"]
    assert len(rejected) == 2
    assert all(r["summary_eligible_url_lead"] == "False" and r["summary_review_required"] == "True" for r in rejected)
    assert rejected[0]["eligible_url_lead"] == "True"
    assert rejected[0]["attribution_exclusion_reason"] == "Explicit other candidate"
    assert len(read_csv(output / "review_claims.csv")) == 2
    assert audit["attribution_exclusions"]["excluded_baseline_url_rows"] == 1
    assert audit["attribution_exclusions"]["excluded_expanded_claim_rows"] == 1
    assert audit["inputs"]["attribution_exclusions.csv"]["sha256"] == hashlib.sha256(exclusions.read_bytes()).hexdigest()
    assert not (tmp_path / "combined").exists()


def test_baseline_exclusion_recomputes_coverage(tmp_path):
    freeze(tmp_path)
    unaccept_baseline(tmp_path)
    audit = summarize(tmp_path, exclusion_file(tmp_path))
    assert audit["active_candidate_years_with_url_leads"] == 0
    assert read_csv(tmp_path / "combined/candidate_urls.csv") == []
    roster = read_csv(tmp_path / "combined/candidate_roster.csv")[0]
    assert roster["candidate_url_count"] == "0"
    assert roster["candidate_site_count"] == "0"
    assert roster["discovery_status"] == "unresolved"
    assert roster["attribution_exclusion_input_discovery_status"] == "historical_url_identified"
    unaffected = read_csv(tmp_path / "combined/candidate_roster.csv")[1]
    assert unaffected["discovery_status"] == "unresolved"
    assert unaffected["attribution_exclusion_input_discovery_status"] == ""


@pytest.mark.parametrize("field,value", [("attribution_exclusion_reason", ""), ("attribution_exclusion_capture_sha256", "bad"), ("attribution_exclusion_capture_timestamp", "bad"), ("attribution_exclusion_evidence_path", "")])
def test_attribution_exclusion_invalid_evidence_fails(tmp_path, field, value):
    freeze(tmp_path)
    path = exclusion_file(tmp_path)
    rows = read_csv(path)
    rows[0][field] = value
    write_csv(path, rows)
    with pytest.raises(ValueError, match="evidence"):
        summarize(tmp_path, path)


def test_attribution_exclusion_unknown_and_acceptance_conflicts_fail(tmp_path):
    freeze(tmp_path)
    with pytest.raises(ValueError, match="Unknown"):
        summarize(tmp_path, exclusion_file(tmp_path, candidate="outside"))
    with pytest.raises(ValueError, match="acceptance"):
        summarize(tmp_path, exclusion_file(tmp_path))
