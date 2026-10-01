import json

import pandas as pd
import pytest

from scripts.reconcile_mayoral_discovery import reconcile, run, url_identity


def fixture_manifest(tmp_path, claims, reviews=None, validation=None):
    roster = pd.DataFrame([
        {"candidate_year_id": "1__2013", "year": "2013", "month": "5", "contest": "city_2013_5", "candidate_contest_id": "city_2013_5__1", "full_name": "Ann Able"},
        {"candidate_year_id": "2__2013", "year": "2013", "month": "5", "contest": "city_2013_5", "candidate_contest_id": "city_2013_5__2", "full_name": "Bob Baker"},
        {"candidate_year_id": "3__2005", "year": "2005", "month": "4", "contest": "city_2005_4", "candidate_contest_id": "city_2005_4__3", "full_name": "Cal Cole"},
    ])
    roster.to_csv(tmp_path / "roster.csv", index=False)
    inputs = []
    for name, rows, role, priority in [("claims", claims, "claims", 0), ("reviews", reviews, "review", 100), ("validation", validation, "validation", 0)]:
        if rows is not None:
            pd.DataFrame(rows).to_csv(tmp_path / f"{name}.csv", index=False)
            inputs.append({"path": f"{name}.csv", "role": role, "priority": priority, "source_id": name})
    return {"roster": "roster.csv", "inputs": inputs}


def claim(candidate="1__2013", url="https://www.AnnAble.com/About?View=Full", **kwargs):
    return {"claim_id": "claim1", "candidate_year_id": candidate, "raw_url": url, "exact_normalized_url": url, "url_type": "custom_domain", **kwargs}


def test_preserves_paths_case_claims_and_full_universe(tmp_path):
    wrapped = "https://web.archive.org/web/20130501000000/https://www.AnnAble.com/About?View=Full"
    rows = [claim(url=wrapped, source_profile_url="https://news.test/one"), claim(claim_id="independent", source_profile_url="https://news.test/two")]
    result = reconcile(fixture_manifest(tmp_path, rows), tmp_path)
    assert len(result["candidate_roster"]) == 3
    assert result["audit"]["active_candidate_years"] == 2
    assert len(result["combined_claims"]) == 2
    urls = result["candidate_urls"]
    assert len(urls) == 1
    assert urls.iloc[0].exact_normalized_url == "https://www.AnnAble.com/About?View=Full"
    assert urls.iloc[0].site_root_url == "https://www.AnnAble.com/"
    assert urls.iloc[0].month == "5"
    assert urls.iloc[0].source_claim_count == 2
    assert result["combined_claims"].raw_urls_json.str.contains("web.archive.org", regex=False).any()
    assert result["unresolved_candidates"].candidate_year_id.tolist() == ["2__2013"]


def test_rejection_suppresses_unreviewed_alias_but_other_candidate_survives(tmp_path):
    rows = [claim(url="https://www.annable.com/"), claim(candidate="2__2013", url="https://annable.com/", claim_id="other_candidate")]
    review = [claim(url="http://annable.com/", claim_id="review", review_status="rejected", acceptance="False")]
    result = reconcile(fixture_manifest(tmp_path, rows, review), tmp_path)
    assert result["candidate_urls"].candidate_year_id.tolist() == ["2__2013"]
    assert (result["combined_claims"].disposition == "human_rejected_url").sum() == 2
    assert url_identity("https://www.annable.com/Path") != url_identity("https://annable.com/path")


def test_conflicting_reviews_remain_for_review(tmp_path):
    rows = [claim(review_status="accepted", acceptance=True)]
    reviews = [claim(claim_id="review2", review_status="rejected", acceptance=False)]
    result = reconcile(fixture_manifest(tmp_path, rows, reviews), tmp_path)
    assert result["combined_claims"].review_conflict.all()
    assert result["combined_claims"].review_required.all()
    assert result["candidate_urls"].empty
    assert "human_accepted" in result["combined_claims"].review_decision.tolist()


def test_archive_metadata_counts_without_replay_download(tmp_path):
    row = claim()
    validation = [claim(nearest_capture_available=True, within_headline_window=True, capture_fetch_status="request_error", capture_content_sha256="", error="timeout")]
    result = reconcile(fixture_manifest(tmp_path, [row], validation=validation), tmp_path)
    saved = result["candidate_urls"].iloc[0]
    assert saved.archive_capture_available == "true"
    assert saved.archive_capture_within_headline_window == "true"
    assert saved.downloaded_content_available == "unknown"
    assert len(result["unresolved_candidates"]) == 1
    archive_records = json.loads(result["combined_claims"].iloc[0].archive_observation_refs_json)
    assert archive_records[0]["error"] == "timeout"


def test_sampled_audit_is_distinct_from_human_approval(tmp_path):
    row = claim(batch_disposition="accepted_under_sampled_audit_rule", strict_under_batch_audit_rule=True, evidence_grade="C", temporal_match="year_only")
    result = reconcile(fixture_manifest(tmp_path, [row]), tmp_path)
    saved = result["candidate_urls"].iloc[0]
    assert saved.sampled_audit_accepted
    assert not saved.human_accepted
    assert saved.historical_url_identified


def test_identity_uncertainty_excluded_types_and_temporal_leads_are_retained(tmp_path):
    rows = [claim(candidate_attribution_status="match_unresolved"), claim(candidate="2__2013", claim_id="social", url_type="social_profile"), claim(candidate="2__2013", claim_id="date", temporal_conflict=True)]
    result = reconcile(fixture_manifest(tmp_path, rows), tmp_path)
    assert len(result["combined_claims"]) == 3
    assert result["audit"]["claim_dispositions"] == {"identity_unresolved": 1, "excluded_url_type": 1, "candidate_url_lead": 1}
    assert result["candidate_urls"].iloc[0].review_required
    assert not result["candidate_urls"].iloc[0].historical_url_identified


def test_unknown_keys_retained_and_rebuild_removes_stale_rows(tmp_path):
    manifest = fixture_manifest(tmp_path, [claim(), claim(candidate="unknown", claim_id="unknown")])
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    out = tmp_path / "output"
    audit = run(manifest_path, out, tmp_path)
    assert audit["unknown_claim_candidate_keys"] == ["unknown"]
    assert len(pd.read_csv(out / "candidate_urls.csv")) == 1
    pd.DataFrame([claim(url="https://new.test/", claim_id="new")]).to_csv(tmp_path / "claims.csv", index=False)
    run(manifest_path, out, tmp_path)
    urls = pd.read_csv(out / "candidate_urls.csv")
    assert urls.exact_normalized_url.tolist() == ["https://new.test/"]
    assert len(json.loads((out / "input_manifest.json").read_text())["inputs"]) == 2


def test_duplicate_roster_and_manifest_fail(tmp_path):
    manifest = fixture_manifest(tmp_path, [claim()])
    manifest["inputs"].append(dict(manifest["inputs"][0]))
    with pytest.raises(ValueError, match="Repeated manifest"):
        reconcile(manifest, tmp_path)
    manifest["inputs"].pop()
    roster = pd.read_csv(tmp_path / "roster.csv")
    pd.concat([roster, roster.iloc[:1]]).to_csv(tmp_path / "roster.csv", index=False)
    with pytest.raises(ValueError, match="one nonempty row"):
        reconcile(manifest, tmp_path)


def test_prior_weak_ownership_remains_review_lead(tmp_path):
    manifest = fixture_manifest(tmp_path, [claim()])
    pd.DataFrame([{"url_claim_id": "claim1", "eligibility_reason": "ownership_signal_too_weak_for_archive_spend"}]).to_csv(tmp_path / "filters.csv", index=False)
    manifest["inputs"].append({"path": "filters.csv", "role": "claim_filters", "source_id": "ownership_decisions"})
    result = reconcile(manifest, tmp_path)
    assert result["candidate_urls"].empty
    assert result["combined_claims"].iloc[0].disposition == "ownership_unresolved"
    assert result["combined_claims"].iloc[0].review_required
    # A later direct human decision still controls this exact candidate URL.
    pd.DataFrame([claim(review_status="accepted")]).to_csv(tmp_path / "claims.csv", index=False)
    result = reconcile(manifest, tmp_path)
    assert len(result["candidate_urls"]) == 1


def test_model_context_remains_distinct_from_verified_source(tmp_path):
    rows = [claim(assistant_output_id="model1", evidence_grade="D"), claim(claim_id="source", assistant_output_id="model1", claim_kind="evidence_url", url="https://source.test/2013")]
    manifest = fixture_manifest(tmp_path, rows)
    pd.DataFrame([{"assistant_output_id": "model1", "notes": "The model reports a campaign URL."}]).to_csv(tmp_path / "context.csv", index=False)
    manifest["inputs"].append({"path": "context.csv", "role": "claim_contexts", "source_id": "model_context"})
    result = reconcile(manifest, tmp_path)
    row = result["combined_claims"].loc[lambda x: x.eligible_url_lead].iloc[0]
    assert row.evidence_text == "The model reports a campaign URL."
    assert row.source_profile_url == ""
    assert json.loads(row.model_cited_source_urls_json) == ["https://source.test/2013"]
    assert not row.historical_url_identified
    assert not row.human_accepted
