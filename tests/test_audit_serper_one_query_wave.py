import hashlib
from pathlib import Path

import pandas as pd
import pytest

from scripts.audit_serper_one_query_wave import (
    CONTRACT_VERSION,
    EXPECTED_SAMPLE_SHA256,
    QUERY_MANIFEST_COLUMNS,
    SearchWaveContractError,
    _validate_request_ledger,
    _validate_url_leads,
    attach_result_claim_ids,
    attach_url_claim_ids,
    audit_filter_stage,
    audit_prior_pipeline,
    audit_wave_outputs,
    build_expected_query_manifest,
    build_one_query,
    build_url_entity_index,
    candidate_identity_status,
    classify_candidate_url,
    classify_result_page,
    exact_normalized_url,
    file_sha256,
    name_aliases,
    rank_result_pages,
    repair_query_identity_text,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/"
    "candidate_screening_sample_2010_2021.csv"
)


def candidate(**overrides):
    row = {
        "candidate_year_id": "123__2013",
        "candidate_contest_id": "contest__123",
        "contest": "contest",
        "full_name": "Mike Duggan",
        "geo_name": "Detroit",
        "state_abb": "MI",
        "year": 2013,
        "month": 11,
        "sample_order": 1,
    }
    row.update(overrides)
    return row


def empty_result_queue():
    return pd.DataFrame(
        columns=[
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
        ]
    )


def empty_url_leads():
    return pd.DataFrame(
        columns=[
            "url_claim_id",
            "url_entity_id",
            "result_claim_id",
            "query_attempt_id",
            "candidate_year_id",
            "raw_url",
            "exact_normalized_url",
            "url_type",
            "identity_attribution_status",
            "eligible_for_temporal_validation",
            "extraction_method",
        ]
    )


def test_query_expands_quoted_nickname_without_mistaking_apostrophes():
    row = candidate(full_name="andrew 'andy' mihelich", geo_name="Elgin", state_abb="IL")
    assert name_aliases(row["full_name"]) == ["andrew mihelich", "andy mihelich"]
    assert build_one_query(row) == (
        '("andrew mihelich" OR "andy mihelich") mayor "Elgin" IL 2013 '
        "(campaign OR website)"
    )
    assert name_aliases("charlotte o'hara") == ["charlotte o'hara"]
    assert name_aliases("jacq'ue 'j' miller") == ["jacq'ue miller", "j miller"]
    assert name_aliases("michael c. taylor") == ["michael c. taylor", "michael taylor"]


def test_query_name_repairs_are_explicit_and_deterministic():
    assert repair_query_identity_text("michael sÃ¡nchez") == (
        "michael sánchez",
        "mojibake_repaired",
    )
    assert repair_query_identity_text("michael c.Â\xa0taylor") == (
        "michael c. taylor",
        "mojibake_repaired",
    )
    assert name_aliases("janifer â\x80\x9cjanâ\x80\x9d kulmann") == [
        "janifer kulmann",
        "jan kulmann",
    ]


def test_frozen_sample_builds_exactly_one_deterministic_query_per_candidate():
    assert file_sha256(SAMPLE_PATH) == EXPECTED_SAMPLE_SHA256
    sample = pd.read_csv(SAMPLE_PATH, low_memory=False)
    manifest = build_expected_query_manifest(sample, EXPECTED_SAMPLE_SHA256)
    assert manifest.columns.tolist() == QUERY_MANIFEST_COLUMNS
    assert len(manifest) == 480
    assert manifest["candidate_year_id"].nunique() == 480
    assert manifest["query_attempt_id"].nunique() == 480
    assert manifest["search_contract_version"].eq(CONTRACT_VERSION).all()
    assert manifest["provider"].eq("serper").all()
    assert manifest["provider_gl"].eq("us").all()
    assert manifest["provider_hl"].eq("en").all()
    assert manifest["requested_result_count"].eq(10).all()
    assert manifest["query_name_repair_status"].eq("mojibake_repaired").sum() == 3
    assert manifest["month"].isna().sum() == 1
    nickname = manifest.loc[manifest["full_name"].eq("andrew 'andy' mihelich")].iloc[0]
    assert '"andrew mihelich" OR "andy mihelich"' in nickname["query"]
    shuffled = build_expected_query_manifest(
        sample.sample(frac=1, random_state=9), EXPECTED_SAMPLE_SHA256
    )
    assert set(manifest["query_attempt_id"]) == set(shuffled["query_attempt_id"])


def test_candidate_attribution_requires_first_or_nickname_plus_surname():
    assert candidate_identity_status(
        "William 'Bill' Kahn", "Bill Kahn for Minneapolis mayor"
    )[0] == "identity_supported"
    assert candidate_identity_status(
        "William 'Bill' Kahn", "Kahn enters the Minneapolis mayoral race"
    ) == ("identity_unresolved", "surname_only")
    assert candidate_identity_status(
        "William 'Bill' Kahn", "Alice Kahn enters the Minneapolis mayoral race"
    )[0] == "identity_unresolved"


def test_url_rules_preserve_case_and_separate_candidate_from_evidence_pages():
    row = candidate()
    exact = exact_normalized_url(
        "https://web.archive.org/web/20131001000000/"
        "https://DugganForDetroit.com/Issues?Ref=Election"
    )
    assert exact == "https://DugganForDetroit.com/Issues?Ref=Election"
    direct = classify_candidate_url(
        exact, row, "Mike Duggan for Detroit mayor in 2013"
    )
    assert direct["url_type"] == "custom_domain"
    assert direct["eligible_campaign_lead"]

    profile = classify_candidate_url(
        "https://ballotpedia.org/Mike_Duggan", row, "Mike Duggan Detroit mayor 2013"
    )
    assert profile["url_type"] == "evidence_profile"
    assert not profile["eligible_campaign_lead"]

    social = classify_candidate_url(
        "https://facebook.com/MikeDuggan", row, "Mike Duggan Detroit mayor 2013"
    )
    assert social["url_type"] == "social_profile"
    assert not social["eligible_campaign_lead"]

    fundraiser = classify_candidate_url(
        "https://secure.winred.com/duggan/donate", row, "Mike Duggan mayor"
    )
    assert fundraiser["url_type"] == "fundraising_page"
    assert not fundraiser["eligible_campaign_lead"]


def test_result_page_eligibility_retains_direct_and_date_conflict_rows():
    row = candidate()
    official = classify_result_page(
        {
            "rank": 1,
            "url": "https://detroitmi.gov/elections/2013/candidate-guide.pdf",
            "title": "2013 Detroit mayor candidate guide",
            "snippet": "Mike Duggan campaign website and candidate filing",
        },
        row,
    )
    assert official["result_page_role"] == "credible_source_page"
    assert official["eligible_for_page_fetch"]

    ballotpedia = classify_result_page(
        {
            "rank": 2,
            "url": "https://ballotpedia.org/Mike_Duggan",
            "title": "Mike Duggan",
            "snippet": "Detroit mayoral election 2013 candidate website",
        },
        row,
    )
    assert ballotpedia["result_page_role"] == "credible_source_page"
    assert ballotpedia["candidate_url_type"] == "evidence_profile"

    direct = classify_result_page(
        {
            "rank": 3,
            "url": "https://DugganForDetroit.com/Issues?Ref=Election",
            "title": "Mike Duggan for Mayor",
            "snippet": "Detroit campaign 2013",
        },
        row,
    )
    assert direct["result_page_role"] == "direct_candidate_site"
    assert not direct["eligible_for_page_fetch"]

    conflict = classify_result_page(
        {
            "rank": 4,
            "url": "https://news.example/2017/duggan",
            "title": "Mike Duggan for mayor",
            "snippet": "Detroit reelection campaign in 2017",
        },
        row,
    )
    assert conflict["result_page_role"] == "temporal_conflict_review"
    assert not conflict["eligible_for_page_fetch"]


def test_result_page_ranking_selects_three_without_deleting_credible_overflow():
    rows = []
    for rank in range(1, 5):
        rows.append(
            {
                "candidate_year_id": "123__2013",
                "result_claim_id": f"result-{rank}",
                "organic_rank": rank,
                "raw_result_url": f"https://news{rank}.example/story",
                "result_host": f"news{rank}.example",
                "eligible_for_page_fetch": True,
                "exact_year_context": rank != 1,
                "page_fetch_exclusion_reason": "",
            }
        )
    ranked = rank_result_pages(pd.DataFrame(rows))
    assert len(ranked) == 4
    assert ranked["selected_for_page_fetch"].sum() == 3
    overflow = ranked.loc[~ranked["selected_for_page_fetch"]].iloc[0]
    assert overflow["eligible_for_page_fetch"]
    assert overflow["page_fetch_selection_reason"] == "eligible_beyond_candidate_cap"


def test_result_and_url_claim_ids_preserve_independent_sources_for_same_url():
    results = attach_result_claim_ids(
        pd.DataFrame(
            [
                {
                    "query_attempt_id": "query-1",
                    "organic_rank": 1,
                    "raw_result_url": "https://news-a.example/story",
                    "result_title": "Candidate guide A",
                    "result_snippet": "Mike Duggan website",
                },
                {
                    "query_attempt_id": "query-1",
                    "organic_rank": 2,
                    "raw_result_url": "https://news-b.example/story",
                    "result_title": "Candidate guide B",
                    "result_snippet": "Mike Duggan website",
                },
            ]
        )
    )
    leads = attach_url_claim_ids(
        pd.DataFrame(
            [
                {
                    "result_claim_id": result_claim_id,
                    "query_attempt_id": "query-1",
                    "candidate_year_id": "123__2013",
                    "exact_normalized_url": "https://DugganForDetroit.com/Issues",
                    "extraction_method": "candidate_labeled_anchor",
                }
                for result_claim_id in results["result_claim_id"]
            ]
        )
    )
    assert len(leads) == 2
    assert leads["url_claim_id"].nunique() == 2
    assert leads["url_entity_id"].nunique() == 1
    entities = build_url_entity_index(leads)
    assert entities.iloc[0]["independent_claim_count"] == 2


def test_direct_candidate_result_cannot_stop_before_url_claim_queue():
    queue = pd.DataFrame(
        [
            {
                "result_claim_id": "result-direct",
                "query_attempt_id": "query-1",
                "candidate_year_id": "123__2013",
                "result_page_role": "direct_candidate_site",
                "raw_result_url": "https://DugganForDetroit.com",
            }
        ]
    )
    with pytest.raises(SearchWaveContractError, match="silently dropped"):
        _validate_url_leads(queue, empty_url_leads())


def test_request_ledger_has_exact_keys_and_retry_semantics():
    manifest = pd.DataFrame(
        [
            {"query_attempt_id": "q1", "candidate_year_id": "c1"},
            {"query_attempt_id": "q2", "candidate_year_id": "c2"},
        ]
    )
    ledger = pd.DataFrame(
        [
            {
                "query_attempt_id": "q1",
                "candidate_year_id": "c1",
                "request_status": "found",
                "terminal_for_run": True,
                "retryable": False,
                "complete": True,
                "result_count": 2,
                "error": "",
            },
            {
                "query_attempt_id": "q2",
                "candidate_year_id": "c2",
                "request_status": "request_error",
                "terminal_for_run": True,
                "retryable": True,
                "complete": False,
                "result_count": 0,
                "error": "timeout",
            },
        ]
    )
    audit = _validate_request_ledger(manifest, ledger)
    assert audit["key_agreement"]
    assert audit["retryable_rows"] == 1
    with pytest.raises(SearchWaveContractError, match="key mismatch"):
        _validate_request_ledger(manifest, ledger.iloc[:1])
    with pytest.raises(SearchWaveContractError, match="one current row"):
        _validate_request_ledger(manifest, pd.concat([ledger, ledger.iloc[:1]]))


def test_over_twenty_percent_filter_requires_exact_reason_audit():
    with pytest.raises(SearchWaveContractError, match="without an exact reason audit"):
        audit_filter_stage("silent", 100, 79)
    audited = audit_filter_stage("reasoned", 100, 70, {"not_candidate_related": 30})
    assert audited["removed_fraction"] == pytest.approx(0.30)
    assert audited["explicit_audit_present"]


def test_empty_result_wave_still_requires_exact_480_terminal_attempts():
    sample = pd.read_csv(SAMPLE_PATH, low_memory=False)
    manifest = build_expected_query_manifest(sample, EXPECTED_SAMPLE_SHA256)
    ledger = manifest[["query_attempt_id", "candidate_year_id"]].copy()
    ledger["request_status"] = "no_result"
    ledger["terminal_for_run"] = True
    ledger["retryable"] = False
    ledger["complete"] = True
    ledger["result_count"] = 0
    ledger["error"] = ""
    audit = audit_wave_outputs(
        sample,
        EXPECTED_SAMPLE_SHA256,
        manifest,
        ledger,
        empty_result_queue(),
        empty_url_leads(),
    )
    assert audit["pass"]
    assert audit["query_attempt_rows"] == 480
    assert audit["request_ledger"]["key_agreement"]
    assert audit["ready_for_downstream_fetch"]


def test_acquisition_runner_interface_accepts_exact_480_offline_no_result_records():
    output_dir = (
        REPO_ROOT
        / "data/municipal_discovery/screening_2010_2021/search_source_expansion"
    )
    sample = pd.read_csv(SAMPLE_PATH, low_memory=False)
    manifest = pd.read_csv(output_dir / "query_manifest.csv", low_memory=False)
    ledger_columns = pd.read_csv(
        output_dir / "request_ledger.csv", nrows=0
    ).columns.tolist()
    ledger_rows = []
    raw_records = []
    body = "{}"
    body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    for sequence, row in enumerate(manifest.to_dict("records"), 1):
        ledger_rows.append(
            {
                "contract_version": row["contract_version"],
                "query_attempt_id": row["query_attempt_id"],
                "candidate_year_id": row["candidate_year_id"],
                "request_sequence": sequence,
                "query_sha256": row["query_sha256"],
                "provider": "serper",
                "endpoint": "https://google.serper.dev/search",
                "request_status": "no_result",
                "request_sent": True,
                "terminal_for_run": True,
                "http_status": 200,
                "results_returned": 0,
                "raw_response_sha256": body_hash,
                "error": "",
                "attempted_at": "2026-08-25T00:00:00Z",
                "completed_at": "2026-08-25T00:00:01Z",
                "retryable": False,
                "complete_for_wave": True,
            }
        )
        raw_records.append(
            {
                "contract_version": row["contract_version"],
                "query_attempt_id": row["query_attempt_id"],
                "candidate_year_id": row["candidate_year_id"],
                "request_status": "no_result",
                "raw_response_sha256": body_hash,
                "raw_response_body": body,
                "raw_response": {},
            }
        )
    audit = audit_wave_outputs(
        sample,
        EXPECTED_SAMPLE_SHA256,
        manifest,
        pd.DataFrame(ledger_rows).reindex(columns=ledger_columns),
        pd.read_csv(
            output_dir / "ranked_result_page_queue.csv", nrows=0, low_memory=False
        ),
        pd.read_csv(output_dir / "url_leads.csv", nrows=0, low_memory=False),
        raw_records,
    )
    assert audit["request_ledger"]["rows"] == 480
    assert audit["raw_responses"]["rows"] == 480
    assert audit["raw_responses"]["exact_ledger_key_agreement"]


def test_prior_pipeline_audit_reproduces_known_accounting_losses():
    audit = audit_prior_pipeline(REPO_ROOT)
    counts = audit["observed_counts"]
    assert counts["pilot_query_manifest_rows"] == 240
    assert counts["pilot_missing_terminal_query_rows"] == 5
    assert counts["pilot_raw_organic_results"] == 1487
    assert counts["pilot_review_queue_rows"] == 55
    assert counts["pilot_eligible_source_page_keys"] == 49
    assert counts["pilot_missing_source_page_attempts"] == 16
    assert counts["pilot_independent_claims_collapsed"] == 1
    assert counts["exact_url_case_loss_rows"] == 1
    assert counts["frozen_480_quoted_nickname_rows"] == 32
    assert counts["frozen_480_mojibake_name_rows"] == 3
    assert counts["frozen_480_missing_election_month_rows"] == 1
    assert counts["pilot_stored_source_evidence_results"] == 528
    assert counts["pilot_current_rescored_source_evidence_results"] == 50
    assert all(item["explicit_audit_present"] for item in audit["filter_audits"])
