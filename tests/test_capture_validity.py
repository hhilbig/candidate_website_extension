import copy
import json

import pandas as pd

from scripts.audit_capture_validity import (
    QUEUE_COLUMNS,
    audit_capture_validity,
    audit_report,
    enrich_queue_with_decisions,
    write_audit_outputs,
)


KNOWN_PROBLEMS = {
    "H4OR06013-2024": {
        "candidate": "Denyc Boles",
        "state": "OR",
        "district": 6,
        "office": "house",
        "urlkey": "https://bolesfororegon.com",
        "text": "Cape Town, South Africa. Berita ekonomi Indonesia dan Jakarta.",
    },
    "S4MN00023-2024": {
        "candidate": "Ole' Savior",
        "state": "MN",
        "district": 0,
        "office": "senate",
        "urlkey": "https://olesavior.org",
        "text": "From Ballotpedia. Ballotpedia Candidate Connection and encyclopedia.",
    },
    "H0MI07135-2020": {
        "candidate": "Christopher Bartholomew Smith",
        "state": "MI",
        "district": 7,
        "office": "house",
        "urlkey": "https://chrissmith.house.gov",
        "text": "Chris Smith represents New Jersey's 4th District. New Jersey constituent services.",
    },
    "H2TN92014-2022": {
        "candidate": "Angela M Hughes",
        "state": "TN",
        "district": 92,
        "office": "house",
        "urlkey": "https://angietn92.com",
        "text": "Running for Tennessee House of Representatives District 92.",
    },
    "H4TN57013-2024": {
        "candidate": "Ailina Carona",
        "state": "TN",
        "district": 57,
        "office": "house",
        "urlkey": "https://caronafortn57.com",
        "text": "Ailina Carona is ready to serve as your State Representative for District 57.",
    },
}


def known_problem_fixture():
    crosswalk_rows = []
    raw_rows = []
    for candidate_id, item in KNOWN_PROBLEMS.items():
        year = int(candidate_id.rsplit("-", 1)[1])
        crosswalk_rows.append({
            "candidate_cycle_id": candidate_id,
            "candidate": item["candidate"],
            "state": item["state"],
            "district": item["district"],
            "office": item["office"],
            "year": year,
        })
        # Twenty rows exercise the dominance and low-name-support rules while
        # keeping the fixture small.  Structural Tennessee errors do not depend
        # on the text rule.
        for row_number in range(20):
            raw_rows.append({
                "candidate_cycle_id": candidate_id,
                "state": item["state"],
                "district": item["district"],
                "office": item["office"],
                "year": year,
                "urlkey": item["urlkey"],
                "page_type": "homepage" if row_number == 0 else "other",
                "text_snap_content": item["text"],
            })
    return pd.DataFrame(raw_rows), pd.DataFrame(crosswalk_rows)


def test_known_problem_candidates_are_detected_and_classified():
    raw, crosswalk = known_problem_fixture()
    queue = audit_capture_validity(raw, crosswalk)
    signals = queue.groupby("candidate_cycle_id").signal.apply(set).to_dict()

    assert "foreign_location_content_dominates" in signals["H4OR06013-2024"]
    assert "third_party_content_dominates" in signals["S4MN00023-2024"]
    assert "text_state_mismatch" in signals["H0MI07135-2020"]
    assert "official_domain_geography_mismatch" in signals["H0MI07135-2020"]
    assert "house_district_out_of_range" in signals["H2TN92014-2022"]
    assert "house_district_out_of_range" in signals["H4TN57013-2024"]

    district_errors = queue[queue.signal == "house_district_out_of_range"]
    assert set(district_errors.finding_class) == {"error"}
    content_findings = queue[queue.signal.str.endswith("dominates")]
    assert set(content_findings.finding_class) == {"review_flag"}
    assert set(KNOWN_PROBLEMS) <= set(queue.candidate_cycle_id)
    foreign = queue[queue.signal.eq("foreign_location_content_dominates")].iloc[0]
    assert "Cape Town" in json.loads(foreign.evidence)["evidence_excerpt"]


def test_audit_does_not_mutate_inputs_or_drop_candidates():
    raw, crosswalk = known_problem_fixture()
    raw_before = raw.copy(deep=True)
    crosswalk_before = crosswalk.copy(deep=True)

    queue = audit_capture_validity(raw, crosswalk)

    pd.testing.assert_frame_equal(raw, raw_before)
    pd.testing.assert_frame_equal(crosswalk, crosswalk_before)
    assert set(KNOWN_PROBLEMS) == set(raw.candidate_cycle_id)
    assert "action" not in queue.columns
    assert queue.columns.tolist() == QUEUE_COLUMNS
    assert set(queue.proposed_action) == {"manual_review"}
    assert set(queue.review_status) == {"pending"}


def test_clean_capture_has_no_findings():
    crosswalk = pd.DataFrame([{
        "candidate_cycle_id": "H4CA12123-2024",
        "candidate": "Jane Example",
        "state": "CA",
        "district": 12,
        "office": "house",
        "year": 2024,
    }])
    raw = pd.DataFrame([{
        "candidate_cycle_id": "H4CA12123-2024",
        "state": "CA",
        "district": 12,
        "office": "house",
        "year": 2024,
        "urlkey": "https://janeexample.com",
        "page_type": "homepage",
        "text_snap_content": "Jane Example is running for Congress in California.",
    }])
    queue = audit_capture_validity(raw, crosswalk)
    assert queue.empty
    assert queue.columns.tolist() == QUEUE_COLUMNS


def test_senate_raw_zero_and_crosswalk_missing_are_equivalent():
    crosswalk = pd.DataFrame([{
        "candidate_cycle_id": "S4CA00123-2024",
        "candidate": "Jane Example",
        "state": "CA",
        "district": pd.NA,
        "office": "senate",
        "year": 2024,
    }])
    raw = pd.DataFrame([{
        "candidate_cycle_id": "S4CA00123-2024",
        "state": "CA",
        "district": 0,
        "office": "senate",
        "year": 2024,
        "urlkey": "https://janeexample.com",
        "page_type": "homepage",
        "text_snap_content": "Jane Example is running for the United States Senate in California.",
    }])
    assert audit_capture_validity(raw, crosswalk).empty


def test_cross_file_mismatch_and_unmatched_ids_are_errors():
    crosswalk = pd.DataFrame([{
        "candidate_cycle_id": "H4CA12123-2024",
        "candidate": "Jane Example",
        "state": "CA",
        "district": 12,
        "office": "house",
        "year": 2024,
    }])
    raw = pd.DataFrame([
        {
            "candidate_cycle_id": "H4CA12123-2024",
            "state": "NV",
            "district": 12,
            "office": "house",
            "year": 2024,
            "text_snap_content": "Jane Example",
        },
        {
            "candidate_cycle_id": "S4XX00000-2024",
            "state": "XX",
            "district": 0,
            "office": "senate",
            "year": 2024,
            "text_snap_content": "Unknown Candidate",
        },
    ])
    queue = audit_capture_validity(raw, crosswalk)
    assert {
        "raw_crosswalk_state_mismatch",
        "raw_candidate_missing_crosswalk",
    } <= set(queue.signal)
    assert set(queue.finding_class) == {"error"}


def test_outputs_are_deterministic_and_report_zero_automatic_exclusions(tmp_path):
    raw, crosswalk = known_problem_fixture()
    queue = audit_capture_validity(raw.sample(frac=1, random_state=9), crosswalk)
    queue_again = audit_capture_validity(raw.sample(frac=1, random_state=19), crosswalk)
    pd.testing.assert_frame_equal(queue, queue_again)

    report = audit_report(
        queue,
        raw_row_count=len(raw),
        crosswalk_candidate_count=crosswalk.candidate_cycle_id.nunique(),
    )
    first = tmp_path / "first"
    second = tmp_path / "second"
    queue_path, report_path = write_audit_outputs(queue, report, first)
    queue_path_2, report_path_2 = write_audit_outputs(queue_again, copy.deepcopy(report), second)

    assert queue_path.read_bytes() == queue_path_2.read_bytes()
    assert report_path.read_bytes() == report_path_2.read_bytes()
    saved = json.loads(report_path.read_text())
    assert saved["automatic_exclusions_applied"] == 0
    assert saved["counts_by_finding_class"]["error"] == 2
    assert saved["counts_by_finding_class"]["review_flag"] >= 3


def test_decision_enrichment_marks_approved_rows_without_filtering_or_mutating():
    raw, crosswalk = known_problem_fixture()
    queue = audit_capture_validity(raw, crosswalk)
    queue_before = queue.copy(deep=True)
    decisions = pd.DataFrame([
        {"year": 2024, "cand_id": "H4OR06013", "action": "exclude"},
        {"year": 2024, "cand_id": "S4MN00023", "action": "exclude"},
    ])

    enriched = enrich_queue_with_decisions(queue, decisions)

    pd.testing.assert_frame_equal(queue, queue_before)
    assert len(enriched) == len(queue)
    approved = enriched.candidate_cycle_id.isin({"H4OR06013-2024", "S4MN00023-2024"})
    assert set(enriched.loc[approved, "proposed_action"]) == {"exclude"}
    assert set(enriched.loc[approved, "review_status"]) == {"approved"}
    assert set(enriched.loc[~approved, "proposed_action"]) == {"manual_review"}
    assert set(enriched.loc[~approved, "review_status"]) == {"pending"}


def test_shared_domains_use_one_frozen_www_normalization_rule():
    crosswalk = pd.DataFrame([
        {"candidate_cycle_id": "H4AA00100-2024", "candidate": "Alice One",
         "state": "AA", "district": 1, "office": "house", "year": 2024},
        {"candidate_cycle_id": "H4AA00200-2024", "candidate": "Bob Two",
         "state": "AA", "district": 2, "office": "house", "year": 2024},
    ])
    raw = pd.DataFrame([
        {"candidate_cycle_id": "H4AA00100-2024", "state": "AA", "district": 1,
         "office": "house", "year": 2024, "urlkey": "https://www2.shared.test",
         "page_type": "homepage", "text_snap_content": "Alice One for Congress."},
        {"candidate_cycle_id": "H4AA00200-2024", "state": "AA", "district": 2,
         "office": "house", "year": 2024, "urlkey": "https://shared.test",
         "page_type": "homepage", "text_snap_content": "Bob Two for Congress."},
    ])
    queue = audit_capture_validity(raw, crosswalk)
    shared = queue[queue.signal.eq("shared_year_domain")]
    assert set(shared.candidate_cycle_id) == set(crosswalk.candidate_cycle_id)
    assert all(json.loads(value)["normalized_full_host"] == "shared.test"
               for value in shared.evidence)


def test_runaway_day_and_high_cjk_are_review_flags():
    crosswalk = pd.DataFrame([{
        "candidate_cycle_id": "H4AA00100-2024", "candidate": "Alice One",
        "state": "AA", "district": 1, "office": "house", "year": 2024,
    }])
    text = "政策" * 50
    raw = pd.DataFrame([{
        "candidate_cycle_id": "H4AA00100-2024", "state": "AA", "district": 1,
        "office": "house", "year": 2024, "urlkey": "https://alice.test",
        "date": "20240101120000", "page_type": "other",
        "text_snap_content": text,
    } for _ in range(101)])
    queue = audit_capture_validity(raw, crosswalk)
    assert {"runaway_snapshot_day", "high_cjk_content_dominates"} <= set(queue.signal)
    assert set(queue.loc[queue.signal.isin({"runaway_snapshot_day",
                                            "high_cjk_content_dominates"}),
                         "finding_class"]) == {"review_flag"}
