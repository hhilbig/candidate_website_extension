import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import requests

import scripts.enumerate_archiveit_mayoral_collections as enumerator
from scripts.enumerate_archiveit_mayoral_collections import (
    ATTEMPT_COLUMNS,
    CANDIDATE_COLUMNS,
    PRIORITY_COLUMNS,
    ArchiveItCollectionSourceUnavailable,
    build_logical_queries,
    build_collection_priority,
    enumerate_collection_candidates,
    fetch_collection_catalog,
    load_collection_catalog,
)


FIXTURES = Path(__file__).parent / "fixtures"
FIXED_TIME = "2026-08-24T12:00:00+00:00"


def universe():
    return pd.DataFrame([
        {"geo_name": "los angeles", "state_abb": "CA", "in_discovery_scope": True},
        {"geo_name": "birmingham", "state_abb": "AL", "in_discovery_scope": True},
        {"geo_name": "austin", "state_abb": "TX", "in_discovery_scope": True},
        {"geo_name": "ignored city", "state_abb": "ZZ", "in_discovery_scope": False},
    ])


def catalog():
    return load_collection_catalog(FIXTURES / "archiveit_collection_catalog.json")


def test_query_grid_covers_each_city_terms_and_collection_subjects_once():
    queries = build_logical_queries(universe())
    assert len(queries) == 3 + 2 + 3 + 6
    assert len({query.query_id for query in queries}) == len(queries)
    assert {(query.query_city, query.query_state) for query in queries if query.query_kind == "city"} == {
        ("austin", "TX"), ("birmingham", "AL"), ("los angeles", "CA"),
    }
    assert {query.query_kind for query in queries} >= {
        "city", "mayoral_term", "election_term", "metadata_subject", "collection_topic",
    }


def test_fixture_enumeration_preserves_query_claims_and_filters_deleted_records():
    candidates, attempts = enumerate_collection_candidates(
        universe(), catalog(), retrieved_at=FIXED_TIME,
    )
    assert list(candidates.columns) == CANDIDATE_COLUMNS
    assert list(attempts.columns) == ATTEMPT_COLUMNS
    assert len(attempts) == len(build_logical_queries(universe()))
    assert not attempts.duplicated("query_id").any()
    assert set(attempts.source_status) <= enumerator.TERMINAL_ATTEMPT_STATUSES
    assert set(attempts.source_status) == {"found", "no_result"}
    assert 99998 not in set(candidates.collection_id)
    assert {5903, 16766}.issubset(set(candidates.collection_id))

    ucla = candidates.loc[candidates.collection_id.eq(5903)]
    assert set(ucla.query_kind) >= {"city", "election_term", "collection_topic"}
    assert ucla.collection_candidate_id.nunique() == len(ucla)
    assert all(ucla.collection_access_url.eq("https://archive-it.org/collections/5903"))
    assert any("Los Angeles" in value for value in ucla.source_collection_json)
    assert set(ucla.promotion_status) == {"already_in_seed_adapter"}


def test_mayor_token_does_not_false_match_sotomayor_and_foreign_lead_is_retained():
    candidates, _ = enumerate_collection_candidates(
        universe(), catalog(), retrieved_at=FIXED_TIME,
    )
    mayor_rows = candidates.loc[
        candidates.query_kind.eq("mayoral_term")
        & candidates.query_term.eq("mayor")
    ]
    assert 1552 not in set(mayor_rows.collection_id)
    assert 3163 in set(candidates.collection_id)
    london = candidates.loc[candidates.collection_id.eq(3163)].iloc[0]
    assert london.matched_target_locations == "[]"
    assert london.target_period_hint == "overlaps_2000_2021"


def test_catalog_error_emits_one_terminal_attempt_for_every_query():
    candidates, attempts = enumerate_collection_candidates(
        universe(), [], retrieved_at=FIXED_TIME,
        catalog_error=("request_error", "HTTP 503"),
    )
    assert candidates.empty
    assert len(attempts) == len(build_logical_queries(universe()))
    assert set(attempts.source_status) == {"request_error"}
    assert set(attempts.error) == {"HTTP 503"}
    assert not attempts.duplicated("query_id").any()


def test_priority_output_has_exact_collection_agreement_and_lossless_query_provenance():
    candidates, _ = enumerate_collection_candidates(
        universe(), catalog(), retrieved_at=FIXED_TIME,
    )
    priority = build_collection_priority(candidates)
    assert list(priority.columns) == PRIORITY_COLUMNS
    assert len(priority) == candidates.collection_id.nunique()
    assert priority.collection_id.nunique() == len(priority)
    assert set(priority.collection_id) == set(candidates.collection_id)

    ucla_claims = candidates.loc[candidates.collection_id.eq(5903)]
    ucla = priority.loc[priority.collection_id.eq(5903)].iloc[0]
    assert ucla.query_claim_count == len(ucla_claims)
    assert set(json.loads(ucla.matched_query_ids)) == set(ucla_claims.query_id)
    assert set(json.loads(ucla.matched_query_kinds)) == set(ucla_claims.query_kind)
    assert set(json.loads(ucla.matched_query_terms)) == set(ucla_claims.query_term)
    assert ucla.priority_score == ucla_claims.priority_score.max()
    assert ucla.collection_access_url == "https://archive-it.org/collections/5903"
    assert ucla.source_collection_json == ucla_claims.source_collection_json.iloc[0]
    assert ucla.known_collection
    assert ucla.collection_novelty == "known_adapter_collection"
    assert ucla.promotion_status == "already_in_seed_adapter"

    new_row = priority.loc[priority.collection_id.eq(9517)].iloc[0]
    assert not new_row.known_collection
    assert new_row.collection_novelty == "new_collection"
    assert new_row.promotion_status == "pending_promotion_review"
    assert new_row.review_status == "pending"


def test_priority_output_rejects_conflicting_collection_stable_provenance():
    candidates, _ = enumerate_collection_candidates(
        universe(), catalog(), retrieved_at=FIXED_TIME,
    )
    ucla_indexes = candidates.index[candidates.collection_id.eq(5903)].tolist()
    assert len(ucla_indexes) > 1
    candidates.loc[ucla_indexes[0], "collection_access_url"] = (
        "https://archive-it.org/collections/not-5903"
    )
    with pytest.raises(ValueError, match="conflicting collection_access_url"):
        build_collection_priority(candidates)


class FakeResponse:
    def __init__(self, status_code, payload=None, json_error=None):
        self.status_code = status_code
        self.payload = payload
        self.json_error = json_error

    def json(self):
        if self.json_error:
            raise self.json_error
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_live_catalog_fetch_retries_transient_errors_and_uses_documented_limit():
    session = FakeSession([
        requests.ConnectionError("temporary"),
        FakeResponse(503),
        FakeResponse(200, [{"id": 1, "name": "Mayoral Election"}]),
    ])
    payload = fetch_collection_catalog(
        attempts=3, delay_seconds=0, session=session,
    )
    assert payload == [{"id": 1, "name": "Mayoral Election"}]
    assert len(session.calls) == 3
    assert session.calls[-1][1]["params"] == {"limit": -1}
    assert "public metadata only" in session.calls[-1][1]["headers"]["User-Agent"]


def test_live_catalog_fetch_classifies_private_or_missing_endpoint_as_unavailable():
    session = FakeSession([FakeResponse(403)])
    with pytest.raises(ArchiveItCollectionSourceUnavailable, match="HTTP 403"):
        fetch_collection_catalog(attempts=3, delay_seconds=0, session=session)
    assert len(session.calls) == 1


def test_cli_fixture_build_writes_nonempty_reviewable_outputs(tmp_path):
    universe_path = tmp_path / "universe.csv"
    universe().to_csv(universe_path, index=False)
    output_dir = tmp_path / "output"
    command = [
        sys.executable,
        "scripts/enumerate_archiveit_mayoral_collections.py",
        "--universe", str(universe_path),
        "--catalog-json", str(FIXTURES / "archiveit_collection_catalog.json"),
        "--output-dir", str(output_dir),
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    summary = json.loads(result.stdout)
    candidates = pd.read_csv(output_dir / "mayoral_archiveit_collection_candidates.csv")
    attempts = pd.read_csv(output_dir / "mayoral_archiveit_collection_query_attempts.csv")
    priority = pd.read_csv(output_dir / "mayoral_archiveit_collection_priority.csv")
    saved_summary = json.loads(
        (output_dir / "mayoral_archiveit_collection_enumeration_summary.json").read_text()
    )
    assert len(candidates) > 0
    assert len(attempts) == len(build_logical_queries(universe()))
    assert len(priority) == candidates.collection_id.nunique()
    assert priority.collection_id.nunique() == len(priority)
    assert set(priority.collection_id) == set(candidates.collection_id)
    assert summary["strict_urls_added"] == 0
    assert summary["deduplicated_priority_rows"] == len(priority)
    assert saved_summary["unique_candidate_collections"] == candidates.collection_id.nunique()


def test_priority_only_cli_reuses_claims_without_catalog_fetch(tmp_path):
    candidates, _ = enumerate_collection_candidates(
        universe(), catalog(), retrieved_at=FIXED_TIME,
    )
    claims_path = tmp_path / "claims.csv"
    candidates.to_csv(claims_path, index=False)
    output_dir = tmp_path / "priority_only"
    command = [
        sys.executable,
        "scripts/enumerate_archiveit_mayoral_collections.py",
        "--candidate-claims", str(claims_path),
        "--output-dir", str(output_dir),
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    summary = json.loads(result.stdout)
    priority = pd.read_csv(output_dir / "mayoral_archiveit_collection_priority.csv")
    assert len(priority) == candidates.collection_id.nunique()
    assert set(priority.collection_id) == set(candidates.collection_id)
    assert summary["priority_built_from_claims"] == str(claims_path)
    assert summary["priority_new_collections"] == len(priority) - 2
    assert not (output_dir / "mayoral_archiveit_collection_query_attempts.csv").exists()
