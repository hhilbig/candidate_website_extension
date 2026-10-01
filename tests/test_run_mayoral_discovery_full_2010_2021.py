from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import run_mayoral_discovery_full_2010_2021 as full


class FakeResponse:
    status_code = 200

    def __init__(self, payload: dict[str, object]):
        self._payload = payload
        self.content = json.dumps(payload).encode("utf-8")
        self.text = self.content.decode("utf-8")

    def json(self) -> dict[str, object]:
        return self._payload


class FakeSession:
    def __init__(self, payload: dict[str, object]):
        self.payload = payload
        self.calls = 0

    def post(self, *args: object, **kwargs: object) -> FakeResponse:
        self.calls += 1
        return FakeResponse(self.payload)


def test_real_production_universe_is_exact_and_excludes_prior_work() -> None:
    unresolved = pd.read_csv(full.DEFAULT_UNRESOLVED, low_memory=False)
    prior = pd.read_csv(full.DEFAULT_PRIOR_SCREEN, low_memory=False)
    claims = pd.read_csv(full.DEFAULT_CLAIMS, low_memory=False)
    universe, audit = full.build_production_universe(unresolved, prior, claims)
    assert len(universe) == 2180
    assert audit["production_by_period"] == {
        "2010_2013": 765,
        "2014_2017": 690,
        "2018_2021": 725,
    }
    assert not set(universe.candidate_year_id) & set(prior.candidate_year_id)
    assert universe.candidate_year_id.is_unique
    assert audit["reduction_rate"] < 0.20


def test_prepare_builds_exact_serper_ceiling(tmp_path: Path) -> None:
    audit = full.prepare(tmp_path)
    manifest = pd.read_csv(tmp_path / "serper/query_manifest.csv", keep_default_na=False)
    assert audit["production_rows"] == 2180
    assert len(manifest) == full.SERPER_REQUEST_CEILING
    assert manifest.query_attempt_id.is_unique
    assert manifest.candidate_year_id.is_unique
    assert manifest["query"].str.endswith("(campaign OR website)").all()


def test_one_serper_request_retains_all_results_and_direct_claim(tmp_path: Path) -> None:
    full.prepare(tmp_path)
    manifest = pd.read_csv(tmp_path / "serper/query_manifest.csv", keep_default_na=False)
    row = manifest.iloc[0]
    compact = manifest.iloc[[0]].copy()
    compact.to_csv(tmp_path / "serper/query_manifest.csv", index=False)
    hostname = "".join(str(row.full_name).lower().split()) + "formayor.org"
    payload = {
        "organic": [
            {
                "position": 1,
                "title": f"{row.full_name} for Mayor",
                "link": f"https://{hostname}/",
                "snippet": f"{row.full_name} campaign for mayor of {row.geo_name} {row.state_abb} {row.year}",
            },
            {
                "position": 2,
                "title": "Election reporting",
                "link": "https://example-news.com/story",
                "snippet": f"{row.full_name} mayor {row.geo_name} {row.year}",
            },
        ]
    }
    session = FakeSession(payload)
    audit = full.run_serper(
        tmp_path, request_budget=1, api_key="test-key", session=session
    )
    queue = pd.read_csv(tmp_path / "serper/ranked_result_page_queue.csv")
    leads = pd.read_csv(tmp_path / "serper/direct_url_leads.csv")
    assert session.calls == 1
    assert audit["organic_result_rows"] == 2
    assert len(queue) == 2
    direct = queue.loc[queue.result_classification.eq("direct_candidate_site")]
    assert len(direct) == 1
    assert set(leads.result_claim_id) == set(direct.result_claim_id)


def test_openai_manifest_is_exact_non_direct_residual(tmp_path: Path) -> None:
    full.prepare(tmp_path)
    universe = pd.read_csv(tmp_path / "production_universe.csv")
    first = universe.iloc[0]
    lead = pd.DataFrame(
        [{column: "" for column in full.URL_LEAD_COLUMNS}]
    )
    lead.loc[0, "candidate_year_id"] = first.candidate_year_id
    lead.to_csv(tmp_path / "serper/direct_url_leads.csv", index=False)
    audit = full.prepare_openai(tmp_path)
    manifest = pd.read_csv(tmp_path / "openai/candidate_manifest.csv")
    assert len(manifest) == len(universe) - 1
    assert audit["serper_direct_candidate_owners"] == 1
    assert first.candidate_year_id not in set(manifest.candidate_year_id)
    assert manifest.candidate_year_id.is_unique
    assert manifest.attempt_id.is_unique


def test_parallel_serper_checkpoints_every_exact_key(tmp_path: Path) -> None:
    full.prepare(tmp_path)
    manifest_path = tmp_path / "serper/query_manifest.csv"
    manifest = pd.read_csv(manifest_path, keep_default_na=False).head(4)
    manifest.to_csv(manifest_path, index=False)
    session = FakeSession({"organic": []})
    audit = full.run_serper(
        tmp_path, request_budget=4, workers=4, api_key="test-key", session=session
    )
    ledger = pd.read_csv(tmp_path / "serper/request_ledger.csv")
    raw = [
        json.loads(line)
        for line in (tmp_path / "serper/raw_responses.jsonl").read_text().splitlines()
    ]
    assert session.calls == 4
    assert audit["attempted_rows"] == 4
    assert set(ledger.query_attempt_id) == set(manifest.query_attempt_id)
    assert {row["query_attempt_id"] for row in raw} == set(manifest.query_attempt_id)


def test_parallel_openai_respects_exact_manifest_and_cost(tmp_path: Path) -> None:
    full.prepare(tmp_path)
    full.prepare_openai(tmp_path)
    manifest_path = tmp_path / "openai/candidate_manifest.csv"
    manifest = pd.read_csv(manifest_path, keep_default_na=False).head(3)
    manifest.to_csv(manifest_path, index=False)
    payload = {
        "id": "resp_test",
        "output": [
            {
                "type": "web_search_call",
                "status": "completed",
                "action": {
                    "sources": [
                        {"type": "url", "title": "Candidate", "url": "https://candidate.example/"}
                    ]
                },
            }
        ],
        "usage": {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120},
    }
    session = FakeSession(payload)
    audit = full.run_openai_parallel(
        tmp_path, request_budget=3, workers=3, api_key="test-key", session=session
    )
    ledger = pd.read_csv(tmp_path / "openai/request_ledger.csv")
    sources = pd.read_csv(tmp_path / "openai/source_urls.csv")
    assert session.calls == 3
    assert audit["attempted_rows"] == 3
    assert audit["cost_ceiling_respected"] is True
    assert set(ledger.attempt_id) == set(manifest.attempt_id)
    assert len(sources) == 3


def test_prepare_retry_selects_only_explicit_parse_errors(tmp_path: Path) -> None:
    full.prepare(tmp_path)
    full.prepare_openai(tmp_path)
    manifest = pd.read_csv(tmp_path / "openai/candidate_manifest.csv", keep_default_na=False)
    outputs = pd.DataFrame({
        "candidate_year_id": manifest.head(3).candidate_year_id,
        "parse_status": ["parsed", "parse_error", "parse_error"],
    })
    extracted = tmp_path / "openai/extracted"
    extracted.mkdir(parents=True)
    outputs.to_csv(extracted / "assistant_outputs.csv", index=False)
    audit = full.prepare_openai_parse_retries(tmp_path)
    retry = pd.read_csv(tmp_path / "openai/retry_parse_errors/candidate_manifest.csv")
    assert audit["retry_rows"] == 2
    assert set(retry.candidate_year_id) == set(manifest.iloc[1:3].candidate_year_id)
    assert retry.attempt_id.str.startswith("openai_search_retry_").all()
