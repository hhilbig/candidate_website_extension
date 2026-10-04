from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

import scripts.run_openai_search_residual_pilot_2010_2021 as pilot


def _screen() -> pd.DataFrame:
    rows = []
    periods = [("2010_2013", 2011), ("2014_2017", 2015), ("2018_2021", 2019)]
    for period, year in periods:
        for index in range(160):
            rows.append(
                {
                    "candidate_year_id": f"{period}-{index}",
                    "candidate_contest_id": f"contest-{period}-{index // 4}",
                    "full_name": f"Candidate {index} Smith",
                    "geo_name": "Example City",
                    "state_abb": "CA",
                    "year": year,
                    "month": 11,
                    "period": period,
                }
            )
    return pd.DataFrame(rows)


def test_residual_sample_is_exact_balanced_and_excludes_serper_leads() -> None:
    screen = _screen()
    screen.insert(0, "sample_order", range(1, len(screen) + 1))
    excluded = pd.DataFrame(
        {"candidate_year_id": ["2010_2013-0", "2014_2017-0", "2018_2021-0"]}
    )
    manifest, audit = pilot.build_candidate_manifest(screen, excluded)
    assert len(manifest) == 100
    assert manifest.groupby("period").size().to_dict() == pilot.PERIOD_QUOTAS
    assert not set(manifest.candidate_year_id) & set(excluded.candidate_year_id)
    assert manifest.attempt_id.is_unique
    assert manifest.max_tool_calls.eq(1).all()
    assert "parent_sample_order" in manifest.columns
    assert manifest.sample_order.tolist() == list(range(1, 101))
    assert audit["residual_rows"] == 477
    assert audit["residual_rows_not_sampled"] == 377
    assert audit["residual_row_reduction_rate"] > 0.20


def test_payload_has_hard_search_and_output_caps() -> None:
    manifest, _ = pilot.build_candidate_manifest(
        _screen(), pd.DataFrame({"candidate_year_id": []})
    )
    payload = pilot.request_payload(manifest.iloc[0])
    assert payload["model"] == pilot.MODEL_SNAPSHOT
    assert payload["max_tool_calls"] == 1
    assert payload["max_output_tokens"] == 800
    assert payload["tools"] == [{"type": "web_search", "search_context_size": "low"}]
    assert payload["parallel_tool_calls"] is False
    assert payload["input"].startswith("Invoke the web search tool exactly once")
    assert payload["store"] is False


def test_source_extraction_preserves_action_sources_and_citations() -> None:
    row = {"attempt_id": "attempt-1", "candidate_year_id": "candidate-1"}
    response = {
        "id": "resp-1",
        "output": [
            {
                "type": "web_search_call",
                "action": {
                    "sources": [
                        {"type": "url", "url": "https://Candidate.example/Vote", "title": "Vote"}
                    ]
                },
            },
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "annotations": [
                            {"type": "url_citation", "url": "https://news.example/election", "title": "News"}
                        ],
                    }
                ],
            },
        ],
    }
    sources = pilot.extract_sources(response, row)
    assert sources.raw_url.tolist() == [
        "https://Candidate.example/Vote",
        "https://news.example/election",
    ]
    assert sources.source_claim_id.is_unique


class _Response:
    status_code = 200
    content = b'{"id":"resp-1","output":[{"type":"web_search_call","action":{"sources":[{"type":"url","url":"https://candidate.example/","title":"Candidate"}]}},{"type":"message","content":[]}],"usage":{"input_tokens":1000,"output_tokens":100,"total_tokens":1100}}'
    text = content.decode()

    def json(self):
        return json.loads(self.text)


class _Session:
    def __init__(self):
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        assert "Authorization" in kwargs["headers"]
        assert "test-secret" in kwargs["headers"]["Authorization"]
        return _Response()


def test_run_checkpoints_full_response_sources_cost_and_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, _ = pilot.build_candidate_manifest(
        _screen(), pd.DataFrame({"candidate_year_id": []})
    )
    monkeypatch.setattr(pilot, "DEFAULT_SCREEN", tmp_path / "screen.csv")
    monkeypatch.setattr(pilot, "DEFAULT_SERPER_ELIGIBLE", tmp_path / "eligible.csv")
    _screen().to_csv(tmp_path / "screen.csv", index=False)
    pd.DataFrame({"candidate_year_id": []}).to_csv(tmp_path / "eligible.csv", index=False)
    pilot.prepare(tmp_path / "out")
    session = _Session()
    audit = pilot.run(
        tmp_path / "out", request_budget=1, session=session, api_key="test-secret"
    )
    assert audit["attempted_rows"] == 1
    assert audit["web_search_calls"] == 1
    assert audit["source_claim_rows"] == 1
    assert audit["estimated_cost_usd"] < 0.02
    raw = (tmp_path / "out/raw_responses.jsonl").read_text()
    assert '"raw_response_text"' in raw and "candidate.example" in raw
    second = pilot.run(
        tmp_path / "out", request_budget=1, session=session, api_key="test-secret"
    )
    assert second["attempted_rows"] == 2
    assert len(session.calls) == 2


def test_env_loader_reads_only_openai_key(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("OTHER_SECRET=nope\nOPENAI_API_KEY='openai-only'\n", encoding="utf-8")
    assert pilot.load_openai_api_key(environ={}, env_path=env) == "openai-only"


def test_cost_guard_is_below_approved_ceiling() -> None:
    assert pilot.MAX_NEXT_RESPONSE_COST < 0.11
    assert pilot.COST_CEILING_USD == 2.0


def test_cost_counts_incomplete_search_items_conservatively() -> None:
    response = {
        "output": [
            {"type": "web_search_call", "status": "completed"},
            {"type": "web_search_call", "status": "searching"},
        ],
        "usage": {"input_tokens": 1000, "output_tokens": 100},
    }
    cost, calls, completed = pilot.estimated_cost(response)
    assert calls == 2
    assert completed == 1
    assert cost > 0.02
