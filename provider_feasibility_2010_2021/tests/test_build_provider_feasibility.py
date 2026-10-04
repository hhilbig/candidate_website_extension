from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from provider_feasibility_2010_2021.scripts import build_provider_feasibility as provider_build


WORKSTREAM_ROOT = Path(__file__).resolve().parents[1]


def _fixture_source() -> pd.DataFrame:
    rows = []
    for year in range(2010, 2022):
        for index in range(30):
            rows.append(
                {
                    "candidate_contest_id": f"contest-{year}-{index}",
                    "candidate_year_id": f"candidate-{year}-{index}",
                    "ledb_candid": year * 100 + index,
                    "full_name": f"Candidate {year} {index}",
                    "geo_name": f"city-{index % 7}",
                    "state_abb": "CA" if index % 2 else "TX",
                    "year": year,
                    "month": 11 if index else pd.NA,
                    "contest": f"contest-{year}",
                    "population_2020": 100000 + index,
                    "stage_scope_status": "stage_unresolved",
                    "in_verified_general_scope": False,
                    "election_page_stage": pd.NA,
                    "election_page_stage_label": pd.NA,
                    "election_page_election_date": pd.NA,
                    "covered": False,
                    "coverage_basis": "uncovered",
                }
            )
    return pd.DataFrame(rows)


def _fixture_parent(source: pd.DataFrame) -> pd.DataFrame:
    parent = source.copy()
    parent.insert(0, "sample_order", range(1, len(parent) + 1))
    parent["sample_kind"] = "fixture_common_screen"
    parent["sample_seed"] = 20260825
    parent["period"] = pd.cut(
        parent["year"],
        bins=[2009, 2013, 2017, 2021],
        labels=["2010_2013", "2014_2017", "2018_2021"],
    ).astype(str)
    parent["design_stratum"] = "fixture"
    parent["balance_cell"] = "fixture"
    parent["inclusion_probability"] = 1.0
    parent["population_weight"] = 1.0
    return parent


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "source.csv"
    parent = tmp_path / "parent.csv"
    fixture = _fixture_source()
    fixture.to_csv(source, index=False)
    _fixture_parent(fixture).to_csv(parent, index=False)
    return source, parent


def test_freeze_and_masked_sample_contract(tmp_path: Path) -> None:
    source, parent = _write_inputs(tmp_path)
    output = tmp_path / "build"
    manifest = provider_build.build(
        source_path=source, parent_screen_path=parent, output_root=output
    )

    frozen = pd.read_csv(output / "data/frozen_unresolved_2010_2021.csv")
    sample = pd.read_csv(output / "data/masked_provider_sample_100.csv")
    crosswalk = pd.read_csv(output / "data/internal_masked_sample_crosswalk.csv")

    assert len(frozen) == 360
    assert len(sample) == 100
    assert sample.groupby("year").size().to_dict() == {2018: 25, 2019: 25, 2020: 25, 2021: 25}
    assert set(sample.columns) == set(provider_build.SHAREABLE_COLUMNS)
    assert not provider_build.PROHIBITED_SHAREABLE_COLUMNS.intersection(sample.columns)
    assert sample["candidate_name"].notna().all()
    assert sample["masked_candidate_id"].str.fullmatch(r"PFS-[0-9A-F]{16}").all()
    assert sample["masked_candidate_id"].is_unique
    assert set(sample["masked_candidate_id"]) == set(crosswalk["masked_candidate_id"])
    assert set(crosswalk["candidate_year_id"]).issubset(
        set(pd.read_csv(parent)["candidate_year_id"])
    )
    assert sample.loc[sample["year"].between(2020, 2021), "democracy_works_requested"].all()
    assert not sample.loc[sample["year"].between(2018, 2019), "democracy_works_requested"].any()
    assert manifest["sampling"]["democracy_works_requested_rows"] == 50


def test_build_is_byte_deterministic(tmp_path: Path) -> None:
    source, parent = _write_inputs(tmp_path)
    first = tmp_path / "first"
    second = tmp_path / "second"
    manifest_first = provider_build.build(
        source_path=source, parent_screen_path=parent, output_root=first
    )
    manifest_second = provider_build.build(
        source_path=source, parent_screen_path=parent, output_root=second
    )

    assert manifest_first == manifest_second
    for item in manifest_first["generated_files"].values():
        relative = Path(item["path"])
        assert (first / relative).read_bytes() == (second / relative).read_bytes()
    assert (first / "sample_manifest.json").read_bytes() == (
        second / "sample_manifest.json"
    ).read_bytes()


def _valid_found_row(masked_id: str, history_status: str = "event_specific_historical") -> dict:
    return {
        "masked_candidate_id": masked_id,
        "provider_name": "ballotpedia",
        "match_status": "found",
        "provider_candidate_id": "can-1",
        "provider_election_id": "election-1",
        "provider_stage_id": "stage-1",
        "candidate_name": "Candidate Name",
        "office_name": "Mayor",
        "office_level": "Local",
        "office_branch": "Executive",
        "jurisdiction_name": "Example City",
        "state_abb": "CA",
        "election_date": "2020-11-03",
        "election_round": "General",
        "campaign_website_url": "https://candidate.example/path",
        "url_observation_date": "2020-10-01",
        "provider_record_observation_date": "2020-10-15",
        "provider_record_url": "https://provider.example/record/1",
        "source_provenance": "provider historical export version 2020-10-15",
        "historical_version_status": history_status,
        "provider_notes": "",
    }


def test_event_specific_response_is_eligible() -> None:
    schema = json.loads(
        (WORKSTREAM_ROOT / "config/provider_response_schema.json").read_text()
    )
    response = pd.DataFrame([_valid_found_row("PFS-0123456789ABCDEF")])
    checked = provider_build.validate_provider_response(
        response,
        schema,
        allowed_masked_ids={"PFS-0123456789ABCDEF"},
        expected_provider="ballotpedia",
        require_complete_attempts=True,
    )
    assert checked["event_specific_eligible"].tolist() == [True]


def test_current_profile_only_is_retained_but_ineligible() -> None:
    schema = json.loads(
        (WORKSTREAM_ROOT / "config/provider_response_schema.json").read_text()
    )
    response = pd.DataFrame(
        [_valid_found_row("PFS-0123456789ABCDEF", history_status="current_profile_only")]
    )
    checked = provider_build.validate_provider_response(response, schema)
    assert checked["event_specific_eligible"].tolist() == [False]


def test_response_validation_rejects_missing_observation_and_duplicate_key() -> None:
    schema = json.loads(
        (WORKSTREAM_ROOT / "config/provider_response_schema.json").read_text()
    )
    missing = _valid_found_row("PFS-0123456789ABCDEF")
    missing["url_observation_date"] = ""
    with pytest.raises(ValueError, match="missing url_observation_date"):
        provider_build.validate_provider_response(pd.DataFrame([missing]), schema)

    duplicate = pd.DataFrame(
        [
            _valid_found_row("PFS-0123456789ABCDEF"),
            _valid_found_row("PFS-0123456789ABCDEF"),
        ]
    )
    with pytest.raises(ValueError, match="duplicate row keys"):
        provider_build.validate_provider_response(duplicate, schema)


def test_response_validation_requires_exact_attempt_set() -> None:
    schema = json.loads(
        (WORKSTREAM_ROOT / "config/provider_response_schema.json").read_text()
    )
    response = pd.DataFrame([_valid_found_row("PFS-0123456789ABCDEF")])
    with pytest.raises(ValueError, match="omits requested masked IDs"):
        provider_build.validate_provider_response(
            response,
            schema,
            allowed_masked_ids={"PFS-0123456789ABCDEF", "PFS-FEDCBA9876543210"},
            expected_provider="ballotpedia",
            require_complete_attempts=True,
        )


def test_response_validation_requires_terminal_status() -> None:
    schema = json.loads(
        (WORKSTREAM_ROOT / "config/provider_response_schema.json").read_text()
    )
    row = _valid_found_row("PFS-0123456789ABCDEF")
    row["match_status"] = pd.NA
    with pytest.raises(ValueError, match="missing terminal match_status"):
        provider_build.validate_provider_response(pd.DataFrame([row]), schema)


def test_canonical_artifacts_match_manifest_and_actual_quotas() -> None:
    manifest = json.loads((WORKSTREAM_ROOT / "sample_manifest.json").read_text())
    sample = pd.read_csv(WORKSTREAM_ROOT / "data/masked_provider_sample_100.csv")
    frozen = pd.read_csv(WORKSTREAM_ROOT / "data/frozen_unresolved_2010_2021.csv")

    crosswalk = pd.read_csv(WORKSTREAM_ROOT / "data/internal_masked_sample_crosswalk.csv")
    parent = pd.read_csv(provider_build.DEFAULT_PARENT_SCREEN)
    source = pd.read_csv(provider_build.DEFAULT_SOURCE, low_memory=False)

    assert len(frozen) == 2669
    assert len(source) == 2669
    assert len(parent) == 480
    assert len(sample) == 100
    assert sample.groupby("year").size().to_dict() == {2018: 25, 2019: 25, 2020: 25, 2021: 25}
    assert int(sample["democracy_works_requested"].sum()) == 50
    assert not source["covered"].fillna(False).astype(bool).any()
    assert not frozen["covered"].fillna(False).astype(bool).any()
    assert not parent["covered"].fillna(False).astype(bool).any()
    assert set(frozen["candidate_year_id"]) == set(source["candidate_year_id"])
    parent_era_ids = set(
        parent.loc[parent["period"].eq("2018_2021"), "candidate_year_id"]
    )
    assert set(crosswalk["candidate_year_id"]).issubset(parent_era_ids)
    assert manifest["sampling"]["sample_is_exact_parent_subset"] is True
    assert manifest["sampling"]["unresolved_frame_has_covered_rows"] is False
    assert manifest["sampling"]["parent_screen_has_covered_rows"] is False
    assert manifest["unresolved_frame_source"]["sha256"] == (
        "f2c8c6f95f471391fac223068c8e4d9dd58c950cd092e50f64a83aef8f871ed5"
    )
    assert manifest["parent_screen_source"]["sha256"] == (
        "646e2688005ca7c4f0f20ed027aeb7709351adbdfa9864af26e143f1caf3eb19"
    )
    for item in manifest["generated_files"].values():
        path = WORKSTREAM_ROOT / item["path"]
        assert path.exists()
        assert path.stat().st_size > 0
        assert provider_build.sha256_file(path) == item["sha256"]


def test_rejects_covered_frame_or_parent_rows() -> None:
    source = _fixture_source()
    source.loc[0, "covered"] = True
    with pytest.raises(ValueError, match="contains covered rows"):
        provider_build.freeze_unresolved_frame(source)

    clean = provider_build.freeze_unresolved_frame(_fixture_source())
    parent = _fixture_parent(clean)
    parent.loc[0, "covered"] = True
    with pytest.raises(ValueError, match="contains covered rows"):
        provider_build.validate_parent_screen(clean, parent)
