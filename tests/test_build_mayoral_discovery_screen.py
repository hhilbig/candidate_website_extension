import json

import pandas as pd
import pytest

from scripts.build_mayoral_discovery_screen import (
    ERA_LABELS,
    OUTPUT_FILENAMES,
    ScreenBuildError,
    build_discovery_screen,
    drop_audit_row,
    stratified_candidate_sample,
    write_outputs,
)


ERA_YEARS = (2001, 2009, 2015, 2019)


def make_inputs(contests_per_era=30):
    summary_rows = []
    universe_rows = []
    pilot_rows = []
    states = ("CA", "TX", "CA", "TX")
    populations = (100_000, 200_000, 400_000, 800_000)
    candidate_number = 0
    for era_index, year in enumerate(ERA_YEARS):
        for contest_number in range(contests_per_era):
            cell = contest_number % 4
            city = f"city {era_index} {contest_number}"
            contest = f"contest-{year}-{contest_number}"
            place_fips = 100_000 + era_index * 1_000 + contest_number
            for within_contest in range(2):
                candidate_number += 1
                candidate_id = f"candidate-{candidate_number}__{year}"
                summary_rows.append({
                    "candidate_year_id": candidate_id,
                    "candidate_contest_id": f"{contest}__candidate-{candidate_number}",
                    "ledb_candid": candidate_number,
                    "full_name": f"Secret Name {candidate_number}",
                    "geo_name": city,
                    "state_abb": states[cell],
                    "year": year,
                    "population_2020": populations[cell],
                    "ballotpedia_url": (
                        f"https://ballotpedia.org/candidate-{candidate_number}"
                        if cell % 2 else None
                    ),
                    "stage_scope_status": "stage_unresolved",
                    "election_stage_status": (
                        "single_recorded_mayoral_contest_per_city_year_stage_not_explicit"
                    ),
                    "has_validated_campaign_url": bool(
                        contest_number == 1 and within_contest == 0
                    ),
                })
                universe_rows.append({
                    "candidate_year_id": candidate_id,
                    "contest": contest,
                    "place_fips": place_fips,
                    "month": 5 + era_index,
                    "contest_n_candidates": 2,
                })
                if contest_number == 0 and within_contest == 0:
                    pilot_rows.extend([
                        {"candidate_year_id": candidate_id, "query_variant": "one"},
                        {"candidate_year_id": candidate_id, "query_variant": "two"},
                    ])
    return (
        pd.DataFrame(summary_rows),
        pd.DataFrame(universe_rows),
        pd.DataFrame(pilot_rows),
    )


def build_small(summary=None, universe=None, pilot=None, **overrides):
    if summary is None:
        summary, universe, pilot = make_inputs()
    options = {
        "seed": 20260824,
        "screen_per_era": 8,
        "contests_per_era": 2,
        "city_priority_n": 3,
        "provider_sample_n": 5,
        "expected_pilot_size": 4,
    }
    options.update(overrides)
    return build_discovery_screen(summary, universe, pilot, **options)


def test_builds_deterministic_disjoint_samples_with_design_weights():
    summary, universe, pilot = make_inputs()
    first = build_small(summary, universe, pilot)
    second = build_small(
        summary.sample(frac=1, random_state=11),
        universe.sample(frac=1, random_state=12),
        pilot.sample(frac=1, random_state=13),
    )

    screen = first["screening_sample"]
    assert len(screen) == 32
    assert screen.groupby("era").size().to_dict() == {
        era: 8 for era in ERA_LABELS
    }
    assert screen["candidate_year_id"].tolist() == second["screening_sample"][
        "candidate_year_id"
    ].tolist()
    pilot_ids = set(pilot["candidate_year_id"])
    assert not set(screen["candidate_year_id"]) & pilot_ids
    assert not screen["has_validated_campaign_url"].any()

    sampled_strata = screen.groupby(["era", "screening_stratum"], observed=True)
    for _, group in sampled_strata:
        assert len(group) == group["stratum_sample_n"].iloc[0]
        expected_probability = (
            group["stratum_sample_n"].iloc[0] / group["stratum_population"].iloc[0]
        )
        assert group["inclusion_probability"].eq(expected_probability).all()
        assert (group["population_weight"] * group["inclusion_probability"]).eq(1).all()

    # Each era begins with 60 rows, then loses one validated and one pilot row.
    represented = screen.groupby("era")["population_weight"].sum()
    assert represented.to_dict() == pytest.approx({era: 58 for era in ERA_LABELS})

    contests = first["contest_sample"]
    assert contests.groupby("era")["contest_year_id"].nunique().to_dict() == {
        era: 2 for era in ERA_LABELS
    }
    assert contests.groupby("contest_year_id").size().eq(2).all()
    assert not set(contests["candidate_year_id"]) & set(screen["candidate_year_id"])
    assert not set(contests["candidate_year_id"]) & pilot_ids
    assert not contests["has_validated_campaign_url"].any()
    assert contests["contest_year_id"].tolist() == second["contest_sample"][
        "contest_year_id"
    ].tolist()


def test_priority_and_masked_provider_outputs_have_required_scope():
    outputs = build_small()
    priority = outputs["city_priority"]
    assert len(priority) == 3
    assert priority["priority_rank"].tolist() == [1, 2, 3]
    assert priority["unresolved_candidate_years"].is_monotonic_decreasing
    assert priority["first_year"].ge(2000).all()
    assert priority["last_year"].le(2007).all()

    masked = outputs["civic_provider_sample"]
    assert masked.columns.tolist() == [
        "masked_candidate_id",
        "geo_name",
        "state_abb",
        "year",
        "office",
        "election_date",
        "election_round",
    ]
    assert len(masked) == 5
    assert masked["masked_candidate_id"].str.match(r"^MCP-[0-9A-F]{16}$").all()
    assert masked["masked_candidate_id"].is_unique
    assert masked["year"].between(2018, 2021).all()
    assert masked["office"].eq("mayor").all()
    assert "Secret Name" not in masked.to_csv(index=False)
    assert "candidate_year_id" not in masked.columns


def test_large_drops_are_explicitly_audited():
    outputs = build_small()
    audit = outputs["drop_audit"]
    large = audit.loc[audit["large_drop"]]
    assert not large.empty
    assert large["note"].str.strip().ne("").all()
    assert large["expected"].all()
    assert not large["requires_investigation"].any()

    with pytest.raises(ScreenBuildError, match="requires an explicit audit note"):
        drop_audit_row(
            "silent_loss", 100, 70, expected=False, note=""
        )


def test_systematic_within_stratum_draw_balances_contest_size():
    rows = []
    for era in ERA_LABELS:
        for number in range(100):
            rows.append({
                "candidate_year_id": f"{era}-{number}",
                "era": era,
                "region": "West",
                "population_quartile": "Q1",
                "profile_availability": "no_profile",
                "stage_status": "stage_unresolved",
                "contest_size_bin": "2" if number < 50 else "5_plus",
                "screening_stratum": "West | Q1 | no_profile | stage_unresolved",
            })
    selected = stratified_candidate_sample(pd.DataFrame(rows), quota=20)
    assert selected.groupby("era").size().eq(20).all()
    assert selected.groupby(["era", "contest_size_bin"]).size().eq(10).all()
    assert selected["inclusion_probability"].eq(0.2).all()
    assert selected["population_weight"].eq(5).all()


def test_fails_when_an_era_quota_cannot_be_met():
    with pytest.raises(ScreenBuildError, match="below quota 100"):
        build_small(screen_per_era=100)


def test_fails_when_pilot_manifest_is_not_the_expected_unique_sample():
    summary, universe, pilot = make_inputs()
    with pytest.raises(ScreenBuildError, match="expected 120"):
        build_discovery_screen(
            summary,
            universe,
            pilot,
            screen_per_era=8,
            contests_per_era=2,
            city_priority_n=3,
            provider_sample_n=5,
        )


def test_writes_all_outputs_and_hashed_manifest(tmp_path):
    summary, universe, pilot = make_inputs()
    outputs = build_small(summary, universe, pilot)
    input_paths = {
        "candidate_summary": tmp_path / "summary.csv",
        "candidate_universe": tmp_path / "universe.csv",
        "pilot_manifest": tmp_path / "pilot.csv",
    }
    summary.to_csv(input_paths["candidate_summary"], index=False)
    universe.to_csv(input_paths["candidate_universe"], index=False)
    pilot.to_csv(input_paths["pilot_manifest"], index=False)

    output_dir = tmp_path / "screening"
    manifest_path = write_outputs(
        outputs, output_dir, input_paths=input_paths, seed=20260824
    )
    manifest = json.loads(manifest_path.read_text())
    assert manifest["sample_seed"] == 20260824
    assert set(manifest["outputs"]) == set(OUTPUT_FILENAMES)
    for key, filename in OUTPUT_FILENAMES.items():
        path = output_dir / filename
        assert path.exists()
        assert path.stat().st_size > 0
        assert manifest["outputs"][key]["rows"] == len(outputs[key])
        assert len(manifest["outputs"][key]["sha256"]) == 64
