import json

import pandas as pd
import pytest

from scripts.build_mayoral_confirmation_sample import (
    ConfirmationSampleError,
    build_confirmation_sample,
    write_confirmation_outputs,
)


ERA_YEARS = {
    "2000_2007": 2001,
    "2008_2013": 2009,
    "2014_2017": 2015,
    "2018_2021": 2019,
}
SMALL_QUOTAS = {
    "2000_2007": 8,
    "2008_2013": 4,
    "2014_2017": 4,
    "2018_2021": 4,
}


def make_inputs(rows_per_era=40):
    summary_rows = []
    universe_rows = []
    pilot_rows = []
    screen_rows = []
    contest_rows = []
    candidate_number = 0
    for era_index, (era, year) in enumerate(ERA_YEARS.items()):
        for within_era in range(rows_per_era):
            candidate_number += 1
            candidate_id = f"candidate-{era_index}-{within_era}"
            contest_size = 2 if within_era < rows_per_era / 2 else 5
            summary_rows.append({
                "candidate_year_id": candidate_id,
                "candidate_contest_id": f"contest-{candidate_id}",
                "ledb_candid": candidate_number,
                "full_name": f"Candidate {candidate_number}",
                "geo_name": f"City {era_index}",
                "state_abb": "CA",
                "year": year,
                "population_2020": 100_000,
                "ballotpedia_url": None,
                "stage_scope_status": "stage_unresolved",
                "election_stage_status": (
                    "single_recorded_mayoral_contest_per_city_year_stage_not_explicit"
                ),
                "has_validated_campaign_url": within_era == 3,
            })
            universe_rows.append({
                "candidate_year_id": candidate_id,
                "contest": f"contest-{era_index}-{within_era}",
                "place_fips": 10000 + era_index,
                "month": 5 + era_index,
                "contest_n_candidates": contest_size,
            })
            if within_era == 0:
                pilot_rows.extend([
                    {"candidate_year_id": candidate_id, "query_variant": "one"},
                    {"candidate_year_id": candidate_id, "query_variant": "two"},
                ])
            if within_era in {1, 2}:
                screen_rows.append({"candidate_year_id": candidate_id})
            if within_era == 4:
                contest_rows.append({
                    "candidate_year_id": candidate_id,
                    "contest": f"complete-contest-{era_index}",
                })
    return (
        pd.DataFrame(summary_rows),
        pd.DataFrame(universe_rows),
        pd.DataFrame(pilot_rows),
        pd.DataFrame(screen_rows),
        pd.DataFrame(contest_rows),
    )


def build_small(summary=None, universe=None, pilot=None, screen=None, contests=None):
    if summary is None:
        summary, universe, pilot, screen, contests = make_inputs()
    return build_confirmation_sample(
        summary,
        universe,
        pilot,
        screen,
        contests,
        quotas=SMALL_QUOTAS,
        expected_pilot_size=4,
        expected_screen_size=8,
        expected_complete_contests=4,
    )


def test_draws_deterministic_disjoint_era_quotas_with_design_weights():
    summary, universe, pilot, screen, contests = make_inputs()
    first = build_small(summary, universe, pilot, screen, contests)
    second = build_small(
        summary.sample(frac=1, random_state=31),
        universe.sample(frac=1, random_state=32),
        pilot.sample(frac=1, random_state=33),
        screen.sample(frac=1, random_state=34),
        contests.sample(frac=1, random_state=35),
    )
    sample = first["confirmation_sample"]

    assert len(sample) == 20
    assert sample.groupby("era").size().to_dict() == SMALL_QUOTAS
    assert sample["candidate_year_id"].tolist() == second[
        "confirmation_sample"
    ]["candidate_year_id"].tolist()
    excluded = (
        set(pilot["candidate_year_id"])
        | set(screen["candidate_year_id"])
        | set(contests["candidate_year_id"])
    )
    assert not set(sample["candidate_year_id"]) & excluded
    assert not sample["has_validated_campaign_url"].astype(bool).any()
    assert sample["sample_seed"].eq(20260824).all()
    assert sample["sample_kind"].eq("candidate_year_confirmation").all()

    grouped = sample.groupby(["era", "screening_stratum"], observed=True)
    for _, group in grouped:
        expected_probability = (
            group["stratum_sample_n"].iloc[0]
            / group["stratum_population"].iloc[0]
        )
        assert group["inclusion_probability"].eq(expected_probability).all()
        assert (
            group["inclusion_probability"] * group["population_weight"]
        ).eq(1.0).all()


def test_retains_all_five_balance_dimensions_and_balances_contest_size():
    sample = build_small()["confirmation_sample"]
    required = {
        "region",
        "population_quartile",
        "profile_availability",
        "stage_status",
        "contest_size_bin",
    }
    assert required.issubset(sample.columns)
    per_era_bins = sample.groupby("era")["contest_size_bin"].nunique()
    assert per_era_bins.eq(2).all()


def test_every_large_drop_has_an_explicit_audit_report():
    audit = build_small()["drop_audit"]
    large = audit.loc[audit["large_drop"]]
    assert not large.empty
    assert large["note"].astype(str).str.strip().ne("").all()


def test_fails_if_the_pilot_and_screen_overlap():
    summary, universe, pilot, screen, contests = make_inputs()
    screen = screen.copy()
    screen.loc[0, "candidate_year_id"] = pilot["candidate_year_id"].iloc[0]

    with pytest.raises(ConfirmationSampleError, match="overlap"):
        build_confirmation_sample(
            summary,
            universe,
            pilot,
            screen,
            contests,
            quotas=SMALL_QUOTAS,
            expected_pilot_size=4,
            expected_screen_size=8,
            expected_complete_contests=4,
        )


def test_fails_when_an_era_cannot_supply_its_quota():
    summary, universe, pilot, screen, contests = make_inputs(rows_per_era=10)
    with pytest.raises(ConfirmationSampleError, match="below quota 8"):
        build_confirmation_sample(
            summary,
            universe,
            pilot,
            screen,
            contests,
            quotas=SMALL_QUOTAS,
            expected_pilot_size=4,
            expected_screen_size=8,
            expected_complete_contests=4,
        )


def test_writes_hashed_outputs_without_marking_an_acquisition_decision(tmp_path):
    summary, universe, pilot, screen, contests = make_inputs()
    outputs = build_small(summary, universe, pilot, screen, contests)
    input_paths = {
        "candidate_summary": tmp_path / "summary.csv",
        "candidate_universe": tmp_path / "universe.csv",
        "pilot_manifest": tmp_path / "pilot.csv",
        "candidate_screen": tmp_path / "screen.csv",
        "complete_contest_sample": tmp_path / "complete_contests.csv",
    }
    for frame, path in zip(
        (summary, universe, pilot, screen, contests),
        input_paths.values(),
    ):
        frame.to_csv(path, index=False)
    output_dir = tmp_path / "confirmation"
    manifest_path = write_confirmation_outputs(
        outputs,
        output_dir,
        input_paths=input_paths,
        quotas=SMALL_QUOTAS,
    )
    manifest = json.loads(manifest_path.read_text())

    assert manifest["sample_is_acquisition_decision"] is False
    assert manifest["era_quotas"] == SMALL_QUOTAS
    for metadata in manifest["outputs"].values():
        path = output_dir / metadata["path"]
        assert path.exists() and path.stat().st_size > 0
        assert len(metadata["sha256"]) == 64
