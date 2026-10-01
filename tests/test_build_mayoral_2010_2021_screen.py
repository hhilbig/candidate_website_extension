import json

import pandas as pd
import pytest

from scripts.build_mayoral_2010_2021_screen import (
    BALANCE_COLUMNS,
    OUTPUT_FILENAMES,
    RevisedScreenError,
    build_revised_screen,
    write_outputs,
)


PERIOD_YEARS = {
    "2010_2013": 2011,
    "2014_2017": 2015,
    "2018_2021": 2019,
}
SMALL_CANDIDATE_QUOTAS = {period: 8 for period in PERIOD_YEARS}
SMALL_CONTEST_QUOTAS = {period: 2 for period in PERIOD_YEARS}


def make_inputs(contests_per_period=20):
    roster_rows = []
    pilot_rows = []
    earlier_screen_rows = []
    earlier_contest_rows = []
    confirmation_rows = []
    candidate_number = 0
    states = ("CA", "CA", "TX", "TX")
    regions = ("West", "West", "South", "South")
    population_groups = ("100k_149999", "250k_499999", "100k_149999", "250k_499999")

    for period_index, (period, year) in enumerate(PERIOD_YEARS.items()):
        for contest_number in range(contests_per_period):
            contest_size = 2 if contest_number % 2 == 0 else 3
            cell = contest_number % 4
            contest = f"contest-{period}-{contest_number}"
            place_fips = 100_000 + period_index * 1_000 + contest_number
            profile = "profile" if cell in {1, 3} else "no_profile"
            stage = "verified_general" if cell == 3 else "stage_unresolved"
            for within_contest in range(contest_size):
                candidate_number += 1
                candidate_id = f"candidate-{candidate_number}"
                covered = contest_number == 0 and within_contest == 0
                row = {
                    "candidate_year_id": candidate_id,
                    "candidate_contest_id": f"{contest}__{candidate_id}",
                    "contest": contest,
                    "ledb_candid": candidate_number,
                    "full_name": f"Candidate {candidate_number}",
                    "place_fips": place_fips,
                    "geo_name": f"City {period_index} {contest_number}",
                    "state_abb": states[cell],
                    "year": year,
                    "month": 5 + period_index,
                    "contest_n_candidates": contest_size,
                    "population_2020": 120_000 if cell % 2 == 0 else 300_000,
                    "ballotpedia_url": (
                        f"https://ballotpedia.org/Candidate_{candidate_number}"
                        if profile == "profile" else None
                    ),
                    "election_stage_status": (
                        "verified_general_election" if stage == "verified_general"
                        else "single_recorded_mayoral_contest_per_city_year_stage_not_explicit"
                    ),
                    "covered": covered,
                    "period": period,
                    "region": regions[cell],
                    "population_group": population_groups[cell],
                    "profile_availability": profile,
                    "stage_status": stage,
                    "contest_size_bin": "2" if contest_size == 2 else "3_4",
                }
                roster_rows.append(row)
                if contest_number == 1 and within_contest == 0:
                    pilot_rows.extend([
                        {"candidate_year_id": candidate_id, "query_variant": "one"},
                        {"candidate_year_id": candidate_id, "query_variant": "two"},
                    ])
                if contest_number == 2 and within_contest == 0:
                    earlier_screen_rows.append({"candidate_year_id": candidate_id})
                if contest_number == 3:
                    earlier_contest_rows.append({
                        "candidate_year_id": candidate_id,
                        "contest": contest,
                    })
                if contest_number == 4 and within_contest == 0:
                    confirmation_rows.append({"candidate_year_id": candidate_id})

    for number in range(2):
        candidate_number += 1
        roster_rows.append({
            "candidate_year_id": f"pre-2010-{number}",
            "candidate_contest_id": f"pre-contest-{number}__candidate",
            "contest": f"pre-contest-{number}",
            "ledb_candid": candidate_number,
            "full_name": f"Pre Candidate {number}",
            "place_fips": 900_000 + number,
            "geo_name": f"Pre City {number}",
            "state_abb": "CA",
            "year": 2009,
            "month": 5,
            "contest_n_candidates": 1,
            "population_2020": 120_000,
            "ballotpedia_url": None,
            "election_stage_status": "single_recorded_mayoral_contest_per_city_year_stage_not_explicit",
            "covered": False,
            "period": None,
            "region": "West",
            "population_group": "100k_149999",
            "profile_availability": "no_profile",
            "stage_status": "stage_unresolved",
            "contest_size_bin": "2",
        })

    roster = pd.DataFrame(roster_rows)
    unresolved = roster.loc[roster["period"].notna() & ~roster["covered"]].copy()
    return (
        roster,
        unresolved,
        pd.DataFrame(pilot_rows),
        pd.DataFrame(earlier_screen_rows),
        pd.DataFrame(earlier_contest_rows),
        pd.DataFrame(confirmation_rows),
    )


def build_small(inputs=None):
    if inputs is None:
        inputs = make_inputs()
    return build_revised_screen(
        *inputs,
        seed=20260825,
        candidate_quotas=SMALL_CANDIDATE_QUOTAS,
        contest_quotas=SMALL_CONTEST_QUOTAS,
        expected_pilot_size=3,
        expected_screen_size=3,
        expected_earlier_contests=3,
        expected_confirmation_size=3,
    )


def test_draws_deterministic_disjoint_samples_with_exact_weights():
    inputs = make_inputs()
    first = build_small(inputs)
    shuffled = tuple(
        frame.sample(frac=1, random_state=100 + number).reset_index(drop=True)
        for number, frame in enumerate(inputs)
    )
    second = build_small(shuffled)

    candidate = first["candidate_sample"]
    assert len(candidate) == 24
    assert candidate.groupby("period").size().to_dict() == SMALL_CANDIDATE_QUOTAS
    assert candidate["candidate_year_id"].tolist() == second["candidate_sample"][
        "candidate_year_id"
    ].tolist()
    assert not candidate["covered"].any()

    pilot_ids = set(inputs[2]["candidate_year_id"])
    earlier_screen_ids = set(inputs[3]["candidate_year_id"])
    earlier_contest_ids = set(inputs[4]["candidate_year_id"])
    explicit_exclusions = pilot_ids | earlier_screen_ids | earlier_contest_ids
    assert not set(candidate["candidate_year_id"]) & explicit_exclusions

    for _, group in candidate.groupby(["period", "design_stratum"], observed=True):
        probability = group["stratum_sample_n"].iloc[0] / group["stratum_population"].iloc[0]
        assert group["inclusion_probability"].eq(probability).all()
        assert (group["inclusion_probability"] * group["population_weight"]).eq(1.0).all()

    unresolved = inputs[1]
    eligible_ids = set(unresolved["candidate_year_id"]) - explicit_exclusions
    eligible = unresolved.loc[unresolved["candidate_year_id"].isin(eligible_ids)]
    expected_population = eligible.groupby("period").size().to_dict()
    assert candidate.groupby("period")["population_weight"].sum().to_dict() == pytest.approx(
        expected_population
    )

    contests = first["contest_sample"]
    assert contests.groupby("period")["contest"].nunique().to_dict() == SMALL_CONTEST_QUOTAS
    assert contests["candidate_year_id"].tolist() == second["contest_sample"][
        "candidate_year_id"
    ].tolist()
    prior_ids = explicit_exclusions | set(inputs[5]["candidate_year_id"])
    assert not set(contests["candidate_year_id"]) & prior_ids
    assert not set(contests["candidate_year_id"]) & set(candidate["candidate_year_id"])
    assert contests.groupby("contest").size().eq(
        contests.groupby("contest")["contest_n_candidates"].first()
    ).all()
    assert (contests["inclusion_probability"] * contests["population_weight"]).eq(1.0).all()


def test_audits_all_balance_fields_large_drops_and_confirmation_asymmetry():
    outputs = build_small()
    audit = outputs["drop_audit"]
    large = audit.loc[audit["large_drop"]]
    assert not large.empty
    assert large["note"].astype(str).str.strip().ne("").all()
    assert not audit["requires_investigation"].any()
    assert {"candidate_year", "contest_year", "city"}.issubset(set(audit["unit"]))

    asymmetry = audit.loc[
        audit["stage"].eq("retain_earlier_confirmation_cases_for_candidate_screen")
    ]
    assert len(asymmetry) == 2
    assert asymmetry["dropped_n"].eq(0).all()
    assert asymmetry["note"].str.contains("excluded only from").all()

    balance = outputs["balance_audit"]
    assert set(balance["dimension"]) == set(BALANCE_COLUMNS)
    assert set(balance["sample_kind"]) == {
        "candidate_year_screen_2010_2021",
        "complete_contest_year_2010_2021",
    }
    candidate_balance = balance.loc[
        balance["sample_kind"].eq("candidate_year_screen_2010_2021")
        & balance["dimension"].isin(BALANCE_COLUMNS[:-1])
    ]
    assert candidate_balance["absolute_weighted_share_difference"].max() < 1e-12


def test_fails_when_frozen_unresolved_keys_do_not_match_roster():
    inputs = list(make_inputs())
    inputs[1] = inputs[1].iloc[1:].copy()
    with pytest.raises(RevisedScreenError, match="key mismatch"):
        build_small(tuple(inputs))


def test_writes_deterministic_hashed_artifacts(tmp_path):
    inputs = make_inputs()
    outputs = build_small(inputs)
    names = (
        "coverage_roster", "unresolved_frame", "pilot_manifest",
        "earlier_screen", "earlier_contests", "earlier_confirmation",
    )
    input_paths = {}
    for name, frame in zip(names, inputs):
        path = tmp_path / f"{name}.csv"
        frame.to_csv(path, index=False)
        input_paths[name] = path

    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_manifest = write_outputs(
        outputs,
        first_dir,
        input_paths=input_paths,
        seed=20260825,
        candidate_quotas=SMALL_CANDIDATE_QUOTAS,
        contest_quotas=SMALL_CONTEST_QUOTAS,
    )
    second_manifest = write_outputs(
        outputs,
        second_dir,
        input_paths=input_paths,
        seed=20260825,
        candidate_quotas=SMALL_CANDIDATE_QUOTAS,
        contest_quotas=SMALL_CONTEST_QUOTAS,
    )
    first = json.loads(first_manifest.read_text())
    second = json.loads(second_manifest.read_text())
    assert first["sample_seed"] == 20260825
    assert first["candidate_quotas"] == SMALL_CANDIDATE_QUOTAS
    assert first["complete_contests_exclude_earlier_confirmation"] is True
    assert first["candidate_excludes_earlier_confirmation"] is False
    assert set(first["outputs"]) == set(OUTPUT_FILENAMES)
    for key, filename in OUTPUT_FILENAMES.items():
        assert first["outputs"][key]["sha256"] == second["outputs"][key]["sha256"]
        assert first["outputs"][key]["rows"] == len(outputs[key])
        assert (first_dir / filename).stat().st_size > 0
