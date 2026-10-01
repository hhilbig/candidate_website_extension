import json

import pandas as pd
import pytest

from scripts.evaluate_mayoral_source_channels import (
    ChannelEvaluationError,
    evaluate_source_channels,
    write_evaluation,
)


def make_screen(n=40, early_n=20):
    rows = []
    for number in range(n):
        rows.append({
            "candidate_year_id": f"candidate-{number}",
            "year": 2001 if number < early_n else 2019,
            "population_weight": 2.0 if number % 2 else 1.0,
        })
    return pd.DataFrame(rows)


def claim(
    candidate_number,
    source_id,
    *,
    grade="A",
    status="accepted",
    acceptance=True,
    url_type="custom_domain",
):
    return {
        "candidate_year_id": f"candidate-{candidate_number}",
        "source_id": source_id,
        "evidence_grade": grade,
        "review_status": status,
        "acceptance": acceptance,
        "url_type": url_type,
    }


def metadata(*channel_ids, costs=None, order=None, drop_reports=None):
    costs = costs or {}
    return {
        "channel_order": list(order or channel_ids),
        "channels": {
            channel_id: {
                "analyst_minutes": 30,
                "license_cost": costs.get(channel_id, 0),
            }
            for channel_id in channel_ids
        },
        "drop_reports": drop_reports or {},
    }


def evaluate(screen, claims, cost_data):
    early_n = int(screen["year"].between(2000, 2007).sum())
    return evaluate_source_channels(
        screen,
        pd.DataFrame(claims),
        cost_data,
        expected_screen_size=len(screen),
        expected_early_size=early_n,
    )


def test_overlapping_channels_have_separate_marginal_and_cumulative_yields():
    screen = make_screen()
    claims = [
        claim(0, "archive_it"),
        claim(1, "archive_it", url_type="shared_host_path"),
        claim(1, "municipal", url_type="shared_host_path"),
        claim(2, "municipal"),
    ]
    result = evaluate(screen, claims, metadata("archive_it", "municipal"))
    first, second = result["channels"]

    assert first["gross_exact_n"] == 2
    assert first["marginal_exact_n"] == 2
    assert first["cumulative_exact_n"] == 2
    assert first["gross_primary_n"] == 1
    assert second["gross_exact_n"] == 2
    assert second["marginal_exact_n"] == 1
    assert second["cumulative_exact_n"] == 3
    assert second["marginal_primary_n"] == 1
    assert result["stack"]["strict_exact_candidate_years"] == 3


def test_precision_uses_all_adjudicated_claims_and_excludes_pending_claims():
    screen = make_screen()
    claims = [claim(number, "municipal") for number in range(8)]
    claims.extend([
        claim(8, "municipal", grade="C"),
        claim(9, "municipal", status="rejected", acceptance=False),
        claim(10, "municipal", grade="C", status="pending", acceptance=None),
    ])
    result = evaluate(screen, claims, metadata("municipal"))
    channel = result["channels"][0]

    assert channel["adjudicated_claim_n"] == 10
    assert channel["strict_claim_n"] == 8
    assert channel["accepted_target_claim_n"] == 9
    assert channel["adjudicated_precision"] == pytest.approx(0.9)
    assert channel["pending_claim_n"] == 1
    assert channel["decision"] == "pending_adjudication"


def test_five_early_strict_urls_pass_the_alternative_yield_gate():
    screen = make_screen()
    claims = [claim(number, "archive_it") for number in range(5)]
    result = evaluate(screen, claims, metadata("archive_it"))
    channel = result["channels"][0]

    assert channel["marginal_exact_n"] == 5
    assert channel["marginal_early_exact_n"] == 5
    assert channel["adjudicated_precision"] == 1.0
    assert channel["cost_per_accepted_url"] == 0.0
    assert channel["decision"] == "continue"


def test_revised_gate_can_define_2010_2013_as_early_period():
    screen = pd.DataFrame([
        {
            "candidate_year_id": f"candidate-{number}",
            "year": 2011 if number < 5 else 2019,
            "population_weight": 1.0,
        }
        for number in range(10)
    ])
    result = evaluate_source_channels(
        screen,
        pd.DataFrame([claim(number, "municipal") for number in range(5)]),
        metadata("municipal"),
        expected_screen_size=10,
        expected_early_size=5,
        early_year_start=2010,
        early_year_end=2013,
    )
    assert result["screen"]["early_year_start"] == 2010
    assert result["screen"]["early_year_end"] == 2013
    assert result["channels"][0]["marginal_early_exact_n"] == 5
    assert result["channels"][0]["decision"] == "continue"


def test_cost_thresholds_use_marginal_accepted_urls():
    screen = make_screen(n=50, early_n=0)
    claims = [claim(number, "cheap") for number in range(10)]
    claims.extend(claim(number, "expensive") for number in range(10, 20))
    claims.extend(claim(number, "boundary") for number in range(20, 30))
    costs = {"cheap": 999, "expensive": 2510, "boundary": 1000}
    result = evaluate(
        screen,
        claims,
        metadata("cheap", "expensive", "boundary", costs=costs),
    )
    by_channel = {row["channel_id"]: row for row in result["channels"]}

    assert by_channel["cheap"]["cost_per_accepted_url"] == pytest.approx(99.9)
    assert by_channel["cheap"]["decision"] == "continue"
    assert by_channel["expensive"]["cost_per_accepted_url"] == pytest.approx(251.0)
    assert by_channel["expensive"]["decision"] == "drop"
    assert by_channel["boundary"]["cost_per_accepted_url"] == pytest.approx(100.0)
    assert by_channel["boundary"]["decision"] == "hold"


def test_full_acquisition_gate_requires_coverage_lower_bound_precision_and_cost():
    screen = make_screen(n=100, early_n=20)
    claims = [claim(number, "archive_it") for number in range(25)]
    cost_data = metadata("archive_it", costs={"archive_it": 0})
    cost_data["acquisition_gate"] = {
        "universe_candidate_years": 100,
        "existing_primary_strict_candidate_years": 0,
        "projected_production_cost": 100,
        "approved_production_cost_ceiling": 200,
    }
    result = evaluate(screen, claims, cost_data)
    gate = result["stack"]["acquisition_gate"]
    assert gate["decision"] == "proceed_to_full_acquisition"
    assert gate["coverage_15_percent_pass"] is True
    assert gate["lower_bound_13_percent_pass"] is True
    assert gate["precision_95_percent_pass"] is True
    assert gate["production_cost_pass"] is True

    cost_data["acquisition_gate"]["projected_production_cost"] = 201
    result = evaluate(screen, claims, cost_data)
    assert result["stack"]["acquisition_gate"]["decision"] == "do_not_proceed"
    assert result["stack"]["acquisition_gate"]["production_cost_pass"] is False


def test_full_acquisition_gate_stops_when_no_channel_passes() -> None:
    screen = make_screen(n=40, early_n=0)
    claims = [claim(number, "search") for number in range(6)]
    result = evaluate(screen, claims, metadata("search", costs={"search": 0.48}))

    assert result["channels"][0]["decision"] == "hold"
    gate = result["stack"]["acquisition_gate"]
    assert gate["continued_channels"] == []
    assert gate["decision"] == "do_not_proceed"


def test_pending_c_grade_blocks_a_pass_and_never_counts_as_strict():
    screen = make_screen(n=30, early_n=0)
    claims = [claim(number, "provider") for number in range(10)]
    claims.append(
        claim(10, "provider", grade="C", status="pending", acceptance=None)
    )
    result = evaluate(screen, claims, metadata("provider"))
    channel = result["channels"][0]

    assert channel["strict_claim_n"] == 10
    assert channel["marginal_exact_n"] == 10
    assert channel["pending_claim_n"] == 1
    assert channel["decision"] == "pending_adjudication"


def test_identity_or_temporal_mismatch_cannot_become_strict():
    screen = make_screen()
    claims = pd.DataFrame([
        {
            **claim(0, "archive_it"),
            "identity_match": False,
            "temporal_match": True,
            "strict_exact_url": False,
        },
        {
            **claim(1, "archive_it"),
            "identity_match": True,
            "temporal_match": "outside_window",
            "strict_exact_url": False,
        },
    ])
    result = evaluate_source_channels(
        screen,
        claims,
        metadata("archive_it"),
        expected_screen_size=len(screen),
        expected_early_size=20,
    )
    channel = result["channels"][0]
    assert channel["strict_claim_n"] == 0
    assert channel["gross_exact_n"] == 0
    assert channel["gross_primary_n"] == 0


def test_channel_with_no_claims_reports_zero_yield_and_drops():
    screen = make_screen()
    claims = [claim(0, "archive_source")]
    cost_data = {
        "channel_order": ["archive_it", "provider"],
        "channels": {
            "archive_it": {
                "source_ids": ["archive_source"],
                "analyst_minutes": 10,
                "license_cost": 0,
            },
            "provider": {
                "source_ids": ["provider_source"],
                "analyst_minutes": 20,
                "license_cost": 0,
            },
        },
    }
    result = evaluate(screen, claims, cost_data)
    provider = result["channels"][1]

    assert provider["claim_n"] == 0
    assert provider["marginal_exact_n"] == 0
    assert provider["adjudicated_precision"] is None
    assert provider["decision"] == "drop"
    assert provider["gross_exact_projection"]["one_sided_90_lower_rate"] == 0.0
    assert provider["gross_exact_projection"]["one_sided_90_lower_additions"] == 0.0


def test_one_source_can_preserve_independent_claims_for_one_candidate():
    screen = make_screen()
    claims = pd.DataFrame([
        claim(0, "archive_it"),
        claim(0, "archive_it", url_type="shared_host_path"),
    ])
    claims.insert(0, "claim_id", ["claim-one", "claim-two"])
    result = evaluate_source_channels(
        screen,
        claims,
        metadata("archive_it"),
        expected_screen_size=len(screen),
        expected_early_size=20,
    )
    channel = result["channels"][0]

    assert channel["claim_n"] == 2
    assert channel["strict_claim_n"] == 2
    assert channel["gross_exact_n"] == 1


def test_large_outside_screen_filter_requires_an_explicit_report():
    screen = make_screen(n=10, early_n=5)
    claims = [claim(0, "archive_it")]
    for number in range(10, 20):
        claims.append(claim(number, "archive_it"))

    with pytest.raises(ChannelEvaluationError, match="without an explicit report"):
        evaluate(screen, claims, metadata("archive_it"))

    reported = metadata(
        "archive_it",
        drop_reports={
            "restrict_claims_to_candidate_screen": (
                "The fixture includes claims from a separate sample for this audit."
            )
        },
    )
    result = evaluate(screen, claims, reported)
    assert result["drop_audit"][0]["large_drop"] is True
    assert result["channels"][0]["claim_n"] == 1


def test_writes_nonempty_json_csv_and_audit_files(tmp_path):
    screen = make_screen()
    result = evaluate(
        screen,
        [claim(number, "archive_it") for number in range(5)],
        metadata("archive_it"),
    )
    paths = write_evaluation(result, tmp_path)

    assert set(paths) == {"report", "channels", "drop_audit"}
    assert all(path.exists() and path.stat().st_size > 0 for path in paths.values())
    stored = json.loads(paths["report"].read_text())
    assert stored["channels"][0]["decision"] == "continue"
