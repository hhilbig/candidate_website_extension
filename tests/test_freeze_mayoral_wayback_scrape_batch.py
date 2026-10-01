from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

import scripts.freeze_mayoral_wayback_scrape_batch as freeze


def _targets() -> pd.DataFrame:
    rows = []
    for index in range(28):
        rows.append({
            "target_id": f"target-{index}", "candidate_year_id": f"candidate-{index // 2}",
            "candidate_contest_id": f"contest-{index}", "contest": f"contest-{index}",
            "full_name": f"Candidate {index}", "geo_name": "Example", "state_abb": "CA",
            "year": 2019, "month": 11, "period": "2018_2021",
            "registrable_domain": f"candidate{index}.example",
            "site_root_url": f"https://www.candidate{index}.example/",
            "primary_exact_url": f"https://www.Candidate{index}.example/About",
            "url_type": "custom_domain", "campaign_claim_count": 1,
            "campaign_claim_ids_json": json.dumps([f"claim-{index}"]),
            "campaign_urls_json": json.dumps([f"https://www.Candidate{index}.example/About"]),
            "evidence_urls_json": "[]",
        })
    return pd.DataFrame(rows)


def test_freeze_preserves_discovery_and_builds_four_domain_variants() -> None:
    frozen, audit = freeze.freeze_targets(_targets())
    assert len(frozen) == 28
    assert frozen.scrape_key.is_unique and frozen.scrape_hash.is_unique
    assert audit["candidate_years"] == 14
    first = frozen.iloc[0]
    variants = json.loads(first.url_variants_json)
    assert len(variants) == 4
    assert {value.split(":", 1)[0] for value in variants} == {"http", "https"}
    assert first.primary_exact_url.endswith("/About")
    assert first.scrape_window_start == "2018-05-01"
    assert first.scrape_window_end == "2020-03-29"


def test_freeze_rejects_missing_month_without_imputation() -> None:
    targets = _targets()
    targets["month"] = targets["month"].astype(object)
    targets.loc[0, "month"] = ""
    with pytest.raises(freeze.MayoralScrapeFreezeError, match="missing election month"):
        freeze.freeze_targets(targets)


def test_build_can_defer_missing_month_with_audited_output(tmp_path: Path) -> None:
    targets = _targets().iloc[:2].copy()
    targets["month"] = targets["month"].astype(object)
    targets.loc[0, "month"] = ""
    input_path = tmp_path / "source.csv"
    targets.to_csv(input_path, index=False)
    audit = freeze.build(
        input_path,
        tmp_path / "frozen",
        expected_targets=1,
        defer_missing_months=True,
    )
    assert audit["target_rows"] == 1
    assert audit["deferred_missing_month_rows"] == 1
    deferred = pd.read_csv(tmp_path / "frozen/deferred_missing_month.csv")
    assert deferred.iloc[0].deferred_reason == "missing_election_month_no_date_imputation"
