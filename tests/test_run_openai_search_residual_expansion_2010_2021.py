from __future__ import annotations

import pandas as pd
import pytest

import scripts.run_openai_search_residual_expansion_2010_2021 as expansion
import scripts.run_openai_search_residual_pilot_2010_2021 as pilot
import scripts.extract_openai_search_residual_leads as extractor


def test_real_expansion_manifest_is_exact_residual() -> None:
    screen = pd.read_csv(expansion.DEFAULT_SCREEN, keep_default_na=False)
    serper = pd.read_csv(expansion.DEFAULT_SERPER_ELIGIBLE, keep_default_na=False)
    prior = pd.read_csv(expansion.DEFAULT_PILOT_MANIFEST, keep_default_na=False)
    manifest, audit = expansion.build_expansion_manifest(screen, serper, prior)
    assert len(manifest) == 357
    assert manifest.candidate_year_id.is_unique and manifest.attempt_id.is_unique
    assert audit["period_counts"] == expansion.EXPECTED_PERIOD_COUNTS
    assert audit["pilot_overlap"] == audit["serper_overlap"] == 0
    assert manifest.contract_version.eq(expansion.CONTRACT_VERSION).all()


def test_generalized_runner_rejects_budget_over_expansion_limit(tmp_path) -> None:
    with pytest.raises(pilot.OpenAISearchPilotError, match="between 1 and 357"):
        pilot.run(
            tmp_path,
            request_budget=358,
            max_requests=357,
            expected_rows=357,
            cost_ceiling_usd=5.5,
            contract_version=expansion.CONTRACT_VERSION,
            api_key="unused",
        )


def test_extractor_reports_configured_expected_row_count() -> None:
    with pytest.raises(extractor.OpenAILeadExtractionError, match="exactly 357"):
        extractor.extract_leads(
            [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            expected_rows=357,
            contract_version=expansion.LEAD_CONTRACT_VERSION,
        )
