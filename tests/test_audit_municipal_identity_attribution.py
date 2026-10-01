from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.audit_municipal_identity_attribution import (
    SOURCE_CONTEXT_COLUMNS,
    IdentityAttributionError,
    audit_identity_attribution,
    require_resolved_identity_attribution,
    validate_full_contest_rosters,
)


REPO = Path(__file__).resolve().parents[1]
AUDIT = (
    REPO
    / "data/municipal_discovery/screening_2010_2021/municipal_curated_acquisition/audit"
)


def _contract() -> tuple[pd.DataFrame, pd.DataFrame]:
    targets = pd.read_csv(AUDIT / "acquisition_targets.csv", dtype=str, keep_default_na=False)
    roster = pd.read_csv(
        AUDIT / "acquisition_candidate_targets.csv", dtype=str, keep_default_na=False
    )
    return targets, roster


def _target(
    targets: pd.DataFrame, source_id: str, geo_name: str
) -> pd.Series:
    rows = targets.loc[
        targets["source_id"].eq(source_id) & targets["geo_name"].eq(geo_name)
    ]
    assert len(rows) == 1
    return rows.iloc[0]


def _candidate(roster: pd.DataFrame, target_id: str, name: str) -> pd.Series:
    rows = roster.loc[
        roster["target_id"].eq(target_id) & roster["full_name"].eq(name)
    ]
    assert len(rows) == 1
    return rows.iloc[0]


def _evidence_row(
    evidence_id: str, target: pd.Series, candidate: pd.Series, url: str,
    source_file_id: str = "sha256:source",
) -> dict[str, str]:
    return {
        "evidence_id": evidence_id,
        "target_id": target["target_id"],
        "source_id": target["source_id"],
        "source_file_id": source_file_id,
        "contest": target["contest"],
        "candidate_year_id": candidate["candidate_year_id"],
        "exact_normalized_url": url,
    }


def _context_row(
    evidence_id: str,
    source_id: str,
    url: str,
    locator: str,
    candidate_name: str,
    *,
    kind: str = "direct_anchor_text",
    nearest_distance: str = "",
    runner_up_distance: str = "",
    explicit: bool = False,
    note: str = "",
    source_file_id: str = "sha256:source",
) -> dict[str, object]:
    return {
        "context_id": f"context-{evidence_id}",
        "evidence_id": evidence_id,
        "source_id": source_id,
        "source_file_id": source_file_id,
        "exact_normalized_url": url,
        "source_locator": locator,
        "association_kind": kind,
        "context_candidate_name": candidate_name,
        "context_text": f"{candidate_name} campaign website {url}",
        "nearest_distance": nearest_distance,
        "runner_up_distance": runner_up_distance,
        "explicit_separate_evidence": explicit,
        "separate_evidence_note": note,
    }


def _audit_subset(
    all_targets: pd.DataFrame,
    all_roster: pd.DataFrame,
    target_ids: list[str],
    evidence_rows: list[dict[str, str]],
    context_rows: list[dict[str, object]],
) -> tuple[pd.DataFrame, dict[str, object]]:
    targets = all_targets.loc[all_targets["target_id"].isin(target_ids)].copy()
    roster = all_roster.loc[all_roster["target_id"].isin(target_ids)].copy()
    return audit_identity_attribution(
        targets,
        roster,
        pd.DataFrame(evidence_rows),
        pd.DataFrame(context_rows, columns=SOURCE_CONTEXT_COLUMNS),
    )


def test_frozen_contract_contains_exact_full_contest_rosters() -> None:
    targets, roster = _contract()
    result = validate_full_contest_rosters(targets, roster)
    assert result == {
        "targets": 47,
        "candidate_source_rows": 264,
        "unique_candidate_years": 230,
    }


def test_prior_alameda_libby_schaaf_assignments_are_rejected() -> None:
    targets, roster = _contract()
    oakland = _target(targets, "alameda_2018_composite_voter_guide", "oakland")
    libby = _candidate(roster, oakland["target_id"], "libby schaaf")
    cases = [
        ("aimee", "http://aimee4schools.com", "page:272:annotation", "Aimee Eng"),
        ("charlie", "http://www.charlie.vote", "page:258:annotation", "Charlie Michelson"),
        ("loren", "http://www.LorenTaylor.org", "page:265:annotation", "Loren Taylor"),
    ]
    evidence = [_evidence_row(key, oakland, libby, url) for key, url, _, _ in cases]
    contexts = [
        _context_row(key, oakland["source_id"], url, locator, name)
        for key, url, locator, name in cases
    ]
    decisions, summary = _audit_subset(
        targets, roster, [oakland["target_id"]], evidence, contexts
    )
    assert set(decisions["identity_attribution_status"]) == {"reject_identity"}
    assert set(decisions["identity_attribution_reason"]) == {
        "context_names_different_person"
    }
    assert set(decisions["exact_normalized_url"].str.casefold()) == {
        "http://aimee4schools.com",
        "http://www.charlie.vote",
        "http://www.lorentaylor.org",
    }
    assert summary["reject_identity"] == 3
    assert summary["claim_approved_rows"] == 0


def test_hayward_candidates_cannot_receive_each_others_domains() -> None:
    targets, roster = _contract()
    hayward = _target(targets, "alameda_2018_composite_voter_guide", "hayward")
    barbara = _candidate(roster, hayward["target_id"], "barbara halliday")
    mark = _candidate(roster, hayward["target_id"], "mark salinas")
    evidence = [
        _evidence_row("halliday-to-mark", hayward, mark, "http://www.barbarahalliday.com"),
        _evidence_row("salinas-to-halliday", hayward, barbara, "http://www.salinasforhayward.com"),
    ]
    contexts = [
        _context_row(
            "halliday-to-mark", hayward["source_id"],
            "http://www.barbarahalliday.com", "page:65:annotation:1", "Barbara Halliday",
        ),
        _context_row(
            "salinas-to-halliday", hayward["source_id"],
            "http://www.salinasforhayward.com", "page:65:annotation:2", "Mark Salinas",
        ),
    ]
    decisions, _ = _audit_subset(
        targets, roster, [hayward["target_id"]], evidence, contexts
    )
    assert set(decisions["identity_attribution_status"]) == {"reject_identity"}
    assert set(decisions["identity_attribution_reason"]) == {
        "context_names_different_contest_candidate"
    }


def test_cbs_direct_anchor_text_supports_only_the_named_candidate() -> None:
    targets, roster = _contract()
    cbs = _target(targets, "minneapolis_2013_cbs_candidate_websites", "minneapolis")
    cases = [
        ("betsy", "betsy hodges", "http://www.betsyhodges.org/", "anchor:18"),
        ("don", "don samuels", "http://samuelsformayor.com/", "anchor:30"),
        ("cam", "cam winton", "http://www.wintonformayor.org/", "anchor:36"),
    ]
    evidence = []
    contexts = []
    for key, name, url, locator in cases:
        candidate = _candidate(roster, cbs["target_id"], name)
        evidence.append(_evidence_row(key, cbs, candidate, url))
        contexts.append(
            _context_row(key, cbs["source_id"], url, locator, name.title())
        )
    decisions, summary = _audit_subset(
        targets, roster, [cbs["target_id"]], evidence, contexts
    )
    assert set(decisions["identity_attribution_status"]) == {"identity_supported"}
    assert set(decisions["identity_attribution_reason"]) == {
        "context_exactly_names_assigned_candidate"
    }
    assert not decisions["claim_approved_by_this_audit"].any()
    assert summary["identity_supported"] == 3
    assert summary["claim_approved_rows"] == 0


def test_explicit_nickname_and_name_order_variants_are_supported() -> None:
    targets, roster = _contract()
    cbs = _target(targets, "minneapolis_2013_cbs_candidate_websites", "minneapolis")
    cases = [
        ("ole", "oloveuse 'ole' savior", "Ole Savior", "http://www.olesavior.org/"),
        (
            "abdul",
            "abdul 'the rock' rahaman",
            'Abdul M. Rahaman "The Rock"',
            "http://abdul.example/",
        ),
    ]
    evidence = []
    contexts = []
    for key, roster_name, source_name, url in cases:
        candidate = _candidate(roster, cbs["target_id"], roster_name)
        evidence.append(_evidence_row(key, cbs, candidate, url))
        contexts.append(
            _context_row(key, cbs["source_id"], url, f"anchor:{key}", source_name)
        )

    decisions, _ = _audit_subset(
        targets, roster, [cbs["target_id"]], evidence, contexts
    )

    assert set(decisions["identity_attribution_status"]) == {"identity_supported"}


def test_shared_surname_does_not_resolve_conflicting_first_name() -> None:
    targets, roster = _contract()
    cbs = _target(targets, "minneapolis_2013_cbs_candidate_websites", "minneapolis")
    candidate = _candidate(roster, cbs["target_id"], "tony benjegerdes")
    evidence = [_evidence_row("tony", cbs, candidate, "http://mayortroy.com/")]
    contexts = [
        _context_row(
            "tony", cbs["source_id"], "http://mayortroy.com/", "anchor:144",
            "Troy Benjegerdes",
        )
    ]

    decisions, _ = _audit_subset(
        targets, roster, [cbs["target_id"]], evidence, contexts
    )

    assert decisions.iloc[0]["identity_attribution_status"] == "match_unresolved"


@pytest.mark.parametrize(
    ("kind", "nearest", "runner_up", "reason"),
    [
        ("page_context", "", "", "page_level_association_ambiguous"),
        ("pdf_annotation_nearest_name", "4", "9", "nearest_name_geometry_ambiguous"),
    ],
)
def test_page_or_close_nearest_name_association_is_match_unresolved(
    kind: str, nearest: str, runner_up: str, reason: str
) -> None:
    targets, roster = _contract()
    cbs = _target(targets, "minneapolis_2013_cbs_candidate_websites", "minneapolis")
    candidate = _candidate(roster, cbs["target_id"], "mark anderson")
    evidence = [_evidence_row("ambiguous", cbs, candidate, "http://mark.example/")]
    contexts = [
        _context_row(
            "ambiguous", cbs["source_id"], "http://mark.example/", "page:1",
            "Mark Anderson", kind=kind, nearest_distance=nearest,
            runner_up_distance=runner_up,
        )
    ]
    decisions, _ = _audit_subset(
        targets, roster, [cbs["target_id"]], evidence, contexts
    )
    assert decisions.iloc[0]["identity_attribution_status"] == "match_unresolved"
    assert decisions.iloc[0]["identity_attribution_reason"] == reason
    assert decisions.iloc[0]["required_attempt_status"] == "match_unresolved"


def test_same_url_for_multiple_candidates_requires_explicit_separate_evidence() -> None:
    targets, roster = _contract()
    hayward = _target(targets, "alameda_2018_composite_voter_guide", "hayward")
    candidates = [
        _candidate(roster, hayward["target_id"], "barbara halliday"),
        _candidate(roster, hayward["target_id"], "mark salinas"),
    ]
    shared_url = "https://shared-campaign.example/"
    evidence = [
        _evidence_row(f"shared-{index}", hayward, candidate, shared_url)
        for index, candidate in enumerate(candidates)
    ]
    contexts = [
        _context_row(
            f"shared-{index}", hayward["source_id"], shared_url,
            f"page:65:annotation:{index}", candidate["full_name"].title(),
        )
        for index, candidate in enumerate(candidates)
    ]
    decisions, _ = _audit_subset(
        targets, roster, [hayward["target_id"]], evidence, contexts
    )
    assert set(decisions["identity_attribution_status"]) == {"reject_identity"}
    assert set(decisions["identity_attribution_reason"]) == {
        "url_assigned_multiple_candidates_without_explicit_separate_evidence"
    }
    with pytest.raises(IdentityAttributionError, match="gate failed"):
        require_resolved_identity_attribution(decisions)


def test_duplicate_url_can_pass_attribution_only_with_distinct_explicit_contexts() -> None:
    targets, roster = _contract()
    hayward = _target(targets, "alameda_2018_composite_voter_guide", "hayward")
    candidates = [
        _candidate(roster, hayward["target_id"], "barbara halliday"),
        _candidate(roster, hayward["target_id"], "mark salinas"),
    ]
    shared_url = "https://shared-slate.example/"
    evidence = [
        _evidence_row(f"slate-{index}", hayward, candidate, shared_url)
        for index, candidate in enumerate(candidates)
    ]
    contexts = [
        _context_row(
            f"slate-{index}", hayward["source_id"], shared_url,
            f"page:65:annotation:{index}", candidate["full_name"].title(),
            explicit=True, note="Distinct candidate-labeled anchor to the shared slate site",
        )
        for index, candidate in enumerate(candidates)
    ]
    decisions, summary = _audit_subset(
        targets, roster, [hayward["target_id"]], evidence, contexts
    )
    assert set(decisions["identity_attribution_status"]) == {"identity_supported"}
    assert summary["claim_approved_rows"] == 0
    require_resolved_identity_attribution(decisions)
