from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from scripts.build_municipal_curated_priority_2010_2021 import (
    DEFAULT_CANDIDATE_SAMPLE,
    DEFAULT_CONTEST_SAMPLE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SOURCE_SPECS,
    DEFAULT_UNRESOLVED,
    PriorityAuditError,
    build_priority_audit,
    load_source_specs,
    write_outputs,
)


def _real_results() -> tuple[dict[str, object], list[dict]]:
    sources = load_source_specs(DEFAULT_SOURCE_SPECS)
    results = build_priority_audit(
        pd.read_csv(DEFAULT_UNRESOLVED),
        pd.read_csv(DEFAULT_CANDIDATE_SAMPLE),
        pd.read_csv(DEFAULT_CONTEST_SAMPLE),
        sources,
    )
    return results, sources


def test_real_priority_audit_intersects_fixed_screen_without_duplicate_targets() -> None:
    results, sources = _real_results()
    summary = results["summary"]
    targets = results["source_targets"]
    wave2 = results["wave2_priority_targets"]
    candidate_targets = results["wave2_candidate_targets"]
    inventory = results["source_inventory"]

    assert summary["unresolved_candidate_years"] == 2669
    assert summary["candidate_screen_rows"] == 480
    assert summary["complete_contest_screen_rows"] == 74
    assert summary["run_now_sources"] == 10
    assert summary["hold_sources"] == 3
    assert summary["candidate_screen_targeted_unique_candidate_years"] == 61
    assert summary["complete_contest_screen_targeted_unique_candidate_years"] == 8
    assert summary["wave2_screen_target_rows"] == 47
    assert summary["access_cost_usd"] == 0
    assert summary["candidate_screen_gap_over_20_percent"] is True
    assert summary["complete_contest_screen_gap_over_20_percent"] is True

    assert len(inventory) == len(sources) == 13
    assert inventory["source_id"].is_unique
    assert targets["target_id"].is_unique
    assert wave2["target_id"].is_unique
    assert wave2["screen_target"].all()
    assert ((wave2["candidate_screen_n"] > 0) | (wave2["complete_contest_screen_n"] > 0)).all()
    assert not candidate_targets.duplicated(["target_id", "candidate_year_id"]).any()
    assert candidate_targets["in_candidate_screen"].any()
    assert candidate_targets["in_complete_contest_screen"].any()
    expected_rows = int(wave2["unresolved_candidate_n"].sum())
    assert len(candidate_targets) == expected_rows
    assert candidate_targets.groupby("target_id").size().to_dict() == (
        wave2.set_index("target_id")["unresolved_candidate_n"].astype(int).to_dict()
    )


def test_exact_urls_provenance_and_downloaded_alameda_fixture_are_frozen() -> None:
    results, _ = _real_results()
    inventory = results["source_inventory"].set_index("source_id")
    alameda_url = (
        "https://acvote.alamedacountyca.gov/acvote-assets/02_election_information/"
        "PDFs/20181106/en/Composite%20Voter%20Information%20Guide/"
        "Composite%20Voter%20Information%20Guide.pdf"
    )
    assert inventory.loc["alameda_2018_composite_voter_guide", "exact_source_url"] == alameda_url
    assert inventory.loc["minneapolis_2013_cbs_candidate_websites", "exact_source_url"] == (
        "https://www.cbsnews.com/minnesota/news/minneapolis-mayoral-election-candidates/"
    )
    for row in inventory.itertuples(index=False):
        assert row.exact_source_url.startswith(("http://", "https://"))
        assert json.loads(row.entry_urls_json)
        assert json.loads(row.provenance_json)
        assert row.wayback_route.startswith("https://web.archive.org/")

    fixture = DEFAULT_OUTPUT_DIR / "alameda_2018_composite_voter_guide.pdf"
    assert fixture.exists()
    assert fixture.stat().st_size == 10899549
    digest = hashlib.sha256(fixture.read_bytes()).hexdigest()
    assert digest == "7f3d2b2181221f01ad24f5d4543988f47c7d111939aaae06a2bd5d3f3dc46e7f"
    assert fixture.read_bytes()[:4] == b"%PDF"


def test_output_manifest_hashes_every_generated_artifact(tmp_path: Path) -> None:
    results, _ = _real_results()
    manifest = write_outputs(
        results,
        tmp_path,
        {
            "unresolved_frame": DEFAULT_UNRESOLVED,
            "candidate_screen": DEFAULT_CANDIDATE_SAMPLE,
            "complete_contest_screen": DEFAULT_CONTEST_SAMPLE,
            "source_specs": DEFAULT_SOURCE_SPECS,
        },
    )
    assert manifest["schema_version"] == "municipal-curated-priority-output-manifest-v1"
    assert manifest["inputs"]["candidate_screen"]["sha256"] == (
        "646e2688005ca7c4f0f20ed027aeb7709351adbdfa9864af26e143f1caf3eb19"
    )
    for record in manifest["outputs"]:
        path = tmp_path / record["path"]
        assert path.exists()
        assert path.stat().st_size == record["bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == record["sha256"]


def test_source_specs_reject_paid_access(tmp_path: Path) -> None:
    payload = json.loads(DEFAULT_SOURCE_SPECS.read_text(encoding="utf-8"))
    payload["sources"][0]["access_cost_usd"] = 1
    path = tmp_path / "source_specs.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    try:
        load_source_specs(path)
    except PriorityAuditError as error:
        assert "Paid source" in str(error)
    else:
        raise AssertionError("Paid source specification was not rejected")
