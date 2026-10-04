from __future__ import annotations

import copy
import json
import itertools
import shutil
import subprocess
from pathlib import Path

import pytest
import scripts.import_candidate_endorsement_stage_b_residual_handcode as importer

from scripts.build_candidate_endorsement_stage_b_residual_handcode import (
    build_task, derive_residuals, load_frozen_failed_run, render_html,
)
from scripts.import_candidate_endorsement_stage_b_residual_handcode import (
    FROZEN_HTML_SHA256, FROZEN_TASK_SHA256, validate_results, validate_task,
)

REPO = Path(__file__).resolve().parents[1]
EXPORT = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_b_preflight_20260826e"
OUTPUT = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_b_preflight_hive_output_20260826e_21470382"
TASK = REPO / "build/candidate_endorsement_stage_b_residual_21470382/task.json"
HTML = TASK.parent / "candidate_endorsement_stage_b_residual_21470382.html"
AUDIT = REPO / "quality_reports/candidate_endorsement_line_ledger_stage_b_preflight_failed_21470382_2026-08-26.json"


def test_failed_run_derives_exact_immutable_23_field_ledger() -> None:
    manifest, batches, rows = load_frozen_failed_run(EXPORT, OUTPUT)
    assert manifest["exported_records"] == 152
    residuals = derive_residuals(batches, rows)
    assert len(residuals) == 23
    assert len({row["batch_id"] for row in residuals}) == 13
    assert {row["field"] for row in residuals} == {
        "endorser_name", "title", "affiliation",
    }
    task = build_task(EXPORT, OUTPUT)
    assert len(task["items"]) == 23
    assert all("answers" not in item for item in task["items"])
    assert task["provenance"]["ordered_residual_ids_sha256"] == (
        "4b32616e6e6323be09b96cbbd62474c3c323d913dfb40080596f2dac359c6955"
    )
    assert all(item["allowed_values"] for item in task["items"])
    assert all(
        None not in item["allowed_values"]
        for item in task["items"]
        if item["residual"]["field"] == "endorser_name"
    )
    assert all(
        item["residual"]["field"] != "endorser_name"
        for item in task["items"] if None in item["allowed_values"]
    )
    audit = json.loads(AUDIT.read_text())
    assert audit["technical_census"]["technical_pass_count"] == 139
    assert audit["technical_census"]["technical_failure_count"] == 13
    assert audit["residual_plan"]["nonexact_field_count"] == 23
    assert audit["residual_plan"]["decisions_filled"] is False


def test_frozen_task_html_and_results_are_fail_closed() -> None:
    task = validate_task(TASK, HTML, EXPORT, OUTPUT)
    assert FROZEN_TASK_SHA256
    assert FROZEN_HTML_SHA256
    responses = [{
        "item_id": item["id"], "status": "complete",
        "answers": {"exact_source_value": item["allowed_values"][0], "notes": ""},
    } for item in task["items"]]
    results = {
        "task_id": task["task_id"], "title": task["title"],
        "format": "handcode-results-v1",
        "exported_at": "2026-08-26T00:00:00.000Z", "n_items": 23,
        "n_complete": 23, "responses": responses,
    }
    assert len(validate_results(task, results)) == 23

    arbitrary = copy.deepcopy(results)
    arbitrary["responses"][0]["answers"]["exact_source_value"] = "a"
    with pytest.raises(ValueError, match="enumerated semantic option"):
        validate_results(task, arbitrary)

    reversed_results = copy.deepcopy(results)
    reversed_results["responses"].reverse()
    with pytest.raises(ValueError, match="response order"):
        validate_results(task, reversed_results)

    float_count = copy.deepcopy(results)
    float_count["n_items"] = 23.0
    with pytest.raises(ValueError, match="coverage"):
        validate_results(task, float_count)

    bad_timestamp = copy.deepcopy(results)
    bad_timestamp["exported_at"] = {"not": "a string"}
    with pytest.raises(ValueError, match="coverage"):
        validate_results(task, bad_timestamp)

    malformed_timestamp = copy.deepcopy(results)
    malformed_timestamp["exported_at"] = "x"
    with pytest.raises(ValueError, match="coverage"):
        validate_results(task, malformed_timestamp)
    for impossible in (
        "2026-99-99T99:99:99.999Z", "2026-02-29T00:00:00.000Z",
    ):
        impossible_timestamp = copy.deepcopy(results)
        impossible_timestamp["exported_at"] = impossible
        with pytest.raises(ValueError, match="coverage"):
            validate_results(task, impossible_timestamp)

    bad_notes = copy.deepcopy(results)
    bad_notes["responses"][0]["answers"]["notes"] = []
    with pytest.raises(ValueError, match="answer shape"):
        validate_results(task, bad_notes)

    missing = copy.deepcopy(results)
    missing["responses"].pop()
    missing["n_complete"] = 22
    with pytest.raises(ValueError, match="coverage"):
        validate_results(task, missing)

    null_name = copy.deepcopy(results)
    name_item = next(
        item for item in task["items"]
        if item["residual"]["field"] == "endorser_name"
    )
    response = next(
        row for row in null_name["responses"] if row["item_id"] == name_item["id"]
    )
    response["answers"]["exact_source_value"] = None
    with pytest.raises(ValueError, match="enumerated semantic option"):
        validate_results(task, null_name)

    crossed = copy.deepcopy(results)
    title_item = next(
        item for item in task["items"]
        if item["residual"]["original_value"] == "Former Chief Scientist, Founder"
    )
    affiliation_item = next(
        item for item in task["items"]
        if item["residual"]["original_value"] == "Amazon, Social Data Lab"
    )
    next(row for row in crossed["responses"] if row["item_id"] == title_item["id"])[
        "answers"
    ]["exact_source_value"] = "Former Chief Scientist"
    next(row for row in crossed["responses"] if row["item_id"] == affiliation_item["id"])[
        "answers"
    ]["exact_source_value"] = "Social Data Lab"
    with pytest.raises(ValueError, match="paired semantic choices"):
        validate_results(task, crossed)


def test_item_specific_semantic_options_and_ui_schema() -> None:
    task = build_task(EXPORT, OUTPUT)
    by_original = {
        item["residual"]["original_value"]: item for item in task["items"]
    }
    assert by_original["Daneya Esgar"]["allowed_values"] == ["Daneya Esgar"]
    assert "Daneya Esgar from Pueblo" not in by_original["Daneya Esgar"]["allowed_values"]
    assert by_original["City Councilor"]["allowed_values"] == ["Councilor"]
    assert by_original["Former City Councilor"]["allowed_values"] == [
        "Former Madras City Councilor"
    ]
    assert by_original["Action"]["allowed_values"] == ["314 Action"]
    assert by_original["Jeff Cristina"]["allowed_values"] == ["Jeff\u00a0 Cristina"]
    html = render_html(task)
    assert "exact_source_value:item.allowed_values" in html
    assert "title:task.title" in html
    assert "notes:saved[item.id].notes||''" in html
    assert "task.provenance.repair_validation_contract_sha256" in html
    assert "task.provenance.ordered_residual_ids_sha256" in html
    assert "task.task_id+':responses'" not in html
    script = html.split("<script>", 1)[1].split("</script>", 1)[0]
    subprocess.run(
        ["node", "--check"], input=script, text=True, check=True,
        capture_output=True,
    )


def test_failed_output_inventory_is_exactly_four_files(tmp_path: Path) -> None:
    copied = tmp_path / "output"
    shutil.copytree(OUTPUT, copied)
    (copied / "unexpected.json").write_text("{}\n")
    with pytest.raises(ValueError, match="exact four-file inventory"):
        load_frozen_failed_run(EXPORT, copied)


def test_connector_accounting_accepts_semantic_name_and_fields() -> None:
    from src.candidate_endorsement_line_ledger import (
        _name_is_maximal_delimited_mention, validate_record_extraction_output,
    )
    assert _name_is_maximal_delimited_mention(
        "Representative Daneya Esgar from Pueblo",
        "Daneya Esgar",
        accounted_prefixes=("Representative", None),
        accounted_suffixes=("Pueblo", None),
        entity_type="individual",
    )
    assert not _name_is_maximal_delimited_mention(
        "Representative Daneya Esgar from Pueblo",
        "Daneya Esgar",
        accounted_prefixes=("Representative", None),
        accounted_suffixes=(None, None),
        entity_type="individual",
    )
    assert not _name_is_maximal_delimited_mention(
        "The City of Hope Foundation", "Foundation",
        accounted_prefixes=("Hope", None), accounted_suffixes=(None, None),
        entity_type="organization",
    )
    assert not _name_is_maximal_delimited_mention(
        "National Association of Nurses United", "Nurses United",
        accounted_prefixes=("National Association",),
        entity_type="organization",
    )
    task = build_task(EXPORT, OUTPUT)
    daneya = next(
        item["residual"] for item in task["items"]
        if item["residual"]["original_value"] == "Daneya Esgar"
    )
    _, batches, rows = load_frozen_failed_run(EXPORT, OUTPUT)
    batch = next(row for row in batches if row["batch_id"] == daneya["batch_id"])
    output = copy.deepcopy(next(
        row["model_output"] for row in rows if row["batch_id"] == daneya["batch_id"]
    ))
    record = output[daneya["slot"]]["records"][daneya["record_index"]]
    record["endorser_name"] = "Daneya Esgar"
    assert record["title"] == "Representative"
    assert record["jurisdiction"] == "Pueblo"
    validation = validate_record_extraction_output(batch, output)
    assert f"{daneya['slot']}:record_{daneya['record_index']}:name_not_exact_in_block" not in validation["errors"]


def test_every_residual_batch_has_an_enumerated_complete_repair() -> None:
    from src.candidate_endorsement_line_ledger import validate_record_extraction_output
    task = build_task(EXPORT, OUTPUT)
    _, batches, rows = load_frozen_failed_run(EXPORT, OUTPUT)
    batch_by_id = {batch["batch_id"]: batch for batch in batches}
    row_by_id = {row["batch_id"]: row for row in rows}
    grouped: dict[str, list[dict[str, object]]] = {}
    for item in task["items"]:
        grouped.setdefault(item["residual"]["batch_id"], []).append(item)
    for batch_id, items in grouped.items():
        found = False
        for values in itertools.product(*(item["allowed_values"] for item in items)):
            output = copy.deepcopy(row_by_id[batch_id]["model_output"])
            for item, value in zip(items, values, strict=True):
                residual = item["residual"]
                output[residual["slot"]]["records"][residual["record_index"]][
                    residual["field"]
                ] = value
            validation = validate_record_extraction_output(batch_by_id[batch_id], output)
            if validation["schema_valid"] and validation["coverage_complete"]:
                found = True
                break
        assert found, batch_id


def test_task_tamper_rejected(tmp_path: Path) -> None:
    attacked = json.loads(TASK.read_text())
    attacked["items"][0]["residual"]["original_value"] = "tampered"
    path = tmp_path / "task.json"
    path.write_text(json.dumps(attacked, indent=2, sort_keys=True) + "\n")
    with pytest.raises(ValueError, match="task bytes differ"):
        validate_task(path, HTML, EXPORT, OUTPUT)


def test_merge_applies_only_authenticated_field_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = {"items": [{"id": "residual", "residual": {
        "batch_id": "batch", "slot": "slot_00", "record_index": 0,
        "field": "title",
    }}]}
    batches = [{"batch_id": "batch"}]
    rows = [{"batch_id": "batch", "model_output": {
        "slot_00": {"records": [{"title": "old"}]}
    }}]
    monkeypatch.setattr(importer, "validate_record_extraction_output", lambda *_: {
        "schema_valid": True, "coverage_complete": True,
    })
    monkeypatch.setattr(importer, "align_record_extraction_output", lambda *_: {
        "public_coverage_complete": True, "withheld_record_count": 0,
    })
    merged = importer.merge_repairs(
        task, {"residual": "Exact title"}, batches, rows
    )
    assert merged["repaired_outputs"]["batch"]["slot_00"]["records"][0][
        "title"
    ] == "Exact title"
    assert rows[0]["model_output"]["slot_00"]["records"][0]["title"] == "old"
