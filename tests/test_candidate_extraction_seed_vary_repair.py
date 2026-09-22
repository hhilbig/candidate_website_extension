"""Focused tests for the seed-vary repair of truncated one-stage windows.

The repair keeps the prompt, schema, model, and contract byte-identical. It
changes only the seed, and it records the seed, the attempt index, and the
accepted outcome in a repair attestation.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from pathlib import Path

import pytest

from scripts.hive.extract_candidate_pages_vllm import (
    MODEL_REVISION,
    REPAIR_ATTEMPT_INDEX,
    REPAIR_MANIFEST_VERSION,
    chunk_is_complete,
    get_extraction_contract,
    load_repair_targets,
    main,
    repair_chunk_is_complete,
    repair_manifest_payload,
    truncated_row_is_terminal,
    write_repair_manifest,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "truncated_window_fixture.jsonl.gz"

from scripts.build_candidate_extraction_seed_vary_repair_targets import (  # noqa: E402
    build_target_payload,
    build_target_payload_from_repair,
    input_chunk_name,
)

DOCUMENT_A = "a" * 64
DOCUMENT_B = "b" * 64
CHUNK = "chunk_00001.jsonl.gz"


def write_gzip_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def target_list(tmp_path: Path, targets: list[dict]) -> Path:
    path = tmp_path / "repair_targets.json"
    path.write_text(
        json.dumps({"repair_manifest_version": REPAIR_MANIFEST_VERSION,
                    "targets": targets}),
        encoding="utf-8",
    )
    return path


def repair_namespace(tmp_path: Path, *, seed: int, repair_seed: int) -> argparse.Namespace:
    return argparse.Namespace(
        schema_version="2.1",
        seed=seed,
        max_model_len=32_768,
        max_output_tokens=24_576,
        compact_repair=False,
        chunks_dir=tmp_path / "chunks",
        output_dir=tmp_path / "output",
    )


def test_target_list_groups_windows_by_chunk(tmp_path: Path) -> None:
    path = target_list(
        tmp_path,
        [
            {"chunk": CHUNK, "document_id": DOCUMENT_A},
            {"chunk": CHUNK, "document_id": DOCUMENT_B},
            {"chunk": "chunk_00002.jsonl.gz", "document_id": DOCUMENT_A},
        ],
    )
    targets = load_repair_targets(path)
    assert targets[CHUNK] == [DOCUMENT_A, DOCUMENT_B]
    assert targets["chunk_00002.jsonl.gz"] == [DOCUMENT_A]


def test_target_list_rejects_an_unsafe_chunk_name(tmp_path: Path) -> None:
    path = target_list(tmp_path, [{"chunk": "../chunk_1.jsonl.gz", "document_id": DOCUMENT_A}])
    with pytest.raises(ValueError, match="unsafe repair target chunk name"):
        load_repair_targets(path)


def test_target_list_rejects_an_output_file_name(tmp_path: Path) -> None:
    """The target list names input chunks, never writer output files."""
    path = target_list(
        tmp_path, [{"chunk": "chunk_00056.out.jsonl.gz", "document_id": DOCUMENT_A}]
    )
    with pytest.raises(ValueError, match="unsafe repair target chunk name"):
        load_repair_targets(path)


def test_target_list_rejects_a_non_digest_document_id(tmp_path: Path) -> None:
    path = target_list(tmp_path, [{"chunk": CHUNK, "document_id": "not-a-digest"}])
    with pytest.raises(ValueError, match="not a sha256 digest"):
        load_repair_targets(path)


def test_target_list_rejects_a_repeated_window(tmp_path: Path) -> None:
    path = target_list(
        tmp_path,
        [
            {"chunk": CHUNK, "document_id": DOCUMENT_A},
            {"chunk": CHUNK, "document_id": DOCUMENT_A},
        ],
    )
    with pytest.raises(ValueError, match="duplicate repair target"):
        load_repair_targets(path)


def test_target_list_rejects_an_empty_list(tmp_path: Path) -> None:
    path = target_list(tmp_path, [])
    with pytest.raises(ValueError, match="holds no targets"):
        load_repair_targets(path)


def identity_target_list(
    tmp_path: Path, *, repair_seed: int, attempt_index: int
) -> Path:
    """Write a target list that declares the attempt identity it was built for."""
    path = tmp_path / "repair_targets_identity.json"
    path.write_text(
        json.dumps(
            {
                "repair_manifest_version": REPAIR_MANIFEST_VERSION,
                "primary_seed": 20260824,
                "repair_seed": repair_seed,
                "attempt_index": attempt_index,
                "targets": [{"chunk": CHUNK, "document_id": DOCUMENT_A}],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_target_list_accepts_the_identity_it_was_built_for(tmp_path: Path) -> None:
    path = identity_target_list(tmp_path, repair_seed=20260918, attempt_index=3)
    targets = load_repair_targets(
        path, expected_repair_seed=20260918, expected_attempt_index=3
    )
    assert targets[CHUNK] == [DOCUMENT_A]


def test_target_list_refuses_a_seed_the_run_does_not_use(tmp_path: Path) -> None:
    path = identity_target_list(tmp_path, repair_seed=20260918, attempt_index=3)
    with pytest.raises(ValueError, match="declares seed 20260918"):
        load_repair_targets(
            path, expected_repair_seed=20260917, expected_attempt_index=3
        )


def test_target_list_refuses_an_attempt_the_run_does_not_use(tmp_path: Path) -> None:
    """Attempt 3's list must not be inferred under attempt 2's identity.

    The wrapper forwards the attempt index, and the attestation records it. A
    silent mismatch would bind attempt 3's rows to attempt 2's declared seed.
    """
    path = identity_target_list(tmp_path, repair_seed=20260918, attempt_index=3)
    with pytest.raises(ValueError, match="declares attempt 3"):
        load_repair_targets(
            path, expected_repair_seed=20260918, expected_attempt_index=2
        )


def test_wrapper_forwards_the_repair_attempt_index() -> None:
    """The wrapper must pass REPAIR_ATTEMPT_INDEX through to the runner.

    Without this line the runner keeps its default of attempt 2, so a third
    attempt records the wrong attempt index and the wrong seed identity.
    """
    wrapper = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "hive"
        / "run_candidate_extraction_pilot.sbatch"
    ).read_text()
    assert 'if [[ -n "${REPAIR_ATTEMPT_INDEX:-}" ]]; then' in wrapper
    assert (
        'SEED_VARY_ARGS+=(--repair-attempt-index "$REPAIR_ATTEMPT_INDEX")' in wrapper
    )


def test_repair_output_is_complete_only_for_target_rows_that_stop(tmp_path: Path) -> None:
    output = tmp_path / CHUNK.replace(".jsonl.gz", ".out.jsonl.gz")
    write_gzip_jsonl(
        output,
        [
            {"document_id": DOCUMENT_A, "finish_reason": "stop"},
            {"document_id": DOCUMENT_B, "finish_reason": "length"},
        ],
    )
    assert repair_chunk_is_complete(output, [DOCUMENT_A, DOCUMENT_B]) is False
    assert repair_chunk_is_complete(output, [DOCUMENT_A, DOCUMENT_B, "c" * 64]) is False
    write_gzip_jsonl(output, [{"document_id": DOCUMENT_A, "finish_reason": "stop"}])
    assert repair_chunk_is_complete(output, [DOCUMENT_A]) is True
    assert repair_chunk_is_complete(tmp_path / "absent.out.jsonl.gz", [DOCUMENT_A]) is False


def test_repair_output_matching_is_order_free(tmp_path: Path) -> None:
    """The runner writes rows in chunk order and the targets are sorted by id.

    A chunk with two targets is written in the order the input chunk holds them,
    so an ordered comparison would refuse a correct output and stop the whole run.
    """
    output = tmp_path / CHUNK.replace(".jsonl.gz", ".out.jsonl.gz")
    write_gzip_jsonl(
        output,
        [
            {"document_id": DOCUMENT_A, "finish_reason": "stop"},
            {"document_id": DOCUMENT_B, "finish_reason": "stop"},
        ],
    )
    assert repair_chunk_is_complete(output, [DOCUMENT_B, DOCUMENT_A]) is True
    assert repair_chunk_is_complete(output, [DOCUMENT_A, DOCUMENT_B]) is True
    assert repair_chunk_is_complete(output, [DOCUMENT_A]) is False
    # A repeated row is still refused, because the repeat lengthens the output.
    write_gzip_jsonl(
        output,
        [
            {"document_id": DOCUMENT_A, "finish_reason": "stop"},
            {"document_id": DOCUMENT_A, "finish_reason": "stop"},
        ],
    )
    assert repair_chunk_is_complete(output, [DOCUMENT_A]) is False


def test_repair_scan_treats_a_cap_limited_repair_row_as_terminal(tmp_path: Path) -> None:
    """A window that reaches the cap again must not block the chunks behind it.

    The real fixture is one cap-limited row. The repair run writes the same row
    shape under the repair seed, so the repair scan accepts it as terminal and
    the manifest records it as unrepaired.
    """
    _, row = load_truncated_fixture()
    output = tmp_path / CHUNK.replace(".jsonl.gz", ".out.jsonl.gz")
    write_gzip_jsonl(output, [row])
    assert repair_chunk_is_complete(output, [row["document_id"]]) is False
    assert (
        repair_chunk_is_complete(
            output, [row["document_id"]], accept_truncated_rows=True
        )
        is True
    )

    # A row that carries a parsed payload is damaged rather than cap-limited.
    damaged = dict(row, extraction={"facts": []}, parse_status="parsed")
    write_gzip_jsonl(output, [damaged])
    assert (
        repair_chunk_is_complete(
            output, [row["document_id"]], accept_truncated_rows=True
        )
        is False
    )


def test_main_resumes_a_repair_run_whose_output_reached_the_cap_again(
    tmp_path: Path, monkeypatch
) -> None:
    """A timed-out repair run must be resumable without touching its output.

    The repair output holds one cap-limited row. The scan accepts it as terminal,
    so the run reaches its end and publishes the repair attestation instead of
    raising over an existing output.
    """
    input_row, output_row = load_truncated_fixture()
    chunks_dir = tmp_path / "chunks"
    output_dir = tmp_path / "output"
    model_dir = tmp_path / MODEL_REVISION
    model_dir.mkdir(parents=True)
    write_gzip_jsonl(chunks_dir / CHUNK, [input_row])
    output_path = output_dir / CHUNK.replace(".jsonl.gz", ".out.jsonl.gz")
    write_gzip_jsonl(output_path, [output_row])
    before = output_path.read_bytes()
    target_path = target_list(
        tmp_path, [{"chunk": CHUNK, "document_id": output_row["document_id"]}]
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "extract_candidate_pages_vllm.py",
            "--chunks-dir",
            str(chunks_dir),
            "--output-dir",
            str(output_dir),
            "--model-path",
            str(model_dir),
            "--schema-version",
            "2.1",
            "--seed",
            "20260824",
            "--max-output-tokens",
            "24576",
            "--repair-targets",
            str(target_path),
            "--repair-seed",
            "20260917",
        ],
    )
    assert main() == 0
    assert output_path.read_bytes() == before
    manifest = json.loads(
        (output_dir / "repair_manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["target_count"] == 1
    assert manifest["repaired_count"] == 0
    assert manifest["unrepaired_count"] == 1
    assert manifest["all_targets_repaired"] is False


def test_repair_manifest_records_seeds_attempt_and_outcome(tmp_path: Path) -> None:
    chunks_dir = tmp_path / "chunks"
    output_dir = tmp_path / "output"
    chunks_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)
    chunk_path = chunks_dir / CHUNK
    write_gzip_jsonl(
        chunk_path,
        [
            {"document_id": DOCUMENT_A, "route": "issues"},
            {"document_id": DOCUMENT_B, "route": "biography"},
        ],
    )
    write_gzip_jsonl(
        output_dir / CHUNK.replace(".jsonl.gz", ".out.jsonl.gz"),
        [
            {
                "document_id": DOCUMENT_A,
                "route": "issues",
                "finish_reason": "stop",
                "parse_status": "parsed",
                "output_tokens": 1_200,
                "raw_output": "{\"issues\": []}",
                "generation_config_sha256": "f" * 64,
            },
            {
                "document_id": DOCUMENT_B,
                "route": "biography",
                "finish_reason": "length",
                "parse_status": "output_truncated",
                "output_tokens": 24_576,
                "raw_output": "{\"biography\": \"",
                "generation_config_sha256": "f" * 64,
            },
        ],
    )
    args = repair_namespace(tmp_path, seed=20260917, repair_seed=20260917)
    args.chunks_dir = chunks_dir
    args.output_dir = output_dir
    payload = repair_manifest_payload(
        args,
        get_extraction_contract("2.1"),
        {CHUNK: [DOCUMENT_A, DOCUMENT_B]},
        primary_seed=20260824,
        repair_seed=20260917,
        primary_generation_hash="e" * 64,
        repair_generation_hash="f" * 64,
        chunks_dir=chunks_dir,
        output_dir=output_dir,
    )
    assert payload["repair_manifest_version"] == REPAIR_MANIFEST_VERSION
    assert payload["primary_seed"] == 20260824
    assert payload["repair_seed"] == 20260917
    assert payload["attempt_index"] == REPAIR_ATTEMPT_INDEX == 2
    assert payload["primary_generation_config_sha256"] == "e" * 64
    assert payload["repair_generation_config_sha256"] == "f" * 64
    assert payload["target_count"] == 2
    assert payload["repaired_count"] == 1
    assert payload["unrepaired_count"] == 1
    assert payload["all_targets_repaired"] is False
    by_id = {entry["document_id"]: entry for entry in payload["targets"]}
    assert by_id[DOCUMENT_A]["repaired"] is True
    assert by_id[DOCUMENT_B]["repaired"] is False
    assert by_id[DOCUMENT_B]["finish_reason"] == "length"
    assert by_id[DOCUMENT_B]["raw_output_sha256"] == hashlib.sha256(
        b"{\"biography\": \""
    ).hexdigest()
    assert by_id[DOCUMENT_A]["input_chunk_sha256"] == hashlib.sha256(
        chunk_path.read_bytes()
    ).hexdigest()
    written = write_repair_manifest(
        args,
        get_extraction_contract("2.1"),
        {CHUNK: [DOCUMENT_A, DOCUMENT_B]},
        primary_seed=20260824,
        repair_seed=20260917,
        primary_generation_hash="e" * 64,
        repair_generation_hash="f" * 64,
    )
    assert (output_dir / "repair_manifest.json").exists()
    assert written["target_count"] == 2


def test_repair_manifest_records_a_later_attempt_index(tmp_path: Path) -> None:
    """A third seed is attempt 3, and the payload must say so."""
    chunks_dir = tmp_path / "chunks"
    output_dir = tmp_path / "output"
    chunks_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)
    write_gzip_jsonl(chunks_dir / CHUNK, [{"document_id": DOCUMENT_A, "route": "issues"}])
    write_gzip_jsonl(
        output_dir / CHUNK.replace(".jsonl.gz", ".out.jsonl.gz"),
        [
            {
                "document_id": DOCUMENT_A,
                "route": "issues",
                "finish_reason": "stop",
                "parse_status": "parsed",
                "output_tokens": 900,
                "raw_output": "{\"issues\": []}",
                "generation_config_sha256": "f" * 64,
            }
        ],
    )
    args = repair_namespace(tmp_path, seed=20260918, repair_seed=20260918)
    args.chunks_dir = chunks_dir
    args.output_dir = output_dir
    payload = repair_manifest_payload(
        args,
        get_extraction_contract("2.1"),
        {CHUNK: [DOCUMENT_A]},
        primary_seed=20260824,
        repair_seed=20260918,
        primary_generation_hash="e" * 64,
        repair_generation_hash="f" * 64,
        chunks_dir=chunks_dir,
        output_dir=output_dir,
        attempt_index=3,
    )
    assert payload["attempt_index"] == 3
    assert payload["repair_seed"] == 20260918
    assert payload["primary_seed"] == 20260824
    assert payload["repaired_count"] == 1

    # The default stays at the first repair, so existing runs are unchanged.
    default_payload = repair_manifest_payload(
        args,
        get_extraction_contract("2.1"),
        {CHUNK: [DOCUMENT_A]},
        primary_seed=20260824,
        repair_seed=20260918,
        primary_generation_hash="e" * 64,
        repair_generation_hash="f" * 64,
        chunks_dir=chunks_dir,
        output_dir=output_dir,
    )
    assert default_payload["attempt_index"] == REPAIR_ATTEMPT_INDEX == 2


def test_main_refuses_an_attempt_index_below_the_first_repair(
    tmp_path: Path, monkeypatch
) -> None:
    path = target_list(tmp_path, [{"chunk": CHUNK, "document_id": DOCUMENT_A}])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "extract_candidate_pages_vllm.py",
            "--chunks-dir",
            str(tmp_path / "chunks"),
            "--output-dir",
            str(tmp_path / "output"),
            "--model-path",
            str(tmp_path / "model"),
            "--seed",
            "20260824",
            "--repair-targets",
            str(path),
            "--repair-seed",
            "20260918",
            "--repair-attempt-index",
            "1",
        ],
    )
    with pytest.raises(ValueError, match="attempt-index starts at 2"):
        main()


def test_main_requires_a_distinct_repair_seed(tmp_path: Path, monkeypatch) -> None:
    path = target_list(tmp_path, [{"chunk": CHUNK, "document_id": DOCUMENT_A}])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "extract_candidate_pages_vllm.py",
            "--chunks-dir",
            str(tmp_path / "chunks"),
            "--output-dir",
            str(tmp_path / "output"),
            "--model-path",
            str(tmp_path / "model"),
            "--seed",
            "20260824",
            "--repair-targets",
            str(path),
            "--repair-seed",
            "20260824",
        ],
    )
    with pytest.raises(ValueError, match="must differ from --seed"):
        main()


def test_main_requires_the_repair_seed_argument(tmp_path: Path, monkeypatch) -> None:
    path = target_list(tmp_path, [{"chunk": CHUNK, "document_id": DOCUMENT_A}])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "extract_candidate_pages_vllm.py",
            "--chunks-dir",
            str(tmp_path / "chunks"),
            "--output-dir",
            str(tmp_path / "output"),
            "--model-path",
            str(tmp_path / "model"),
            "--repair-targets",
            str(path),
        ],
    )
    with pytest.raises(ValueError, match="--repair-seed is required"):
        main()


def test_main_refuses_to_shard_a_repair_run(tmp_path: Path, monkeypatch) -> None:
    path = target_list(tmp_path, [{"chunk": CHUNK, "document_id": DOCUMENT_A}])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "extract_candidate_pages_vllm.py",
            "--chunks-dir",
            str(tmp_path / "chunks"),
            "--output-dir",
            str(tmp_path / "output"),
            "--model-path",
            str(tmp_path / "model"),
            "--repair-targets",
            str(path),
            "--repair-seed",
            "20260917",
            "--shard-index",
            "0",
            "--shard-count",
            "20",
        ],
    )
    with pytest.raises(ValueError, match="does not shard"):
        main()


def test_main_refuses_the_repair_without_schema_version_2_1(
    tmp_path: Path, monkeypatch
) -> None:
    path = target_list(tmp_path, [{"chunk": CHUNK, "document_id": DOCUMENT_A}])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "extract_candidate_pages_vllm.py",
            "--chunks-dir",
            str(tmp_path / "chunks"),
            "--output-dir",
            str(tmp_path / "output"),
            "--model-path",
            str(tmp_path / "model"),
            "--schema-version",
            "1.0",
            "--repair-targets",
            str(path),
            "--repair-seed",
            "20260917",
        ],
    )
    with pytest.raises(ValueError, match="schema version 2.1 only"):
        main()


def load_truncated_fixture() -> tuple[dict, dict]:
    """Read one real cap-limited window: its input row and its output row."""
    with gzip.open(FIXTURE_PATH, "rt", encoding="utf-8") as handle:
        payload = json.loads(handle.readline())
    return payload["input_row"], payload["output_row"]


def test_real_truncated_row_is_terminal() -> None:
    _, row = load_truncated_fixture()
    assert row["finish_reason"] == "length"
    assert row["output_tokens"] == row["max_output_tokens"] == 24_576
    assert truncated_row_is_terminal(row) is True

    stopped = dict(row, finish_reason="stop")
    assert truncated_row_is_terminal(stopped) is False

    parsed = dict(row, extraction={"facts": []})
    assert truncated_row_is_terminal(parsed) is False

    aligned = dict(row, aligned_extraction_sha256="f" * 64)
    assert truncated_row_is_terminal(aligned) is False

    wrong_hash = dict(row, raw_output_sha256="f" * 64)
    assert truncated_row_is_terminal(wrong_hash) is False

    under_cap = dict(row, output_tokens=24_575)
    assert truncated_row_is_terminal(under_cap) is False

    assert truncated_row_is_terminal({}) is False


def test_resume_scan_treats_a_published_truncated_chunk_as_complete(
    tmp_path: Path,
) -> None:
    input_row, output_row = load_truncated_fixture()
    chunks_dir = tmp_path / "chunks"
    output_dir = tmp_path / "output"
    input_path = chunks_dir / CHUNK
    output_path = output_dir / CHUNK.replace(".jsonl.gz", ".out.jsonl.gz")
    write_gzip_jsonl(input_path, [input_row])
    write_gzip_jsonl(output_path, [output_row])
    expected = {
        "schema_version": "2.1",
        "expected_generation_config_sha256": output_row["generation_config_sha256"],
        "expected_max_output_tokens": output_row["max_output_tokens"],
    }
    assert chunk_is_complete(input_path, output_path, **expected) is False
    assert chunk_is_complete(
        input_path, output_path, accept_truncated_rows=True, **expected
    ) is True

    # A row that carries a parsed payload is damaged rather than cap-limited, so
    # the resume scan keeps refusing it.
    damaged_path = output_dir / "damaged.out.jsonl.gz"
    write_gzip_jsonl(
        damaged_path,
        [dict(output_row, extraction={"facts": []}, parse_status="parsed")],
    )
    assert chunk_is_complete(
        input_path, damaged_path, accept_truncated_rows=True, **expected
    ) is False

    # The same data under another schema version stays incomplete, because the
    # allowance is defined for schema version 2.1 only.
    assert chunk_is_complete(
        input_path, output_path, schema_version="2.0", accept_truncated_rows=True
    ) is False


def test_target_builder_maps_output_names_to_input_chunks() -> None:
    assert input_chunk_name("chunk_00056.out.jsonl.gz") == "chunk_00056.jsonl.gz"
    assert input_chunk_name("chunk_00056.jsonl.gz") == "chunk_00056.jsonl.gz"
    audit = {
        "defects": {
            "output_truncated_finish_reason_length": [
                {"chunk": "chunk_00056.out.jsonl.gz", "document_id": DOCUMENT_A, "route": "issues"},
                {"chunk": "chunk_00119.out.jsonl.gz", "document_id": DOCUMENT_B, "route": "biography"},
            ]
        }
    }
    payload = build_target_payload(
        audit, primary_seed=20260824, repair_seed=20260917, audit_source="audit.json"
    )
    assert payload["target_count"] == 2
    assert payload["repair_seed"] == 20260917
    assert payload["attempt_index"] == 2
    assert {item["chunk"] for item in payload["targets"]} == {
        "chunk_00056.jsonl.gz",
        "chunk_00119.jsonl.gz",
    }
    assert all(".out." not in item["chunk"] for item in payload["targets"])


def test_target_builder_refuses_a_repeated_window() -> None:
    audit = {
        "defects": {
            "output_truncated_finish_reason_length": [
                {"chunk": "chunk_00056.out.jsonl.gz", "document_id": DOCUMENT_A},
                {"chunk": "chunk_00056.out.jsonl.gz", "document_id": DOCUMENT_A},
            ]
        }
    }
    with pytest.raises(ValueError, match="repeated cap-limited window"):
        build_target_payload(
            audit, primary_seed=20260824, repair_seed=20260917, audit_source="audit.json"
        )


def test_target_builder_refuses_a_clean_audit() -> None:
    with pytest.raises(ValueError, match="no cap-limited windows"):
        build_target_payload(
            {"defects": {"output_truncated_finish_reason_length": []}},
            primary_seed=20260824,
            repair_seed=20260917,
            audit_source="audit.json",
        )


# --- scoping a later attempt from the previous attestation ---------------------


def _previous_repair_manifest(*, repaired_flags: list[bool]) -> dict:
    targets = []
    for index, repaired in enumerate(repaired_flags):
        targets.append(
            {
                "chunk": f"chunk_{index:05d}.jsonl.gz",
                "document_id": f"{index:064d}",
                "route": "issues",
                "input_chunk_sha256": "a" * 64,
                "repair_output_file": f"chunk_{index:05d}.out.jsonl.gz",
                "repair_output_sha256": "b" * 64,
                "finish_reason": "stop" if repaired else "length",
                "parse_status": "parsed" if repaired else "output_truncated",
                "output_tokens": 900 if repaired else 24_576,
                "raw_output_sha256": "c" * 64,
                "generation_config_sha256": "d" * 64,
                "repaired": repaired,
            }
        )
    return {
        "repair_manifest_version": "candidate_extraction_seed_vary_repair_v1",
        "attempt_index": 2,
        "repair_seed": 20260917,
        "target_count": len(targets),
        "repaired_count": sum(1 for flag in repaired_flags if flag),
        "unrepaired_count": sum(1 for flag in repaired_flags if not flag),
        "all_targets_repaired": all(repaired_flags),
        "targets": targets,
    }


def test_later_attempt_is_scoped_to_the_unrepaired_windows() -> None:
    """A window the previous attempt recovered is finished and is not retried."""
    payload = build_target_payload_from_repair(
        _previous_repair_manifest(repaired_flags=[True, False, False]),
        primary_seed=20260824,
        repair_seed=20260918,
        manifest_source="repair_manifest.json",
        attempt_index=3,
    )
    assert payload["target_count"] == 2
    assert payload["attempt_index"] == 3
    assert payload["repair_seed"] == 20260918
    assert payload["created_from_attempt_index"] == 2
    assert [item["chunk"] for item in payload["targets"]] == [
        "chunk_00001.jsonl.gz",
        "chunk_00002.jsonl.gz",
    ]
    assert all(".out." not in item["chunk"] for item in payload["targets"])


@pytest.mark.parametrize(
    "mutation, message",
    [
        ("version", "version differs"),
        ("attempt_index", "does not follow"),
        ("seed", "repeats a seed already used"),
        ("all_repaired", "no unrepaired window"),
        ("count", "unrepaired count differs"),
    ],
)
def test_later_attempt_builder_refuses_a_bad_source(
    mutation: str, message: str
) -> None:
    previous = _previous_repair_manifest(repaired_flags=[True, False])
    if mutation == "version":
        previous["repair_manifest_version"] = "something_else"
    elif mutation == "attempt_index":
        previous["attempt_index"] = 3
    elif mutation == "seed":
        previous["repair_seed"] = 20260918
    elif mutation == "all_repaired":
        previous = _previous_repair_manifest(repaired_flags=[True])
    elif mutation == "count":
        previous["unrepaired_count"] = 5
    with pytest.raises(ValueError, match=message):
        build_target_payload_from_repair(
            previous,
            primary_seed=20260824,
            repair_seed=20260918,
            manifest_source="repair_manifest.json",
            attempt_index=3,
        )
