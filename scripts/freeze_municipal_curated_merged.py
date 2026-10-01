#!/usr/bin/env python3
"""Offline merge, identity audit, and freeze for municipal/curated shards."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

import pandas as pd

from scripts.audit_municipal_identity_attribution import (
    SOURCE_CONTEXT_COLUMNS,
    audit_identity_attribution,
)
from scripts.build_municipal_curated_acquisition_contract import (
    ATTEMPT_COLUMNS,
    EVIDENCE_COLUMNS,
    FILE_COLUMNS,
    audit_post_shard_directories,
)
from scripts.extract_municipal_mayoral_urls import normalize_exact_url


REPO_ROOT = Path(__file__).resolve().parents[1]
ACQUISITION_DIR = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/municipal_curated_acquisition"
)
AUDIT_DIR = ACQUISITION_DIR / "audit"
DEFAULT_OUTPUT_DIR = ACQUISITION_DIR / "merged"
EXPECTED_ATTEMPTS = 47
EXPECTED_EVIDENCE = 40
EXPECTED_CANDIDATE_SOURCE_ROWS = 264

EVIDENCE_CONTEXT_PATTERN = re.compile(
    r"^(?P<method>[a-z_]+)\s+"
    r"(?P<locator>(?:anchor|page|row):[^: ]+(?::[^: ]+)*):\s+"
    r"source labels (?P<label>.+?) and supplies (?P<supplied>.+?);\s+"
    r"matched to (?P<matched>.+?) via (?P<match_method>[^.]+)\.$"
)


class MunicipalMergeError(ValueError):
    """Raised when the immutable shard merge cannot be frozen safely."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_id(prefix: str, *parts: object) -> str:
    payload = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(payload).hexdigest()[:24]}"


def _url_key(value: object) -> str:
    exact = normalize_exact_url(str(value).strip())
    if not exact:
        raise MunicipalMergeError(f"Invalid evidence URL {value!r}")
    return exact.split("://", 1)[-1].rstrip("/")


def _candidate_label(source_label: str) -> str:
    """Remove a terminal party/affiliation and suffix, but retain the source name."""
    value = " ".join(str(source_label).split())
    value = re.sub(r"\s+\([^()]*\)\s*$", "", value).strip()
    value = re.sub(r",?\s+(?:jr|sr|ii|iii|iv)\.?$", "", value, flags=re.IGNORECASE)
    if not value:
        raise MunicipalMergeError(f"Source label contains no candidate name: {source_label!r}")
    return value


def source_contexts_from_evidence(evidence: pd.DataFrame) -> pd.DataFrame:
    """Parse exact source labels and locators retained by the shard evidence."""
    missing = sorted(
        {
            "evidence_id", "source_id", "source_file_id", "exact_normalized_url",
            "evidence_text", "identity_match",
        }
        - set(evidence.columns)
    )
    if missing:
        raise MunicipalMergeError(f"Evidence lacks source-context columns: {missing}")
    rows: list[dict[str, object]] = []
    for item in evidence.sort_values("evidence_id").itertuples(index=False):
        match = EVIDENCE_CONTEXT_PATTERN.fullmatch(str(item.evidence_text).strip())
        if match is None:
            raise MunicipalMergeError(
                f"Evidence {item.evidence_id} has no exact source-label/locator record"
            )
        fields = match.groupdict()
        if _url_key(fields["supplied"]) != _url_key(item.exact_normalized_url):
            raise MunicipalMergeError(
                f"Evidence {item.evidence_id} source-supplied URL differs from its exact URL"
            )
        if fields["match_method"] != str(item.identity_match):
            raise MunicipalMergeError(
                f"Evidence {item.evidence_id} context match method differs from identity_match"
            )
        if fields["method"] == "html_anchor":
            association_kind = "direct_anchor_text"
        elif (
            fields["method"] == "pdf_visible_text"
            and fields["match_method"] == "candidate_statement_owner_exact"
        ):
            association_kind = "manual_exact_context"
        else:
            association_kind = "page_context"
        context_name = _candidate_label(fields["label"])
        rows.append(
            {
                "context_id": _stable_id(
                    "municipal_context",
                    item.evidence_id,
                    fields["method"],
                    fields["locator"],
                    fields["label"],
                    fields["supplied"],
                ),
                "evidence_id": str(item.evidence_id),
                "source_id": str(item.source_id),
                "source_file_id": str(item.source_file_id),
                "exact_normalized_url": str(item.exact_normalized_url),
                "source_locator": fields["locator"],
                "association_kind": association_kind,
                "context_candidate_name": context_name,
                "context_text": str(item.evidence_text),
                "nearest_distance": "",
                "runner_up_distance": "",
                "explicit_separate_evidence": False,
                "separate_evidence_note": "",
            }
        )
    result = pd.DataFrame(rows, columns=SOURCE_CONTEXT_COLUMNS)
    if len(result) != len(evidence):
        raise MunicipalMergeError("Source-context derivation changed the evidence row count")
    if result["context_id"].duplicated().any() or result["evidence_id"].duplicated().any():
        raise MunicipalMergeError("Source-context derivation produced duplicate keys")
    return result


def _load_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if frame.columns.tolist() != columns:
        raise MunicipalMergeError(f"Unexpected schema in {path}")
    return frame


def build_merged_freeze(
    audit_dir: Path,
    shard_roots: dict[int, Path],
) -> dict[str, object]:
    """Audit immutable shards, merge exact interfaces, and audit attribution."""
    targets = pd.read_csv(audit_dir / "acquisition_targets.csv", dtype=str, keep_default_na=False)
    roster = pd.read_csv(
        audit_dir / "acquisition_candidate_targets.csv", dtype=str, keep_default_na=False
    )
    parser_matrix = pd.read_csv(
        audit_dir / "parser_support_matrix.csv", dtype=str, keep_default_na=False
    )
    if len(targets) != EXPECTED_ATTEMPTS or len(roster) != EXPECTED_CANDIDATE_SOURCE_ROWS:
        raise MunicipalMergeError(
            f"Frozen contract drift: targets={len(targets)}, candidate rows={len(roster)}"
        )
    structural_audit = audit_post_shard_directories(
        targets, roster, parser_matrix, shard_roots
    )

    attempts = pd.concat(
        [_load_csv(shard_roots[shard] / "attempts.csv", ATTEMPT_COLUMNS) for shard in sorted(shard_roots)],
        ignore_index=True,
    ).sort_values(["attempt_key"]).reset_index(drop=True)
    files = pd.concat(
        [_load_csv(shard_roots[shard] / "downloaded_files.csv", FILE_COLUMNS) for shard in sorted(shard_roots)],
        ignore_index=True,
    ).sort_values(["file_id"]).reset_index(drop=True)
    evidence = pd.concat(
        [_load_csv(shard_roots[shard] / "evidence.csv", EVIDENCE_COLUMNS) for shard in sorted(shard_roots)],
        ignore_index=True,
    ).sort_values(["evidence_id"]).reset_index(drop=True)
    if len(attempts) != EXPECTED_ATTEMPTS or len(evidence) != EXPECTED_EVIDENCE:
        raise MunicipalMergeError(
            f"Frozen merged row-count drift: attempts={len(attempts)}, evidence={len(evidence)}"
        )
    if attempts["attempt_key"].duplicated().any() or evidence["evidence_id"].duplicated().any():
        raise MunicipalMergeError("Merged interfaces contain duplicate keys")

    source_contexts = source_contexts_from_evidence(evidence)
    identity_decisions, identity_summary = audit_identity_attribution(
        targets, roster, evidence, source_contexts
    )
    identity_decisions = identity_decisions.sort_values("evidence_id").reset_index(drop=True)
    if len(identity_decisions) != len(evidence):
        raise MunicipalMergeError("Identity audit did not return one decision per evidence row")
    if identity_decisions["claim_approved_by_this_audit"].astype(bool).any():
        raise MunicipalMergeError("Identity attribution audit must not approve claims")

    troy = identity_decisions.loc[
        identity_decisions["context_candidate_name"].str.casefold().eq("troy benjegerdes")
    ]
    if len(troy) != 1 or troy.iloc[0]["identity_attribution_status"] == "identity_supported":
        raise MunicipalMergeError("Troy Benjegerdes must remain rejected or unresolved")

    audit_payload = {
        "schema_version": "municipal-curated-merged-audit-v1",
        "structural_audit": structural_audit,
        "identity_attribution": identity_summary,
        "merged_counts": {
            "attempts": int(len(attempts)),
            "downloaded_files": int(len(files)),
            "evidence": int(len(evidence)),
            "source_contexts": int(len(source_contexts)),
            "identity_decisions": int(len(identity_decisions)),
        },
        "key_checks": {
            "attempt_keys_exact": set(attempts["attempt_key"]) == set(targets["attempt_key"]),
            "evidence_context_keys_exact": set(evidence["evidence_id"])
            == set(source_contexts["evidence_id"]),
            "evidence_decision_keys_exact": set(evidence["evidence_id"])
            == set(identity_decisions["evidence_id"]),
            "claim_approved_rows": 0,
        },
        "claim_status": "not_approved; identity attribution only",
    }
    if not all(
        value is True
        for key, value in audit_payload["key_checks"].items()
        if key != "claim_approved_rows"
    ):
        raise MunicipalMergeError("Merged key agreement failed")
    return {
        "attempts": attempts,
        "downloaded_files": files,
        "evidence": evidence,
        "source_contexts": source_contexts,
        "identity_decisions": identity_decisions,
        "audit": audit_payload,
    }


def _input_paths(audit_dir: Path, shard_roots: dict[int, Path]) -> list[Path]:
    paths = [
        audit_dir / "acquisition_targets.csv",
        audit_dir / "acquisition_candidate_targets.csv",
        audit_dir / "parser_support_matrix.csv",
    ]
    for shard in sorted(shard_roots):
        root = shard_roots[shard]
        paths.extend(root / name for name in ["attempts.csv", "downloaded_files.csv", "evidence.csv"])
        paths.extend(sorted(path for path in (root / "files").rglob("*") if path.is_file()))
    return paths


def _file_record(path: Path) -> dict[str, object]:
    try:
        display_path = str(path.resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        display_path = str(path.resolve())
    return {
        "path": display_path,
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def write_merged_freeze(
    results: dict[str, object],
    output_dir: Path,
    audit_dir: Path,
    shard_roots: dict[int, Path],
) -> dict[str, object]:
    """Write deterministic merged artifacts and a hash manifest."""
    output_dir.mkdir(parents=True, exist_ok=True)
    expected_names = {
        "attempts.csv", "downloaded_files.csv", "evidence.csv", "source_contexts.csv",
        "identity_decisions.csv", "audit.json", "manifest.json",
    }
    unexpected = {path.name for path in output_dir.iterdir()} - expected_names
    if unexpected:
        raise MunicipalMergeError(f"Merged output directory has unexpected files: {sorted(unexpected)}")

    frames = {
        "attempts.csv": results["attempts"],
        "downloaded_files.csv": results["downloaded_files"],
        "evidence.csv": results["evidence"],
        "source_contexts.csv": results["source_contexts"],
        "identity_decisions.csv": results["identity_decisions"],
    }
    for name, frame in frames.items():
        frame.to_csv(output_dir / name, index=False)
    audit_path = output_dir / "audit.json"
    audit_path.write_text(
        json.dumps(results["audit"], indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    output_paths = [output_dir / name for name in frames] + [audit_path]
    for path in output_paths:
        if not path.is_file() or path.stat().st_size == 0:
            raise MunicipalMergeError(f"Merged output is missing or empty: {path}")
    manifest = {
        "schema_version": "municipal-curated-merged-manifest-v1",
        "inputs": [_file_record(path) for path in _input_paths(audit_dir, shard_roots)],
        "outputs": [_file_record(path) for path in output_paths],
        "immutable_shard_policy": "inputs were read and hashed; no shard file was modified",
        "claim_status": "not_approved",
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if not manifest_path.is_file() or manifest_path.stat().st_size == 0:
        raise MunicipalMergeError("Merged manifest was not written")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=AUDIT_DIR)
    parser.add_argument("--shard-0", type=Path, default=ACQUISITION_DIR / "shard_0")
    parser.add_argument("--shard-1", type=Path, default=ACQUISITION_DIR / "shard_1")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    shards = {0: args.shard_0, 1: args.shard_1}
    results = build_merged_freeze(args.audit_dir, shards)
    manifest = write_merged_freeze(results, args.output_dir, args.audit_dir, shards)
    print(
        json.dumps(
            {"audit": results["audit"], "manifest_outputs": manifest["outputs"]},
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
