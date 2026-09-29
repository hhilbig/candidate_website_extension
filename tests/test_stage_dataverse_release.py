"""Tests for unpublished Dataverse staging and byte verification."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.stage_dataverse_release import (
    CORE_DATA_FILES,
    DOCUMENTATION_FILES,
    EXTENSION_DATA_FILES,
    EXTENSION_MANIFEST,
    MultipartStream,
    build_manifest,
    canonical_remote_name,
    validate_local_package,
    verify_downloaded_package,
)


def _write_package(directory: Path) -> None:
    directory.mkdir()
    for name in sorted(CORE_DATA_FILES | EXTENSION_DATA_FILES):
        path = directory / name
        if name.endswith(".csv"):
            pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}).to_csv(path, index=False)
        else:
            pq.write_table(pa.table({"id": [1, 2], "value": ["a", "b"]}), path)
    for name in DOCUMENTATION_FILES:
        (directory / name).write_text(f"# {name}\n")
    manifest = build_manifest(directory, CORE_DATA_FILES)
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    extension = build_manifest(directory, EXTENSION_DATA_FILES)
    (directory / EXTENSION_MANIFEST).write_text(json.dumps(extension, indent=2) + "\n")


def _remote_version(directory: Path) -> dict:
    files = []
    for index, path in enumerate(sorted(directory.iterdir()), start=100):
        if path.name == "dataverse_draft_verification.json":
            continue
        if path.suffix == ".csv":
            mime = "text/comma-separated-values"
        elif path.suffix == ".parquet":
            mime = "application/octet-stream"
        elif path.suffix == ".md":
            mime = "text/markdown"
        else:
            mime = "application/json"
        files.append(
            {
                "label": path.name,
                "dataFile": {
                    "id": index,
                    "filename": path.name,
                    "contentType": mime,
                    "filesize": path.stat().st_size,
                    "tabularData": False,
                },
            }
        )
    return {
        "datasetPersistentId": "doi:10.7910/DVN/BZ2JRS",
        "versionNumber": 2,
        "versionMinorNumber": 1,
        "versionState": "DRAFT",
        "files": files,
    }


def test_local_package_manifest_must_match_bytes(tmp_path: Path) -> None:
    package = tmp_path / "release"
    _write_package(package)
    assert len(validate_local_package(package)) == 8
    with (package / "panel_candidate_year.csv").open("a") as handle:
        handle.write("3,c\n")
    with pytest.raises(ValueError, match="manifest.json"):
        validate_local_package(package)


def test_multipart_stream_is_reiterable_and_keeps_file_bytes(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv"
    path.write_bytes(b"x,y\n1,2\n")
    stream = MultipartStream(path, {"tabIngest": "false"}, "text/comma-separated-values")
    first = b"".join(stream)
    second = b"".join(stream)
    assert first == second
    assert len(first) == len(stream)
    assert path.read_bytes() in first
    assert b'"tabIngest":"false"' in first


def test_canonical_remote_name_uses_pre_ingest_filename() -> None:
    entry = {
        "dataFile": {
            "filename": "candidate_crosswalk.tab",
            "originalFileName": "candidate_crosswalk.csv",
        }
    }
    assert canonical_remote_name(entry) == "candidate_crosswalk.csv"


def test_download_verification_checks_sha_shapes_and_no_ingest(tmp_path: Path) -> None:
    release = tmp_path / "release"
    downloaded = tmp_path / "downloaded"
    _write_package(release)
    downloaded.mkdir()
    for path in release.iterdir():
        (downloaded / path.name).write_bytes(path.read_bytes())
    version = _remote_version(downloaded)
    mimes = {
        entry["dataFile"]["filename"]: entry["dataFile"]["contentType"]
        for entry in version["files"]
    }
    report = verify_downloaded_package(release, downloaded, version, mimes)
    assert report["version_state"] == "DRAFT"
    assert report["published"] is False
    assert len(report["files"]) == 19
    assert report["regenerated_manifest"] == json.loads(
        (release / "manifest.json").read_text()
    )

    raw = downloaded / "raw_corpus.parquet"
    raw.write_bytes(raw.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="byte size differs"):
        verify_downloaded_package(release, downloaded, version, mimes)


def test_download_verification_rejects_tabular_ingest(tmp_path: Path) -> None:
    release = tmp_path / "release"
    downloaded = tmp_path / "downloaded"
    _write_package(release)
    downloaded.mkdir()
    for path in release.iterdir():
        (downloaded / path.name).write_bytes(path.read_bytes())
    version = _remote_version(downloaded)
    version["files"][0]["dataFile"]["tabularData"] = True
    mimes = {
        entry["dataFile"]["filename"]: entry["dataFile"]["contentType"]
        for entry in version["files"]
    }
    with pytest.raises(ValueError, match="tabular ingest"):
        verify_downloaded_package(release, downloaded, version, mimes)
