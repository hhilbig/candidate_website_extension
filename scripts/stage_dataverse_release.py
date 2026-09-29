#!/usr/bin/env python3
"""Stage and byte-verify an unpublished Harvard Dataverse release draft.

This script has no publication operation. It replaces matching draft files,
adds new files, downloads the resulting draft, and validates the downloaded
bytes against the local release package.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import quote

import pandas as pd
import pyarrow.parquet as pq
import requests


CORE_DATA_FILES = {
    "raw_corpus.parquet",
    "panel_candidate_year.csv",
    "panel_icpsr_compat.csv",
    "candidate_crosswalk.csv",
    "release_roster.csv",
    "candidate_year_universe.csv",
    "candidate_year_provenance.csv",
    "panel_snapshot_day.parquet",
}
# Version 4.0 adds the candidate-extraction extension. Hanno approved its
# publication on 2026-09-28. The core files stay byte-identical to version 3.0.
EXTENSION_DATA_FILES = {
    "candidate_extraction_status.csv",
    "candidate_extraction_documents.parquet",
    "candidate_extraction_segments.parquet",
    "candidate_issue_statements.parquet",
    "candidate_biography_facts.parquet",
    "candidate_endorsements.parquet",
}
EXTENSION_MANIFEST = "candidate_extraction_manifest.json"
DOCUMENTATION_FILES = {"README.md", "codebook.md", "candidate_extraction_codebook.md"}
PACKAGE_FILES = (
    CORE_DATA_FILES
    | EXTENSION_DATA_FILES
    | DOCUMENTATION_FILES
    | {"manifest.json", EXTENSION_MANIFEST}
)
DATA_FILES = CORE_DATA_FILES | EXTENSION_DATA_FILES
DEFAULT_SERVER = "https://dataverse.harvard.edu"
DEFAULT_PID = "doi:10.7910/DVN/BZ2JRS"
EXPECTED_PUBLISHED_VERSION = (3, 0)


def md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def table_shape(path: Path) -> tuple[int, int]:
    if path.suffix == ".csv":
        frame = pd.read_csv(path, low_memory=False)
        return frame.shape
    if path.suffix == ".parquet":
        metadata = pq.ParquetFile(path).metadata
        return metadata.num_rows, metadata.num_columns
    raise ValueError(f"not a release data table: {path.name}")


def build_manifest(directory: Path, data_files: Iterable[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in sorted(data_files):
        path = directory / name
        if not path.is_file():
            raise FileNotFoundError(f"missing release data file: {path}")
        nrows, ncols = table_shape(path)
        rows.append(
            {
                "file": name,
                "rows": nrows,
                "columns": ncols,
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
        )
    return rows


def validate_local_package(release_dir: Path) -> list[dict[str, Any]]:
    if not release_dir.is_dir():
        raise FileNotFoundError(f"release directory does not exist: {release_dir}")
    disk_files = {path.name for path in release_dir.iterdir() if path.is_file()}
    if disk_files != PACKAGE_FILES:
        missing = sorted(PACKAGE_FILES - disk_files)
        extra = sorted(disk_files - PACKAGE_FILES)
        raise ValueError(f"release inventory mismatch: missing={missing}, extra={extra}")
    stored = json.loads((release_dir / "manifest.json").read_text())
    generated = build_manifest(release_dir, CORE_DATA_FILES)
    if stored != generated:
        raise ValueError("manifest.json does not match the local release bytes")
    _check_extension_manifest(release_dir)
    return generated


def _check_extension_manifest(directory: Path) -> None:
    stored = json.loads((directory / EXTENSION_MANIFEST).read_text())
    generated = build_manifest(directory, EXTENSION_DATA_FILES)
    if sorted(stored, key=lambda row: row["file"]) != generated:
        raise ValueError(f"{EXTENSION_MANIFEST} does not match the release bytes")


def expected_mime(name: str) -> str:
    suffix = Path(name).suffix.lower()
    fixed = {
        ".csv": "text/csv",
        ".parquet": "application/octet-stream",
        ".md": "text/markdown",
        ".json": "application/json",
    }
    if suffix in fixed:
        return fixed[suffix]
    guessed, _ = mimetypes.guess_type(name)
    return guessed or "application/octet-stream"


def canonical_remote_name(file_entry: dict[str, Any]) -> str:
    data_file = file_entry["dataFile"]
    return str(data_file.get("originalFileName") or data_file["filename"])


def remote_file_map(version: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for entry in version.get("files", []):
        name = canonical_remote_name(entry)
        if name in mapped:
            raise ValueError(f"Dataverse draft has duplicate canonical filename: {name}")
        mapped[name] = entry
    return mapped


class MultipartStream:
    """Stream one file and one JSON field without loading the file into RAM."""

    def __init__(self, path: Path, metadata: dict[str, Any], content_type: str):
        self.path = path
        self.boundary = f"candidate-release-{uuid.uuid4().hex}"
        encoded_metadata = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
        self.prefix = (
            f"--{self.boundary}\r\n"
            'Content-Disposition: form-data; name="jsonData"\r\n'
            "Content-Type: application/json; charset=utf-8\r\n\r\n"
        ).encode("ascii") + encoded_metadata + (
            f"\r\n--{self.boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{path.name}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode("utf-8")
        self.suffix = f"\r\n--{self.boundary}--\r\n".encode("ascii")

    def __len__(self) -> int:
        return len(self.prefix) + self.path.stat().st_size + len(self.suffix)

    def __iter__(self) -> Iterator[bytes]:
        yield self.prefix
        with self.path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                yield chunk
        yield self.suffix

    @property
    def content_type(self) -> str:
        return f"multipart/form-data; boundary={self.boundary}"


@dataclass
class DataverseClient:
    server: str
    persistent_id: str
    api_token: str
    session: requests.Session

    @classmethod
    def create(cls, server: str, persistent_id: str, api_token: str) -> "DataverseClient":
        session = requests.Session()
        session.headers.update({"X-Dataverse-key": api_token})
        return cls(server.rstrip("/"), persistent_id, api_token, session)

    def _json(self, response: requests.Response) -> dict[str, Any]:
        if not response.ok:
            detail = response.text[:1000]
            raise RuntimeError(f"Dataverse API returned HTTP {response.status_code}: {detail}")
        payload = response.json()
        if payload.get("status") == "ERROR":
            raise RuntimeError(f"Dataverse API error: {payload.get('message', payload)}")
        return payload["data"]

    def get_version(self, version: str) -> dict[str, Any] | None:
        encoded_pid = quote(self.persistent_id, safe="")
        url = (
            f"{self.server}/api/datasets/:persistentId/versions/{version}"
            f"?persistentId={encoded_pid}&excludeFiles=false"
        )
        response = self.session.get(url, timeout=(30, 300))
        if response.status_code == 404:
            return None
        return self._json(response)

    def _upload(self, url: str, path: Path, *, replacing: bool) -> dict[str, Any]:
        category = "Documentation" if path.name in DOCUMENTATION_FILES else "Data"
        metadata: dict[str, Any] = {
            "categories": [category],
            "restrict": "false",
            "tabIngest": "false",
        }
        if replacing:
            metadata["forceReplace"] = False
        body = MultipartStream(path, metadata, expected_mime(path.name))
        headers = {
            "Content-Type": body.content_type,
            "Content-Length": str(len(body)),
        }
        response = self.session.post(
            url,
            data=body,
            headers=headers,
            timeout=(30, 7200),
        )
        return self._json(response)

    def add_file(self, path: Path) -> dict[str, Any]:
        encoded_pid = quote(self.persistent_id, safe="")
        url = (
            f"{self.server}/api/datasets/:persistentId/add"
            f"?persistentId={encoded_pid}"
        )
        return self._upload(url, path, replacing=False)

    def replace_file(self, file_id: int, path: Path) -> dict[str, Any]:
        url = f"{self.server}/api/files/{file_id}/replace"
        return self._upload(url, path, replacing=True)

    def download_file(self, file_id: int, target: Path) -> str:
        url = f"{self.server}/api/access/datafile/{file_id}?format=original"
        with self.session.get(url, stream=True, timeout=(30, 7200)) as response:
            if not response.ok:
                raise RuntimeError(
                    f"Dataverse download returned HTTP {response.status_code}: "
                    f"{response.text[:1000]}"
                )
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0]
            with target.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        return content_type


def require_target_draft(version: dict[str, Any]) -> None:
    # Dataverse assigns the version number at publication, so only the state
    # is checked here.
    if version.get("versionState") != "DRAFT":
        raise ValueError("remote version is not an unpublished draft")


def verify_downloaded_package(
    release_dir: Path,
    download_dir: Path,
    remote_version: dict[str, Any],
    response_mimes: dict[str, str],
) -> dict[str, Any]:
    remote = remote_file_map(remote_version)
    if set(remote) != PACKAGE_FILES:
        raise ValueError(
            "Dataverse inventory mismatch after upload: "
            f"missing={sorted(PACKAGE_FILES - set(remote))}, "
            f"extra={sorted(set(remote) - PACKAGE_FILES)}"
        )

    files: list[dict[str, Any]] = []
    for name in sorted(PACKAGE_FILES):
        local = release_dir / name
        downloaded = download_dir / name
        entry = remote[name]["dataFile"]
        remote_mime = str(entry.get("contentType", ""))
        wanted_mime = expected_mime(name)
        if remote_mime != wanted_mime:
            raise ValueError(f"wrong Dataverse MIME for {name}: {remote_mime}; expected {wanted_mime}")
        if response_mimes.get(name) != wanted_mime:
            raise ValueError(
                f"wrong download MIME for {name}: {response_mimes.get(name)}; expected {wanted_mime}"
            )
        if bool(entry.get("tabularData", False)):
            raise ValueError(f"Dataverse tabular ingest was not disabled for {name}")
        if downloaded.stat().st_size != local.stat().st_size:
            raise ValueError(f"downloaded byte size differs for {name}")
        if sha256(downloaded) != sha256(local):
            raise ValueError(f"downloaded SHA-256 differs for {name}")
        record: dict[str, Any] = {
            "file": name,
            "dataverse_file_id": int(entry["id"]),
            "mime_type": remote_mime,
            "bytes": downloaded.stat().st_size,
            "sha256": sha256(downloaded),
        }
        if name in DATA_FILES:
            record["rows"], record["columns"] = table_shape(downloaded)
        files.append(record)

    regenerated = build_manifest(download_dir, CORE_DATA_FILES)
    stored_downloaded = json.loads((download_dir / "manifest.json").read_text())
    stored_local = json.loads((release_dir / "manifest.json").read_text())
    if regenerated != stored_downloaded or regenerated != stored_local:
        raise ValueError("downloaded data bytes do not regenerate the staged manifest")
    _check_extension_manifest(download_dir)

    return {
        "persistent_id": str(remote_version.get("datasetPersistentId", DEFAULT_PID)),
        "version": "draft after {}.{}".format(*EXPECTED_PUBLISHED_VERSION),
        "version_state": remote_version["versionState"],
        "published": False,
        "tabular_ingest_disabled": True,
        "files": files,
        "regenerated_manifest": regenerated,
    }


def stage_and_verify(
    client: DataverseClient,
    release_dir: Path,
    download_dir: Path,
    *,
    resume_draft: bool = False,
) -> dict[str, Any]:
    validate_local_package(release_dir)
    if download_dir.exists() and any(download_dir.iterdir()):
        raise FileExistsError(f"download directory is not empty: {download_dir}")
    download_dir.mkdir(parents=True, exist_ok=True)

    draft = client.get_version(":draft")
    if draft is not None and not resume_draft:
        raise RuntimeError("an unpublished Dataverse draft already exists; inspect it before using --resume-draft")
    base = draft or client.get_version(":latest-published")
    if base is None:
        raise RuntimeError("Dataverse dataset has no published version")
    if draft is None:
        pair = (int(base.get("versionNumber", -1)), int(base.get("versionMinorNumber", -1)))
        if base.get("versionState") != "RELEASED" or pair != EXPECTED_PUBLISHED_VERSION:
            raise RuntimeError(
                "expected latest published version {}.{}, found {}.{}".format(
                    *EXPECTED_PUBLISHED_VERSION, *pair
                )
            )

    remote = remote_file_map(base)
    if set(remote) - PACKAGE_FILES:
        raise RuntimeError(f"unexpected Dataverse files: {sorted(set(remote) - PACKAGE_FILES)}")

    for name in sorted(PACKAGE_FILES):
        path = release_dir / name
        if name in remote:
            checksum = remote[name]["dataFile"].get("checksum") or {}
            # An unchanged core file keeps its published file ID.
            if checksum.get("type") == "MD5" and checksum.get("value") == md5(path):
                continue
            client.replace_file(int(remote[name]["dataFile"]["id"]), path)
        else:
            client.add_file(path)

    verified_draft = client.get_version(":draft")
    if verified_draft is None:
        raise RuntimeError("Dataverse did not create an unpublished draft")
    require_target_draft(verified_draft)
    final_remote = remote_file_map(verified_draft)
    if set(final_remote) != PACKAGE_FILES:
        raise RuntimeError("Dataverse draft inventory differs from the staged package")

    response_mimes: dict[str, str] = {}
    for name in sorted(PACKAGE_FILES):
        file_id = int(final_remote[name]["dataFile"]["id"])
        response_mimes[name] = client.download_file(file_id, download_dir / name)

    report = verify_downloaded_package(
        release_dir,
        download_dir,
        verified_draft,
        response_mimes,
    )
    report_path = download_dir / "dataverse_draft_verification.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    return report


def load_api_token(token_file: Path | None) -> str:
    if token_file is not None:
        token = token_file.read_text().strip()
    else:
        token = os.environ.get("DATAVERSE_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "set DATAVERSE_API_TOKEN or pass --token-file; the token is never written to output"
        )
    return token


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release_dir", type=Path)
    parser.add_argument("download_dir", type=Path)
    parser.add_argument("--server", default=DEFAULT_SERVER)
    parser.add_argument("--persistent-id", default=DEFAULT_PID)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--resume-draft", action="store_true")
    args = parser.parse_args(argv)

    token = load_api_token(args.token_file)
    client = DataverseClient.create(args.server, args.persistent_id, token)
    report = stage_and_verify(
        client,
        args.release_dir,
        args.download_dir,
        resume_draft=args.resume_draft,
    )
    print(
        f"verified unpublished Dataverse {report['version']} draft: "
        f"{len(report['files'])} files"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
