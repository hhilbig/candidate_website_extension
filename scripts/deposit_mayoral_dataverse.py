#!/usr/bin/env python3
"""Create the mayoral Harvard Dataverse dataset as an unpublished draft, upload, and byte-verify.

Metadata approved by Hanno on 2026-10-24 (docs/mayoral_release/dataverse_deposit_approval.md).
This script has no publication operation. It is resumable: the persistent ID of the created
draft is kept in build/mayoral_dataverse_deposit.json, and files already in the draft are not
uploaded again.

Usage: python scripts/deposit_mayoral_dataverse.py [--token-file ~/.dataverse_token]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parent))
from stage_dataverse_release import (  # noqa: E402
    DataverseClient,
    MultipartStream,
    expected_mime,
    load_api_token,
    remote_file_map,
    sha256,
)

SERVER = "https://dataverse.harvard.edu"
COLLECTION = "harvard"
RELEASE = Path("build/mayoral_release_v6_candidate")
DOCS = Path("docs/mayoral_release")
PACKAGE = Path("build/mayoral_dataverse_package")
DOWNLOAD = Path("build/mayoral_dataverse_download")
STATE = Path("build/mayoral_dataverse_deposit.json")
DOCUMENTATION = ["README.md", "CODEBOOK.md"]

TITLE = "Archived Campaign Websites of U.S. Mayoral Candidates"
DESCRIPTION = (
    "Archived campaign websites of mayoral candidates in the 100 largest U.S. cities, January 2010 to "
    "September 2026. For each election that decided who became mayor, the data include the winner and the "
    "runner-up; in cities with partisan mayoral primaries, they also include the top two candidates of the "
    "decisive primary. All pages come from the Internet Archive's Wayback Machine. The release contains a "
    "corpus of 46,466 archived pages with flags for usability and campaign period, candidate-level panels "
    "with the joined website text, files on cities and elections, and a crosswalk to the American Local "
    "Government Elections Database (ALGED). Of the 860 finalists in 430 elections, 710 have usable website "
    "text. See README.md for the population and the collection, and CODEBOOK.md for every column."
)
KEYWORDS = ["campaign websites", "mayoral elections", "local politics", "Wayback Machine", "text as data"]
RELATED = [
    "de Benedictis-Kessner, Justin, Diana Da In Lee, Yamil R. Velez, and Christopher Warshaw. American Local "
    "Government Elections Database. OSF. https://osf.io/mv5e6/",
    "Hilbig, Hanno. U.S. House (2018–2024) and Senate (2002–2024) Candidate Websites. Harvard "
    "Dataverse. https://doi.org/10.7910/DVN/BZ2JRS",
]


def primitive(name: str, value, multiple: bool = False) -> dict:
    return {"typeName": name, "multiple": multiple, "typeClass": "primitive", "value": value}


def compound(name: str, values: list[dict]) -> dict:
    return {"typeName": name, "multiple": True, "typeClass": "compound", "value": values}


def dataset_json() -> dict:
    fields = [
        primitive("title", TITLE),
        compound("author", [{
            "authorName": primitive("authorName", "Hilbig, Hanno"),
            "authorAffiliation": primitive("authorAffiliation", "https://ror.org/05rrcem69"),
        }]),
        compound("datasetContact", [{
            "datasetContactName": primitive("datasetContactName", "Hilbig, Hanno"),
            "datasetContactAffiliation": primitive("datasetContactAffiliation", "University of California, Davis"),
            "datasetContactEmail": primitive("datasetContactEmail", "hhilbig@ucdavis.edu"),
        }]),
        compound("dsDescription", [{"dsDescriptionValue": primitive("dsDescriptionValue", DESCRIPTION)}]),
        {"typeName": "subject", "multiple": True, "typeClass": "controlledVocabulary", "value": ["Social Sciences"]},
        compound("keyword", [{"keywordValue": primitive("keywordValue", k)} for k in KEYWORDS]),
        compound("timePeriodCovered", [{
            "timePeriodCoveredStart": primitive("timePeriodCoveredStart", "2010-01-01"),
            "timePeriodCoveredEnd": primitive("timePeriodCoveredEnd", "2026-09-14"),
        }]),
        primitive("relatedDatasets", RELATED, multiple=True),
    ]
    return {"datasetVersion": {
        "license": {"name": "CC BY 4.0", "uri": "http://creativecommons.org/licenses/by/4.0"},
        "metadataBlocks": {"citation": {"displayName": "Citation Metadata", "fields": fields}},
    }}


def stage_package() -> list[str]:
    PACKAGE.mkdir(parents=True, exist_ok=True)
    sources = sorted(RELEASE.iterdir()) + [DOCS / name for name in DOCUMENTATION]
    for src in sources:
        shutil.copy2(src, PACKAGE / src.name)
    names = sorted(p.name for p in sources)
    if set(names) != {p.name for p in PACKAGE.iterdir()}:
        raise ValueError("package folder holds files outside the release")
    return names


def upload(client: DataverseClient, path: Path) -> None:
    category = "Documentation" if path.name in DOCUMENTATION else "Data"
    body = MultipartStream(path, {"categories": [category], "restrict": "false", "tabIngest": "false"},
                           expected_mime(path.name))
    url = f"{client.server}/api/datasets/:persistentId/add?persistentId={quote(client.persistent_id, safe='')}"
    response = client.session.post(url, data=body, timeout=(30, 7200),
                                   headers={"Content-Type": body.content_type, "Content-Length": str(len(body))})
    client._json(response)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-file", type=Path)
    args = parser.parse_args()
    token = load_api_token(args.token_file)
    names = stage_package()

    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    if "persistent_id" not in state:
        client = DataverseClient.create(SERVER, "", token)
        created = client._json(client.session.post(f"{SERVER}/api/dataverses/{COLLECTION}/datasets",
                                                   json=dataset_json(), timeout=(30, 300)))
        state = {"persistent_id": created["persistentId"], "dataset_id": created["id"]}
        STATE.write_text(json.dumps(state, indent=1) + "\n")
        print(f"created draft {state['persistent_id']}", flush=True)
    client = DataverseClient.create(SERVER, state["persistent_id"], token)

    draft = client.get_version(":draft")
    if draft is None or draft.get("versionState") != "DRAFT":
        raise ValueError("the dataset has no unpublished draft")
    present = set(remote_file_map(draft))
    if present - set(names):
        raise ValueError(f"draft holds unexpected files: {sorted(present - set(names))}")
    for name in names:
        if name not in present:
            print(f"uploading {name}", flush=True)
            upload(client, PACKAGE / name)

    draft = client.get_version(":draft")
    remote = remote_file_map(draft)
    if set(remote) != set(names):
        raise ValueError(f"draft inventory differs: {sorted(set(remote) ^ set(names))}")
    DOWNLOAD.mkdir(parents=True, exist_ok=True)
    files = []
    for name in names:
        entry = remote[name]["dataFile"]
        if entry.get("tabularData"):
            raise ValueError(f"tabular ingest was not disabled for {name}")
        client.download_file(int(entry["id"]), DOWNLOAD / name)
        local, down = PACKAGE / name, DOWNLOAD / name
        if down.stat().st_size != local.stat().st_size or sha256(down) != sha256(local):
            raise ValueError(f"downloaded bytes differ for {name}")
        files.append({"file": name, "id": int(entry["id"]), "bytes": local.stat().st_size,
                      "sha256": sha256(local), "content_type": entry.get("contentType")})
        print(f"verified {name}", flush=True)
    state.update({"version_state": draft["versionState"], "published": False, "files": files})
    STATE.write_text(json.dumps(state, indent=1) + "\n")
    print(f"draft {state['persistent_id']}: {len(files)} files byte-identical", flush=True)


if __name__ == "__main__":
    main()
