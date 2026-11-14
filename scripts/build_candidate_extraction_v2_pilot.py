#!/usr/bin/env python3
"""Build the balanced plus structured-decoding stress v2.1 pilot."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.candidate_extraction import (
    ARRAY_ITEM_CAP_SPEC_ID,
    ARRAY_ITEM_CAP_SPEC_SHA256,
    INFERENCE_DISPLAY_ESCAPE_ID,
    INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256,
    MODEL_ITEM_REPETITION_SPEC_ID,
    MODEL_ITEM_REPETITION_SPEC_SHA256,
    V2_1_MODEL_ITEM_CAPS,
    get_extraction_contract,
)


DEFAULT_SEED = 20260824
ROUTES = ("biography", "endorsements", "issues")
DISPLAY_STRESS_GROUP = "control_character_stress"
STRAIGHT_QUOTE_STRESS_GROUP = "straight_quote_whitespace_stress"
DISPLAY_CODEC_CHANGED_DESCRIPTION = (
    "literal double quote, backslash, or non-LF C0 U+0000-U+0009/U+000B-U+001F"
)
CONTROL_CODEC_CHANGED_DESCRIPTION = (
    "literal backslash or non-LF C0 U+0000-U+0009/U+000B-U+001F"
)
STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID = (
    "f151afdff5d59a2dd0625dffc3bb5181b94694d836cff3e20c07922abfa8d556"
)
STRUCTURED_WHITESPACE_FAILURE_PARENT_ID = (
    "a475c524f2a6c7fe18510dbd2e3aa5b6d28ccebaaa671161b5003ed6f3519938"
)
STRUCTURED_WHITESPACE_FAILURE_CONTROL = "structured_whitespace_truncation"
QUOTE_REPETITION_FAILURE_DOCUMENT_ID = (
    "7bdfc2a16483fb4225e35bc14b9d84583ad33d2e2fe413a0059b93d2be303315"
)
QUOTE_REPETITION_FAILURE_PARENT_ID = (
    "fc409dcdcd425a928076f0954677926c7c8a9068f82ce0eea75eba41392e03f0"
)
QUOTE_REPETITION_FAILURE_CONTROL = "quote_repetition_truncation"
MAX_COMPLEXITY_BIOGRAPHY_DOCUMENT_ID = (
    "70810bf5d41250cebfd6dd03f559ad03bad8c9b60ab60b39c9b879099eff72a4"
)
MAX_DENSITY_ENDORSEMENT_DOCUMENT_ID = (
    "aa0b99eb1331464dce28db32d73a6f53bddbf7e4aa10f46f4c17c48b12720433"
)
EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID = (
    "5fa65c8ef604e192c08138fd14334f4145feb557f26bb97ed6a30157dda2e436"
)
EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID = (
    "0555f63d681c04fcf043c385abef0ec9470e72349893543c8b10926a5f6192ce"
)
EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID = (
    "03fcaab04e96c57dd9347de4b63edd681e2812e765dd3c8bce74fb58b0cae52a"
)
EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID = (
    "a8885c81fde803fea71cd73c15f3e9058a77283fd1bab96cec6687f9b93cb70d"
)
QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID = (
    "36a087fabff4e9fe643765073f0d1fa1e84d4ad25b47c69cb865d67adc97820a"
)
QUOTE_BEARING_ENDORSEMENT_NAME_PARENT_ID = (
    "5092019feb6f9fae9d18671bb9e7daec3a4ac4f372ccbdd92cca0054aae53054"
)
QUOTE_BEARING_ENDORSEMENT_NAME_EVIDENCE = (
    'Texas State Rep. Jolanda "Jo" Jones'
)
EXACT_ANCHOR_ABLATION_CONTROL_DOCUMENT_IDS = (
    EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID,
    EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID,
    EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID,
    EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID,
)
EXACT_ANCHOR_ABLATION_PARENT_IDS = {
    EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID: (
        "71c197e1b81d48216f8c2cb757f1da7e2b769819dd64e0e7865645d96b62bec8"
    ),
    EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID: (
        "bfc30422807490102ed361342c623de00a418ecc4fc3614de2692833c71b8f5c"
    ),
    EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID: (
        "df03f7b60754314aaee0069f87a7fb54feddd5ebfcb9e66fc35851c170bb7c13"
    ),
    EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID: (
        "df9abdfcb024b2e62dc57ceb585f6b233d23764a36704930b1e18de376d3eeb5"
    ),
}
# The preflight exports every segment for the four added control parents. This
# preserves the production complete-parent invariant for reconstruction and
# zero-result interpretation. One issue sibling was already in the 19-row m
# preflight, so this 14-row closure adds 13 unique rows to that inventory.
EXACT_ANCHOR_ABLATION_PARENT_CLOSURE_DOCUMENT_IDS = (
    "029290d53febc0f3c06a8e63fdce60ef6bdf722d5e7a991ceda3c980626f163f",
    EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID,
    "23f764b67dd8e0e7cb6e8d67432dce90f9abe63841f2772de5543b87cf47e729",
    EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID,
    "8785d3dbe97ecdec36ee103b4680c166f7f8d5dd12d9b23715537e60fb7d8381",
    EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID,
    "ce871772301198af9e9eb9ed560a36ae290f7861aa31d38139fd370f8fcf07be",
    "22febf040afafcded1da25a4a19c2f2f4f55b21ec79be5bc24fe1a99c8e8c476",
    "e2348899517313d4155f9511d622923f87764cd4ec4584321ea601ac9e33acf2",
    "22e850f56d0eecfdd1b13b7bc1def76a873e847b174011c238cc24d305be63df",
    "c89770e6ba845a734750ff236ac40f7e4828f329cf8bd34fb03b20d66fe0d69b",
    "a898dd04079f7d473e75facfc95017a203978f693ca4734ef70d8c45f867ef83",
    EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID,
    "e56c1b40a3bc139cc9ac53e347673ab061bdc26da15a4333e928571aeff4eb32",
)
QUOTE_REGRESSION_ISSUE_DOCUMENT_IDS = (
    "5d4209bcbd85019b199a558b6b08faa4e26689feab1d3a6c1e6a5613aa7688e6",
    "a92d91fc4d76ec90b64511fd6de257bd7f5830124d61a2dc1680c4ba421bf125",
    "2daf39bb58202e3b81a50343ba7d05ada45af5e94f4394755aa87af3a6573758",
    "c59f8a0dde2f7b9022f94189b8035613fb5e29aa81e70d56935fbc46d075ab02",
)
QUOTE_COPULA_ISSUE_DOCUMENT_IDS = (
    "3cab5c5c1cc3c6860c344a6186e172bf04a26aaf33b8be905cac0b6092a7c421",
    "224e1afb9106150ded6b057d0c734199b95ab0f2e73efee2ac4c0405556c5e01",
    "966609d243c5ca204474bbfd51ff0fdd21bc3d494365a214070ba044e91bd7fa",
    "a34443e23ccf54c547413747ee3dc781dfb2e257515603817657e74f604eea79",
    "82e15a89a06dde8643ae6d9ac60ddf43e1b58c2458b36982d2a5d52c252fa827",
    "23f764b67dd8e0e7cb6e8d67432dce90f9abe63841f2772de5543b87cf47e729",
    "0dcc29aaeefd7c1539fad46b19eb496a5d2461fd204f2fa062b1a87928880911",
    "0f3b3c5712297a358df45f8fa3f94edd138128e3dd757522115b60e21aca70c7",
    "27d32ad39a51da6490c7b264e8d15bb0e9e1212b058251d09c220a066b1cfba5",
    "cd179ab68cd2246ba2bdf1715a822ec00f3e64e6af9ba9a26c5f1247c1fe8f1f",
    "c7a74f196350dd7bc288d7257698dc1283a6c20b3d5c6b3332bac0c88d153cbe",
    QUOTE_REPETITION_FAILURE_DOCUMENT_ID,
    "b91a757b899c47f8fbfeb33605c6fc8a8ac2bc82606d12f73cb1eca55e5e64ff",
)
QUOTE_CAP_PREFLIGHT_NAMED_DOCUMENT_IDS = tuple(dict.fromkeys((
    *QUOTE_COPULA_ISSUE_DOCUMENT_IDS,
    *QUOTE_REGRESSION_ISSUE_DOCUMENT_IDS,
    MAX_COMPLEXITY_BIOGRAPHY_DOCUMENT_ID,
    MAX_DENSITY_ENDORSEMENT_DOCUMENT_ID,
    *EXACT_ANCHOR_ABLATION_CONTROL_DOCUMENT_IDS,
    QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID,
)))
# Immutable complete-parent closure of all 18 parents touched by the named
# decoder controls. Ordered by route, parent ID, then segment index.
QUOTE_CAP_PREFLIGHT_DOCUMENT_IDS = (
    "70810bf5d41250cebfd6dd03f559ad03bad8c9b60ab60b39c9b879099eff72a4",
    "64533b349d4b16af0ddbccb48b198b0c2d5957ad129e62d21596fce4769b211b",
    "0555f63d681c04fcf043c385abef0ec9470e72349893543c8b10926a5f6192ce",
    "8785d3dbe97ecdec36ee103b4680c166f7f8d5dd12d9b23715537e60fb7d8381",
    "385baa6869ddbdf08937e844bd276fdff046dcfe8b82e6c95c83ebe3e97bf3a0",
    "db76f307a7db68408d1d5b12b799013349b48b95e033e10feb4c74398753c92d",
    "e7c1aa9ab7d229fec823871bd44d0ae4b2e42d7d55288d36f5bf947337d5dfc1",
    "aa0b99eb1331464dce28db32d73a6f53bddbf7e4aa10f46f4c17c48b12720433",
    "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73",
    "03fcaab04e96c57dd9347de4b63edd681e2812e765dd3c8bce74fb58b0cae52a",
    "ce871772301198af9e9eb9ed560a36ae290f7861aa31d38139fd370f8fcf07be",
    "22febf040afafcded1da25a4a19c2f2f4f55b21ec79be5bc24fe1a99c8e8c476",
    "e2348899517313d4155f9511d622923f87764cd4ec4584321ea601ac9e33acf2",
    "22e850f56d0eecfdd1b13b7bc1def76a873e847b174011c238cc24d305be63df",
    "c89770e6ba845a734750ff236ac40f7e4828f329cf8bd34fb03b20d66fe0d69b",
    "a898dd04079f7d473e75facfc95017a203978f693ca4734ef70d8c45f867ef83",
    "a8885c81fde803fea71cd73c15f3e9058a77283fd1bab96cec6687f9b93cb70d",
    "e56c1b40a3bc139cc9ac53e347673ab061bdc26da15a4333e928571aeff4eb32",
    QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID,
    "36f1cfc2b04bb0dfce341726350039f8339c5107cade950c5f1e8a7eab6f0645",
    "3cab5c5c1cc3c6860c344a6186e172bf04a26aaf33b8be905cac0b6092a7c421",
    "224e1afb9106150ded6b057d0c734199b95ab0f2e73efee2ac4c0405556c5e01",
    "66e6d3a67b4c9074a6729ec4a2b46d501233ed8b05cd0f15046e472d81b35dab",
    "966609d243c5ca204474bbfd51ff0fdd21bc3d494365a214070ba044e91bd7fa",
    "a34443e23ccf54c547413747ee3dc781dfb2e257515603817657e74f604eea79",
    "5d4209bcbd85019b199a558b6b08faa4e26689feab1d3a6c1e6a5613aa7688e6",
    "5f24f2bb65e542b77c5d15f73f0cf05677bf396fabbbd5d2b08be44587cf2b31",
    "82e15a89a06dde8643ae6d9ac60ddf43e1b58c2458b36982d2a5d52c252fa827",
    "a92d91fc4d76ec90b64511fd6de257bd7f5830124d61a2dc1680c4ba421bf125",
    "029290d53febc0f3c06a8e63fdce60ef6bdf722d5e7a991ceda3c980626f163f",
    "5fa65c8ef604e192c08138fd14334f4145feb557f26bb97ed6a30157dda2e436",
    "23f764b67dd8e0e7cb6e8d67432dce90f9abe63841f2772de5543b87cf47e729",
    "0dcc29aaeefd7c1539fad46b19eb496a5d2461fd204f2fa062b1a87928880911",
    "2daf39bb58202e3b81a50343ba7d05ada45af5e94f4394755aa87af3a6573758",
    "d86e3c4b33c168af9f13ea5739af0b0923ab22de372e03c04000adda2479c8f5",
    "0f3b3c5712297a358df45f8fa3f94edd138128e3dd757522115b60e21aca70c7",
    "27d32ad39a51da6490c7b264e8d15bb0e9e1212b058251d09c220a066b1cfba5",
    "7701e10104ec0d9c18255e793d37ef158ba7a4ffc71d468d372731476e9f464b",
    "cd179ab68cd2246ba2bdf1715a822ec00f3e64e6af9ba9a26c5f1247c1fe8f1f",
    "c7a74f196350dd7bc288d7257698dc1283a6c20b3d5c6b3332bac0c88d153cbe",
    "c59f8a0dde2f7b9022f94189b8035613fb5e29aa81e70d56935fbc46d075ab02",
    "e219cd485d4b3f5fa1fc4faff1d05280dd2bf59f4bc4152673098aa5ee35abbf",
    "7bdfc2a16483fb4225e35bc14b9d84583ad33d2e2fe413a0059b93d2be303315",
    "b91a757b899c47f8fbfeb33605c6fc8a8ac2bc82606d12f73cb1eca55e5e64ff",
)


def display_codec_stress_metrics(text: str) -> dict[str, Any]:
    """Return exact metrics for characters changed by the minimal display codec."""
    if not isinstance(text, str):
        raise TypeError("source text must be a string")
    control_codepoint_counts = Counter(
        f"U+{ord(character):04X}"
        for character in text
        if character == "\\" or (ord(character) < 0x20 and character != "\n")
    )
    codepoint_counts = Counter(control_codepoint_counts)
    if '"' in text:
        codepoint_counts["U+0022"] += text.count('"')
    non_lf_c0_count = sum(
        count
        for label, count in control_codepoint_counts.items()
        if label != "U+005C"
    )
    inline_tab = any(
        len(line) <= 160
        and line.count("\t") == 1
        and line[:index].strip()
        and line[index + 1 :].strip()
        for line in text.splitlines()
        for index, character in enumerate(line)
        if character == "\t"
    )
    tab_nonempty_lines = sum(
        1 for line in text.splitlines() if "\t" in line and line.strip()
    )
    return {
        "changed_character_count": sum(codepoint_counts.values()),
        "control_changed_character_count": sum(control_codepoint_counts.values()),
        "non_lf_c0_count": non_lf_c0_count,
        "backslash_count": int(codepoint_counts.get("U+005C", 0)),
        "tab_count": int(codepoint_counts.get("U+0009", 0)),
        "u0003_count": int(codepoint_counts.get("U+0003", 0)),
        "inline_tab": bool(inline_tab),
        "tab_nonempty_lines": int(tab_nonempty_lines),
        "codepoint_counts": dict(sorted(codepoint_counts.items())),
        "control_codepoint_counts": dict(sorted(control_codepoint_counts.items())),
    }


def straight_quote_stress_metrics(text: str) -> dict[str, int | bool]:
    """Count literal U+0022 characters that can precede JSON grammar boundaries."""
    if not isinstance(text, str):
        raise TypeError("source text must be a string")
    count = text.count('"')
    return {"straight_quote_count": count, "has_straight_quote": count > 0}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_score(seed: int, value: str) -> str:
    return hashlib.sha256(f"{seed}\n{value}".encode("utf-8")).hexdigest()


def read_jsonl_gz(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def write_jsonl_gz(path: Path, rows: list[dict[str, Any]]) -> None:
    temp = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    with temp.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            for row in rows:
                line = json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                compressed.write(line.encode("utf-8"))
    os.replace(temp, path)


def v2_1_source_row(row: dict[str, Any]) -> dict[str, Any]:
    """Expose core candidate_icpsr and remove the legacy candidate_name mislabel."""
    output = dict(row)
    candidate_icpsr = output.get("candidate_icpsr", output.get("candidate_name"))
    if not isinstance(candidate_icpsr, str) or not candidate_icpsr.strip():
        raise ValueError(f"{row.get('document_id')}: missing candidate_icpsr")
    output["candidate_icpsr"] = candidate_icpsr
    output.pop("candidate_name", None)
    return output


def pilot_manifest_source_metadata(
    source_manifest: dict[str, Any], transformed_exported_parent_documents: int
) -> dict[str, Any]:
    """Keep the source wire format and transform distinct from inference."""
    source_version = str(source_manifest.get("schema_version"))
    if source_version != "1.4":
        raise ValueError(f"v2.1 pilot requires source export schema 1.4, got {source_version}")
    transform = source_manifest.get("source_text_transform")
    if not isinstance(transform, dict):
        raise ValueError("v2.1 pilot requires structured source-text transform provenance")
    endorsement_context = source_manifest.get("endorsement_context_provenance")
    if not isinstance(endorsement_context, dict):
        raise ValueError("v2.1 pilot requires endorsement-context provenance")
    pilot_transform = dict(transform)
    pilot_transform["transformed_exported_parent_documents"] = int(
        transformed_exported_parent_documents
    )
    return {
        "schema_version": source_version,
        "inference_schema_version": "2.1",
        "source_text_transform": pilot_transform,
        "endorsement_context_provenance": dict(endorsement_context),
    }


def source_chunk_paths(export_dir: Path) -> tuple[dict[str, Any], list[Path]]:
    manifest_path = export_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    chunks: list[Path] = []
    for item in manifest["chunks"]:
        path = export_dir / "chunks" / item["file"]
        if sha256_file(path) != item["sha256"]:
            raise ValueError(f"source chunk hash mismatch: {path}")
        chunks.append(path)
    return manifest, chunks


def parent_inventory(chunks: list[Path]) -> pd.DataFrame:
    parents: dict[str, dict[str, Any]] = {}
    document_ids: set[str] = set()
    for path in chunks:
        for row in read_jsonl_gz(path):
            row = v2_1_source_row(row)
            document_id = str(row["document_id"])
            if document_id in document_ids:
                raise ValueError(f"duplicate source segment: {document_id}")
            document_ids.add(document_id)
            parent_id = str(row.get("parent_document_id") or document_id)
            payload = parents.setdefault(
                parent_id,
                {
                    "parent_document_id": parent_id,
                    "route": str(row["route"]),
                    "candidate_cycle_id": str(row["candidate_cycle_id"]),
                    "candidate_icpsr": str(row["candidate_icpsr"]),
                    "segment_count": 0,
                    "display_codec_changed_segment_count": 0,
                    "display_codec_changed_character_count": 0,
                    "control_codec_changed_segment_count": 0,
                    "control_codec_changed_character_count": 0,
                    "non_lf_c0_segment_count": 0,
                    "non_lf_c0_character_count": 0,
                    "backslash_segment_count": 0,
                    "backslash_character_count": 0,
                    "tab_segment_count": 0,
                    "tab_character_count": 0,
                    "u0003_segment_count": 0,
                    "u0003_character_count": 0,
                    "straight_quote_segment_count": 0,
                    "straight_quote_character_count": 0,
                    "structured_whitespace_failure_segment_id": None,
                    "quote_repetition_failure_segment_id": None,
                    "max_tab_segment_nonempty_lines": 0,
                    "max_tab_segment_id": None,
                    "min_inline_tab_segment_n_chars": None,
                    "min_inline_tab_segment_id": None,
                    "min_u0003_segment_n_chars": None,
                    "min_u0003_segment_id": None,
                    "display_codec_codepoint_counts": Counter(),
                },
            )
            for field in ("route", "candidate_cycle_id"):
                if str(payload[field]) != str(row[field]):
                    raise ValueError(f"parent metadata mismatch for {parent_id}: {field}")
            payload["segment_count"] += 1
            source_text = row.get("source_text")
            if not isinstance(source_text, str):
                raise TypeError(f"{document_id}: source_text must be a string")
            metrics = display_codec_stress_metrics(source_text)
            quote_metrics = straight_quote_stress_metrics(source_text)
            changed = int(metrics["changed_character_count"])
            control_changed = int(metrics["control_changed_character_count"])
            non_lf_c0 = int(metrics["non_lf_c0_count"])
            backslashes = int(metrics["backslash_count"])
            tabs = int(metrics["tab_count"])
            u0003 = int(metrics["u0003_count"])
            payload["display_codec_changed_character_count"] += changed
            payload["control_codec_changed_character_count"] += control_changed
            payload["non_lf_c0_character_count"] += non_lf_c0
            payload["backslash_character_count"] += backslashes
            payload["tab_character_count"] += tabs
            payload["u0003_character_count"] += u0003
            straight_quotes = int(quote_metrics["straight_quote_count"])
            payload["straight_quote_character_count"] += straight_quotes
            payload["display_codec_changed_segment_count"] += int(changed > 0)
            payload["control_codec_changed_segment_count"] += int(
                control_changed > 0
            )
            payload["non_lf_c0_segment_count"] += int(non_lf_c0 > 0)
            payload["backslash_segment_count"] += int(backslashes > 0)
            payload["tab_segment_count"] += int(tabs > 0)
            payload["u0003_segment_count"] += int(u0003 > 0)
            payload["straight_quote_segment_count"] += int(straight_quotes > 0)
            if document_id == STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID:
                if (
                    parent_id != STRUCTURED_WHITESPACE_FAILURE_PARENT_ID
                    or str(row["route"]) != "biography"
                    or straight_quotes < 1
                ):
                    raise ValueError(
                        "frozen structured-whitespace failure identity changed"
                    )
                payload["structured_whitespace_failure_segment_id"] = document_id
            if document_id == QUOTE_REPETITION_FAILURE_DOCUMENT_ID:
                if (
                    parent_id != QUOTE_REPETITION_FAILURE_PARENT_ID
                    or str(row["route"]) != "issues"
                    or straight_quotes < 1
                ):
                    raise ValueError("frozen quote-repetition failure identity changed")
                payload["quote_repetition_failure_segment_id"] = document_id
            payload["display_codec_codepoint_counts"].update(
                metrics["codepoint_counts"]
            )
            tab_lines = int(metrics["tab_nonempty_lines"])
            if tab_lines > int(payload["max_tab_segment_nonempty_lines"]) or (
                tab_lines > 0
                and tab_lines == int(payload["max_tab_segment_nonempty_lines"])
                and (
                    payload["max_tab_segment_id"] is None
                    or document_id < str(payload["max_tab_segment_id"])
                )
            ):
                payload["max_tab_segment_nonempty_lines"] = tab_lines
                payload["max_tab_segment_id"] = document_id
            if metrics["inline_tab"] and (
                payload["min_inline_tab_segment_n_chars"] is None
                or len(source_text) < payload["min_inline_tab_segment_n_chars"]
                or (
                    len(source_text) == payload["min_inline_tab_segment_n_chars"]
                    and document_id < str(payload["min_inline_tab_segment_id"])
                )
            ):
                payload["min_inline_tab_segment_n_chars"] = len(source_text)
                payload["min_inline_tab_segment_id"] = document_id
            if u0003 and (
                payload["min_u0003_segment_n_chars"] is None
                or len(source_text) < payload["min_u0003_segment_n_chars"]
                or (
                    len(source_text) == payload["min_u0003_segment_n_chars"]
                    and document_id < str(payload["min_u0003_segment_id"])
                )
            ):
                payload["min_u0003_segment_n_chars"] = len(source_text)
                payload["min_u0003_segment_id"] = document_id
    for payload in parents.values():
        payload["display_codec_codepoint_counts_json"] = json.dumps(
            dict(sorted(payload.pop("display_codec_codepoint_counts").items())),
            sort_keys=True,
            separators=(",", ":"),
        )
    frame = pd.DataFrame(parents.values())
    if frame.empty or set(frame["route"]) != set(ROUTES):
        raise ValueError("source export does not contain all three routes")
    return frame


def legacy_parent_status(path: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in read_jsonl_gz(path):
        rows.append(
            {
                "parent_document_id": str(row["parent_document_id"]),
                "legacy_parent_parse_status": str(row.get("parent_parse_status") or ""),
                "legacy_record_count": len(row.get("records") or []),
            }
        )
    frame = pd.DataFrame(rows)
    if frame["parent_document_id"].duplicated().any():
        raise ValueError("legacy parent extraction contains duplicate parent IDs")
    return frame


def legacy_segment_density(path: Path) -> pd.DataFrame:
    """Return each parent's densest individual v1 inference-window output."""
    list_keys = {
        "biography": "facts",
        "endorsements": "endorsements",
        "issues": "issue_blocks",
    }
    densest: dict[str, tuple[int, str]] = {}
    for row in read_jsonl_gz(path):
        parent_id = str(row["parent_document_id"])
        route = str(row["route"])
        extraction = row.get("extraction")
        records = (
            extraction.get(list_keys[route])
            if isinstance(extraction, dict)
            else None
        )
        count = len(records) if isinstance(records, list) else 0
        document_id = str(row["document_id"])
        current = densest.get(parent_id)
        if current is None or count > current[0] or (
            count == current[0] and document_id < current[1]
        ):
            densest[parent_id] = (count, document_id)
    frame = pd.DataFrame([
        {
            "parent_document_id": parent_id,
            "legacy_max_segment_record_count": count,
            "legacy_densest_segment_id": document_id,
        }
        for parent_id, (count, document_id) in densest.items()
    ])
    if frame.empty or frame["parent_document_id"].duplicated().any():
        raise ValueError("legacy segment extraction has an invalid parent inventory")
    return frame


def choose_per_route(
    pool: pd.DataFrame,
    count: int,
    seed: int,
    excluded: set[str],
) -> pd.DataFrame:
    selected: list[pd.DataFrame] = []
    available = pool.loc[~pool["parent_document_id"].astype(str).isin(excluded)].copy()
    available["_rank"] = available["parent_document_id"].map(
        lambda value: stable_score(seed, str(value))
    )
    for route in ROUTES:
        route_pool = available.loc[available["route"].eq(route)].sort_values("_rank")
        route_pool = route_pool.drop_duplicates("parent_document_id")
        if len(route_pool) < count:
            raise ValueError(f"not enough {route} controls: need {count}, found {len(route_pool)}")
        selected.append(route_pool.head(count))
    return pd.concat(selected, ignore_index=True).drop(columns="_rank")


def choose_worst_density_per_route(
    pool: pd.DataFrame,
    count: int,
    seed: int,
    excluded: set[str],
) -> pd.DataFrame:
    """Choose the densest legacy outputs, with seeded exact-ID tie breaks."""
    selected: list[pd.DataFrame] = []
    available = pool.loc[~pool["parent_document_id"].astype(str).isin(excluded)].copy()
    available["_rank"] = available["parent_document_id"].map(
        lambda value: stable_score(seed, str(value))
    )
    for route in ROUTES:
        route_pool = available.loc[available["route"].eq(route)].sort_values(
            ["legacy_max_segment_record_count", "_rank", "parent_document_id"],
            ascending=[False, True, True],
        )
        route_pool = route_pool.drop_duplicates("parent_document_id")
        if len(route_pool) < count:
            raise ValueError(
                f"not enough {route} worst-density controls: need {count}, "
                f"found {len(route_pool)}"
            )
        selected.append(route_pool.head(count))
    return pd.concat(selected, ignore_index=True).drop(columns="_rank")


def _with_display_metric_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    # Compatibility for focused synthetic inventories created before quotes
    # joined the display codec. Authoritative parent_inventory() always emits
    # the explicit control-only fields.
    for control_field, prior_field in (
        ("control_codec_changed_segment_count", "display_codec_changed_segment_count"),
        (
            "control_codec_changed_character_count",
            "display_codec_changed_character_count",
        ),
    ):
        if control_field not in output:
            output[control_field] = output.get(prior_field, 0)
        elif prior_field in output:
            output[control_field] = output[control_field].fillna(output[prior_field])
    integer_defaults = (
        "display_codec_changed_segment_count",
        "display_codec_changed_character_count",
        "control_codec_changed_segment_count",
        "control_codec_changed_character_count",
        "non_lf_c0_segment_count",
        "non_lf_c0_character_count",
        "backslash_segment_count",
        "backslash_character_count",
        "tab_segment_count",
        "tab_character_count",
        "u0003_segment_count",
        "u0003_character_count",
        "straight_quote_segment_count",
        "straight_quote_character_count",
        "max_tab_segment_nonempty_lines",
    )
    for field in integer_defaults:
        if field not in output:
            output[field] = 0
        output[field] = output[field].fillna(0).astype(int)
    nullable_defaults = (
        "max_tab_segment_id",
        "min_inline_tab_segment_n_chars",
        "min_inline_tab_segment_id",
        "min_u0003_segment_n_chars",
        "min_u0003_segment_id",
        "structured_whitespace_failure_segment_id",
        "quote_repetition_failure_segment_id",
    )
    for field in nullable_defaults:
        if field not in output:
            output[field] = None
    if "display_codec_codepoint_counts_json" not in output:
        output["display_codec_codepoint_counts_json"] = "{}"
    return output


def _assign_named_display_controls(
    selection: pd.DataFrame, seed: int
) -> pd.DataFrame:
    """Name three deterministic controls without hardcoding a document ID."""
    result = selection.copy()
    result["pilot_control_kind"] = ""
    result["pilot_control_segment_id"] = ""
    if not result["control_codec_changed_segment_count"].gt(0).any():
        return result
    result["_display_rank"] = result["parent_document_id"].map(
        lambda value: stable_score(seed + 4, str(value))
    )
    dense_pool = result.loc[
        result["route"].eq("endorsements")
        & result["tab_segment_count"].gt(0)
    ].sort_values(
        [
            "legacy_max_segment_record_count",
            "max_tab_segment_nonempty_lines",
            "tab_character_count",
            "_display_rank",
            "parent_document_id",
        ],
        ascending=[False, False, False, True, True],
    )
    if dense_pool.empty:
        raise ValueError("display stress set lacks a tabbed endorsement parent")
    dense_index = dense_pool.index[0]
    dense_parent = str(result.at[dense_index, "parent_document_id"])
    dense_cycle = str(result.at[dense_index, "candidate_cycle_id"])

    small_pool = result.loc[
        result["route"].eq("endorsements")
        & result["min_inline_tab_segment_n_chars"].notna()
        & ~result["parent_document_id"].astype(str).eq(dense_parent)
        & ~result["candidate_cycle_id"].astype(str).eq(dense_cycle)
    ].sort_values(
        ["min_inline_tab_segment_n_chars", "_display_rank", "parent_document_id"]
    )
    if small_pool.empty:
        raise ValueError("display stress set lacks a distinct small inline-tab control")
    small_index = small_pool.index[0]

    used = {
        dense_parent,
        str(result.at[small_index, "parent_document_id"]),
    }
    u0003_pool = result.loc[
        result["u0003_segment_count"].gt(0)
        & ~result["parent_document_id"].astype(str).isin(used)
    ].sort_values(
        ["min_u0003_segment_n_chars", "_display_rank", "parent_document_id"]
    )
    if u0003_pool.empty:
        raise ValueError("display stress set lacks a distinct U+0003 control")
    u0003_index = u0003_pool.index[0]

    controls = (
        (dense_index, "dense_tab_endorsement", "max_tab_segment_id"),
        (small_index, "small_tab_endorsement", "min_inline_tab_segment_id"),
        (u0003_index, "u0003", "min_u0003_segment_id"),
    )
    for index, kind, segment_field in controls:
        segment_id = result.at[index, segment_field]
        if not isinstance(segment_id, str) or not segment_id:
            raise ValueError(f"{kind} lacks a deterministic control segment")
        result.at[index, "pilot_control_kind"] = kind
        result.at[index, "pilot_control_segment_id"] = segment_id
    return result.drop(columns="_display_rank")


def _assign_structured_whitespace_failure_control(
    selection: pd.DataFrame,
) -> pd.DataFrame:
    """Name the preserved production failure when its frozen parent is present."""
    result = selection.copy()
    mask = result["parent_document_id"].astype(str).eq(
        STRUCTURED_WHITESPACE_FAILURE_PARENT_ID
    )
    if not mask.any():
        return result
    if int(mask.sum()) != 1:
        raise ValueError("structured-whitespace failure parent is not unique")
    index = result.index[mask][0]
    segment_id = result.at[index, "structured_whitespace_failure_segment_id"]
    if segment_id != STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID:
        raise ValueError("structured-whitespace failure segment is absent")
    if str(result.at[index, "route"]) != "biography":
        raise ValueError("structured-whitespace failure route changed")
    if str(result.at[index, "pilot_control_kind"]):
        raise ValueError("structured-whitespace failure collides with a named control")
    result.at[index, "pilot_control_kind"] = STRUCTURED_WHITESPACE_FAILURE_CONTROL
    result.at[index, "pilot_control_segment_id"] = (
        STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID
    )
    return result


def _assign_quote_repetition_failure_control(
    selection: pd.DataFrame,
) -> pd.DataFrame:
    """Name the frozen issue quote/repetition truncation control."""
    result = selection.copy()
    mask = result["parent_document_id"].astype(str).eq(
        QUOTE_REPETITION_FAILURE_PARENT_ID
    )
    if not mask.any():
        return result
    if int(mask.sum()) != 1:
        raise ValueError("quote-repetition failure parent is not unique")
    index = result.index[mask][0]
    segment_id = result.at[index, "quote_repetition_failure_segment_id"]
    if segment_id != QUOTE_REPETITION_FAILURE_DOCUMENT_ID:
        raise ValueError("quote-repetition failure segment is absent")
    if str(result.at[index, "route"]) != "issues":
        raise ValueError("quote-repetition failure route changed")
    if str(result.at[index, "pilot_control_kind"]):
        raise ValueError("quote-repetition failure collides with a named control")
    result.at[index, "pilot_control_kind"] = QUOTE_REPETITION_FAILURE_CONTROL
    result.at[index, "pilot_control_segment_id"] = (
        QUOTE_REPETITION_FAILURE_DOCUMENT_ID
    )
    return result


def build_selection(
    inventory: pd.DataFrame,
    legacy: pd.DataFrame,
    completed_recall: pd.DataFrame,
    completed_precision: pd.DataFrame,
    seed: int = DEFAULT_SEED,
) -> pd.DataFrame:
    merged = inventory.merge(legacy, on="parent_document_id", how="left", validate="one_to_one")
    merged = _with_display_metric_defaults(merged)
    required_legacy = {
        "legacy_record_count", "legacy_max_segment_record_count",
        "legacy_densest_segment_id",
    }
    missing_legacy = required_legacy - set(merged.columns)
    if missing_legacy:
        raise ValueError(f"legacy extraction lacks density fields: {sorted(missing_legacy)}")
    if merged[list(required_legacy)].isna().any().any():
        raise ValueError("legacy extraction does not cover every source parent")

    tuning_ids = set(completed_recall["parent_document_id"].astype(str))
    tuning = merged.loc[merged["parent_document_id"].isin(tuning_ids)].copy()
    if len(tuning) != 15 or tuning.groupby("route").size().to_dict() != {
        "biography": 5,
        "endorsements": 5,
        "issues": 5,
    }:
        raise ValueError("completed recall file is not the frozen balanced 15-page tuning set")
    tuning["pilot_group"] = "known_tuning"
    selected_ids = set(tuning["parent_document_id"])

    # Retain completed_precision as a frozen provenance input to build_pilot,
    # but replace its lower-value passing control with one worst-density parent
    # per route. The legacy count is the deterministic primary rank.
    _ = completed_precision
    dense_pool = merged.loc[merged["legacy_parent_parse_status"].eq("parsed")]
    dense = choose_worst_density_per_route(dense_pool, 1, seed + 1, selected_ids)
    dense["pilot_group"] = "worst_density_control"
    selected_ids.update(dense["parent_document_id"])

    zero_pool = merged.loc[
        merged["legacy_parent_parse_status"].eq("parsed")
        & merged["legacy_record_count"].eq(0)
    ]
    zero = choose_per_route(zero_pool, 3, seed + 2, selected_ids)
    zero["pilot_group"] = "zero_result_control"
    selected_ids.update(zero["parent_document_id"])

    multi_pool = merged.loc[
        merged["legacy_parent_parse_status"].eq("parsed")
        & merged["segment_count"].gt(1)
    ]
    multi = choose_per_route(multi_pool, 3, seed + 3, selected_ids)
    multi["pilot_group"] = "multi_segment_control"

    base = pd.concat([tuning, dense, zero, multi], ignore_index=True)
    if base["parent_document_id"].duplicated().any():
        raise ValueError("pilot selection contains a parent in multiple groups")
    route_counts = base.groupby("route").size().to_dict()
    if route_counts != {"biography": 12, "endorsements": 12, "issues": 12}:
        raise ValueError(f"pilot route balance failed: {route_counts}")
    base_ids = set(base["parent_document_id"].astype(str))
    stress = merged.loc[
        merged["control_codec_changed_segment_count"].gt(0)
        & ~merged["parent_document_id"].astype(str).isin(base_ids)
    ].copy()
    stress["pilot_group"] = DISPLAY_STRESS_GROUP
    prior = pd.concat([base, stress], ignore_index=True)
    prior_ids = set(prior["parent_document_id"].astype(str))
    quote_stress = merged.loc[
        merged["straight_quote_segment_count"].gt(0)
        & ~merged["parent_document_id"].astype(str).isin(prior_ids)
    ].copy()
    quote_stress["pilot_group"] = STRAIGHT_QUOTE_STRESS_GROUP
    result = pd.concat([prior, quote_stress], ignore_index=True)
    result["display_codec_stress"] = result[
        "display_codec_changed_segment_count"
    ].gt(0)
    result["straight_quote_whitespace_stress"] = result[
        "straight_quote_segment_count"
    ].gt(0)
    result = _assign_named_display_controls(result, seed)
    result = _assign_structured_whitespace_failure_control(result)
    result = _assign_quote_repetition_failure_control(result)
    if result["parent_document_id"].duplicated().any():
        raise ValueError("stress pilot selection contains a duplicate parent")
    return result.sort_values(
        ["route", "pilot_group", "parent_document_id"]
    ).reset_index(drop=True)


def known_omissions(completed_recall: pd.DataFrame) -> list[dict[str, Any]]:
    failures = completed_recall.loc[
        completed_recall["all_requested_records_captured"]
        .fillna("")
        .astype(str)
        .str.casefold()
        .eq("no")
    ]
    omissions: list[dict[str, Any]] = []
    for row in failures.to_dict("records"):
        missed = json.loads(str(row["missed_records_json"]))
        for index, item in enumerate(missed):
            evidence = str(item["evidence"])
            omissions.append(
                {
                    "omission_id": hashlib.sha256(
                        f"{row['parent_document_id']}\n{index}\n{evidence}".encode("utf-8")
                    ).hexdigest(),
                    "parent_document_id": str(row["parent_document_id"]),
                    "candidate_cycle_id": str(row["candidate_cycle_id"]),
                    "route": str(row["route"]),
                    "expected_type": str(item.get("type") or ""),
                    "expected_value": str(item.get("value") or ""),
                    "evidence": evidence,
                    "source": "frozen_ai_assisted_tuning_audit",
                }
            )
    if len(omissions) != 11:
        raise ValueError(f"expected 11 known omissions, found {len(omissions)}")
    return omissions


def build_structured_whitespace_preflight(
    full_export_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Build the exact one-window reproducer for the preserved truncation."""
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"refusing nonempty output directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir = output_dir / "chunks"
    chunks_dir.mkdir()

    source_manifest, chunks = source_chunk_paths(full_export_dir)
    matches = [
        v2_1_source_row(row)
        for path in chunks
        for row in read_jsonl_gz(path)
        if str(row.get("document_id"))
        == STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID
    ]
    if len(matches) != 1:
        raise ValueError(
            "frozen structured-whitespace failure window is not unique in source"
        )
    row = matches[0]
    if (
        str(row.get("parent_document_id"))
        != STRUCTURED_WHITESPACE_FAILURE_PARENT_ID
        or str(row.get("route")) != "biography"
        or int(row.get("segment_count", 0)) != 1
        or '"' not in str(row.get("source_text"))
    ):
        raise ValueError("frozen structured-whitespace failure source changed")

    chunk_path = chunks_dir / "chunk_00000.jsonl.gz"
    write_jsonl_gz(chunk_path, [row])
    selection = pd.DataFrame([{
        "pilot_group": STRAIGHT_QUOTE_STRESS_GROUP,
        "pilot_control_kind": STRUCTURED_WHITESPACE_FAILURE_CONTROL,
        "pilot_control_segment_id": STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID,
        "document_id": STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID,
        "parent_document_id": STRUCTURED_WHITESPACE_FAILURE_PARENT_ID,
        "candidate_cycle_id": str(row["candidate_cycle_id"]),
        "candidate_icpsr": str(row["candidate_icpsr"]),
        "route": str(row["route"]),
        "segment_index": int(row["segment_index"]),
        "segment_count": int(row["segment_count"]),
        "source_text_characters": len(str(row["source_text"])),
        "straight_quote_characters": str(row["source_text"]).count('"'),
    }])
    selection.to_csv(output_dir / "selection.csv", index=False)
    manifest = {
        **pilot_manifest_source_metadata(
            source_manifest,
            int(row.get("source_text_transform_changed") is True),
        ),
        "mode": "v2_1_structured_whitespace_preflight",
        "seed": DEFAULT_SEED,
        "source_corpus_sha256": source_manifest["source_corpus_sha256"],
        "source_export_manifest_sha256": sha256_file(
            full_export_dir / "manifest.json"
        ),
        "candidate_metadata_interface": "candidate_icpsr_labeled_candidate_id",
        "parent_documents": 1,
        "exported_records": 1,
        "records_by_route": {"biography": 1},
        "target_runtime_policy": {
            "backend": "xgrammar",
            "disable_any_whitespace": True,
            "scope": "JSON structural whitespace only; string content unchanged",
        },
        "failure_control": selection.iloc[0].to_dict(),
        "failed_production_job_id": "21296223",
        "failed_output_chunk": "chunk_00091.out.jsonl.gz",
        "failed_finish_reason": "length",
        "failed_output_tokens": 24_576,
        "failed_trailing_tab_characters": 23_268,
        "chunk_size": 1,
        "chunks": [{
            "file": chunk_path.name,
            "records": 1,
            "sha256": sha256_file(chunk_path),
        }],
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    )
    return manifest


def build_quote_cap_preflight(
    full_export_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Build the frozen all-route quote/cap/repetition runtime preflight."""
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"refusing nonempty output directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir = output_dir / "chunks"
    chunks_dir.mkdir()

    source_manifest, chunks = source_chunk_paths(full_export_dir)
    expected_ids = set(QUOTE_CAP_PREFLIGHT_DOCUMENT_IDS)
    matches: dict[str, dict[str, Any]] = {}
    for path in chunks:
        for raw_row in read_jsonl_gz(path):
            document_id = str(raw_row.get("document_id"))
            if document_id not in expected_ids:
                continue
            if document_id in matches:
                raise ValueError(f"duplicate preflight document ID: {document_id}")
            matches[document_id] = v2_1_source_row(raw_row)
    missing = sorted(expected_ids - set(matches))
    if missing:
        raise ValueError(f"quote/cap preflight controls absent from source: {missing}")

    rows = sorted(
        matches.values(),
        key=lambda row: (str(row["route"]), str(row["document_id"])),
    )
    by_route = Counter(str(row["route"]) for row in rows)
    expected_by_route = {"biography": 4, "endorsements": 15, "issues": 25}
    if dict(sorted(by_route.items())) != expected_by_route:
        raise ValueError(f"quote/cap preflight route counts changed: {by_route}")
    if str(matches[QUOTE_REPETITION_FAILURE_DOCUMENT_ID]["parent_document_id"]) != (
        QUOTE_REPETITION_FAILURE_PARENT_ID
    ):
        raise ValueError("quote-repetition failure parent changed")
    for document_id in (
        *QUOTE_COPULA_ISSUE_DOCUMENT_IDS,
        *QUOTE_REGRESSION_ISSUE_DOCUMENT_IDS,
    ):
        if '"' not in str(matches[document_id]["source_text"]):
            raise ValueError(f"quote regression control lost its quote: {document_id}")
    expected_control_routes = {
        EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID: "issues",
        EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID: "biography",
        EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID: "endorsements",
        EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID: "endorsements",
        QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID: "endorsements",
    }
    for document_id, expected_route in expected_control_routes.items():
        row = matches[document_id]
        if str(row["route"]) != expected_route:
            raise ValueError(
                f"exact-anchor control route changed: {document_id}"
            )
        expected_parent_id = (
            QUOTE_BEARING_ENDORSEMENT_NAME_PARENT_ID
            if document_id == QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID
            else EXACT_ANCHOR_ABLATION_PARENT_IDS[document_id]
        )
        if str(row["parent_document_id"]) != expected_parent_id:
            raise ValueError(
                f"exact-anchor control parent changed: {document_id}"
            )
    for document_id in (
        EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID,
        EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID,
        EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID,
        QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID,
    ):
        if '"' not in str(matches[document_id]["source_text"]):
            raise ValueError(
                f"exact-anchor quote control lost its quote: {document_id}"
            )
    for document_id in (
        EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID,
        EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID,
        QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID,
    ):
        context = matches[document_id].get("endorsement_context_provenance")
        if not isinstance(context, dict) or context.get("source_scope") != "segment":
            raise ValueError(
                f"endorsement control lost segment-scoped context: {document_id}"
            )
    quote_endorsement = matches[QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID]
    if (
        str(quote_endorsement["source_text"]).count(
            QUOTE_BEARING_ENDORSEMENT_NAME_EVIDENCE
        ) != 1
        or int(quote_endorsement["segment_index"]) != 0
        or int(quote_endorsement["segment_count"]) != 1
        or quote_endorsement["endorsement_context_provenance"].get(
            "context_text"
        ) != "Endorsements"
    ):
        raise ValueError("quote-bearing endorsement-name control changed")
    dense_nonempty_lines = sum(
        bool(line.strip())
        for line in str(
            matches[EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID][
                "source_text"
            ]
        ).splitlines()
    )
    if dense_nonempty_lines < 200:
        raise ValueError("dense endorsement control fell below 200 nonempty lines")

    selected_parent_ids = {
        str(row["parent_document_id"]) for row in rows
    }
    for parent_id in sorted(selected_parent_ids):
        parent_rows = [
            row for row in rows if str(row["parent_document_id"]) == parent_id
        ]
        expected_count = int(parent_rows[0]["segment_count"])
        if len(parent_rows) != expected_count or sorted(
            int(row["segment_index"]) for row in parent_rows
        ) != list(range(expected_count)):
            raise ValueError(
                f"exact-anchor control parent is not complete: {parent_id}"
            )

    chunk_path = chunks_dir / "chunk_00000.jsonl.gz"
    write_jsonl_gz(chunk_path, rows)
    selection_rows = []
    copula_ids = set(QUOTE_COPULA_ISSUE_DOCUMENT_IDS)
    regression_ids = set(QUOTE_REGRESSION_ISSUE_DOCUMENT_IDS)
    for row in rows:
        document_id = str(row["document_id"])
        if document_id == EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID:
            control_kind = "issue_boundary_parent_coverage"
        elif document_id == EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID:
            control_kind = "quote_bearing_biography_evidence"
        elif document_id == EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID:
            control_kind = "testimonial_separate_speaker_page_context"
        elif document_id == EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID:
            control_kind = "dense_segment_context_endorsement"
        elif document_id == QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID:
            control_kind = "quote_bearing_endorsement_name_evidence"
        elif document_id == QUOTE_REPETITION_FAILURE_DOCUMENT_ID:
            control_kind = QUOTE_REPETITION_FAILURE_CONTROL
        elif document_id == MAX_COMPLEXITY_BIOGRAPHY_DOCUMENT_ID:
            control_kind = "max_complexity_biography"
        elif document_id == MAX_DENSITY_ENDORSEMENT_DOCUMENT_ID:
            control_kind = "max_density_endorsement"
        elif document_id in regression_ids:
            control_kind = "quote_escape_regression"
        elif document_id in copula_ids:
            control_kind = "quote_copula_peer"
        elif document_id in EXACT_ANCHOR_ABLATION_PARENT_CLOSURE_DOCUMENT_IDS:
            control_kind = "exact_anchor_parent_closure"
        elif document_id in QUOTE_CAP_PREFLIGHT_DOCUMENT_IDS:
            control_kind = "preflight_parent_closure"
        else:
            raise AssertionError(f"unclassified preflight control: {document_id}")
        selection_rows.append({
            "pilot_group": "quote_cap_repetition_preflight",
            "pilot_control_kind": control_kind,
            "document_id": document_id,
            "parent_document_id": str(row["parent_document_id"]),
            "candidate_cycle_id": str(row["candidate_cycle_id"]),
            "candidate_icpsr": str(row["candidate_icpsr"]),
            "route": str(row["route"]),
            "segment_index": int(row["segment_index"]),
            "segment_count": int(row["segment_count"]),
            "source_text_characters": len(str(row["source_text"])),
            "straight_quote_characters": str(row["source_text"]).count('"'),
        })
    selection = pd.DataFrame(selection_rows)
    selection.to_csv(output_dir / "selection.csv", index=False)
    contract = get_extraction_contract("2.1")
    manifest = {
        **pilot_manifest_source_metadata(
            source_manifest,
            len({
                str(row["parent_document_id"])
                for row in rows
                if row.get("source_text_transform_changed") is True
            }),
        ),
        "mode": "v2_1_quote_cap_repetition_preflight",
        "seed": DEFAULT_SEED,
        "source_corpus_sha256": source_manifest["source_corpus_sha256"],
        "source_export_manifest_sha256": sha256_file(
            full_export_dir / "manifest.json"
        ),
        "candidate_metadata_interface": "candidate_icpsr_labeled_candidate_id",
        "target_extraction_contract": contract,
        "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
        "inference_display_escape_spec_sha256": (
            INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
        ),
        "model_item_guards": {
            "route_array_item_caps": dict(V2_1_MODEL_ITEM_CAPS),
            "array_item_cap_spec_id": ARRAY_ITEM_CAP_SPEC_ID,
            "array_item_cap_spec_sha256": ARRAY_ITEM_CAP_SPEC_SHA256,
            "model_item_repetition_spec_id": MODEL_ITEM_REPETITION_SPEC_ID,
            "model_item_repetition_spec_sha256": (
                MODEL_ITEM_REPETITION_SPEC_SHA256
            ),
        },
        "parent_documents": len({str(row["parent_document_id"]) for row in rows}),
        "exported_records": len(rows),
        "records_by_route": dict(sorted(by_route.items())),
        "expected_control_ids": list(QUOTE_CAP_PREFLIGHT_DOCUMENT_IDS),
        "preflight_revision": (
            "exact_display_context_projection_and_segment_boundary_recall_v2"
        ),
        "exact_copy_anchor_guidance_ablation": {
            "guided_schema_fields_without_min_length": {
                "issues": ["evidence"],
                "biography": ["evidence"],
                "endorsements": [
                    "record_evidence",
                    "explicit_text.endorsement_context_evidence",
                ],
            },
            "python_nonblank_gate_unchanged": True,
            "public_empty_or_whitespace_anchor_allowed": False,
            "named_control_document_ids": list(
                EXACT_ANCHOR_ABLATION_CONTROL_DOCUMENT_IDS
            ),
            "exact_anchor_control_parent_ids": sorted(
                set(EXACT_ANCHOR_ABLATION_PARENT_IDS.values())
            ),
            "exact_anchor_parent_closure_document_ids": list(
                EXACT_ANCHOR_ABLATION_PARENT_CLOSURE_DOCUMENT_IDS
            ),
            "exact_anchor_parent_closure_record_count": len(
                EXACT_ANCHOR_ABLATION_PARENT_CLOSURE_DOCUMENT_IDS
            ),
            "all_selected_parents_complete": True,
            "complete_selected_parent_ids": sorted(selected_parent_ids),
            "parent_level_zero_or_coverage_interpretation_allowed": True,
        },
        "endorsement_dense_recall_correction": {
            "guided_schema_max_items": {
                "issues": V2_1_MODEL_ITEM_CAPS["issues"],
                "biography": V2_1_MODEL_ITEM_CAPS["biography"],
                "endorsements": None,
            },
            "post_generation_caps_unchanged": dict(V2_1_MODEL_ITEM_CAPS),
            "exact_text_scope_continuation_instruction": True,
            "exact_display_inverse_alignment": True,
            "exact_segment_context_extension_projected_to_carried_span": True,
            "leading_and_trailing_partial_line_withholding": True,
            "segment_boundary_provenance_in_user_message": True,
            "segment_has_complete_line_with_nullable_source_parent_and_display_bounds": True,
            "operative_prompt_bounds_use_inference_display_coordinates": True,
            "restart_binds_exact_user_message_sha256": True,
            "fixed_structural_spacing_unchanged": True,
            "predeclared_content_gates": {
                MAX_DENSITY_ENDORSEMENT_DOCUMENT_ID: {
                    "minimum_non_candidate_unique_identities": 224,
                    "exclude_exact": ["Greg Tanaka"],
                },
                "1aad8662c07cd4f1f6343f78fe04b5f460235f17ecf57e8e5faa43024819fde8": {
                    "minimum_non_candidate_unique_identities": 390,
                    "exclude_exact": ["Greg Tanaka"],
                },
                "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73": {
                    "minimum_model_unique_identities": 86,
                    "minimum_uniquely_aligned_identities": 84,
                    "exclude_exact": ["ena Song", "Greg Tanaka"],
                    "exclude_page_interface_text": True,
                },
                "e56c1b40a3bc139cc9ac53e347673ab061bdc26da15a4333e928571aeff4eb32": {
                    "minimum_model_unique_identities": 71,
                    "minimum_uniquely_aligned_identities": 69,
                    "exclude_exact": ["cy Ruffeno"],
                },
                EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID: {
                    "minimum_model_items": 180,
                    "minimum_aligned_items": 171,
                },
                "df9abdfcb024b2e62dc57ceb585f6b233d23764a36704930b1e18de376d3eeb5": {
                    "minimum_unique_valid_identities": 222,
                    "reference_unique_valid_identities": 230,
                    "exclude_clipped_partials": True,
                },
                EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID: {
                    "minimum_aligned_items": 11,
                    "must_include_exact": ["Vets for Trump"],
                },
                QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID: {
                    "must_resolve_quote_bearing_record_evidence": (
                        QUOTE_BEARING_ENDORSEMENT_NAME_EVIDENCE
                    ),
                },
                EXACT_ANCHOR_ABLATION_PARENT_IDS[
                    EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID
                ]: {
                    "minimum_quote_bearing_resolved_issue_records": 2,
                    "minimum_fully_accounted_private_conflict_groups": 1,
                    "required_conflict_statement_kinds": ["proposal", "oppose"],
                    "conflict_disposition": "conflict_withheld_private",
                },
            },
        },
        "control_kind_counts": dict(sorted(Counter(
            selection["pilot_control_kind"].astype(str)
        ).items())),
        "failed_preflight_job_id": "21424457",
        "most_recent_failed_preflight_job_id": "21426930",
        "prior_failed_preflight_job_id": "21422367",
        "superseded_preflight_no_go": {
            "job_id": "21424457",
            "technical_gate_passed": True,
            "endorsement_recall_failure_document_ids": [
                MAX_DENSITY_ENDORSEMENT_DOCUMENT_ID,
                "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73",
                "e56c1b40a3bc139cc9ac53e347673ab061bdc26da15a4333e928571aeff4eb32",
            ],
            "stress_rerun_submitted": False,
        },
        "superseded_suffix_o_no_go": {
            "job_id": "21426930",
            "technical_gate_passed": True,
            "content_gate_passed": False,
            "failed_controls": {
                "2e9a0cd8": "true_generation_omission",
                "e56c1b40": "clipped_leading_partial_was_public",
                "a8885c81": "exact_context_extension_was_not_projected",
                "36a087fa": "canonical_display_quote_was_not_inverted",
                "71c197e1": "old_resolved_count_gate_ignored_private_conflict",
            },
            "stress_rerun_submitted": False,
        },
        "historical_quote_repetition_failure": {
            "job_id": "21304738",
            "document_id": QUOTE_REPETITION_FAILURE_DOCUMENT_ID,
            "finish_reason": "length",
            "output_tokens": 24_576,
        },
        "chunk_size": len(rows),
        "chunks": [{
            "file": chunk_path.name,
            "records": len(rows),
            "sha256": sha256_file(chunk_path),
        }],
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    )
    return manifest


def build_pilot(
    full_export_dir: Path,
    legacy_parent_path: Path,
    completed_validation_dir: Path,
    output_dir: Path,
    seed: int = DEFAULT_SEED,
    chunk_size: int = 60,
    legacy_segment_path: Path | None = None,
) -> dict[str, Any]:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"refusing nonempty output directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir = output_dir / "chunks"
    chunks_dir.mkdir()

    source_manifest, chunks = source_chunk_paths(full_export_dir)
    inventory = parent_inventory(chunks)
    if legacy_segment_path is None:
        legacy_segment_path = legacy_parent_path.with_name("full_extractions.jsonl.gz")
    legacy = legacy_parent_status(legacy_parent_path).merge(
        legacy_segment_density(legacy_segment_path),
        on="parent_document_id",
        how="left",
        validate="one_to_one",
    )
    recall_path = completed_validation_dir / "page_recall_coding.csv"
    precision_path = completed_validation_dir / "record_precision_coding.csv"
    recall = pd.read_csv(recall_path)
    precision = pd.read_csv(precision_path)
    selection = build_selection(inventory, legacy, recall, precision, seed)
    selection.to_csv(output_dir / "selection.csv", index=False)
    omissions = known_omissions(recall)
    (output_dir / "known_omissions.json").write_text(
        json.dumps(omissions, indent=2, ensure_ascii=False) + "\n"
    )

    selected_ids = set(selection["parent_document_id"].astype(str))
    rows: list[dict[str, Any]] = []
    for path in chunks:
        for row in read_jsonl_gz(path):
            parent_id = str(row.get("parent_document_id") or row["document_id"])
            if parent_id in selected_ids:
                rows.append(v2_1_source_row(row))
    rows.sort(key=lambda row: (str(row["route"]), str(row.get("parent_document_id")), int(row.get("segment_index", 0))))
    observed_selected_ids = {
        str(row.get("parent_document_id") or row["document_id"]) for row in rows
    }
    if observed_selected_ids != selected_ids:
        raise ValueError("selected pilot segments do not cover every selected parent")

    chunk_entries: list[dict[str, Any]] = []
    for start in range(0, len(rows), chunk_size):
        part = rows[start : start + chunk_size]
        path = chunks_dir / f"chunk_{start // chunk_size:05d}.jsonl.gz"
        write_jsonl_gz(path, part)
        chunk_entries.append(
            {"file": path.name, "records": len(part), "sha256": sha256_file(path)}
        )

    by_route = Counter(str(row["route"]) for row in rows)
    parent_by_route = selection.groupby("route").size().to_dict()
    transformed_pilot_parents = {
        str(row.get("parent_document_id") or row["document_id"])
        for row in rows
        if row.get("source_text_transform_changed") is True
    }
    base_selection = selection.loc[
        ~selection["pilot_group"].isin(
            [DISPLAY_STRESS_GROUP, STRAIGHT_QUOTE_STRESS_GROUP]
        )
    ]
    prior_stress_selection = selection.loc[
        ~selection["pilot_group"].eq(STRAIGHT_QUOTE_STRESS_GROUP)
    ]
    changed_inventory = inventory.loc[
        inventory["display_codec_changed_segment_count"].gt(0)
    ]
    control_changed_inventory = inventory.loc[
        inventory["control_codec_changed_segment_count"].gt(0)
    ]
    quote_inventory = inventory.loc[
        inventory["straight_quote_segment_count"].gt(0)
    ]
    base_ids = set(base_selection["parent_document_id"].astype(str))
    changed_ids = set(changed_inventory["parent_document_id"].astype(str))
    control_changed_ids = set(
        control_changed_inventory["parent_document_id"].astype(str)
    )
    quote_ids = set(quote_inventory["parent_document_id"].astype(str))
    if not changed_ids.issubset(selected_ids):
        raise ValueError("pilot selection does not cover every display-changed parent")
    if not quote_ids.issubset(selected_ids):
        raise ValueError("pilot selection does not cover every straight-quote parent")
    base_segment_count = int(base_selection["segment_count"].sum())
    changed_segment_count = int(
        changed_inventory["display_codec_changed_segment_count"].sum()
    )
    control_changed_segment_count = int(
        control_changed_inventory["control_codec_changed_segment_count"].sum()
    )
    overlap_changed_segment_count = int(
        inventory.loc[
            inventory["parent_document_id"].astype(str).isin(base_ids),
            "display_codec_changed_segment_count",
        ].sum()
    )
    changed_or_base_segment_count = (
        base_segment_count + changed_segment_count - overlap_changed_segment_count
    )
    prior_stress_ids = set(
        prior_stress_selection["parent_document_id"].astype(str)
    )
    prior_stress_segment_count = int(prior_stress_selection["segment_count"].sum())
    quote_segment_count = int(quote_inventory["straight_quote_segment_count"].sum())
    quote_complete_parent_segment_count = int(quote_inventory["segment_count"].sum())
    quote_parent_overlap_with_prior = quote_ids & prior_stress_ids
    quote_complete_segment_overlap_with_prior = int(
        inventory.loc[
            inventory["parent_document_id"].astype(str).isin(
                quote_parent_overlap_with_prior
            ),
            "segment_count",
        ].sum()
    )
    quote_segment_overlap_with_prior = int(
        inventory.loc[
            inventory["parent_document_id"].astype(str).isin(prior_stress_ids),
            "straight_quote_segment_count",
        ].sum()
    )
    quote_or_prior_exact_segment_count = (
        prior_stress_segment_count
        + quote_segment_count
        - quote_segment_overlap_with_prior
    )
    codepoint_counts: Counter[str] = Counter()
    for counts_json in inventory["display_codec_codepoint_counts_json"]:
        codepoint_counts.update(json.loads(str(counts_json)))
    controls = selection.loc[selection["pilot_control_kind"].ne("")]
    named_controls = [
        {
            "kind": str(row["pilot_control_kind"]),
            "parent_document_id": str(row["parent_document_id"]),
            "candidate_cycle_id": str(row["candidate_cycle_id"]),
            "route": str(row["route"]),
            "segment_id": str(row["pilot_control_segment_id"]),
        }
        for row in controls.sort_values("pilot_control_kind").to_dict("records")
    ]
    display_named_controls = [
        item
        for item in named_controls
        if item["kind"] in {
            "dense_tab_endorsement", "small_tab_endorsement", "u0003",
        }
    ]
    structured_whitespace_failure_control = next(
        (
            item
            for item in named_controls
            if item["kind"] == STRUCTURED_WHITESPACE_FAILURE_CONTROL
        ),
        None,
    )
    quote_repetition_failure_control = next(
        (
            item
            for item in named_controls
            if item["kind"] == QUOTE_REPETITION_FAILURE_CONTROL
        ),
        None,
    )
    expected_named_controls = set()
    if control_changed_ids:
        expected_named_controls.update({
            "dense_tab_endorsement", "small_tab_endorsement", "u0003",
        })
    if STRUCTURED_WHITESPACE_FAILURE_PARENT_ID in set(
        inventory["parent_document_id"].astype(str)
    ):
        expected_named_controls.add(STRUCTURED_WHITESPACE_FAILURE_CONTROL)
    if QUOTE_REPETITION_FAILURE_PARENT_ID in set(
        inventory["parent_document_id"].astype(str)
    ):
        expected_named_controls.add(QUOTE_REPETITION_FAILURE_CONTROL)
    if {item["kind"] for item in named_controls} != expected_named_controls:
        raise ValueError("stress pilot does not contain all named controls")
    if structured_whitespace_failure_control is None:
        raise ValueError(
            "full source export lacks the frozen structured-whitespace failure control"
        )
    if quote_repetition_failure_control is None:
        raise ValueError(
            "full source export lacks the frozen quote-repetition failure control"
        )
    contract = get_extraction_contract("2.1")
    manifest = {
        # Pilot rows use the same source-export wire format as the full export.
        # The model-output contract is recorded separately below.
        **pilot_manifest_source_metadata(
            source_manifest, len(transformed_pilot_parents)
        ),
        "mode": "v2_1_pilot",
        "seed": seed,
        "source_corpus_sha256": source_manifest["source_corpus_sha256"],
        "source_export_manifest_sha256": sha256_file(full_export_dir / "manifest.json"),
        "source_export_schema_version": source_manifest["schema_version"],
        "candidate_metadata_interface": "candidate_icpsr_labeled_candidate_id",
        "target_extraction_contract": contract,
        "model_item_guards": {
            "route_array_item_caps": dict(V2_1_MODEL_ITEM_CAPS),
            "array_item_cap_spec_id": ARRAY_ITEM_CAP_SPEC_ID,
            "array_item_cap_spec_sha256": ARRAY_ITEM_CAP_SPEC_SHA256,
            "model_item_repetition_spec_id": MODEL_ITEM_REPETITION_SPEC_ID,
            "model_item_repetition_spec_sha256": (
                MODEL_ITEM_REPETITION_SPEC_SHA256
            ),
        },
        "legacy_segment_extraction_sha256": sha256_file(legacy_segment_path),
        "completed_recall_sha256": sha256_file(recall_path),
        "completed_precision_sha256": sha256_file(precision_path),
        "parent_documents": len(selection),
        "parent_documents_by_route": {
            key: int(value) for key, value in sorted(parent_by_route.items())
        },
        "exported_records": len(rows),
        "records_by_route": dict(sorted(by_route.items())),
        "known_omissions": len(omissions),
        "pilot_groups": {
            key: int(value)
            for key, value in sorted(selection["pilot_group"].value_counts().items())
        },
        "worst_density_max_segment_record_counts_by_route": {
            str(row["route"]): int(row["legacy_max_segment_record_count"])
            for row in selection.loc[
                selection["pilot_group"].eq("worst_density_control")
            ].to_dict("records")
        },
        "inference_display_stress": {
            "changed_source_characters": DISPLAY_CODEC_CHANGED_DESCRIPTION,
            "preserved_without_display_escape": (
                "U+000A line feed, U+007F DEL, and U+0080-U+009F C1"
            ),
            "selection_unit": (
                "every parent containing a changed segment, with all sibling "
                "segments retained for parent reconstruction"
            ),
            "source_parent_documents": int(len(inventory)),
            "source_segments": int(inventory["segment_count"].sum()),
            "changed_parent_documents": int(len(changed_ids)),
            "changed_segments": changed_segment_count,
            "changed_characters": int(
                inventory["display_codec_changed_character_count"].sum()
            ),
            "changed_codepoint_counts": dict(sorted(codepoint_counts.items())),
            "control_only_changed_source_characters": (
                CONTROL_CODEC_CHANGED_DESCRIPTION
            ),
            "control_only_changed_parent_documents": int(
                len(control_changed_ids)
            ),
            "control_only_changed_segments": control_changed_segment_count,
            "prior_balanced_parent_documents": int(len(base_selection)),
            "prior_balanced_segments": base_segment_count,
            "changed_parent_overlap_with_prior": int(len(changed_ids & base_ids)),
            "changed_segment_overlap_with_prior": overlap_changed_segment_count,
            "changed_or_prior_unique_segments": changed_or_base_segment_count,
            "selected_complete_parent_documents": int(len(selection)),
            "selected_complete_parent_segments": int(len(rows)),
            "additional_sibling_context_segments": int(
                len(rows) - changed_or_base_segment_count
            ),
            "named_control_selection_rules": {
                "dense_tab_endorsement": (
                    "highest legacy per-segment record density among tabbed "
                    "endorsement parents, then tab-line and occurrence counts"
                ),
                "small_tab_endorsement": (
                    "shortest endorsement segment with one inline tab on a "
                    "nonempty line of at most 160 characters, excluding the "
                    "dense control candidate-cycle"
                ),
                "u0003": "shortest segment containing U+0003",
                "tie_break": "seeded SHA-256 parent rank then parent ID",
            },
            "named_controls": display_named_controls,
        },
        "structured_whitespace_stress": {
            "failure_mode": (
                "unbounded JSON structural TAB whitespace under xgrammar when "
                "disable_any_whitespace was false"
            ),
            "target_runtime_policy": {
                "backend": "xgrammar",
                "disable_any_whitespace": True,
                "scope": "JSON structural whitespace only; string content unchanged",
            },
            "selection_unit": (
                "every parent with at least one segment containing literal U+0022, "
                "with all sibling segments retained for parent reconstruction, "
                "unioned with the prior balanced/control-character pilot"
            ),
            "source_parent_documents": int(len(inventory)),
            "source_segments": int(inventory["segment_count"].sum()),
            "straight_quote_parent_documents": int(len(quote_ids)),
            "straight_quote_segments": quote_segment_count,
            "straight_quote_characters": int(
                inventory["straight_quote_character_count"].sum()
            ),
            "straight_quote_complete_parent_segments": (
                quote_complete_parent_segment_count
            ),
            "prior_pilot_parent_documents": int(len(prior_stress_selection)),
            "prior_pilot_segments": prior_stress_segment_count,
            "straight_quote_parent_overlap_with_prior": int(
                len(quote_parent_overlap_with_prior)
            ),
            "straight_quote_complete_segment_overlap_with_prior": (
                quote_complete_segment_overlap_with_prior
            ),
            "straight_quote_segment_overlap_with_prior": (
                quote_segment_overlap_with_prior
            ),
            "straight_quote_or_prior_exact_segments": (
                quote_or_prior_exact_segment_count
            ),
            "selected_complete_parent_documents": int(len(selection)),
            "selected_complete_parent_segments": int(len(rows)),
            "additional_sibling_context_segments": int(
                len(rows) - quote_or_prior_exact_segment_count
            ),
            "named_failure_control": structured_whitespace_failure_control,
            "failed_production_job_id": "21296223",
            "failed_output_chunk": "chunk_00091.out.jsonl.gz",
            "failed_finish_reason": "length",
            "failed_output_tokens": 24_576,
            "failed_trailing_tab_characters": 23_268,
        },
        "quote_and_repetition_stress": {
            "quote_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
            "quote_display_escape_spec_sha256": (
                INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
            ),
            "nullable_guided_schema_compatibility": (
                "nullable_free_text_has_no_minLength; Python nullable-nonblank "
                "validation remains a hard post-generation gate"
            ),
            "repetition_failure_status": "model_item_repetition_error",
            "repetition_threshold": 3,
            "array_cap_failure_status": "array_item_cap_reached",
            "named_failure_control": quote_repetition_failure_control,
            "failed_stress_pilot_job_id": "21304738",
            "failed_finish_reason": "length",
            "failed_output_tokens": 24_576,
            "failed_complete_repeated_items": 299,
        },
        "chunk_size": chunk_size,
        "chunks": chunk_entries,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-export-dir",
        type=Path,
        default=Path("build/hive_candidate_extraction/full_segmented_export"),
    )
    parser.add_argument(
        "--legacy-parent-path",
        type=Path,
        default=Path(
            "build/hive_candidate_extraction/full_segmented_imported/"
            "full_parent_extractions.jsonl.gz"
        ),
    )
    parser.add_argument(
        "--legacy-segment-path",
        type=Path,
        default=Path(
            "build/hive_candidate_extraction/full_segmented_imported/"
            "full_extractions.jsonl.gz"
        ),
    )
    parser.add_argument(
        "--completed-validation-dir",
        type=Path,
        default=Path("build/hive_candidate_extraction/human_validation_completed"),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--chunk-size", type=int, default=60)
    parser.add_argument(
        "--structured-whitespace-preflight",
        action="store_true",
        help="export only the frozen production TAB-loop reproducer",
    )
    parser.add_argument(
        "--quote-cap-preflight",
        action="store_true",
        help=(
            "export the frozen 44-window all-route quote/cap/repetition and "
            "exact-anchor preflight"
        ),
    )
    args = parser.parse_args()
    if args.structured_whitespace_preflight and args.quote_cap_preflight:
        raise ValueError("choose at most one preflight mode")
    if args.quote_cap_preflight:
        report = build_quote_cap_preflight(
            args.full_export_dir,
            args.output_dir,
        )
    elif args.structured_whitespace_preflight:
        report = build_structured_whitespace_preflight(
            args.full_export_dir,
            args.output_dir,
        )
    else:
        report = build_pilot(
            args.full_export_dir,
            args.legacy_parent_path,
            args.completed_validation_dir,
            args.output_dir,
            args.seed,
            args.chunk_size,
            args.legacy_segment_path,
        )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
