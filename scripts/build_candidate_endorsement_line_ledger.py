#!/usr/bin/env python3
"""Build deterministic endorsement parent-line ledger exports.

This builder performs no model inference.  It replays the frozen v2.1 source
export, reconstructs complete endorsement parents, and emits fixed-slot Stage-A
line decisions, bounded Stage-A2 exception adjudications, or Stage-B record
extractions derived from complete validated upstream runs.
"""
from __future__ import annotations

import argparse
import copy
import csv
import gzip
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.candidate_endorsement_line_ledger import (  # noqa: E402
    BASE_BIOGRAPHY_PROMPT_SHA256,
    BASE_BIOGRAPHY_SCHEMA_SHA256,
    BASE_ISSUE_PROMPT_SHA256,
    BASE_ISSUE_SCHEMA_SHA256,
    FROZEN_STAGE_A1_DECISION_IDENTITY_COUNT,
    FROZEN_STAGE_A1_DECISION_IDENTITY_LEDGER_SHA256,
    FROZEN_STAGE_A1_ORDERED_DECISION_SOURCE_LINE_IDS_SHA256,
    FROZEN_STAGE_A_V2_CONTRACT_SHA256,
    FROZEN_LINE_LEDGER_CONTRACT_SHA256,
    FROZEN_STAGE_A2_EXCEPTION_IDENTITY_LEDGER_SHA256,
    FROZEN_STAGE_A2_ORDERED_SOURCE_LINE_IDS_SHA256,
    FROZEN_STAGE_A3_AUTHENTICATION,
    LINE_BATCH_SPEC_SHA256,
    LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256,
    LINE_ADJUDICATION_SPEC_SHA256,
    LINE_ADJUDICATION_STAGE,
    LINE_DECISION_STAGE,
    LINE_INVENTORY_SPEC_SHA256,
    RECORD_EXTRACTION_STAGE,
    RECORD_EXTRACTION_BATCH_SCHEMA_RULE_SHA256,
    STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256,
    STAGE_B_THREE_PASS_PROVENANCE_SPEC_SHA256,
    build_line_adjudication_batches,
    build_recall40_round3_line_adjudication_batches,
    build_line_decision_batches,
    build_record_extraction_batches,
    build_record_extraction_batches_from_adjudicated_merge,
    computed_recall40_stage_a2_contract,
    computed_recall40_stage_b_contract,
    computed_recall40_round3_stage_a_contract,
    computed_recall40_round3_stage_a2_contract,
    computed_recall40_round3_stage_b_contract,
    computed_recall40_round2_stage_a_contract,
    get_frozen_recall40_round2_stage_b_contract,
    get_frozen_recall40_round2_stage_a2_contract,
    get_frozen_recall40_round2_stage_a_contract,
    get_frozen_recall40_round3_stage_a_contract,
    get_frozen_recall40_round3_stage_a2_contract,
    get_frozen_recall40_round3_stage_b_contract,
    FROZEN_RECALL40_ROUND2_STAGE_A2_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND2_STAGE_B_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_CHUNKS,
    RECALL40_ROUND3_STAGE_A2_BATCH_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A2_SCHEMA_RULE_SHA256,
    RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC,
    RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A2_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A_SEMANTIC_REVISION_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A_CONTEXT_PROFILE_SHA256,
    RECALL40_ROUND3_STAGE_A_LINE_BATCH_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A_SCOPE_SPEC,
    RECALL40_ROUND3_STAGE_A_SCOPE_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_BATCH_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC,
    RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_RECORD_PROJECTION_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_RENDERING_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_SCHEMA_RULE_SHA256,
    RECALL40_ROUND3_STAGE_B_SCOPE_SPEC,
    RECALL40_ROUND3_STAGE_B_SCOPE_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_SEMANTIC_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_SOURCE_CATALOG_SPEC_SHA256,
    FROZEN_RECALL40_STAGE_A2_CONTRACT_SHA256,
    FROZEN_RECALL40_STAGE_B_CONTRACT_SHA256,
    get_frozen_recall40_stage_a2_contract,
    get_frozen_recall40_stage_b_contract,
    expected_batch_counts,
    get_frozen_stage_a_v2_contract,
    get_line_ledger_contract,
    inventory_census,
    line_adjudication_exception_ledger,
    round3_line_adjudication_exception_ledger,
    line_adjudication_exception_identity_ledger,
    line_adjudication_ledger,
    line_decision_ledger,
    merge_line_adjudication_ledger,
    continuation_ownership_report,
    reconstruct_parent_inventory,
    round3_stage_b_closure_ledgers,
    sha256_json,
    stage_a_decision_identity_ledger,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (  # noqa: E402
    FROZEN_GENERATION_SEED,
    PROMPT_TOKEN_COUNT_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    line_ledger_chunk_is_complete,
    load_validated_stage_run,
    schema_variant_inventory,
    validate_input_manifest,
)


DEFAULT_FULL_EXPORT_DIR = (
    REPO / "build/hive_candidate_extraction/v2_1_full_export_20260824p"
)
DEFAULT_CHUNK_SIZE = 60
FROZEN_PRIVATE_RECALL40_SELECTION_SHA256 = (
    "5e6b0908f12cd59560699079fe19ea190303b0a8a14b9cebc1792761ed0567e6"
)
FROZEN_PRIVATE_RESERVE80_PILOT40_SELECTION_SHA256 = (
    "ef378019b0216a3ef8d1cb7ab9777c6d4f383e383857b3da013802430932202d"
)
FROZEN_PRIVATE_RECALL40_ASSIGNMENT_SHA256 = (
    "68fe62747c357239e87e785b3a444ec946c9ee1d96bc521bcaeece73f06f6877"
)
FROZEN_RECALL40_STAGE_A_INPUT_MANIFEST_SHA256 = (
    "d4672273f9c2eab6e13e09b9219200d4008e33d75efc63cc9e213beba28f1fd9"
)
FROZEN_RECALL40_STAGE_A_GENERATION_CONFIG_SHA256 = (
    "7c89e227eb81f372125be51660d7d2adec406d3250c2f4df4e7c3a923e7c6ede"
)
FROZEN_RECALL40_STAGE_A2_INPUT_MANIFEST_SHA256 = (
    "ba79b091b1f39be0f86c894824b8b9f3331ff7fef14c791d2db5cbf47ebde641"
)
FROZEN_RECALL40_STAGE_A2_GENERATION_CONFIG_SHA256 = (
    "3a61f7607f5f79ad3da6a13f1de058f9899f503d4d7d2fba076bb0f1c290c01b"
)
FROZEN_RECALL40_ROUND3_STAGE_A2_INPUT_MANIFEST_SHA256 = (
    "10d72f47bdfa5954d7112e1daaa9b9d20f08091e8ab6df6eb5b31576c03b07b2"
)
FROZEN_RECALL40_ROUND3_STAGE_A2_GENERATION_CONFIG_SHA256 = (
    "c4b8370b1f3f2c7b488dc90a3e71f1765ba2283b344a7cc5e4793b7b6b741969"
)
FROZEN_RECALL40_ROUND3_STAGE_A2_OUTPUT_BATCH_IDS_SHA256 = (
    "2f17855f099313522eb845ada967354dd34935b49ca74c8d6514ad9e307164f4"
)
FROZEN_RECALL40_ROUND3_STAGE_A_OUTPUT_BATCH_IDS_SHA256 = (
    "7684d24bb5eaab9c7987ecb375328d0b05a499012409d7544d813981f7218bfb"
)
FROZEN_RECALL40_ROUND3_STAGE_A_DECISION_LEDGER_SHA256 = (
    "d93b8bd3d6944fff35aa6e78e064a6043d226cccf3618d0cfcbf0d07768141f0"
)
FROZEN_RECALL40_ROUND3_STAGE_A_PROVENANCE_SHA256 = (
    "ed6e151f6bcbfecebf6f5a4502e855c57d92489caa1d8337349200585b3acfa1"
)
FROZEN_RECALL40_STAGE_A_SELECTION_BASE_ROWS_SHA256 = (
    "162768261a6a219dbbe32853a9711cb42086129be354ce7974084121213590d4"
)

FROZEN_FULL_EXPORT_MANIFEST_SHA256 = (
    "f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c"
)
FROZEN_SOURCE_CORPUS_SHA256 = (
    "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3"
)
FROZEN_SOURCE_SCHEMA_VERSION = "1.4"
FROZEN_SOURCE_SEGMENTS_BY_ROUTE = {
    "biography": 8_444,
    "endorsements": 6_096,
    "issues": 20_673,
}
FROZEN_SOURCE_PARENTS_BY_ROUTE = {
    "biography": 6_962,
    "endorsements": 2_583,
    "issues": 14_326,
}
FROZEN_FULL_ENDORSEMENT_INVENTORY = {
    "parent_documents": 2_583,
    "source_segments": 6_096,
    "physical_lines": 307_819,
    "nonempty_physical_lines": 307_503,
    "empty_physical_lines": 316,
    "model_decision_lines": 286_704,
    "model_decision_line_characters": 12_439_357,
    "outside_scope_lines": 19_300,
    "context_anchor_boundary_lines": 1_499,
    "reconstructed_long_lines": 39,
    "complete_line_overlap_duplicate_occurrences": 20_106,
    "unique_source_line_ids": 307_819,
    "unique_model_decision_line_ids": 286_704,
}
FROZEN_FULL_BATCH_COUNTS = {
    "line_decision_batches": 19_142,
    "record_extraction_batches_maximum": 36_995,
}

PILOT_CONTROLS = (
    (
        "tanaka_dense_parent",
        "1aad8662c07cd4f1f6343f78fe04b5f460235f17ecf57e8e5faa43024819fde8",
    ),
    (
        "champion_dense_parent",
        "df9abdfcb024b2e62dc57ceb585f6b233d23764a36704930b1e18de376d3eeb5",
    ),
    (
        "quote_bearing_name",
        "5092019feb6f9fae9d18671bb9e7daec3a4ac4f372ccbdd92cca0054aae53054",
    ),
    (
        "compound_anchor",
        "266bd0ed39563683dc9b13237fa6606547e18cad89dbacef1c64aafae7f32b72",
    ),
    (
        "long_testimonial",
        "04c8d82039daafab6c5b44bc677a2a6d45d111fe99e1a7577125f067b5a24037",
    ),
    (
        "long_json_line",
        "495263ff9183d6b966c1089473a5b0120bbfdb72b6525843e9a0d293259f1038",
    ),
    (
        "magaziner_url_context",
        "ef028a46900938122cb2b836cead4d08c308b72c572363928942ecca4f8cf211",
    ),
    (
        "explicit_testimonial",
        "df03f7b60754314aaee0069f87a7fb54feddd5ebfcb9e66fc35851c170bb7c13",
    ),
)
PILOT_PARENT_IDS = tuple(parent_id for _, parent_id in PILOT_CONTROLS)
PILOT_CONTROL_BY_PARENT = {
    parent_id: control for control, parent_id in PILOT_CONTROLS
}
FROZEN_PILOT_INVENTORY = {
    "parent_documents": 8,
    "source_segments": 33,
    "physical_lines": 1_792,
    "nonempty_physical_lines": 1_792,
    "empty_physical_lines": 0,
    "model_decision_lines": 1_758,
    "model_decision_line_characters": 75_866,
    "outside_scope_lines": 30,
    "context_anchor_boundary_lines": 4,
    "reconstructed_long_lines": 3,
    "complete_line_overlap_duplicate_occurrences": 155,
    "unique_source_line_ids": 1_792,
    "unique_model_decision_line_ids": 1_758,
}
FROZEN_PILOT_BATCH_COUNTS = {
    "line_decision_batches": 113,
    "record_extraction_batches_maximum": 222,
}

# The adjudication preflight is deliberately chained to this one completed
# Stage-A pilot, not to any merely contract-compatible rerun.  Exact byte
# inventories keep the first pass immutable even if the live line-ledger
# contract later gains an adjudication stage.
FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256 = (
    "66448050157a3e1b18fc44c13e31c9bab1de3e6c0035eb4967e6b63a3f4fda9e"
)
FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256 = (
    "57a5761dc5d35dc340d2d31c9828c403782b51dfd77e6d3824f815207d10fcb4"
)
FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256 = (
    "ecbcef016feb01945bb519cf41f53dda87661a091809b574d06e90baff836d64"
)
FROZEN_STAGE_A_SUFFIX_B_DECISION_LEDGER_SHA256 = (
    "0cf6db52d8f39a44453fdf12da40ec20966ea16c082922b41ea291cb1e5eb918"
)
FROZEN_STAGE_A_SUFFIX_B_INPUT_FILES = {
    "chunks/chunk_00000.jsonl.gz": {
        "bytes": 230_803,
        "sha256": "c9e10039b6b1a849df34e4ca7675c85aef47b13d98c5177d8cd034d0c0009468",
    },
    "chunks/chunk_00001.jsonl.gz": {
        "bytes": 193_398,
        "sha256": "a95f053bbe4c956af816fa0429a1c378e8e6b133322dd4d881dbbff909569f56",
    },
    "manifest.json": {
        "bytes": 37_420,
        "sha256": FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256,
    },
    "selection.csv": {
        "bytes": 2_404,
        "sha256": "c84fca8de2f3e458908749f29c91c8942a6f2931911e96d0751daa6f84b28cec",
    },
}
FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES = {
    "chunk_00000.out.jsonl.gz": {
        "bytes": 17_437,
        "records": 60,
        "content_sha256": "c6a3fcbdc954e449afc4704b5f961ce1a3d67593fb87d98c1212ffd2f65f61ac",
        "sha256": "67f2a5c8a6a71e7f8c621eeca7bf3442aa8cf301774cb7d0ff8e895ae593dea6",
    },
    "chunk_00001.out.jsonl.gz": {
        "bytes": 16_233,
        "records": 53,
        "content_sha256": "556eea8d3eaf4494c1ae1656a955dfbbb0b61adffc507f9bfff05200edd65ccf",
        "sha256": "568cde783665ce37dd6828152fb978be24a01f1c15ed80ab624ba1c32968544c",
    },
    "run_metadata.json": {
        "bytes": 8_617,
        "sha256": "7f748f1594a52d282ae00fbf34cafd7eb96fb9dd2e3b11b1bdec82288e8f34fe",
    },
}
FROZEN_STAGE_A2_EXCEPTION_COUNTS = {
    "candidate_self": 2,
    "unclear": 195,
    "unowned_continuation": 9,
}
FROZEN_STAGE_A2_EXCEPTION_COUNT = sum(FROZEN_STAGE_A2_EXCEPTION_COUNTS.values())

ISSUE_CONTROL_PARENT_ID = (
    "71c197e1b81d48216f8c2cb757f1da7e2b769819dd64e0e7865645d96b62bec8"
)
CORRECTED_ISSUE_AUDIT_GATE = {
    "route": "issues",
    "parent_document_id": ISSUE_CONTROL_PARENT_ID,
    "route_status": "unchanged_v2_1_issue_route_control",
    "minimum_quote_bearing_resolved_issue_records": 2,
    "minimum_fully_accounted_private_conflict_groups": 1,
    "required_conflict_statement_kinds": ["proposal", "oppose"],
    "conflict_disposition": "conflict_withheld_private",
    "legacy_minimum_three_resolved_records_gate_disallowed": True,
}

LONG_TESTIMONIAL_CONTINUATION_LINE_IDS = (
    "8ed964c676d3499bdf97f43d466d54c197998eb7c93fbe357f06638f5a6183d6",
    "5d0ddfee745f643da67a8bc537344ce53f7b870e8885ff521d9d31ca3b4a08e5",
    "f4a917b10764c543d435627283fb1163cdfe9784965bd0e579a702396d09edd4",
    "59e77aa7868e6fa788bd0dd2f7730cc4d6a2b060deaf37329445d828eb581d64",
    "4bad706cc8665d03cc0317327370ff10fb7fdc8bf6d624d507c18291a10cab15",
    "40cff0adbebea1fcf334f1e80d784f0ca155ac5a119a6802cb11f715e950c14b",
    "cfd32220b8fe8735b258862dc8cbe4a1fc28aca331a4329b4d13205014bdd636",
    "eef1ba5d53dd57e471c4616117b25f839766f54e9ca9fecfbad1c83eff869dcd",
    "c0ffa0cb038d6d32ecf8a7e3418b334a5cc82ef4554b6c22eef96b5c1a497e93",
    "467a5eaab3f0cb9afd68a0f05bfb4a8de272f1cdda04316b728d6f978368a68f",
    "cccab1d711da97c705e7871ba438267d9a0fb95dca943e35ac40f85a9abf7306",
    "dc7ceaf485bfda4d887da57726bc3764dbe3da5b1afc64e673b8b13ac07ecba4",
    "e6d862e57d6004fbfcd0aca502be649b947831c5f50b01dbeb707d603bf19c9d",
    "b88febae872ed4c08d16b8330014ceeb66b78f47e7e310ba414fa016012758a1",
    "2ab1f779224413cdcc9c47aed12cce26134af31e2697ddf32bdddb6c7d87c1bb",
    "e9fc59283618a72abd0e7304e4c219c0756c46f3d9b198ff5014c75976786c6a",
    "babbf65f4c1726100ad514c21532f5116c3d8461b5ff268b2910f805b1cd4d79",
    "4d02f23801fbcf91e5772d7b632d4c5ab143e28f80cacdae0eab54c3bc07f1cb",
    "06dc1ca89e0fac6c91898fdd1d445ce3c05c1d6b963180c03913f5e4d144b8ce",
    "e1103831288a09288ecb2390b08b183f8b4b7d0f91eed9f1a9e458d63bd4ec74",
    "1b130c601968751815d0e71276eb37f73e8af545666b5ed6d0336c7b52e61cff",
    "b3220eb1423ff75d1444b38d1f52da987ba9eeb1f0293e93c49c801e006b408b",
    "c2e3f76a837c8a2f69cedf2e0a10b7e7eec897a78724239948e71760cdc8fbae",
    "c7c36f4615843cda643ccbbe7c43dd4ad3d824a95b6fd14ed6155d2e48fb8a82",
    "685da53213154f22e57c726a3032b4cabfef7328fe4cccff5800dd693c396116",
    "ccc62f313ff98aff468a847ab628ba026bedd9c31f5dd450b499b16111f6bb50",
    "302832169c5b39a7d3551443e2a2013be0cc841111473b982f3ff96cc799271b",
    "1004431133adf036aec48bc644b4d1787e066cea6eeb6482864758b1fb6514ff",
    "f35a3084dfbda305deeecc7d33206f6c0b28dea74c014d78fc33c1bffeb5b9a9",
    "1dacf64588c4586f2253ebfa31887c2e74f5562f895d9d2e96e114c86fb2baef",
    "40f150cae7b965640b1c984885553085706d0bfcffd42f466ee3c6ff98569355",
    "2fe41ebc64f5d7a0a76b21c7aae579ed34f8b7bd015e31bf703fec195d08518e",
    "ecdd804ab50cea7cb29a27e03179a3edfac991443dd39ddea1a558b1eb28e69e",
    "0a972df95be3dd89ec5429616564b87d4821f6ca9c4c7a80a70958fdb8b6dd05",
    "eaa0d437cb3b19e3ae026f8d1d725c7594274be6ef13869f73cfde72e3191b59",
    "257e2d02bb07a8bb0d6f8ca835cc7f7f9875c98f893f4336a2fd4d301a93dd73",
    "ca3f20fbd9a3ef26b9681260b443a2b60ef0d5c78d76326bd80a309ac5e00829",
    "f38ef259ea364ffad2c519d86bb1bcf65e90d5bceca887f3a87cc997739848e8",
)
LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR = dict(zip(
    LONG_TESTIMONIAL_CONTINUATION_LINE_IDS,
    (
        "ae837b6f1c1fe778861b27279084bd6b899832cce9842dd0ee54cfab9f0e1d8f",
        "995863d597442750a6f7bed9deaada2df30e687f61c5e51064d01445b11128f9",
        "faf5e83d9416986ff32bdc2c608c0c5183b8239439c64ff9644f9521417e900a",
        "128355d2cca683476dba9c87f9999b8576b3fff5688d556905b032c077976f58",
        "bd3a50d10a91176c267236b155f8f9ed6d3eec4a660accef85b7481c8cb360f1",
        "8704c54d80f35b2067b299cef1cf9a21ae2c83a9c46a59dafeead66314b7b41b",
        "8704c54d80f35b2067b299cef1cf9a21ae2c83a9c46a59dafeead66314b7b41b",
        "8704c54d80f35b2067b299cef1cf9a21ae2c83a9c46a59dafeead66314b7b41b",
        "8704c54d80f35b2067b299cef1cf9a21ae2c83a9c46a59dafeead66314b7b41b",
        "7d60d9a4810555c2afdde52fd22c12aee6fd237a926e995cced8796f12941843",
        "7d60d9a4810555c2afdde52fd22c12aee6fd237a926e995cced8796f12941843",
        "5047f2d141056cd417aca512d9089b79359952ced5d017dfe2f00f18e88e6ff5",
        "48fcbf5df02b8f2b63f6a494eee0140f99651723dfbd995e5e2ed3388ee415ed",
        "78b5da49a8843fb7989c1954e4215344c01f2f87e169a7fba3e39c3766f1fe15",
        "53775cf71b07817451b56f0d352854101e2be11499a2565c0d3b858a46c648fa",
        "53775cf71b07817451b56f0d352854101e2be11499a2565c0d3b858a46c648fa",
        "53775cf71b07817451b56f0d352854101e2be11499a2565c0d3b858a46c648fa",
        "53775cf71b07817451b56f0d352854101e2be11499a2565c0d3b858a46c648fa",
        "5d52f9aaac1db4e85597d461c3c079c676807f1416e5ba42d6d7747e4f301db2",
        "5d52f9aaac1db4e85597d461c3c079c676807f1416e5ba42d6d7747e4f301db2",
        "dbe3743e16009ddd2ee014c8a8a39b16c506f5c178611826e2fcd2bb71ea475b",
        "745160fe0ac2bf0d4f3b09270314b2fe2bc339df17ff9d78f00d0506680d67c5",
        "745160fe0ac2bf0d4f3b09270314b2fe2bc339df17ff9d78f00d0506680d67c5",
        "f5e03ff4c199d86eeb3596a08bb0e7d2cc2674d2a8f0df82e3dfd0bfc9490bee",
        "f5e03ff4c199d86eeb3596a08bb0e7d2cc2674d2a8f0df82e3dfd0bfc9490bee",
        "f5e03ff4c199d86eeb3596a08bb0e7d2cc2674d2a8f0df82e3dfd0bfc9490bee",
        "d8275a90bf86cbec2bf0a185d42c261694a560196b41f3a3c78ba99157a06c40",
        "3008e69f4eb75ba9476745a9e1656df50d119e62c91a0485ce3bb41ab3cd05e6",
        "03b0316ccca47381bd10b3f5443183768e880163760355c2100da682a5b5ca78",
        "bd813c947da350bd021e993fe86dbc89f24caa48e46d6f362596654251fdbd51",
        "39cba158bb38ad00b834d5891a78a2ded8e4633993c8cb0465bd2293595a9123",
        "fcd98c06b494513bcab1c11cf36fbf34524d69aff84b9e273daa6e69999e5630",
        "086127ad2b4b614919fe2b0c7e22b4064de5de5e233ebf19501f0e31a315e6a9",
        "c1dc8bdc2448f514769cfabb5e1e47ad4f56e4e52b348bd081f22025ba74f7b7",
        "2813c48154518caa93c16c1da9b3fafbb5d8ea5291daca71fede3c9c30591152",
        "9902d0ab0c154ae32a24e1f1cd96ef38792e90f2dca5804122fab687ba124fa8",
        "46c2ea1d223ac521f71c041dfa77d98cc082a1b048edf5bb6ad20cdc2014060a",
        "759a93f94e4d8267c3d962613f223893045c3532b2f9a93f6cba6cc0abaf950c",
    ),
    strict=True,
))

TANAKA_INTERFACE_LINE_TEXT = {
    "5bd21e92756d590074acfadce01129f7624874884a6206bb4d1e4629a7121bcb": "Get Involved",
    "124263611a04ed6b1a71c154a36b9515460ba170615a4863c348321293269bd5": "Events",
    "1e854237a625e4459a9d5efadef41d58c3ca1fab14f14d9f0f3d077bb5454a0b": "Volunteer",
    "e0bd395dbe9d3dfe0b6572e1faf48225b2a921d1bba62360e098ef30a619f59e": "Internship",
    "265d7702ed09843cce1ddc7d9b5d0d9f59495ecadead8b8019d67a99b4da8752": "Office Hours",
    "6fabce8bcd4798e37b2d9f35432f89be7c6d4472cf398f18aa0192fee5822cc3": "Lawn Sign",
    "bce69333ba5f4cb88f67c56fa1c075029be0e393809fcd7c210678f3ae587841": "Press",
    "15bd435f76076df91492d480e09be49866936a25beaeda46d9944f3b4c919024": "More",
    "2eb728e97fd026a138dbcd8d3c8156f2ec0469b2032f17294752fefcb9029777": "© Copyright 2024. All Rights Reserved.",
    "b006f11e0b3907eed7fd46120686d1353f00165d62c8bb8e521fc6fbc67cd821": "Paid for by",
    "5c7724d06f37e57a91463b5ae414a325e3b44d951b9da1f1876d3333691639e0": "Greg Tanaka For Congress 2024",
    "d70456a0840fe5ea8ec78402c3c2b7aa057f6c4bcff08b09e259cf3141008593": "Tanaka for Palo Alto City Council in 2020",
    "b6842aa76b77c7a0325eaa856b751707330f0e66ab0f57da0e8ec2967427a109": "FEC #C00864884",
    "7353c879bc48b03ec4f6b447fa87a6ac395b6a76ec9280db2a3c6289ec7d49b6": "Donate",
    "a9bae952985a327af1750b3dffa51c4e0d887372e3d94be58bee54b55b98c43d": "Contact",
    "378820b7d50561a4d5d5f1a3b75e4d0bcfd3596632f374fc061e07ed2bed282b": "Privacy Policy",
    "20359d12a885eff45207104778638237dee3a42ce302b3df308ee75536e2f6c9": "Press",
    "1924a99e44954e814cac0b997d4187ecc32bb83ead9adef7f6527dd13991506d": "bottom of page",
}
CHAMPION_INTERFACE_LINE_TEXT = {
    "d6e1f7de77f4d16c19d6341fbc3b2acb66039427eeb7c368d8c0fa8d6828e9a3": "Contact Team Champion 2020",
    "efb942bdf898ce18e88afd3866e70fed06b5ea491de4a0801695e19486047201": "Name",
    "b9f0baa4d8326a4dc11eb015712b0d24ee687237e2de8b60c904aca29475fbe0": "Email Address",
    "e75d4ef3218301bd959c13bda0dce3f6d508c4537282a3b7335fe4c9f5cecf66": "Message",
    "301ddae603ac210f0d0fe5440bf1cf882ef8cc72ec65ca6ed5d5468aa2473b71": "Send Message",
    "ff2dc1f3299ef37af1aa9e169420354c20b4817121582140d3278c05074f2203": "Wendell Champion is running for Congressional District 18. Champion 2020 is dedicated to improving the lives of the community. Together, we can alter the destiny of a great American city. Together we all advance. Together, we can help more of our neighbors be the Champions they were meant to be! Together We Win!",
    "5cafb40d5a9a61aab8098b36db2cb422d06df0377397b377bbd61bb0890e73ab": "Quick Sitelinks:",
    "7fb65dc9871111c239d4c03b57f05c58f66d50d07f35ba70d1a63ec3c01db639": "About Wendell Champion",
    "ed060ff62fe013da5fc8d4b853c562be6155d8db4f07eaa088638d90d128f806": "Platform & Issues",
    "d841d6111d5c724d100e949cddcc82f46e22bc0a93c490644892e6eaf3ca4ea3": "Get Involved",
    "b861d91c963dfbf28584f7b8b2d0d3d2a18edefb10757e1e04fa327ccaeccd9c": "Contribute",
    "339ac76d56459e0d188815f6c5b882ec77e4f3884a6ec3c36b17c00d216c7d85": "Tweets by @ChampionTx2020",
    "82139ddbfa116d7966ff0deaf712e14ac1c936dfd9b065c54956fbdfdec4f536": "PAID FOR BY CHAMPION FOR CONGRESS",
    "9d6911b7bc1ef5ef91d0db8fe97f4976763a519a0e56e5a0265b35d2e4a68176": "Powered by",
    "c7b056a6e0e41b882e7a335f7043aaf8f462aabcb9a979ebe1a98c780b726bdd": "ASTOUNDZ.",
}

STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES = {
    # Exact candidate self-reference on the Tanaka parent (Stage-A unclear).
    "af281dff680e36b1ac925ba0509577d5a648aba60189c65140e0cab0a72915bd": (
        "candidate_self"
    ),
    # Exact candidate self-reference on the Magaziner parent.
    "edc7c8e209ff8511b6996e9080518b6e6f677a57b74876b91c84eb75fcf651ad": (
        "candidate_self"
    ),
    # False Stage-A candidate-self proposal: a policy-summary sentence.
    "5cac96d3488b283289854cc46cf973d12ac498a6d6c2649c9f3f07a9a18d10f1": (
        "nonrecord_content"
    ),
}

STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS = {
    "2d680e5f8e0164873011d06b65925bd8cd27fbe4b89c0fc36f1d219a63507425": (
        "7d550c3dfd6691afeb17c49c6d1995d13ec48de0062f688a90fc07fc40637e15"
    ),
    "6d3204507829dbc39e3ed7d6eb4690eea96a87254ba7bd4c5c0114dd006375f7": (
        "7d550c3dfd6691afeb17c49c6d1995d13ec48de0062f688a90fc07fc40637e15"
    ),
    "29a627a195f761e26448d0259b7ff4749bfe9d0b65314e666fc45033b7ab6b66": (
        "7d550c3dfd6691afeb17c49c6d1995d13ec48de0062f688a90fc07fc40637e15"
    ),
}
STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS = {
    "f6b2499c03690e58cee21554072762af69ec0553353b9ef5907734e9c0c479b0": (
        "navigation_form_footer"
    ),
    "5f874e69e9c9e2e230f593f0ac0baef16a3a20e57bc6df445f9e14439ae6f825": (
        "navigation_form_footer"
    ),
    "dfa0bc54df91172b4fd21bbf0a3084453a3f862179a382874c5f81d4d57b2074": (
        "navigation_form_footer"
    ),
    "272f598ea02ea32cf2c029627acce29286bad39c714398549fde3f57a744102d": (
        "navigation_form_footer"
    ),
    "af39c6b7074f379dd53f3a7865d769ba935b3de7a9f264f0bc0b3e05ca3a4816": (
        "navigation_form_footer"
    ),
    "18829023194d146574c486c2bde03d9bcd09dd44432632153efdb6e59adf4cdc": (
        "nonrecord_content"
    ),
}

PILOT_CONTENT_GATES = {
    PILOT_PARENT_IDS[0]: {
        "minimum_non_candidate_full_parent_unique_identities": 390,
        "required_segment_counts": {
            "aa0b99eb1331464dce28db32d73a6f53bddbf7e4aa10f46f4c17c48b12720433": 224,
            "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73": 86,
        },
        "minimum_uniquely_aligned_for_2e9a0cd8": 84,
        "exclude_exact": ["Greg Tanaka", "ena Song"],
        "required_navigation_form_footer_lines": TANAKA_INTERFACE_LINE_TEXT,
    },
    PILOT_PARENT_IDS[1]: {
        "minimum_model_positive_unique_identities": 230,
        "minimum_uniquely_aligned_identities": 222,
        "exclude_exact": ["cy Ruffeno"],
        "required_navigation_form_footer_lines": CHAMPION_INTERFACE_LINE_TEXT,
    },
    PILOT_PARENT_IDS[2]: {
        "required_source_line_id": (
            "95b2a078f8b90976a6b1336fe266ab96a8bb9605638a6019725d2309c1111ea2"
        ),
        "must_resolve_quote_bearing_record_evidence": (
            'Texas State Rep. Jolanda "Jo" Jones'
        ),
    },
    PILOT_PARENT_IDS[3]: {
        "required_compound_source_line_id": (
            "749919130bee61f058243dba99570d6cb5579a18d792a4f939ce0d69e1ead000"
        ),
        "exact_compound_names": [
            "Shimiko Montgomery",
            "Melissa Barnes-Dholakia",
            "Carrie Douglass",
        ],
    },
    PILOT_PARENT_IDS[4]: {
        "required_continuation_source_line_ids": list(
            LONG_TESTIMONIAL_CONTINUATION_LINE_IDS
        ),
        "required_continuation_to_preceding_anchor": (
            LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR
        ),
        "all_required_continuations_consumed_exactly_once": True,
        "adjacent_speakers_must_remain_distinct": True,
    },
    PILOT_PARENT_IDS[5]: {
        "long_line_characters": 7_855,
        "required_source_line_id": (
            "7c42640254692dee92b9845f00ac172dc45dce7f31a07b3fc02c4bcef3f6e076"
        ),
        "required_stage_a_role": "nonrecord_content",
        "must_be_absent_from_stage_b_anchors": True,
    },
    PILOT_PARENT_IDS[6]: {
        "minimum_records": 1,
        "required_context_scope": "page_or_section_route_context",
        "candidate_self_support_disallowed": True,
    },
    PILOT_PARENT_IDS[7]: {
        "minimum_aligned_records": 11,
        "required_source_line_id": (
            "917a9324fedf4b4f898858584f78ae89c594e07279c515123b5772ccad9b01f2"
        ),
        "must_include_exact": ["Vets for Trump"],
    },
}


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json_no_duplicates(path: Path) -> Any:
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate JSON key in {path}: {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)


def read_jsonl_gz(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                raise ValueError(f"blank JSONL row: {path}:{line_number}")
            value = json.loads(line)
            if not isinstance(value, dict):
                raise TypeError(f"JSONL row is not an object: {path}:{line_number}")
            yield value


def write_jsonl_gz(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    temporary = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, mtime=0
        ) as compressed:
            for row in rows:
                encoded = (
                    json.dumps(
                        row, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8")
                compressed.write(encoded)
    os.replace(temporary, path)


def ensure_empty_output_dir(output_dir: Path) -> None:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"refusing nonempty output directory: {output_dir}")


def _require_equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        raise ValueError(f"{label} changed: expected {expected!r}, found {actual!r}")


def _listed_chunk_paths(root: Path, manifest: Mapping[str, Any]) -> list[Path]:
    entries = manifest.get("chunks")
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"manifest has no chunks: {root}")
    paths: list[Path] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise TypeError("chunk manifest entry is not an object")
        name = entry.get("file")
        if not isinstance(name, str) or Path(name).name != name:
            raise ValueError(f"unsafe chunk filename: {name!r}")
        path = root / "chunks" / name
        if not path.is_file():
            raise FileNotFoundError(path)
        _require_equal(sha256_file(path), entry.get("sha256"), f"chunk hash {name}")
        paths.append(path)
    actual = {path.name for path in (root / "chunks").glob("*.jsonl.gz")}
    listed = {path.name for path in paths}
    _require_equal(actual, listed, "chunk inventory")
    return paths


def replay_frozen_source(
    full_export_dir: Path,
    *,
    enforce_frozen_source: bool = True,
) -> dict[str, Any]:
    """Verify every frozen source chunk and retain endorsement parent closure."""
    manifest_path = full_export_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    actual_manifest_sha256 = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise TypeError("source manifest is not an object")
    if enforce_frozen_source:
        _require_equal(
            actual_manifest_sha256,
            FROZEN_FULL_EXPORT_MANIFEST_SHA256,
            "full source export manifest SHA-256",
        )
        _require_equal(
            manifest.get("source_corpus_sha256"),
            FROZEN_SOURCE_CORPUS_SHA256,
            "source corpus SHA-256",
        )
        _require_equal(
            str(manifest.get("schema_version")),
            FROZEN_SOURCE_SCHEMA_VERSION,
            "source schema version",
        )

    chunks = _listed_chunk_paths(full_export_dir, manifest)
    route_segments: Counter[str] = Counter()
    route_parents: dict[str, set[str]] = defaultdict(set)
    endorsement_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    issue_control_rows: list[dict[str, Any]] = []
    document_ids: list[str] = []
    seen_document_ids: set[str] = set()
    rows_seen = 0
    for entry, path in zip(manifest["chunks"], chunks):
        chunk_rows = 0
        for row in read_jsonl_gz(path):
            chunk_rows += 1
            rows_seen += 1
            document_id = str(row.get("document_id"))
            if document_id in seen_document_ids:
                raise ValueError(f"duplicate source document ID: {document_id}")
            seen_document_ids.add(document_id)
            document_ids.append(document_id)
            route = str(row.get("route"))
            if route not in FROZEN_SOURCE_SEGMENTS_BY_ROUTE:
                raise ValueError(f"unexpected source route: {route}")
            parent_id = str(row.get("parent_document_id"))
            route_segments[route] += 1
            route_parents[route].add(parent_id)
            if route == "endorsements":
                endorsement_groups[parent_id].append(row)
            if parent_id == ISSUE_CONTROL_PARENT_ID:
                issue_control_rows.append(row)
        _require_equal(chunk_rows, int(entry.get("records", -1)), f"chunk rows {path.name}")

    _require_equal(rows_seen, int(manifest.get("exported_records", -1)), "source rows")
    replay_segments = dict(sorted(route_segments.items()))
    replay_parents = {
        route: len(parent_ids) for route, parent_ids in sorted(route_parents.items())
    }
    if enforce_frozen_source:
        _require_equal(
            replay_segments, FROZEN_SOURCE_SEGMENTS_BY_ROUTE, "source route rows"
        )
        _require_equal(
            replay_parents, FROZEN_SOURCE_PARENTS_BY_ROUTE, "source route parents"
        )

    for parent_id, rows in endorsement_groups.items():
        segment_count = int(rows[0].get("segment_count", -1))
        indexes = sorted(int(row.get("segment_index", -1)) for row in rows)
        if len(rows) != segment_count or indexes != list(range(segment_count)):
            raise ValueError(f"incomplete endorsement parent closure: {parent_id}")
    if len(issue_control_rows) != 3:
        raise ValueError("corrected 71c issue control does not have three source segments")
    if any(row.get("route") != "issues" for row in issue_control_rows):
        raise ValueError("corrected 71c control is not an issue parent")
    if sorted(int(row["segment_index"]) for row in issue_control_rows) != [0, 1, 2]:
        raise ValueError("corrected 71c issue control parent closure changed")

    return {
        "manifest": manifest,
        "manifest_sha256": actual_manifest_sha256,
        "route_segments": replay_segments,
        "route_parents": replay_parents,
        "source_document_ids_sha256": sha256_json(document_ids),
        "endorsement_groups": endorsement_groups,
        "issue_control_source_segments": len(issue_control_rows),
    }


def reconstruct_selected_parents(
    replay: Mapping[str, Any], selected_parent_ids: Sequence[str],
    *, round2_route_revision: bool = False,
) -> list[dict[str, Any]]:
    groups = replay["endorsement_groups"]
    selected_set = set(selected_parent_ids)
    if len(selected_set) != len(selected_parent_ids):
        raise ValueError("selected parent IDs contain duplicates")
    missing = sorted(selected_set - set(groups))
    if missing:
        raise ValueError(f"selected endorsement parents absent from source: {missing}")
    parents = [
        reconstruct_parent_inventory(
            groups[parent_id], round2_route_revision=round2_route_revision
        )
        for parent_id in selected_parent_ids
    ]
    if {str(parent["parent_document_id"]) for parent in parents} != selected_set:
        raise AssertionError("reconstructed parent set differs from selection")
    return parents


def _selection_rows(
    parents: Sequence[Mapping[str, Any]],
    *,
    mode: str,
    stage_a_decisions: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for parent in parents:
        lines = list(parent["lines"])
        model_lines = [line for line in lines if line["model_decision_required"]]
        line_batches = build_line_decision_batches([parent])
        maximum = expected_batch_counts([parent])["record_extraction_batches_maximum"]
        row: dict[str, Any] = {
            "selection_group": (
                "private_recall_pilot40" if mode in {
                    "line_decision_recall_pilot40",
                    "line_adjudication_recall_pilot40",
                    "line_adjudication_recall40_round2",
                    "line_decision_recall40_round3",
                    "line_adjudication_recall40_round3",
                    "record_extraction_recall_pilot40",
                    "record_extraction_recall40_round2",
                    "record_extraction_recall40_round3",
                }
                else "bounded_preflight" if "preflight" in mode else "full"
            ),
            "pilot_control_kind": PILOT_CONTROL_BY_PARENT.get(
                str(parent["parent_document_id"]), ""
            ),
            "parent_document_id": parent["parent_document_id"],
            "candidate_cycle_id": parent["candidate_cycle_id"],
            "candidate_icpsr": parent["candidate_icpsr"],
            "state": parent["state"],
            "office": parent["office"],
            "year": parent["year"],
            "party": parent["party"],
            "segment_count": len(parent["segment_descriptors"]),
            "physical_line_count": len(lines),
            "model_decision_line_count": len(model_lines),
            "line_decision_batch_count": len(line_batches),
            "record_extraction_batch_count_maximum": maximum,
            "parent_line_inventory_sha256": parent["parent_line_inventory_sha256"],
            "context_scope": parent["endorsement_line_context"]["source_scope"],
        }
        if stage_a_decisions is not None:
            roles = Counter(
                str(stage_a_decisions[str(line["source_line_id"])]["role"])
                for line in model_lines
            )
            anchors = roles["record_anchor"] + roles["compound_record_anchor"]
            row["stage_a_anchor_line_count"] = anchors
            row["stage_a_unclear_line_count"] = roles["unclear"]
            row["stage_a_role_counts_json"] = canonical_json(dict(sorted(roles.items())))
            row["record_extraction_batch_count"] = (anchors + 7) // 8
        rows.append(row)
    return rows


def write_selection(path: Path, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ValueError("selection cannot be empty")
    fieldnames = list(rows[0])
    if any(list(row) != fieldnames for row in rows):
        raise ValueError("selection rows have inconsistent fields or order")
    temporary = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)
    return {
        "file": path.name,
        "rows": len(rows),
        "columns": fieldnames,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def write_batch_chunks(
    chunks_dir: Path,
    rows: Sequence[Mapping[str, Any]],
    chunk_size: int,
) -> list[dict[str, Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk size must be positive")
    chunks_dir.mkdir(parents=True, exist_ok=False)
    entries: list[dict[str, Any]] = []
    for offset in range(0, len(rows), chunk_size):
        part = rows[offset:offset + chunk_size]
        path = chunks_dir / f"chunk_{offset // chunk_size:05d}.jsonl.gz"
        write_jsonl_gz(path, part)
        entries.append({
            "file": path.name,
            "records": len(part),
            "first_document_id": str(part[0]["document_id"]),
            "last_document_id": str(part[-1]["document_id"]),
            "content_sha256": sha256_json(list(part)),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        })
    return entries


def finalize_manifest(output_dir: Path, manifest_payload: Mapping[str, Any]) -> dict[str, Any]:
    manifest = dict(manifest_payload)
    manifest["manifest_payload_sha256"] = sha256_json(manifest)
    path = output_dir / "manifest.json"
    path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def validate_manifest_payload_hash(manifest: Mapping[str, Any]) -> None:
    payload = dict(manifest)
    recorded = payload.pop("manifest_payload_sha256", None)
    _require_equal(sha256_json(payload), recorded, "manifest payload SHA-256")


def _validate_frozen_file_inventory(
    root: Path,
    expected: Mapping[str, Mapping[str, Any]],
    *,
    label: str,
) -> None:
    if not root.is_dir():
        raise FileNotFoundError(root)
    actual = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file()
    }
    _require_equal(actual, set(expected), f"{label} exact file inventory")
    for relative_name, entry in expected.items():
        path = root / relative_name
        _require_equal(path.stat().st_size, entry["bytes"], f"{label} bytes {relative_name}")
        _require_equal(sha256_file(path), entry["sha256"], f"{label} SHA-256 {relative_name}")


def load_frozen_stage_a_suffix_b(
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, Any],
]:
    """Authenticate and replay the exact completed suffix-b Stage-A pilot."""
    _validate_frozen_file_inventory(
        stage_a_export_dir,
        FROZEN_STAGE_A_SUFFIX_B_INPUT_FILES,
        label="frozen suffix-b Stage-A input",
    )
    _validate_frozen_file_inventory(
        stage_a_output_dir,
        FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES,
        label="frozen suffix-b Stage-A output",
    )
    manifest_path = stage_a_export_dir / "manifest.json"
    historical_contract = get_frozen_stage_a_v2_contract()
    validated_manifest, validated_manifest_sha256, validated_input_paths = (
        validate_input_manifest(
            manifest_path,
            stage_a_export_dir / "chunks",
            stage=LINE_DECISION_STAGE,
            contract=historical_contract,
        )
    )
    _require_equal(
        validated_manifest_sha256,
        FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256,
        "suffix-b Stage-A authenticated manifest bytes",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise TypeError("frozen suffix-b Stage-A manifest is not an object")
    validate_manifest_payload_hash(manifest)
    _require_equal(manifest.get("stage"), LINE_DECISION_STAGE, "suffix-b Stage-A stage")
    _require_equal(manifest.get("mode"), "line_decision_preflight", "suffix-b Stage-A mode")
    _require_equal(
        manifest.get("selected_parent_ids"),
        list(PILOT_PARENT_IDS),
        "suffix-b Stage-A selected pilot parents",
    )
    _require_equal(manifest.get("exported_records"), 113, "suffix-b Stage-A batches")
    _require_equal(
        manifest.get("source_export", {}).get("source_export_manifest_sha256"),
        FROZEN_FULL_EXPORT_MANIFEST_SHA256,
        "suffix-b Stage-A frozen source export",
    )

    _require_equal(validated_manifest, manifest, "suffix-b Stage-A parsed manifest")
    input_paths = list(validated_input_paths)
    output_paths = [
        stage_a_output_dir / "chunk_00000.out.jsonl.gz",
        stage_a_output_dir / "chunk_00001.out.jsonl.gz",
    ]
    input_rows_by_path = {
        path: list(read_jsonl_gz(path)) for path in input_paths
    }
    output_rows_by_path = {
        path: list(read_jsonl_gz(path)) for path in output_paths
    }
    batches = [row for path in input_paths for row in input_rows_by_path[path]]
    rows = [row for path in output_paths for row in output_rows_by_path[path]]
    _require_equal(len(batches), 113, "suffix-b Stage-A input rows")
    _require_equal(len(rows), 113, "suffix-b Stage-A output rows")
    batch_ids = [str(batch.get("batch_id")) for batch in batches]
    output_ids = [str(row.get("batch_id")) for row in rows]
    _require_equal(batch_ids, output_ids, "suffix-b Stage-A input/output order")
    _require_equal(
        sha256_json(batch_ids),
        FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256,
        "suffix-b Stage-A batch-ID digest",
    )
    for batch, row in zip(batches, rows):
        batch_id = str(batch["batch_id"])
        _require_equal(row.get("input_batch_sha256"), sha256_json(batch), f"suffix-b Stage-A input binding {batch_id}")
        _require_equal(
            row.get("input_manifest_sha256"),
            FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256,
            f"suffix-b Stage-A manifest binding {batch_id}",
        )
        _require_equal(row.get("stage"), LINE_DECISION_STAGE, f"suffix-b Stage-A row stage {batch_id}")
        _require_equal(row.get("parse_status"), "parsed", f"suffix-b Stage-A parse status {batch_id}")
        _require_equal(row.get("technical_pass"), True, f"suffix-b Stage-A technical status {batch_id}")
        _require_equal(
            row.get("generation_config_sha256"),
            FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256,
            f"suffix-b Stage-A generation binding {batch_id}",
        )
    decisions = line_decision_ledger(batches, rows)
    _require_equal(
        sha256_json(decisions),
        FROZEN_STAGE_A_SUFFIX_B_DECISION_LEDGER_SHA256,
        "suffix-b Stage-A decision-ledger digest",
    )
    metadata = json.loads(
        (stage_a_output_dir / "run_metadata.json").read_text(encoding="utf-8")
    )
    if not isinstance(metadata, dict):
        raise TypeError("frozen suffix-b Stage-A run metadata is not an object")
    _require_equal(
        metadata.get("generation_config_sha256"),
        FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256,
        "suffix-b Stage-A generation configuration",
    )
    _require_equal(metadata.get("selected_batches"), 113, "suffix-b Stage-A selected batches")
    _require_equal(metadata.get("processed_outputs_this_run"), 113, "suffix-b Stage-A processed batches")
    batch_file_provenance: dict[str, dict[str, Any]] = {}
    for input_path, output_path in zip(input_paths, output_paths):
        input_rows = input_rows_by_path[input_path]
        output_rows = output_rows_by_path[output_path]
        _require_equal(
            [row["batch_id"] for row in input_rows],
            [row["batch_id"] for row in output_rows],
            f"suffix-b Stage-A chunk order {input_path.name}",
        )
        input_entry = FROZEN_STAGE_A_SUFFIX_B_INPUT_FILES[
            f"chunks/{input_path.name}"
        ]
        output_entry = FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES[output_path.name]
        _require_equal(
            sha256_json(output_rows),
            output_entry["content_sha256"],
            f"suffix-b Stage-A output content digest {output_path.name}",
        )
        for batch, row in zip(input_rows, output_rows):
            batch_id = str(batch["batch_id"])
            batch_file_provenance[batch_id] = {
                "input_chunk_file": f"chunks/{input_path.name}",
                "input_chunk_sha256": input_entry["sha256"],
                "output_chunk_file": output_path.name,
                "output_chunk_sha256": output_entry["sha256"],
                "input_batch_sha256": sha256_json(batch),
                "raw_output_sha256": hashlib.sha256(
                    str(row["raw_output"]).encode("utf-8")
                ).hexdigest(),
                "model_output_sha256": row["model_output_sha256"],
                "generation_config_sha256": (
                    FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256
                ),
            }
    _require_equal(
        set(batch_file_provenance), set(batch_ids),
        "suffix-b Stage-A per-batch provenance coverage",
    )
    required_stage_a_provenance = {
        "stage_a_input_manifest_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256
        ),
        "stage_a_line_ledger_contract_sha256": manifest[
            "line_ledger_contract_sha256"
        ],
        "stage_a_generation_config_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256
        ),
        "stage_a_ordered_output_batch_ids_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256
        ),
        "stage_a_output_files": [
            {"file": path.name, **dict(
                FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES[path.name]
            )}
            for path in output_paths
        ],
        "by_batch_id": batch_file_provenance,
    }
    provenance_payload = {
        **required_stage_a_provenance,
        "upstream_stage": LINE_DECISION_STAGE,
        "upstream_stage_a_contract_sha256": manifest[
            "line_ledger_contract_sha256"
        ],
        "input_export_manifest_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256
        ),
        "input_files": [
            {"file": name, **dict(entry)}
            for name, entry in sorted(FROZEN_STAGE_A_SUFFIX_B_INPUT_FILES.items())
        ],
        "run_metadata_files": [{
            "file": "run_metadata.json",
            **dict(FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES["run_metadata.json"]),
        }],
        "generation_config": metadata.get("generation_config"),
        "generation_config_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256
        ),
        "output_files": [
            {"file": name, **dict(entry)}
            for name, entry in sorted(FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES.items())
            if name.endswith(".out.jsonl.gz")
        ],
        "output_batch_ids_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256
        ),
        "validated_output_batches": len(rows),
        "source_line_decision_count": len(decisions),
        "source_line_decision_ledger_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_DECISION_LEDGER_SHA256
        ),
        "role_counts": dict(sorted(Counter(
            str(decision["role"]) for decision in decisions.values()
        ).items())),
        "exact_frozen_byte_inventory_validated": True,
    }
    provenance = {
        **provenance_payload,
        "stage_a_provenance_sha256": sha256_json(provenance_payload),
    }
    return manifest, batches, rows, decisions, provenance


def load_recall40_stage_a_run(
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
) -> tuple[
    dict[str, Any], list[dict[str, Any]], list[dict[str, Any]],
    dict[str, dict[str, Any]], dict[str, Any],
]:
    """Authenticate a complete private-recall40 Stage-A run.

    Unlike the historical eight-parent loader, all row counts and decision
    identities are derived from the externally frozen suffix-d manifest.  The
    shared loader rejects absent, partial, technically invalid, or mixed-shard
    output before any exception inventory can be computed.
    """
    manifest_path = stage_a_export_dir / "manifest.json"
    _require_equal(
        sha256_file(manifest_path),
        FROZEN_RECALL40_STAGE_A_INPUT_MANIFEST_SHA256,
        "recall40 Stage-A input manifest bytes",
    )
    manifest, batches, rows, artifacts = load_validated_stage_run(
        stage_a_export_dir, stage_a_output_dir, stage=LINE_DECISION_STAGE,
    )
    _require_equal(manifest.get("mode"), "line_decision_recall_pilot40",
                   "recall40 Stage-A mode")
    private = manifest.get("private_recall40")
    if not isinstance(private, Mapping):
        raise ValueError("recall40 Stage-A private scope is absent")
    _require_equal(private.get("reserve80_loaded"), False,
                   "recall40 reserve isolation")
    _require_equal(len(manifest.get("selected_parent_ids", [])), 40,
                   "recall40 selected-parent count")
    _require_equal(artifacts.get("generation_config_sha256"),
                   FROZEN_RECALL40_STAGE_A_GENERATION_CONFIG_SHA256,
                   "recall40 Stage-A generation identity")
    _require_equal(len(batches), manifest.get("expected_stage_batch_count"),
                   "recall40 Stage-A input closure")
    _require_equal(len(rows), len(batches), "recall40 Stage-A output closure")
    decisions = line_decision_ledger(batches, rows)
    decision_ids = [str(value) for value in decisions]
    _require_equal(len(decision_ids), len(set(decision_ids)),
                   "recall40 Stage-A unique decision identities")
    with (stage_a_export_dir / "selection.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        stage_a_selection = list(csv.DictReader(handle))
    base_columns = list(stage_a_selection[0])[:16]
    stage_a_selection_base_rows = [
        {key: row[key] for key in base_columns} for row in stage_a_selection
    ]
    _require_equal(
        sha256_json(stage_a_selection_base_rows),
        FROZEN_RECALL40_STAGE_A_SELECTION_BASE_ROWS_SHA256,
        "recall40 Stage-A selection base rows",
    )
    row_by_batch = {str(row["batch_id"]): row for row in rows}
    output_file_by_name = {
        str(entry["file"]): dict(entry) for entry in artifacts["output_files"]
    }
    by_batch_id: dict[str, dict[str, Any]] = {}
    for chunk_entry in manifest["files"]["chunks"]:
        input_name = str(chunk_entry["file"])
        output_name = input_name.replace(".jsonl.gz", ".out.jsonl.gz")
        output_entry = output_file_by_name.get(output_name)
        if output_entry is None:
            raise ValueError(f"recall40 output file is absent: {output_name}")
        for batch in read_jsonl_gz(stage_a_export_dir / "chunks" / input_name):
            batch_id = str(batch["batch_id"])
            row = row_by_batch.get(batch_id)
            if row is None:
                raise ValueError(f"recall40 output batch is absent: {batch_id}")
            by_batch_id[batch_id] = {
                "input_chunk_file": f"chunks/{input_name}",
                "input_chunk_sha256": chunk_entry["sha256"],
                "output_chunk_file": output_name,
                "output_chunk_sha256": output_entry["sha256"],
                "input_batch_sha256": sha256_json(batch),
                "raw_output_sha256": hashlib.sha256(
                    str(row["raw_output"]).encode("utf-8")
                ).hexdigest(),
                "model_output_sha256": row["model_output_sha256"],
                "generation_config_sha256": artifacts[
                    "generation_config_sha256"
                ],
            }
    _require_equal(set(by_batch_id), set(row_by_batch),
                   "recall40 per-batch provenance coverage")
    provenance = {
        "stage_a_input_manifest_sha256": artifacts["input_manifest_sha256"],
        "stage_a_line_ledger_contract_sha256": manifest[
            "line_ledger_contract_sha256"
        ],
        "stage_a_generation_config_sha256": artifacts[
            "generation_config_sha256"
        ],
        "stage_a_ordered_output_batch_ids_sha256": artifacts[
            "output_batch_ids_sha256"
        ],
        "source_line_decision_ledger_sha256": sha256_json(decisions),
        "validated_output_batches": artifacts["validated_output_batches"],
        "input_files": artifacts["input_files"],
        "output_files": artifacts["output_files"],
        "run_metadata_files": artifacts["run_metadata_files"],
        "typed_restart_replay_complete": True,
        "stage_a_selection_base_rows_sha256": (
            FROZEN_RECALL40_STAGE_A_SELECTION_BASE_ROWS_SHA256
        ),
        "by_batch_id": by_batch_id,
    }
    provenance["stage_a_provenance_sha256"] = sha256_json(provenance)
    return manifest, batches, rows, decisions, provenance


def load_recall40_round3_stage_a_run(
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
) -> tuple[
    dict[str, Any], list[dict[str, Any]], list[dict[str, Any]],
    dict[str, dict[str, Any]], dict[str, Any],
]:
    """Authenticate the exact five-shard round-three Stage-A run."""
    manifest_path = stage_a_export_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    _require_equal(
        sha256_file(manifest_path),
        FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256,
        "round-three Stage-A input manifest bytes",
    )
    manifest, batches, rows, artifacts = load_validated_stage_run(
        stage_a_export_dir,
        stage_a_output_dir,
        stage=LINE_DECISION_STAGE,
        contract_override=get_frozen_recall40_round3_stage_a_contract(),
    )
    _require_equal(
        manifest.get("mode"), "line_decision_recall40_round3",
        "round-three Stage-A mode",
    )
    _require_equal(
        artifacts.get("generation_config_sha256"),
        FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256,
        "round-three Stage-A generation identity",
    )
    _require_equal(len(batches), 270, "round-three Stage-A input closure")
    _require_equal(len(rows), 270, "round-three Stage-A output closure")
    declared_chunks = manifest.get("files", {}).get("chunks")
    expected_chunks = FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_CHUNKS
    if not isinstance(declared_chunks, list):
        raise ValueError("round-three Stage-A input chunks are absent")
    _require_equal(
        [
            {
                "file": entry.get("file"),
                "sha256": entry.get("sha256"),
                "records": entry.get("records"),
            }
            for entry in declared_chunks
        ],
        expected_chunks,
        "round-three Stage-A five input shards",
    )
    _require_equal(
        [entry.get("file") for entry in artifacts["output_files"]],
        [
            str(entry["file"]).replace(".jsonl.gz", ".out.jsonl.gz")
            for entry in expected_chunks
        ],
        "round-three Stage-A five output shards",
    )
    _require_equal(
        [entry.get("file") for entry in artifacts["run_metadata_files"]],
        [f"run_metadata_shard_{index:05d}.json" for index in range(5)],
        "round-three Stage-A five metadata shards",
    )
    if manifest.get("selected_parent_ids_sha256") != (
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["selected_parent_ids_sha256"]
    ) or len(manifest.get("selected_parent_ids", [])) != 40:
        raise ValueError("round-three Stage-A selected parents differ")
    selection = manifest.get("files", {}).get("selection")
    if not isinstance(selection, Mapping) or selection.get("sha256") != (
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["selection_csv_sha256"]
    ):
        raise ValueError("round-three Stage-A selection differs")
    decisions = line_decision_ledger(batches, rows)
    _require_equal(len(decisions), 4019, "round-three Stage-A line closure")
    _require_equal(
        artifacts.get("output_batch_ids_sha256"),
        FROZEN_RECALL40_ROUND3_STAGE_A_OUTPUT_BATCH_IDS_SHA256,
        "round-three Stage-A output batch identity",
    )
    _require_equal(
        sha256_json(decisions),
        FROZEN_RECALL40_ROUND3_STAGE_A_DECISION_LEDGER_SHA256,
        "round-three Stage-A decision ledger identity",
    )
    row_by_batch = {str(row["batch_id"]): row for row in rows}
    output_files = {
        str(entry["file"]): dict(entry) for entry in artifacts["output_files"]
    }
    by_batch_id: dict[str, dict[str, Any]] = {}
    for chunk in declared_chunks:
        input_name = str(chunk["file"])
        output_name = input_name.replace(".jsonl.gz", ".out.jsonl.gz")
        output_entry = output_files.get(output_name)
        if output_entry is None:
            raise ValueError(
                f"round-three Stage-A output file is absent: {output_name}"
            )
        for batch in read_jsonl_gz(stage_a_export_dir / "chunks" / input_name):
            batch_id = str(batch["batch_id"])
            row = row_by_batch.get(batch_id)
            if row is None:
                raise ValueError(
                    f"round-three Stage-A output batch is absent: {batch_id}"
                )
            by_batch_id[batch_id] = {
                "input_chunk_file": f"chunks/{input_name}",
                "input_chunk_sha256": chunk["sha256"],
                "output_chunk_file": output_name,
                "output_chunk_sha256": output_entry["sha256"],
                "input_batch_sha256": sha256_json(batch),
                "raw_output_sha256": hashlib.sha256(
                    str(row["raw_output"]).encode("utf-8")
                ).hexdigest(),
                "model_output_sha256": row["model_output_sha256"],
                "generation_config_sha256": artifacts[
                    "generation_config_sha256"
                ],
            }
    _require_equal(
        set(by_batch_id), set(row_by_batch),
        "round-three Stage-A per-batch provenance closure",
    )
    provenance = {
        "stage_a_input_manifest_sha256": artifacts["input_manifest_sha256"],
        "stage_a_line_ledger_contract_sha256": manifest[
            "line_ledger_contract_sha256"
        ],
        "stage_a_generation_config_sha256": artifacts[
            "generation_config_sha256"
        ],
        "stage_a_ordered_output_batch_ids_sha256": artifacts[
            "output_batch_ids_sha256"
        ],
        "source_line_decision_ledger_sha256": sha256_json(decisions),
        "validated_output_batches": artifacts["validated_output_batches"],
        "input_files": artifacts["input_files"],
        "output_files": artifacts["output_files"],
        "run_metadata_files": artifacts["run_metadata_files"],
        "typed_restart_replay_complete": True,
        "selected_parent_ids_sha256": manifest[
            "selected_parent_ids_sha256"
        ],
        "by_batch_id": by_batch_id,
    }
    provenance["stage_a_provenance_sha256"] = sha256_json(provenance)
    _require_equal(
        provenance["stage_a_provenance_sha256"],
        FROZEN_RECALL40_ROUND3_STAGE_A_PROVENANCE_SHA256,
        "round-three Stage-A provenance identity",
    )
    return manifest, batches, rows, decisions, provenance


def load_recall40_round2_stage_a_run(
    stage_a_export_dir: Path, stage_a_output_dir: Path,
) -> tuple[
    dict[str, Any], list[dict[str, Any]], list[dict[str, Any]],
    dict[str, dict[str, Any]], dict[str, Any],
]:
    """Authenticate the complete round-two Stage-A run and its provenance."""
    expected_manifest = "7918a6d6c38a91ad65f7fb1e8aa93065451509cb10cd771f5aa50df9d9c15df5"
    expected_generation = "55692dba9251bf6f82e419c3dad529257cfe99da1bd63d309716f3d7a476fe7e"
    _require_equal(sha256_file(stage_a_export_dir / "manifest.json"),
                   expected_manifest, "round-two Stage-A manifest bytes")
    manifest, batches, rows, artifacts = load_validated_stage_run(
        stage_a_export_dir, stage_a_output_dir, stage=LINE_DECISION_STAGE,
        contract_override=get_frozen_recall40_round2_stage_a_contract(),
    )
    _require_equal(manifest.get("mode"), "line_decision_recall40_round2",
                   "round-two Stage-A mode")
    _require_equal(artifacts.get("generation_config_sha256"), expected_generation,
                   "round-two Stage-A generation identity")
    _require_equal(len(batches), 270, "round-two Stage-A batch closure")
    _require_equal(len(rows), 270, "round-two Stage-A output closure")
    decisions = line_decision_ledger(batches, rows)
    _require_equal(len(decisions), 4019, "round-two Stage-A line closure")
    with (stage_a_export_dir / "selection.csv").open(newline="", encoding="utf-8") as handle:
        selection = list(csv.DictReader(handle))
    base_columns = list(selection[0])[:16]
    selection_base = [{key: row[key] for key in base_columns} for row in selection]
    row_by_batch = {str(row["batch_id"]): row for row in rows}
    output_files = {str(entry["file"]): dict(entry) for entry in artifacts["output_files"]}
    by_batch_id: dict[str, dict[str, Any]] = {}
    for chunk in manifest["files"]["chunks"]:
        input_name = str(chunk["file"])
        output_name = input_name.replace(".jsonl.gz", ".out.jsonl.gz")
        output_entry = output_files[output_name]
        for batch in read_jsonl_gz(stage_a_export_dir / "chunks" / input_name):
            batch_id = str(batch["batch_id"]); row = row_by_batch[batch_id]
            by_batch_id[batch_id] = {
                "input_chunk_file": f"chunks/{input_name}",
                "input_chunk_sha256": chunk["sha256"],
                "output_chunk_file": output_name,
                "output_chunk_sha256": output_entry["sha256"],
                "input_batch_sha256": sha256_json(batch),
                "raw_output_sha256": hashlib.sha256(str(row["raw_output"]).encode()).hexdigest(),
                "model_output_sha256": row["model_output_sha256"],
                "generation_config_sha256": artifacts["generation_config_sha256"],
            }
    _require_equal(set(by_batch_id), set(row_by_batch),
                   "round-two Stage-A provenance coverage")
    provenance = {
        "stage_a_input_manifest_sha256": artifacts["input_manifest_sha256"],
        "stage_a_line_ledger_contract_sha256": manifest["line_ledger_contract_sha256"],
        "stage_a_generation_config_sha256": artifacts["generation_config_sha256"],
        "stage_a_ordered_output_batch_ids_sha256": artifacts["output_batch_ids_sha256"],
        "source_line_decision_ledger_sha256": sha256_json(decisions),
        "validated_output_batches": artifacts["validated_output_batches"],
        "input_files": artifacts["input_files"], "output_files": artifacts["output_files"],
        "run_metadata_files": artifacts["run_metadata_files"],
        "typed_restart_replay_complete": True,
        "stage_a_selection_base_rows_sha256": sha256_json(selection_base),
        "by_batch_id": by_batch_id,
    }
    provenance["stage_a_provenance_sha256"] = sha256_json(provenance)
    return manifest, batches, rows, decisions, provenance


def derive_recall40_round2_stage_a2_inventory(
    full_export_dir: Path, stage_a_export_dir: Path, stage_a_output_dir: Path,
) -> dict[str, Any]:
    manifest, _batches, _rows, decisions, provenance = (
        load_recall40_round2_stage_a_run(stage_a_export_dir, stage_a_output_dir)
    )
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    selected_ids = [str(value) for value in manifest["selected_parent_ids"]]
    parents = reconstruct_selected_parents(
        replay, selected_ids, round2_route_revision=True
    )
    identities = stage_a_decision_identity_ledger(parents, decisions)
    exceptions = line_adjudication_exception_ledger(
        parents, decisions,
        upstream_stage_a_contract_sha256=manifest["line_ledger_contract_sha256"],
    )
    return {
        "stage_a_decision_count": len(identities),
        "stage_a_decision_identity_ledger_sha256": sha256_json(identities),
        "stage_a_decisions_sha256": sha256_json(decisions),
        "stage_a_provenance_sha256": provenance["stage_a_provenance_sha256"],
        "exception_count": len(exceptions),
        "exception_reason_counts": dict(sorted(Counter(
            str(row["exception_reason"]) for row in exceptions
        ).items())),
        "ordered_exception_source_line_ids_sha256": sha256_json([
            str(row["source_line_id"]) for row in exceptions
        ]),
        "exception_ledger_sha256": sha256_json(exceptions),
    }


def derive_recall40_round3_stage_a2_exception_inventory(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
) -> dict[str, Any]:
    """Derive the complete round-three A2 inventory from exact Stage-A rows."""
    manifest, _batches, _rows, decisions, provenance = (
        load_recall40_round3_stage_a_run(
            stage_a_export_dir, stage_a_output_dir
        )
    )
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    selected_ids = [str(value) for value in manifest["selected_parent_ids"]]
    parents = reconstruct_selected_parents(
        replay, selected_ids, round2_route_revision=True
    )
    identity_ledger = stage_a_decision_identity_ledger(parents, decisions)
    exceptions = round3_line_adjudication_exception_ledger(
        parents,
        decisions,
        upstream_stage_a_contract_sha256=(
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
    )
    source_ids = [str(row["source_line_id"]) for row in exceptions]
    exception_ids = [str(row["exception_id"]) for row in exceptions]
    if (
        len(source_ids) != len(set(source_ids))
        or len(exception_ids) != len(set(exception_ids))
    ):
        raise ValueError("round-three Stage-A2 exception identities duplicate")
    return {
        "scope": "private_recall40_round3_stage_a2_inventory_only",
        "stage_a_contract_sha256": manifest["line_ledger_contract_sha256"],
        "stage_a_input_manifest_sha256": provenance[
            "stage_a_input_manifest_sha256"
        ],
        "stage_a_generation_config_sha256": provenance[
            "stage_a_generation_config_sha256"
        ],
        "stage_a_output_batch_ids_sha256": provenance[
            "stage_a_ordered_output_batch_ids_sha256"
        ],
        "stage_a_decision_ledger_sha256": provenance[
            "source_line_decision_ledger_sha256"
        ],
        "stage_a_provenance_sha256": provenance[
            "stage_a_provenance_sha256"
        ],
        "selected_parent_count": len(selected_ids),
        "selected_parent_ids_sha256": sha256_json(selected_ids),
        "stage_a_decision_count": len(identity_ledger),
        "stage_a_decision_identity_ledger_sha256": sha256_json(
            identity_ledger
        ),
        "exception_count": len(exceptions),
        "exception_reason_counts": dict(sorted(Counter(
            str(row["exception_reason"]) for row in exceptions
        ).items())),
        "ordered_exception_source_line_ids_sha256": sha256_json(source_ids),
        "ordered_exception_ids_sha256": sha256_json(exception_ids),
        "exception_ledger_sha256": sha256_json(exceptions),
        "a2_inputs_built": False,
        "hive_submission_authorized": False,
        "stage_b_or_release_authorized": False,
    }


def build_recall40_round3_line_adjudication_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build an open round-three A2 shell after exact Stage-A completion."""
    manifest, _a_batches, _a_rows, decisions, provenance = (
        load_recall40_round3_stage_a_run(
            stage_a_export_dir, stage_a_output_dir
        )
    )
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    selected_ids = [str(value) for value in manifest["selected_parent_ids"]]
    parents = reconstruct_selected_parents(
        replay, selected_ids, round2_route_revision=True
    )
    identity_ledger = stage_a_decision_identity_ledger(parents, decisions)
    exceptions = round3_line_adjudication_exception_ledger(
        parents,
        decisions,
        upstream_stage_a_contract_sha256=(
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
    )
    contract = (
        get_frozen_recall40_round3_stage_a2_contract()
        if FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256
        else computed_recall40_round3_stage_a2_contract()
    )
    batches = build_recall40_round3_line_adjudication_batches(
        parents,
        decisions,
        stage_a_provenance=provenance,
        contract_override=contract,
    )
    batched_exceptions = [
        {
            key: copy.deepcopy(value)
            for key, value in target.items()
            if key not in {"slot", "stage_a_provenance_locator"}
        }
        for batch in batches for target in batch["targets"]
    ]
    _require_equal(
        batched_exceptions, exceptions,
        "round-three Stage-A2 global exception closure",
    )
    reason_counts = dict(sorted(Counter(
        str(row["exception_reason"]) for row in exceptions
    ).items()))
    source_ids = [str(row["source_line_id"]) for row in exceptions]
    exception_ids = [str(row["exception_id"]) for row in exceptions]
    ensure_empty_output_dir(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    mode = "line_adjudication_recall40_round3"
    selection_entry = write_selection(
        output_dir / "selection.csv",
        _selection_rows(
            parents, mode=mode, stage_a_decisions=decisions
        ),
    )
    chunks = write_batch_chunks(
        output_dir / "chunks", batches, chunk_size
    )
    result = _base_manifest(
        mode=mode,
        stage=LINE_ADJUDICATION_STAGE,
        replay=replay,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunks,
        chunk_size=chunk_size,
        contract_override=contract,
        frozen_contract_sha256_override=(
            contract["line_ledger_contract_sha256"]
            if FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256 else None
        ),
    )
    exception_identity = line_adjudication_exception_identity_ledger(batches)
    result.update({
        "line_adjudication_batch_schema_rule_sha256": (
            RECALL40_ROUND3_STAGE_A2_SCHEMA_RULE_SHA256
        ),
        "line_adjudication_spec_sha256": (
            RECALL40_ROUND3_STAGE_A2_SPEC_SHA256
        ),
        "recall40_round3_stage_a2_scope_spec_sha256": (
            RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC_SHA256
        ),
        "upstream_stage_a_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
        "upstream_stage_a_input_manifest_sha256": provenance[
            "stage_a_input_manifest_sha256"
        ],
        "upstream_stage_a_generation_config_sha256": provenance[
            "stage_a_generation_config_sha256"
        ],
        "upstream_stage_a_output_batch_ids_sha256": provenance[
            "stage_a_ordered_output_batch_ids_sha256"
        ],
        "upstream_stage_a_decision_ledger_sha256": provenance[
            "source_line_decision_ledger_sha256"
        ],
        "stage_a_provenance": provenance,
        "stage_a_decision_identity_ledger": identity_ledger,
        "stage_a_decision_identity_ledger_sha256": sha256_json(
            identity_ledger
        ),
        "stage_a_exception_count": len(exceptions),
        "stage_a_exception_ledger_sha256": sha256_json(exceptions),
        "stage_a_exception_source_line_ids_sha256": sha256_json(source_ids),
        "stage_a_exception_reason_counts": reason_counts,
        "stage_a_exception_identity_ledger": exception_identity,
        "stage_a_exception_identity_ledger_sha256": sha256_json(
            exception_identity
        ),
        "expected_stage_a2_batch_count": len(batches),
        "exception_ledger": {
            "count": len(exceptions),
            "ordered_exception_ids_sha256": sha256_json(exception_ids),
            "ordered_source_line_ids_sha256": sha256_json(source_ids),
            "reason_counts": reason_counts,
        },
        "private_recall40_round3_stage_a2": {
            "enabled": True,
            "round": 3,
            "source_stage_a_contract_sha256": (
                FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
            ),
            "source_stage_a_input_manifest_sha256": (
                FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256
            ),
            "source_stage_a_generation_config_sha256": (
                FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
            ),
            "selected_parent_count": 40,
            "stage_a_decision_count": len(identity_ledger),
            "exception_count": len(exceptions),
            "exception_reason_counts": reason_counts,
            "builder_performed_inference": False,
            "hive_submission_authorized": False,
            "stage_b_or_release_authorized": False,
        },
    })
    finalized = finalize_manifest(output_dir, result)
    validated, _manifest_sha, _paths = validate_input_manifest(
        output_dir / "manifest.json",
        output_dir / "chunks",
        stage=LINE_ADJUDICATION_STAGE,
        contract=contract,
        allow_unfrozen_review=(
            not bool(FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256)
        ),
    )
    _require_equal(
        validated, finalized,
        "round-three Stage-A2 manifest replay",
    )
    return finalized


RECALL40_ROUND3_POST_A2_REPORT_VERSION = (
    "candidate_endorsement_recall40_round3_post_a2_gate_v1"
)
RECALL40_ROUND3_POST_A2_REPORT_FIELDS = frozenset({
    "version",
    "evaluation_scope",
    "gate_decision",
    "passed_checks",
    "failed_checks",
    "checks",
    "selected_parent_count",
    "selected_parent_ids_sha256",
    "stage_a_contract_sha256",
    "stage_a_input_manifest_sha256",
    "stage_a_generation_config_sha256",
    "stage_a_output_batch_ids_sha256",
    "stage_a_batch_count",
    "stage_a_decision_count",
    "stage_a_decisions_sha256",
    "stage_a2_contract_sha256",
    "stage_a2_input_manifest_sha256",
    "stage_a2_generation_config_sha256",
    "stage_a2_output_batch_ids_sha256",
    "stage_a2_batch_count",
    "stage_a2_exception_count",
    "stage_a2_decisions_sha256",
    "stage_a2_run_artifacts_sha256",
    "final_decision_count",
    "final_decisions_sha256",
    "per_line_provenance_sha256",
    "merge_validation",
    "authorized_next_action",
    "bounded_stage_b_input_build_authorized",
    "stage_b_hive_submission_authorized",
    "full_expansion_or_release_authorized",
    "report_payload_sha256",
})


def replay_recall40_round3_stage_a_and_a2(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
) -> dict[str, Any]:
    """Authenticate and merge the exact round-three Stage-A and Stage-A2 runs."""
    (
        stage_a_manifest,
        stage_a_batches,
        stage_a_rows,
        stage_a_decisions,
        stage_a_provenance,
    ) = load_recall40_round3_stage_a_run(
        stage_a_export_dir, stage_a_output_dir
    )
    stage_a2_manifest_path = stage_a2_export_dir / "manifest.json"
    _require_equal(
        sha256_file(stage_a2_manifest_path),
        FROZEN_RECALL40_ROUND3_STAGE_A2_INPUT_MANIFEST_SHA256,
        "round-three Stage-A2 input manifest bytes",
    )
    (
        stage_a2_manifest,
        stage_a2_batches,
        stage_a2_rows,
        stage_a2_artifacts,
    ) = load_validated_stage_run(
        stage_a2_export_dir,
        stage_a2_output_dir,
        stage=LINE_ADJUDICATION_STAGE,
        contract_override=get_frozen_recall40_round3_stage_a2_contract(),
    )
    _require_equal(
        stage_a2_artifacts.get("generation_config_sha256"),
        FROZEN_RECALL40_ROUND3_STAGE_A2_GENERATION_CONFIG_SHA256,
        "round-three Stage-A2 generation identity",
    )
    _require_equal(
        stage_a2_artifacts.get("output_batch_ids_sha256"),
        FROZEN_RECALL40_ROUND3_STAGE_A2_OUTPUT_BATCH_IDS_SHA256,
        "round-three Stage-A2 output batch identity",
    )
    _require_equal(
        len(stage_a2_batches), 3,
        "round-three Stage-A2 input batch closure",
    )
    _require_equal(
        len(stage_a2_rows), 3,
        "round-three Stage-A2 output batch closure",
    )
    selected_ids = [
        str(value) for value in stage_a_manifest["selected_parent_ids"]
    ]
    _require_equal(
        [str(value) for value in stage_a2_manifest["selected_parent_ids"]],
        selected_ids,
        "round-three Stage-A/A2 selected-parent order",
    )
    _require_equal(
        stage_a2_manifest.get("stage_a_provenance"),
        stage_a_provenance,
        "round-three Stage-A2 exact Stage-A provenance",
    )
    source = replay_frozen_source(
        full_export_dir, enforce_frozen_source=True
    )
    _require_equal(
        stage_a_manifest.get("source_export", {}).get(
            "source_export_manifest_sha256"
        ),
        source["manifest_sha256"],
        "round-three Stage-A frozen source binding",
    )
    parents = reconstruct_selected_parents(
        source, selected_ids, round2_route_revision=True
    )
    regenerated = build_recall40_round3_line_adjudication_batches(
        parents,
        stage_a_decisions,
        stage_a_provenance=stage_a_provenance,
        contract_override=get_frozen_recall40_round3_stage_a2_contract(),
    )
    _require_equal(
        [sha256_json(batch) for batch in stage_a2_batches],
        [sha256_json(batch) for batch in regenerated],
        "round-three regenerated Stage-A2 inputs",
    )
    stage_a2_decisions = line_adjudication_ledger(
        stage_a2_batches, stage_a2_rows
    )
    expected_exceptions = round3_line_adjudication_exception_ledger(
        parents,
        stage_a_decisions,
        upstream_stage_a_contract_sha256=(
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
    )
    _require_equal(
        len(stage_a_batches), 270,
        "round-three Stage-A input batch closure at merge",
    )
    _require_equal(
        len(stage_a_rows), 270,
        "round-three Stage-A output batch closure at merge",
    )
    _require_equal(
        len(stage_a_decisions), 4_019,
        "round-three Stage-A line closure at merge",
    )
    _require_equal(
        len(stage_a2_decisions), 11,
        "round-three Stage-A2 decision closure",
    )
    expected_by_line = {
        str(row["source_line_id"]): row for row in expected_exceptions
    }
    _require_equal(
        set(stage_a2_decisions),
        set(expected_by_line),
        "round-three Stage-A2 exception source-line closure",
    )
    for line_id, adjudication in stage_a2_decisions.items():
        expected = expected_by_line[line_id]
        _require_equal(
            adjudication.get("exception_id"),
            expected["exception_id"],
            f"round-three Stage-A2 exception identity {line_id}",
        )
        _require_equal(
            adjudication.get("exception_reason"),
            expected["exception_reason"],
            f"round-three Stage-A2 exception reason {line_id}",
        )
    merged = merge_line_adjudication_ledger(
        parents,
        stage_a_decisions,
        stage_a2_decisions,
        upstream_stage_a_contract_sha256=(
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
        exception_ledger_semantics="recall40_round3",
    )
    _require_equal(
        set(merged["final_decisions"]),
        set(stage_a_decisions),
        "round-three final line closure",
    )
    _require_equal(
        set(merged["per_line_provenance"]),
        set(stage_a_decisions),
        "round-three final provenance closure",
    )
    return {
        "stage_a_manifest": stage_a_manifest,
        "stage_a_batches": stage_a_batches,
        "stage_a_rows": stage_a_rows,
        "stage_a_decisions": stage_a_decisions,
        "stage_a_provenance": stage_a_provenance,
        "stage_a2_manifest": stage_a2_manifest,
        "stage_a2_batches": stage_a2_batches,
        "stage_a2_rows": stage_a2_rows,
        "stage_a2_decisions": stage_a2_decisions,
        "stage_a2_artifacts": stage_a2_artifacts,
        "parents": parents,
        "merge": merged,
    }


def recall40_round3_post_a2_evaluation_report(
    replay: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the canonical fail-closed round-three A2-to-B build gate."""
    stage_a_provenance = replay["stage_a_provenance"]
    stage_a2_manifest = replay["stage_a2_manifest"]
    stage_a2_artifacts = replay["stage_a2_artifacts"]
    merge = replay["merge"]
    validation = merge["validation"]
    expected_validation = {
        "expected_exception_count": 11,
        "adjudicated_exception_count": 11,
        "unresolved_line_count": 0,
        "withheld_line_count": 0,
        "unowned_continuation_count": 0,
        "duplicate_continuation_owner_count": 0,
        "ownership_conflict_count": 0,
        "coverage_complete": True,
        "validation_passed": True,
    }
    final_decisions = merge["final_decisions"]
    per_line_provenance = merge["per_line_provenance"]
    selected_parent_ids = [
        str(parent["parent_document_id"])
        for parent in replay["parents"]
    ]
    selected_parent_ids_sha256 = sha256_json(selected_parent_ids)
    checks = [
        {
            "check_id": "round3_exact_parent_closure",
            "passed": (
                len(selected_parent_ids) == 40
                and selected_parent_ids_sha256
                == RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
                    "selected_parent_ids_sha256"
                ]
            ),
            "observed": {
                "parent_count": len(selected_parent_ids),
                "selected_parent_ids_sha256": selected_parent_ids_sha256,
            },
            "expected": {
                "parent_count": 40,
                "selected_parent_ids_sha256": (
                    RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
                        "selected_parent_ids_sha256"
                    ]
                ),
            },
        },
        {
            "check_id": "round3_exact_stage_a_replay",
            "passed": (
                len(replay["stage_a_batches"]) == 270
                and len(replay["stage_a_rows"]) == 270
                and len(replay["stage_a_decisions"]) == 4_019
                and stage_a_provenance.get(
                    "stage_a_input_manifest_sha256"
                ) == FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256
                and stage_a_provenance.get(
                    "stage_a_generation_config_sha256"
                ) == FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
                and stage_a_provenance.get(
                    "stage_a_ordered_output_batch_ids_sha256"
                ) == FROZEN_RECALL40_ROUND3_STAGE_A_OUTPUT_BATCH_IDS_SHA256
                and stage_a_provenance.get(
                    "source_line_decision_ledger_sha256"
                ) == FROZEN_RECALL40_ROUND3_STAGE_A_DECISION_LEDGER_SHA256
                and stage_a_provenance.get(
                    "stage_a_provenance_sha256"
                ) == FROZEN_RECALL40_ROUND3_STAGE_A_PROVENANCE_SHA256
            ),
            "observed": {
                "batch_count": len(replay["stage_a_batches"]),
                "output_count": len(replay["stage_a_rows"]),
                "decision_count": len(replay["stage_a_decisions"]),
                "input_manifest_sha256": stage_a_provenance.get(
                    "stage_a_input_manifest_sha256"
                ),
                "generation_config_sha256": stage_a_provenance.get(
                    "stage_a_generation_config_sha256"
                ),
                "output_batch_ids_sha256": stage_a_provenance.get(
                    "stage_a_ordered_output_batch_ids_sha256"
                ),
                "decision_ledger_sha256": stage_a_provenance.get(
                    "source_line_decision_ledger_sha256"
                ),
                "provenance_sha256": stage_a_provenance.get(
                    "stage_a_provenance_sha256"
                ),
            },
            "expected": {
                "batch_count": 270,
                "output_count": 270,
                "decision_count": 4_019,
                "input_manifest_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256
                ),
                "generation_config_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
                ),
                "output_batch_ids_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A_OUTPUT_BATCH_IDS_SHA256
                ),
                "decision_ledger_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A_DECISION_LEDGER_SHA256
                ),
                "provenance_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A_PROVENANCE_SHA256
                ),
            },
        },
        {
            "check_id": "round3_exact_stage_a2_replay",
            "passed": (
                len(replay["stage_a2_batches"]) == 3
                and len(replay["stage_a2_rows"]) == 3
                and len(replay["stage_a2_decisions"]) == 11
                and stage_a2_manifest.get("line_ledger_contract_sha256")
                == FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256
                and stage_a2_artifacts.get("input_manifest_sha256")
                == FROZEN_RECALL40_ROUND3_STAGE_A2_INPUT_MANIFEST_SHA256
                and stage_a2_artifacts.get("generation_config_sha256")
                == FROZEN_RECALL40_ROUND3_STAGE_A2_GENERATION_CONFIG_SHA256
                and stage_a2_artifacts.get("output_batch_ids_sha256")
                == FROZEN_RECALL40_ROUND3_STAGE_A2_OUTPUT_BATCH_IDS_SHA256
                and stage_a2_artifacts.get("typed_restart_replay_complete")
                is True
            ),
            "observed": {
                "batch_count": len(replay["stage_a2_batches"]),
                "output_count": len(replay["stage_a2_rows"]),
                "decision_count": len(replay["stage_a2_decisions"]),
                "contract_sha256": stage_a2_manifest.get(
                    "line_ledger_contract_sha256"
                ),
                "input_manifest_sha256": stage_a2_artifacts.get(
                    "input_manifest_sha256"
                ),
                "generation_config_sha256": stage_a2_artifacts.get(
                    "generation_config_sha256"
                ),
                "output_batch_ids_sha256": stage_a2_artifacts.get(
                    "output_batch_ids_sha256"
                ),
                "typed_restart_replay_complete": stage_a2_artifacts.get(
                    "typed_restart_replay_complete"
                ),
            },
            "expected": {
                "batch_count": 3,
                "output_count": 3,
                "decision_count": 11,
                "contract_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256
                ),
                "input_manifest_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A2_INPUT_MANIFEST_SHA256
                ),
                "generation_config_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A2_GENERATION_CONFIG_SHA256
                ),
                "output_batch_ids_sha256": (
                    FROZEN_RECALL40_ROUND3_STAGE_A2_OUTPUT_BATCH_IDS_SHA256
                ),
                "typed_restart_replay_complete": True,
            },
        },
        {
            "check_id": "round3_stage_a2_exact_upstream_binding",
            "passed": (
                stage_a2_manifest.get("stage_a_provenance")
                == stage_a_provenance
            ),
            "observed": stage_a2_manifest.get(
                "stage_a_provenance", {}
            ).get("stage_a_provenance_sha256"),
            "expected": stage_a_provenance.get(
                "stage_a_provenance_sha256"
            ),
        },
        {
            "check_id": "round3_zero_conflict_two_pass_merge",
            "passed": sha256_json(validation) == sha256_json(
                expected_validation
            ),
            "observed": copy.deepcopy(validation),
            "expected": expected_validation,
        },
        {
            "check_id": "round3_final_line_and_provenance_closure",
            "passed": (
                len(final_decisions) == 4_019
                and set(final_decisions) == set(replay["stage_a_decisions"])
                and set(per_line_provenance)
                == set(replay["stage_a_decisions"])
                and merge.get("final_decisions_sha256")
                == sha256_json(final_decisions)
                and merge.get("per_line_provenance_sha256")
                == sha256_json(per_line_provenance)
            ),
            "observed": {
                "final_decision_count": len(final_decisions),
                "provenance_count": len(per_line_provenance),
                "final_decisions_sha256": merge.get(
                    "final_decisions_sha256"
                ),
                "per_line_provenance_sha256": merge.get(
                    "per_line_provenance_sha256"
                ),
            },
            "expected": {
                "final_decision_count": 4_019,
                "provenance_count": 4_019,
                "final_decisions_sha256": sha256_json(final_decisions),
                "per_line_provenance_sha256": sha256_json(
                    per_line_provenance
                ),
            },
        },
    ]
    passed_checks = sum(check["passed"] is True for check in checks)
    authorized = passed_checks == len(checks)
    stage_a2_run_artifacts = {
        key: copy.deepcopy(stage_a2_artifacts[key])
        for key in (
            "input_manifest_sha256",
            "generation_config_sha256",
            "input_files",
            "output_files",
            "output_batch_ids_sha256",
            "validated_output_batches",
            "run_metadata_files",
            "typed_restart_replay_complete",
        )
    }
    report = {
        "version": RECALL40_ROUND3_POST_A2_REPORT_VERSION,
        "evaluation_scope": (
            "private_recall40_round3_stage_a2_to_bounded_stage_b_inputs_only"
        ),
        "gate_decision": (
            "GO_BUILD_BOUNDED_STAGE_B_INPUTS" if authorized else "NO_GO"
        ),
        "passed_checks": passed_checks,
        "failed_checks": len(checks) - passed_checks,
        "checks": checks,
        "selected_parent_count": len(selected_parent_ids),
        "selected_parent_ids_sha256": selected_parent_ids_sha256,
        "stage_a_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
        "stage_a_input_manifest_sha256": stage_a_provenance[
            "stage_a_input_manifest_sha256"
        ],
        "stage_a_generation_config_sha256": stage_a_provenance[
            "stage_a_generation_config_sha256"
        ],
        "stage_a_output_batch_ids_sha256": stage_a_provenance[
            "stage_a_ordered_output_batch_ids_sha256"
        ],
        "stage_a_batch_count": len(replay["stage_a_batches"]),
        "stage_a_decision_count": len(replay["stage_a_decisions"]),
        "stage_a_decisions_sha256": sha256_json(
            replay["stage_a_decisions"]
        ),
        "stage_a2_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256
        ),
        "stage_a2_input_manifest_sha256": stage_a2_artifacts[
            "input_manifest_sha256"
        ],
        "stage_a2_generation_config_sha256": stage_a2_artifacts[
            "generation_config_sha256"
        ],
        "stage_a2_output_batch_ids_sha256": stage_a2_artifacts[
            "output_batch_ids_sha256"
        ],
        "stage_a2_batch_count": len(replay["stage_a2_batches"]),
        "stage_a2_exception_count": len(replay["stage_a2_decisions"]),
        "stage_a2_decisions_sha256": sha256_json(
            replay["stage_a2_decisions"]
        ),
        "stage_a2_run_artifacts_sha256": sha256_json(
            stage_a2_run_artifacts
        ),
        "final_decision_count": len(final_decisions),
        "final_decisions_sha256": merge["final_decisions_sha256"],
        "per_line_provenance_sha256": merge[
            "per_line_provenance_sha256"
        ],
        "merge_validation": copy.deepcopy(validation),
        "authorized_next_action": (
            "build_private_recall40_round3_bounded_stage_b_inputs"
            if authorized else None
        ),
        "bounded_stage_b_input_build_authorized": authorized,
        "stage_b_hive_submission_authorized": False,
        "full_expansion_or_release_authorized": False,
    }
    report["report_payload_sha256"] = sha256_json(report)
    if set(report) != RECALL40_ROUND3_POST_A2_REPORT_FIELDS:
        raise AssertionError("round-three post-A2 report field set drifted")
    return report


def validate_recall40_round3_post_a2_evaluation_report(
    report: Mapping[str, Any],
    replay: Mapping[str, Any],
) -> dict[str, Any]:
    """Require an exact canonical report derived from the supplied replay."""
    if set(report) != RECALL40_ROUND3_POST_A2_REPORT_FIELDS:
        raise ValueError("round-three post-A2 report field set differs")
    payload = copy.deepcopy(dict(report))
    recorded = payload.pop("report_payload_sha256", None)
    if not isinstance(recorded, str) or sha256_json(payload) != recorded:
        raise ValueError("round-three post-A2 report payload hash differs")
    canonical = recall40_round3_post_a2_evaluation_report(replay)
    if sha256_json(report) != sha256_json(canonical):
        raise ValueError("round-three canonical post-A2 report differs")
    return canonical


def load_recall40_round3_post_a2_evaluation_report(
    report_path: Path,
    replay: Mapping[str, Any],
) -> dict[str, Any]:
    """Load one duplicate-key-free report and bind it to an exact replay."""
    report = load_json_no_duplicates(report_path)
    if not isinstance(report, Mapping):
        raise TypeError("round-three post-A2 report is not an object")
    return validate_recall40_round3_post_a2_evaluation_report(
        report, replay
    )


def build_recall40_round3_record_extraction_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    evaluation_report_path: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build the open round-three Stage-B review artifact after exact A2 GO."""
    ensure_empty_output_dir(output_dir)
    resolved_inputs = {
        full_export_dir.resolve(), stage_a_export_dir.resolve(),
        stage_a_output_dir.resolve(), stage_a2_export_dir.resolve(),
        stage_a2_output_dir.resolve(), evaluation_report_path.resolve(),
    }
    if output_dir.resolve() in resolved_inputs:
        raise ValueError("round-three Stage-B output cannot overwrite an input")
    if chunk_size != RECALL40_ROUND3_STAGE_B_SCOPE_SPEC["chunk_size"]:
        raise ValueError("round-three Stage-B chunk size differs")

    replay = replay_recall40_round3_stage_a_and_a2(
        full_export_dir,
        stage_a_export_dir,
        stage_a_output_dir,
        stage_a2_export_dir,
        stage_a2_output_dir,
    )
    report = load_recall40_round3_post_a2_evaluation_report(
        evaluation_report_path, replay
    )
    _require_equal(
        sha256_file(evaluation_report_path),
        RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "post_a2_report_file_sha256"
        ],
        "round-three post-A2 report bytes",
    )
    _require_equal(
        sha256_json(report),
        RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "post_a2_report_object_sha256"
        ],
        "round-three post-A2 whole report object",
    )
    _require_equal(
        report.get("report_payload_sha256"),
        RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "post_a2_report_payload_sha256"
        ],
        "round-three post-A2 report payload",
    )
    if (
        report.get("gate_decision") != "GO_BUILD_BOUNDED_STAGE_B_INPUTS"
        or type(report.get("passed_checks")) is not int
        or report["passed_checks"] != 6
        or type(report.get("failed_checks")) is not int
        or report["failed_checks"] != 0
        or report.get("bounded_stage_b_input_build_authorized") is not True
        or report.get("stage_b_hive_submission_authorized") is not False
        or report.get("full_expansion_or_release_authorized") is not False
    ):
        raise ValueError("round-three post-A2 report does not authorize this build")

    contract = get_frozen_recall40_round3_stage_b_contract()
    batches = build_record_extraction_batches_from_adjudicated_merge(
        replay["parents"], replay["merge"], contract_override=contract
    )
    closure = round3_stage_b_closure_ledgers(batches)
    for field, actual in closure.items():
        _require_equal(
            actual, RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[field],
            f"round-three Stage-B {field}",
        )

    stage_a_provenance = replay["stage_a_provenance"]
    stage_a2_artifacts = replay["stage_a2_artifacts"]
    stage_a2_run_artifacts = {
        key: copy.deepcopy(stage_a2_artifacts[key])
        for key in (
            "input_manifest_sha256", "generation_config_sha256",
            "input_files", "output_files", "output_batch_ids_sha256",
            "validated_output_batches", "run_metadata_files",
            "typed_restart_replay_complete",
        )
    }
    upstream_artifact_hashes = {
        "stage_a_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
        "stage_a_input_manifest_sha256": (
            stage_a_provenance["stage_a_input_manifest_sha256"]
        ),
        "stage_a_generation_config_sha256": (
            stage_a_provenance["stage_a_generation_config_sha256"]
        ),
        "stage_a_output_batch_ids_sha256": (
            stage_a_provenance["stage_a_ordered_output_batch_ids_sha256"]
        ),
        "stage_a_decision_ledger_sha256": (
            stage_a_provenance["source_line_decision_ledger_sha256"]
        ),
        "stage_a_provenance_sha256": (
            stage_a_provenance["stage_a_provenance_sha256"]
        ),
        "stage_a_provenance_object_sha256": sha256_json(
            stage_a_provenance
        ),
        "stage_a_input_files_sha256": sha256_json(
            stage_a_provenance["input_files"]
        ),
        "stage_a_output_files_sha256": sha256_json(
            stage_a_provenance["output_files"]
        ),
        "stage_a_metadata_files_sha256": sha256_json(
            stage_a_provenance["run_metadata_files"]
        ),
        "stage_a2_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256
        ),
        "stage_a2_input_manifest_sha256": (
            stage_a2_artifacts["input_manifest_sha256"]
        ),
        "stage_a2_generation_config_sha256": (
            stage_a2_artifacts["generation_config_sha256"]
        ),
        "stage_a2_output_batch_ids_sha256": (
            stage_a2_artifacts["output_batch_ids_sha256"]
        ),
        "stage_a2_decision_ledger_sha256": sha256_json(
            replay["stage_a2_decisions"]
        ),
        "stage_a2_artifacts_object_sha256": sha256_json(
            stage_a2_artifacts
        ),
        "stage_a2_run_artifacts_sha256": sha256_json(
            stage_a2_run_artifacts
        ),
        "stage_a2_input_files_sha256": sha256_json(
            stage_a2_artifacts["input_files"]
        ),
        "stage_a2_output_files_sha256": sha256_json(
            stage_a2_artifacts["output_files"]
        ),
        "stage_a2_metadata_files_sha256": sha256_json(
            stage_a2_artifacts["run_metadata_files"]
        ),
    }
    expected_artifact_hashes = {
        "stage_a_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
        "stage_a_input_manifest_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_input_manifest_sha256"
            ]
        ),
        "stage_a_generation_config_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_generation_config_sha256"
            ]
        ),
        "stage_a_output_batch_ids_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_output_batch_ids_sha256"
            ]
        ),
        "stage_a_decision_ledger_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_decision_ledger_sha256"
            ]
        ),
        "stage_a_provenance_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_provenance_sha256"
            ]
        ),
        "stage_a_provenance_object_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a_provenance_object_sha256"
            ]
        ),
        "stage_a_input_files_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a_input_files_sha256"
            ]
        ),
        "stage_a_output_files_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a_output_files_sha256"
            ]
        ),
        "stage_a_metadata_files_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a_metadata_files_sha256"
            ]
        ),
        "stage_a2_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256
        ),
        "stage_a2_input_manifest_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a2_input_manifest_sha256"
            ]
        ),
        "stage_a2_generation_config_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a2_generation_config_sha256"
            ]
        ),
        "stage_a2_output_batch_ids_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a2_output_batch_ids_sha256"
            ]
        ),
        "stage_a2_decision_ledger_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a2_decision_ledger_sha256"
            ]
        ),
        "stage_a2_artifacts_object_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a2_artifacts_object_sha256"
            ]
        ),
        "stage_a2_run_artifacts_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a2_run_artifacts_sha256"
            ]
        ),
        "stage_a2_input_files_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a2_input_files_sha256"
            ]
        ),
        "stage_a2_output_files_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a2_output_files_sha256"
            ]
        ),
        "stage_a2_metadata_files_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                "upstream_stage_a2_metadata_files_sha256"
            ]
        ),
    }
    _require_equal(
        upstream_artifact_hashes, expected_artifact_hashes,
        "round-three Stage-B exact upstream artifacts",
    )
    upstream = two_pass_stage_b_manifest_provenance(
        stage_a_provenance,
        stage_a2_artifacts,
        replay["stage_a2_decisions"],
        replay["merge"],
    )
    _require_equal(
        sha256_json(upstream["upstream_stage_a"]),
        RECALL40_ROUND3_STAGE_B_SCOPE_SPEC["upstream_stage_a_object_sha256"],
        "round-three Stage-B upstream Stage-A object",
    )
    _require_equal(
        sha256_json(upstream["upstream_stage_a2"]),
        RECALL40_ROUND3_STAGE_B_SCOPE_SPEC["upstream_stage_a2_object_sha256"],
        "round-three Stage-B upstream Stage-A2 object",
    )
    _require_equal(
        sha256_json(upstream["two_pass_merge"]),
        RECALL40_ROUND3_STAGE_B_SCOPE_SPEC["two_pass_merge_object_sha256"],
        "round-three Stage-B two-pass merge object",
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    mode = "record_extraction_recall40_round3"
    selection_entry = write_selection(
        output_dir / "selection.csv",
        _selection_rows(
            replay["parents"], mode=mode,
            stage_a_decisions=replay["merge"]["final_decisions"],
        ),
    )
    _require_equal(
        selection_entry["sha256"],
        RECALL40_ROUND3_STAGE_B_SCOPE_SPEC["selection_csv_sha256"],
        "round-three Stage-B selection bytes",
    )
    _require_equal(
        selection_entry["bytes"],
        RECALL40_ROUND3_STAGE_B_SCOPE_SPEC["selection_csv_bytes"],
        "round-three Stage-B selection size",
    )
    chunk_entries = write_batch_chunks(
        output_dir / "chunks", batches, chunk_size
    )
    _require_equal(
        [entry["records"] for entry in chunk_entries],
        RECALL40_ROUND3_STAGE_B_SCOPE_SPEC["chunk_record_counts"],
        "round-three Stage-B chunk partition",
    )
    source = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    manifest = _base_manifest(
        mode=mode,
        stage=RECORD_EXTRACTION_STAGE,
        replay=source,
        parents=replay["parents"],
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunk_entries,
        chunk_size=chunk_size,
        contract_override=contract,
        frozen_contract_sha256_override=(
            FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
        ),
    )
    manifest.update({
        "stage_b_upstream_pass_count": 2,
        "stage_b_two_pass_provenance_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC_SHA256
        ),
        "record_extraction_batch_schema_rule_sha256": (
            RECALL40_ROUND3_STAGE_B_SCHEMA_RULE_SHA256
        ),
        "recall40_round3_stage_b_semantic_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_SEMANTIC_SPEC_SHA256
        ),
        "recall40_round3_stage_b_source_catalog_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_SOURCE_CATALOG_SPEC_SHA256
        ),
        "recall40_round3_stage_b_record_projection_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_RECORD_PROJECTION_SPEC_SHA256
        ),
        "recall40_round3_stage_b_rendering_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_RENDERING_SPEC_SHA256
        ),
        "recall40_round3_stage_b_scope_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC_SHA256
        ),
        "stage_b_closure": closure,
        "upstream_artifact_hashes": upstream_artifact_hashes,
        **upstream,
        "round3_stage_b_gate": {
            "report_file_sha256": sha256_file(evaluation_report_path),
            "report_object_sha256": sha256_json(report),
            "report": copy.deepcopy(report),
        },
        "private_recall40_round3_stage_b": {
            "enabled": True,
            "round": 3,
            "authorization_scope": (
                "private_recall40_round3_bounded_stage_b_inputs_only"
            ),
            "selected_parent_count": 40,
            "stage_a_line_count": len(replay["stage_a_decisions"]),
            "stage_a2_exception_count": len(replay["stage_a2_decisions"]),
            "final_anchor_count": closure["stage_b_anchor_count"],
            "ordinary_anchor_count": closure["ordinary_anchor_count"],
            "compound_anchor_count": closure["compound_anchor_count"],
            "stage_b_batch_count": closure["stage_b_batch_count"],
            "stage_b_input_build_authorized": True,
            "builder_performed_inference": False,
            "contract_is_frozen": True,
            "human_name_adjudication_imported": False,
            "independent_pre_freeze_review_required": False,
            "hive_submission_authorized": False,
            "full_expansion_or_release_authorized": False,
        },
    })
    finalized = finalize_manifest(output_dir, manifest)
    validated, _manifest_sha256, _paths = validate_input_manifest(
        output_dir / "manifest.json",
        output_dir / "chunks",
        stage=RECORD_EXTRACTION_STAGE,
        contract=contract,
    )
    _require_equal(
        validated, finalized, "round-three Stage-B manifest replay"
    )
    return finalized


def derive_recall40_stage_a2_exception_inventory(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
) -> dict[str, Any]:
    """Return the complete mechanically derived A2 inventory, without IDs/text."""
    manifest, _batches, _rows, decisions, provenance = (
        load_recall40_stage_a_run(stage_a_export_dir, stage_a_output_dir)
    )
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    selected_ids = [str(value) for value in manifest["selected_parent_ids"]]
    parents = reconstruct_selected_parents(replay, selected_ids)
    identity_ledger = stage_a_decision_identity_ledger(parents, decisions)
    exceptions = line_adjudication_exception_ledger(
        parents, decisions,
        upstream_stage_a_contract_sha256=manifest[
            "line_ledger_contract_sha256"
        ],
    )
    reason_counts = Counter(str(row["exception_reason"]) for row in exceptions)
    source_ids = [str(row["source_line_id"]) for row in exceptions]
    if len(source_ids) != len(set(source_ids)):
        raise ValueError("recall40 Stage-A2 exception identities duplicate")
    return {
        "scope": "private_recall40_stage_a2_inventory_only",
        "stage_a_input_manifest_sha256": provenance[
            "stage_a_input_manifest_sha256"
        ],
        "stage_a_generation_config_sha256": provenance[
            "stage_a_generation_config_sha256"
        ],
        "selected_parent_count": len(selected_ids),
        "stage_a_decision_count": len(identity_ledger),
        "stage_a_decision_identity_ledger_sha256": sha256_json(identity_ledger),
        "exception_count": len(exceptions),
        "exception_reason_counts": dict(sorted(reason_counts.items())),
        "ordered_exception_source_line_ids_sha256": sha256_json(source_ids),
        "a2_inputs_built": False,
        "requires_distinct_upstream_contract_rotation": (
            manifest["line_ledger_contract_sha256"]
            != FROZEN_STAGE_A_V2_CONTRACT_SHA256
        ),
        "stage_b_authorized": False,
    }


def build_recall40_line_adjudication_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build private recall40 A2 inputs only from a complete real Stage-A run."""
    ensure_empty_output_dir(output_dir)
    manifest, _a_batches, _a_rows, decisions, provenance = (
        load_recall40_stage_a_run(stage_a_export_dir, stage_a_output_dir)
    )
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    selected_ids = [str(value) for value in manifest["selected_parent_ids"]]
    parents = reconstruct_selected_parents(replay, selected_ids)
    identity_ledger = stage_a_decision_identity_ledger(parents, decisions)
    exceptions = line_adjudication_exception_ledger(
        parents, decisions,
        upstream_stage_a_contract_sha256=manifest[
            "line_ledger_contract_sha256"
        ],
    )
    reason_counts = _validate_stage_a2_exception_census_generic(
        exceptions, decisions
    )
    recall40_contract = get_frozen_recall40_stage_a2_contract()
    batches = build_line_adjudication_batches(
        parents, decisions, stage_a_provenance=provenance,
        contract_override=recall40_contract,
    )
    _require_equal(
        sum(len(batch["targets"]) for batch in batches), len(exceptions),
        "recall40 Stage-A2 target closure",
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    selection_entry = write_selection(
        output_dir / "selection.csv",
        _selection_rows(parents, mode="line_adjudication_recall_pilot40",
                        stage_a_decisions=decisions),
    )
    chunk_entries = write_batch_chunks(output_dir / "chunks", batches, chunk_size)
    result = _base_manifest(
        mode="line_adjudication_recall_pilot40",
        stage=LINE_ADJUDICATION_STAGE,
        replay=replay,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunk_entries,
        chunk_size=chunk_size,
        contract_override=recall40_contract,
        frozen_contract_sha256_override=(
            FROZEN_RECALL40_STAGE_A2_CONTRACT_SHA256
        ),
    )
    exception_identity = line_adjudication_exception_identity_ledger(batches)
    batched_exceptions = [
        {key: value for key, value in target.items()
         if key not in {"slot", "stage_a_provenance_locator"}}
        for batch in batches for target in batch["targets"]
    ]
    source_line_ids = [str(row["source_line_id"]) for row in batched_exceptions]
    exception_ids = [str(row["exception_id"]) for row in batched_exceptions]
    result.update({
        "line_adjudication_batch_schema_rule_sha256": (
            LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256
        ),
        "line_adjudication_spec_sha256": LINE_ADJUDICATION_SPEC_SHA256,
        "upstream_stage_a_contract_sha256": manifest[
            "line_ledger_contract_sha256"
        ],
        "stage_a_provenance": provenance,
        "stage_a_decision_identity_ledger": identity_ledger,
        "stage_a_decision_identity_ledger_sha256": sha256_json(identity_ledger),
        "stage_a_exception_count": len(exceptions),
        "stage_a_exception_ledger_sha256": sha256_json(batched_exceptions),
        "stage_a_exception_source_line_ids_sha256": sha256_json(source_line_ids),
        "stage_a_exception_reason_counts": reason_counts,
        "stage_a_exception_identity_ledger": exception_identity,
        "stage_a_exception_identity_ledger_sha256": sha256_json(exception_identity),
        "expected_stage_a2_batch_count": len(batches),
        "exception_ledger": {
            "count": len(batched_exceptions),
            "ordered_exception_ids_sha256": sha256_json(exception_ids),
            "ordered_source_line_ids_sha256": sha256_json(source_line_ids),
            "reason_counts": reason_counts,
        },
        "private_recall40_stage_a2": {
            "enabled": True,
            "source_stage_a_input_manifest_sha256": (
                FROZEN_RECALL40_STAGE_A_INPUT_MANIFEST_SHA256
            ),
            "source_stage_a_generation_config_sha256": (
                FROZEN_RECALL40_STAGE_A_GENERATION_CONFIG_SHA256
            ),
            "selected_parent_count": 40,
            "exception_count": len(exceptions),
            "exception_reason_counts": reason_counts,
            "builder_performed_inference": False,
            "hive_submission_authorized": False,
            "stage_b_authorized": False,
        },
    })
    finalized = finalize_manifest(output_dir, result)
    validated, _manifest_sha, _paths = validate_input_manifest(
        output_dir / "manifest.json", output_dir / "chunks",
        stage=LINE_ADJUDICATION_STAGE, contract=recall40_contract,
    )
    _require_equal(validated, finalized, "recall40 frozen A2 manifest replay")
    return finalized


def build_recall40_round2_line_adjudication_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build the frozen private round-two A2 input from the real Stage-A run."""
    ensure_empty_output_dir(output_dir)
    manifest, _a_batches, _a_rows, decisions, provenance = (
        load_recall40_round2_stage_a_run(stage_a_export_dir, stage_a_output_dir)
    )
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    selected_ids = [str(value) for value in manifest["selected_parent_ids"]]
    parents = reconstruct_selected_parents(
        replay, selected_ids, round2_route_revision=True
    )
    identity_ledger = stage_a_decision_identity_ledger(parents, decisions)
    exceptions = line_adjudication_exception_ledger(
        parents,
        decisions,
        upstream_stage_a_contract_sha256=manifest[
            "line_ledger_contract_sha256"
        ],
    )
    reason_counts = _validate_stage_a2_exception_census_generic(
        exceptions, decisions
    )
    contract = get_frozen_recall40_round2_stage_a2_contract()
    batches = build_line_adjudication_batches(
        parents,
        decisions,
        stage_a_provenance=provenance,
        contract_override=contract,
    )
    _require_equal(
        sum(len(batch["targets"]) for batch in batches),
        len(exceptions),
        "round-two Stage-A2 target closure",
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    selection_entry = write_selection(
        output_dir / "selection.csv",
        _selection_rows(
            parents,
            mode="line_adjudication_recall40_round2",
            stage_a_decisions=decisions,
        ),
    )
    chunk_entries = write_batch_chunks(
        output_dir / "chunks", batches, chunk_size
    )
    result = _base_manifest(
        mode="line_adjudication_recall40_round2",
        stage=LINE_ADJUDICATION_STAGE,
        replay=replay,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunk_entries,
        chunk_size=chunk_size,
        contract_override=contract,
        frozen_contract_sha256_override=(
            FROZEN_RECALL40_ROUND2_STAGE_A2_CONTRACT_SHA256
        ),
    )
    exception_identity = line_adjudication_exception_identity_ledger(batches)
    batched_exceptions = [
        {
            key: value
            for key, value in target.items()
            if key not in {"slot", "stage_a_provenance_locator"}
        }
        for batch in batches
        for target in batch["targets"]
    ]
    source_line_ids = [
        str(row["source_line_id"]) for row in batched_exceptions
    ]
    exception_ids = [str(row["exception_id"]) for row in batched_exceptions]
    result.update(
        {
            "line_adjudication_batch_schema_rule_sha256": (
                LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256
            ),
            "line_adjudication_spec_sha256": LINE_ADJUDICATION_SPEC_SHA256,
            "upstream_stage_a_contract_sha256": manifest[
                "line_ledger_contract_sha256"
            ],
            "stage_a_provenance": provenance,
            "stage_a_decision_identity_ledger": identity_ledger,
            "stage_a_decision_identity_ledger_sha256": sha256_json(
                identity_ledger
            ),
            "stage_a_exception_count": len(exceptions),
            "stage_a_exception_ledger_sha256": sha256_json(
                batched_exceptions
            ),
            "stage_a_exception_source_line_ids_sha256": sha256_json(
                source_line_ids
            ),
            "stage_a_exception_reason_counts": reason_counts,
            "stage_a_exception_identity_ledger": exception_identity,
            "stage_a_exception_identity_ledger_sha256": sha256_json(
                exception_identity
            ),
            "expected_stage_a2_batch_count": len(batches),
            "exception_ledger": {
                "count": len(batched_exceptions),
                "ordered_exception_ids_sha256": sha256_json(exception_ids),
                "ordered_source_line_ids_sha256": sha256_json(source_line_ids),
                "reason_counts": reason_counts,
            },
            "private_recall40_round2_stage_a2": {
                "enabled": True,
                "source_stage_a_input_manifest_sha256": (
                    "7918a6d6c38a91ad65f7fb1e8aa93065451509cb10cd771f5aa50df9d9c15df5"
                ),
                "source_stage_a_generation_config_sha256": (
                    "55692dba9251bf6f82e419c3dad529257cfe99da1bd63d309716f3d7a476fe7e"
                ),
                "selected_parent_count": 40,
                "stage_a_decision_count": len(identity_ledger),
                "exception_count": len(exceptions),
                "exception_reason_counts": reason_counts,
                "builder_performed_inference": False,
                "hive_submission_authorized": False,
                "stage_b_authorized": False,
                "full_expansion_or_release_authorized": False,
            },
        }
    )
    finalized = finalize_manifest(output_dir, result)
    validated, _manifest_sha, _paths = validate_input_manifest(
        output_dir / "manifest.json",
        output_dir / "chunks",
        stage=LINE_ADJUDICATION_STAGE,
        contract=contract,
    )
    _require_equal(
        validated, finalized, "round-two frozen A2 manifest replay"
    )
    return finalized


def replay_recall40_stage_a_and_a2(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
) -> dict[str, Any]:
    """Authenticate and merge the exact private recall40 A1+A2 runs."""
    (
        stage_a_manifest,
        stage_a_batches,
        stage_a_rows,
        stage_a_decisions,
        stage_a_provenance,
    ) = load_recall40_stage_a_run(stage_a_export_dir, stage_a_output_dir)
    _require_equal(
        sha256_file(stage_a2_export_dir / "manifest.json"),
        FROZEN_RECALL40_STAGE_A2_INPUT_MANIFEST_SHA256,
        "recall40 Stage-A2 input manifest bytes",
    )
    stage_a2_manifest, stage_a2_batches, stage_a2_rows, stage_a2_artifacts = (
        load_validated_stage_run(
            stage_a2_export_dir,
            stage_a2_output_dir,
            stage=LINE_ADJUDICATION_STAGE,
            contract_override=get_frozen_recall40_stage_a2_contract(),
        )
    )
    _require_equal(
        stage_a2_artifacts.get("generation_config_sha256"),
        FROZEN_RECALL40_STAGE_A2_GENERATION_CONFIG_SHA256,
        "recall40 Stage-A2 generation identity",
    )
    selected_ids = [str(value) for value in stage_a_manifest["selected_parent_ids"]]
    _require_equal(
        [str(value) for value in stage_a2_manifest["selected_parent_ids"]],
        selected_ids,
        "recall40 A1/A2 selected-parent order",
    )
    _require_equal(len(selected_ids), 40, "recall40 selected-parent count")
    _require_equal(
        stage_a2_manifest.get("stage_a_provenance"),
        stage_a_provenance,
        "recall40 Stage-A2 exact Stage-A provenance",
    )
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    parents = reconstruct_selected_parents(replay, selected_ids)
    regenerated = build_line_adjudication_batches(
        parents,
        stage_a_decisions,
        stage_a_provenance=stage_a_provenance,
        contract_override=get_frozen_recall40_stage_a2_contract(),
    )
    _require_equal(
        [sha256_json(batch) for batch in stage_a2_batches],
        [sha256_json(batch) for batch in regenerated],
        "recall40 regenerated Stage-A2 inputs",
    )
    stage_a2_decisions = line_adjudication_ledger(
        stage_a2_batches, stage_a2_rows
    )
    _require_equal(len(stage_a_decisions), 1115, "recall40 Stage-A line closure")
    _require_equal(len(stage_a2_decisions), 607, "recall40 Stage-A2 coverage")
    merged = merge_line_adjudication_ledger(
        parents,
        stage_a_decisions,
        stage_a2_decisions,
        upstream_stage_a_contract_sha256=stage_a_manifest[
            "line_ledger_contract_sha256"
        ],
    )
    _require_equal(
        set(merged["final_decisions"]),
        set(stage_a_decisions),
        "recall40 final line closure",
    )
    return {
        "stage_a_manifest": stage_a_manifest,
        "stage_a_batches": stage_a_batches,
        "stage_a_rows": stage_a_rows,
        "stage_a_decisions": stage_a_decisions,
        "stage_a_provenance": stage_a_provenance,
        "stage_a2_manifest": stage_a2_manifest,
        "stage_a2_batches": stage_a2_batches,
        "stage_a2_rows": stage_a2_rows,
        "stage_a2_decisions": stage_a2_decisions,
        "stage_a2_artifacts": stage_a2_artifacts,
        "parents": parents,
        "merge": merged,
    }


def recall40_post_a2_evaluation_report(replay: Mapping[str, Any]) -> dict[str, Any]:
    """Construct the sole canonical recall40 post-A2 gate report."""
    merge = replay["merge"]
    validation = merge["validation"]
    expected_validation = {
        "expected_exception_count": 607,
        "adjudicated_exception_count": 607,
        "unresolved_line_count": 0,
        "withheld_line_count": 0,
        "unowned_continuation_count": 0,
        "duplicate_continuation_owner_count": 0,
        "ownership_conflict_count": 0,
        "coverage_complete": True,
        "validation_passed": True,
    }
    checks = [
        {"check_id": "recall40_exact_parent_closure", "passed": len(replay["parents"]) == 40,
         "observed": len(replay["parents"]), "expected": 40},
        {"check_id": "recall40_exact_stage_a_line_closure", "passed": len(replay["stage_a_decisions"]) == 1115,
         "observed": len(replay["stage_a_decisions"]), "expected": 1115},
        {"check_id": "recall40_exact_stage_a2_coverage", "passed": len(replay["stage_a2_decisions"]) == 607,
         "observed": len(replay["stage_a2_decisions"]), "expected": 607},
        {"check_id": "recall40_zero_conflict_two_pass_merge",
         "passed": sha256_json(validation) == sha256_json(expected_validation),
         "observed": validation, "expected": expected_validation},
        {"check_id": "recall40_final_line_identity_closure",
         "passed": set(merge["final_decisions"]) == set(replay["stage_a_decisions"]),
         "observed": len(merge["final_decisions"]), "expected": 1115},
    ]
    passed = sum(check["passed"] is True for check in checks)
    authorized = passed == len(checks)
    return {
        "evaluation_scope": "private_recall40_stage_a2_to_bounded_stage_b_only",
        "gate_decision": "GO_BUILD_BOUNDED_STAGE_B_INPUTS" if authorized else "NO_GO",
        "passed_checks": passed,
        "failed_checks": len(checks) - passed,
        "checks": checks,
        "stage_a_input_manifest_sha256": replay["stage_a_provenance"]["stage_a_input_manifest_sha256"],
        "stage_a_generation_config_sha256": replay["stage_a_provenance"]["stage_a_generation_config_sha256"],
        "stage_a2_input_manifest_sha256": replay["stage_a2_artifacts"]["input_manifest_sha256"],
        "stage_a2_generation_config_sha256": replay["stage_a2_artifacts"]["generation_config_sha256"],
        "stage_a2_output_batch_ids_sha256": replay["stage_a2_artifacts"]["output_batch_ids_sha256"],
        "final_decisions_sha256": merge["final_decisions_sha256"],
        "per_line_provenance_sha256": merge["per_line_provenance_sha256"],
        "merge_validation": validation,
        "bounded_stage_b_input_build_authorized": authorized,
        "stage_b_hive_submission_authorized": False,
        "full_expansion_or_release_authorized": False,
    }


def build_recall40_record_extraction_batches_after_gate(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    evaluation_report_path: Path,
    *,
    contract_override: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Derive bounded Stage-B batches only after exact recall40 A2 GO.

    This is deliberately an in-memory construction boundary. A separately
    reviewed Stage-B contract/export is still required before files or Hive
    inputs may be produced.
    """
    replay = replay_recall40_stage_a_and_a2(
        full_export_dir,
        stage_a_export_dir,
        stage_a_output_dir,
        stage_a2_export_dir,
        stage_a2_output_dir,
    )
    report = load_json_no_duplicates(evaluation_report_path)
    if not isinstance(report, Mapping):
        raise TypeError("recall40 post-A2 evaluation report is not an object")
    canonical_report = recall40_post_a2_evaluation_report(replay)
    if sha256_json(report) != sha256_json(canonical_report):
        raise ValueError("recall40 canonical A2 evaluation report changed")
    if canonical_report["bounded_stage_b_input_build_authorized"] is not True:
        raise ValueError("recall40 A2 evaluation does not authorize Stage-B inputs")
    merge = replay["merge"]
    batches = build_record_extraction_batches_from_adjudicated_merge(
        replay["parents"], merge, contract_override=contract_override
    )
    authorization = {
        "authorization_scope": "private_recall40_bounded_stage_b_inputs_only",
        "evaluation_report_sha256": sha256_file(evaluation_report_path),
        **canonical_report,
        "stage_b_batch_count": len(batches),
        "stage_b_ordered_batch_sha256": sha256_json(
            [sha256_json(batch) for batch in batches]
        ),
        "stage_b_hive_submission_authorized": False,
        "full_expansion_or_release_authorized": False,
    }
    return batches, authorization


def build_recall40_record_extraction_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    evaluation_report_path: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build an open, bounded recall40 Stage-B artifact after exact A2 GO."""
    ensure_empty_output_dir(output_dir)
    contract = get_frozen_recall40_stage_b_contract()
    batches, authorization = build_recall40_record_extraction_batches_after_gate(
        full_export_dir,
        stage_a_export_dir,
        stage_a_output_dir,
        stage_a2_export_dir,
        stage_a2_output_dir,
        evaluation_report_path,
        contract_override=contract,
    )
    replay = replay_recall40_stage_a_and_a2(
        full_export_dir, stage_a_export_dir, stage_a_output_dir,
        stage_a2_export_dir, stage_a2_output_dir,
    )
    parents = replay["parents"]
    output_dir.mkdir(parents=True, exist_ok=True)
    mode = "record_extraction_recall_pilot40"
    selection = _selection_rows(
        parents, mode=mode, stage_a_decisions=replay["merge"]["final_decisions"]
    )
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunk_entries = write_batch_chunks(output_dir / "chunks", batches, chunk_size)
    source = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    manifest = _base_manifest(
        mode=mode,
        stage=RECORD_EXTRACTION_STAGE,
        replay=source,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunk_entries,
        chunk_size=chunk_size,
        contract_override=contract,
        frozen_contract_sha256_override=(
            FROZEN_RECALL40_STAGE_B_CONTRACT_SHA256
        ),
    )
    manifest.update({
        "stage_b_upstream_pass_count": 2,
        "stage_b_two_pass_provenance_spec_sha256": (
            STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256
        ),
        "record_extraction_batch_schema_rule_sha256": (
            RECORD_EXTRACTION_BATCH_SCHEMA_RULE_SHA256
        ),
        "private_recall40_stage_b": {
            **authorization,
            "builder_performed_inference": False,
            "hive_submission_authorized": False,
            "full_expansion_or_release_authorized": False,
        },
    })
    return finalize_manifest(output_dir, manifest)


def build_recall40_round2_record_extraction_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    stage_a3_task_path: Path,
    stage_a3_html_path: Path,
    stage_a3_results_path: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build bounded round-two Stage B after exact human A3 closure."""
    ensure_empty_output_dir(output_dir)
    expected_hashes = {
        "task": "cabbbf52640151a809e7fc614474025861bf2448bed97cfce3f98e5ac69f0f36",
        "html": "91a2c54db5ee51b8d51f7eb180e813a6b6909e43bbe4194c830c6913b8c60590",
        "results": "4c44826f2b5abdec63ef7b2ec695f0765eed4089501026fffad0b16d150493c1",
    }
    for label, path in (
        ("task", stage_a3_task_path),
        ("html", stage_a3_html_path),
        ("results", stage_a3_results_path),
    ):
        _require_equal(sha256_file(path), expected_hashes[label], f"round-two A3 {label} bytes")
    task = load_json_no_duplicates(stage_a3_task_path)
    results = load_json_no_duplicates(stage_a3_results_path)
    if not isinstance(task, Mapping) or not isinstance(results, Mapping):
        raise TypeError("round-two A3 task/results must be objects")
    items = task.get("items")
    responses = results.get("responses")
    if (
        task.get("task_id") != "candidate_endorsement_recall40_round2_a3_20260901"
        or results.get("task_id") != task.get("task_id")
        or not isinstance(items, list)
        or not isinstance(responses, list)
        or len(items) != 14
        or len(responses) != 14
        or type(results.get("n_items")) is not int
        or type(results.get("n_complete")) is not int
        or results["n_items"] != 14
        or results["n_complete"] != 14
    ):
        raise ValueError("round-two A3 results closure differs")
    item_by_id = {str(item["id"]): item for item in items}
    if len(item_by_id) != 14 or [str(row.get("item_id")) for row in responses] != list(item_by_id):
        raise ValueError("round-two A3 response order differs")

    stage_a_manifest, _ab, _ar, stage_a_decisions, stage_a_provenance = (
        load_recall40_round2_stage_a_run(stage_a_export_dir, stage_a_output_dir)
    )
    _require_equal(sha256_file(stage_a2_export_dir / "manifest.json"),
                   "c63dc64f7089fcf352a6ce00e1e596d9c4a61fc30c4d49269b11610dd079a433",
                   "round-two A2 manifest bytes")
    stage_a2_manifest, stage_a2_batches, stage_a2_rows, stage_a2_artifacts = (
        load_validated_stage_run(
            stage_a2_export_dir, stage_a2_output_dir,
            stage=LINE_ADJUDICATION_STAGE,
            contract_override=get_frozen_recall40_round2_stage_a2_contract(),
        )
    )
    stage_a2_decisions = line_adjudication_ledger(stage_a2_batches, stage_a2_rows)
    source = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    parents = reconstruct_selected_parents(
        source, stage_a_manifest["selected_parent_ids"],
        round2_route_revision=True,
    )
    merge = merge_line_adjudication_ledger(
        parents, stage_a_decisions, stage_a2_decisions,
        upstream_stage_a_contract_sha256=(
            stage_a_manifest["line_ledger_contract_sha256"]
        ),
    )
    final = copy.deepcopy(merge["final_decisions"])
    provenance = copy.deepcopy(merge["per_line_provenance"])
    parent_lines = {
        str(parent["parent_document_id"]): {
            str(line["source_line_id"]): line for line in parent["lines"]
        }
        for parent in parents
    }
    a3_decisions: dict[str, dict[str, Any]] = {}
    for response in responses:
        line_id = str(response["item_id"])
        if response.get("status") != "complete" or line_id not in item_by_id:
            raise ValueError("round-two A3 response is incomplete")
        answers = response.get("answers")
        if not isinstance(answers, Mapping):
            raise TypeError("round-two A3 answer is not an object")
        residual = item_by_id[line_id]["residual"]
        row = final[line_id]
        if answers.get("resolution") == "terminal":
            if set(answers) != {"resolution", "final_role"} or answers.get(
                "final_role"
            ) not in {"navigation_form_footer", "nonrecord_content"}:
                raise ValueError("round-two A3 terminal answer is invalid")
            row["role"] = str(answers["final_role"])
            row["continuation_owner_source_line_id"] = None
        elif answers.get("resolution") == "attach":
            if set(answers) != {"resolution", "anchor_source_line_id"}:
                raise ValueError("round-two A3 attachment answer is invalid")
            owner = str(answers["anchor_source_line_id"])
            if owner not in residual["eligible_final_anchor_source_line_ids"]:
                raise ValueError("round-two A3 attachment is ineligible")
            lines = parent_lines[str(residual["parent_document_id"])]
            source_index = int(lines[line_id]["parent_line_index"])
            owner_index = int(lines[owner]["parent_line_index"])
            row["role"] = (
                "record_continuation_next"
                if source_index < owner_index
                else "record_continuation_previous"
            )
            row["continuation_owner_source_line_id"] = owner
        else:
            raise ValueError("round-two A3 resolution is invalid")
        a3_decisions[line_id] = {
            "item_id": line_id,
            "answer": copy.deepcopy(dict(answers)),
        }
        provenance[line_id]["stage_a3_human_adjudication"] = copy.deepcopy(
            a3_decisions[line_id]
        )
        provenance[line_id]["final_role"] = row["role"]
        provenance[line_id]["continuation_owner_source_line_id"] = row[
            "continuation_owner_source_line_id"
        ]
    _require_equal(sha256_json(a3_decisions),
                   "dec1dccc84d5e19f7fac7f3547d2934f9fb769295f714269938d57296c33957e",
                   "round-two A3 decision ledger")
    reports = [continuation_ownership_report(parent, final) for parent in parents]
    conflict_count = sum(int(report["ownership_conflict_count"]) for report in reports)
    if conflict_count != 0:
        raise ValueError("round-two A3 ownership conflicts remain")
    _require_equal(sha256_json(final),
                   "78a7a01baf31c75274226972249b744840de484b8ececd506063be6381417fda",
                   "round-two A3 final decisions")
    _require_equal(sha256_json(provenance),
                   "856cb7f0e8868d837d6b3bc5c7af582a7a12443a953ad11298b3cae01e9dba0a",
                   "round-two A3 per-line provenance")
    contract = get_frozen_recall40_round2_stage_b_contract()
    batches = build_record_extraction_batches(
        parents, final, per_line_provenance=provenance,
        adjudication_pass_count=3, contract_override=contract,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    mode = "record_extraction_recall40_round2"
    selection_entry = write_selection(
        output_dir / "selection.csv",
        _selection_rows(parents, mode=mode, stage_a_decisions=final),
    )
    chunk_entries = write_batch_chunks(
        output_dir / "chunks", batches, chunk_size
    )
    manifest = _base_manifest(
        mode=mode, stage=RECORD_EXTRACTION_STAGE, replay=source,
        parents=parents, batches=batches, selection_entry=selection_entry,
        chunks=chunk_entries, chunk_size=chunk_size,
        contract_override=contract,
        frozen_contract_sha256_override=(
            FROZEN_RECALL40_ROUND2_STAGE_B_CONTRACT_SHA256
        ),
    )
    manifest.update({
        "stage_b_upstream_pass_count": 3,
        "stage_b_three_pass_provenance_spec_sha256": contract[
            "stage_b_three_pass_provenance_spec_sha256"
        ],
        "record_extraction_batch_schema_rule_sha256": contract[
            "record_extraction_batch_schema_rule_sha256"
        ],
        "private_recall40_round2_stage_b": {
            "stage_a_input_manifest_sha256": stage_a_provenance[
                "stage_a_input_manifest_sha256"
            ],
            "stage_a_generation_config_sha256": stage_a_provenance[
                "stage_a_generation_config_sha256"
            ],
            "stage_a2_input_manifest_sha256": stage_a2_artifacts[
                "input_manifest_sha256"
            ],
            "stage_a2_generation_config_sha256": stage_a2_artifacts[
                "generation_config_sha256"
            ],
            "stage_a2_output_batch_ids_sha256": stage_a2_artifacts[
                "output_batch_ids_sha256"
            ],
            "stage_a3_task_sha256": expected_hashes["task"],
            "stage_a3_html_sha256": expected_hashes["html"],
            "stage_a3_results_sha256": expected_hashes["results"],
            "stage_a3_decisions_sha256": sha256_json(a3_decisions),
            "final_decisions_sha256": sha256_json(final),
            "per_line_provenance_sha256": sha256_json(provenance),
            "selected_parent_count": 40,
            "stage_a_line_count": len(stage_a_decisions),
            "stage_a2_exception_count": len(stage_a2_decisions),
            "stage_a3_residual_count": len(a3_decisions),
            "final_anchor_count": sum(
                row["role"] in {"record_anchor", "compound_record_anchor"}
                for row in final.values()
            ),
            "builder_performed_inference": False,
            "hive_submission_authorized": False,
            "full_expansion_or_release_authorized": False,
        },
    })
    return finalize_manifest(output_dir, manifest)


def _validate_stage_a2_exception_census_generic(
    exceptions: Sequence[Mapping[str, Any]],
    stage_a_decisions: Mapping[str, Mapping[str, Any]],
) -> dict[str, int]:
    """Validate role/reason closure without an eight-parent count constant."""
    source_ids = [str(row.get("source_line_id")) for row in exceptions]
    _require_equal(len(source_ids), len(set(source_ids)),
                   "Stage-A2 unique exception lines")
    expected_roles = {
        "stage_a_unclear": {"unclear"},
        "stage_a_candidate_self_review": {"candidate_self"},
        "stage_a_unowned_continuation": {
            "record_continuation_previous", "record_continuation_next",
        },
    }
    counts = Counter()
    for row in exceptions:
        reason = str(row.get("exception_reason"))
        if reason not in expected_roles:
            raise ValueError("unknown Stage-A2 exception reason")
        line_id = str(row["source_line_id"])
        if stage_a_decisions.get(line_id, {}).get("role") not in expected_roles[reason]:
            raise ValueError(f"Stage-A2 exception reason/role mismatch: {line_id}")
        counts[reason] += 1
    return dict(sorted(counts.items()))


def _base_manifest(
    *,
    mode: str,
    stage: str,
    replay: Mapping[str, Any],
    parents: Sequence[Mapping[str, Any]],
    batches: Sequence[Mapping[str, Any]],
    selection_entry: Mapping[str, Any],
    chunks: Sequence[Mapping[str, Any]],
    chunk_size: int,
    contract_override: Mapping[str, Any] | None = None,
    frozen_contract_sha256_override: str | None = None,
) -> dict[str, Any]:
    contract = (
        dict(contract_override)
        if contract_override is not None else get_line_ledger_contract()
    )
    parent_ids = [str(parent["parent_document_id"]) for parent in parents]
    batch_ids = [str(batch["batch_id"]) for batch in batches]
    variant_inventory = schema_variant_inventory(list(batches))
    census = inventory_census(parents)
    maximums = expected_batch_counts(parents)
    return {
        "builder": "scripts/build_candidate_endorsement_line_ledger.py",
        "builder_version": "candidate_endorsement_line_ledger_export_v1",
        "mode": mode,
        "stage": stage,
        "route": "endorsements",
        "source_export": {
            "schema_version": str(replay["manifest"].get("schema_version")),
            "source_corpus_sha256": replay["manifest"].get("source_corpus_sha256"),
            "source_export_manifest_sha256": replay["manifest_sha256"],
            "source_document_ids_sha256": replay["source_document_ids_sha256"],
            "replayed_segments_by_route": replay["route_segments"],
            "replayed_parents_by_route": replay["route_parents"],
            "corrected_issue_control_source_segments": replay[
                "issue_control_source_segments"
            ],
        },
        "line_ledger_contract": contract,
        "line_ledger_contract_sha256": contract["line_ledger_contract_sha256"],
        "frozen_line_ledger_contract_sha256": (
            frozen_contract_sha256_override
            if frozen_contract_sha256_override is not None
            else None if contract_override is not None
            else (FROZEN_LINE_LEDGER_CONTRACT_SHA256 or None)
        ),
        "contract_is_frozen": (
            True if frozen_contract_sha256_override is not None
            else False if contract_override is not None
            else bool(FROZEN_LINE_LEDGER_CONTRACT_SHA256)
        ),
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
        "json_schema_catalog_sha256": contract[
            "json_schema_catalog_sha256"
        ][stage],
        "json_schema_variant_sha256": contract[
            "json_schema_variant_sha256"
        ][stage],
        **variant_inventory,
        "unchanged_route_hashes": {
            "issue_prompt_sha256": BASE_ISSUE_PROMPT_SHA256,
            "issue_schema_sha256": BASE_ISSUE_SCHEMA_SHA256,
            "biography_prompt_sha256": BASE_BIOGRAPHY_PROMPT_SHA256,
            "biography_schema_sha256": BASE_BIOGRAPHY_SCHEMA_SHA256,
        },
        "corrected_71c_issue_audit_gate": CORRECTED_ISSUE_AUDIT_GATE,
        "selected_parent_ids": parent_ids,
        "selected_parent_ids_sha256": sha256_json(parent_ids),
        "complete_parent_closure": True,
        "inventory_census": census,
        "expected_batch_counts": maximums,
        "expected_batch_ids": batch_ids,
        "expected_batch_ids_sha256": sha256_json(batch_ids),
        "expected_stage_batch_count": len(batches),
        "exported_records": len(batches),
        "chunk_size": chunk_size,
        "files": {
            "selection": dict(selection_entry),
            "chunks": list(chunks),
        },
    }


def build_line_decision_export(
    full_export_dir: Path,
    output_dir: Path,
    *,
    preflight: bool,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    enforce_frozen_source: bool = True,
) -> dict[str, Any]:
    ensure_empty_output_dir(output_dir)
    if output_dir.resolve() == full_export_dir.resolve():
        raise ValueError("output directory cannot be the frozen source export")
    replay = replay_frozen_source(
        full_export_dir, enforce_frozen_source=enforce_frozen_source
    )
    selected_ids = (
        list(PILOT_PARENT_IDS)
        if preflight
        else sorted(str(parent_id) for parent_id in replay["endorsement_groups"])
    )
    parents = reconstruct_selected_parents(replay, selected_ids)
    census = inventory_census(parents)
    counts = expected_batch_counts(parents)
    if enforce_frozen_source:
        _require_equal(
            census,
            FROZEN_PILOT_INVENTORY if preflight else FROZEN_FULL_ENDORSEMENT_INVENTORY,
            "selected line inventory census",
        )
        _require_equal(
            counts,
            FROZEN_PILOT_BATCH_COUNTS if preflight else FROZEN_FULL_BATCH_COUNTS,
            "selected batch census",
        )
    batches = build_line_decision_batches(parents)
    _require_equal(len(batches), counts["line_decision_batches"], "Stage-A batch count")

    output_dir.mkdir(parents=True, exist_ok=True)
    selection = _selection_rows(
        parents,
        mode="line_decision_preflight" if preflight else "line_decision_full",
    )
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunk_entries = write_batch_chunks(
        output_dir / "chunks", batches, chunk_size
    )
    mode = "line_decision_preflight" if preflight else "line_decision_full"
    manifest = _base_manifest(
        mode=mode,
        stage=LINE_DECISION_STAGE,
        replay=replay,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunk_entries,
        chunk_size=chunk_size,
    )
    manifest["bounded_preflight"] = {
        "enabled": preflight,
        "pilot_controls": [
            {"control_kind": kind, "parent_document_id": parent_id}
            for kind, parent_id in PILOT_CONTROLS
        ] if preflight else [],
        "predeclared_content_gates": PILOT_CONTENT_GATES if preflight else {},
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }
    return finalize_manifest(output_dir, manifest)


def build_recall40_line_decision_export(
    full_export_dir: Path, private_selection_path: Path, output_dir: Path,
    *, chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build only the sealed pilot40 Stage-A inputs; never load reserve80 IDs."""
    ensure_empty_output_dir(output_dir)
    _require_equal(
        sha256_file(private_selection_path), FROZEN_PRIVATE_RECALL40_SELECTION_SHA256,
        "private recall40 selection bytes",
    )
    private = load_json_no_duplicates(private_selection_path)
    _require_equal(set(private), {
        "version", "source_private_selection_sha256",
        "canonical_assignment_sha256", "pilot_components", "pilot_parent_count",
    }, "private pilot40 selection field set")
    _require_equal(private.get("source_private_selection_sha256"),
                   FROZEN_PRIVATE_RESERVE80_PILOT40_SELECTION_SHA256,
                   "private pilot40 source selection")
    _require_equal(private.get("canonical_assignment_sha256"),
                   FROZEN_PRIVATE_RECALL40_ASSIGNMENT_SHA256,
                   "private recall40 assignment")
    pilot_components = private.get("pilot_components")
    if not isinstance(pilot_components, list):
        raise ValueError("private recall40 pilot components missing")
    selected_ids = [
        str(parent_id) for component in pilot_components
        for parent_id in component["parent_document_ids"]
    ]
    _require_equal(len(selected_ids), 40, "private recall40 parent count")
    _require_equal(len(set(selected_ids)), 40, "private recall40 unique parents")
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    parents = reconstruct_selected_parents(replay, selected_ids)
    census = inventory_census(parents)
    counts = expected_batch_counts(parents)
    batches = build_line_decision_batches(parents)
    _require_equal(len(batches), counts["line_decision_batches"], "recall40 Stage-A batches")
    output_dir.mkdir(parents=True, exist_ok=True)
    selection = _selection_rows(parents, mode="line_decision_recall_pilot40")
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunks = write_batch_chunks(output_dir / "chunks", batches, chunk_size)
    manifest = _base_manifest(
        mode="line_decision_recall_pilot40", stage=LINE_DECISION_STAGE,
        replay=replay, parents=parents, batches=batches,
        selection_entry=selection_entry, chunks=chunks, chunk_size=chunk_size,
    )
    manifest["private_recall40"] = {
        "enabled": True,
        "private_selection_file_sha256": FROZEN_PRIVATE_RECALL40_SELECTION_SHA256,
        "canonical_assignment_sha256": FROZEN_PRIVATE_RECALL40_ASSIGNMENT_SHA256,
        "selected_parent_count": 40,
        "selected_parent_ids_sha256": sha256_json(selected_ids),
        "inventory_census": census,
        "batch_census": counts,
        "source_preview_or_human_task_rendered": False,
        "hive_submission_authorized": False,
        "reserve80_loaded": False,
        "downstream_derivation_shell": {
            "stage_a2": "derive only after authenticated complete Stage-A outputs and separate review",
            "stage_b": "derive only after authenticated Stage-A/A2 residual handling and separate review",
            "full_expansion_or_release_authorized": False,
        },
    }
    return finalize_manifest(output_dir, manifest)


def build_recall40_round2_line_decision_export(
    full_export_dir: Path, private_selection_path: Path, output_dir: Path,
    *, chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build review-only round-two Stage-A inputs under the revised route."""
    ensure_empty_output_dir(output_dir)
    expected_selection_sha = (
        "cda7d58561c439aee65dbb4be391c6c9edcbc86a7209625512bf12c619cebdac"
    )
    _require_equal(
        sha256_file(private_selection_path), expected_selection_sha,
        "private recall40 round-two selection bytes",
    )
    private = load_json_no_duplicates(private_selection_path)
    _require_equal(private.get("version"),
                   "candidate_endorsement_private_recall40_round2_selection_v1",
                   "private recall40 round-two selection version")
    _require_equal(private.get("seed"), 20261827, "round-two seed")
    _require_equal(private.get("pilot_parent_count"), 40, "round-two parent count")
    selected_ids = [
        str(parent_id) for component in private["pilot_components"]
        for parent_id in component["parent_document_ids"]
    ]
    _require_equal(len(selected_ids), 40, "round-two selected parent count")
    _require_equal(len(set(selected_ids)), 40, "round-two unique parent count")
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    parents = reconstruct_selected_parents(
        replay, selected_ids, round2_route_revision=True
    )
    contract = get_frozen_recall40_round2_stage_a_contract()
    batches = build_line_decision_batches(parents, contract_override=contract)
    output_dir.mkdir(parents=True, exist_ok=True)
    selection = _selection_rows(parents, mode="line_decision_recall40_round2")
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunks = write_batch_chunks(output_dir / "chunks", batches, chunk_size)
    manifest = _base_manifest(
        mode="line_decision_recall40_round2", stage=LINE_DECISION_STAGE,
        replay=replay, parents=parents, batches=batches,
        selection_entry=selection_entry, chunks=chunks, chunk_size=chunk_size,
        contract_override=contract,
        frozen_contract_sha256_override=contract["line_ledger_contract_sha256"],
    )
    manifest["private_recall40_round2"] = {
        "enabled": True,
        "private_selection_file_sha256": expected_selection_sha,
        "round": 2, "seed": 20261827,
        "selected_parent_count": 40,
        "selected_parent_ids_sha256": sha256_json(selected_ids),
        "route_revision_spec_sha256": private["route_revision_spec_sha256"],
        "source_preview_or_human_task_rendered": False,
        "hive_submission_authorized": False,
        "stage_a2_stage_b_full_release_authorized": False,
    }
    return finalize_manifest(output_dir, manifest)


def build_recall40_round3_line_decision_export(
    full_export_dir: Path,
    private_selection_path: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build the same private 40-parent sample under the round-three contract."""
    ensure_empty_output_dir(output_dir)
    expected_selection_sha = (
        "cda7d58561c439aee65dbb4be391c6c9edcbc86a7209625512bf12c619cebdac"
    )
    _require_equal(
        sha256_file(private_selection_path), expected_selection_sha,
        "private recall40 round-three selection bytes",
    )
    private = load_json_no_duplicates(private_selection_path)
    _require_equal(
        private.get("version"),
        "candidate_endorsement_private_recall40_round2_selection_v1",
        "private recall40 round-three source selection version",
    )
    _require_equal(private.get("round"), 2, "round-three reused sample round")
    _require_equal(private.get("seed"), 20261827, "round-three reused sample seed")
    _require_equal(
        private.get("pilot_parent_count"), 40,
        "round-three selected parent count",
    )
    selected_ids = [
        str(parent_id)
        for component in private["pilot_components"]
        for parent_id in component["parent_document_ids"]
    ]
    _require_equal(len(selected_ids), 40, "round-three selected parent count")
    _require_equal(
        len(set(selected_ids)), 40, "round-three unique selected parent count"
    )
    _require_equal(
        sha256_json(selected_ids),
        "25962ac169c3f88bccbf84ef4767e62dd15f82267017ac126efa50e3b43ec191",
        "round-three selected parent identity",
    )
    replay = replay_frozen_source(full_export_dir, enforce_frozen_source=True)
    parents = reconstruct_selected_parents(
        replay, selected_ids, round2_route_revision=True,
    )
    contract = (
        get_frozen_recall40_round3_stage_a_contract()
        if FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        else computed_recall40_round3_stage_a_contract()
    )
    batches = build_line_decision_batches(parents, contract_override=contract)
    output_dir.mkdir(parents=True, exist_ok=True)
    mode = "line_decision_recall40_round3"
    selection = _selection_rows(parents, mode=mode)
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunks = write_batch_chunks(output_dir / "chunks", batches, chunk_size)
    manifest = _base_manifest(
        mode=mode,
        stage=LINE_DECISION_STAGE,
        replay=replay,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunks,
        chunk_size=chunk_size,
        contract_override=contract,
        frozen_contract_sha256_override=(
            contract["line_ledger_contract_sha256"]
            if FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256 else None
        ),
    )
    manifest["private_recall40_round3"] = {
        "enabled": True,
        "round": 3,
        "source_round": 2,
        "private_selection_file_sha256": expected_selection_sha,
        "selected_parent_count": 40,
        "selected_parent_ids_sha256": sha256_json(selected_ids),
        "stage_a_semantic_revision_spec_sha256": (
            RECALL40_ROUND3_STAGE_A_SEMANTIC_REVISION_SPEC_SHA256
        ),
        "stage_a_context_profile_sha256": (
            RECALL40_ROUND3_STAGE_A_CONTEXT_PROFILE_SHA256
        ),
        "stage_a_line_batch_spec_sha256": (
            RECALL40_ROUND3_STAGE_A_LINE_BATCH_SPEC_SHA256
        ),
        "stage_a_scope_spec_sha256": (
            RECALL40_ROUND3_STAGE_A_SCOPE_SPEC_SHA256
        ),
        "source_preview_or_human_task_rendered": False,
        "hive_submission_authorized": False,
        "stage_a2_stage_b_full_release_authorized": False,
    }
    _require_equal(
        manifest["inventory_census"]["parent_documents"],
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["parent_count"],
        "round-three parent census",
    )
    _require_equal(
        manifest["inventory_census"]["source_segments"],
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["source_segment_count"],
        "round-three source-segment census",
    )
    _require_equal(
        manifest["inventory_census"]["physical_lines"],
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["physical_line_count"],
        "round-three physical-line census",
    )
    _require_equal(
        manifest["inventory_census"]["model_decision_lines"],
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["model_decision_line_count"],
        "round-three model-line census",
    )
    _require_equal(
        len(batches), RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["stage_a_batch_count"],
        "round-three Stage-A batch census",
    )
    _require_equal(
        sha256_json(manifest["inventory_census"]),
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["inventory_census_sha256"],
        "round-three inventory census identity",
    )
    _require_equal(
        sha256_json(manifest["expected_batch_counts"]),
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["expected_batch_counts_sha256"],
        "round-three batch census identity",
    )
    _require_equal(
        manifest["expected_batch_counts"][
            "record_extraction_batches_maximum"
        ],
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
            "record_extraction_batch_count_maximum"
        ],
        "round-three Stage-B maximum batch census",
    )
    _require_equal(
        selection_entry["sha256"],
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["selection_csv_sha256"],
        "round-three selection CSV identity",
    )
    _require_equal(
        selection_entry["bytes"],
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["selection_csv_bytes"],
        "round-three selection CSV byte count",
    )
    target_ids = [
        str(target["source_line_id"])
        for batch in batches for target in batch["targets"]
    ]
    _require_equal(
        len(target_ids), len(set(target_ids)),
        "round-three unique Stage-A target count",
    )
    _require_equal(
        sha256_json(target_ids),
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
            "ordered_target_source_line_ids_sha256"
        ],
        "round-three ordered Stage-A target identity",
    )
    parent_inventory_ledger = [
        {
            "parent_document_id": str(parent["parent_document_id"]),
            "parent_line_inventory_sha256": str(
                parent["parent_line_inventory_sha256"]
            ),
        }
        for parent in sorted(
            parents, key=lambda row: str(row["parent_document_id"])
        )
    ]
    _require_equal(
        sha256_json(parent_inventory_ledger),
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
            "parent_line_inventory_ledger_sha256"
        ],
        "round-three parent inventory ledger",
    )
    parent_catalog_ledger = []
    seen_parent_ids: set[str] = set()
    for batch in batches:
        parent_id = str(batch["parent_document_id"])
        if parent_id in seen_parent_ids:
            continue
        seen_parent_ids.add(parent_id)
        parent_catalog_ledger.append({
            "parent_document_id": parent_id,
            "full_parent_physical_lines_sha256": str(
                batch["full_parent_physical_lines_sha256"]
            ),
            "full_parent_source_line_ids_sha256": str(
                batch["full_parent_source_line_ids_sha256"]
            ),
        })
    _require_equal(
        sha256_json(parent_catalog_ledger),
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
            "parent_catalog_ledger_sha256"
        ],
        "round-three parent catalog ledger",
    )
    parent_prompt_metadata_keys = (
        "candidate_cycle_id", "candidate_icpsr", "state", "office", "year",
        "party", "raw_text_sha256", "parent_document_id",
        "extraction_source_text_sha256", "extraction_source_n_chars",
        "source_urls", "parent_line_inventory_sha256",
        "endorsement_line_context",
    )
    parent_prompt_metadata_ledger = []
    seen_parent_ids.clear()
    for batch in batches:
        parent_id = str(batch["parent_document_id"])
        if parent_id in seen_parent_ids:
            continue
        seen_parent_ids.add(parent_id)
        parent_prompt_metadata_ledger.append({
            key: copy.deepcopy(batch[key])
            for key in parent_prompt_metadata_keys
        })
    _require_equal(
        sha256_json(parent_prompt_metadata_ledger),
        RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
            "parent_prompt_metadata_ledger_sha256"
        ],
        "round-three parent prompt-metadata ledger",
    )
    return finalize_manifest(output_dir, manifest)


def _validate_stage_a2_exception_census(
    exceptions: Sequence[Mapping[str, Any]],
    stage_a_decisions: Mapping[str, Mapping[str, Any]],
) -> dict[str, int]:
    reason_counts = Counter(str(row.get("exception_reason")) for row in exceptions)
    expected_by_reason = {
        "stage_a_unclear": FROZEN_STAGE_A2_EXCEPTION_COUNTS["unclear"],
        "stage_a_candidate_self_review": FROZEN_STAGE_A2_EXCEPTION_COUNTS[
            "candidate_self"
        ],
        "stage_a_unowned_continuation": FROZEN_STAGE_A2_EXCEPTION_COUNTS[
            "unowned_continuation"
        ],
    }
    _require_equal(
        dict(sorted(reason_counts.items())),
        dict(sorted(expected_by_reason.items())),
        "frozen Stage-A2 exception-reason census",
    )
    _require_equal(
        len(exceptions), FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "frozen Stage-A2 exception count",
    )
    source_ids = [str(row.get("source_line_id")) for row in exceptions]
    _require_equal(
        len(source_ids), len(set(source_ids)),
        "frozen Stage-A2 unique exception lines",
    )
    expected_stage_a_roles = {
        "stage_a_unclear": {"unclear"},
        "stage_a_candidate_self_review": {"candidate_self"},
        "stage_a_unowned_continuation": {
            "record_continuation_previous", "record_continuation_next",
        },
    }
    for row in exceptions:
        line_id = str(row["source_line_id"])
        reason = str(row["exception_reason"])
        role = stage_a_decisions.get(line_id, {}).get("role")
        if role not in expected_stage_a_roles[reason]:
            raise ValueError(
                f"Stage-A2 exception reason/role mismatch: {line_id}"
            )
    return dict(sorted(reason_counts.items()))


def build_line_adjudication_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    enforce_frozen_source: bool = True,
) -> dict[str, Any]:
    """Build the bounded Stage-A2 pilot from the exact suffix-b first pass."""
    ensure_empty_output_dir(output_dir)
    resolved_inputs = {
        full_export_dir.resolve(),
        stage_a_export_dir.resolve(),
        stage_a_output_dir.resolve(),
    }
    if output_dir.resolve() in resolved_inputs:
        raise ValueError("Stage-A2 output directory cannot overwrite an input")
    (
        stage_a_manifest,
        _stage_a_batches,
        _stage_a_rows,
        stage_a_decisions,
        stage_a_provenance,
    ) = load_frozen_stage_a_suffix_b(stage_a_export_dir, stage_a_output_dir)
    replay = replay_frozen_source(
        full_export_dir, enforce_frozen_source=enforce_frozen_source
    )
    _require_equal(
        stage_a_manifest.get("source_export", {}).get(
            "source_export_manifest_sha256"
        ),
        replay["manifest_sha256"],
        "suffix-b Stage-A source-export binding",
    )
    selected_ids = [
        str(value) for value in stage_a_manifest["selected_parent_ids"]
    ]
    _require_equal(
        selected_ids, list(PILOT_PARENT_IDS),
        "Stage-A2 bounded pilot parent selection",
    )
    parents = reconstruct_selected_parents(replay, selected_ids)
    if enforce_frozen_source:
        _require_equal(
            inventory_census(parents), FROZEN_PILOT_INVENTORY,
            "Stage-A2 pilot source inventory",
        )
    stage_a_identity_ledger = stage_a_decision_identity_ledger(
        parents, stage_a_decisions
    )
    _require_equal(
        len(stage_a_identity_ledger),
        FROZEN_STAGE_A1_DECISION_IDENTITY_COUNT,
        "Stage-A2 frozen Stage-A decision identity count",
    )
    _require_equal(
        sha256_json([
            row["source_line_id"] for row in stage_a_identity_ledger
        ]),
        FROZEN_STAGE_A1_ORDERED_DECISION_SOURCE_LINE_IDS_SHA256,
        "Stage-A2 frozen ordered Stage-A decision source lines",
    )
    _require_equal(
        sha256_json(stage_a_identity_ledger),
        FROZEN_STAGE_A1_DECISION_IDENTITY_LEDGER_SHA256,
        "Stage-A2 frozen Stage-A decision identity ledger",
    )
    exceptions = line_adjudication_exception_ledger(
        parents, stage_a_decisions
    )
    reason_counts = _validate_stage_a2_exception_census(
        exceptions, stage_a_decisions
    )
    batches = build_line_adjudication_batches(
        parents,
        stage_a_decisions,
        stage_a_provenance=stage_a_provenance,
    )
    _require_equal(
        sum(len(batch["targets"]) for batch in batches),
        FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "Stage-A2 target closure",
    )
    _require_equal(len(batches), 29, "Stage-A2 bounded batch count")
    batched_exceptions = [
        {
            key: value for key, value in target.items()
            if key not in {"slot", "stage_a_provenance_locator"}
        }
        for batch in batches for target in batch["targets"]
    ]
    _require_equal(
        [row["source_line_id"] for row in batched_exceptions],
        [row["source_line_id"] for row in exceptions],
        "Stage-A2 compact exception order",
    )
    exception_identity_ledger = line_adjudication_exception_identity_ledger(
        batches
    )
    _require_equal(
        sha256_json([
            row["source_line_id"] for row in exception_identity_ledger
        ]),
        FROZEN_STAGE_A2_ORDERED_SOURCE_LINE_IDS_SHA256,
        "Stage-A2 frozen ordered exception source-line identities",
    )
    _require_equal(
        sha256_json(exception_identity_ledger),
        FROZEN_STAGE_A2_EXCEPTION_IDENTITY_LEDGER_SHA256,
        "Stage-A2 frozen historical exception identity ledger",
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    mode = "line_adjudication_preflight_from_frozen_stage_a_suffix_b"
    selection = _selection_rows(
        parents, mode=mode, stage_a_decisions=stage_a_decisions
    )
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunk_entries = write_batch_chunks(
        output_dir / "chunks", batches, chunk_size
    )
    manifest = _base_manifest(
        mode=mode,
        stage=LINE_ADJUDICATION_STAGE,
        replay=replay,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunk_entries,
        chunk_size=chunk_size,
    )
    manifest.update({
        "line_adjudication_batch_schema_rule_sha256": (
            LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256
        ),
        "line_adjudication_spec_sha256": LINE_ADJUDICATION_SPEC_SHA256,
        "upstream_stage_a_contract_sha256": stage_a_provenance[
            "stage_a_line_ledger_contract_sha256"
        ],
        "stage_a_provenance": stage_a_provenance,
        "stage_a_decision_identity_ledger": stage_a_identity_ledger,
        "stage_a_decision_identity_ledger_sha256": sha256_json(
            stage_a_identity_ledger
        ),
        "stage_a_exception_ledger_sha256": sha256_json(batched_exceptions),
        "stage_a_exception_source_line_ids_sha256": sha256_json([
            str(row["source_line_id"]) for row in batched_exceptions
        ]),
        "stage_a_exception_count": len(batched_exceptions),
        "stage_a_exception_reason_counts": reason_counts,
        "stage_a_exception_identity_ledger": exception_identity_ledger,
        "stage_a_exception_identity_ledger_sha256": sha256_json(
            exception_identity_ledger
        ),
        "expected_stage_a2_batch_count": len(batches),
        "exception_ledger": {
            "count": len(batched_exceptions),
            "ordered_exception_ids_sha256": sha256_json([
                str(row["exception_id"]) for row in batched_exceptions
            ]),
            "ordered_source_line_ids_sha256": sha256_json([
                str(row["source_line_id"]) for row in batched_exceptions
            ]),
            "reason_counts": reason_counts,
        },
    })
    manifest["bounded_preflight"] = {
        "enabled": True,
        "pilot_controls": [
            {"control_kind": kind, "parent_document_id": parent_id}
            for kind, parent_id in PILOT_CONTROLS
        ],
        "predeclared_content_gates": PILOT_CONTENT_GATES,
        "exact_stage_a_suffix_b_only": True,
        "expected_exception_count": FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "expected_exception_counts": dict(FROZEN_STAGE_A2_EXCEPTION_COUNTS),
        "candidate_control_final_roles": dict(
            STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES
        ),
        "unowned_attachment_controls": dict(
            STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS
        ),
        "unowned_final_role_controls": dict(
            STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS
        ),
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }
    return finalize_manifest(output_dir, manifest)


def two_pass_stage_b_manifest_provenance(
    stage_a_provenance: Mapping[str, Any],
    stage_a2_artifacts: Mapping[str, Any],
    stage_a2_decisions: Mapping[str, Mapping[str, Any]],
    merged: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the exact authenticated A1/A2/merge objects bound to Stage B."""
    return {
        "upstream_stage_a": {
            "contract_sha256": stage_a_provenance[
                "stage_a_line_ledger_contract_sha256"
            ],
            "input_manifest_sha256": stage_a_provenance[
                "stage_a_input_manifest_sha256"
            ],
            "generation_config_sha256": stage_a_provenance[
                "stage_a_generation_config_sha256"
            ],
            "ordered_output_batch_ids_sha256": stage_a_provenance[
                "stage_a_ordered_output_batch_ids_sha256"
            ],
            "decision_ledger_sha256": stage_a_provenance[
                "source_line_decision_ledger_sha256"
            ],
            "provenance_sha256": stage_a_provenance[
                "stage_a_provenance_sha256"
            ],
        },
        "upstream_stage_a2": {
            **dict(stage_a2_artifacts),
            "decision_ledger_sha256": sha256_json(stage_a2_decisions),
        },
        "two_pass_merge": {
            "validation": dict(merged["validation"]),
            "final_decisions_sha256": merged["final_decisions_sha256"],
            "per_line_provenance_sha256": merged[
                "per_line_provenance_sha256"
            ],
            "ownership_by_continuation_sha256": sha256_json(
                merged["ownership_by_continuation"]
            ),
        },
    }


def validate_two_pass_stage_b_manifest_provenance(
    manifest: Mapping[str, Any],
    stage_a_provenance: Mapping[str, Any],
    stage_a2_artifacts: Mapping[str, Any],
    stage_a2_decisions: Mapping[str, Mapping[str, Any]],
    merged: Mapping[str, Any],
) -> dict[str, Any]:
    """Replay Stage-B upstream objects against the actual authenticated runs."""
    expected = two_pass_stage_b_manifest_provenance(
        stage_a_provenance,
        stage_a2_artifacts,
        stage_a2_decisions,
        merged,
    )
    for field, value in expected.items():
        _require_equal(
            manifest.get(field), value, f"Stage-B {field} replay binding"
        )
    return expected


def build_adjudicated_record_extraction_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    enforce_frozen_source: bool = True,
) -> dict[str, Any]:
    """Build Stage B only after exact two-pass replay and a passing merge."""
    ensure_empty_output_dir(output_dir)
    resolved_inputs = {
        full_export_dir.resolve(),
        stage_a_export_dir.resolve(),
        stage_a_output_dir.resolve(),
        stage_a2_export_dir.resolve(),
        stage_a2_output_dir.resolve(),
    }
    if output_dir.resolve() in resolved_inputs:
        raise ValueError("Stage-B output directory cannot overwrite an input")

    (
        stage_a_manifest,
        _stage_a_batches,
        _stage_a_rows,
        stage_a_decisions,
        stage_a_provenance,
    ) = load_frozen_stage_a_suffix_b(stage_a_export_dir, stage_a_output_dir)
    (
        stage_a2_manifest,
        stage_a2_batches,
        stage_a2_rows,
        stage_a2_artifacts,
    ) = load_validated_stage_run(
        stage_a2_export_dir,
        stage_a2_output_dir,
        stage=LINE_ADJUDICATION_STAGE,
    )
    _require_equal(
        stage_a2_manifest.get("stage_a_provenance"),
        stage_a_provenance,
        "Stage-A2 exact frozen Stage-A provenance",
    )
    _require_equal(
        stage_a2_manifest.get("stage_a_exception_count"),
        FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "Stage-A2 bounded exception count",
    )
    selected_ids = [
        str(value) for value in stage_a_manifest["selected_parent_ids"]
    ]
    _require_equal(
        selected_ids,
        [str(value) for value in stage_a2_manifest["selected_parent_ids"]],
        "Stage-A and Stage-A2 selected parents",
    )
    _require_equal(
        selected_ids,
        list(PILOT_PARENT_IDS),
        "adjudicated Stage-B bounded pilot parents",
    )
    _require_equal(
        stage_a_manifest.get("complete_parent_closure"), True,
        "Stage-A complete-parent closure",
    )
    _require_equal(
        stage_a2_manifest.get("complete_parent_closure"), True,
        "Stage-A2 complete-parent closure",
    )

    replay = replay_frozen_source(
        full_export_dir, enforce_frozen_source=enforce_frozen_source
    )
    source_manifest_hash = replay["manifest_sha256"]
    _require_equal(
        stage_a_manifest.get("source_export", {}).get(
            "source_export_manifest_sha256"
        ),
        source_manifest_hash,
        "Stage-A frozen source export",
    )
    _require_equal(
        stage_a2_manifest.get("source_export", {}).get(
            "source_export_manifest_sha256"
        ),
        source_manifest_hash,
        "Stage-A2 frozen source export",
    )
    parents = reconstruct_selected_parents(replay, selected_ids)
    regenerated_stage_a2 = build_line_adjudication_batches(
        parents,
        stage_a_decisions,
        stage_a_provenance=stage_a_provenance,
    )
    _require_equal(
        [sha256_json(batch) for batch in stage_a2_batches],
        [sha256_json(batch) for batch in regenerated_stage_a2],
        "regenerated Stage-A2 inputs",
    )
    stage_a2_decisions = line_adjudication_ledger(
        stage_a2_batches, stage_a2_rows
    )
    merged = merge_line_adjudication_ledger(
        parents, stage_a_decisions, stage_a2_decisions
    )
    expected_merge_gate = {
        "expected_exception_count": FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "adjudicated_exception_count": FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "unresolved_line_count": 0,
        "withheld_line_count": 0,
        "unowned_continuation_count": 0,
        "duplicate_continuation_owner_count": 0,
        "ownership_conflict_count": 0,
        "coverage_complete": True,
        "validation_passed": True,
    }
    _require_equal(
        merged.get("validation"),
        expected_merge_gate,
        "Stage-A2 final merge gate",
    )
    batches = build_record_extraction_batches_from_adjudicated_merge(
        parents, merged
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    mode = "record_extraction_preflight_from_stage_a2"
    selection = _selection_rows(
        parents,
        mode=mode,
        stage_a_decisions=merged["final_decisions"],
    )
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunk_entries = write_batch_chunks(
        output_dir / "chunks", batches, chunk_size
    )
    manifest = _base_manifest(
        mode=mode,
        stage=RECORD_EXTRACTION_STAGE,
        replay=replay,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunk_entries,
        chunk_size=chunk_size,
    )
    manifest.update({
        "stage_b_two_pass_provenance_spec_sha256": (
            STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256
        ),
        **two_pass_stage_b_manifest_provenance(
            stage_a_provenance,
            stage_a2_artifacts,
            stage_a2_decisions,
            merged,
        ),
    })
    manifest["bounded_preflight"] = {
        "enabled": True,
        "pilot_controls": [
            {"control_kind": kind, "parent_document_id": parent_id}
            for kind, parent_id in PILOT_CONTROLS
        ],
        "predeclared_content_gates": PILOT_CONTENT_GATES,
        "exact_stage_a_suffix_b_and_stage_a2_only": True,
        "stage_a2_merge_validation_passed": True,
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }
    return finalize_manifest(output_dir, manifest)


def build_human_a3_record_extraction_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    stage_a2_evaluation_report: Path,
    stage_a3_task_json: Path,
    stage_a3_html: Path,
    stage_a3_results_json: Path,
    stage_a3_import_artifact: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Build bounded Stage B only from an authenticated A1+A2+A3 GO."""
    # These imports are intentionally local: the evaluator imports this builder.
    from scripts.build_candidate_endorsement_line_ledger_a3_handcode import (
        A3_SPEC_SHA256,
        FROZEN_A3_TASK_SHA256,
        FROZEN_A3_HTML_SHA256,
        FROZEN_A3_RESULTS_SHA256,
        FROZEN_A3_IMPORT_ARTIFACT_SHA256,
        build_a3_provenance,
        derive_a3_residuals,
        sha256_file,
    )
    from scripts.evaluate_candidate_endorsement_line_ledger_pilot import (
        evaluate_stage_a2_only,
        replay_stage_a2_merge,
    )
    from scripts.import_candidate_endorsement_line_ledger_a3_handcode import (
        human_a3_import_artifact_payload,
        merge_human_a3,
        unchanged_gate_replay,
        validate_results,
        validate_task_provenance,
    )

    ensure_empty_output_dir(output_dir)
    input_paths = (
        full_export_dir, stage_a_export_dir, stage_a_output_dir,
        stage_a2_export_dir, stage_a2_output_dir,
    )
    if output_dir.resolve() in {path.resolve() for path in input_paths}:
        raise ValueError("Stage-B output directory cannot overwrite an input")

    replayed = replay_stage_a2_merge(
        stage_a_export_dir, stage_a_output_dir,
        stage_a2_export_dir, stage_a2_output_dir, full_export_dir,
    )
    evaluation = evaluate_stage_a2_only(
        stage_a_export_dir, stage_a_output_dir,
        stage_a2_export_dir, stage_a2_output_dir, full_export_dir,
    )
    stored_evaluation = json.loads(stage_a2_evaluation_report.read_text())
    _require_equal(stored_evaluation, evaluation, "Stage-A2 evaluation replay")
    residuals = derive_a3_residuals(replayed, evaluation)

    task_sha256 = sha256_file(stage_a3_task_json)
    _require_equal(task_sha256, FROZEN_A3_TASK_SHA256, "frozen Stage-A3 task bytes")
    html_sha256 = sha256_file(stage_a3_html)
    _require_equal(html_sha256, FROZEN_A3_HTML_SHA256, "frozen Stage-A3 HTML bytes")
    task = json.loads(stage_a3_task_json.read_text())
    expected_provenance = build_a3_provenance(
        replayed,
        residuals,
        stage_a2_export_dir=stage_a2_export_dir,
        stage_a2_evaluation_report=stage_a2_evaluation_report,
    )
    validate_task_provenance(task, expected_provenance)
    _require_equal(
        [item.get("residual") for item in task.get("items", [])],
        residuals,
        "Stage-A3 exact residual ledger",
    )
    results_sha256 = sha256_file(stage_a3_results_json)
    _require_equal(
        results_sha256, FROZEN_A3_RESULTS_SHA256,
        "frozen Stage-A3 completed results bytes",
    )
    results = json.loads(stage_a3_results_json.read_text())
    decisions = validate_results(task, results)
    merged = merge_human_a3(
        replayed["parents"], replayed["stage_a2_merge"], residuals, decisions,
        task_sha256=task_sha256, results_sha256=results_sha256,
    )
    gate_replay = unchanged_gate_replay(replayed, merged)["evaluation"]
    expected_import = human_a3_import_artifact_payload(
        task_sha256=task_sha256,
        results_sha256=results_sha256,
        results_exported_at=results["exported_at"],
        residual_count=len(residuals),
        decisions=decisions,
        stage_a2_merge=replayed["stage_a2_merge"],
        stage_a3_merge=merged,
        gate_replay=gate_replay,
    )
    imported = json.loads(stage_a3_import_artifact.read_text())
    _require_equal(
        sha256_file(stage_a3_import_artifact),
        FROZEN_A3_IMPORT_ARTIFACT_SHA256,
        "frozen Stage-A3 imported authorization bytes",
    )
    _require_equal(imported, expected_import, "Stage-A3 imported authorization")
    if imported.get("stage_b_authorized") is not True:
        raise ValueError("Stage-A3 import does not authorize Stage B")
    checks = gate_replay.get("checks")
    if (
        gate_replay.get("gate_decision") != "GO"
        or not isinstance(checks, list)
        or len(checks) != 11
        or any(check.get("passed") is not True for check in checks)
    ):
        raise ValueError("Stage-A3 unchanged content gates are not 11/11 GO")

    three_pass_authorization = {
        **FROZEN_STAGE_A3_AUTHENTICATION,
        "stage_a3_decisions_sha256": merged["stage_a3_decisions_sha256"],
        "stage_a3_final_decisions_sha256": merged["final_decisions_sha256"],
        "stage_a3_per_line_provenance_sha256": merged[
            "per_line_provenance_sha256"
        ],
    }
    batches = build_record_extraction_batches_from_adjudicated_merge(
        replayed["parents"], merged,
        three_pass_authorization=three_pass_authorization,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    mode = "record_extraction_preflight_from_stage_a3"
    selection = _selection_rows(
        replayed["parents"], mode=mode,
        stage_a_decisions=merged["final_decisions"],
    )
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunk_entries = write_batch_chunks(output_dir / "chunks", batches, chunk_size)
    source_replay = replay_frozen_source(full_export_dir)
    manifest = _base_manifest(
        mode=mode, stage=RECORD_EXTRACTION_STAGE,
        replay=source_replay, parents=replayed["parents"],
        batches=batches, selection_entry=selection_entry,
        chunks=chunk_entries, chunk_size=chunk_size,
    )
    stage_a2_run_artifacts = dict(replayed["stage_a2_artifacts"])
    stage_a2_run_artifacts.pop("stage_a2_decision_ledger_sha256", None)
    manifest.update({
        "stage_b_three_pass_provenance_spec_sha256": (
            STAGE_B_THREE_PASS_PROVENANCE_SPEC_SHA256
        ),
        "record_extraction_batch_schema_rule_sha256": (
            RECORD_EXTRACTION_BATCH_SCHEMA_RULE_SHA256
        ),
        "stage_b_upstream_pass_count": 3,
        "upstream_stage_a": two_pass_stage_b_manifest_provenance(
            replayed["stage_a_artifacts"], stage_a2_run_artifacts,
            replayed["stage_a2_decisions"], replayed["stage_a2_merge"],
        )["upstream_stage_a"],
        "upstream_stage_a2": two_pass_stage_b_manifest_provenance(
            replayed["stage_a_artifacts"], stage_a2_run_artifacts,
            replayed["stage_a2_decisions"], replayed["stage_a2_merge"],
        )["upstream_stage_a2"],
        "two_pass_merge": two_pass_stage_b_manifest_provenance(
            replayed["stage_a_artifacts"], stage_a2_run_artifacts,
            replayed["stage_a2_decisions"], replayed["stage_a2_merge"],
        )["two_pass_merge"],
        "upstream_stage_a3": {
            **three_pass_authorization,
        },
        "three_pass_merge": {
            "validation": dict(merged["validation"]),
            "final_decisions_sha256": merged["final_decisions_sha256"],
            "per_line_provenance_sha256": merged["per_line_provenance_sha256"],
            "ownership_by_continuation_sha256": sha256_json(
                merged["ownership_by_continuation"]
            ),
            "unchanged_gate_replay": gate_replay,
        },
    })
    manifest["bounded_preflight"] = {
        "enabled": True,
        "pilot_controls": [
            {"control_kind": kind, "parent_document_id": parent_id}
            for kind, parent_id in PILOT_CONTROLS
        ],
        "predeclared_content_gates": PILOT_CONTENT_GATES,
        "a1_a2_a3_authenticated": True,
        "unchanged_gate_count": 11,
        "unchanged_gate_decision": "GO",
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }
    return finalize_manifest(output_dir, manifest)


def _load_stage_a_input_export(
    stage_a_export_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    manifest_path = stage_a_export_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    manifest, manifest_sha256, paths = validate_input_manifest(
        manifest_path,
        stage_a_export_dir / "chunks",
        stage=LINE_DECISION_STAGE,
        contract=get_line_ledger_contract(),
    )
    rows = [row for path in paths for row in read_jsonl_gz(path)]
    return manifest, rows, manifest_sha256


def _load_stage_a_outputs(
    output_dir: Path,
    expected_batches: Sequence[Mapping[str, Any]],
    *,
    stage_a_export_dir: Path,
    stage_a_input_manifest_sha256: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    unsharded_metadata = output_dir / "run_metadata.json"
    sharded_metadata = sorted(output_dir.glob("run_metadata_shard_*.json"))
    if unsharded_metadata.is_file() and sharded_metadata:
        raise ValueError("Stage-A output mixes sharded and unsharded run metadata")
    metadata_paths = (
        [unsharded_metadata] if unsharded_metadata.is_file() else sharded_metadata
    )
    if not metadata_paths:
        raise FileNotFoundError(
            f"Stage-A output has no run_metadata.json or shard metadata: {output_dir}"
        )
    metadata_rows = []
    for metadata_path in metadata_paths:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if not isinstance(metadata, dict):
            raise TypeError(f"Stage-A run metadata is not an object: {metadata_path}")
        metadata_rows.append(metadata)
    contract = get_line_ledger_contract()
    contract_sha256 = contract["line_ledger_contract_sha256"]
    for metadata_path, metadata in zip(metadata_paths, metadata_rows):
        label = metadata_path.name
        _require_equal(
            metadata.get("stage"), LINE_DECISION_STAGE,
            f"Stage-A output stage ({label})",
        )
        _require_equal(
            metadata.get("input_manifest_sha256"),
            stage_a_input_manifest_sha256,
            f"Stage-A output input-manifest binding ({label})",
        )
        _require_equal(
            metadata.get("line_ledger_contract_sha256"),
            contract_sha256,
            f"Stage-A output contract binding ({label})",
        )
        for field in ("model_id", "model_revision", "runtime_lock_sha256"):
            _require_equal(
                metadata.get(field), contract[field],
                f"Stage-A output {field} ({label})",
            )
        if not isinstance(metadata.get("generation_config_sha256"), str):
            raise ValueError(f"Stage-A output lacks generation hash ({label})")
        generation_config = metadata.get("generation_config")
        if not isinstance(generation_config, Mapping):
            raise ValueError(
                f"Stage-A output lacks canonical generation payload ({label})"
            )
        _require_equal(
            sha256_json(generation_config),
            metadata["generation_config_sha256"],
            f"Stage-A generation payload hash ({label})",
        )
        _require_equal(
            metadata.get("prompt_sha256"),
            contract["prompt_sha256"][LINE_DECISION_STAGE],
            f"Stage-A output prompt hash ({label})",
        )
        _require_equal(
            metadata.get("json_schema_catalog_sha256"),
            contract["json_schema_catalog_sha256"][LINE_DECISION_STAGE],
            f"Stage-A output schema catalog hash ({label})",
        )
        _require_equal(
            metadata.get("json_schema_variant_sha256"),
            contract["json_schema_variant_sha256"][LINE_DECISION_STAGE],
            f"Stage-A output schema variant map ({label})",
        )
        _require_equal(
            metadata.get("line_inventory_spec_sha256"),
            LINE_INVENTORY_SPEC_SHA256,
            f"Stage-A output inventory spec ({label})",
        )
        _require_equal(
            metadata.get("line_batch_spec_sha256"),
            LINE_BATCH_SPEC_SHA256,
            f"Stage-A output batch spec ({label})",
        )
        _require_equal(
            metadata.get("structured_decoding_backend"),
            STRUCTURED_DECODING_BACKEND,
            f"Stage-A structured-decoding backend ({label})",
        )
        _require_equal(
            metadata.get("structured_decoding_disable_any_whitespace"),
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
            f"Stage-A structured-decoding whitespace policy ({label})",
        )
        _require_equal(
            metadata.get("prompt_token_count_spec_sha256"),
            PROMPT_TOKEN_COUNT_SPEC_SHA256,
            f"Stage-A prompt-token count spec ({label})",
        )
        for field in ("max_model_len", "max_output_tokens"):
            value = metadata.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 256
            ):
                raise ValueError(f"Stage-A {field} is invalid ({label})")
        expected_generation_fields = {
            "stage": LINE_DECISION_STAGE,
            "model_id": contract["model_id"],
            "model_revision": contract["model_revision"],
            "runtime_lock_sha256": contract["runtime_lock_sha256"],
            "line_ledger_contract_sha256": contract_sha256,
            "prompt_sha256": contract["prompt_sha256"][LINE_DECISION_STAGE],
            "json_schema_catalog_sha256": contract[
                "json_schema_catalog_sha256"
            ][LINE_DECISION_STAGE],
            "json_schema_variant_sha256": contract[
                "json_schema_variant_sha256"
            ][LINE_DECISION_STAGE],
            "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
            "line_batch_spec_sha256": LINE_BATCH_SPEC_SHA256,
            "input_manifest_sha256": stage_a_input_manifest_sha256,
            "seed": FROZEN_GENERATION_SEED,
            "max_model_len": metadata["max_model_len"],
            "max_output_tokens": metadata["max_output_tokens"],
            "temperature": 0.0,
            "thinking_enabled": False,
            "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        }
        for field, expected in expected_generation_fields.items():
            _require_equal(
                generation_config.get(field), expected,
                f"Stage-A generation payload {field} ({label})",
            )
        _require_equal(
            generation_config.get("structured_decoding"),
            {
                "mode": "json_schema",
                "backend": STRUCTURED_DECODING_BACKEND,
                "disable_any_whitespace": (
                    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
                ),
            },
            f"Stage-A generation payload structured decoding ({label})",
        )
        selected_batches = metadata.get("selected_batches")
        if (
            not isinstance(selected_batches, int)
            or isinstance(selected_batches, bool)
            or selected_batches < 1
        ):
            raise ValueError(f"Stage-A selected batch count is invalid ({label})")
    _require_equal(
        len({metadata["generation_config_sha256"] for metadata in metadata_rows}),
        1,
        "Stage-A generation configuration across shards",
    )
    for field in ("max_model_len", "max_output_tokens"):
        _require_equal(
            len({metadata[field] for metadata in metadata_rows}),
            1,
            f"Stage-A {field} across shards",
        )
    _require_equal(
        sum(int(metadata["selected_batches"]) for metadata in metadata_rows),
        len(expected_batches),
        "Stage-A metadata selected-batch closure",
    )
    if sharded_metadata:
        shard_counts = {metadata.get("shard_count") for metadata in metadata_rows}
        if len(shard_counts) != 1:
            raise ValueError("Stage-A shard metadata disagrees on shard count")
        shard_count = next(iter(shard_counts))
        if not isinstance(shard_count, int) or isinstance(shard_count, bool):
            raise ValueError("Stage-A shard count is not an integer")
        _require_equal(
            sorted(metadata.get("shard_index") for metadata in metadata_rows),
            list(range(shard_count)),
            "Stage-A shard metadata closure",
        )
    else:
        _require_equal(
            metadata_rows[0].get("shard_index"), None,
            "unsharded Stage-A shard index",
        )
        _require_equal(
            metadata_rows[0].get("shard_count"), None,
            "unsharded Stage-A shard count",
        )

    input_paths = sorted(
        (stage_a_export_dir / "chunks").glob("chunk_*.jsonl.gz")
    )
    if not input_paths:
        raise ValueError("Stage-A input export contains no chunks")
    for metadata_path, metadata in zip(metadata_paths, metadata_rows):
        if sharded_metadata:
            shard_index = int(metadata["shard_index"])
            shard_count = int(metadata["shard_count"])
            selected_input_paths = [
                path for index, path in enumerate(input_paths)
                if index % shard_count == shard_index
            ]
        else:
            selected_input_paths = input_paths
        selected_input_batches = [
            row for path in selected_input_paths for row in read_jsonl_gz(path)
        ]
        expected_variant_inventory = schema_variant_inventory(
            selected_input_batches
        )
        metadata_variant_fields = {
            "selected_schema_variant_counts": "schema_variant_counts",
            "selected_schema_variant_hashes_used": (
                "schema_variant_hashes_used"
            ),
            "selected_schema_variant_ids_sha256": (
                "ordered_schema_variant_ids_sha256"
            ),
            "selected_schema_variant_hashes_sha256": (
                "ordered_schema_variant_hashes_sha256"
            ),
        }
        for metadata_field, inventory_field in metadata_variant_fields.items():
            _require_equal(
                metadata.get(metadata_field),
                expected_variant_inventory[inventory_field],
                f"Stage-A selected schema variants ({metadata_path.name})",
            )
    expected_output_names = [
        path.name.replace(".jsonl.gz", ".out.jsonl.gz")
        for path in input_paths
    ]
    paths = [output_dir / name for name in expected_output_names]
    actual_output_paths = sorted(output_dir.rglob("*.out.jsonl.gz"))
    if any(path.parent != output_dir for path in actual_output_paths):
        raise ValueError("Stage-A output chunks must be direct children of output dir")
    _require_equal(
        [path.name for path in actual_output_paths],
        sorted(expected_output_names),
        "Stage-A input/output chunk filename mapping",
    )
    allowed_files = {
        *(path.name for path in paths),
        *(path.name for path in metadata_paths),
    }
    actual_files = {
        str(path.relative_to(output_dir))
        for path in output_dir.rglob("*")
        if path.is_file()
    }
    _require_equal(actual_files, allowed_files, "Stage-A output directory inventory")
    unexpected_directories = [
        str(path.relative_to(output_dir))
        for path in output_dir.rglob("*")
        if path.is_dir()
    ]
    _require_equal(
        unexpected_directories, [], "Stage-A output directory nesting"
    )
    generation_hash = str(metadata_rows[0]["generation_config_sha256"])
    max_model_len = int(metadata_rows[0]["max_model_len"])
    max_output_tokens = int(metadata_rows[0]["max_output_tokens"])
    for input_path, output_path in zip(input_paths, paths):
        if not line_ledger_chunk_is_complete(
            input_path,
            output_path,
            stage=LINE_DECISION_STAGE,
            input_manifest_sha256=stage_a_input_manifest_sha256,
            expected_generation_config_sha256=generation_hash,
            expected_max_model_len=max_model_len,
            expected_max_output_tokens=max_output_tokens,
        ):
            raise ValueError(
                "Stage-A output failed raw-to-canonical typed replay: "
                f"{output_path.name}"
            )
    rows_by_id: dict[str, dict[str, Any]] = {}
    output_files = []
    for path in paths:
        chunk_rows = list(read_jsonl_gz(path))
        output_files.append({
            "file": str(path.relative_to(output_dir)),
            "records": len(chunk_rows),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        })
        for row in chunk_rows:
            batch_id = str(row.get("batch_id"))
            if batch_id in rows_by_id:
                raise ValueError(f"duplicate Stage-A output batch ID: {batch_id}")
            rows_by_id[batch_id] = row
    expected_by_id = {str(batch["batch_id"]): batch for batch in expected_batches}
    _require_equal(set(rows_by_id), set(expected_by_id), "Stage-A output batch coverage")
    ordered: list[dict[str, Any]] = []
    for batch in expected_batches:
        batch_id = str(batch["batch_id"])
        row = rows_by_id[batch_id]
        _require_equal(str(row.get("document_id")), batch_id, f"Stage-A document ID {batch_id}")
        _require_equal(row.get("stage"), LINE_DECISION_STAGE, f"Stage-A row stage {batch_id}")
        _require_equal(
            row.get("input_batch_sha256"), sha256_json(batch),
            f"Stage-A input batch binding {batch_id}",
        )
        _require_equal(
            row.get("input_manifest_sha256"), stage_a_input_manifest_sha256,
            f"Stage-A input manifest binding {batch_id}",
        )
        _require_equal(
            row.get("line_ledger_contract_sha256"), contract_sha256,
            f"Stage-A contract binding {batch_id}",
        )
        _require_equal(
            row.get("parse_status"), "parsed", f"Stage-A parse status {batch_id}"
        )
        _require_equal(
            row.get("technical_pass"), True, f"Stage-A technical gate {batch_id}"
        )
        validation = row.get("stage_validation")
        if not isinstance(validation, Mapping):
            raise ValueError(f"Stage-A row lacks stage validation: {batch_id}")
        _require_equal(validation.get("schema_valid"), True, f"Stage-A schema gate {batch_id}")
        _require_equal(validation.get("coverage_complete"), True, f"Stage-A coverage gate {batch_id}")
        if not isinstance(row.get("model_output"), Mapping):
            raise ValueError(f"Stage-A row lacks model output: {batch_id}")
        ordered.append(row)
    provenance = {
        "run_metadata_files": [
            {
                "file": path.name,
                "sha256": sha256_file(path),
                "shard_index": metadata.get("shard_index"),
                "shard_count": metadata.get("shard_count"),
                "selected_batches": metadata.get("selected_batches"),
                "processed_outputs_this_run": metadata.get(
                    "processed_outputs_this_run"
                ),
            }
            for path, metadata in zip(metadata_paths, metadata_rows)
        ],
        "input_manifest_sha256": stage_a_input_manifest_sha256,
        "generation_config_sha256": generation_hash,
        "json_schema_catalog_sha256": contract[
            "json_schema_catalog_sha256"
        ][LINE_DECISION_STAGE],
        "json_schema_variant_sha256": contract[
            "json_schema_variant_sha256"
        ][LINE_DECISION_STAGE],
        "schema_variant_inventory": schema_variant_inventory(
            list(expected_batches)
        ),
        "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        "max_model_len": max_model_len,
        "max_output_tokens": max_output_tokens,
        "raw_to_canonical_typed_replay_complete": True,
        "exact_input_output_chunk_mapping": True,
        "output_files": output_files,
        "output_batch_ids_sha256": sha256_json([
            str(row["batch_id"]) for row in ordered
        ]),
        "validated_output_batches": len(ordered),
    }
    return ordered, provenance


def build_record_extraction_export(
    full_export_dir: Path,
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    output_dir: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    enforce_frozen_source: bool = True,
) -> dict[str, Any]:
    ensure_empty_output_dir(output_dir)
    resolved_inputs = {
        full_export_dir.resolve(), stage_a_export_dir.resolve(),
        stage_a_output_dir.resolve(),
    }
    if output_dir.resolve() in resolved_inputs:
        raise ValueError("Stage-B output directory cannot overwrite an input")
    stage_a_manifest, stage_a_batches, stage_a_manifest_sha256 = (
        _load_stage_a_input_export(stage_a_export_dir)
    )
    replay = replay_frozen_source(
        full_export_dir, enforce_frozen_source=enforce_frozen_source
    )
    _require_equal(
        stage_a_manifest.get("source_export", {}).get("source_export_manifest_sha256"),
        replay["manifest_sha256"],
        "Stage-A source-export binding",
    )
    selected_ids = [str(value) for value in stage_a_manifest["selected_parent_ids"]]
    parents = reconstruct_selected_parents(replay, selected_ids)
    regenerated_stage_a = build_line_decision_batches(parents)
    _require_equal(
        [sha256_json(batch) for batch in regenerated_stage_a],
        [sha256_json(batch) for batch in stage_a_batches],
        "regenerated Stage-A inputs",
    )
    stage_a_outputs, stage_a_provenance = _load_stage_a_outputs(
        stage_a_output_dir,
        stage_a_batches,
        stage_a_export_dir=stage_a_export_dir,
        stage_a_input_manifest_sha256=stage_a_manifest_sha256,
    )
    decisions = line_decision_ledger(stage_a_batches, stage_a_outputs)
    _require_equal(
        len(decisions),
        inventory_census(parents)["model_decision_lines"],
        "Stage-A source-line decision coverage",
    )
    batches = build_record_extraction_batches(parents, decisions)

    output_dir.mkdir(parents=True, exist_ok=True)
    preflight = stage_a_manifest.get("mode") == "line_decision_preflight"
    mode = (
        "record_extraction_preflight_from_stage_a"
        if preflight else "record_extraction_full_from_stage_a"
    )
    selection = _selection_rows(
        parents, mode=mode, stage_a_decisions=decisions
    )
    selection_entry = write_selection(output_dir / "selection.csv", selection)
    chunk_entries = write_batch_chunks(
        output_dir / "chunks", batches, chunk_size
    )
    manifest = _base_manifest(
        mode=mode,
        stage=RECORD_EXTRACTION_STAGE,
        replay=replay,
        parents=parents,
        batches=batches,
        selection_entry=selection_entry,
        chunks=chunk_entries,
        chunk_size=chunk_size,
    )
    roles = Counter(str(decision["role"]) for decision in decisions.values())
    manifest["stage_a_provenance"] = {
        **stage_a_provenance,
        "input_export_mode": stage_a_manifest["mode"],
        "input_export_manifest_sha256": stage_a_manifest_sha256,
        "input_batch_count": len(stage_a_batches),
        "source_line_decision_count": len(decisions),
        "role_counts": dict(sorted(roles.items())),
        "anchor_line_count": roles["record_anchor"] + roles["compound_record_anchor"],
        "unclear_line_count": roles["unclear"],
    }
    manifest["bounded_preflight"] = {
        "enabled": preflight,
        "pilot_controls": [
            {"control_kind": kind, "parent_document_id": parent_id}
            for kind, parent_id in PILOT_CONTROLS
        ] if preflight else [],
        "predeclared_content_gates": PILOT_CONTENT_GATES if preflight else {},
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }
    return finalize_manifest(output_dir, manifest)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=(
            "line-decision-preflight",
            "line-decision-full",
            "line-decision-recall-pilot40",
            "line-decision-recall40-round3",
            "line-adjudication-recall-pilot40",
            "line-adjudication-recall40-round3",
            "record-extraction-recall-pilot40-from-stage-a2",
            "record-extraction-recall40-round3-from-stage-a2",
            "line-adjudication-preflight-from-stage-a",
            "record-extraction-from-stage-a",
            "record-extraction-from-stage-a2",
            "record-extraction-from-stage-a3",
        ),
    )
    parser.add_argument(
        "--full-export-dir", type=Path, default=DEFAULT_FULL_EXPORT_DIR
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--stage-a-export-dir", type=Path)
    parser.add_argument("--stage-a-output-dir", type=Path)
    parser.add_argument("--stage-a2-export-dir", type=Path)
    parser.add_argument("--stage-a2-output-dir", type=Path)
    parser.add_argument("--stage-a2-evaluation-report", type=Path)
    parser.add_argument("--stage-a3-task-json", type=Path)
    parser.add_argument("--stage-a3-html", type=Path)
    parser.add_argument("--stage-a3-results-json", type=Path)
    parser.add_argument("--stage-a3-import-artifact", type=Path)
    parser.add_argument("--private-selection-path", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "line-decision-preflight":
        manifest = build_line_decision_export(
            args.full_export_dir,
            args.output_dir,
            preflight=True,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "line-decision-full":
        manifest = build_line_decision_export(
            args.full_export_dir,
            args.output_dir,
            preflight=False,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "line-decision-recall-pilot40":
        if args.private_selection_path is None:
            raise SystemExit("line-decision-recall-pilot40 requires --private-selection-path")
        manifest = build_recall40_line_decision_export(
            args.full_export_dir, args.private_selection_path, args.output_dir,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "line-decision-recall40-round3":
        if args.private_selection_path is None:
            raise SystemExit(
                "line-decision-recall40-round3 requires --private-selection-path"
            )
        manifest = build_recall40_round3_line_decision_export(
            args.full_export_dir, args.private_selection_path, args.output_dir,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "line-adjudication-preflight-from-stage-a":
        if args.stage_a_export_dir is None or args.stage_a_output_dir is None:
            raise SystemExit(
                "line-adjudication-preflight-from-stage-a requires "
                "--stage-a-export-dir and --stage-a-output-dir"
            )
        manifest = build_line_adjudication_export(
            args.full_export_dir,
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.output_dir,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "line-adjudication-recall-pilot40":
        if args.stage_a_export_dir is None or args.stage_a_output_dir is None:
            raise SystemExit(
                "line-adjudication-recall-pilot40 requires --stage-a-export-dir "
                "and --stage-a-output-dir"
            )
        manifest = build_recall40_line_adjudication_export(
            args.full_export_dir, args.stage_a_export_dir,
            args.stage_a_output_dir, args.output_dir,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "line-adjudication-recall40-round3":
        if args.stage_a_export_dir is None or args.stage_a_output_dir is None:
            raise SystemExit(
                "line-adjudication-recall40-round3 requires "
                "--stage-a-export-dir and --stage-a-output-dir"
            )
        manifest = build_recall40_round3_line_adjudication_export(
            args.full_export_dir,
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.output_dir,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "record-extraction-recall-pilot40-from-stage-a2":
        required = (
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.stage_a2_export_dir,
            args.stage_a2_output_dir,
            args.stage_a2_evaluation_report,
        )
        if any(value is None for value in required):
            raise SystemExit(
                "record-extraction-recall-pilot40-from-stage-a2 requires all "
                "--stage-a*, --stage-a2*, and --stage-a2-evaluation-report inputs"
            )
        manifest = build_recall40_record_extraction_export(
            args.full_export_dir,
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.stage_a2_export_dir,
            args.stage_a2_output_dir,
            args.stage_a2_evaluation_report,
            args.output_dir,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "record-extraction-recall40-round3-from-stage-a2":
        required = (
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.stage_a2_export_dir,
            args.stage_a2_output_dir,
            args.stage_a2_evaluation_report,
        )
        if any(value is None for value in required):
            raise SystemExit(
                "record-extraction-recall40-round3-from-stage-a2 requires all "
                "--stage-a*, --stage-a2*, and --stage-a2-evaluation-report inputs"
            )
        manifest = build_recall40_round3_record_extraction_export(
            args.full_export_dir,
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.stage_a2_export_dir,
            args.stage_a2_output_dir,
            args.stage_a2_evaluation_report,
            args.output_dir,
            chunk_size=args.chunk_size,
        )
    elif args.mode == "record-extraction-from-stage-a":
        raise SystemExit(
            "record-extraction-from-stage-a is a preserved historical builder "
            "only; the current contract requires record-extraction-from-stage-a2"
        )
    elif args.mode == "record-extraction-from-stage-a2":
        raise SystemExit(
            "record-extraction-from-stage-a2 is a preserved historical builder "
            "only; current Stage B requires authenticated A1+A2+A3"
        )
    else:
        required = {
            "--stage-a-export-dir": args.stage_a_export_dir,
            "--stage-a-output-dir": args.stage_a_output_dir,
            "--stage-a2-export-dir": args.stage_a2_export_dir,
            "--stage-a2-output-dir": args.stage_a2_output_dir,
            "--stage-a2-evaluation-report": args.stage_a2_evaluation_report,
            "--stage-a3-task-json": args.stage_a3_task_json,
            "--stage-a3-html": args.stage_a3_html,
            "--stage-a3-results-json": args.stage_a3_results_json,
            "--stage-a3-import-artifact": args.stage_a3_import_artifact,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            raise SystemExit(
                "record-extraction-from-stage-a3 requires " + ", ".join(missing)
            )
        manifest = build_human_a3_record_extraction_export(
            args.full_export_dir,
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.stage_a2_export_dir,
            args.stage_a2_output_dir,
            args.stage_a2_evaluation_report,
            args.stage_a3_task_json,
            args.stage_a3_html,
            args.stage_a3_results_json,
            args.stage_a3_import_artifact,
            args.output_dir,
            chunk_size=args.chunk_size,
        )
    print(json.dumps({
        "mode": manifest["mode"],
        "stage": manifest["stage"],
        "parents": len(manifest["selected_parent_ids"]),
        "records": manifest["exported_records"],
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "output_dir": str(args.output_dir),
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
