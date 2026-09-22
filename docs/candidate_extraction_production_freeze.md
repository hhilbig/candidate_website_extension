# Candidate extraction production freeze

Written September 15, 2026. This file records step 1 of the extracted-variables
release checklist in [status.md](status.md). It names one source manifest, one
model configuration, and one extraction contract for each route.

Steps 2 through 5 read this file. The [codebook](candidate_extraction_extension_codebook.md)
still controls the six-file schema and the missingness definitions.

## Authority

Hanno made three decisions on September 15, 2026.

1. Round four is the production endorsement implementation. The line-ledger
   implementation becomes historical evidence.
2. Step 1 reconciles round four with the release importer, packager, and
   validator in code. Plan approval supplies the production-integration
   authority that the round-four development record withheld.
3. The release uses an accounting gate instead of a corpus-wide coverage
   boolean. The section "Coverage contract" states the gate.

This file does not authorize a release. Release needs human validation in step 4
and publication approval in step 5. The production binding therefore sets
`release_authorized` to `False`.

Full-corpus inference is step 2. It needs a separate full-run approval.

## Source corpus

All three routes read one corpus and one export.

| Item | Value |
|---|---|
| Source corpus | `build/release_v2_1_candidate/raw_corpus.parquet` |
| Source corpus SHA-256 | `c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3` |
| Source export | `build/hive_candidate_extraction/v2_1_full_export_20260824p` |
| Export manifest SHA-256 | `f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c` |
| Parent documents | 23,871 |
| Text windows | 35,213 |

The corpus hash matches public Dataverse version 3.0. The packager stops if the
hash differs.

## Model configuration

All three routes use one model and one runtime.

| Item | Value |
|---|---|
| Model | `Qwen/Qwen3.6-27B-FP8` |
| Model revision | `e89b16ebf1988b3d6befa7de50abc2d76f26eb09` |
| Runtime lock SHA-256 | `4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3` |
| Temperature | 0.0 |
| Thinking | disabled |
| Structured decoding | xgrammar, JSON schema, no free whitespace |
| Maximum model length | 32,768 tokens |

No commercial API produces a released value. Every run uses UC Davis Hive.

## Per-route contract

| Route | Documents | Windows | Contract SHA-256 | Prompt SHA-256 | Schema SHA-256 | Architecture |
|---|---:|---:|---|---|---|---|
| Issues | 14,326 | 20,673 | `82530ab0f4126e1640dcffbe48a66bffdfab1538fc40240b5f3e55ab0eed5574` | `e7ad149cbbdfa5657b32235c3421adc4f5ca89fa564f7075e6a26e26f00fcb19` | `c5b9922e9447cc5c573bd3eeefa432710a0657e37c484c46e30dc0e83a41c719` | one stage per segment |
| Biography | 6,962 | 8,444 | `82530ab0f4126e1640dcffbe48a66bffdfab1538fc40240b5f3e55ab0eed5574` | `d32cf8c87062d607cc1ee09e9485b90f2cc1a581559944583dc4839efa177960` | `ee197f260e3e2242d65ac05644d6cba72c2c2e3114fe127c579be6070ab3ffd7` | one stage per segment |
| Endorsements | 2,583 | 6,096 | `f8c755ce5097345cd5e99755c62f470ac6fdfdfadc116d4b8ecc41da7797522b` | Stage A `35a8a6cc907568fa6e4d7e19ec0e92cd2e926f1cec7e7fd99a027e1637549546`, Stage B `bb51ffe6cd0c0f5d026cfb4e2f2448e8f98d4447613a724d40a7d67a034f56a4` | Stage A catalog `e45b50b8b1502b01d578b333c71deabd5b549fc8678567d251332c49a5fa1ed1`, Stage B catalog `feebaf613ac4ea9425aa73ba93bec225e40d867b1ebe235566170bf6f9213d7a` | round four, two stages |

Issues and biography use seed 20260824. Endorsements use seed 20260907.
Stage A allows 2,048 output tokens. Stage B allows 16,384 output tokens.

The endorsement contract also binds these frozen specifications.

| Component | SHA-256 |
|---|---|
| Production binding | `0ff9120fd4930c113d5752d973a68f54fc699ce4360920b749dbb9e2c5b48c72` |
| Record projection specification | `7418bfc2c88ca0351a33b3d5312a9290bfdf5e842530b3d127c3977e1a739f76` |
| Failure accounting specification | `a331a7721f2a347a2271a7f6a7c3f0bac03481df2f2c9db0782b54c58c4422e3` |
| Coverage specification | `a873e6800c569ca7bd176d1510d6afccf2930334bae6bf3e80e0336ae1997b78` |
| Development binding, unchanged | `a5c482b99244d3541e45c72621f358f60562c857e57220e28195cb480e4a8f40` |

The production module asserts that the production prompts equal the reviewed
development prompts. A test checks this. Production therefore runs the exact
semantics that the September 14 development review covered.

The line inventory uses its base scope, `reconstruct_parent_inventory_base_scope_v1`.
The independent sample used the same scope.

## Endorsement implementation decision

Round four replaces the line ledger. Round four has two model stages. It has no
model adjudication stage and no human line stage.

This choice has one direct consequence. The line ledger cleared every open line
with its Stage A2 and Stage A3 passes. Round four cannot clear an open line.
A Stage A `unclear` line and an unowned continuation line therefore stay open.
The ledger counts them and names them. They do not stop the release.

The 14-row Stage A3 adjudication in
`quality_reports/candidate_endorsement_line_ledger_human_a3_21447139.json`
authorized the line-ledger Stage B only. It reports zero unresolved lines.
It stays valid for the line-ledger bundle. It does not transfer to round four.
Do not repeat that adjudication. Step 4 may use its 14 decisions as evidence.

The line-ledger code, its artifacts, and its frozen hashes stay in place.
Three development files carry an origin attestation. The production path adds
files. It never edits those three files.

## Technical failure accounting

One Stage B response has exactly one of five outcomes.

| Class | Detection | Treatment | Re-inference |
|---|---|---|---|
| `complete` | the technical check passes | accept every slot | no |
| `optional_field_repaired` | a slot replay passes after optional-field sanitization | accept the slot, null the field, record the change | no |
| `unresolved_target_block` | a slot replay still fails | withhold the record with a typed reason | no |
| `infrastructure_failure` | missing row, incomplete generation, parse error, or truncated input | bounded retry | yes |
| `unlocalizable_schema_failure` | the batch schema fails with no slot isolation | bounded retry | yes |

A withheld record is never a measured zero. A parent with no aligned record and
at least one withheld record reports `processed_all_withheld`. The packager then
writes `withheld_validation`, not `observed_zero`.

Re-inference cannot repair the first three classes. Decoding is deterministic at
temperature zero with a fixed seed, so a rerun returns identical bytes.

### Retry rule

1. Retry only an `infrastructure_failure` or an `unlocalizable_schema_failure`.
2. Allow at most two attempts for each batch. This matches the project rule.
3. Reuse the identical prompt, schema, seed, and model. Only the input manifest
   hash changes, because a retry artifact holds only the failed batches.
4. Keep every attempt. Each row records the attempt index, the input manifest
   hash, the generation config hash, the Slurm job identifier, the raw output
   hash, and the typed outcome. Mark the accepted attempt.
5. A batch that still fails after two attempts stops the release. Report it.

The ledger build refuses to run while a retryable failure stays open. This keeps
the technical gate in the packager meaningful.

### Measured baseline

The September 14 independent sample gives the reference numbers. The replay in
`quality_reports/candidate_endorsement_round4_release_replay_2026-09-15.json`
reproduces them with no model call.

| Quantity | Value |
|---|---:|
| Stage A responses, all parsed, job 23312106 | 292 |
| Stage B responses, job 23312502 | 639 |
| Stage B content failures | 37 |
| Failures repaired at slot level | 27 |
| Unresolved target blocks | 11 |
| Stage B anchors | 2,512 |
| Records projected with complete lineage | 2,496 |
| Records whose evidence matches the exact parent span | 2,496 |
| Model records dropped without a record | 0 |

All 37 failures stopped normally. None was a truncation.

## Coverage contract

Each parent keeps its own coverage flag. A parent is complete when every
in-scope line holds a terminal Stage A role and every anchor holds a resolved
Stage B decision. A `not_endorsement` decision is resolved and keeps the parent
complete.

The release no longer requires complete coverage across the corpus. It requires
complete accounting. Three conditions must hold.

1. `all_model_records_accounted` is true.
2. `unaccounted_outputs` is zero.
3. `unresolved_infrastructure_failures` is zero.

Every remaining gap must carry a typed reason and an identifier. The validator
refuses a gap without both. This gate is stricter about enumeration than the old
boolean. It is weaker in that it accepts enumerated gaps. Hanno approved this
change on September 15, 2026.

## What step 2 must do

Step 2 runs three inference jobs. No full-corpus output exists yet.

| Job | Scale |
|---|---:|
| One-stage suffix-p run over all three routes | 35,213 windows |
| Round-four Stage A over the endorsement parents | 19,142 batches |
| Round-four Stage B over the Stage A anchors | 41,005 batches, estimated |

The Stage A count is exact. `scripts/build_candidate_endorsement_round4_production_inputs.py census`
computes it from the frozen export with no model call. Its output is in
`quality_reports/candidate_endorsement_round4_production_census_2026-09-15.json`.

The Stage B figure is an estimate. It scales the independent sample's anchor
rate to the corpus. That sample was selected for endorsement content, so the
estimate is high. The upper bound is 71,676 batches. Step 2 computes the exact
count after Stage A finishes.

### Context window correction, September 15, 2026

A token preflight over the full corpus found a blocker before any run started.
The development runs used a 32,768-token window, which suits a 40-parent sample.
The full corpus does not fit that window. 106 Stage-A prompts exceed its budget,
and the largest needs 34,248 tokens against a budget of 30,464.

The runner refuses to truncate an input. It raises before any inference, so one
oversized batch would abort the whole job after the model had loaded.

Character counts do not predict this. The measured ratio at the maximum is 3.47
characters per token, not the four a reader might assume.

The correction is the smallest one available. The checkpoint supports 262,144
positions, so the window was a runner setting and never a model limit. The
production runner now uses 49,152. Every prompt, schema, seed, and frozen hash
stays identical, and the contract hash does not change. A larger window raises
the memory each sequence reserves, so throughput per GPU falls; step 2 should
measure the rate in its pilot rather than reuse the development rate.

Run `scripts/hive/preflight_candidate_endorsement_round4_prompt_tokens.py` on
the Hive login node before any submission. It loads no weights and needs no GPU.
Submit only when it reports `fits`. The evidence is in
`quality_reports/candidate_endorsement_round4_prompt_token_preflight_2026-09-15.json`.

Stage B prompts cannot be measured yet, because Stage A anchors define them.
Step 2 must run the same preflight for Stage B before submitting that stage.

Step 2 must also fill four constants in `scripts/import_candidate_extraction_v2.py`.
They are `None` now, and they name the per-run manifest and generation-config
hashes for each stage. The structural checks run while they are `None`. The
exact byte identity is required once they hold a value.

Step 2 needs full-run authority and a cost estimate before submission.
