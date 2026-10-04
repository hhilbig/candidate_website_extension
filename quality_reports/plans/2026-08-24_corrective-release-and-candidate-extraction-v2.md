# Corrective release and candidate extraction v2

**Date:** 2026-08-24
**Status:** core submitted separately; Stage-A job 21437036 passed every
technical gate but is a substantive NO-GO; bounded Stage-A2 correction only is
approved, no Stage B or full run was submitted, and the extraction package and
human validation remain blocked
**Authority:** the user approved this plan in the Codex task on 2026-08-24

## Current state

The user reported that the corrected core data were submitted to Dataverse with
additional corrections. No further core upload or draft action belongs to this
extraction task. The extraction extension remains nonpublishable.

Issues and biography retain the frozen one-stage suffix-p route outputs.
Public endorsements now use a separate two-stage parent-page line ledger. The
one-stage endorsement output remains complete historical and private
provenance, but it cannot supply a public endorsement record. Stage A assigns a
role to every exact parent-page source line. Stage B extracts records only from
the Stage-A record anchors and must account for every target. The first Stage-A
preflight, Hive job 21435372, failed 10 of 113 batches because the model used
padding decisions for real targets. Those artifacts are frozen. The corrected
target-count-specific contract is frozen under SHA-256
`ac8d6ed7f06e58a7961e08f8d50f7a86907f1bada12f1c70f5e9963011a335c2`.
Replacement Stage-A preflight 21437036 is queued and has not passed.

The downstream importer, release packager, release validator, and dormant
human-validation support have been rebound to the corrected contract. They
replay both stages from the frozen source, retain decisions, blocks, raw model
outputs, alignments, and withheld records privately, and permit only Stage-B
aligned records in the public endorsement table. No extraction release package,
handcoding app, or assignment has been generated. The provisional human
workload is 240 sampled tasks in four batches of 60, with the first batch timed;
the 11 known-omission link confirmations remain separate.

Earlier one-stage suffix-n, suffix-o, and suffix-p preflights are failed tuning
audits. Their detailed chronology remains below. The full 35,213-window source
export passes the local provenance and context-budget census with zero windows
over model context and a minimum margin of 1,308 tokens. This local result does
not replace the two-stage Hive content gates.

The core correction and the extraction extension therefore use separate release
steps. The core correction does not wait for the extraction rerun.

## Step 1: core correction submitted separately and out of scope

**External status reported by the user:** submitted separately on 2026-08-24.
Do not create or modify another core draft from this extraction workflow.
The core counts and publication checks remain part of the core release record,
not work to repeat here. Publication and any public announcement still require
their own approval if they have not already occurred.

## Step 2: rerun candidate extraction on Hive

Preserve the first extraction as a failed audit. Do not overwrite it. Use
Qwen/Qwen3.6-27B-FP8 at revision
`e89b16ebf1988b3d6befa7de50abc2d76f26eb09`. Use runtime lock
`4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3`,
temperature zero, and structured output. No released value can use a commercial
API.

Pilot the revised schema on the existing tuning pages and balanced controls.
Then rerun all 35,213 windows: 20,673 issue windows, 8,444 biography windows,
and 6,096 endorsement windows. Require exact source spans and exact offsets.
Do not use normalized substring matches.

The first v2 pilot, Hive job 21211898, failed and is frozen in
`quality_reports/candidate_extraction_v2_pilot_failed_2026-08-24.json`. Only 12
of 69 windows passed, and all 12 were empty-result windows. The model returned
the right exact string at a different source location for 1,082 of 1,217
anchors, returned ambiguous exact strings for 61, altered 51 source strings,
and supplied only 23 correct offsets. Three dense endorsement outputs also hit
the 8,192-token output limit. Do not run the full corpus under that contract.

The revised internal contract derives offsets only from verified exact Unicode
source strings, records the match count and alignment method, and withholds
altered or ambiguous strings from public records. It also records the
generation finish reason and output-token count, rejects length truncation, and
includes worst-density controls. Exact aligned records from an otherwise valid
window remain public; every withheld model item remains private with a stable
ID and reason. A measured zero requires zero aligned and zero withheld items.

Two later stress-pilot jobs are also audit artifacts, not release inputs. Hive
job 21226762 parsed 60 of 61 windows and truncated one dense endorsement
window. Job 21229337 parsed all 61 windows after the output limit increased,
but the strict importer found incomplete omission recovery and overlap or
endorsement-context cases that required a contract revision. Do not patch
those outputs into the release.

The superseded pre-control-character v2.1 contract SHA-256 is
`1cd69a9639267dbea62969b3cb8604bf5feeedb08fca10eadffcbb23c10613bc`.
The source inventory remains 23,871 parents and 35,213 windows. A full-corpus
preflight assigned the 6,096 endorsement windows to 1,501 segment contexts,
1,964 carried parent-document contexts, and 2,631 classifier-qualified
route-URL contexts, with zero provenance-validation errors. These are source
context assignments, not model results or human validation. The immutable
schema-1.4 full source export has manifest SHA-256
`973050bc6f159bee3385788588bf0c807804c73227f0c9fbabf43809cb82c179`.

Stress-pilot job 21246308 completed successfully on 2026-08-24. All 61
windows stopped normally, parsed, passed the schema, and produced exact aligned
public-eligible output. The largest response used 18,873 of 24,576 output
tokens. The strict importer passed and recovered all 11 known omissions through
ordinary same-model records and exact parent-span overlap. It resolved 1,118
records while retaining 178 unsupported model items and 39 overlap-conflict
groups privately. Human semantic confirmation of the 11 links remains part of
the final handcoding gate. This pilot did not establish that the model would
serialize copied C0 control characters safely in JSON.

Full Hive array 21250200 started the 35,213 windows in 30 restart-safe shards
with at most four GPUs active at once. A rolling audit stopped the job after
2,100 windows. Of those, 2,094 parsed and six endorsement windows failed strict
JSON parsing. Each failure stopped normally but copied a literal U+0009 tab
inside a JSON string. The source-wide audit found tabs in 905 windows and
U+0003 in 14, so this was a general contract defect rather than six rows to
patch. The four active array tasks were cancelled after 32 minutes and 40
seconds; the other 26 never started. The 35 completed chunk files and exact
diagnostic are frozen in
`quality_reports/candidate_extraction_v2_1_full_failed_2026-08-24.json`. They
must never be resumed, repaired, or merged into a later run.

The corrective contract keeps the authoritative source text, hashes, and
Unicode offsets unchanged. The model display preserves line feeds, displays
source U+0022 quotation marks as `\u0022`, doubles source backslashes, and
escapes non-linefeed C0 controls. Strict JSON decoding restores these escapes
to their original Unicode before exact alignment against the unchanged source.
A deterministic boundary map attests display-codec roundtrip and collision
boundaries; it is not a fallback or span-repair path. The runner keeps the
verbatim generation, then uses a narrow lexical canonicalizer that escapes
raw C0 characters only when they occur inside a JSON string. Every other
malformed condition still fails, and the canonical payload goes through the
existing strict duplicate-key, nonfinite-number, and invalid-escape parser.
The contract records the display and canonicalizer specifications, raw and
canonical payload hashes, and exact repair counts. Parsed evidence must still
align directly to the unchanged source; there is no fallback repair for a
display-encoded string.

The now-superseded quote-safe v2.1 contract SHA-256 is
`87175138600c9ba032d1218f318083645fab82c20d35ab910b1b962e197bab71`.
The generation configuration SHA-256 at seed 20260824, maximum model length
32,768, and maximum output length 24,576 is
`fcce9b2077476deba9982fb4f12e0deda1dabe946a485dde9b0659da17d55cac`.
The display specification SHA-256 is
`89d114207b6d25517f89eed76350608c3d0f3ffc9154ba6e34bac5f11971891f`,
and the JSON-string canonicalizer specification SHA-256 is
`64844ef4d08777e33c01795cb86a937200b8f5d5770a6ff0724b86f50c8ec5a9`.

Before another full run, the new stress pilot must include complete segment
sets for 867 parents and 1,787 windows. This covers every parent containing one
of the 962 changed-display windows plus all earlier pilot parents. Its manifest
must separately attest the 1,021-window union of changed prompts and prior
controls. The pilot must stop and parse every window, recover all 11 known
omissions through ordinary model output, preserve every source hash and offset,
and demonstrate exact decoded alignment for tabs, U+0003, and literal
backslashes. Only then may the complete 35,213-window run start in a new empty
directory under the newly frozen contract hash.

Hive array 21265577 ran this expanded pilot in four restart-safe shards. Its
input manifest SHA-256 is
`c60da213bbbb6c2908a21f82dfd03f33f5277e1222bcd017d172e692a9daa25e`;
its parent-selection file SHA-256 is
`e199287a609ec9ff39e6fc3f2f64dfc74821a148a4ffdddcf497f0bd8f276b01`.
All 1,787 windows stopped normally, parsed, and produced exact schema-valid
public output. The canonicalizer handled 22 literal tabs in five outputs with
zero unrepaired controls. The pilot nevertheless failed the model-schema gate:
one biography item used `local_roots` with null timing, and one endorsement
used `explicit_text` with null context evidence. Both were withheld privately,
but `invalid_schema` remains a hard failure. The complete diagnostic and 38-file
inventory are frozen in
`quality_reports/candidate_extraction_v2_1_control_pilot_failed_2026-08-24.json`.

The generic guided biography item schema must therefore become three disjoint
full-object branches for local roots, timing-required occupation or service,
and optional-timing education or military records. The endorsement item schema
must become two disjoint branches for explicit-text and page-or-section support
modes. These changes encode the already-approved rules at generation time; they
do not change the public field scope. Run a one-chunk Hive schema-compilation
preflight and then rerun the complete 1,787-window pilot under the recomputed
hashes. Neither failed pilot output nor the later full output is a release
artifact until the strict importer and all downstream gates pass.

The first three-route schema-compilation preflight, Hive job 21277141, failed
before generation even though Slurm exited zero. All three route rows had
`finish_reason=error`, and the pinned xgrammar 0.2.3 compiler raised
`bitset::set: __position (which is 128) >= _Nb (which is 128)`. The shared
nonblank-string pattern used a negated class containing Python Unicode strip
characters above byte 127; xgrammar warned that it clamped the class to byte
127 and then failed. The exact diagnostic and file hashes are frozen in
`quality_reports/candidate_extraction_v2_1_schema_preflight_failed_2026-08-24.json`.
Keep the conditional branches, use only an xgrammar-compatible guided nonblank
restriction, and retain the full Python Unicode-strip check as a fail-closed
post-generation rule. Recompute the affected hashes and repeat this three-row
preflight before rerunning the 1,787-window pilot.

The replacement preflight, Hive job 21279840, compiled all three route schemas
but exposed a second xgrammar incompatibility. Each route generated repeated
one-character records until it reached the 24,576-token output limit. All three
rows had `finish_reason=length` and `parse_status=output_truncated`; none parsed
or entered alignment. This was not an OOM, input truncation, hash mismatch, or
control-character failure. The exact diagnostic and hashes are frozen in
`quality_reports/candidate_extraction_v2_1_schema_preflight_regex_failed_2026-08-24.json`.
Remove regex guidance from the model schema and return to the previously stable
`minLength: 1` restriction. The Python Unicode-strip validator remains the hard
model-item gate. Repeat the three-route preflight before any larger Hive run.

The now-superseded final pattern-free contract uses `minLength: 1` on all 22 free-text schema
nodes and keeps the full Python Unicode-strip check as the fail-closed
model-item validator. Its contract SHA-256 is
`1a5da28b31a2ed7fb35ac970181f776a83aa03ac709c1ac772e0928f55a2a71a`,
and its generation-configuration SHA-256 is
`88e12103cff65e7b3a2197ab08a1726325e1681cdd12d7ff406361b8e00b86ed`.
The issue, biography, and endorsement schema SHA-256 values are respectively
`475d3af460738cdaac93be5765675ca7b058ef5755347241fa8b142f4823bbd8`,
`cad901193d9a2bce88fa80e5dff075854b0ca26d64258499da997bff567e2a6d`,
and `caf8587e2456b68caa330dff779b68b66b9c9446f532801bab3477818ed8bcac`.

Hive preflight 21284904 passed this contract on one window from every route.
All three stopped normally, strict-parsed, passed the raw model schema, and
produced exact supported public output. Run metadata reconciled 3 selected,
processed, parsed, schema-valid, and exactly supported rows; 60 raw model items
were fully accounted as 34 aligned public items and 26 private withheld items.
The output used at most 1,894 tokens. No grammar, truncation, hash, or
control-character failure occurred.

Hive array 21286197 then passed the complete 1,787-window stress pilot. All four
tasks exited zero; the exact 867 complete parents and all expected route counts
were present. Every window stopped normally, strict-parsed, and passed both the
raw model and public schema gates. The maximum response was 18,867 of 24,576
tokens. The canonicalizer processed 22 literal U+0009 tabs in five outputs,
with zero invalid outside-string or unrepaired controls. The strict importer
accounted for 29,842 raw model items as 26,954 aligned public items and 2,888
private withheld items. It recovered all 11 known omissions through ordinary
same-model records and exact parent-span overlap; semantic confirmation of those
links remains part of the human-only gate.

The full 35,213-window production rerun started as Hive array 21296223 in a new
output directory. A rolling audit cancelled it after 1,200 rows. One biography
window used only 993 input tokens but reached the full 24,576-token output cap.
The model had generated 13 complete facts, reached a straight quotation mark in
a fourteenth evidence span, and then emitted 23,268 grammar-permitted TAB
characters outside the JSON string. The other 1,199 completed rows stopped and
parsed normally. This is structured-decoder whitespace degeneration, not an
input-size or record-density problem. The 20 completed chunks and four logs are
frozen in
`quality_reports/candidate_extraction_v2_1_full_whitespace_failed_2026-08-24.json`.

Do not increase the token limit, resume the job, or patch this candidate. Keep
the prompts, schemas, display, and source offsets unchanged. Disable arbitrary
JSON whitespace in the engine-level xgrammar configuration and bind the setting
to the generation hash. Rerun the failing window first. Then run a complete-
parent stress pilot containing the prior 1,787-window pilot plus every parent
with a straight-quote segment before restarting the 35,213 windows in another
new directory. The extension remains nonpublishable.

The now-superseded structured-whitespace pilot first used generation configuration version
`candidate_extraction_generation_v2`, SHA-256
`bd16d688235686f9162337daabc0aac8eda20b132ea01f52d5fbff2c31cfc01e`.
It requires the xgrammar backend with arbitrary structural whitespace disabled.
Every output row and run-metadata object must carry the same two top-level
attestations, and downstream import, packaging, and handcoding tools reject a
missing or weaker value. Under that pilot, the extraction-contract SHA-256 was
`1a5da28b31a2ed7fb35ac970181f776a83aa03ac709c1ac772e0928f55a2a71a`;
the three route schema hashes were unchanged. The exact failing-window
preflight and the complete-parent 7,838-window stress-pilot inputs were
exported locally. The one-window preflight completed on Hive on 2026-08-24. It
stopped normally, strict-parsed, passed the schema, and passed exact support.
The raw output contained 180 colons or commas and exactly 180 required
separator spaces, with no other structural whitespace. The earlier failed
window therefore passed under the engine-level restriction.

Hive job 21304738 then ran the complete-parent 7,838-window pilot. Slurm and all
eight tasks exited zero, but the release gate failed. Exactly 7,837 windows
stopped normally, strict-parsed, and passed the model and public schemas. One
issue window (`7bdfc2a16483fb4225e35bc14b9d84583ad33d2e2fe413a0059b93d2be303315`)
used only 1,104 input tokens but reached the 24,576-token output cap. It emitted
33 non-loop records, then 299 complete copies of one K Street record, and began
another copy before truncation. The output's fixed structural spacing was
valid. Across the pilot, none of 630,617 decoded model string fields contained
a straight quote even though the sources contained 66,616 straight quotes.
This establishes a quote-copy defect rather than another structural-whitespace
failure. The exact audit is frozen in
`quality_reports/candidate_extraction_v2_1_quote_repetition_pilot_failed_2026-08-24.json`.

The replacement contract displays U+0022 as `\u0022`; strict JSON decoding
restores the original source character before exact source alignment. It sets
route array caps of 128 issue records, 96 biography records, and 256 endorsement
records. A count at or above the cap fails before alignment. A separate
route-wide guard canonicalizes model items and fails any window in which the
same exact item appears at least three times, including nonconsecutive cycles.
The importer recomputes the strict parsed object, its hash, and both guard
reports from the raw generation. Guard failures retain that parsed payload and
provenance only in the private bundle; they never enter alignment, release, or
human-validation inputs. The release and handcoding gates require zero guard
failures and exact guard identities, hashes, and route caps.
Nullable free-text schema nodes omit `minLength` because the pinned xgrammar
version suppressed quote escapes when nullable strings used that keyword; the
unchanged Python Unicode-whitespace validator still rejects empty or whitespace-
only non-null strings.

The frozen suffix-o extraction-contract SHA-256 is
`675dae0474ee7f77a4125e02d7847c759706afe9859d17fbfd1dc6819091a9f7`,
and the generation-configuration SHA-256 is
`15ed8ee1f0ffe0c0605ba353efffea08fb2e883cd9468915c5d3ae7ffc1d00d6`.
The issue, biography, and endorsement schema SHA-256 values are respectively
`c5b9922e9447cc5c573bd3eeefa432710a0657e37c484c46e30dc0e83a41c719`,
`ee197f260e3e2242d65ac05644d6cba72c2c2e3114fe127c579be6070ab3ffd7`,
and `267901032a0bebbca582446d0df81ea508493bd970c51c061a4cd31ebf6ec3c6`.
The route-array-cap specification SHA-256 is
`d5c09cd2a12821248ad579aac6980866632a6fd96df17efb598c65080d5ad150`.
An exact-tokenizer census applied the pinned tokenizer to the complete system
prompt and user message for all 35,213 windows, including carried endorsement
context. Zero windows exceed the 32,768-token model context after reserving
24,576 output tokens and a 500-token safety allowance. The smallest remaining
margin is 1,311 tokens; the suffix-o endorsement system prompt uses 988 tokens.
The reproducible result is frozen in
`quality_reports/candidate_extraction_v2_1_context_budget_census_suffix_o_2026-08-25.json`.
The 43-row suffix-n preflight (the frozen artifacts labeled `20260824n`),
Hive job 21424457, superseded the 19-row test.
Every row passed the deterministic technical gates, and the revised quote and
repetition controls passed. The preflight nevertheless returned empty arrays
for endorsement windows `aa0b99eb`, `e56c1b40`, and `2e9a0cd8`, even though
they contain bare-name lists under exact endorsement context. The first row had
previously produced 225 exact records under the same fixed-whitespace model
family without endorsement `maxItems`; the second had previously produced 72
records before the fixed-whitespace contract. The third is the next segment of
the first row's complete parent and contains additional names beyond their
300-character overlap. This is a recall failure, not a valid measured zero.
The immutable audit is
`quality_reports/candidate_extraction_v2_1_quote_cap_preflight_failed_21424457_2026-08-25.json`.

The approved suffix-o correction (the new contract-bound replacement labeled
`20260824o`) removes `maxItems` only from the endorsement guided schema while
retaining the fail-closed 256-record post-generation cap, the exact-item
repetition threshold of three, and all strict validation gates. It also states
explicitly that exact `parent_document` context remains the controlling
endorsement context for bare names in the current continuation segment, even
though the carried heading lies outside that segment. The model must still copy
that context unchanged and may not extract a name absent from the current
segment. The issue and biography guided caps remain unchanged.

The suffix-o preflight contained the required 44 rows from 18 complete parents:
25 issue, 4 biography, and 15 endorsement rows. The input manifest stated an
impossible minimum of 87 model identities for `2e9a0cd8` while also excluding
the candidate. The source contains 86 eligible unique non-candidate identities.
Ed Wu and Tom Zhang each occur twice, so both exact anchors must remain
ambiguous and withheld. The corrected gate is therefore at least 86 valid
non-candidate model identities and 84 uniquely aligned identities after
excluding Greg Tanaka, clipped `ena Song`, and page-interface or footer text.
The output returned 1 model and 1 aligned identity, so it still fails. The first
monitor summary also counted 4 quote-bearing variants for `71c197e1`; the
literal predeclared gate counts resolved records, of which there are only 2, so
that gate fails its minimum of 3. The local 7,838-window suffix-o stress export
was not submitted. Stress and the 35,213-window production rerun remain blocked
until a new contract passes a fresh preflight.

The frozen suffix-p extraction-contract SHA-256 is
`82530ab0f4126e1640dcffbe48a66bffdfab1538fc40240b5f3e55ab0eed5574`,
and the generation-configuration SHA-256 is
`edfe53dd421acb30f1da4bc17404be4b5415a3d6060b77650c448d76edafc13f`.
The suffix-p endorsement prompt SHA-256 is
`6a369a82639adf1ab6c068863b1a07f2ab5856eb48ffd0d639240c46be837f27`;
the three route schema hashes are unchanged. The alignment, semantic,
endorsement-context, user-message-template, and inference-display
specification hashes are respectively
`c55aed337762f712c7a4edcf9dd109a94c881b936955cfa5e4d6ba8c114114e5`,
`0ef0dc40eda7cd1ebfb83f4042360c432b8876315a8966756b5e79ccc795c9a5`,
`b7689a181c3b9d323ee7dff10978b53d9f51e5e68d7ea709948a6529f483edb5`,
`ace05e7a19cc203b824506d41b5ebf0f5a3aacc39dd871cbbd11e9d97c8ee7d7`,
and
`92566223fd29db40a5b6031833a99992b58c4939d206ba8517d2309f9fb1724b`.
The importer requires a per-window `user_message_sha256` and independently
rebuilds the exact user message before accepting it. The private bundle retains
all display-anchor adjustment and endorsement-context projection reports. The
public segment table exposes complete-line boundary status and nullable local,
parent, and display-coordinate bounds, but no raw source text, model text, or
private reports.

The frozen suffix-p full source-manifest SHA-256 is
`f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c`.
Its endorsement complete-line census is 6,096 segments: 6,055 with a complete
line, 41 without one, 169 with a different first source/display boundary, and
988 with a different last boundary. Derived preflight and stress manifests
inherit that full-universe census. The importer therefore authenticates the
full-manifest hash and frozen census, then independently recomputes the subset
census from the selected rows. It does not compare a selected subset count to
the inherited full-universe total. The private bundle stores the upstream
full-manifest hash and selected-row census separately so their denominators are
not ambiguous downstream. The importer rejects a direct export unless its
manifest bytes match that frozen full-export hash and its corpus hash matches
the authoritative corrected corpus. The public packager and human-only sampler
are full-bundle gates: they require the exact full-export and corpus hashes and
reject any non-null upstream-subset reference.

The two-stage endorsement replacement uses line-ledger contract SHA-256
`ac8d6ed7f06e58a7961e08f8d50f7a86907f1bada12f1c70f5e9963011a335c2`,
line-inventory specification SHA-256
`b1d79e313b42a1d1e1826f6c7687dc5b513ade605f4033d1b18e8b85dbf34aa0`,
line-batch specification SHA-256
`33248f961b0d4929430cbc53d6eb36503885e61c225d787b56d9f645419f1d76`,
and prompt-token-count specification SHA-256
`587c65388f06ac5ca2e61bb81c053912d2204b6fb80e8b20a0051b4e14f3f755`.
Stage A uses prompt hash
`a5a592019dbc1aee631f11c6bbdd7b61f2d59fc173050fddc0d8b1cb1f7b1c4a`
and catalog hash
`bbb78a98b19a0119c6f20e32b95d6585dee2678bbbb2479bfd2c8b69a8b31d8f`
with target-count variants 1 through 16. Stage B uses prompt hash
`ddc90ae105620211907edc553848c717e807bbd74aa0a6a762129eb3c904c95f`
and catalog hash
`913011667e4625f87f8bceb0f618e0c6068fe8bdd4cbca7867e1edb88b7304ad`
with target-count variants 1 through 8. Every batch, runner row, restart check,
metadata file, and manifest must bind its catalog, selected variant ID and
hash, and exact real-target count.

The private bundle stores exact Stage-A and Stage-B manifests, batches,
outputs, decisions, target blocks, alignment reports, and withheld records. The
importer independently rebuilds both stages from the authenticated full source
and accepts public endorsements only from Stage-B `aligned_records`. Every
public record carries the line inventory ID, source line ID, both batch IDs and
slots, model-record index, carrier-alignment model-item ID, inventory-owner
segment, record-evidence carrier segment, target-block alignment method and
match count, and exact record-evidence parent bounds. Raw output, parsed model
output, decisions, blocks, alignment reports, and withheld records remain
private.

One endorsement document or source segment can feed several ledger batches.
Its singular one-stage prompt, schema, generation, user-message-template,
token, and alignment columns are therefore null in the public document and
segment tables. The endorsement `extraction_contract_sha256` is the frozen
line-ledger contract hash. Issues and biography retain their exact one-stage
values. Shared model, revision, runtime, source, URL, context, and text-free
boundary provenance remain populated.

A later code audit found that downstream validation trusted the declared
engine setting without verifying its observable raw-output form. The separate
validator `xgrammar_fixed_separator_json_whitespace_v1`, specification SHA-256
`4a03ebdf798ceab3f841b7359d59ad17f34033c2f6e677edd74551935ffd8774`,
now lexes every raw output before canonicalization and strict parsing. Outside
quoted strings, every colon and comma must be followed by exactly one U+0020;
spaces elsewhere and structural tabs, line feeds, or carriage returns fail for
a normally stopped output. Non-stop output retains its finish-reason failure
status because truncation can cut off a required separator; its raw violation
is still counted in the private aggregate.
The importer recomputes the result from raw bytes, including for the already
completed preflight, so no old runner attestation is required. The private
bundle records aggregate pass and violation counts, and the release packager
and human-only gate require zero invalid outputs. This is a validation rule for
the already frozen engine setting. It does not change the generation SHA-256,
extraction-contract SHA-256, route schemas, or canonicalizer specification.

The biography release contains six fields: education, military service,
occupation, local roots, elected-office experience, and public-service
experience. It excludes 15,650 family records and 9,899 residual other records
from the first extraction. This approved rule removes 28.8 percent of the first
biography output. The internal audit retains the excluded rows.

Occupation and office or service records require current, former, or unclear
timing. A local-roots record must retain the stated relationship and timing. It
must not imply a current residence from a historical statement.

Issue records retain an exact source heading, a claim, exact evidence, and a
statement kind. The statement kind is support, oppose, proposal, past action,
current action, or unclear. The release does not add an issue topic or ideology.
For overlapping issue variants, exactly equal claims and statement kinds may
resolve across different exact evidence boundaries; a valid exact heading is
preferred as provenance. Any claim or statement-kind disagreement is withheld
as a private conflict.

Every overlap conflict remains fully accounted in the private bundle and no
variant from that group enters a public long table. An accounted conflict does
not fail the technical gate. A route with another released record can remain
`observed_positive`; a conflict-only route is `withheld_validation`, never
`observed_zero`. The public status, document, and segment tables report exact
conflict-group counts.

Endorsement records separate a name, entity type, category, title, affiliation,
and jurisdiction. They also separate record evidence from endorsement context.
Context can be exact text in the segment, an exact structural heading carried
from the parent document, or a classifier-qualified route URL when no controlled
literal heading exists. Only the URL fallback permits null context text and
offsets. The public data does not contain the first extraction's generic
organization field.

## Step 3: build the extension package

The release packager consumes frozen Hive output. It never runs inference. It
must stop when the source corpus hash differs from
`c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3`.

The extension adds six data files:

1. `candidate_extraction_status.csv`, with 45,123 candidate-year by route rows;
2. `candidate_extraction_documents.parquet`, with 23,871 parent documents;
3. `candidate_extraction_segments.parquet`, with 35,213 inference windows;
4. `candidate_issue_statements.parquet`;
5. `candidate_biography_facts.parquet`; and
6. `candidate_endorsements.parquet`.

The document and segment tables retain source URLs, capture bounds, hashes,
offsets, and model provenance. They do not duplicate the released raw text. The
long tables link to both source levels. Endorsement document and segment rows
null singular one-stage prompt, schema, generation, user-template, token, and
alignment fields; issue and biography rows retain their exact values. Every
public endorsement includes the 14 required Stage-A, Stage-B, line-owner,
record-carrier, target-block, and parent-offset lineage fields.

The validator must enforce exact source coverage, unique keys, valid schemas,
exact evidence spans, valid status values, and complete manifest hashes. It must
also enforce all approved privacy and field-scope rules.

## Step 4: run the human-only release test

After the extraction package is complete and frozen, build keyboard-first
handcoding applications. Do not generate apps or assignments before that
boundary. The earlier 630-task design is too large for the assigned human
coder. Based on the recent 20-organization coding pilot, plan for at most 240
tasks in four resumable batches of 60. Start with one 60-task batch and record
active coding time before assigning the remaining batches.

The provisional 240-task allocation is 60 precision records per route and 20
complete parent pages per route. Biography precision includes 10 records for
each released field. Issue precision remains stratified by year, party, text
length, segmentation, heading availability, and statement kind. Endorsement
precision covers every observed entity type, category, support mode, and
context source scope. Precision and recall samples use disjoint parent
documents, and all 11 known-omission link confirmations remain separate
required tasks. Freeze the exact allocation only after the first batch provides
an observed minutes-per-task estimate; do not expand the assignment above 240
without a new workload decision.

Before scoring, reload the unchanged frozen bundle and deterministically replay
selection from the recorded seed, tuning exclusions, and batch size. Require
typed-exact agreement for the selection summary, ordered sample metadata, and
every task payload. Rebuild every HTML app with the recorded shared-builder and
template identities and require byte-exact agreement with the app used by the
coder. A handcoding manifest and task files that agree only with their own
recomputed hashes are not valid release-gate inputs.

The extension can pass only when:

- every deterministic schema, source, hash, and evidence check passes;
- each route has at least 90 percent complete record accuracy;
- each released subtype or category has at least 80 percent accuracy;
- each route has at least 90 percent complete-page recall; and
- every sampled endorsement has valid combined record and context support.

The first AI-assisted review is a tuning sample. It is not the final release
test. A failed route requires a new run and a new untouched human sample. The
extension waits until all three routes pass.

## Publication boundary

The core correction uses Dataverse version 2.1. The extraction extension uses a
later major Dataverse version on the same DOI. The agent can create and verify an
unpublished draft under this plan. The agent cannot publish a version or post an
announcement without a separate explicit approval.
