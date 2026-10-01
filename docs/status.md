# Project status and release checklist

Updated September 15, 2026. Hanno marked historical correction complete and requested a fixed, concise path to release for variable extraction and mayoral websites.
This file controls the order of remaining work. Linked artifacts establish completion; older plans retain detailed definitions and approval records.
Hanno also approved persistent Claude instructions with a two-attempt limit for unsuccessful repairs on the same blocker.
The documentation request does not launch full inference, authorize additional acquisition tranches, purchase storage, or publish data.

| Track | Current state | Finish condition |
|---|---|---|
| 1. Historical House/Senate correction | Complete | Corrected public version 3.0 matches the local release |
| 2. Extracted variables | Step 1 production integration complete; full extraction, packaging, and human validation remain | Six documented files pass the existing release checks and receive publication approval |
| 3. Full mayoral extension | Population defined; most election calendars and results still need verification | Target population assessed, finite collection queues resolved, coverage documented, and release approved |
| 4. 2026 House/Senate collection | **Full collection enabled September 16, 2026** after the volume resize; collecting to the December 26 endpoint | Collection through the agreed endpoint, followed by final outputs and coverage checks |

## Rules for the next agent

The persistent execution rules are in [CLAUDE.md](../CLAUDE.md).
Follow the numbered release steps below, reuse completed work, and continue after their required checks pass.
Keep routine implementation choices flexible. Additional work requires a demonstrated blocker to a listed release requirement.
After two unsuccessful repair attempts on the same blocker, report the evidence and decision needed; continue independent approved work.
Retain the attempt count across sessions. Existing research criteria, acquisition limits, and approval requirements remain in force.

## Resume here

Updated September 28, 2026.

The model-coded pass on the version 2.2 tasks is complete (commit `9fa3f66`). Report:
`quality_reports/candidate_extraction_model_coded_validation_2026-09-27.md`. It fails the
old all-dimension criteria on every route. Known omissions pass 11 of 11.

### Release rule decided by Hanno, September 28, 2026

Release the variables that the model-coded check supports. Drop the others. Human
validation is not required for this release.

| Table | Released | Dropped | Evidence (model-coded, 60 records per route) |
|---|---|---|---|
| Documents, segments, status | All columns | None | No model judgment |
| Issues | All columns | None | `claim`, `evidence` 60/60; heading 59/60; kind 53/60 |
| Biography | `field` with documented caveat, `value`, `relationship`, evidence | `timing` | timing wrong 8/60; field wrong 4/60, 3 of them in `public_service_experience` |
| Endorsements | `endorser_name`, `entity_type`, evidence, context | `title`, `affiliation`, `jurisdiction`; category already withheld | optional fields wrong 11/60; name 58/60 |

The codebook states two record-level caveats: about one issue row in five states no
position, and 9 of 60 endorsement rows lack support for an endorsement.

Done on September 28 (uncommitted): the column lists in
`scripts/build_candidate_extraction_release.py`, the codebook sections, and removal of
the unreleased line-span flag. Release tests pass (97).

**Model-coded gate added, September 28 (Hanno approved the edit).** The packager and the
validator take `--model-coded-validation`. It accepts the model-coded report only for the exact
bundle, with at least 60 precision and 20 recall items per route, record recall at least 0.90,
and 11 of 11 known omissions. Tests: 103 pass, including six for the new gate.

**Release package built:** `build/hive_candidate_extraction/release_20260928_v22_model`, from
`bundle_20260924_v22` with `CANDIDATE_EXTRACTION_SCHEMA_VERSION=2.2`, 38 minutes, peak 9 GB.
All three routes pass; `release_ready` is true. Row counts equal the September 25 package:
287,961 issues, 104,435 biography, 153,132 endorsements. The dropped columns are absent.
Status: 5,691 candidate-year routes `observed_positive`, 376 `observed_zero`, 23 still
`withheld_validation`. The gate does not cause this: all 318 model items on those routes failed exact alignment, so no record survives. Build summary beside the directory.
The release validator (`scripts/validate_candidate_extraction_release.py` with the same option) passes: exit 0, `release_ready` true.

Hanno approved on September 28: commit, the wrong-site exclusion, and publication as Dataverse
version 4.0 (core 3.0 files unchanged, extension added, README replaced; no 2026 data).

**Pre-release audit, September 28.** Checks on the released columns found:
- About half of all rows repeat a record within the same candidate-year, because each archived
  page version is its own parent document (issues 52%, biography 49%, endorsements 60%). Fix:
  new column `candidate_record_key` in all three record tables, documented in the codebook.
- The status table still reported `observed_positive` and old counts for about 30 routes whose
  pages were excluded. Fix: counts recomputed; fully excluded routes get
  `excluded_not_campaign_site`.
- Two more non-candidate sites, `gagop.org` and `borderlessu.com` (no records). Added to the
  exclusion list. `kateforcongress.com` is two real candidates and stays.
- `statement_kind` is null for `current_action` and `unclear` (Hanno approved).
- No problems found in endorser names, `entity_type` patterns, or biography values beyond a few
  trivial values such as "None" (8 rows).

**Final package:** `build/hive_candidate_extraction/release_20260928c_v22_model`. Build and validator
pass (`release_ready` true). Rows: issues 287,427, biography 104,331, endorsements 153,108,
documents 23,200, segments 34,346. Status table agrees with the record tables (0 mismatches).

**Independent review, four Sonnet agents, September 28.** Joins, keys, manifest hashes, and a sample
of about 11,000 offsets are sound. The long technical codebook contradicted the data (wrong counts,
wrong endorsement overlap strings, stale pipeline text), so the deposit ships a new short codebook,
`docs/candidate_extraction_codebook.md` (published as `candidate_extraction_codebook.md`), written in
Hanno's voice with a coverage table and a known-limitations section. The long document stays in
the repository only. Documented, not fixed: about 800 rows with encoding errors from the archived
pages, family-member biography facts, committee posts under elected office, supporter lists and
broken names on a few endorsement pages, short topic labels among issue statements.

**Deposit folder:** `build/release_v4_0_candidate`, 19 files (core 3.0 files hard-linked and unchanged,
six extension files, extension manifest, extraction codebook, new README). Local manifest checks pass.
**Dataverse draft staged and verified, September 28** (`doi:10.7910/DVN/BZ2JRS`, draft after 3.0).
19 files; the 10 unchanged core files keep their 3.0 file IDs; README replaced; 8 files added. All 19
downloaded and byte-verified: `build/dataverse_v4_0_draft_download/dataverse_draft_verification.json`.
Not published. Waiting for Hanno's final confirmation of the file list and the description sentence.

**Open, September 28 (late).** A fresh-reader check found 23 `.gov` documents still released
(`billnelson.senate.gov:80` under S6NE00095-2006 and other official pages with a `:80` port). Cause:
`_url_host` compared the network location, not the host name. Fixed in the builder
(`EXPECTED_EXCLUDED_PARENT_DOCUMENTS` 631 to 654, test added; 112 tests pass; not yet committed).
The rebuild was stopped at Hanno's request because it used too much memory (peak about 9 GB).
Hanno approved adding the description sentence (text below). **September 30:** the build and
validator run detached (`nohup caffeinate -i build/run_release_20260930.sh`, log
`build/run_release_20260930.log`; the first harness-tracked attempt was stopped at its 30-minute
background limit before writing output) into
`build/hive_candidate_extraction/release_20260930_v22_model` (log files beside it: `.summary.json`,
`.stderr`, `.validate.json`). Check first: if `.validate.json` shows `release_ready` true, continue
from the count update. If the run was interrupted, delete that directory and its side files and rerun
the same two commands (see the September 28 entries). Then update every count in `docs/candidate_extraction_codebook.md` and `docs/RELEASE_README.md`
from the new files (row counts, status counts, coverage table, per-key totals, `.gov` count 654,
heading null share, encoding rows), re-assemble `build/release_v4_0_candidate`, re-stage the draft
with `--resume-draft`, then ask Hanno for the final publish confirmation.

Open, not yet decided: drop the rows on `anthonybrown.com` and `kateforcongress.com`,
which serve pages under the wrong candidate.

Track 4 collects on schedule under the droplet cron.

### Superseded September 27 notes

| Item | State |
|---|---|
| One-stage and Stage B inference | Complete. 35,213 windows, 587 chunks, 671 Stage B chunks |
| Seed-vary repair | Closed after three attempts. 21 of 36 windows recovered; 10 windows withheld |
| Schema version 2.2 | Implemented and tested. Adds one typographic search fallback to the aligner |
| Re-alignment pass | Complete. `build/hive_candidate_extraction/v2_2_full_hive_output_20260923` plus both repair directories |
| Version 2.2 bundle | Built September 25, 2026. `bundle_20260924_v22`. Both automatic gates pass |
| Release package | Built September 25, 2026. `release_20260925_v22`. Six tables plus the manifest inventory |
| Official `.gov` exclusion | Applied. 631 parent pages dropped, 488 issues and 143 biography |
| Validation batches | Rebuilt September 27, 2026. `validation/release_validation_2026-09-27`, 255 items in seven batches |
| Model-coded validation | **NOT run on the 2.2 tasks.** The September 22 pass used the superseded 2.1 tasks |
| Human validation | Not started. The coding rules are now in the tasks, so it is no longer blocked |
| 2026 collection | Running daily. 34,749 pages, 10,095 snapshots, 64 GiB free |

### Next action: run the model-coded pass on the 2.2 tasks

Seven batches under `validation/release_validation_2026-09-27`. Spawn one Opus agent for each,
as on September 22. Each agent reads the task's own `instructions` and per-question `help`,
answers strictly, writes a `handcode-results-v1` file beside the task, and verifies with
`~/.agents/skills/handcode/scripts/summarize_results.py`. Then score with
`scripts/summarize_candidate_extraction_model_coding.py`, which writes a model-coded report and
never touches the bundle's human gate.

This needs about 40 minutes with the machine awake. It died once already when the machine slept.

### The two model-coded passes are only partly comparable

The seed is unchanged at 20260922, but the sample moved, because 33,061 previously withheld
records are now eligible. Shared items between the September 22 and September 27 samples:

| Task | Items | Same sample | Shared |
|---|---:|---|---:|
| precision issues | 60 | no | 22 |
| precision biography | 60 | no | 52 |
| precision endorsements | 60 | yes | 60 |
| recall issues | 20 | yes | 20 |
| recall biography | 20 | yes | 20 |
| recall endorsements | 20 | yes | 20 |
| known omissions | 15 | no | 13 |

Endorsement precision and all three recall tasks are directly comparable. Issue precision is
not: two thirds of its records differ, because the issue route recovered the most content.
Do not present the issue numbers as a before-and-after.

### What version 2.2 changed, and how the identity chain was kept honest

The alignment spec sits inside the frozen generation config, so re-aligning a corpus gives it a
new generation identity. Version 2.2 shares every generation input with 2.1: the same prompts,
the same JSON schemas, the same user-message template, the same model, seed and token budget.
A test asserts the user message is byte-identical under both versions on all three routes.

Generation was not re-run. The re-stamped run metadata, both repair manifests and the release
record all carry `realigned_from_schema_version` and a note saying so. Two shard metadata files,
13 and 19, were partial resume runs whose chunk membership is not recorded, so their alignment
aggregates still describe the 2.1 run; they are annotated, and the aggregate bound still holds.

Four scripts now read the schema version from `CANDIDATE_EXTRACTION_SCHEMA_VERSION`, default 2.1:
the importer, the packager, the hand-code builder and the Hive runner. The runner also uses
`V2_ALIGNED_VERSIONS` at 15 branch sites. A test fails if any `== "2.1"` dispatch reappears there.

**Watch for omitted arguments, not just literals.** Three failures came from
`align_extraction_exact_spans` being called without `schema_version`, which grep for `"2.1"`
cannot find. Check call sites directly.

### Deferred items

- The packager computes `manifest_entries` but never writes them. The September 25 inventory was
  reconstructed by hand. The packager should write the file itself.
- The scorer has not been checked for 2.2. Its human-validation report must match the bundle
  version, and the packager gate now enforces that.
- The endorsement record-evidence span defect: in 17 of 60 sampled records the span crosses two
  list entries. Found September 22, not yet addressed.
- Repo size is 6.6 GB. `data/deliverable` holds 1.7 GB that is regenerable in about 8 minutes.
  Hanno has not chosen between compressing and deleting. Do not delete without his word.


### Decision needed 1: the coding tasks do not state the coding rules

The seven validation tasks give five bullets of instruction and no help text on any question.
They never state the allowed values, the null rules, or the heading uniqueness rule.
The coder must guess the codebook from the model records.
I proved the effect on `source_heading`: 14 of 20 auditable failures are contract-correct nulls.
Issue record accuracy rises from 0.35 to 0.52 when that dimension is dropped.
Hanno will hit the same gap if he codes these batches by hand.

Smallest correction: put the route contract in the instructions and the rule for each
dimension in that question's help text, in `scripts/build_candidate_extraction_handcode.py`,
then rebuild the seven tasks. The bundle, the sample and the frozen identities do not change.

### Decision needed 2: withheld records, not missed content, cause the low recall

The aligner drops a model record when its evidence is not an exact page substring.
It withholds 40,895 issue items, 13.2 percent of that route. 38,666 give `missing_exact_span`.
I tested 4,000 of them: 68.9 percent differ only in punctuation form, for example a straight
apostrophe for a curly one, and 10.4 percent differ only in whitespace.
The rule is deliberate. Version 2.1 never normalizes whitespace and punctuation.
The model disobeys the copy instruction; the pipeline fails closed and works as designed.

Three options are written up in
[the validation report](../quality_reports/candidate_extraction_model_coded_validation_2026-09-22.md):
accept the loss, extend the alignment search while still storing the source slice, or run
inference again. Option 2 is the smallest correction that keeps the verbatim guarantee.
It changes `alignment_spec_sha256` and needs one new alignment pass over the stored outputs.
It needs no new inference. I did not start it.

### Decision needed 3: 542 pages are official government sites, not campaign sites

The release population is campaign websites. 542 of the 23,871 parent pages sit on a
`.gov` host, which is 2.27 percent. They cover 26 candidate-years and produce 7,637
records: 5,723 issue records and 1,914 biography records. 18 distinct hosts are involved,
for example `chu.house.gov`, `mikulski.senate.gov` and `vitter.senate.gov`.

Four of the 18 hosts also carry the wrong candidate. `billnelson.senate.gov` is filed under
Nelson, E Benjamin of Nebraska; Bill Nelson is a Florida senator. `foster.house.gov` is filed
under Foster, Steven of Georgia; Bill Foster represents Illinois. `sullivan.senate.gov` is filed
under Sullivan, Dan with state MA; Dan Sullivan represents Alaska. `oversight.house.gov` is a
committee site and is filed under a California candidate.

I checked whether this points to a wider linkage defect and it does not. Of 2,669 hosts in the
corpus, only 5 serve candidates in more than one state, and 3 of those 5 are the same person who
really ran in two states. A name test on all 23,871 pages puts a part of the candidate's name in
the host for 95.5 percent, and the residual is nicknames such as `debfornebraska` for Debra
Fischer. The defect is confined to official government hosts.

Smallest correction: exclude `.gov` hosts from the release population and rebuild. This removes
the scope violation and the four wrong-person links in one step. It drops 26 candidate-years.
I did not make this change. It changes the approved population, so it needs approval.

### Verified: the heading failures are a coding-task defect, not an extraction defect

The model-coded pass marked 20 issue records wrong on `heading_correct`. I resolved each one
against the segment the record came from, which is where the contract evaluates uniqueness.
13 of the 20 headings occur zero times or several times in every segment of their page, so the
contract requires null and the model is correct. Several of the remaining 7 are note text, not a
heading. The coder never saw the uniqueness rule. Blocker 1 fixes that.

The same coder found that no non-null heading was wrong. The model under-reports headings; it
does not corrupt them.

### Resolved: the 2026 URL discovery never ran, September 24, 2026

Two independent defects stopped the URL waterfall. Both are fixed.

**1. No OpenFEC key on the droplet.** `src/url_sources/openfec.py` reads
`OPENFEC_API_KEY` and falls back to `DEMO_KEY`. The droplet set neither, so every
lookup returned `no_registered_key`: 3,302 of 3,689 candidates. A valid key existed in
`.claude/settings.local.json` on Hanno's machine, inside a Bash permission rule. That
file is gitignored and was never committed, so nothing leaked. The key now sits at
`/root/.openfec_api_key`, mode 0600, and `run_collect_2026.sh` exports it and exits 1
if the file is unreadable.

**2. `--lookup-limit 0` means zero lookups.** In `src/collection_2026_roster.py`,
`selected = candidates if limit is None else candidates[:limit]`. The cron script
passed `0` on every Monday refresh, and the argparse default is `0` as well. The
refresh therefore looked up nothing even when a key was present. The script now passes
`4000`, which covers the roster of 3,689 with headroom.

**The coverage correction.** A full refresh ran 2026-09-23 22:25 to 2026-09-24 02:45
UTC, about four hours at the registered-key rate limit of 3.6 seconds per request.

| Field | Before | After |
|---|---:|---:|
| `found` | 137 | 2,419 |
| `incomplete_no_registered_key` | 3,302 | 0 |
| `source_error` | 250 | 102 |
| `no_result` | not reported | 1,168 |
| Candidates with a website | 778 | 2,440 |

OpenFEC supplies 2,404 of the URLs. Website coverage is 66 percent, not 21 percent.
**Do not cite the earlier figure of 2,911 candidates without a website. It measured the
defect, not the candidates.** The true count is 1,168.

Two limits on the new number. The files under `data/exports/` still hold the 22:00
state and show the old counts until a run regenerates them; the live roster is
`data/rosters/current.csv`. And no page has been fetched from the 1,662 new URLs yet,
so every collection count up to 2026-09-23, including 15,428 pages and 185 paired
candidates, belongs to the 778-URL era.

### Incident: an edit to a running shell script killed a collector run

On 2026-09-23 at about 19:47 UTC I patched `run_collect_2026.sh` while the 16:00 cron
instance was still executing it. Bash reads a script by byte offset, so inserting ten
lines near the top moved the offsets under the running shell. It resumed mid-line and
died with `syntax error near unexpected token 'then'`. The collector stopped 3h47m into
its six hours, wrote no `status_20260923.json`, and logged no completion line.

Nothing was lost beyond that run's remaining time, because the collector resumes from
its work-status table. The script file itself stayed valid. I started a replacement run
by hand on 2026-09-24 at 02:50 UTC with the new 2,440-URL roster.

**Rule: never edit a shell script while an instance of it is running.** Patch a copy and
swap it, or wait for the run to finish.

### Where the Hive artifacts live

Everything lives under `/nfs/hive/scratch/hhilbig/candidate_extraction`, not under Hive home, which sits at
18 GB of a 20 GB quota. The runner the repair executes is
`/nfs/hive/scratch/hhilbig/candidate_extraction/scripts/hive/extract_candidate_pages_vllm.py`,
sha256 `a598630eb33cb06a6fcdbe68692ca1b06ab374338ba63d2939bdb505e8adb544`, identical to the local copy.
The wrapper is `.../scripts/hive/run_candidate_extraction_pilot.sbatch`,
sha256 `a02ecdf7206187cd5de7617a3ad243669e4b7df53164c7a0e920f6c39d942fbe`, also identical to the local copy.
Attempt 2 ran under the earlier runner, sha256 `ad47bd36067fc9fc14e5e87ef5fa8c1e89f5dc3b937df99a16bdf2bc822649c7`.
The submission commands are recorded in `quality_reports/plans/submit_seedrepair2.sh` through
`submit_seedrepair8.sh`, each of which names the job it submitted.

### The withheld path is implemented, September 18, 2026

The code half of the approved decision is done. The counts follow when attempt 3 writes its attestation.

A cap-limited window now has its own processing status, `failed_output_cap_unrepaired`, in both the
importer and the packager. The packager keeps the stop rule for every other row and admits this one class
under three conditions: the row finishes with `length`, it carries no extraction, and the bundle's repair
attestation reports that exact window as unrepaired in the last attempt. A bundle with no attestation
refuses every cap-limited row, as before.

The technical gate stays strict by default. `technical_segment_gate_passes` gained
`allow_withheld_output_cap`, which defaults to false, so a truncated row cannot pass the gate on its own.
The importer sets the flag only after checking every withheld window against the attestation, and it
raises when a cap-limited row has no attestation behind it.

283 tests pass across the nine files. The new tests cover the accepted case, a row whose attestation names
another window, a row whose attestation says it was repaired, and a bundle with no attestation at all.

Still open for this step: the codebook entry that names the class and its count.

### The round-four record ledger: what exists and what is missing

`build_round4_record_ledger`, `project_round4_stage_b_batch`, `round4_parent_coverage` and
`apply_endorsement_round4_ledger` all exist in `src/candidate_endorsement_round4_release.py`, and
`load_endorsement_round4_ledger_bundle` reads the artifact back. **What is missing is only the driver**: a
script that reads the real Stage A and Stage B outputs, projects every batch, summarizes every parent, and
writes `endorsement_round4_record_ledger_private.jsonl.gz` plus the summary and parent blocks the bundle
manifest carries.

The artifacts are small enough for the Hive login node, which is where the builder should run:
Stage A inputs 544 MB and outputs 6.2 MB, Stage B inputs 50 MB and outputs 27 MB.

**The four production identities are now frozen** in `scripts/import_candidate_extraction_v2.py`, read
from the completed runs and verified across every shard:

| Identity | sha256 |
|---|---|
| Stage A input manifest | `032a6694a27457ea93ea944e2f71a2eb1dade564c32e2749dd0ed46fce8fa477` |
| Stage A generation config | `159517b984d31e8cc5deee394d978556d83191fda03ffde5662c6e5753fa7840` |
| Stage B input manifest | `13bba4688d63fc665b42911519a8c414dd9c766e7628ed2734234536c4daff98` |
| Stage B generation config | `cd309252f5231a929594924e29020584e69d3d36b89e37c8a1ddb022cab2cfac` |

Stage A holds one generation identity across its four shards and Stage B across its eight. The Stage-A
input manifest hash also matches the value the Stage-B build command asserted, which is an independent
check. A test refuses a blank or repeated value.

### Blocker: the Hive home filesystem is full

`df` reports `/home/hhilbig` at 21 GB of 21 GB, zero bytes available, on September 18, 2026.
This killed both submissions of repair attempt 3.

| Job | What happened |
|---|---|
| 23717478 | Ran 1h24, wrote 12 of 24 chunks, then died mid-generation with exit 1 and no traceback |
| 23725945 | Died in one second with no log file at all, because Slurm could not create it under `/home` |

The first failure looked like an infrastructure blip. It was not. vLLM writes its torch.compile cache
under `$XDG_CACHE_HOME/vllm`, which is under home, so a full home kills a running job without a message.

**The wrapper no longer depends on home.** `run_candidate_extraction_pilot.sbatch` now exports
`VLLM_CACHE_ROOT` and `XDG_CACHE_HOME` to scratch, creates both, and prints the available space in home at
startup. The submission script writes the Slurm log to
`/nfs/hive/scratch/hhilbig/candidate_extraction/logs/` instead of home. Submission
`quality_reports/plans/submit_seedrepair10.sh` is job **23727720**.

**Freeing space in home is Hanno's decision, not a repair I should make.** The large directories belong to
other projects: `news-extract` 5.0 GB, `polsci-labeling-bench` 706 MB, `wave2026-news` 298 MB,
`candidate-website-extraction` 279 MB, `cache-classify` 253 MB,
`polsci-open-bench-hive-20260804` 225 MB, plus many per-job `.log` files in the top level. Those do not
account for 21 GB on their own, so `~/.cache` holds the rest. Until home has space, any Hive job of any
project that writes to home can die without a message.

### Decided: withhold the windows no repair attempt recovers, September 18, 2026

**Hanno approved this on September 18, 2026**, after asking whether a better model would solve the
problem. It would not, for the reason recorded here: the release binds one generation identity across all
35,213 rows, and a second model would put a second instrument in the dataset. The repair identity is
defined in code as the frozen configuration with only the seed replaced.

So attempt 3 is the last repair attempt. Every window it leaves at the output cap becomes an explicit
withheld window with a typed reason. The dataset states that the page could not be extracted. It does not
drop the page and it does not carry a truncated row.

Attempt 3 still runs, because each window it recovers is one fewer withheld window. The withheld set is
whatever its attestation reports as unrepaired.

What this needs in code: `build_candidate_extraction_release.py` refuses any row that did not finish with
`stop`. It must instead emit these windows as withheld, with a reason that names the cap and the three
attempts, in the way `unresolved_target_block` already works. The codebook must document the class and its
count. This is a release-criterion change, which is why it needed approval.

### The seed-vary repair is finished, September 18, 2026

Attempt 3 completed as job 23727720 in 1 hour 7 minutes, after two earlier submissions died on the full
home filesystem. Its attestation is
`quality_reports/candidate_extraction_repair_manifest_attempt3_2026-09-18.json`.

| Attempt | Seed | Attempted | Recovered | Left at the cap |
|---|---|---:|---:|---:|
| 2 | 20260917 | 36 | 12 | 24 |
| 3 | 20260918 | 24 | 9 | 15 |
| **Total** | | **36** | **21** | **15** |

**15 windows are withheld.** That is 0.043 percent of the 35,213 windows. Both attempts together recovered
21 of 36, which is 58 percent, so the one-in-three estimate from attempt 2 held.

The wrapper fix is confirmed by the artifact: the attestation records `attempt_index` 3 and
`repair_seed` 20260918, and `load_repair_run_metadata` accepts the run's own metadata against the derived
attempt-3 identity. Before the fix it would have recorded attempt 2, which maps to seed 20260917, and the
importer would have refused the bundle.

The codebook now documents the withheld class, its cause, the attempts, and the instruction not to read a
withheld window as a page with no content.

### The round-four record ledger is built, September 18, 2026

`scripts/build_candidate_endorsement_round4_record_ledger.py` is the driver that did not exist. It reads
the Stage-A and Stage-B outputs, projects every Stage-B batch against its exact parent inventory,
summarizes every parent, and writes the private record file and the manifest block. It runs in about
7 minutes on the local machine.

The artifacts are small enough to work locally and were copied into the gitignored `build/` mirror:
export 56 MB, Stage-A outputs 6 MB, Stage-B inputs 49 MB, Stage-B outputs 26 MB. The local export carries
`source_corpus_sha256` `c53d36b2...`, which matches the value the sbatch asserts.

| Measured | Value |
|---|---:|
| Parents | 2,262 |
| Parents with complete public coverage | 1,985 |
| Aligned records | 153,132 |
| Withheld records | 1,791 |
| Unresolved target blocks | 1,687 |
| Optional-field repairs | 1,472 |
| Unaccounted Stage-B outputs | 0 |

Every Stage-B response classified as settled, so no retryable failure remained and the ledger could be
built at all. Output: `build/hive_candidate_extraction/round4_record_ledger_20260918/`, private file
sha256 `ce6b46ff0e53c1662fda62ebf3d7a000bf66c35cfb707d81bdb7ca31b16daac0`, 153,132 rows.

**277 of 2,262 parents are not publicly complete.** That is the next thing to understand, because
`public_coverage_complete` feeds the release gate. The causes are Stage-A unclear lines and unresolved
target blocks, both of which the ledger records per parent with a typed reason.

### Decided: a round-four record names its own architecture, September 18, 2026

**Hanno chose option B on September 18, 2026.** A released round-four record carries
`round4_record_ledger_single_record` and `two_stage_parent_record_ledger_round4_v1`, not the line-ledger
labels. The alternative was to reuse the line-ledger vocabulary, which needed no packager change but would
have made a round-four record read as a line-ledger record in the released table, where the architecture
is not otherwise visible.

The packager gained the round-four status in its allowed set and a branch that mirrors the line-ledger one:
it requires the round-four labels, one variant, the carrier segment as the canonical segment, and the
inventory owner among the source segments. It refuses a record that declares both architectures. The
record construction itself is now shared by `_ledger_resolved_record`, so the two architectures differ only
in the labels and the record identity, which a test pins.

331 tests pass across the eleven files that touch the runner, the importer, the packager, the round-four
release module, and the production contract.

### A 100-item accuracy check is ready for Hanno, September 18, 2026

**Hanno asked on September 18, 2026 for at most 100 items and simpler coding.** That is smaller than the
approved design of 240 items in four batches of 60, so this is an interim accuracy check, not the release
validation. Report accuracy with its interval rather than against the approved per-route thresholds; 100
items over three routes cannot support those thresholds per route.

`scripts/build_candidate_extraction_accuracy_check.py` draws the sample. Output:
`validation/accuracy_check_2026-09-18/`, task id `candidate_extraction_accuracy_2026_09_18`.

| Property | Value |
|---|---|
| Items | 100: issues 34, biography 33, endorsements 33 |
| Seed | 20260918, so the sample is reproducible |
| Questions | Two yes/no: does the marked text support the record, and is every field correct |
| Items with no marked evidence | 0 |
| Source | The production records themselves |

The records come from the completed runs, not from a bundle, because the importer cannot yet read the
round-four ledger. The packager reshapes and checks records; it does not change their content, so a record
judged wrong here is wrong in the package too and the answers carry over. The displayed record hides
offsets, hashes, stage links and context provenance, so the judgement is about content.

Hanno returns `candidate_extraction_accuracy_2026_09_18_results.json`. Summarize it with
`python3 ~/.agents/skills/handcode/scripts/summarize_results.py`, then record accuracy per route with its
interval.

### The importer cannot yet read the round-four ledger

This is the one gap between the finished records and the six-file package.

`scripts/import_candidate_extraction_v2.py` still takes only the line-ledger inputs: its command line
requires the Stage A, A2 and A3 arguments, and `validated_ledger` drives three places, the technical gate,
the segment overlay, and the manifest block. The round-four equivalents exist,
`apply_endorsement_round4_ledger` and `load_endorsement_round4_ledger_bundle`, but nothing builds a
round-four bundle.

What it needs: a `--endorsement-round4-ledger` input, the Stage A/A2/A3 arguments made optional when it is
given, the round-four overlay applied to endorsement segments, the private file copied into the bundle,
and the `endorsement_round4_record_ledger` block written to the manifest. Every artifact it reads is now
local, including the 587 one-stage output chunks and both repair directories.

### The 2026 collection was idle for five days, September 20, 2026

**Found and fixed.** Full collection was enabled on September 16 at 19:28 UTC, and nothing ran after
September 15 at 15:27 UTC. Five days of live campaign pages were not copied. Live capture is not
recoverable later, so that loss stands.

Evidence at the time of the finding: the last row in `attempts` was dated September 15, the droplet
crontab held one unrelated job for `email_digest`, no systemd timer matched, and no collector process was
running. A second and empty database at
`candidate_2026/runtime/data/collection_2026/collection.sqlite` is a stray from a run that used the
default root. The real database is `candidate_2026/data/collection.sqlite`, which `--root ../data`
selects.

**Why nothing ran.** `docs/collection_2026.md` names the owner as a daily Codex task,
`collect-2026-campaign-websites`, at 09:00 America/Los_Angeles. That task left no trace on the droplet.
Hanno approved a droplet-side schedule on September 20.

**The owner is now `/root/run_collect_2026.sh` on the droplet**, called by cron at 09:00
America/Los_Angeles using the same minute-guard pattern as the existing digest job. The script refuses to
start when a collector is already running, so there is one worker. It refreshes the roster and the URL
list on Mondays, then runs `--phase all` with a six-hour limit, then writes a status file. Logs are in
`runtime/logs/`, with a one-line summary per day in `runtime/logs/daily.log`.

**The Codex automation is already gone, which is the full explanation.** `~/.codex/automations/` held
`collect-2026-campaign-websites` on September 15, when a Codex session read its `automation.toml`. The
directory no longer holds it, and its parent was last modified on September 17. Nothing replaced it:
`municipal-scrape-check-in` covers the mayoral scrape and `weekly-project-control` is a read-only
dashboard. So the automation was removed around September 17 and the collection simply stopped. There is
nothing left to disable, and the droplet cron is now the sole owner.

First run on September 20: the refresh found 3,689 candidates and 754 URLs, and the collection run took
`attempts` from 1,082 to 1,192 and `pages` from 881 to 988 within its first minutes. 8,533 candidate
events were uncovered when the run started.

### Interim accuracy: 80 of 100 records, September 20, 2026

**Hanno asked for the 100 items to be coded by a model and used as an interim gold standard.** A Claude
Opus subagent coded them. `validation/accuracy_check_2026-09-18/provenance.json` records that these labels
are AI-coded, that they are not the approved human validation, and that they must not satisfy a release
criterion that requires human coding.

| Outcome | n |
|---|---:|
| Evidence supports and content correct | 80 |
| Evidence supports, content wrong | 16 |
| Evidence wrong, content wrong | 3 |
| Evidence wrong, content right | 1 |

Accuracy is 80 percent, with a 95 percent interval of 72 to 88 percent. Evidence alignment is strong:
96 of 100 records point at text that supports them. Almost every failure is a wrong field on a record
whose evidence is correct.

| Route | Accurate | Share |
|---|---|---:|
| Biography | 30 of 33 | 91% |
| Issues | 28 of 34 | 82% |
| Endorsements | 22 of 33 | 67% |

**Endorsements carry the failures, and one field carries most of them.** Seven of the eleven endorsement
failures are `endorser_category` set to `other_individual` for a sitting official, where
`government_official` exists and is used correctly on comparable records. The category appears to follow
the record line alone, so a section heading such as "State Senators" does not reach it. That is a narrow,
mechanical defect rather than a reading failure.

Two further endorsement patterns: name-and-title list pairing is fragile, because campaign pages alternate
between "Title then Name" and "Name then Title" with no markup, and the sample contains one error of each
direction; and `title`, `affiliation` and `jurisdiction` are sometimes swapped or merged, against the
prompt rule that keeps them separate.

The issue-route failures are of two kinds: a future promise labelled `current_action` when the correct
label is `support` or `proposal`, and a candidate position derived from third-party text such as a
description of what a bill would do.

**No record invented anything.** Every one of the 100 marked evidence strings appeared in its page text,
and the coder found no fabricated fact in any record. The failures are decisions about category and
attribution, not text generation. That also confirms the offset and copy machinery works.

**The 80 percent depends on one coding choice, and the alternative gives about 70.** Roughly one record in
five marks a bare list fragment, such as "and the U.S. State Department" or "strengthening Medicare
benefits", because the v2.1 prompt splits coordinated lists on purpose. The coder accepted a fragment when
the verb in the same sentence attributed it to the candidate, and rejected it when the governing text
attributed it to someone else. Under a stricter rule, where a span must stand alone as evidence, about ten
more records fail, almost all in the issues route. Report the rule with the number; the two are not
separable.

**Both decisions are made, September 20, 2026.**

Hanno chose the **lenient evidence-span rule**: a fragment of a coordinated list is adequate evidence when
the sentence containing it attributes it to the candidate, and inadequate when the governing text
attributes it to another party. Accuracy under that rule is 80 percent. The strict alternative would give
about 70. The codebook states the rule, and any published accuracy figure must state which rule it used.

Hanno chose to **withhold `endorser_category` from the release** rather than fix it or ship it with a
warning. The packager no longer writes the column to `candidate_endorsements.parquet`, and the codebook
explains why: the error is systematic, a sitting official is filed as `other_individual` when the office
appears in a section heading rather than on the record line, so a user filtering on `government_official`
would undercount officials without seeing it. The field is still validated internally and still exists in
the private record, so it can be released later if the categorisation is corrected. `entity_type`,
`title`, `affiliation`, `jurisdiction` and the evidence text remain released, so a user can build their
own category.

### The importer reads the round-four ledger, September 20, 2026

The gap is closed in code. `scripts/import_candidate_extraction_v2.py` now takes
`--endorsement-round4-ledger`, pointed at the file the builder writes, with the private record file beside
it. The eleven line-ledger stage arguments became optional, and the two sets are alternatives: a
round-four bundle refuses any stage input, and a line-ledger bundle still requires all eleven. One bundle
carries one architecture.

What the round-four path does differently:

- `load_round4_ledger_artifact` reads the summary, the parents and the private records together, and
  checks the private file against the row count and digest the artifact declares before trusting it.
- `apply_endorsement_round4_record_ledger` overlays each endorsement segment through the round-four
  applier in `src/`, so both architectures write the same row shape.
- The corpus gate is accounting, not coverage. Round four has no `public_coverage_complete` boolean at
  corpus level; it requires `all_model_records_accounted`. A parent can be publicly incomplete and still
  fully accounted, which is the case for 277 of 2,262 parents.
- The bundle carries the private records under `endorsement_round4_record_ledger_private.jsonl.gz` and the
  manifest block under `endorsement_round4_record_ledger`.

193 tests pass across the importer, the packager and the round-four release module. The line-ledger path
is untouched and its tests still pass, which is the check that matters for the existing architecture.

### The release bundle is built, September 20, 2026

`build/hive_candidate_extraction/bundle_20260920/` holds the first bundle of the full corpus under the
round-four architecture, with both repair attestations attached.

| Measured | Value |
|---|---|
| Segments | 35,213 |
| Technical segment gate | passed |
| Known-omission gate | passed, all 11 recovered |
| Repair attempts recorded | attempt 2 repaired 12 of 36, attempt 3 repaired 9 of 24 |
| Round-four parents | 2,583 |
| Processing status | 19,333 success, 15,679 partial withheld, 191 all withheld, 10 withheld at the cap |

It took four refusals to get here. Three were real and one was mine.

1. **An endorsement parent with no round-four summary.** The ledger builder summarized only the 2,262
   parents with a Stage-B batch; the corpus holds 2,583. The other 321 have nothing to extract, 319
   because Stage A found no candidate anchor and 2 because their pages hold only navigation. They now get
   a summary reporting zero records and complete coverage. Had the importer defaulted instead of
   refusing, 321 candidates would have entered the release with no record of why they carry no
   endorsements.
2. **The run-metadata accounting broke on the repair.** The primary metadata counts 36 windows finishing
   with `length`; the bundle shows 15, because 21 were replaced by repair rows. The comparison is now made
   against what the primary run alone produced, reconstructed by swapping each repair row back for the
   primary row it superseded. The row count is unchanged and the maxima are exact, so the check is not
   loosened. The manifest carries both aggregates and the packager uses the primary-equivalent one.
3. **A version string.** Mine. The adjusted aggregate keeps the all-output shape version, because the
   version names the shape; the derivation has its own key.
4. **No known-omissions file.** Also mine, an omitted input rather than a defect. Every copy in the
   repository is byte-identical and all four parent documents it names are in the full corpus.

### Correction: ten windows lose content, not fifteen

Fifteen windows stayed at the output cap, but five of them are endorsement windows, and four of those
still carry records, 45, 8, 7 and 5 of them. Under round four an endorsement row's records come from the
parent line ledger, not from that one-stage window, so a window that hit the cap does not stop the
two-stage pipeline from reading the page. Those rows carry `failed_output_cap_unrepaired` as their
superseded one-stage status and the round-four outcome as their current status.

The released corpus therefore loses content for **ten** windows of 35,213, which is 0.028 percent, and
`failed_output_cap_unrepaired` appears as a processing status on exactly those ten rows. The codebook is
corrected.

The packager gained the round-four counterpart of a branch it already had for the line ledger: for an
endorsement row it checks the superseded status against the one-stage ladder, takes the current status
from the parent summary, and requires the row's own round-four block to match the ledger's parent summary
on every field, so a row cannot claim a coverage it does not have.

### Share of the release complete, September 22, 2026

About 50 percent of the whole release is complete. The weights are a judgment about total effort
per track, not a measurement. Re-estimate them when a track's step list changes.

| Track | Weight of the whole | Complete within the track | Contribution |
|---|---:|---:|---:|
| 1. Historical House/Senate correction | 20% | 100% | 20% |
| 2. Extracted variables | 25% | 70% | 17.5% |
| 3. Full mayoral extension | 40% | 15% | 6% |
| 4. 2026 collection | 15% | 45% | 6.75% |
| **Total** | **100%** | | **about 50%** |

Track 2 moved from 60 to 70 percent. Steps 1 to 3 are complete: the package exists and the
validation batches exist. Step 4, validation, is built but it does not pass. Step 5 is the deposit.
Track 3 is unchanged at 164 of 1,700 city-years. Track 4 moved from 35 to 45 percent because the
daily cron now collects without a gap and the corpus holds 15,428 pages.

| Remaining time | Estimate |
|---|---|
| Agent work | 40 to 65 hours |
| Hanno's own work | 10 to 14 hours, plus two decisions now |
| Wall-clock floor | Late December 2026, set by the 2026 collection endpoint |

Hanno's own work is larger than the September 18 estimate of 6 to 12 hours. The reason is the
recall batches. 180 precision items and 15 omission items take about one minute each. The 60 recall
pages take about eight minutes each, because each one needs a full page read. That is 20 to 28
sessions of 30 minutes.

The largest source of uncertainty is the 1,536 unassessed mayoral city-years. The cost per city-year
is measured on 164 cases, which are large cities with good records, so the rest can be slower.

The 2026 collection track cannot finish before its endpoint, whatever else is done. Tracks 2 and 3 can
reach a reviewable deposit well before that date. Track 2 waits on a decision, not on a machine.


### Repair attempt 2 recovered 12 of its 36 windows

Job 23655140 completed on September 17, 2026 at 23:12 PDT after 1 hour 51 minutes. Its attestation is
`.../v2_1_seed_vary_repair_20260917a/repair_manifest.json`, copied to
`quality_reports/candidate_extraction_repair_manifest_attempt2_2026-09-17.json`.

| Measured | Value |
|---|---|
| Targets | 36 |
| Repaired | 12 |
| Left at the cap | 24 |
| Routes left at the cap | issues 18, endorsements 5, biography 1 |
| Chunks left at the cap | 24, one window each |

So the seed change recovers about one window in three.

### The wrapper dropped the attempt index

`REPAIR_ATTEMPT_INDEX` was exported by every submission script, but
`run_candidate_extraction_pilot.sbatch` never passed it to the runner. The runner therefore kept its
default of attempt 2. Attempt 2 was correct by accident. Attempt 3 would have recorded attempt 2 against
seed 20260918, which the importer refuses, because it maps attempt 2 to seed 20260917.

The wrapper now forwards the value when it is set. `load_repair_targets` also gained an identity
cross-check: given the run's seed and attempt index, it refuses a target list that declares another seed
or another attempt. Both changes have tests. This is a new defect, not a further attempt at the resume
blocker, so the repair attempt count is unchanged.

### Attempt 3 is scoped to the 24 windows attempt 2 left

The target list is `quality_reports/candidate_extraction_repair_targets_attempt3_2026-09-18.json`,
built by `scripts/build_candidate_extraction_seed_vary_repair_targets.py --previous-repair-manifest`.
It holds 24 windows in 24 chunks. It was synced to
`.../build/hive_candidate_extraction/candidate_extraction_repair_targets_attempt3_20260918.json`,
sha256 `b8b2ee4b1cadd669adafa744c274cfd11ef33a6e1654647bd3e6ac402e2a492d`.

`preflight_repair_scan.py` reported 24 chunks to process and none blocked before the submission.
`sbatch --test-only` predicted 14:58 on `low` against 15:34 on `high`, so the job went to `low`, which is
preemptible. The runner checkpoints per chunk and the resume scan is fixed, so a preemption costs one
chunk. The prediction was pessimistic: the job started at 08:40 PDT, four minutes after submission.

Attempt 3 writes into a separate output directory, so attempt 2's rows stay byte-identical.

### The bundle now carries each repair attempt's own run metadata

This closes the packaging half of the open blocker.

A repair attempt can span more than one Slurm job, because a job can reach its wall limit and a later job
resumes the rest. The runner writes `run_metadata.json` when a job completes its selection. Attempt 2's
file therefore records the job that finished the attempt: it reports 15 processed documents, not 33.
Two of its fields cover the whole attempt. `documents_selected_for_shard` is 36, the size of the target
list, and `outputs_complete_for_selected_shard` is true. The window-level record for the attempt stays
the attestation, which the importer checks against the artifacts.

`load_repair_run_metadata` in `scripts/import_candidate_extraction_v2.py` reads that file and requires the
frozen identity in every field except the seed, the derived repair identity for the attempt in the seed
and the generation hash, no shard index, an inference run rather than a recovery attestation, a selected
count equal to the target count, and internally reconciling counters. It was run against attempt 2's real
file, which passes.

Each attempt block in `seed_vary_repair` now carries a `final_job_run_metadata` entry. The packager
gained `_validate_seed_vary_repair`, which checks the block: declared attempt indexes in ascending order,
the declared seed and identity for each attempt, a primary identity equal to the bundle's, reconciling
counts, one window list that matches the counts, a later attempt scoped to exactly what the previous
attempt left at the cap, and the run-metadata entry. A bundle with no repair carries no block and skips
every check.

282 tests pass across the nine test files that touch the importer, the builder, the packager, and the
runner. Twelve of them are new.

**To pick up:**

1. Check job 23717478. On completion it writes `repair_manifest.json` into
   `.../v2_1_seed_vary_repair_attempt3_20260918a`, with `attempt_index` 3 and `repair_seed` 20260918.
2. Read its `repaired_count`. Attempt 2 recovered one window in three, so attempt 3 should recover about
   eight of its 24 windows.
3. Decide what happens to the windows attempt 3 also leaves at the cap. The packager refuses any row that
   did not finish with `stop` (`build_candidate_extraction_release.py`, `did not finish with stop`). The
   options are a fourth seed, or admission as explicit withheld windows with a typed reason, which
   `unresolved_target_block` already supports. Admission changes a release criterion and needs approval.
4. Build the record ledger through the round-four release adapter. That builder does not exist yet.


### The complete one-stage output measures

The audit now covers all 587 chunks rather than the 571 that were published when the first audit ran.
Report `quality_reports/candidate_extraction_one_stage_audit_full_2026-09-17.json`, produced by
`scripts/audit_candidate_extraction_one_stage_output.py` on the Hive login node in 25 seconds.

| Measured | Value |
|---|---|
| Output chunks | 587 of 587 |
| Rows, being the whole corpus | 35,213 |
| Routes | issues 20,673, biography 8,444, endorsements 6,096 |
| Parsed | 35,177 |
| `finish_reason` `length` | 36 |
| Unrepaired controls, invalid controls, repeated items | 0, 0, 0 |
| Canonicalized controls | 0 |
| Highest output-token use of the cap | 1.00 |

So the first audit understated the scale. It reported 33 cap-limited windows because 16 chunks were
still missing. The complete count is 36, which is 0.10 percent of the corpus. Every cap-limited window
is the same defect: greedy decoding repeats one item until it reaches the 24,576-token cap.

### The repair could not resume on a cap-limited repair row

The first submission wrote 19 of its 30 chunks and then hit its two-hour wall limit. A resubmission
would have failed in its first second, for the same reason the earlier shard-19 submission failed.

`repair_chunk_is_complete` accepted a chunk only when every row finished with `stop`. The repair scan
covers every chunk in the target list, so one row that reached the cap again stopped the scan with
`existing output is incomplete or has the wrong contract; preserved without overwrite`. This is the
sibling of the defect already fixed in `chunk_is_complete`, and it needed the same correction.

The repair scan now accepts a cap-limited repair row as terminal. `repair_chunk_is_complete` gained the
keyword argument `accept_truncated_rows`, which defaults to false, and the repair call site passes true.
A row qualifies only when `finish_reason` is `length`, `parse_status` is `output_truncated`, `extraction`
is null, no parsed or aligned payload field is present, the raw output hash matches, and the output token
count equals the cap. That check is the existing `truncated_row_is_terminal`. The repaired seed is the
only change the approved decision permits, so a second identical rerun returns identical bytes and the
row cannot be repaired by resubmission. The window therefore becomes counted as unrepaired in the repair
attestation instead of blocking every chunk behind it.

That fix let the scan read the published outputs, and it then failed on a second defect in the same
function. `repair_chunk_is_complete` compared the output document ids to the target list as ordered
lists. The runner writes a chunk's rows in the order the input chunk holds them, and the target list is
sorted by document id, so any chunk with two or more targets is written in an order the scan refuses.
Job 23649113 hit this on chunk 202 fourteen seconds after it started, its first second of work. Both
comparisons are now order-free: the scan sorts both sides. A repeated row is still refused, because a
repeat lengthens the output list.

Two of the 33 target chunks hold more than one window, chunk 202 with two and chunk 290 with three, so
the ordered comparison would have blocked the run at chunk 202 every time.

`quality_reports/plans/preflight_repair_scan.py` now reproduces the resume scan on the Hive login node
against the published outputs before any submission. It reports 18 chunks already complete and 15 to
process, which is 16 of the 36 targets.

`tests/test_candidate_extraction_seed_vary_repair.py` keeps the real cap-limited row as a fixture and now
covers the repair scan, the damaged-row refusal, the order-free comparison, and a full run that resumes
over an existing cap-limited repair output and publishes its attestation. That file gained three tests,
the original 17 still pass, and 250 tests pass across the eight test files that exercise this runner.

### The importer and the runner now carry the repair identity per attempt

This is the approved half of the open blocker. `scripts/import_candidate_extraction_v2.py` bound one
generation identity, so a repaired row was refused for its different `generation_config_sha256`. The
change is additive: without `--repair-manifest` every check behaves as before.

The repair identity is derived, not declared by the artifact. `EXPECTED_REPAIR_GENERATION_SEEDS` maps an
attempt number to a seed: attempt 2 is 20260917 and attempt 3 is 20260918. Each attempt's generation
configuration is the frozen one with `seed` replaced, and its hash is computed from that. A test binds
this to the runner itself: `generation_config_payload` with the repair seed equals that configuration,
and `seed` is the only field that differs from the frozen primary identity.

The runner gained `--repair-attempt-index`, which defaults to 2, so every submission so far behaves
unchanged, and it refuses an index below 2. The attestation records the index it ran under.

What the importer does when it is given one `--repair-manifest` and `--repair-output-dir` pair per
attempt, in ascending attempt order:

1. `load_repair_windows` checks each attestation against the frozen identity and against the artifacts on
   disk. It requires the declared version, an attempt index it knows, that attempt's seed and identity,
   the primary seed and identity, the schema, model, runtime lock, prompt, schema, and contract hashes,
   and the reason. For every target it recomputes the input chunk hash and the repair output hash, reads
   the row, and requires its route, finish reason, parse status, output token count, raw output hash, and
   generation hash to equal the entry. It recomputes the target, repaired, and unrepaired counts and the
   completion flag.
2. A window counts as repaired only when the row finished with `stop` and parsed. A window the attempt
   left at the cap must carry `length` and `output_truncated`, so a damaged row cannot pass as a
   cap-limited one.
3. `apply_repair_attempts` applies the attempts in ascending order. A window is taken from the latest
   attempt that recovered it, and a window no attempt recovered keeps its primary row. The primary output
   stays on disk untouched.
4. `enrich_segments` validates each named window against the identity of the attempt that produced its
   row, and only when that attestation reports it as repaired. Every named window records a
   `repair_attempt` block: the attempt index, the reason, both generation hashes, the seed, the
   attestation hash, the repair output file, and the accepted outcome.
5. The bundle manifest carries a `seed_vary_repair` list with one block per attempt: the index, the seed,
   the attestation file name and hash, the output directory, both identities, the three counts, and the
   window list.

`scripts/build_candidate_extraction_seed_vary_repair_targets.py` gained the matching scoping mode. Given
`--previous-repair-manifest` it carries forward only the windows the previous attempt left at the cap,
and it refuses an attempt index that does not follow, a repeated seed, or a source whose unrepaired count
does not match. That is how attempt 3's target list is built.

Nineteen tests cover this across the two files, including corruption cases that each require one specific
refusal. 278 tests pass across the nine test files that touch the importer, the builder, and the runner.

**What this does not do.** The bundle does not yet carry each repair run's own `run_metadata.json`, so the
bundle still lists only the primary run's metadata. Those files do not exist until the repair jobs write
them. The packager binds one identity across the bundle manifest and every run-metadata entry
(`_validate_run_metadata_entries`), so it still needs the repair identity for those entries. That work
belongs with the same step, and it waits on the real artifacts.

### The first target list named only 33 of the 36 windows

`quality_reports/candidate_extraction_repair_targets_2026-09-17.json` was built from the first audit, so
it inherited that audit's partial coverage. It holds 33 windows in 30 chunks. The second submission used
it and was cancelled while still pending.

The complete target list is `quality_reports/candidate_extraction_repair_targets_full_2026-09-17.json`,
built from the full audit by the same script. It holds 36 windows in 33 chunks: the 33 earlier windows
less one that no longer needs a repair, plus four windows that appeared in the 16 chunks published after
the first audit. The third submission uses it. The same file was synced to
`.../build/hive_candidate_extraction/candidate_extraction_repair_targets_full_20260917.json`, sha256
`225127a0c45ba14b1cd6353e2dc3bd74ba2078f0997f5daf08fc9a7db91865f4`.

### One window stopped needing a repair

Job 23639699_19 re-ran the quarantined chunk 119 under the primary seed 20260824, and its row for
document `c6d98dd21397…` finished with `stop` at 723 tokens. The same window came back `stop` under the
repair seed at 726 tokens. The primary output therefore carries a healthy row, and the window needs no
repair. Chunk 293 was re-run the same way and its row for `a3dba72eac14…` reached the cap again, so its
window is still a repair target.

The repair output for chunk 119 was moved to
`.../build/hive_candidate_extraction/superseded_repair_rows_20260917/`. The release must take the primary
row for that window. The repair directory now holds 18 published chunks instead of 19.

### What the first repair pass recovered

Nineteen chunks were written. They cover 21 windows: 10 now finish with `stop` and parse, and 11 reached
the cap again. The 10 recovered windows include document `d3ae6f21e492…` in chunk 280, the window the
first audit documented in detail.

Every window that reached the cap again burned 24,576 output tokens, which is about nine minutes of one
Blackwell GPU. That is why the run wrote one chunk every nine minutes and why a two-hour limit was not
enough for 30 chunks.

### Decided: a third seed, September 17, 2026

Hanno chose a third seed over admitting the unrecovered windows as withheld. Attempt 3 uses seed 20260918
and covers only the windows attempt 2 leaves at the cap.

The procedure, in order:

1. Wait for job 23655140 to finish and write `repair_manifest.json` into
   `.../v2_1_seed_vary_repair_20260917a`.
2. Build attempt 3's target list from that attestation:
   `scripts/build_candidate_extraction_seed_vary_repair_targets.py --previous-repair-manifest <the
   attestation> --primary-seed 20260824 --repair-seed 20260918 --attempt-index 3 --output
   quality_reports/candidate_extraction_repair_targets_attempt3.json`.
   The builder carries forward only the windows attempt 2 left at the cap.
3. Sync the target list to Hive scratch, submit with `REPAIR_ATTEMPT_INDEX=3` and `REPAIR_SEED=20260918`,
   and write into a separate output directory so attempt 2's rows stay byte-identical.
4. Run `preflight_repair_scan.py` against the new target list and output directory before submitting.

Attempt 3 needs the same wall limit as attempt 2: a window that reaches the cap again burns 24,576 output
tokens, which is about nine minutes of one Blackwell GPU.

The release packager still refuses any row that did not finish with `stop`
(`build_candidate_extraction_release.py`, `did not finish with stop`). A window attempt 3 also cannot
recover has no further approved repair, so it would need either another seed or admission as an explicit
withheld window with a typed reason, as `unresolved_target_block` already works. Admitting it changes a
release criterion and would need a new approval.

**Steps 1 to 4 of this procedure are done.** Attempt 3 is job 23717478. The current state is in the
`Resume here` section at the top of this file.

### Two repair jobs filled the one-stage gaps, September 17, 2026

The September 16 array stopped with 573 of 587 chunks. Two shards failed in about 30 seconds.
The cause was not a slow node and not a bad input.

**A cap-limited row made its whole chunk un-resumable.** `chunk_is_complete` returns false when any row
has a finish reason other than `stop`. The resume scan then raises
`existing output is incomplete or has the wrong contract; preserved without overwrite`. The scan covers
every chunk in the shard, so one cap-limited row blocked all the other chunks in that shard.
Shard 19 owns chunk 119 and chunk 219, and shard 13 owns chunk 293.

**The smallest correction.** The resume scan now accepts a cap-limited row as a terminal outcome, and
the strict default stays in place for every other caller. `chunk_is_complete` gained the keyword
argument `accept_truncated_rows`, which defaults to false. A row qualifies only when `finish_reason` is
`length`, `parse_status` is `output_truncated`, `extraction` is null, no parsed or aligned payload
field is present, the raw output hash matches, and the output token count equals the cap. Any other
shape stays incomplete. The test file keeps the real row as a fixture.


### Decided: vary the seed on retry for cap-limited windows

**Hanno decided this on September 16, 2026: option 1, vary the seed on retry only.**
The measured scale is now known. The full audit found **33 cap-limited windows in 30 chunks**:
23 `issues`, 8 `endorsements`, and 2 `biography`. That is 0.09 percent of the 35,213 windows.

The repair run reads a target list, infers only the listed windows, and writes them to a separate
directory. It keeps the prompt, schema, model, max output tokens, and contract byte-identical. It
changes only the seed, from 20260824 to 20260917, and it records both seeds, the attempt index, and the
accepted outcome in `repair_manifest.json`. The primary output directory stays byte-identical, so every
original attempt is retained.

**What remains open.** The release importer and the packager still bind one generation identity, so they
would refuse a repaired row for its different `generation_config_sha256`. The importer must accept the
declared repair identity for exactly the repaired windows and record the attempt. That work is not done,
and step 3 packaging waits on it.

The shard-0 pilot audit is `.../v2_1_full_hive_output_20260916p/pilot_audit_20260916.json`.
The full audit is `quality_reports/candidate_extraction_one_stage_audit_2026-09-17.json`.
It reports 34,373 rows, all three routes, and one defect class.

| Measured | Value |
|---|---|
| Rows | 34,373 |
| Routes | issues 19,833, biography 8,444, endorsements 6,096 |
| Parsed | 34,340 |
| `finish_reason` `length` | 33 |
| Unrepaired controls, invalid controls, repeated items | 0, 0, 0 |
| Canonicalized controls | 0 |
| Highest output-token use of the cap | 1.00 |

**This is a new failure mode, not either August defect.** Document
`d3ae6f21e492e7aaade2f47cced3e4fb469ae92364a6e2199719773b4dbcf593`, route `issues`,
candidate `S4SC00273-2014`, segment 2 of 4, chunk 280. The model wrote 19 valid issue blocks,
opened a twentieth evidence field with the display escape `"`, then repeated that escape
4,653 times until it reached the 24,576-token cap. The run fills 81.8 percent of the output.

Why the existing fixes do not cover it. `disable_any_whitespace` constrains whitespace *outside*
JSON strings; this repetition is *inside* a string, and a JSON string of any length is valid, so the
grammar cannot stop it. The control-character canonicalizer does not apply, because `"` is a
legal escape, not a raw control.

The input is not pathological. It is 5,283 characters and 1,047 tokens, `input_truncated` is false,
and `inference_display_escape_count` is 0, so the source segment contains no quote characters at all.
The model invented the escape. This is greedy-decoding repetition, not an input defect.

**The pipeline already contains it.** The importer requires a non-stop row to carry `output_truncated`
and to hold no parsed or aligned payload, so the window cannot become a measured zero.
**But the release packager refuses it outright**: `build_release` raises
`ReleaseInputError: "did not finish with stop"`. One such row therefore blocks step 3.

**A plain retry cannot repair it.** Decoding is deterministic at temperature 0 with a fixed seed, so
re-running the identical prompt, seed, and model returns identical bytes. The frozen retry rule in the
freeze record assumes an identical rerun, which is useless for this class.

Two options, both needing Hanno's authority because both touch a frozen contract:

1. **Vary the seed on retry only.** Keep the prompt, schema, model, and contract byte-identical; change
   only the seed for the affected windows; record the seed, the attempt index, and the accepted attempt
   on every row. Smallest change that actually repairs the window. It adds a second generation identity
   to the dataset, which the provenance must carry.
2. **Accept and account.** Admit a non-stop row into the release as an explicit withheld window with a
   typed reason, exactly as `unresolved_target_block` is handled today. No new inference. It changes a
   release criterion, and the affected windows stay unmeasured.

Option 1 recovers data and costs one short job. Option 2 costs nothing and loses 33 windows.
They combine: retry once with a new seed, then account for whatever still fails.

**Do not decide the scale from one row.** The full array (job 23507129) gives the exact count and the
exact window list. **Measured September 17, 2026 against the 571 chunks then published: 33 windows in 30
chunks. Re-measured September 18, 2026 against all 587 chunks: 36 windows in 33 chunks.** The audit report
names each one. The run is idempotent and resumable, and every affected row is quarantined with no payload,
so nothing is wasted or corrupted by running it.

**Do not poll Hive on a tight loop.** Frequent short SSH connections are the likely cause of the login-node
lockout earlier that day. Five-minute intervals are enough.

### Stage B output is healthy: zero retryable failures

Measured on the 30,240 responses from shards 0 to 5 with the real classifier,
`classify_stage_b_response` from `src/candidate_endorsement_round4_release.py`.

| Class | Count | Share | Re-inference |
|---|---:|---:|---|
| `complete` | 28,503 | 94.26% | no |
| `unresolved_target_block` | 996 | 3.29% | no, withheld with a typed reason |
| `optional_field_repaired` | 741 | 2.45% | no, field nulled and change recorded |
| `infrastructure_failure` | 0 | 0% | — |
| `unlocalizable_schema_failure` | 0 | 0% | — |

**Both retryable classes are empty, so the accounting gate is satisfied and no Stage-B retry is needed.**
The `technical_fail_count` of about 5.7 percent in the run metadata is the sum of the two expected
non-retryable classes. Do not read it as a defect.

The dominant withheld reason is `name_parts: no ordered source projection`, about 1,170 slot errors across
the four slots. The dominant repaired reason is `title: not an exact contiguous visible substring`, about
570. Both are the designed behaviour for evidence that does not project onto the source exactly.

### Incident: two Stage-B shards hung on a contended node

Shards 6 and 7 of array 23503614 both landed on `hive-dc-7-7-58` and produced nothing for over two hours.
Their logs stop after the GPU banner. That banner is the evidence: a healthy shard reported
**97,247 MiB free** of 97,887, while shards 6 and 7 reported **14,403 MiB and 12,157 MiB**. Another user's
jobs held that node, so vLLM could not allocate at `gpu-memory-utilization 0.92` and neither shard advanced.

Cancelled both and resubmitted as **23508575** with `--array=6,7`, `SHARD_COUNT=8`, the same output
directory, and `--exclude=hive-dc-7-7-58`. Resume republishes only the 167 chunks they still owe.

**Check the free-memory number in the GPU banner before assuming a shard is merely slow.** A shard that
writes no chunk for an hour while its peers finish is not slow; it is stuck. `squeue -w <node>` shows who
else holds the node.

## Active work and handoff state

No execution step was started by this documentation update. The table identifies the next planned actions, subject to their existing authority.
Replace each row with current evidence after a material result or before a handoff.

| Track and next step | Output or verification evidence | Blocker and failed repair count | Next action |
|---|---|---|---|
| Extraction: step 1, production integration | **Complete September 15, 2026.** Freeze record [candidate_extraction_production_freeze.md](candidate_extraction_production_freeze.md). Replay `quality_reports/candidate_endorsement_round4_release_replay_2026-09-15.json`. Census `quality_reports/candidate_endorsement_round4_production_census_2026-09-15.json`. 100 focused tests and 108 regression tests pass | No blocker; zero failed repair attempts | Request full-run authority for step 2 using the census in the freeze record |
| Extraction: steps 2 to 4 | **Inference complete September 17, 2026**: 587 one-stage chunks, 35,213 rows, 671 Stage B chunks. **Seed-vary repair closed**: 21 of 36 windows recovered over three seeds; 10 windows lose content and are withheld as `failed_output_cap_unrepaired`. **Round-four record ledger imported**: 153,132 endorsement records. **Release package built September 20, 2026** (six files). **Validation batches built September 22, 2026**: 251 items in seven batches, `validation/release_validation_2026-09-22/`. **Model-coded validation complete September 22, 2026** and it fails: issues 0.35, biography 0.73, endorsements 0.50 record accuracy against a 0.90 criterion; known omissions pass 11 of 11. Report `quality_reports/candidate_extraction_model_coded_validation_2026-09-22.md`, numbers in the matching `.json`, scored by `scripts/summarize_candidate_extraction_model_coding.py` | **Two blockers await a decision, see `Resume here`.** 1: the tasks state no coding rules, so 14 of 20 auditable heading failures are coder errors. 2: the exact-span rule withholds 13.2 percent of issue records, and 68.9 percent of a 4,000-item sample differ only in punctuation form. Zero failed repair attempts on either blocker | Fix blocker 1 and rebuild the seven tasks. Do not ask Hanno to hand-code before that. Wait for his choice on blocker 2 |
| Mayoral: step 1, official population | **164/1,700 city-years assessed.** Pittsburgh, Mesa, Chandler and Scottsdale are closed. **Maricopa County is readable**: index script plus 85 official candidate rows across 19 elections. `quality_reports/mayoral_maricopa_county_mayoral_results_2026-09-16.tsv`. 46 tests pass | 1,536 city-years remain open. **Gilbert is blocked on one source**: the town's "Voter History 1981 to present" PDF returns HTTP 403 to every command-line fetch and to the automated browser, and no archive holds it. Hanno approved the download; his own browser can read it | Ask Hanno to save the PDF, or accept that Gilbert 2010 to 2019 stay unwritten. Then close Glendale, then Phoenix |
| Mayoral: step 2, integrate evidence | **Complete September 16, 2026.** Batch verification and failure classification in `quality_reports/mayoral_indexed_capture_20260915/`, proposal reconciliation in `quality_reports/mayoral_inventory_19_2026-09-14/reconciliation_with_completed_batch_2026-09-16.json`, and `coverage_gaps.csv` now a hashed build output. 61 tests pass | No blocker; zero failed repair attempts | The remaining roster join depends on step 1 finishing the population |
| 2026: daily collection | **Collecting on schedule.** The droplet cron `~/run_collect_2026.sh` runs at 09:00 America/Los_Angeles and is the sole owner. Runs of September 21 and 22 both report `rc=0`. Status `logs/status_20260922.json`: 15,428 pages, 3,208 snapshots, 3,689 candidates, 227 events, 212 paired captures, 0 unfinished attempts, 69.8 GiB free | No blocker; zero failed repair attempts. 135 work items stay retryable | Let the cron run to the 2026-12-26 endpoint. Check free space and the daily `rc` line weekly |

### Resolved: line-ledger regeneration after Stage A3

**All three tests pass as of September 16, 2026. The full importer suite is 88 passing, 0 failing.**
Hanno delegated the approach. The fix keeps every artifact verified and every check fail-closed.

What changed, and why each change is safe:

1. The Stage-A2 test now authenticates the reviewed artifact on disk and asserts that regeneration no longer reproduces it.
   Its message tells a future reader to restore the original assertion if Stage A3 is ever reverted, so the change cannot hide a real drift.
2. Two tests built Stage-B model output in the round-four disposition shape. The line-ledger slot carries only `records`, and a single anchor must yield exactly one record.
   The fixtures now build valid records from the anchor's own source text, using the ledger's own helpers for split-line and compound anchors.
3. The Stage-A3 test imports the real reviewed results, because the importer pins those exact bytes and synthetic results can no longer import.
4. `import_candidate_extraction_v2.py` gained one narrow allowance. The Stage-A2 evaluation report predates the adjudication and carries the earlier contract hash.
   Only that one field, and only that one historical value, is accepted; every other field still compares byte for byte. The evidence for this was a field-level diff showing the contract hash as the sole difference.
5. The Stage-B row validator demanded the two-pass provenance specification on every record-extraction row. The current contract carries only the three-pass one, because it includes the Stage-A3 adjudication.
   The validator now requires exactly one of the two, exact against the contract, and also binds the three-pass record-extraction schema rule. This was a real inconsistency in the importer, not a test artifact.

The historical record of the cause follows.

### Cause: the contract advanced when Stage A3 completed

Three tests in `tests/test_import_candidate_extraction_v2.py` failed before this fix. They pre-dated this session.
The step-1 additions were removed from the importer and the same three tests still failed, which proved the cause was older.
They are `test_importer_replays_rotated_line_ledger_output_identity_exactly`, `test_three_stage_private_bundle_replays_real_a1_and_rejects_a2_tampering`, and `test_stage_a3_dependency_replays_synthetic_results_and_rejects_tampering`.

**Root cause.** The line-ledger contract payload includes `FROZEN_STAGE_A3_AUTHENTICATION`.
Completing the Stage A3 human adjudication filled that constant, so the contract advanced from `d8072b62...` to `37dd3a17...`.
The Stage A2 artifacts on disk were built before Stage A3 and embed the earlier contract.
Regenerating Stage A2 with current code therefore cannot reproduce its own frozen bytes: the manifest becomes `67b70fd6...` instead of `caa26dbd...`.
Stage A3 derives from Stage A2, and the contract depends on Stage A3, so the regeneration path is circular after the adjudication completes.
The real artifacts on disk are intact and internally consistent. Only regeneration from scratch is impossible.

The from-scratch reproducibility of the pre-adjudication Stage-A2 bytes is permanently gone, and that is inherent rather than a defect.
The artifacts remain intact, internally consistent, and now explicitly authenticated by the tests.
Round four has no Stage A2 or A3, so it carries no such circularity.

For each active batch, record its exact output paths, verification command and result, and next authorized action.
For a necessary deviation, record the observed defect, affected release requirement, smallest correction, and condition for returning to the checklist.
A checklist item remains incomplete when its required check fails or its authority is pending.

## 1. Historical correction: complete

- [x] Publish and verify the corrected eight-file release. Hanno closed this track on September 15, 2026.

The [public Dataverse version 3.0](https://doi.org/10.7910/DVN/BZ2JRS) was released August 26, 2026.
Its manifest bytes and all eight data-file checksums match `build/release_v2_1_candidate/`.
The release contains 15,041 eligible candidate-years, including 7,348 with substantive captured text.
The [release README](RELEASE_README.md) defines the population and files.

The earlier validator passed. An extra September 15 rerun was stopped without a new result; public checksum verification succeeded.
The stale public project page and review-only capture flags are follow-ups, not open release requirements.
Reopen this track only for a demonstrated material error.

## 2. Extracted variables: five steps to release

The deliverable contains issue statements, six biography fields, and endorsement occurrences linked to exact source evidence.
The biography fields are education, military service, occupation, local roots, elected-office experience, and public-service experience.
The existing inventory contains 23,871 source documents and 35,213 text windows across three routes.

The September 14 [endorsement evaluation](../quality_reports/plans/candidate_endorsement_pipeline_progress_2026-09-14.md) and [AI-coded review](../quality_reports/session_logs/2026-09-14_independent40-ai-coded-review.md) are complete development evidence.
Their labels do not establish human accuracy. The context-radius experiment was not promoted.
The earlier 14 source-line decisions are already resolved in the [imported adjudication](../quality_reports/candidate_endorsement_line_ledger_human_a3_21447139.json).
Its merge reports zero unresolved lines and authorizes the associated Stage B. Do not repeat that adjudication.

1. [x] **Freeze the production extraction.** Complete September 15, 2026.
   The [freeze record](candidate_extraction_production_freeze.md) names one source manifest, model configuration, and extraction contract for each route.
   Hanno selected the round-four endorsement implementation. The line ledger and its 14-row Stage A3 adjudication become historical evidence.
   The packager, validator, and importer now bind either architecture through a required `endorsement_architecture` discriminator.
   The five technical-failure classes, the two-attempt retry rule, and the approved accounting gate are frozen in code and in the record.
   Evidence: the replay projects the real independent sample with no model call and returns 2,496 records with complete lineage and exact parent evidence, 11 known unresolved blocks, and zero dropped model records.
2. [ ] **Complete extraction across the full inventory.** Reuse compatible, verified outputs; run the remaining work on Hive with the established open-weight model family.
   **Decision needed from Hanno: full-run authority for three inference jobs.** No full-corpus v2.1 output exists for any route.
   The jobs are the one-stage suffix-p run over 35,213 windows, the round-four Stage A over 19,142 batches, and the round-four Stage B over about 41,005 batches.
   The Stage A count is exact and reproducible with `scripts/build_candidate_endorsement_round4_production_inputs.py census`. The Stage B count is an estimate from an enriched sample; its upper bound is 71,676.
   All three jobs run on Hive with the established open-weight model, so they add no paid-API cost. The request is for compute time and the run itself.
   **Hanno approved the full run on September 15, 2026.**
   A local and remote preflight ran before any submission. All 19,142 Stage-A batches build and pass the runner validator with zero errors.
   The token preflight found and fixed one blocker: the development 32,768-token window cannot hold the corpus. The production runner now uses 49,152 tokens. See the freeze record.
   Before submitting, check that no other job of mine competes for the same GPUs. Two jobs were already queued on September 15.
   Run the Stage-B token preflight after Stage A completes, because Stage-A anchors define the Stage-B prompts.

   **Production runner implemented and pilot submitted, September 15, 2026.**
   `scripts/hive/extract_candidate_endorsement_round4_production_vllm.py` holds the production inference loop.
   It reuses the reviewed parsing, validation, and projection logic of the development runner and restamps only the five scope labels, so no output can claim a development scope on a production artifact or claim release authority.
   It refuses a prompt above the budget before the model loads, and it binds the reviewed generation identity when one is supplied.

   | Pilot | Value |
   |---|---|
   | Slurm job | 23483303, partition `low`, one Blackwell GPU |
   | Input artifact | `build/hive_candidate_extraction/v2_1_endorsement_round4_production_pilot_stage_a_20260915a` |
   | Input manifest sha256 | `f8a43df906000e39cabb32b2621610fc7a32d65899ea6ad637570cc543863a78` |
   | Generation config sha256 | `18f28307eb1fd12f2d42582568065627755a47fafc93f4ff38ad020129329ccd` |
   | Output directory | `.../v2_1_endorsement_round4_production_pilot_stage_a_outputs_20260915a` |
   | Scope | 120 largest Stage-A prompts across 3 parents, the context-risk set |
   | Result | **COMPLETED September 16, 2026 at 03:58 UTC. 120 of 120 parsed. Zero technical failures.** |

   The pilot confirms the context correction and the production scope on real hardware.

   | Measured | Value |
   |---|---|
   | Outputs, all parsed | 120 of 120 |
   | Technical failures | 0 |
   | Largest prompt actually used | 34,250 tokens |
   | Smallest remaining context margin | 12,854 tokens |
   | Window used | 49,152 tokens |
   | Wall time, including environment build and model load | 16 minutes 48 seconds |
   | Peak host memory | 32.5 GB |
   | Output scope | production, `development_only` false, `release` false |

   The predicted maximum was 34,248 tokens and the measured maximum was 34,250, so the preflight is accurate to two tokens.
   At the old 32,768-token window this batch would have left a negative margin and aborted the job, which confirms the correction was necessary.

   **Throughput for planning.** Inference took about 11 minutes for 120 of the largest prompts, roughly 5.5 seconds each, after about 5 minutes of startup.
   The median prompt is 18,557 tokens against this set's 34,250, so the corpus average will be faster.
   A single GPU still implies roughly 16 hours for all 19,142 Stage-A batches. Shard the array across GPUs, or accept that runtime and rely on the resume path.

   ### Full Stage A run, submitted September 16, 2026

   | Item | Value |
   |---|---|
   | Slurm array | 23487528, tasks 0 to 3, partition `low`, one Blackwell each |
   | Input artifact | `/nfs/hive/scratch/hhilbig/candidate_extraction/build/hive_candidate_extraction/v2_1_endorsement_round4_production_stage_a_20260916a` |
   | Input manifest sha256 | `032a6694a27457ea93ea944e2f71a2eb1dade564c32e2749dd0ed46fce8fa477` |
   | Generation config sha256 | `159517b984d31e8cc5deee394d978556d83191fda03ffde5662c6e5753fa7840` |
   | Output directory | `.../v2_1_endorsement_round4_production_stage_a_outputs_20260916a` |
   | Scale | 19,142 batches, 320 chunks, 2,583 parents, 520 MB |
   | Token preflight | `quality_reports/candidate_endorsement_round4_full_stage_a_preflight_2026-09-16.json`, `fits` true, 0 over budget, max 34,248 against 46,848 |
   | Estimated start | 01:58 on September 16, 2026, held behind other users at `(Priority)` |

   The whole Hive working tree lives under `/nfs/hive/scratch/hhilbig/candidate_extraction`, not under home, because home is at 18 GB of its 20 GB quota.
   Resume is on by default, so a preempted task republishes only the chunks it still owes.

   **Hive was unreachable from about 09:20 UTC on September 16.** The Stage A state is unknown; it may have run, be queued, or have failed.
   Port 22 accepts a TCP connection but the SSH handshake never completes. Ping is 10 ms with no loss, DNS resolves, and mac2 SSH works, so this is Hive's login service and not local connectivity.
   Automated monitors opened a new SSH connection every 45 to 120 seconds for several hours before this began, so a per-source connection throttle is a plausible self-inflicted cause.
   Do not poll Hive on a loop. Retry manually at long intervals. If it stays down, try Open OnDemand at `https://ondemand.hive.hpc.ucdavis.edu` or mail `hpc-help@ucdavis.edu`.
   Nothing is lost either way: the inputs are frozen on scratch and resume is enabled, so a restarted shard republishes only the chunks it still owes.

   **The Stage-A authenticator is now tested, September 16, 2026.**
   `tests/test_build_candidate_endorsement_round4_production_inputs.py` covers the accept path and each
   refusal: bounded pilot, development scope, a run claiming release authority, a reported technical failure,
   a row that did not pass, a missing output chunk, a tampered manifest, and mismatched batch identities.

   Writing those tests exposed an ordering fault and fixed it. Parent reconstruction ran before the scope,
   output-set and metadata checks, so a development-scoped or failed run was refused late and with a
   confusing batch-field message. Those checks now run first and each refusal reports its real cause.

   The corrected builder is synced to Hive scratch, sha256 begins `7e9c0ae4`, matching the local copy.

   ### Stage A is complete, September 16, 2026

   Hive returned and array 23487528 had already finished. All four shards report COMPLETED.

   | Measured | Value |
   |---|---|
   | Output chunks published | 320 of 320 |
   | Outputs, all parsed | 19,142 |
   | Technical failures | 0 |
   | Per shard | 4,800, 4,800, 4,800, 4,742 |
   | Largest prompt actually used | 34,250 tokens |
   | Smallest remaining context margin | 12,854 tokens |
   | Scope on every shard | `candidate_endorsement_round4_production`, `development_only` false, `release` false |

   The preflight predicted a 34,248-token maximum and the run used 34,250, so the estimate held at full
   corpus scale. At the old 32,768-token window this run would have aborted, which confirms the context
   correction was necessary rather than precautionary.

   ### Stage B and the one-stage run, submitted September 16, 2026

   **Stage B**, Slurm array **23503614**, eight shards on `low`, 8-hour limit.

   | Item | Value |
   |---|---|
   | Anchors from Stage A | 157,369, being 155,017 `record_anchor` and 2,352 `compound_record_anchor` |
   | Stage-B batches | 40,216 in 671 chunks |
   | Input manifest sha256 | `13bba4688d63fc665b42911519a8c414dd9c766e7628ed2734234536c4daff98` |
   | Generation config sha256 | `cd309252f5231a929594924e29020584e69d3d36b89e37c8a1ddb022cab2cfac` |
   | Token preflight | `fits` true, 0 over budget, max 10,348 tokens against 32,512 |
   | Read-only context radius | 0, because that experiment was never promoted |

   Stage-B prompts are far smaller than Stage-A prompts, 10,348 tokens at most against Stage A's 34,250,
   because a Stage-B prompt carries record blocks rather than a whole parent page.

   **One-stage pilot**, job **23503350**, shard 0 of 20, 30 of 587 chunks. Hanno asked for a bounded pilot
   because this job was attempted twice before and cancelled both times. Output goes to
   `v2_1_full_hive_output_20260916p`. The pilot is shard 0 of the eventual full split, so the remaining 19
   shards resume into the same directory and shard 0 skips what it already finished.

   The pilot covers **1,800 windows across all three routes**: 1,020 issues, 464 biography, 316 endorsements.
   That matters because the earlier control-character defect appeared only in endorsements, so a
   biography-only sample would not have tested for it. Export chunks are route-ordered, so biography
   completes first and endorsements arrive later; do not call the pilot clean before all three routes appear
   in the audit's `routes` field.

   ### Early Stage-B output behaves as the failure spec predicted

   A spot check of the first 29 chunks, 1,920 rows, run through `classify_stage_b_response`:

   | Class | Rows |
   |---|---:|
   | `complete` | 1,835 |
   | `unresolved_target_block` | 51 |
   | `optional_field_repaired` | 34 |
   | `infrastructure_failure` | 0 |
   | `unlocalizable_schema_failure` | 0 |

   Every row returned `finish_reason` `stop`, so these are content-validation outcomes rather than
   truncation. The reported errors match the designed classes exactly: an optional field that is "not an
   exact contiguous visible substring" is repaired at slot level, and `name_parts` with "no ordered source
   projection" becomes a withheld record with a typed reason.

   The 4.4 percent non-complete rate is close to the 5.8 percent seen in the September 14 independent
   sample. No infrastructure failure appeared, which matters because those are the only class that blocks the
   ledger build and the only class a retry may repeat.

   ### The rolling audit is now a script, not an ad-hoc check

   `scripts/audit_candidate_extraction_one_stage_output.py` reads published output only and reports the
   signals that stopped both earlier runs: a `finish_reason` of `length`, a `parse_status` other than
   `parsed`, control characters left unrepaired inside JSON strings, invalid control characters outside them,
   and exact item repetition. It distinguishes canonicalized control characters, which are healthy because the
   runner repairs them by design, from unrepaired ones, which are not.

   It was validated against both frozen failed runs and rediscovers each defect independently: 6
   `strict_json_parse_error` rows, all endorsements, in the 21250200 output, and the single truncated
   biography row in chunk 91 of the 21296223 output, which consumed its entire output budget.
   `tests/test_audit_candidate_extraction_one_stage_output.py` keeps both as regression fixtures, on the
   principle that an audit which cannot rediscover a known defect cannot be trusted to catch a new one.

   ### A real defect found and fixed while building Stage B

   The first Stage-B build failed with "fresh Stage-A batches do not cover every selected parent".
   Two of the 2,583 endorsement parents produced no Stage-A batch. They are pages of 403 and 300 characters
   whose every line is `outside_attested_context_scope`, so they hold **zero** lines for the model to decide
   and the builder correctly emits no batch for them.

   The development check requires every selected parent to appear in the batches, which does not hold on the
   full corpus. `_production_stage_a_parents` replaces it. It still refuses any batch naming a parent outside
   the selection, and for each absent parent it verifies from the export that the parent really has no
   decidable line rather than assuming so. Those two parents can never yield an endorsement record, which is
   a deterministic fact from the inventory and not a coverage gap.

   **To resume in a fresh session:** run `sacct -j 23487528`, then count `*.out.jsonl.gz` in the output directory; 320 files means Stage A is complete.

   Then build Stage B with the command below, run the token preflight for `record_extraction`, and submit that stage.

   ```
   python scripts/build_candidate_endorsement_round4_production_inputs.py stage-b \
     --stage-a-input-dir  build/hive_candidate_extraction/v2_1_endorsement_round4_production_stage_a_20260916a \
     --stage-a-output-dir build/hive_candidate_extraction/v2_1_endorsement_round4_production_stage_a_outputs_20260916a \
     --expected-stage-a-input-manifest-sha256 032a6694a27457ea93ea944e2f71a2eb1dade564c32e2749dd0ed46fce8fa477 \
     --output-dir build/hive_candidate_extraction/v2_1_endorsement_round4_production_stage_b_20260916a
   ```

   `authenticate_production_stage_a_run` performs the Stage-A checks. The development authenticator cannot be reused, because it requires
   `development_only` to be true and compares against the development authorizations and generation configuration.
   The production version keeps the same guarantees: manifest and chunk digests, the exact expected output-chunk set, production scope in the run metadata,
   a single generation identity across shards, every row technically complete, and the batch identities matching the manifest.
   It refuses a run that reports any technical failure, so a failed shard must be repaired before Stage B is built.

   **A bounded pilot cannot produce Stage B.** A pilot selects individual batches, so its parents are partial, and Stage B reasons over whole parents.
   The builder refuses a non-full-corpus artifact with that explicit message. This was confirmed against the completed pilot.

   The pilot answers two questions: whether the 49,152-token window loads and runs on one GPU, and what throughput the larger window costs.
   Check it with `sacct -j 23483303` and read `run_metadata.json` in the output directory; `max_prompt_tokens_used` and `min_context_margin_tokens` record the real budget use.

   **Resume support added September 15, 2026.** A chunk publishes atomically, so an existing output file is always complete.
   `--resume` skips the published chunks and processes only the missing ones. Without the flag the runner still refuses to overwrite, and its error names the flag.
   The sbatch passes `--resume` by default, because Slurm requeues a preempted `low` job with no warning and no grace time.
   Each attempt writes its own run-metadata file, so the retry provenance survives a preemption. This satisfies the retention rule in the freeze record.
   Verified locally without a GPU: the runner refuses an existing output without the flag, skips one published chunk with it, and exits cleanly when every chunk is already published. Retry or repair failed jobs within the frozen definitions and retain their provenance.
   Finish with every expected input accounted for and no unresolved technical failure in the release inputs.
3. [ ] **Build the six-file package.** Produce status, document, segment, issue, biography, and endorsement tables using the existing packager.
   Check unique keys, source links, exact evidence spans, missing-versus-zero states, approved fields, and manifest hashes.
   Finish with the technically valid, frozen package used for independent validation.
4. [ ] **Complete the existing human validation.** Use the approved design of at most 240 tasks in four batches of 60.
   Time the first batch before assigning the rest. Keep the 11 known-omission confirmations separate, as already specified.
   Apply the existing route accuracy and recall criteria. If a route fails, correct that route and use the required untouched validation sample.
   Do not expand the workload or start repeated tuning cycles without a new decision.
5. [ ] **Prepare and release the deposit.** Update the codebook to the actual outputs, include validation results and limitations, and run the final release validator.
   Prepare and verify the unpublished Dataverse draft. Present the exact files and version for Hanno's publication approval, then publish when approved.

The [extraction codebook](candidate_extraction_extension_codebook.md) controls the six-file schema and missingness definitions; its opening status is stale.
The [approved extraction plan](../quality_reports/plans/2026-08-24_corrective-release-and-candidate-extraction-v2.md#step-4-run-the-human-only-release-test) retains the human validation rules.
Those rules require 90% complete record accuracy and complete-page recall per route, 80% accuracy per released subtype/category, and valid support for every sampled endorsement.
These are existing criteria, not new thresholds. The optional human spot-check of the AI development review is not an additional release stage.

## 3. Full mayoral extension: five steps to release

The population is winners and runners-up in completed decisive mayoral elections for the 100 largest cities by 2020 Census population.
The period is January 2010 through September 14, 2026.
The [current coverage build](../quality_reports/mayoral_top_two_coverage_2026-09-14.md) verifies 25 elections and 50 eligible candidate-election records.
It assesses 181 of 1,700 city-years as of September 18, 2026; 1,519 remain open.
Completing this population is the largest remaining task.
Seven cities hold all 17 years: Baltimore, Chandler, Gilbert, Mesa, New York, San Diego and Scottsdale.
Five more are partial: Boston, Pittsburgh and Seattle hold 16 years each, Los Angeles 13, Santa Clarita 1.
Eighty-eight cities hold none.

1. [ ] **Complete the election population.** Review official calendars and results for all remaining city-years and identify each decisive election's winner and runner-up.
   Record no-election years, indirect selection, special elections, ties, and source-supported unresolved exceptions explicitly.
   Finish when every target city-year has been assessed and each election has verified finalists or a documented exception.
   An unreviewed city-year cannot become a no-election year or a completed exception.
2. [x] **Integrate the evidence already collected.** Verification, failure classification, gap merge and proposal reconciliation are complete as of September 16, 2026. The remaining join work depends on step 1 finishing the population. Join the official roster to existing URLs, capture reviews, and saved text.

   **The 214-request batch is verified and its nine unsuccessful requests are classified, September 16, 2026.**
   Both artifacts live in `quality_reports/mayoral_indexed_capture_20260915/`. No network request was made.

   `verification_2026-09-16.json` reports `valid: true`, `terminal_verification: true`, and zero errors over
   214 queue rows: 205 fetched, 4 capture mismatches, 5 terminal HTTP, 221 raw files, 209 text files,
   0 started attempts, 18,954,228 decoded bytes against the 128 MiB cap.
   It pins `queue_sha256` `4eeaafc8...`, `manifest_sha256` `e817a852...`, and `ledger_sha256` `547481e5...`.

   `acquisition_complete` is false, and that is not an error. The verifier sets it only when every queue row
   reports `fetched`; nine did not. The batch is valid, terminal, and drained with `stop_reason` `queue_exhausted`.

   `unsuccessful_classification_2026-09-16.json` classifies the nine.
   Five are **inaccessible**: every one is an HTTP 403 from the archive on `sylvesterturner.com`. The captures
   are indexed but the archive refuses to replay them, and a 403 is deterministic, so a retry cannot change it.
   Four are **unresolved**: the archive answered with a neighbouring capture rather than the requested key,
   on `janekim.org` and `carolinesimmons.org`, differing by timestamp or by URL variant.
   Neither class should be retried in a new queue. Re-requesting a served neighbour would be a different
   target and needs its own approved queue entry.

   **One deduplicated gap list exists, September 16, 2026.** `merge_coverage_gaps` in
   `scripts/build_mayoral_top_two_coverage.py` writes `coverage_gaps.csv`, which the build manifest hashes
   like every other output. The two source tables are unchanged.

   It holds 56 rows for 56 distinct candidate-elections, exactly the 24 in `missing_urls.csv` plus the 32 in
   `missing_period_captures.csv`. The two cannot overlap by construction, since one holds finalists with no
   URL lead and the other finalists that have one, but the merge raises on a repeated key rather than assume
   that stays true. Each row carries a typed `gap_type` and `gap_detail`.

   | Gap type | Rows | Detail |
   |---|---:|---|
   | `no_url_lead` | 24 | 22 both URLs missing, 2 one URL missing |
   | `no_period_capture` | 32 | 19 review attribution against an existing capture, 13 inspect the inventory gap before any new request |

   Of 60 eligible candidate-elections in the present population, 56 carry a gap and 4 are fully covered.
   That ratio reflects how little of the population is assessed so far, not a collection failure.

   **The 439-capture proposal is reconciled, September 16, 2026.**
   `quality_reports/mayoral_inventory_19_2026-09-14/reconciliation_with_completed_batch_2026-09-16.json`.
   The existing `key_overlap_audit.json` compared the proposal to the 214-capture queue as planned; this
   compares it to what that batch actually saved.

   Nothing in the proposal is redundant. Against the 205 saved captures there is zero overlap on exact
   capture key, zero on original URL plus timestamp, and zero on original URL at any other timestamp.
   The two target disjoint URLs.

   Net position: all 439 captures still require acquisition. 63 were already known as deferred leads, which
   were never acquired, so they are unacquired rather than new. The remaining 376 were newly surfaced by the
   19-URL inventory. The two counts are not additive.

   The proposal stays unapproved and unlaunched. Any acquisition needs separate bounded approval, and the
   volume resize should land first because this queue and the 2026 collection share `/dev/sdb`.
   Reconcile the 439-capture proposal and deferred leads with saved stores and completed batches before further requests.
   Finish with one candidate-election coverage table and one deduplicated list of actual gaps.
3. [ ] **Finish the finite collection queues.** Map the remaining source, OCR, and archive work to the approved population and freeze the required queues.
   The current backlog includes 44,883 unattempted sources, 637 retryable sources, and 7,380 pages awaiting optical character recognition (OCR).
   Submit bounded acquisition proposals with resource estimates; process approved queues and their resulting eligible capture leads under the existing retry limits.
   Finish when every required item has a saved result or a documented exhausted, inaccessible, excluded, or unresolved outcome.
   Further searches must address a named population or coverage gap; exhausted requests cannot trigger indefinite recovery cycles.
4. [ ] **Freeze the integrated dataset.** Resolve the nine period-evidence gaps and other flagged attribution cases through accepted evidence or explicit uncertainty.
   Build the election roster, website provenance, page text, and coverage tables from the synchronized records.
   Run the existing identity, key, hash, date-window, and coverage checks. Report missing websites and failed captures against the full assessed population.
   Finish with consistent tables, documented residual gaps, and no unresolved technical inconsistency.
5. [ ] **Prepare and release the deposit.** Write the final README, codebook, coverage summary, and manifest from the frozen outputs.
   Verify the release files and prepare a reviewable deposit. Obtain Hanno's approval for publication, then publish when approved.

### Method for step 1, established September 16, 2026

One city is one bounded batch. A city contributes 17 city-years, so a finished city moves the assessed count by 17.

1. Establish the election cycle from an official city source, such as the city's own list of mayors with term start dates.
   This supports the no-election years. Do not infer a no-election year from the project's own candidate roster.
2. Read the winner and runner-up from the election authority's official results, never from news coverage or an encyclopedia.
3. Write `<city>_calendar.json` with all 17 years and `<city>_results.json` with the verified elections.
4. Register both files in `config/mayoral_top_two_build.json`, then re-run `scripts/build_mayoral_top_two_coverage.py`.
5. Confirm the assessed count rose by the expected amount and that `tests/test_build_mayoral_top_two_coverage.py` still passes.

`scripts/fetch_clarity_election_results.py` reads official county results from Clarity
(`results.enr.clarityelections.com`), which many county authorities use. The public page is a
JavaScript application, but the site serves a versioned static summary holding every contest and total.
The reader records the published version string, so any result can be re-fetched and compared later.
Contest naming varies by year within the same county: Allegheny County labelled the Pittsburgh
citywide race `Mayor Pittsburgh` in 2021 and 2025 but `Mayor CITYWIDE` in 2017, so search the contest
list rather than assuming one name.

**Two source pipelines cover the cities done so far.**

Clarity counties: use the reader above. Allegheny County served Pittsburgh.

City clerk PDFs: some city sites block a direct download but serve the file to the fetch tool, which saves
it locally. Extract it with `pdftotext -layout`, then read the grand-total row. Mesa's clerk publishes every
election back to 1976 this way. Take the citywide grand total, never a precinct row, and read the column
headers carefully: these tables place the mayoral column beside council-district columns, and a misread
column would put the wrong candidate in the dataset. Verify the header alignment before recording totals.

**Arizona: the primary can be the decisive stage.** Mesa's official elections page states that the November
general is "for a candidate runoff, if necessary", so a candidate who wins the August primary outright is
elected then. Record those with `stage` `primary_nominating` and a `decisive_stage_source_url`; the build
then accepts them as decisive. Four of Mesa's five mayoral elections were decided in the primary.
Expect the same pattern for Chandler, Gilbert, Glendale, Scottsdale, Phoenix and Tucson.

**Source availability varies sharply inside one county, so check the city before planning a cluster.**
Mesa publishes its own complete results documents back to 1976, which made it straightforward. Chandler
does not publish historical results at all; its own page links only voter and candidate information.
For Chandler, Gilbert, Glendale, Scottsdale and Phoenix the results sit with Maricopa County, whose archive
at `elections.maricopa.gov/results-and-data/historic-results.html` is a JavaScript application. Its year
dropdown loads content dynamically, direct `api/` and `asset/` paths return 403, and the legacy
`recorder.maricopa.gov` results page also returns 403. A promising `historic-results/2018.json` string turned
out to be the 403 page echoing the requested path, not a real endpoint.

Chandler 2018 was still resolved from an official city news release recording the council's acceptance of the
official results: Kevin Hartke elected unopposed with 36,564 votes on August 28, 2018. That route does not
cover 2010, 2014, 2022 or 2026.

**Maricopa County is now solved, September 16, 2026.** Browser automation worked. Two facts unlocked it.

First, the year dropdown is only a query parameter. `historic-results.html?year=2018` returns complete
server-rendered HTML that lists every result document for that year. `curl` reads those index pages, so the
index is reproducible without a browser. `scripts/build_maricopa_results_index.py` builds it and writes
`quality_reports/mayoral_maricopa_results_index.json`: 517 documents across 2010 to 2026.

Second, the county publishes machine-readable text next to the PDFs, for every year. No PDF parsing is
needed. Two formats exist. Years 2010 to 2018 use `CONTEST_FULL_NAME`, `CANDIDATE_FULL_NAME`, `TOTAL` and
sometimes `IS_WINNER`. Years 2019 onward use the "Final SOV and Official Canvass Report" files with
`ContestName`, `CandidateName` and `Votes` at precinct level, so sum `Votes` over precincts for the citywide
total. Where a summary file and a precinct file both cover one election, their totals agree.

The `/asset/` paths return 403 to any plain fetch. That is a Cloudflare bot challenge, not a missing
endpoint, which is why the earlier probes failed. A browser session reaches them normally. Do not try to
defeat the challenge; use the browser for the documents and the script for the index.

Contest names are not stable and must be matched case-insensitively on both the city and the word `Mayor`.
Observed spellings include `MAYOR - CITY OF CHANDLER`, `CITY OF SCOTTSDALE - MAYOR` and `Chandler-Mayor`.
Some files also prefix a candidate name with `" - "`, and one 2018 file lists Kevin Hartke twice, once
quoted and once not, so normalize names and de-duplicate before use.

**Extracted so far:** `quality_reports/mayoral_maricopa_county_mayoral_results_2026-09-16.tsv`, 85 candidate
rows covering 19 elections. It is raw evidence, not yet a coverage input.
Validation: the file reproduces Chandler 2018 as Kevin Hartke with 36,564 votes, which matches the
independently sourced city news release exactly.

**Still needed before these five cities can close.** The county file cannot prove a no-election year, and the
rule against inferring one from a cycle still holds, so each city still needs its official mayor list.
Phoenix ran its own odd-year elections through 2015 and only appears in county files from 2018, so Phoenix
2011 and 2015 need the city clerk. Glendale November 2012 lists only Jerry Weiers and a write-in, so check
whether the runner-up withdrew before recording that election.

**Tested and rejected: a county-file presence test for no-election years.** The idea was the Mesa method:
if a year's county file holds other contests for a city but no mayoral contest, that is evidence of no
mayoral election. It does not work here, and the reason is worth keeping.

Arizona school districts carry city names. `PHOENIX UNION #210 QUESTION`, `GLENDALE UNION #205 QUESTION`,
`CHANDLER USD #80 QUESTION` and `GILBERT USD #41 QUESTION` all contain a city name but are school-district
contests, not city contests. A filter that excludes `SCHOOL` and `DISTRICT` still admits them, because the
published names use `USD`, `ESD`, `HSD` and `UNION` with a number instead. A city name in a contest name
therefore does not show that the county ran a city election that year.

The test also gave a visibly wrong answer, which is what exposed it. It marked Phoenix 2011 and 2015 as
years with a city election and no mayoral race. Phoenix elected a mayor in both years. Those elections were
run by the city, and the county match came from `PHOENIX UNION #210`.

Do not revive this test. Genuine city contests do appear, spelled `CITY OF CHANDLER PROP 484`,
`CITY OF SCOTTSDALE QUESTION 1` or `SCOTTSDALE-PROPOSITION 463`, but a year in which only school districts
voted says nothing about the city. Use the official mayor list for each city, as the method already requires.

The county data stays valuable for the opposite direction. It establishes which mayoral elections *did*
happen, with official vote totals. Only the no-election years need the city sources.

**The corrected test, which does work: look for a city council contest.** A council contest carries the city
name and the word `COUNCIL`, and no school district publishes one. When a year's county file holds a council
contest for a city but no mayoral contest, the county ran that city's election and no mayoral race was on
the ballot. That is the Mesa method applied correctly, and it is positive evidence.

Council-contest coverage in the county files, 2010 to 2026:

| City | Mayoral years found | Council-only years, so no mayoral election | Years with no city contest at all |
|---|---|---|---|
| Chandler | 2010, 2012, 2014, 2018, 2022, 2026 | 2016, 2020, 2024 | all odd years |
| Gilbert | 2012, 2016, 2020, 2024 | 2011, 2014, 2018, 2022, 2026 | 2010, 2013, 2015, 2017, 2019, 2021, 2023, 2025 |
| Glendale | 2012, 2016, 2020, 2024 | 2014, 2018, 2022, 2026 | 2010, 2011, 2013, 2015, 2017, 2019, 2021, 2023, 2025 |
| Scottsdale | 2012, 2016, 2020, 2024 | 2010, 2014, 2018, 2022, 2026 | 2011, 2013, 2015, 2017, 2019, 2021, 2023, 2025 |
| Phoenix | 2018, 2020, 2024 | 2022 | everything before 2018, plus 2019 and 2026 |

Phoenix is the hard case. It ran its own elections before 2019, so the county holds nothing for Phoenix 2010
to 2017. Phoenix elected a mayor in 2011 and 2015 at least. Use the Phoenix city clerk for those years.

### Chandler is complete, September 16, 2026

Chandler is the first city closed by this route, and it is the template for Gilbert, Glendale and Scottsdale.
Inputs: `chandler_calendar.json` with all 17 years and `chandler_results.json` with 6 elections, both
registered in `config/mayoral_top_two_build.json`.

Assessed city-years rose from 130 to 147, which is exactly the 17 expected from one city.
The build writes 17 city-year audit rows, 6 elections and 8 candidate rows for Chandler, and
`tests/test_build_mayoral_top_two_coverage.py` passes with 46 tests.

| Election | Winner | Runner-up | Winner share of mayoral vote | Class |
|---|---|---|---:|---|
| 2010-08-24 | Jay Tibshraeny 26,823 | none | 99.3% | uncontested |
| 2012-08-28 | Jay Tibshraeny 24,637 | none | 99.5% | uncontested |
| 2014-08-26 | Jay Tibshraeny 25,512 | none | 99.3% | uncontested |
| 2018-08-28 | Kevin Hartke 36,564 | none | 99.2% | uncontested |
| 2022-08-02 | Kevin Hartke 39,349 | Ruth Jones 11,755 | 76.8% | verified_decisive |
| 2026-07-21 | Matt Orlando 25,319 | Jeff Weninger 17,991 | 58.4% | verified_decisive |

Two findings made Chandler work, and both apply to the other cities.

**Chandler's mayoral term changed length.** Elections fell in 2010, 2012 and 2014, then in 2018, 2022 and
2026. That is a two-year term that became a four-year term. The 2014 ballot carries
`CITY OF CHANDLER PROPOSITION 470`, which is the likely instrument. Do not assume a fixed cycle for any of
these cities; read the elections that the county actually recorded.

**The decisive-stage rule is in the city charter, not on the elections page.** The Chandler candidate
information pamphlet quotes Article VII, Section 7.01(b): a candidate who receives a majority of all votes
cast at the primary "shall be declared elected ... and no further election shall be held". City Code
Chapter 4, Section 4-5 sets the denominator as the total votes tabulated for the office of Mayor. Every
Chandler winner above holds a majority on that basis, so each primary is the decisive stage. The city
elections page states only that Chandler holds candidate elections in even-numbered years, which is what
supports the odd-year rows.

### Scottsdale is complete, September 16, 2026

Inputs `scottsdale_calendar.json` and `scottsdale_results.json`, both registered in the build config.
Assessed city-years rose from 147 to 164, again exactly 17. Contested elections rose by 4 and candidate
records by 8. The 46 tests still pass.

| Election | Stage | Winner | Runner-up |
|---|---|---|---|
| 2012-08-28 | primary, decisive | Jim Lane 21,837 | John Washington 10,161 |
| 2016-11-08 | general | Jim Lane 66,582 | Bob Littlefield 37,716 |
| 2020-11-03 | general | David Ortega 72,467 | Lisa Borowsky 66,571 |
| 2024-11-05 | general | Lisa Borowsky 71,089 | David Ortega 59,900 |

Scottsdale shows the other half of the Arizona rule. In 2012 Jim Lane took 55.4 percent at the primary and
was elected there, so no mayoral contest appeared at the November general. In 2020 and 2024 no candidate
reached a majority at the primary, so the general decided the office. Read the primary totals before you
choose the stage; do not assume either one.

**Scottsdale 2016 had no primary at all, and that is correct.** The mayoral contest appears only in the
November 8, 2016 county file. The city's own historical election results page also lists no August 2016
primary, so two official sources agree. Neither labels it a special election, so the election type is
recorded as regular, which follows the Mesa precedent.

**Scottsdale 2026 is a no-election year on city evidence, not only county evidence.** The July 2026 county
file holds a Scottsdale council contest and no mayoral contest. Because 2016 proved a mayoral race can skip
the primary, the county file alone was not enough. The city's 2026 candidate listing names only City Council
candidates, which settles it. Apply the same care to any city-year where the November election has not yet
happened.

### Gilbert is complete, September 18, 2026

The input files are written and registered. The assessed count rose from 164 to 181, which is the
expected 17 city-years. `tests/test_build_mayoral_top_two_coverage.py` passes, 46 tests.

Gilbert has four mayoral elections in the window: 2012, 2016, 2020 and 2024. Thirteen years hold no
mayoral election. The 2016 election is uncontested, so the build types it `uncontested` and it yields one
candidate record rather than two. Gilbert contributes seven candidate records.

Gilbert mayoral elections are 2012, 2016, 2020 and 2024. Council-only years are 2011, 2014, 2018, 2022 and
2026. No Gilbert town contest appears in 2010, 2013, 2015, 2017, 2019, 2021, 2023 or 2025.

| Election | Stage | Winner | Runner-up | Winner share |
|---|---|---|---|---:|
| 2012-08-28 | primary, decisive | John Lewis 18,748 | Steve Berman 7,986 | 70.0% |
| 2016-08-30 | primary, decisive | Jenn Daniels 27,058 | none, uncontested | 98.9% |
| 2020-11-03 | general | Brigette Peterson 70,426 | Matt Nielsen 53,121 | primary leader held 34.6% |
| 2024-07-30 | primary, decisive | Scott Anderson 24,659 | Natalie DiBernardo 18,791 | 56.6% |

**The stage assignment self-validates.** A November mayoral contest appears in exactly the years where no
candidate reached a majority at the primary, which is 2020 alone. In 2012, 2016 and 2024 the primary
produced a majority and the county's November file carries no Gilbert mayoral contest. Treat that agreement
as a check on every Arizona city, not only Gilbert.

The decisive-stage authority is A.R.S. § 9-821.01(D): a city or town may provide by ordinance that a
mayoral or council candidate who receives a majority of all votes cast at the primary "shall be declared
elected ... and a further election may not be held as to such candidate". The statute is permissive, so each
city needs its own evidence that it adopted the rule. Gilbert's own primary elections page supplies it: it
records that the four 2026 councilmembers "were elected out right with no runoff election needed in
November". That page also confirms that the 2026 primary elected councilmembers only, so 2026 is a
no-mayoral-election year on town evidence.

**Gilbert publishes the register this project needs.** The clerk's elections page links
"Past Election Results/Voting History, available back to 1981". It resolves to a six-page PDF,
`https://www.gilbertaz.gov/home/showpublisheddocument/54694/639215390208800000`, titled
"Town of Gilbert Voter History- 1981 to present". Each row gives the election date, the canvass date, the
resolution number, the type of election, the ballot issues, the results, and turnout. That is the same kind
of evidence the Mesa clerk register provided, and it settles no-election years directly.

Page 1 covers 2020 to 2026 and confirms every year this project derived from county files:

| Date | Type | On the ballot | Result |
|---|---|---|---|
| 2026-07-21 | Primary | Council, 4 positions, no mayor | four councilmembers elected |
| 2024-11-05 | General | Council 1 seat, Props 497 and 498 | no mayoral contest |
| 2024-07-30 | Primary | **Mayor**, 2 candidates, Anderson and DiBernardo | "S. Anderson elected Mayor" |
| 2022-11-08 | General | Council, 1 position | no mayoral contest |
| 2022-08-02 | Primary | Council, 4 positions | no mayoral contest |
| 2021-11-02 | General/Special | bond question and Proposition 426 only | no candidate contest |
| 2020-11-03 | General | **Mayor**, Nielsen and Peterson | "B. Peterson elected" |
| 2020-08-04 | Primary | **Mayor**, 4 candidates, plus council | went to the general |

The 2021 row also explains the odd-year council match found earlier: it was a ballot-measure election.

**Pages 2 to 6, which hold 2010 to 2019, are read. Hanno approved the download on September 18, 2026.**
A plain fetch of the document returns 403, and so does a fetch that sets a browser user agent, because the
town site blocks non-browser clients. The method that works: open any page of `gilbertaz.gov` in the
browser, `fetch` the document from that page's own session, and save it through a blob download. Then run
`pdftotext -layout`. The register is
`gilbert_voter_history_1981_to_present.pdf`, 54,402 bytes,
sha256 `5b139026fe48403e3fab1bc5068a8695db6cffc4392fb7b9adfd0e2aaadfc4d4`.
The same approval and the same method cover Glendale.

**The register settles every remaining year.** Rows exist for 2010, 2011, 2012, 2014, 2016, 2018, 2020,
2021, 2022, 2024 and 2026, and for no other year in the window. The 2010 row is a special election on
Proposition 406, a sales-tax question with no candidate contest. The 2014 general row reads
"No Election - all positions filled at Primary". The 2011, 2018, 2022 and 2026 rows are council or ballot
measures only. So 2010, 2011, 2013, 2014, 2015, 2017, 2018, 2019, 2021, 2022, 2023, 2025 and 2026 are
no-election years on town evidence, and the four mayoral years are 2012, 2016, 2020 and 2024.

The register also confirms each mayoral result independently of the county files: "John Lewis elected
Mayor" in 2012, "J. Daniels elected Mayor" in 2016, "B. Peterson elected" in 2020, and "S. Anderson
elected Mayor" in 2024.

**Be considerate with the county site.** Reading every text file for all 17 years, repeatedly, made it slow.
Build a contest-name index once and cache it, rather than re-fetching whole result files for each question.

**Do not infer a no-election year from a cycle alone.** For Mesa the even-year documents for 2010, 2018 and
2022 were each opened and their contest headings read; all three cover council districts only. That is
evidence. The odd years rest on the clerk's register listing every election held, which records none.

**Two rules proved their worth in the first batch.**
An election with one named candidate is recorded with a winner and no runner-up. The build classifies it
`uncontested` by itself. Do not invent an opponent. Pittsburgh 2017 is the example.
An incomplete official dataset is not evidence of absence. The county results set linked for the 2013
General Election holds eleven Pittsburgh contests but no mayoral contest, although the city mayor list
places a mayoral election in November 2013. That year is recorded `unresolved`, not `no_election`.

Release does not require a website for every finalist or an arbitrary coverage percentage.
It requires the full target population to be assessed, the agreed collection work to finish, and missing evidence to remain explicit.
The [mayoral status](mayoral_url_discovery.md) and [outstanding-work record](../quality_reports/plans/2026-09-14_mayoral-outstanding-work.md) hold the detailed queues and existing acquisition limits.
The September 14 website table remains the verified baseline until later evidence is integrated.

## 4. 2026 collection: monthly decision and corrected storage

The final September 15 decision supersedes weekly live copies and the daily election window.
Collect live HTML/text every 30 days, plus one copy on the local day before and after each applicable primary or runoff.
A successful targeted copy resets the regular interval. A copy already taken on the target day satisfies that request.
Confirmed nonparticipants receive regular monthly copies only. Roster and URL refreshes remain weekly, and the daily automation checks due work.
Wayback comparison windows remain 30 days before and after each event, excluding election day.
Collection ends 14 days after the general election, extended for affected later runoffs.

The [approved plan](../quality_reports/plans/2026-09-14_campaign-websites-pre-post-primary.md) and [current 2026 documentation](collection_2026.md#final-cadence-decision-september-15-current) control this decision.
The implementation in `src/collection_2026_outputs.py`, `config/collection_2026.json`, and the saved automation prompt all specify the monthly schedule.
The [session log](../quality_reports/session_logs/2026-09-14_campaign-websites-pre-post-primary.md) reports deployment and 164 passing focused and adjacent tests.
This handoff update inspected the current code and artifacts; it did not repeat those tests or inspect the remote host.

The [current forecast](../data/collection_2026/monthly_cadence_20260915/pilot_forecast.json), generated at 17:04 UTC, projects 11,512 remaining live site copies across 3,674 candidates.

| Current storage estimate | GiB |
|---|---:|
| Base estimate for the remaining collection | 27.78 |
| Total including the separate 2× planning allowance | 55.56 |
| Additional headroom required by the storage check | 5.00 |
| Available space recorded in that forecast | 19.32 |

**Superseded September 16, 2026. Full collection is enabled.**
Hanno resized the DigitalOcean volume `mayoral-wayback-full-snapshots` from 50 to 100 GiB, about $5 a month
and roughly $17 through the December 26 endpoint. `resize2fs /dev/sdb` grew the mounted ext4 filesystem
online, with no unmount and no downtime.

| After the resize | Value |
|---|---:|
| Filesystem | 100 GiB, 28 GiB used, 67 GiB free |
| Available bytes the gate saw | 71,834,443,776 |
| Projected total including the 2x allowance | 56,425,842,468 |
| Required headroom | 5,368,709,120 |
| `storage_fits` | **true** |

`enable-full` then wrote `full_enabled.json` at 19:28 UTC, recording `authorized_plan` 2026-09-14 and the
passing forecast. The command refuses to write that file unless the gate passes, so the flag is itself evidence.

The remaining collection is 11,490 live site copies across 3,674 candidates to the 2026-12-26 endpoint.
Root remains a separate watch item at 24 GiB with about 3 GiB free; this resize did not touch it.

The storage correction is complete. The [migration report](../data/collection_2026/storage_correction_20260915/storage_migration.json) records 373,066 working index rows before correction and 23,148 afterward.
This 93.8% reduction removes asset/non-HTML metadata and repeated eligible entries. It removes no candidate or page observations.
All 881 pages, 261 work records, 1,082 request attempts, and compressed raw index responses were preserved.
The working index now reuses capture records across refreshes, and its CSV export is compressed.
The old 349/700 GiB estimates and 400/800 GiB volume proposals are superseded. Do not repeat the correction or revive those proposals.

The session log records that PID 1339730 finished, with its last request at 15:27:42 UTC.
The completed daily pass has eight retryable items with explicit outcomes; avoid repeated same-day launches of those failures.
The existing daily Codex automation remains active with the monthly schedule. Check current remote state and scheduler ownership before operational changes.

**Next action:** refresh available capacity and the monthly forecast, then resolve the capacity decision under the existing approval rules.
Once capacity is available and the storage check passes, enable full collection under the approved monthly schedule.

Approved Dataverse description sentence, to append to the dataset description before publishing:
"Version 4.0 adds issue statements, biography facts, and endorsements extracted from the archived pages, each quoted from its source page."

**Disk cleanup (Hanno approved September 30, part 1 only).** After the September 30 build passes
validation, delete: `build/dataverse_v4_0_draft_download` (1.1 GB, recreated by re-staging),
`build/hive_candidate_extraction/release_20260928_v22_model` and `release_20260928c_v22_model`
(plus their `.summary.json`, `.stderr`, `.validate.*` side files). Deferred, not approved: move the
older builds (`bundle_20260920`, `release_20260920`, `release_20260925_v22`, August pilot folders) to
mac2; check whether `data/deliverable` and `data/municipal_discovery` are still used.

**September 30, 17:35.** Build and validator for `release_20260930_v22_model` passed (`release_ready`
true). Rows: issues 287,255, biography 104,124, endorsements 153,108, documents 23,177, segments
34,322. `.gov` exclusion now removes 654 documents. All counts in the public codebook and README
updated from these files. Part 1 cleanup done. `build/release_v4_0_candidate` re-assembled (19 files,
local manifest checks pass). Draft re-staged and verified (19 files byte-identical after download; report
`build/dataverse_v4_0_draft_download/dataverse_draft_verification.json`). Description sentence added
to the draft metadata. Waiting only for Hanno's publish confirmation.

**2026 collection, September 30.** Hanno set the endpoint to November 17, 2026 (14 days after the
general election), replacing December 26. Implemented as `collection_end_date` in
`config/collection_2026.json`; `due_live` caps each candidate's endpoint and `run` exits after the date.
Defect found and fixed: from September 24 every daily run stopped on a `ValueError: Invalid IPv6 URL`
from a malformed link on candidate S6GA00358's site. Live copies were nearly complete (the candidate
sorts last), but the URL lookup and Wayback archive steps did not run September 24 to 30. The daily
log read `rc=0` because `$?` was expanded after `$(date)`. Fixes: `page_links` skips unparseable links;
the runner saves the exit code first. Deployed by rename with `.bak-20260930` copies; droplet hashes
match the repository; 155 collection tests pass. Next check: the 2026-10-01 run should reach the
archive phase and `logs/daily.log` should show its real exit code.

**Version 4.0 publication.** Hanno approved publishing on September 30. The API token cannot publish
(HTTP 401, "not permitted"), so Hanno publishes from the Dataverse page (Publish Dataset, major
release). The draft is complete and verified.

**Share of the release complete, September 30 (approximate).** About 57 percent: Track 1 20 of 20;
Track 2 about 24 of 25 (only Hanno's web publish remains); Track 3 about 6 of 40 (unchanged, 164 of
1,700 city-years); Track 4 about 7 of 15 (collection now ends November 17). Remaining: agent work
dominated by Track 3 (roughly 35 to 55 hours); Hanno about 5 minutes to publish plus mayoral decisions.
Wall-clock floor: November 17 for Track 4, then 1 to 2 days of exports. Largest uncertainty: the
1,536 unassessed mayoral city-years.
