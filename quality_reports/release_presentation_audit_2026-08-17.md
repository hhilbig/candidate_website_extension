# Release presentation audit

> **Historical audit, resolved.** This report describes the pre-repair release
> candidate. Commit `ca9069e` rebuilt the population, removed invalid captures,
> repaired identity handling and labels, and regenerated the five products.
> Use `docs/RELEASE_README.md` and the current validation output for release
> decisions. The findings below are preserved as provenance.

**Date:** 2026-08-17  
**Scope:** the five data products listed in `docs/RELEASE_README.md`, plus the
release README and codebook  
**Verdict:** do not deposit the current files

## Bottom line

The release has a clear structure and useful documentation, but the current
files are not ready for deposit. The roster builder assigns the FEC file year
to every candidate without checking the candidate's recorded election year.
This creates many suspect candidate-years. A same-year name screen against the
official FEC candidate files flags 2,577 of 9,944 panel rows, or 25.9%. That is
a screening count, not an exact deletion count: reviewed identity mappings and
ballot exceptions can rescue some rows. The defect still changes the sample,
every headline count, and several coverage claims.

The release package also contains known non-campaign text. Examples include AOL,
Yahoo, Facebook, Google, and a law-firm website. The README identifies only the
law-firm record. These records need removal or correction before any document
or figure receives a final presentation pass.

## What was checked

The audit treated the following files as the intended release:

| Published name | Current source | Status |
|---|---|---|
| `raw_corpus.parquet` | `data/deliverable/raw_corpus_icpsr.parquet` | Source exists under a different name |
| `panel_candidate_year.csv` | `data/deliverable/panel_candidate_year.csv` | Present |
| `panel_icpsr_compat.csv` | `data/deliverable/panel_icpsr_compat.csv` | Present |
| `candidate_crosswalk.csv` | `quality_reports/coverage_audit/csv/candidate_crosswalk.csv` | Exists outside the deliverable directory |
| `release_roster.csv` | `data/deliverable/release_roster.csv` | Present |

The audit parsed all three release CSVs and the candidate crosswalk. It read the
Parquet metadata and selected columns. It compared keys and missing values
across products. It also compared each release year with the official FEC bulk
candidate file for that cycle. The same-year comparison used candidate name,
state, and office. It ignored district and party, so it gives the current files
the benefit of any redistricting or party change.

## Release blockers

### 1. The roster builder does not filter the FEC election year

`src/build_candidate_roster.py` reads `cand_election_yr` but never uses it in
`build_fec_roster()`. The function filters office and party, then sets every
row's `year` to the requested file cycle. The code comment says it keeps active
candidates, but it does not filter `cand_status` either.

This comparison shows the effect on the current files:

| Year | Panel before | Same-year FEC record | Implied drop | Drop share |
|---:|---:|---:|---:|---:|
| 2002 | 22 | 13 | 9 | 40.9% |
| 2004 | 68 | 51 | 17 | 25.0% |
| 2006 | 107 | 85 | 22 | 20.6% |
| 2008 | 155 | 107 | 48 | 31.0% |
| 2010 | 181 | 147 | 34 | 18.8% |
| 2012 | 210 | 156 | 54 | 25.7% |
| 2014 | 243 | 156 | 87 | 35.8% |
| 2016 | 232 | 137 | 95 | 40.9% |
| 2018 | 1,965 | 1,538 | 427 | 21.7% |
| 2020 | 2,128 | 1,650 | 478 | 22.5% |
| 2022 | 2,591 | 1,929 | 662 | 25.5% |
| 2024 | 2,042 | 1,398 | 644 | 31.5% |
| **Total** | **9,944** | **7,367** | **2,577** | **25.9%** |

The roster has the same problem at a larger scale. The same-year screen retains
9,845 of 16,945 rows and removes 7,100, or 41.9%.

This is not a cosmetic definition issue. The panel assigns website captures to
years when the person was not recorded as a candidate for that election. For
example, the 2024 FEC file still contains Carolyn Maloney with
`cand_election_yr = 2022`; the current panel creates a 2024 row from her expired
website. Similar stale rows occur throughout the release.

**Required repair:** filter on `cand_election_yr == year` at roster creation,
rebuild all rosters and deliverables, and rerun every count, coverage table,
figure, coded variable, and merge test. Investigate `cand_status` separately;
do not add a status filter until its effect and FEC definition are checked.

### 2. Several captures are not campaign websites

The older scraper interpreted some email-like or generic URLs as domains. The
raw corpus then followed large public sites. Confirmed examples include:

| Assigned candidate-year | Stored source | Content actually captured |
|---|---|---|
| Margaret Ruth Engebretson, House 2018 | `https://vetfordemocracy@aol.com` | AOL homepage and news, 64,726 page rows |
| M Latroy Williams, House 2018 | `https://ml.williams66@yahoo.com` | Yahoo homepage and news, 83,499 page rows |
| Eddie Hamilton, House 2018 | email-like Facebook URL | Generic Facebook homepage, 9,299 page rows |
| Arturo Pacheo Reyes, House 2020 | two email-like Facebook URLs | Generic Facebook homepage, 9,552 page rows |
| Joan F M Malone, House 2018 | `https://www.google.com` | Google pages, 7,577 page rows |
| J James Exon, Senate 2002 | `https://tbartels@clinewilliams.com` | A law-firm website, 48 page rows |

Kelly McCarthy's malformed URL contributes an error page rather than campaign
text. Some URLs rejected by the current cleaner are nevertheless valid campaign
sites, including Tammy Savoie and Frank Vondersaar. The repair therefore needs
a small manual adjudication table rather than a blanket `has_url == false`
deletion.

The README already calls J James Exon spurious and tells users to drop him. A
presentable release should not include a record known to be false.

**Required repair:** remove confirmed unrelated captures at source, document the
manual decisions, and rerun all downstream products. Review every row where
`captured == true` and `has_url == false`, plus generic-domain URLs.

### 3. The stated release package is not assembled

The release README promises `raw_corpus.parquet`, but the current file is named
`raw_corpus_icpsr.parquet`. It also promises `candidate_crosswalk.csv`, but that
file remains under `quality_reports/coverage_audit/csv/`. The deliverable
directory contains four `.prefilter` backups that are not part of the published
file list and must not be uploaded.

**Required repair:** create one staging directory containing exactly the five
published data filenames and the final documentation. Generate a manifest with
row counts, column counts, byte sizes, and SHA-256 checksums.

## Important documentation problems

### 4. The release describes the wrong population

The title, opening, scope section, `stage = 2`, and `general_wayback` label all
describe general-election candidates. Only 3,037 of the current 9,944 panel rows
match a general-election ballot. The README later acknowledges this, but it also
says the remaining rows filed and lost or left a primary. The missing FEC-year
filter shows that this statement is false for many rows. Some are prior-cycle or
dormant candidate records.

After the data repair, the documents must describe the remaining population
precisely. If the target is all same-year Democratic and Republican FEC
candidates, do not call them general-election candidates. Treat `on_ballot` as
the general-election indicator. Reconsider whether the synthetic `stage = 2`
and `data_source = general_wayback` labels are defensible for non-ballot rows.

### 5. The codebook describes an older package

The codebook is not a standalone guide to the five intended files.

- Its product table lists `panel_candidate_year_icpsr.csv` and
  `panel_candidate_year_icpsr_meta.csv`, which are not release files.
- It omits `release_roster.csv` from that table.
- It reports old sizes, including 1.6 MB for a current 15.9 MB coded panel.
- It documents `party_icpsr` and `district_num`, while the crosswalk uses
  `party_full` and `district_i`.
- It retains the disproved claim that the 2016/2018 length difference remains
  1.5 to 1.65 times and mainly reflects crawl depth. The release README correctly
  states that the cleaning step reduces the boundary change to 0.9%.
- It cites ICPSR DOI `10.3886/E232001V1`; the project otherwise uses
  `10.3886/E226001V1`.
- It points readers to internal quality reports, a private parent-project path,
  the droplet, and local machine steps that are not part of the deposit.
- Its regeneration section gives only three partial steps and does not rebuild
  the five published products.

**Required repair:** rewrite the codebook around the five actual release files.
Give every column a definition, unit, missing-value rule, and key. Move
validation history into a short methods appendix or a separate technical note.

### 6. Reproduction is not currently supported by the linked environment

The release README links the GitHub repository and says the pipeline rebuilds
every derived file. The repository's `requirements.txt` omits dependencies used
by that pipeline: `numpy`, `pyarrow`, `nltk`, `scikit-learn`, and the test runner
`pytest`. Both available project Python environments lacked `pytest`, and the
repository documented no test runner or offline/live split. One environment
also lacked `pyarrow`, so it
could not read the largest release file.

The tests also mix offline unit checks and live Wayback checks without markers
or separate commands. A new user cannot tell which tests are stable and which
need network access.

**Required repair:** provide a locked or fully specified release environment,
an exact build command, and separate offline and live test commands. Run the
offline suite from a fresh environment before deposit.

### 7. Cross-file geography is not fully consistent

Eight captured candidate-years match a roster row on name, state, office, and
year but disagree on district. Michael Doyle in 2022 also differs on party,
because the same name identifies two different politicians. The current roster
collapses duplicate district rows at a key that excludes district and party,
then marks capture by name, state, office, and year. This can attach a capture
to the wrong roster record.

Examples include Liz Matory 2018, Johnny Nalbandian 2022, Amy Vilela 2022,
Ryan Krause 2022, Jonah Schulz 2022, Susan Narvaiz 2018, Andrei Cherny 2024,
and Michael Doyle 2022.

**Required repair:** use `cand_id` as the primary candidate identity and retain
the same-year district and party. Do not collapse candidate records on a key
that can merge different candidacies.

## What already passes

- The three current release CSVs parse successfully.
- No release CSV contains exact duplicate rows.
- The documented candidate-year keys are unique in the text panel, coded panel,
  roster, and candidate crosswalk.
- The text and coded panels contain 9,944 rows each and join to the crosswalk.
- All 31 main topic shares sum to one within numerical precision.
- `general_votes` is present for every `on_ballot == true` row and absent for
  every `on_ballot == false` row.
- The raw Parquet file has 1,101,303 rows, 23 columns, and 9,944 row groups.
  Its core candidate, date, URL, count, and text columns contain no missing
  values in the checked data.
- The release README has a useful file-first structure, explains selection, and
  gives direct warnings about topic interpretation and text quality.

These checks show that the final data model is workable. They do not rescue the
incorrect candidate-year assignment.

## Presentation repairs after the data rebuild

1. Replace every headline count and percentage from rebuilt files.
2. Remove the instruction to drop a known bad record; ship clean files instead.
3. Add a citation for this dataset, with the final Dataverse DOI. Keep the full
   citation for Di Tella et al.
4. State file sizes with explicit units, preferably MB/GB in decimal or MiB/GiB.
5. Add a one-page release manifest. List filenames, grains, rows, columns,
   sizes, checksums, and primary keys.
6. Keep the release README short. Put the full field dictionary and methods in
   the codebook.
7. Remove internal history, machine names, and private project paths from the
   public codebook.

## Recommended repair order

1. Fix roster construction and rebuild the candidate universe.
2. Review and remove unrelated website captures.
3. Rebuild all five products and rerun cross-file checks.
4. Recompute validation statistics, coverage tables, and figures.
5. Rewrite the release README and codebook against the rebuilt files.
6. Assemble the final staging directory and manifest.
7. Test reproduction from a clean environment.
8. Perform the final visual and copy edit only after every number is stable.
