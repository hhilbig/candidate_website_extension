# Plan: Mayoral URL discovery, 2010-2021

**Date:** 2026-08-25  
**Status:** OpenAI residual screen complete; archive acquisition running and queued  
**Supersedes for new acquisition:** the pre-2010 municipal-record strategy in
`2026-08-24_mayoral-url-discovery-next-phase.md`

## Approved amendment: bounded search-led expansion

On 2026-08-25, Hanno reopened generic search for a bounded test on the same
480-case blinded screen. The first wave uses one Serper query per candidate,
so it can consume at most 480 of the 2,500 available account credits. Under the
repository's existing cost model, this is at most $0.48 in credit value. The
pipeline will save the result list, fetch and parse up to three credible
election-specific source pages per candidate, extract candidate-attributed
external URLs, and send only identity-supported campaign-site leads to temporal
validation. It will measure yield before running fallback queries; any second
query wave requires a new review of the first-wave result.

This amendment replaces the statements below that generic web search remains
closed or that all Serper credits remain unused. It does not change the evidence
grades, coverage rules, blinded sample, validation windows, or human-review
requirements.

## Implementation result

The bounded search wave used exactly 480 Serper credits and no fallback
queries. It produced six human-approved grade-A custom domains. Three website
owners are members of the fixed 480-candidate screen; three are candidates from
the same contests who were found while searching for a sampled candidate.
Those spillovers increase coverage but cannot enter the candidate-screen yield.

The prespecified screening result is `drop`: 3 of 480 sampled candidate owners
gain strict coverage, 0 of 160 gain coverage in 2010-2013, and adjudicated
precision is 100 percent. The weighted on-sample rate is 0.49 percent among the
2,228 candidate-years represented by the screen. It projects 10.9 additions,
with a one-sided 90 percent lower bound of 4.7. Total observed coverage rises
from 145 to 151 of 4,817 candidate-years because all six approved websites are
valid additions. No independent 800-case search confirmation or full Serper
acquisition proceeds.

## OpenAI residual search pilot

This pilot tested whether OpenAI's hosted web-search index finds historical
campaign URLs that the one-query Serper pipeline did not send to temporal
validation. The comparison uses 100 candidates drawn from the 457 members of
the fixed screen who had no Serper temporal-validation lead. It draws 34 cases
from 2010-2013 and 33 cases from each later period. The model made one Responses
API request per candidate with a maximum of one completed web search and no
fallback request.

OpenAI search passes the discovery-stage test. The model returned an explicit
campaign URL for 27 candidates. One URL was a
clearly wrong-office domain and was excluded, leaving 28 new candidate-domain
targets for 26 candidates. None of those candidate-domain pairs appears in the
Serper URL-lead table. The raw candidate lead rate is 26.0 percent. Reweighting
the three sample periods to their 457-case residual populations gives 26.0
percent, or about 119 candidate leads; the one-sided 90 percent lower projection
is about 96. These are grade-D leads, not coverage additions.

The extractor preserves 36 eligible campaign-URL claims. Collapsing paths on
the same candidate-owned domain produces 28 validation targets, a 22.2 percent
reduction. This is an expected request-level deduplication, not evidence loss;
the 36 source claims remain in `extracted/url_claims.csv`.

The paid stage completed 100 of 100 requests without an HTTP error. It preserved
2,099 source claims, including 2,036 distinct source URLs. The conservative cost
estimate is $1.409 against the approved $2 ceiling. That estimate charges all
120 search-call items in the responses even though only 100 completed; it also
includes 864,692 input tokens and 28,740 output tokens.

Wayback metadata locates a snapshot for 13 of the 28 domains. Ten have a
reported snapshot within 365 days of the election. Twelve availability lookups
failed, so their snapshot status is unknown; three domains returned a confirmed
no-capture result. These outcomes do not change discovery success. Downloading
and reading archived page content belongs to the later coverage-validation
stage, not URL discovery.

The implication is that OpenAI search passes the discovery phase with a 26.0
percent candidate lead rate. The next decision is whether to approve expansion
to additional unresolved candidates. The 28 domains remain grade-D leads until
later content validation, so this result does not yet change strict coverage.

## OpenAI residual expansion result

Hanno approved a $5.50 ceiling for the remaining 357 fixed-screen candidates.
The expansion completed 357 of 357 requests without an HTTP or parsing error and
cost an estimated $4.976. It preserved 7,471 source claims and 6,927 distinct
source URLs. The deterministic extractor found 132 eligible campaign-URL claims,
which collapse to 122 candidate-domain targets for 116 candidates. A Ballotpedia
profile placed in the model's campaign-URL field was retained in the claim table
but excluded from the candidate-site targets.

Two of the 122 candidate-domain pairs were already present in the broader Serper
lead table. The expansion therefore adds 120 new candidate-domain pairs for 114
candidates, a 31.9 percent new-candidate yield among the 357 searches. Across the
full 457-case OpenAI residual screen, the pilot and expansion together add 148
candidate-domain pairs for 140 candidates that Serper did not recover. This is a
30.6 percent candidate yield. These remain grade-D discovery leads and do not
change strict coverage.

Hanno directed the project to add these URLs to the candidate list before
archive validation. The unified claim table now contains all 168 eligible
OpenAI path-level claims from the pilot and expansion. They represent 142
candidate-years and 150 candidate-domain pairs. Every OpenAI claim is pending
grade D; none is human accepted or counted as strict coverage.

The archive queue contains 121 of the 122 expansion targets, representing 115
candidates. Daniel Rickenmann's claim remains deferred because the candidate
record has no election month and the returned sources concern the 2021 campaign
while the roster assigns the candidate to 2020. The pipeline does not impute an
election month. The 121-target archive batch will start on the droplet after the
151-covered-candidate batch ends, so the two jobs never query Wayback
concurrently. A Slack watcher reports completion or an actionable failure.

Authoritative pilot artifacts are under
`data/municipal_discovery/screening_2010_2021/openai_search_residual_pilot/production/`.
The paid-request audit is `audit.json`, extracted claims and targets are under
`extracted/`, and the hash-pinned archive result is
`temporal_validation/merged/temporal_validation.csv`.

The integrated OpenAI and temporal-validation tests pass (76 tests). The full
non-live repository suite reports 772 passes, 29 live-test deselections, and one
failure outside this channel: the candidate-extraction v2 test expects a
different frozen stage-A2 manifest hash.

## Current state and decision

The project has strict campaign-website coverage for 151 of 4,817 mayoral
candidate-years (3.13 percent). Broad Archive-It acquisition is closed because
its final 20-collection batch added only one strict candidate-year. The Serper
pilot is closed because it produced low and uncertain strict yield. The first official
municipal-file pass tested 335 unresolved candidate-years from 2000 through
2007 and produced no URL claims.

New acquisition will target elections from 2010 through 2021. A domain only
improves strict coverage when election-period archived content establishes the
candidate, mayoral campaign, city, and timing. Sparse Wayback coverage before
2010 makes many historical domains impossible to validate under this rule.
Existing pre-2010 evidence remains in the dataset, but the project will not
spend the next discovery round or records-request effort on that period.

The frozen legacy roster has 2,695 unresolved candidate-years from 2010 through
2021: 932 in 2010-2013, 861 in 2014-2017, and 902 in 2018-2021. These counts
predate the accepted Archive-It additions. The first implementation task must
therefore rebuild the current unresolved roster rather than treating 2,695 as
the final denominator.

## Question and comparison

The next screen asks which no-cost or review-before-purchase source channels
recover strict campaign URLs for unresolved mayoral candidates from 2010
through 2021. Every channel will run on the same candidate-year sample. The
comparison will measure new strict URLs, adjudicated precision, analyst time,
license cost, and cost per accepted URL.

## Fixed assumptions

1. The full reporting universe remains 4,817 candidate-years. Restricting active
   acquisition to 2010-2021 does not remove pre-2010 cases from coverage reports.
2. The candidate-year remains the coverage unit. Reports continue to distinguish
   verified general elections from other recorded rounds.
3. The active acquisition frame contains unresolved in-scope candidate-years
   from 2010 through 2021 after all accepted claims are incorporated.
4. The pipeline retains missing election months and never substitutes November.
5. No further Serper credits or paid OpenAI search calls are used without a new
   approved budget and a decision based on the completed pilot.
6. No paid API, data purchase, or external message proceeds without the existing
   approval requirements.
7. Pre-2010 discovery can return only as a labeled sensitivity exercise when a
   source also supplies contemporaneous archived campaign content. A domain name
   alone is insufficient.

## Evidence and coverage rules

- Grade A: an election-period capture of the exact URL identifies the candidate,
  mayoral campaign, and city.
- Grade B: a contemporaneous official source links the URL and archived campaign
  content confirms ownership and timing.
- Grade C: a source supports the URL, but no qualifying campaign capture exists.
- Grade D: domain shape, current content, social metadata, or another forensic
  signal supplies only a lead.

Only accepted grade A and B claims count as strict coverage. The primary measure
counts candidate-owned custom domains. The secondary measure also counts exact
candidate subdomains and shared-host paths. Fundraising, social-media, and
officeholder pages remain leads and count in neither measure.

The headline timing window remains 365 days from the election. The sensitivity
window runs from 18 months before through 120 days after the election. Domain
reuse checks inspect two years on each side.

## Implementation order

### 1. Rebuild the current acquisition frame

Combine the frozen legacy benchmark, every accepted direct claim, and every
accepted sampled-audit claim into one current coverage roster. Require one row
per candidate-year and preserve the distinction between directly adjudicated
and audit-inferred coverage.

Then create the unresolved 2010-2021 frame. Report counts by period, year,
region, city population, profile availability, election-stage status, contest
size, and missing election month. Fail on duplicate candidate-year keys or a
coverage key absent from the 4,817-case universe.

Expected output directory:
`data/municipal_discovery/screening_2010_2021/`

### 2. Draw a new blinded screen

Use seed `20260825`. Draw 480 candidate-years:

- 160 from 2010-2013;
- 160 from 2014-2017;
- 160 from 2018-2021.

Exclude the 120-case search pilot, the earlier 480-case screen, the earlier
complete-contest sample, and all covered candidate-years. Balance region, city
population, profile availability, stage status, and contest size. Retain exact
inclusion probabilities and population weights.

Draw 30 additional complete contest-years, ten per period. They must be disjoint
from the candidate screen and every earlier pilot or screen. Record every
exclusion and fail if an unreported filter removes more than 20 percent of cases
or cities.

### 3. Re-rank source targets for 2010-2021

Build a city priority table from the new unresolved frame. Rank cities using:

1. unresolved candidate-years;
2. evidence that an official or curated source stores campaign-website fields;
3. observed Wayback availability near the relevant elections;
4. the ability to parse one source once for a whole contest;
5. expected analyst time and access cost.

Do not give high priority to a source that supplies domains without a plausible
route to grade A or B validation.

Run these channels on the same sample:

1. official municipal candidate lists, voter guides, filing systems, and
   campaign-finance exports for 2010-2021;
2. already identified curated or civic archives with election-specific URLs;
3. historical civic-data provider samples, beginning with BallotReady or
   CivicEngine and Ballotpedia for 2018-2021, then Democracy Works for 2020-2021.

Broad Archive-It enumeration and further paid search expansion remain closed.
Individual Archive-It collections may be used when another source identifies a
specific high-probability collection. The 28 OpenAI candidate-domain targets
remain eligible for no-cost archive-content recovery.

### 4. Acquire and parse official records

For each selected contest-source target:

- freeze the exact source URL, response date, bytes, and SHA-256 hash;
- recover HTML, CSV, TSV, Excel, JSON, and PDF files;
- parse each physical file once for every candidate in the contest;
- extract PDF hyperlink annotations and visible URLs;
- preserve raw URL case and path, exact normalized URL, site root, and domain;
- emit exactly one terminal source attempt per contest-source key;
- retry transient errors and keep stale-row removal active.

Send all proposed URLs through temporal ownership validation. If only a few
claims emerge, adjudicate them directly in chat. Build a handcoding app only
when the queue is too large for reliable in-chat review.

### 5. Evaluate channels

For each channel, report raw and weighted marginal yield, cumulative yield,
adjudicated precision, analyst time, license cost, and cost per accepted URL.
Report results within the 2010-2021 frame and as percentage-point changes in the
full 4,817-case coverage measure.

A channel continues when it produces either:

- at least ten new strict URLs among 480 cases; or
- at least five new strict URLs among the 160 cases from 2010-2013.

It must also reach at least 95 percent adjudicated precision and a projected
cost below $100 per accepted URL. Drop a channel when it produces no more than
three strict additions, falls below 90 percent precision, or exceeds $250 per
accepted URL.

Pending claims cannot trigger a continuation or acquisition decision.

### 6. Confirm a passed source stack

Only after a source stack passes the screen, draw an independent 800-case
confirmation sample:

- 320 from 2010-2013;
- 240 from 2014-2017;
- 240 from 2018-2021.

Exclude all earlier pilot, screen, and complete-contest cases. Preserve weights
and the same balance fields. Full acquisition requires projected primary
coverage of at least 15 percent of all 4,817 candidate-years, a one-sided 90
percent lower bound of at least 13 percent, precision of at least 95 percent,
and production cost below the approved ceiling.

## Parallel execution with agents

Parallel work begins only after the root agent freezes the current coverage
roster and unresolved 2010-2021 frame. Four concurrent slots are available: the
root agent and at most three subagents. The root agent owns integration,
authoritative configs, master evidence tables, project documentation, and final
verification. Subagents work in disjoint files or output directories.

### Wave 0: serial prerequisite

The root agent rebuilds and audits the current coverage roster. It freezes the
schema, exact unresolved keys, period definitions, exclusion keys, and output
locations. No sampler or source-acquisition agent starts before this contract is
written because a stale denominator would invalidate every downstream result.

### Wave 1: three independent preparations

After Wave 0, run these workstreams concurrently:

| Agent | Workstream | Owned outputs | Must not edit |
|---|---|---|---|
| 1 | Revised 480-case and 30-contest samplers, inclusion probabilities, balance audit, and tests | New sampler script, its tests, and `screening_2010_2021/` sample artifacts | Master evidence, source registry, project docs |
| 2 | Municipal and curated-source priority audit for 2010-2021 | New source inventory, city priority table, access notes, and source-specific fixture files | Sampler, master manifest, outreach messages |
| 3 | Historical civic-provider feasibility and masked-sample requirements | Provider comparison, required schema, terms/cost checklist, and unsent outreach proposals | Gmail, paid services, source-acquisition outputs |

The root agent concurrently prepares the explicit build-manifest and registry
changes needed to ingest the new outputs. It reviews each agent's tests and
artifacts before merging anything into an authoritative table.

### Wave 2: partitioned official-source acquisition

If the municipal-source audit identifies enough viable targets, split them by
stable source-target hash, not by an informal list that can drift:

- Agent 1 processes hash partition 0 of 2.
- Agent 2 processes hash partition 1 of 2.
- Agent 3 audits parser compatibility, archive-query readiness, and source-field
  coverage without making duplicate network requests.

Each acquisition agent writes to its own immutable shard directory and emits a
download manifest plus one terminal attempt per assigned contest-source target.
Agents never append to the same CSV and never edit the central registry. The
root agent checks that shard keys are disjoint, their union equals the frozen
target set, and every source file hash resolves before performing an atomic
offline merge.

Requests to the same host must not run concurrently. The root agent groups
targets by registrable domain before partitioning or assigns a shared per-host
rate limit. Transient errors remain retryable; an agent cannot mark them as a
completed negative.

### Wave 3: partitioned temporal validation

If acquisition yields candidate URLs, the root agent freezes the evidence-key
set. Validation can then run in two disjoint hash shards while a third agent
performs an independent identity and domain-reuse audit:

- Agent 1 validates evidence-key partition 0 of 2.
- Agent 2 validates evidence-key partition 1 of 2.
- Agent 3 checks candidate identity, election timing, URL type, and two-year
  domain reuse on the frozen claims.

The root agent merges validation shards offline with the current version-four
method. It rejects duplicate, missing, unexpected, stale, or non-current-method
keys. Human adjudication occurs only after this merge.

### Wave 4: serial integration and decision

The root agent alone rebuilds the unified claim table, coverage roster, channel
evaluation, and frozen state manifest. It runs the clean build, focused tests,
and full non-live suite. Subagent summaries are supporting information, not
evidence that code or outputs work; the root agent verifies every reported
artifact directly.

### Agent safety boundaries

- No agent sends email, submits a public-record request, contacts a provider,
  uses Serper, or purchases data.
- No agent modifies or deletes another agent's output shard.
- No agent changes the sample, evidence rules, thresholds, or denominators.
- Any proposed change to those rules returns to the root agent and Hanno before
  implementation.
- Every agent reports files changed, commands run, tests passed, unresolved
  errors, and any observation or city loss above 20 percent.

## Paused actions

The approved Columbus, Austin, and Pomona requests are saved as Gmail drafts.
They remain unsent because all three concern elections before 2010. Do not send,
edit, or delete those drafts unless Hanno explicitly reactivates the pre-2010
sensitivity exercise.

No civic-data provider has been contacted. Before any provider outreach, Hanno
must approve the exact recipient and message. Before any purchase, Hanno must
also review a masked sample, quote, license terms, and projected cost per
accepted strict URL.

## Verification requirements

After each code change:

1. run the focused mayoral and municipal tests with `PYTHONPATH=.`;
2. build the pipeline in a clean temporary directory;
3. require exact key agreement between accepted claims, coverage roster, and
   archive validation;
4. require one terminal attempt for every contest-source target;
5. verify all output files are nonempty when rows are expected;
6. freeze row counts, byte counts, and SHA-256 hashes;
7. run the full non-live test suite before declaring the phase complete.

The repository was green at this handoff: 515 tests passed and 29 live tests
were deselected.

## Authoritative restart files

- Project status: `docs/mayoral_url_discovery.md`
- Previous implementation plan:
  `quality_reports/plans/2026-08-24_mayoral-url-discovery-next-phase.md`
- Source-channel status:
  `quality_reports/mayoral_url_discovery_2026-08-24/source_channel_status.md`
- Municipal results:
  `quality_reports/mayoral_url_discovery_2026-08-25/municipal_records_phase_report.md`
- Paused request batch:
  `quality_reports/mayoral_url_discovery_2026-08-25/municipal_request_batch_1.md`
- Frozen municipal state:
  `quality_reports/mayoral_url_discovery_2026-08-25/municipal_records_state_manifest.json`
- Candidate universe:
  `data/municipal_discovery/final/mayoral_candidate_universe.csv`
- Existing evidence table:
  `data/municipal_discovery/final/mayoral_url_claims.csv`

## First action after restart

Treat the completed 100-case OpenAI run as a successful discovery pilot. Its 28
candidate-domain targets are now frozen. Do not run more paid search until the
archive-acquisition result for these targets is known.

Discovery and archive acquisition are separate pipelines inside this repository,
not separate Git repositories. Discovery owns candidate URL claims and their
source evidence. Archive acquisition consumes an immutable claim queue and may
save captures, pages, and extracted text, but it may not add a URL or reinterpret
candidate ownership. New archive outputs belong under
`data/municipal_archive_acquisition/`; discovery outputs remain under
`data/municipal_discovery/`. This boundary keeps the claim history and candidate
universe in one versioned project while allowing either pipeline to be rebuilt
independently.

The current acquisition batch consumes the 28 frozen OpenAI targets in
`data/municipal_archive_acquisition/openai_residual_100_20260826/frozen/targets.csv`.
It inventories Wayback captures over the sensitivity window, fetches at most 150
election-nearest pages per candidate-domain, retains raw HTML and extracted text,
and records retryable errors separately from completed negatives. The production
run executes on the droplet with one request per second, five bounded attempts,
a circuit breaker after repeated failures, and a 1.5 GB raw-byte ceiling.

After the batch completes, report (i) candidate-domains with any capture,
(ii) candidate-domains with at least one downloaded page, (iii) pages and text
successfully saved, (iv) terminal versus retryable failures, and (v) any page-cap
or byte-ceiling loss. Use those results to decide whether to expand discovery.
Only accepted grade A or B claims change strict coverage; archive acquisition by
itself does not do so.

Hanno subsequently authorized archive acquisition for every candidate-year
already counted as covered. The authoritative 151 covered candidate-years map
to 153 candidate-domain targets because two candidates used two campaign
domains. This batch is frozen under
`data/municipal_archive_acquisition/covered_151_20260826/`. It includes the
pre-2010 covered cases: the rule against spending discovery effort before 2010
does not justify discarding URLs that the project already possesses. The same
rate limits, retry rules, storage ceiling, and discovery/acquisition boundary
apply. A droplet watcher sends one Slack message when the run either completes
or stops with an actionable failure.
