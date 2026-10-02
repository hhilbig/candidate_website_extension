export const meta = {
  name: 'mayoral-second-pass',
  description: 'Resolve unresolved mayoral city-years via alternative official routes, verify changed years, repair once',
  phases: [
    { title: 'Resolve', detail: 'one Sonnet agent per city, unresolved years only', model: 'sonnet' },
    { title: 'Verify', detail: 'independent Sonnet check of changed years', model: 'sonnet' },
    { title: 'Repair', detail: 'one repair pass', model: 'sonnet' },
  ],
}

const REPO = '/Users/hanno/Documents/GitHub/candidate_website_extension'
const IN = REPO + '/data/municipal_discovery/top100_coverage/inputs'
const STAGE2 = REPO + '/data/municipal_discovery/top100_coverage/staging2'

const RULES = `
SCOPE. Population: the mayoral election for the city named below. Period: January 1, 2010 to September 14, 2026 (the cutoff). Assess every year 2010 through 2026 (17 years).

SOURCES. Winner, runner-up and vote totals must come from an official election authority: the city clerk or city election office, the county board of elections or county clerk/recorder, or the state election authority (secretary of state, state board). Never cite news, Wikipedia, Ballotpedia or campaign sites as the source of results. You may use them only to find dates and to locate the official document. The election cycle (which years have mayoral elections, term length) should come from an official city source (charter, clerk's list of mayors or elections) or the official results index. Do not infer a no-election year from absence in a search result alone; cite the official source that shows the cycle or lists all elections for that year.

DECISIVE STAGE. Record the stage that decided the office: "general", "runoff", "ranked_choice" or "ranked_choice_final", or "special". If a primary decided the office outright (for example a nonpartisan primary where a majority winner is elected, as in Arizona cities), use stage "primary_nominating" AND give "decisive_stage_source_url" citing the official rule that a majority in the primary elects. For a partisan system (separate party primaries, then a general), the general is decisive; record only the general. If a general is followed by a runoff, record the runoff as decisive (the general is an earlier round; do not record it as a separate verified election). Give a year status "earlier_round_only" when a mayoral first round was held that year but the decisive round fell in a later year or after the September 14, 2026 cutoff (for example a November first round with a runoff the next spring; record the runoff under its own year). Do not use "no_election" for a year in which any mayoral round was held. Some elections may already exist in other project files; record them anyway, the integrator reconciles duplicates.

CANDIDATES. Exactly one "winner" and one "runner_up" (the candidate with the second-most votes in the decisive stage), with official vote totals as integers. If ballot lines are aggregated (fusion voting), sum them and explain in verification_note. If only one candidate appeared, record only the winner (the build treats it as uncontested). For ranked-choice, use the final-round totals and say so. Leave "candidate_year_id" as "".

UNCERTAINTY. Never guess. If an official source cannot be found or opened, set the calendar year to "unresolved" and explain exactly what was tried (URLs and errors). An unresolved year is acceptable; a wrong one is not. Do not fabricate URLs: every URL you write must be one you actually opened (or that returned a specific error you record).

FILE FORMATS. Write two JSON files (UTF-8, indent 1) in ${STAGE2}:
(a) <slug>_calendar.json: a list of exactly 17 objects, one per year 2010..2026, each:
 {"city_id": "<city_id exactly as given>", "year": <int>, "status": one of "complete" | "no_election" | "indirect_selection" | "earlier_round_only" | "unresolved",
  "expected_election_dates": [ISO dates of the decisive election(s) in that year; MUST be [] unless status is "complete"],
  "source_url": "<official source>", "additional_source_urls": [...], "note": "<what the source shows, plain sentences>"}
 Use "complete" only when every mayoral election in that year is recorded in the results file. "indirect_selection" if the council selects the mayor.
(b) <slug>_results.json: a list (possibly empty) of objects, one per decisive election inside the window:
 {"city_name": "<city_name as given>", "state_abb": "..", "roster_place_fips": "<as given>", "election_date": "YYYY-MM-DD",
  "election_type": "regular" | "special", "stage": see above, "verification_status": "verified_decisive" (or "unresolved" if you could not confirm),
  "certification_status": "certified" | "source_verified", "source_url": "<official results document>", "source_locator": "<where in the document: page, table, row>",
  "certification_source_url": "<certifying document or results index, may equal source_url>", "additional_source_urls": [...],
  "verification_note": "<plain sentences: what the source shows, totals, any aggregation>",
  "candidates": [{"name": "..", "role": "winner", "votes": <int>, "candidate_year_id": ""}, {"name": "..", "role": "runner_up", "votes": <int>, "candidate_year_id": ""}],
  "decisive_stage_source_url": "<only for primary_nominating>"}
 Every year with status "complete" must have its expected_election_dates matching election_date values in the results file, and vice versa.

EXAMPLES. Read ${IN}/mesa_calendar.json, ${IN}/mesa_results.json (primary-decisive cities) and ${IN}/new_york_results.json (general elections, fusion aggregation) before writing. Match their style.

TOOLS. Use WebSearch and WebFetch. Many official PDFs can be read with WebFetch; if a PDF fails, download with curl into /private/tmp/claude-501/mayoral2/<slug>/ (create it) and read with pdftotext -layout. Delete that folder before you finish. Read column headers carefully in tabular results so you take the mayoral column and the citywide grand total, never a precinct or council row. Do not modify any file outside ${STAGE2}, and only your own city's two files there.`

const ROUTES = `ALTERNATIVE ROUTES (the first pass already failed on the obvious city page; read each unresolved year's note to see what was tried, and do not repeat a failed request more than once):
- The county election board or county clerk/recorder that runs the city's elections (official canvass, statement of votes, Clarity ENR at results.enr.clarityelections.com).
- The state election authority (secretary of state or state election board) election results archive, if it publishes municipal results.
- City council or city clerk minutes, resolutions or ordinances that canvass and declare the mayoral result (these are official and often list vote totals).
- Archived copies of the official pages and PDFs on web.archive.org (cite the archived URL; an archived official page counts as official). Try the CDX API: https://web.archive.org/cdx/search/cdx?url=<url>&output=json
- For the election cycle: the city charter (municode, amlegal, library.municode.com, ecode360), the clerk's list of past mayors with terms, or council minutes.
- If the city does not elect its mayor directly (the council selects the mayor), use status "indirect_selection" with an official source.`

const RESEARCH_SCHEMA = {
  type: 'object',
  properties: {
    city_id: { type: 'string' },
    files_written: { type: 'array', items: { type: 'string' } },
    years_complete: { type: 'integer' },
    years_no_election: { type: 'integer' },
    years_unresolved: { type: 'integer' },
    elections_verified: { type: 'integer' },
    hardest_problems: { type: 'array', items: { type: 'string' } },
  },
  required: ['city_id', 'files_written', 'years_complete', 'years_no_election', 'years_unresolved', 'elections_verified', 'hardest_problems'],
}

const VERIFY_SCHEMA = {
  type: 'object',
  properties: {
    city_id: { type: 'string' },
    elections_checked: { type: 'integer' },
    no_election_years_checked: { type: 'integer' },
    problems: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          year: { type: 'integer' },
          severity: { type: 'string', enum: ['wrong_fact', 'unsupported', 'format', 'minor'] },
          issue: { type: 'string' },
          evidence: { type: 'string' },
        },
        required: ['year', 'severity', 'issue', 'evidence'],
      },
    },
  },
  required: ['city_id', 'elections_checked', 'no_election_years_checked', 'problems'],
}


const cities = args.map(c => ({ ...c, orig_calendar: REPO + "/" + c.orig_calendar, orig_results: c.orig_results ? REPO + "/" + c.orig_results : "" }))

const results = await pipeline(
  cities,
  (c) => agent(`Second pass for the mayoral elections of ${c.city_name}, ${c.state_abb}. city_id="${c.city_id}", roster_place_fips="${c.roster_place_fips}", slug="${c.slug}".
The files ${STAGE2}/${c.slug}_calendar.json and ${STAGE2}/${c.slug}_results.json are copies of the current project files. These years are "unresolved": ${JSON.stringify(c.unresolved_years)}. Your job is to resolve as many of them as an official source allows. Change ONLY those years' calendar entries and add or correct ONLY the results records for elections in those years. Leave every other entry exactly as it is.
${RULES}
${ROUTES}

If after trying the routes a year still cannot be established from an official source, leave it "unresolved" and append to its note what you tried in this pass. Re-read both files at the end and check the format rules. Return the summary (years_* counts for the whole file; list in hardest_problems which years you resolved and which stay unresolved and why).`,
    { label: `resolve:${c.slug}`, phase: 'Resolve', schema: RESEARCH_SCHEMA, model: 'sonnet' }),
  (r, c) => agent(`You are an independent checker. Another agent edited ${STAGE2}/${c.slug}_calendar.json and ${STAGE2}/${c.slug}_results.json for the mayoral elections of ${c.city_name}, ${c.state_abb}. The originals are ${c.orig_calendar} and ${c.orig_results || '(no results file existed)'}. Compare each file with its original to find every changed year (the agent was allowed to change only ${JSON.stringify(c.unresolved_years)}). Report any change outside those years as a format problem.
For every changed year: open each cited source (WebFetch, or curl into /private/tmp/claude-501/mayoral2/check_${c.slug}/ plus pdftotext -layout; delete that folder before you finish) and confirm the date, stage, winner and runner-up names and vote totals, or the no_election / indirect_selection / earlier_round_only finding. Confirm the sources are official (archived copies of official pages count). Do NOT edit the files.
Rules the writer followed:
${RULES}
Report every problem with the year, severity (wrong_fact, unsupported, format, minor), what is wrong, and the evidence. Empty list if everything checks out.`,
    { label: `verify:${c.slug}`, phase: 'Verify', schema: VERIFY_SCHEMA, model: 'sonnet' }).then(v => ({ research: r, verify: v })),
  (rv, c) => {
    const probs = (rv && rv.verify && rv.verify.problems || []).filter(p => p.severity !== 'minor')
    if (!probs.length) return { city: c.slug, research: rv && rv.research, verify: rv && rv.verify, repaired: false }
    return agent(`An independent checker found problems in ${STAGE2}/${c.slug}_calendar.json and ${STAGE2}/${c.slug}_results.json (mayoral elections of ${c.city_name}, ${c.state_abb}; second pass, editable years ${JSON.stringify(c.unresolved_years)} only). Fix them. If a fact cannot be established from an official source, return that year to "unresolved" (remove or mark its results record unresolved, keep the calendar consistent) and explain in the note. Do not change years outside ${JSON.stringify(c.unresolved_years)}; if the checker reports such a change, restore the original from ${c.orig_calendar} / ${c.orig_results || '(none)'}.
Problems:
${JSON.stringify(probs, null, 1)}
Rules:
${RULES}
Use /private/tmp/claude-501/mayoral2/${c.slug}/ for downloads and delete it before you finish. Re-read both files and confirm the format rules. Return the summary.`,
      { label: `repair:${c.slug}`, phase: 'Repair', schema: RESEARCH_SCHEMA, model: 'sonnet' })
      .then(fix => ({ city: c.slug, research: rv.research, verify: rv.verify, repaired: true, repair: fix }))
  },
)
return results
