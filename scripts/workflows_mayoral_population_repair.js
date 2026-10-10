export const meta = {
  name: 'mayoral-population-repair',
  description: 'Targeted repair of the mayoral population: verify listed out-of-population elections and unresolved years from official sources',
  phases: [
    { title: 'Research', detail: 'one Sonnet agent per city writes staging calendar and results files', model: 'sonnet' },
    { title: 'Verify', detail: 'independent Sonnet agent re-opens every cited source', model: 'sonnet' },
    { title: 'Repair', detail: 'one repair pass for cities with problems', model: 'sonnet' },
  ],
}

const REPO = '/Users/hanno/Documents/GitHub/candidate_website_extension'
const IN = REPO + '/data/municipal_discovery/top100_coverage/inputs'
const STAGE = REPO + '/data/municipal_discovery/top100_coverage/staging/repair_20261010'
// Approved by Hanno on October 10, 2026 (docs/status.md, item a). Each city gets only its listed years.

const RULES = `
SCOPE. Population: the mayoral election for the city named below. Period: January 1, 2010 to September 14, 2026 (the cutoff). This is a targeted repair: assess ONLY the years listed for your city. These years failed in an earlier pass, mostly because official sites blocked automated access or documents did not open; the earlier notes are in the input. The project already holds the other years; do not redo them.

SOURCES. Winner, runner-up and vote totals must come from an official election authority: the city clerk or city election office, the county board of elections or county clerk/recorder, or the state election authority (secretary of state, state board). Never cite news, Wikipedia, Ballotpedia or campaign sites as the source of results. You may use them only to find dates and to locate the official document. The election cycle (which years have mayoral elections, term length) should come from an official city source (charter, clerk's list of mayors or elections) or the official results index. Do not infer a no-election year from absence in a search result alone; cite the official source that shows the cycle or lists all elections for that year.

DECISIVE STAGE. Record the stage that decided the office: "general", "runoff", "ranked_choice" or "ranked_choice_final", or "special". If a primary decided the office outright (for example a nonpartisan primary where a majority winner is elected, as in Arizona cities), use stage "primary_nominating" AND give "decisive_stage_source_url" citing the official rule that a majority in the primary elects. For a partisan system (separate party primaries, then a general), the general is decisive; record only the general. If a general is followed by a runoff, record the runoff as decisive (the general is an earlier round; do not record it as a separate verified election). Give a year status "earlier_round_only" when a mayoral first round was held that year but the decisive round fell in a later year or after the September 14, 2026 cutoff (for example a November first round with a runoff the next spring; record the runoff under its own year). Do not use "no_election" for a year in which any mayoral round was held. Some elections may already exist in other project files; record them anyway, the integrator reconciles duplicates.

CANDIDATES. Exactly one "winner" and one "runner_up" (the candidate with the second-most votes in the decisive stage), with official vote totals as integers. If ballot lines are aggregated (fusion voting), sum them and explain in verification_note. If only one candidate appeared, record only the winner (the build treats it as uncontested). For ranked-choice, use the final-round totals and say so. Leave "candidate_year_id" as "".

UNCERTAINTY. Never guess. If an official source cannot be found or opened, set the calendar year to "unresolved" and explain exactly what was tried (URLs and errors). An unresolved year is acceptable; a wrong one is not. Do not fabricate URLs: every URL you write must be one you actually opened (or that returned a specific error you record).

FILE FORMATS. Write two JSON files (UTF-8, indent 1) in ${STAGE}:
(a) <slug>_calendar.json: a list with one object for each listed year (no other years), each:
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
  "total_votes": <int: all votes cast for mayor in the decisive contest, all candidates and write-ins>, "total_votes_basis": "<what the total counts, e.g. 'sum of all candidates incl. write-ins, citywide'>",
  "decisive_stage_source_url": "<only for primary_nominating>"}
 Every listed year with status "complete" must have its expected_election_dates matching election_date values in the results file, and vice versa.

EXAMPLES. Read ${IN}/mesa_calendar.json, ${IN}/mesa_results.json (primary-decisive cities) and ${IN}/new_york_results.json (general elections, fusion aggregation) before writing. Match their style.

TOOLS. Use WebSearch and WebFetch. If WebSearch is refused (budget used up), search through mac2 with Brave, for example: ssh -n mac2 'curl -sL --max-time 60 -A "Mozilla/5.0" "https://search.brave.com/search?q=<url-encoded query>"' and read the result links. Official documents that block direct access are often in the Internet Archive: query ssh -n mac2 'curl -s --max-time 60 "https://web.archive.org/cdx/search/cdx?url=<official url or prefix>&matchType=prefix&limit=50"' and read the archived copy (https://web.archive.org/web/<timestamp>id_/<url>). An archived copy of an official document counts as the official source; cite the archive URL in source_url and the original URL in additional_source_urls. You may also fetch official sites through mac2 with a browser user agent. Many official PDFs can be read with WebFetch; if a PDF fails, try curl to a temp file under /private/tmp/claude-501/ and pdftotext -layout. Read column headers carefully in tabular results so you take the mayoral column and the citywide grand total, never a precinct or council row. Do not modify any file outside ${STAGE}, and only your own city's two files there.`

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

const cities = args
// Run at most this many cities at once through the pipeline's own cap.

const results = await pipeline(
  cities,
  (c) => agent(`You are assessing mayoral elections for ${c.city_name}, ${c.state_abb}. city_id="${c.city_id}", roster_place_fips="${c.roster_place_fips}", slug="${c.slug}".
Years to assess: ${JSON.stringify(c.years)}. What the earlier pass found or tried is in the "focus" field of your entry in ${STAGE}/repair_args.json; read it first: python3 -c 'import json; print([c for c in json.load(open("${STAGE}/repair_args.json")) if c["slug"]=="${c.slug}"][0]["focus"])'
The project's existing files for this city are ${IN}/${c.slug}_calendar.json and ${IN}/${c.slug}_results.json (if present); read them for context, do not edit them.
${RULES}

Write ${STAGE}/${c.slug}_calendar.json and ${STAGE}/${c.slug}_results.json, then re-read both files and check: one calendar row per listed year, statuses valid, expected dates only on complete years and matching the results file, each verified election has exactly one winner and one runner_up (or only a winner if uncontested). Return the summary.`,
    { label: `research:${c.slug}`, phase: 'Research', schema: RESEARCH_SCHEMA, model: 'sonnet' }),
  (r, c) => agent(`You are an independent checker. Another agent wrote ${STAGE}/${c.slug}_calendar.json and ${STAGE}/${c.slug}_results.json for the mayoral elections of ${c.city_name}, ${c.state_abb}, for the years ${JSON.stringify(c.years)} only. Your job is to find errors. Assume nothing is right until you have seen it in the source.

For EVERY election in the results file: open the cited source_url (WebFetch, or curl plus pdftotext -layout for PDFs) and confirm the date, the stage, the winner and runner-up names, and both vote totals against the citywide mayoral totals. Confirm the stage is the decisive one under the rules below (for example a general followed by a runoff means the runoff is decisive; a primary is decisive only with an official rule cited). Check that the sources are official election authorities, not news or Wikipedia.
For the calendar: check that no year with a real mayoral election is marked no_election (search for each election year independently), and open the cited source for every no_election year. Check total_votes against the source. Check the format rules.

The rules the writer followed:
${RULES}

Do NOT edit the files. Report every problem with the year, severity (wrong_fact = a name, vote, date, stage or missing election is wrong; unsupported = the cited source does not show it or is not official; format = file rule broken; minor = wording), what is wrong, and the evidence (URL and what it shows). If everything checks out, return an empty problems list.`,
    { label: `verify:${c.slug}`, phase: 'Verify', schema: VERIFY_SCHEMA, model: 'sonnet' }).then(v => ({ research: r, verify: v })),
  (rv, c) => {
    const probs = (rv && rv.verify && rv.verify.problems || []).filter(p => p.severity !== 'minor')
    if (!probs.length) return { city: c.slug, research: rv && rv.research, verify: rv && rv.verify, repaired: false }
    return agent(`An independent checker found problems in ${STAGE}/${c.slug}_calendar.json and ${STAGE}/${c.slug}_results.json (mayoral elections of ${c.city_name}, ${c.state_abb}). Fix them. For each problem, re-open the official source and correct the file. If you cannot establish the fact from an official source, mark that year "unresolved" (and set the election's verification_status to "unresolved" or remove it, keeping the calendar consistent) and explain in the note. Do not argue with the checker without evidence; if you are confident the checker is wrong, keep the value and add the evidence to the note.

Problems:
${JSON.stringify(probs, null, 1)}

Rules:
${RULES}

After editing, re-read both files and confirm the format rules. Return the summary.`,
      { label: `repair:${c.slug}`, phase: 'Repair', schema: RESEARCH_SCHEMA, model: 'sonnet' })
      .then(fix => ({ city: c.slug, research: rv.research, verify: rv.verify, repaired: true, repair: fix }))
  },
)
return results
