export const meta = {
  name: 'mayoral-url-discovery',
  description: 'Find and Wayback-verify campaign website URLs for mayoral finalists without a URL lead, one city per agent, independent check',
  phases: [
    { title: 'Discover', detail: 'one Sonnet agent per city finds candidate campaign URLs and checks Wayback captures', model: 'sonnet' },
    { title: 'Check', detail: 'independent Sonnet agent re-tests every accepted URL', model: 'sonnet' },
  ],
}

// Launch from a fresh session (web-search budget):
//   Workflow({scriptPath: 'scripts/workflows_mayoral_url_discovery.js',
//             args: <contents of data/municipal_discovery/top100_coverage/url_discovery/stage2_args.json>})
// Input: 96 cities, 473 verified finalists without a URL lead (built 2026-10-02).

const REPO = '/Users/hanno/Documents/GitHub/candidate_website_extension'
const OUT = REPO + '/data/municipal_discovery/top100_coverage/url_discovery'

const RULES = `
GOAL. For each listed mayoral finalist, find the URL of the candidate's own campaign website used for that election, and confirm it with the Internet Archive. A Facebook, X/Twitter, Instagram or similar social profile is NOT a campaign website; record it only in "other_profiles". An official city page (council member page, mayor's office page) is NOT a campaign website.

WHERE TO LOOK (leads only; the Wayback check below decides acceptance):
- Official candidate lists, candidate filings or voter guides from the city clerk, county election office or state (they often list a website).
- League of Women Voters / Vote411 guides, Ballotpedia candidate pages, local news candidate profiles and debate pages (leads only).
- Search the web for the candidate's name, city and "mayor" plus the election year. Typical campaign domains: <name>formayor.com, <name>4mayor.com, <lastname>formayor.com, vote<name>.com, <name><year>.com.

WAYBACK CHECK (required for acceptance). This machine cannot reach web.archive.org directly; run Wayback requests on the droplet, for example:
  ssh root@droplet 'curl -s --max-time 60 "https://web.archive.org/cdx/search/cdx?url=<domain>&matchType=host&from=<YYYY of election - 1><MMDD>&to=<YYYYMMDD of election>&fl=timestamp,original,statuscode&filter=statuscode:200&limit=50"'
  ssh root@droplet 'curl -s --max-time 60 "https://web.archive.org/web/<timestamp>id_/<original>"' | head -c 20000
Accept a URL only if (a) there is at least one HTTP 200 capture of the site in the 365 days up to and including the election date, and (b) an archived page from that window names the candidate and refers to a mayoral campaign (for example "for Mayor", "elect", "vote"). Record the capture you read. If captures exist only after the election or only before the window, record status "outside_window" with the nearest capture timestamps. If a domain later belongs to someone else, judge only the in-window capture.

DO NOT GUESS. If no URL can be confirmed, status "not_found" with what you tried. Never record a URL you did not check. Keep the Wayback request rate modest (one request at a time, no parallel loops over many timestamps).

OUTPUT. Write ${OUT}/<slug>_url_leads.json: a list with one object per listed finalist:
 {"candidate_election_id": "...", "candidate_name": "...", "election_date": "YYYY-MM-DD",
  "status": "accepted" | "outside_window" | "not_found",
  "campaign_url": "<exact URL as archived, or ''>", "registrable_domain": "...",
  "lead_sources": ["<URL where you found the website mentioned>", ...],
  "wayback_window_captures": <int count of 200 captures in window>, "first_capture_in_window": "<14-digit timestamp or ''>",
  "checked_capture_url": "https://web.archive.org/web/<timestamp>/<original>", "page_evidence": "<short quote from the archived page naming the candidate and the mayoral race>",
  "other_profiles": ["<social profile URLs, optional>"], "note": "<plain sentences: what was tried, any doubt>"}
Downloads go to /private/tmp/claude-501/urldisc/<slug>/ and must be deleted before you finish. Modify no other file.`

const LEADS_SCHEMA = {
  type: 'object',
  properties: {
    city_id: { type: 'string' },
    file_written: { type: 'string' },
    accepted: { type: 'integer' },
    outside_window: { type: 'integer' },
    not_found: { type: 'integer' },
    notes: { type: 'array', items: { type: 'string' } },
  },
  required: ['city_id', 'file_written', 'accepted', 'outside_window', 'not_found', 'notes'],
}

const CHECK_SCHEMA = {
  type: 'object',
  properties: {
    city_id: { type: 'string' },
    checked: { type: 'integer' },
    problems: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          candidate_election_id: { type: 'string' },
          severity: { type: 'string', enum: ['reject', 'downgrade', 'minor'] },
          issue: { type: 'string' },
          evidence: { type: 'string' },
        },
        required: ['candidate_election_id', 'severity', 'issue', 'evidence'],
      },
    },
  },
  required: ['city_id', 'checked', 'problems'],
}

const results = await pipeline(
  args,
  (c) => agent(`Find campaign websites for mayoral finalists in ${c.city_name}, ${c.state_abb} (city_id ${c.city_id}, slug ${c.slug}). These verified finalists have no URL lead yet:
${JSON.stringify(c.finalists, null, 1)}
${RULES}
Return the summary.`,
    { label: `discover:${c.slug}`, phase: 'Discover', schema: LEADS_SCHEMA, model: 'sonnet' }),
  (r, c) => agent(`You are an independent checker. Another agent wrote ${OUT}/${c.slug}_url_leads.json for mayoral finalists in ${c.city_name}, ${c.state_abb}. For EVERY entry with status "accepted": re-run the Wayback CDX query on the droplet for the window (365 days up to the election date), open the checked capture (or another in-window capture) and confirm that it is the candidate's own campaign site for that mayoral election, not a social profile, an official city page, a different person with the same name, or a domain that belonged to someone else at the time. Also spot-check two "not_found" entries by trying the most obvious domain patterns. Do NOT edit the file.
Rules the writer followed:
${RULES}
Report problems: severity "reject" (URL is not this candidate's campaign site or has no in-window capture), "downgrade" (should be outside_window), "minor" (note or field issue). Empty list if all accepted entries hold.`,
    { label: `check:${c.slug}`, phase: 'Check', schema: CHECK_SCHEMA, model: 'sonnet' }).then(v => ({ city: c.slug, discover: r, check: v })),
)
return results
