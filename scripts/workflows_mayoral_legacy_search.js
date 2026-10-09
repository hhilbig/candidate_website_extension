export const meta = {
  name: 'mayoral-legacy-search',
  description: 'Search and check campaign sites for 107 mayoral finalists whose only lead is an old roster URL without an archived copy',
  phases: [
    { title: 'Discover', detail: 'one Sonnet agent per city, three cities at a time', model: 'sonnet' },
    { title: 'Check', detail: 'independent Sonnet checker after each city', model: 'sonnet' },
  ],
}

// Approved by Hanno on October 9, 2026. Same acceptance rule as stage 2 and the repair passes.
// Input: legacy_search_20261009/search_args.json (58 cities, 107 finalists). Three cities at a time,
// so Wayback requests through mac2 stay light (wayback-scraping skill).
const REPO = '/Users/hanno/Documents/GitHub/candidate_website_extension'
const OUT = REPO + '/data/municipal_discovery/top100_coverage/url_discovery/legacy_search_20261009'

const RULES = `
CONTEXT. Each finalist has "old_roster_urls": campaign URLs from an older roster. None of them has an archived copy in the 365 days before the election, and many are outdated or wrong (an older campaign, a different domain the candidate used later). No search was ever run for these finalists. Check the old URLs as one lead (also outside the window, and other URL variants), then search for the site the candidate actually used for THIS election.

GOAL. For each listed mayoral finalist, find the URL of the candidate's own campaign website used for that election, and confirm it with the Internet Archive. A Facebook, X/Twitter, Instagram or similar social profile is NOT a campaign website; record it only in "other_profiles". An official city page (council member page, mayor's office page) is NOT a campaign website. These are finalists in the 100 largest US cities, so most of them had a campaign website.

WHERE TO LOOK (leads only; the Wayback check below decides acceptance):
- Web search for the candidate's name, city, "mayor" and the election year. Use the WebSearch tool (load it with ToolSearch if needed) and WebFetch.
- Ballotpedia candidate pages (they often list "Campaign website"), Wikipedia infoboxes and references, Vote411 / League of Women Voters guides, local news candidate profiles, official candidate lists.
- If WebSearch is refused (budget used up) or WebFetch fails, search through mac2 instead, for example: ssh mac2 'curl -sL --max-time 60 -A "Mozilla/5.0" "https://html.duckduckgo.com/html/?q=<url-encoded query>"' | python3 -c 'import sys,re,html; t=sys.stdin.read(); print("\n".join(html.unescape(m) for m in re.findall(r"uddg=([^&\"]+)", t))[:4000])' and fetch result pages the same way. A DuckDuckGo result list counts as a search that ran.
- Typical campaign domains: <name>formayor.com, <name>4mayor.com, <lastname>formayor.com, vote<name>.com, <name><year>.com, <name>.com. Big-city campaigns often use their own name, for example chicagotogether.org.

WAYBACK CHECK (required for acceptance). Run every Wayback request through mac2, which reaches web.archive.org directly. Do NOT use the droplet. For example:
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/cdx/search/cdx?url=<domain>&matchType=host&from=<YYYYMMDD 365 days before election>&to=<YYYYMMDD of election>&fl=timestamp,original,statuscode&filter=statuscode:200&limit=50"'
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/web/<timestamp>id_/<original>"' | head -c 20000
If a request fails, wait 30 seconds and retry, up to 3 times. Accept a URL only if (a) there is at least one HTTP 200 capture of the site in the 365 days up to and including the election date, and (b) an archived page from that window names the candidate and refers to a mayoral campaign (for example "for Mayor", "elect", "vote"). Record the capture you read. If captures exist only after the election or only before the window, record status "outside_window" with the nearest capture timestamps. If a domain later belongs to someone else, judge only the in-window capture.

STATUS RULES. "not_found" means: a web search actually ran AND every domain you found or guessed was checked in the archive, with no accepted result. If the search tools did not work and no fallback worked, or the archive could not be reached after retries, record status "incomplete" and say exactly what failed. Never record "not_found" for a check that did not finish. Never record a URL you did not check. Keep the Wayback request rate modest: one request at a time.

OUTPUT. Write ${OUT}/<slug>_url_leads.json (one object for each finalist in YOUR list only): a list with one object per listed finalist:
 {"candidate_election_id": "...", "candidate_name": "...", "election_date": "YYYY-MM-DD",
  "status": "accepted" | "outside_window" | "not_found" | "incomplete",
  "campaign_url": "<exact URL as archived, or ''>", "registrable_domain": "...",
  "lead_sources": ["<URL where you found the website mentioned>", ...],
  "searches_run": ["<each search query or page you used>"],
  "wayback_window_captures": <int count of 200 captures in window>, "first_capture_in_window": "<14-digit timestamp or ''>",
  "checked_capture_url": "https://web.archive.org/web/<timestamp>/<original>", "page_evidence": "<short quote from the archived page naming the candidate and the mayoral race>",
  "other_profiles": ["<social profile URLs, optional>"], "note": "<plain sentences: what was tried, any doubt>"}
Downloads go to /private/tmp/claude-501/urlrepair/<slug>/ and must be deleted before you finish. Modify no other file.`

const LEADS_SCHEMA = {
  type: 'object',
  properties: {
    city_id: { type: 'string' },
    file_written: { type: 'string' },
    accepted: { type: 'integer' },
    outside_window: { type: 'integer' },
    not_found: { type: 'integer' },
    incomplete: { type: 'integer' },
    notes: { type: 'array', items: { type: 'string' } },
  },
  required: ['city_id', 'file_written', 'accepted', 'outside_window', 'not_found', 'incomplete', 'notes'],
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


const runCity = async (c) => {
  const r = await agent(`Find campaign websites for mayoral finalists in ${c.city_name}, ${c.state_abb} (city_id ${c.city_id}, slug ${c.slug}). Read your finalists first: python3 -c 'import json; print(json.dumps([c for c in json.load(open("${OUT}/search_args.json")) if c["slug"]=="${c.slug}"][0]["finalists"], indent=1))'. Write one output object for every finalist in that list.
${RULES}
Return the summary.`, { label: `discover:${c.slug}`, phase: 'Discover', schema: LEADS_SCHEMA, model: 'sonnet' })
  const v = await agent(`You are an independent checker. Another agent wrote ${OUT}/${c.slug}_url_leads.json for mayoral finalists in ${c.city_name}, ${c.state_abb}. For EVERY entry with status "accepted": re-run the Wayback CDX query through mac2 for the window (365 days up to the election date), open the checked capture (or another in-window capture) and confirm that it is the candidate's own campaign site for that mayoral election, not a social profile, an official city page, a different person with the same name, or a domain that belonged to someone else at the time. For every "not_found" entry, confirm from "searches_run" and "note" that a search actually ran and that every named domain was checked; if not, report severity "minor" with issue "search did not run" or "domain not checked". Do NOT edit the file.
Rules the writer followed:
${RULES}
Report problems: severity "reject" (URL is not this candidate's campaign site or has no in-window capture), "downgrade" (should be outside_window), "minor" (note or field issue). Empty list if all entries hold.`, { label: `check:${c.slug}`, phase: 'Check', schema: CHECK_SCHEMA, model: 'sonnet' })
  return { city: c.slug, discover: r, check: v }
}
const results = []
for (let i = 0; i < args.length; i += 3) {
  results.push(...await parallel(args.slice(i, i + 3).map(c => () => runCity(c))))
}
return results
