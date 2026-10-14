export const meta = {
  name: 'mayoral-search-pass4',
  description: 'Fourth search pass: Sonnet searches campaign sites for 99 mayoral finalists, with an independent check per city',
  phases: [
    { title: 'Discover', detail: 'one Sonnet agent per city, two cities at a time', model: 'sonnet' },
    { title: 'Check', detail: 'independent Sonnet checker after each city', model: 'sonnet' },
  ],
}
// Approved by Hanno on October 10, 2026 (docs/status.md, approval (b)); repair attempt 4 on the stage 2 search blocker.
// Input: pass4_args.json from scripts/build_mayoral_pass4_args.py. Model: Sonnet, chosen after the Haiku pilot.
const REPO = '/Users/hanno/Documents/GitHub/candidate_website_extension'
// args.out overrides the output folder. The October 13 rerun used a folder outside ~/Documents,
// because macOS refused the agents access to ~/Documents for the last 16 cities on October 10.
const OUT = args.out || REPO + '/data/municipal_discovery/top100_coverage/url_discovery/pass4_20261010'
// args.wayback_host = 'droplet' sends archive requests through the droplet (October 14 rerun of the incomplete entries,
// while a stage 3 scrape used mac2's archive access). Brave searches always go through mac2.
const WB_NAME = args.wayback_host === 'droplet' ? 'the droplet' : 'mac2'
const WB_SSH = args.wayback_host === 'droplet' ? 'ssh -o ConnectTimeout=20 root@droplet' : 'ssh -n mac2'
const WB_NOT = args.wayback_host === 'droplet' ? 'mac2' : 'the droplet'
const RULES = `
CONTEXT. Each finalist has a "group":
- "stage2_only": the first search ran without a working search engine. Search from the start.
- "check_incomplete": an earlier check of a roster URL could not finish. Read "earlier_attempts"; the roster domain often has no in-window capture, so look for a different domain.
- "excluded": a site check rejected the only known site for this election (see "earlier_attempts" and "site_check_rejected_targets"; the reason was a different person, not a campaign site, or content from a different election). Do NOT accept that rejected domain again for this election unless an in-window capture clearly shows this candidate's campaign for THIS election. Search for another site.
- "new_finalist": never searched. If "known_roster_urls" lists URLs, check them first, then search anyway.
Read "earlier_attempts" first and do not repeat checks that already finished.

GOAL. For each listed mayoral finalist, find the URL of the candidate's own campaign website used for that election, and confirm it with the Internet Archive. A Facebook, X/Twitter, Instagram or similar social profile is NOT a campaign website; record it only in "other_profiles". An official city page (council member page, mayor's office page) is NOT a campaign website. These are finalists in the 100 largest US cities, so most of them had a campaign website.

WHERE TO LOOK (leads only; the Wayback check below decides acceptance):
- Search with Brave through mac2. Do not use the WebSearch tool. For example:
  ssh -n mac2 'curl -sL --max-time 60 -A "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36" "https://search.brave.com/search?q=<url-encoded query>"' | python3 -c 'import sys,re; t=sys.stdin.read(); print("\\n".join(dict.fromkeys(u for u in re.findall(r"href=\\"(https?://[^\\"]+)\\"", t) if "brave" not in u))[:3000])'
  Query the candidate's name, city, "mayor" and the election year, then variations ("campaign", "for mayor", "website"). Wait 5 seconds between searches. If Brave returns no result links, wait 60 seconds and retry, up to 3 times.
- ARCHIVED BALLOTPEDIA (always do this): Ballotpedia candidate pages often list "Campaign website". Read the archived copy from the election year through ${WB_NAME}, for example
  ${WB_SSH} 'curl -sL --max-time 60 "https://web.archive.org/web/<election year>1231/https://ballotpedia.org/<First_Last>"' | grep -io 'campaign website[^<]*<[^>]*href="[^"]*"' | head
  Try the name forms Ballotpedia uses (First_Last, First_M._Last, nickname). Also read the live Ballotpedia page and the Wikipedia article for the election through WebFetch.
- Local news candidate profiles, Vote411 / League of Women Voters guides, official candidate lists.
- Typical campaign domains: <name>formayor.com, <name>4mayor.com, <lastname>formayor.com, vote<name>.com, vote<firstname>.com, reelect<name>.com, <name><year>.com, <name>.com, .org/.net/.us variants, city abbreviations.

WAYBACK CHECK (required for acceptance). Run every Wayback request through ${WB_NAME}, which reaches web.archive.org directly. Do NOT use ${WB_NOT} for Wayback requests. For example:
  ${WB_SSH} 'curl -s --max-time 60 "https://web.archive.org/cdx/search/cdx?url=<domain>&matchType=host&from=<YYYYMMDD 365 days before election>&to=<YYYYMMDD of election>&fl=timestamp,original,statuscode&filter=statuscode:200&limit=50"'
  ${WB_SSH} 'curl -s --max-time 60 "https://web.archive.org/web/<timestamp>id_/<original>"' | head -c 20000
If a request fails or times out, wait 30 seconds and retry, up to 5 times. Accept a URL only if (a) there is at least one HTTP 200 capture of the site in the 365 days up to and including the election date, and (b) an archived page from that window names the candidate and refers to a mayoral campaign (for example "for Mayor", "elect", "vote"). Record the capture you read. If captures exist only after the election or only before the window, record status "outside_window" with the nearest capture timestamps. If a domain later belongs to someone else, judge only the in-window capture.

STATUS RULES. "not_found" means: a web search actually ran AND every domain you found or guessed was checked in the archive, with no accepted result. If the search tools did not work and no fallback worked, or the archive could not be reached after retries, record status "incomplete" and say exactly what failed. Never record "not_found" for a check that did not finish. Never record a URL you did not check. Keep the Wayback request rate modest: one request at a time.

OUTPUT. Write ${OUT}/<slug>_url_leads.json (one object for each finalist in YOUR list only), then read the file back to confirm it exists and parses as JSON: a list with one object per listed finalist:
 {"candidate_election_id": "...", "candidate_name": "...", "election_date": "YYYY-MM-DD", "group": "<from input>",
  "status": "accepted" | "outside_window" | "not_found" | "incomplete",
  "campaign_url": "<exact URL as archived, or ''>", "registrable_domain": "...",
  "lead_sources": ["<URL where you found the website mentioned>", ...],
  "searches_run": ["<each search query or page you used>"],
  "wayback_window_captures": <int count of 200 captures in window>, "first_capture_in_window": "<14-digit timestamp or ''>",
  "checked_capture_url": "https://web.archive.org/web/<exact 14-digit timestamp from CDX>/<original>", "page_evidence": "<short quote from the archived page naming the candidate and the mayoral race>",
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

const CITIES = args.cities
const results = []
for (let i = 0; i < CITIES.length; i += 2) {
  results.push(...await pipeline(CITIES.slice(i, i + 2),
    c => agent(`Find campaign websites for mayoral finalists in ${c.city_name}, ${c.state_abb} (city_id ${c.city_id}, slug ${c.slug}). Read your finalists first: python3 -c 'import json; print(json.dumps([c for c in json.load(open("${OUT}/pass4_args.json")) if c["slug"]=="${c.slug}"][0]["finalists"], indent=1))'. Write one output object for every finalist in that list.
${RULES}
Return the summary.`, { label: `discover:${c.slug}`, phase: 'Discover', schema: LEADS_SCHEMA, model: 'sonnet' }),
    (r, c) => agent(`You are an independent checker. Another agent wrote ${OUT}/${c.slug}_url_leads.json for mayoral finalists in ${c.city_name}, ${c.state_abb}. If the file is missing or does not parse, report one "minor" problem with candidate_election_id "" and issue "file missing" and stop. For EVERY entry with status "accepted": re-run the Wayback CDX query through ${WB_NAME} for the window (365 days up to the election date), open the checked capture (or another in-window capture) and confirm that it is the candidate's own campaign site for that mayoral election, not a social profile, an official city page, a different person with the same name, a site from a different election, or a domain that belonged to someone else at the time. For every "not_found" entry, confirm from "searches_run" and "note" that a search actually ran and that every named domain was checked; if not, report severity "minor" with issue "search did not run" or "domain not checked". Do NOT edit the file.
Rules the writer followed:
${RULES}
Report problems: severity "reject" (URL is not this candidate's campaign site for this election or has no in-window capture), "downgrade" (should be outside_window), "minor" (note or field issue). Empty list if all entries hold.`,
      { label: `check:${c.slug}`, phase: 'Check', schema: CHECK_SCHEMA, model: 'sonnet' })
      .then(v => ({ city: c.slug, discover: r, check: v }))))
  log(`${Math.min(i + 2, CITIES.length)} of ${CITIES.length} cities done`)
}
return results
