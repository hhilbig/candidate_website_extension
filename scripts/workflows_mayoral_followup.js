export const meta = {
  name: 'mayoral-followup',
  description: 'Follow-up pass, one city at a time: finish 39 unfinished or flagged repair-search finalists and 8 incomplete legacy attribution checks',
  phases: [
    { title: 'Discover', detail: 'one Sonnet agent per city, sequential', model: 'sonnet' },
    { title: 'Check', detail: 'independent Sonnet checker after each city', model: 'sonnet' },
    { title: 'Legacy', detail: 'legacy attribution items, one city at a time', model: 'sonnet' },
  ],
}

// Repair attempts 2 (args.out 'followup', October 6) and 3 (args.out 'followup2', October 7, approved by Hanno)
// on the stage 2 search blocker. Attempt 3 redoes only the entries still incomplete after attempt 2;
// its input adds followup_note and followup_domains_tried.
// Runs one agent at a time so Wayback requests through mac2 are not overloaded.
const REPO = '/Users/hanno/Documents/GitHub/candidate_website_extension'
const OUT = REPO + '/data/municipal_discovery/top100_coverage/url_discovery/repair_20261004/' + (args.out || 'followup')
const LEGACY_ARGS = REPO + '/data/municipal_discovery/top100_coverage/legacy_site_attribution/followup_check_args.json'

const RULES = `
CONTEXT. This is the follow-up to the October 4 repair search. Each finalist has "reason": "incomplete" (the first repair could not finish, usually because Wayback requests timed out or the search budget ran out) or "checker_flagged_..." (an independent checker found an unchecked domain or a likely site). Read "first_repair_note" and "first_repair_domains_tried" (and, when present, "followup_note" and "followup_domains_tried" from the second attempt) first, and finish exactly what is open. You run alone, so the archive is not under load from other agents.

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

const LEGACY_SCHEMA = {
  type: 'object',
  properties: {
    city_id: { type: 'string' },
    verdicts: { type: 'array', items: { type: 'object', properties: {
      item_id: { type: 'string' },
      status: { type: 'string', enum: ['accepted', 'outside_window', 'not_found', 'incomplete'] },
      checked_capture_url: { type: 'string' }, page_evidence: { type: 'string' }, note: { type: 'string' } },
      required: ['item_id', 'status', 'checked_capture_url', 'page_evidence', 'note'] } },
  },
  required: ['city_id', 'verdicts'],
}

const cities = args.cities, legacy = args.legacy
const results = []
for (const c of cities) {
  const r = await agent(`Find campaign websites for mayoral finalists in ${c.city_name}, ${c.state_abb} (city_id ${c.city_id}, slug ${c.slug}). Read your finalists first: python3 -c 'import json; print(json.dumps([c for c in json.load(open("${OUT}/followup_args.json")) if c["slug"]=="${c.slug}"][0]["finalists"], indent=1))'. Write one output object for every finalist in that list.
${RULES}
Return the summary.`, { label: `discover:${c.slug}`, phase: 'Discover', schema: LEADS_SCHEMA, model: 'sonnet' })
  const v = await agent(`You are an independent checker. Another agent wrote ${OUT}/${c.slug}_url_leads.json for mayoral finalists in ${c.city_name}, ${c.state_abb}. For EVERY entry with status "accepted": re-run the Wayback CDX query through mac2 for the window (365 days up to the election date), open the checked capture (or another in-window capture) and confirm that it is the candidate's own campaign site for that mayoral election, not a social profile, an official city page, a different person with the same name, or a domain that belonged to someone else at the time. For every "not_found" entry, confirm from "searches_run" and "note" that a search actually ran and that every named domain was checked; if not, report severity "minor" with issue "search did not run" or "domain not checked". Do NOT edit the file.
Rules the writer followed:
${RULES}
Report problems: severity "reject" (URL is not this candidate's campaign site or has no in-window capture), "downgrade" (should be outside_window), "minor" (note or field issue). Empty list if all entries hold.`, { label: `check:${c.slug}`, phase: 'Check', schema: CHECK_SCHEMA, model: 'sonnet' })
  results.push({ city: c.slug, discover: r, check: v })
}
const legacyResults = []
for (const c of legacy) {
  legacyResults.push(await agent(`Check whether known URLs are the finalists' own mayoral campaign websites in ${c.city_name}, ${c.state_abb} (city_id ${c.city_id}).
An older pipeline linked each URL to the finalist, and a local model judged saved copies to be campaign sites. That model is weak on election timing (it accepts a real campaign site from a DIFFERENT election year). Decide strictly; do not trust either.
Read your input first: python3 -c 'import json; print(json.dumps([c for c in json.load(open("${LEGACY_ARGS}")) if c["city_id"]=="${c.city_id}"][0], indent=1))'
Each item has the finalist, the election_date, the URL, the archived copies that were saved, and the model's reason.

RULE. accepted only if (a) the URL's site has at least one HTTP 200 capture in the 365 days up to and including the election date, and (b) an archived page from that window names THIS candidate and shows THEIR mayoral campaign in THIS city for THIS election (for example "for Mayor", "elect", "vote", "re-elect", donate/volunteer calls, "Paid for by ... for Mayor"). Content left over from an earlier campaign counts only if the site is visibly being used for this election (a dated post, the right election date, a re-election call in that window). outside_window: it is the candidate's own mayoral campaign site but no qualifying page falls in that window (for example only after the election). not_found: a different person, city or office, an official government page, a news page, a parked/404/empty page, or content only from another election.

HOW. This machine cannot reach web.archive.org; run requests on mac2, one at a time:
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/cdx/search/cdx?url=<host>&matchType=domain&from=<YYYYMMDD window start>&to=<YYYYMMDD election>&fl=timestamp,original,statuscode&filter=statuscode:200&limit=50"'
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/web/<timestamp>id_/<original>"' | python3 -c 'import sys,re,html; t=sys.stdin.read(); t=re.sub(r"(?s)<(script|style).*?</\\1>"," ",t); print(re.sub(r"\\s+"," ",html.unescape(re.sub(r"<[^>]+>"," ",t)))[:4000])'
If a request fails or times out, wait 30 seconds and retry, up to 3 times. If the archive still cannot be read, return status incomplete and say what failed; never return not_found for a check that did not finish.
Prefer the saved captures listed in the input when they fall before the election; otherwise find an in-window root-page capture. Report the capture you read (https://web.archive.org/web/<timestamp>/<original>), a short quote, and a plain note on any doubt. Edit no file. Return one verdict per item.`,
  { label: `legacy:${c.city_name}`, phase: 'Legacy', schema: LEGACY_SCHEMA, model: 'sonnet' }))
}
return { results, legacyResults }