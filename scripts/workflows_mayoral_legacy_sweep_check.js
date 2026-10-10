export const meta = {
  name: 'mayoral-legacy-sweep-check',
  description: 'Agent check of domain-sweep hits for legacy-lead finalists the agent search left open',
  phases: [{ title: 'Check', detail: 'one Sonnet agent per city, three at a time; returns verdicts, edits no file', model: 'sonnet' }],
}
// Option 3, approved by Hanno on October 7, 2026. Input: legacy_search_20261009/sweep_check_args.json
// (hits from the 120 days after the election that passed the name and "mayor" screen).
// Three agents at a time, so Wayback requests through mac2 stay light (wayback-scraping skill).
const REPO = '/Users/hanno/Documents/GitHub/candidate_website_extension'
const ARGS = REPO + '/data/municipal_discovery/top100_coverage/url_discovery/legacy_search_20261009/sweep_check_args.json'
const SCHEMA = {
  type: 'object',
  properties: {
    city_id: { type: 'string' },
    verdicts: { type: 'array', items: { type: 'object', properties: {
      candidate_election_id: { type: 'string' },
      status: { type: 'string', enum: ['accepted', 'outside_window', 'not_found', 'incomplete'] },
      campaign_url: { type: 'string' }, registrable_domain: { type: 'string' },
      wayback_window_captures: { type: 'integer' }, first_capture_in_window: { type: 'string' },
      checked_capture_url: { type: 'string' }, page_evidence: { type: 'string' }, note: { type: 'string' } },
      required: ['candidate_election_id', 'status', 'campaign_url', 'registrable_domain', 'wayback_window_captures',
                 'first_capture_in_window', 'checked_capture_url', 'page_evidence', 'note'] } },
  },
  required: ['city_id', 'verdicts'],
}
const prompt = (c) => `Check candidate campaign websites for mayoral finalists in ${c.city_name}, ${c.state_abb} (city_id ${c.city_id}).
A script generated domain patterns (for example lastnameformayor.com) and found archived pages (before or after the election, see each hit's window) that mention the candidate's last name and "mayor". These are leads only. An agent search left these finalists open; its status and note are in the input. Decide strictly for each finalist.
Read your input first: python3 -c 'import json; print(json.dumps([c for c in json.load(open("${ARGS}")) if c["city_id"]=="${c.city_id}"][0], indent=1))'
Each finalist has an election_date, an after_window (the day after the election to 120 days after) and one or more hits (domain, an archived capture URL, its page title).

RULE. A domain qualifies only if an archived page names THIS candidate and shows THEIR mayoral campaign in THIS city for THIS election (for example "for Mayor", "elect", "vote", "re-elect", donate or volunteer calls, a thank-you or victory message for this election). Reject: a different person with the same surname, a different city or office (council, county, state legislature, Congress), an official city-government page, a parked, spam or for-sale page, a news or fan site, or a site whose content is only from another election.
Status for a qualifying domain: first run the CDX query for the 365 days up to and including the election date. If that window has an HTTP 200 capture whose page qualifies, status accepted, and report that capture. Otherwise, if a qualifying page falls in the after_window, status outside_window, and report that capture. If no hit qualifies, not_found with campaign_url ''. If the archive cannot be read after retries, incomplete, and say what failed; never return not_found for a check that did not finish.

HOW. This machine cannot reach web.archive.org; run requests on mac2, one at a time, for example:
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/cdx/search/cdx?url=<domain>&matchType=domain&from=<YYYYMMDD>&to=<YYYYMMDD>&fl=timestamp,original,statuscode&filter=statuscode:200&limit=50"'
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/web/<timestamp>id_/<original>"' | python3 -c 'import sys,re,html; t=sys.stdin.read(); t=re.sub(r"(?s)<(script|style).*?</\\1>"," ",t); print(re.sub(r"\\s+"," ",html.unescape(re.sub(r"<[^>]+>"," ",t)))[:4000])'
If a request fails or times out, wait 30 seconds and retry, up to 3 times. Read at least one page for every accepted or outside_window domain. Report the capture you read in checked_capture_url (https://web.archive.org/web/<timestamp>/<original>), the exact campaign_url as archived (normally the site root), registrable_domain as the bare domain only, the count of HTTP 200 captures in the window that decided the status, the first timestamp in that window, and a short quote in page_evidence. Write plain short notes on any doubt. Edit no file. Return one verdict per finalist in the input.`
const out = []
for (let i = 0; i < args.length; i += 3) {
  const batch = args.slice(i, i + 3)
  out.push(...await parallel(batch.map(c => () => agent(prompt(c), { label: `check:${c.city_name}`, phase: 'Check', schema: SCHEMA, model: 'sonnet' }))))
}
return out
