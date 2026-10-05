export const meta = {
  name: 'mayoral-legacy-site-check',
  description: 'Agent check of the 113 legacy-lead scrape targets (stage 3L) that the Hive model marked as campaign sites',
  phases: [{ title: 'Check', detail: 'one Sonnet agent per city; returns verdicts, edits no file', model: 'sonnet' }],
}
const ARGS = '/Users/hanno/Documents/GitHub/candidate_website_extension/data/municipal_discovery/top100_coverage/legacy_site_attribution/agent_check_args.json'
const SCHEMA = {
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
return await parallel(args.map(c => () => agent(`Check whether known URLs are the finalists' own mayoral campaign websites in ${c.city_name}, ${c.state_abb} (city_id ${c.city_id}).
An older pipeline linked each URL to the finalist, and a local model judged saved copies to be campaign sites. That model is weak on election timing (it accepts a real campaign site from a DIFFERENT election year). Decide strictly; do not trust either.
Read your input first: python3 -c 'import json; print(json.dumps([c for c in json.load(open("${ARGS}")) if c["city_id"]=="${c.city_id}"][0], indent=1))'
Each item has the finalist, the election_date, the URL, the archived copies that were saved, and the model's reason.

RULE. accepted only if (a) the URL's site has at least one HTTP 200 capture in the 365 days up to and including the election date, and (b) an archived page from that window names THIS candidate and shows THEIR mayoral campaign in THIS city for THIS election (for example "for Mayor", "elect", "vote", "re-elect", donate/volunteer calls, "Paid for by ... for Mayor"). Content left over from an earlier campaign counts only if the site is visibly being used for this election (a dated post, the right election date, a re-election call in that window). outside_window: it is the candidate's own mayoral campaign site but no qualifying page falls in that window (for example only after the election). not_found: a different person, city or office, an official government page, a news page, a parked/404/empty page, or content only from another election.

HOW. This machine cannot reach web.archive.org; run requests on mac2, one at a time:
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/cdx/search/cdx?url=<host>&matchType=domain&from=<YYYYMMDD window start>&to=<YYYYMMDD election>&fl=timestamp,original,statuscode&filter=statuscode:200&limit=50"'
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/web/<timestamp>id_/<original>"' | python3 -c 'import sys,re,html; t=sys.stdin.read(); t=re.sub(r"(?s)<(script|style).*?</\\1>"," ",t); print(re.sub(r"\\s+"," ",html.unescape(re.sub(r"<[^>]+>"," ",t)))[:4000])'
If a request fails or times out, wait 30 seconds and retry, up to 3 times. If the archive still cannot be read, return status incomplete and say what failed; never return not_found for a check that did not finish.
Prefer the saved captures listed in the input when they fall before the election; otherwise find an in-window root-page capture. Report the capture you read (https://web.archive.org/web/<timestamp>/<original>), a short quote, and a plain note on any doubt. Edit no file. Return one verdict per item.`,
  { label: `check:${c.city_name}`, phase: 'Check', schema: SCHEMA, model: 'sonnet' })))