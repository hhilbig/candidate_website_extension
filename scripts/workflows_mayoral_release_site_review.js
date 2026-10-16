export const meta = {
  name: 'mayoral-release-site-review',
  description: 'Agent review of release sites the Hive site check doubted (no or unclear); a site is excluded only on the agent verdict',
  phases: [{ title: 'Review', detail: 'one Sonnet agent per city, two at a time; returns verdicts, edits no file', model: 'sonnet' }],
}
// Release stage 4 (docs/status.md). Input: release_site_check/review_args.json, the 54 sites (44 cities) where
// Hive job 24714340 answered "no" or "unclear". The Hive model is weak on election timing and reads incumbents'
// re-election sites as official pages, so its verdict alone excludes nothing.
const REPO = '/Users/hanno/Documents/GitHub/candidate_website_extension'
// args: either the city list (first round, review_args.json) or {file, cities} for a later round.
// args.dir overrides the input folder. The October 16 round used a folder outside ~/Documents,
// because macOS can refuse the agents access to ~/Documents.
const ARGS = (args.dir || REPO + '/data/municipal_discovery/top100_coverage/release_site_check') + '/' + (args.file || 'review_args.json')
const CITIES = args.cities || args
const SCHEMA = {
  type: 'object',
  properties: {
    city_id: { type: 'string' },
    verdicts: { type: 'array', items: { type: 'object', properties: {
      item_id: { type: 'string' },
      status: { type: 'string', enum: ['pass', 'exclude', 'incomplete'] },
      reason: { type: 'string', enum: ['campaign_site_this_election', 'different_person', 'other_office', 'not_campaign_site',
                                       'parked_or_spam', 'other_election_only', 'archive_unreadable'] },
      checked_capture_url: { type: 'string' }, evidence: { type: 'string' }, note: { type: 'string' } },
      required: ['item_id', 'status', 'reason', 'checked_capture_url', 'evidence', 'note'] } },
  },
  required: ['city_id', 'verdicts'],
}
const prompt = (c) => `Review campaign websites of mayoral finalists in ${c.city_name}, ${c.state_abb} (city_id ${c.city_id}) for a research dataset.
Read your input first: python3 -c 'import json; print(json.dumps([c for c in json.load(open("${ARGS}")) if c["city_id"]=="${c.city_id}"][0], indent=1))'
Each site has the finalist, the election date, up to three sample pages saved within 365 days of the election (archive URL and text), and a local model's doubt ("no" or "unclear") with its reason. The model is often wrong about incumbents: it calls a mayor's re-election site an "official page" because the site describes the mayor's record. It is also weak on election timing. Decide yourself.

RULE. pass (reason campaign_site_this_election) if the site is THIS candidate's own campaign site and was used for a mayoral campaign in THIS city for THIS election: for example "for Mayor", "elect", "vote", "re-elect", donate or volunteer calls, a campaign committee footer, or an incumbent's re-election site in that period. Content carried over from an earlier campaign counts if the site is visibly in use for this election in that period.
exclude with one reason: different_person (same surname, other city or state), other_office (council, county, state legislature, Congress, with no mayoral campaign), not_campaign_site (official government page, business or personal site, news or endorsement reprint site with no campaign of the candidate's own), parked_or_spam (parked, expired, for-sale or spam content across the sample), other_election_only (campaign content only for an earlier or later election, nothing for this one).
incomplete (reason archive_unreadable) only if you cannot decide from the sample and the archive cannot be read after retries.
If the sample is thin, read more pages: this machine cannot reach web.archive.org; run requests on mac2, one at a time, for example:
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/cdx/search/cdx?url=<domain>&matchType=domain&from=<YYYYMMDD>&to=<YYYYMMDD>&fl=timestamp,original,statuscode&filter=statuscode:200&filter=mimetype:text/html&collapse=urlkey&limit=60"'
  ssh mac2 'curl -s --max-time 60 "https://web.archive.org/web/<timestamp>id_/<original>"' | python3 -c 'import sys,re,html; t=sys.stdin.read(); t=re.sub(r"(?s)<(script|style).*?</\\1>"," ",t); print(re.sub(r"\\s+"," ",html.unescape(re.sub(r"<[^>]+>"," ",t)))[:4000])'
If a request fails, wait 30 seconds and retry, up to 3 times. Report the capture that decided your verdict, a short quote in evidence, and a plain note on any doubt. Edit no file. Return one verdict per site.`
const out = []
for (let i = 0; i < CITIES.length; i += 2) {
  out.push(...await parallel(CITIES.slice(i, i + 2).map(c => () => agent(prompt(c), { label: `review:${c.city_name}`, phase: 'Review', schema: SCHEMA, model: 'sonnet' }))))
}
return out
