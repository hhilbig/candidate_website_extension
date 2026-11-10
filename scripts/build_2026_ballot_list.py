"""Build a provisional list of 2026 general-election candidates and match it to the roster.

Source: the result tables of the English Wikipedia state pages for the 2026
House and Senate elections (MediaWiki API; the revision ID of each page is
saved). The published dataset uses the MIT Election Lab returns for this flag.
Those do not have 2026 yet, so this list is provisional.

Steps: `fetch` saves the page text. `build` reads the saved text, writes one row
for each candidate in a general-election table, and matches the Democratic and
Republican rows to the 2026 roster on state, office, district, and name.

Usage:
  python scripts/build_2026_ballot_list.py fetch --dir data/collection_2026/ballot_2026
  python scripts/build_2026_ballot_list.py build --dir data/collection_2026/ballot_2026 \
      --input data/collection_2026/final_20261109 --out build/release_v5_0_2026
"""
import argparse
import json
import re
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests
from rapidfuzz import fuzz

STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado',
    'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
    'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
    'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina',
    'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
    'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas',
    'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'the District of Columbia', 'GU': 'Guam',
    'AS': 'American Samoa', 'MP': 'the Northern Mariana Islands', 'VI': 'the United States Virgin Islands',
}
AT_LARGE = {'AK', 'DE', 'ND', 'SD', 'VT', 'WY', 'DC', 'GU', 'AS', 'MP', 'VI'}
API = 'https://en.wikipedia.org/w/api.php'
SUFFIXES = {'jr', 'sr', 'ii', 'iii', 'iv'}
# Wikipedia name -> FEC name, where the two share no family name.
ALIASES = {('AS', 'Amata Coleman Radewagen'): 'Aumua Amata'}


def titles(senate_states, special_states):
    out = []
    for abbr, name in STATES.items():
        plural = 'election' if abbr in AT_LARGE else 'elections'
        out.append((abbr, 'house', 'general', f'2026 United States House of Representatives {plural} in {name}'))
    for part in ('districts 1–26', 'districts 27–52'):  # the California page has two sub-pages
        out.append(('CA', 'house', f'general_{part[10:12].strip("–")}',
                    f'2026 United States House of Representatives elections in California ({part})'))
    for abbr in senate_states:
        out.append((abbr, 'senate', 'general', f'2026 United States Senate election in {STATES[abbr]}'))
    for abbr in special_states:
        out.append((abbr, 'senate', 'special_general', f'2026 United States Senate special election in {STATES[abbr]}'))
    return out


def fetch(args):
    folder = Path(args.dir)
    (folder / 'wikitext').mkdir(parents=True, exist_ok=True)
    calendar = pd.read_parquet(Path(args.input) / 'exports/election_calendar.parquet')
    day = calendar[(calendar.date.astype(str).str[:10] == '2026-11-03') & (calendar.office == 'senate')]
    regular = sorted(set(day[day.event_type == 'general'].state) & set(STATES))
    special = sorted(set(day[day.event_type == 'special_general'].state) & set(STATES))
    session = requests.Session()
    session.headers['User-Agent'] = 'research-script/1.0 (academic data collection)'
    index = []
    for abbr, office, kind, title in titles(regular, special):
        for attempt in range(5):
            response = session.get(API, params=dict(action='query', prop='revisions', rvprop='content|ids|timestamp',
                                                    rvslots='main', titles=title, redirects=1, format='json',
                                                    formatversion=2, maxlag=5), timeout=60)
            if response.status_code == 200 and response.text.startswith('{') and 'query' in response.json():
                break
            print('retry', title, response.status_code, response.text[:120].replace('\n', ' '))
            time.sleep(15 * (attempt + 1))
        else:
            raise SystemExit(f'no reply for {title}')
        reply = response.json()
        page = reply['query']['pages'][0]
        entry = dict(state=abbr, office=office, kind=kind, requested_title=title, found=not page.get('missing', False))
        if entry['found']:
            revision = page['revisions'][0]
            name = f'{office}_{kind}_{abbr}.txt'.replace(' ', '_')
            (folder / 'wikitext' / name).write_text(revision['slots']['main']['content'])
            entry.update(title=page['title'], revision_id=revision['revid'], revision_time=revision['timestamp'], file=name)
        index.append(entry)
        time.sleep(2)
    (folder / 'pages.json').write_text(json.dumps(index, indent=2))
    print(len(index), 'pages requested;', sum(not e['found'] for e in index), 'missing:',
          [e['requested_title'] for e in index if not e['found']])


def plain(value):
    value = re.sub(r'<ref[^>]*?/>|<ref.*?</ref>', '', value, flags=re.S)
    value = re.sub(r'\{\{(?:nowrap|nobr)\|(.*?)\}\}', r'\1', value)
    value = re.sub(r'\[\[(?:[^\]|]*\|)?([^\]]*)\]\]', r'\1', value)
    value = re.sub(r"'{2,}", '', value)
    value = re.sub(r'\{\{[^{}]*\}\}', '', value)
    return re.sub(r'\s+', ' ', value).strip()


def template_fields(body):
    fields, depth, part, parts = {}, 0, '', []
    for i, ch in enumerate(body):
        pair = body[i:i + 2]
        if pair in ('{{', '[['):
            depth += 1
        elif pair in ('}}', ']]'):
            depth -= 1
        if ch == '|' and depth == 0:
            parts.append(part)
            part = ''
        else:
            part += ch
    parts.append(part)
    for item in parts[1:]:
        if '=' in item:
            key, value = item.split('=', 1)
            fields[key.strip()] = value.strip()
    return fields


def templates(text, start):
    """Yield (position, body) of each {{Election box ...}} template after start."""
    for match in re.finditer(r'\{\{\s*Election box', text[start:], flags=re.I):
        begin = start + match.start()
        depth, i = 0, begin
        while i < len(text):
            if text.startswith('{{', i):
                depth += 1
                i += 2
            elif text.startswith('}}', i):
                depth -= 1
                i += 2
                if depth == 0:
                    break
            else:
                i += 1
        yield begin, text[begin + 2:i - 2]


def party_code(value):
    value = plain(value).lower()
    if 'democrat' in value:
        return 'D'
    if 'republican' in value:
        return 'R'
    return 'other'


def parse_page(text, state, office, kind):
    rows = []
    headings = [(m.start(), len(m.group(1)), plain(m.group(2))) for m in
                re.finditer(r'^(={2,6})\s*(.*?)\s*\1\s*$', text, flags=re.M)]

    def context(position):
        district, section = None, ''
        stack = {}
        for start, level, title in headings:
            if start > position:
                break
            stack = {k: v for k, v in stack.items() if k < level}
            stack[level] = title
        for title in stack.values():
            m = re.match(r'District (\d+)', title)
            if m:
                district = int(m.group(1))
            section += ' / ' + title
        return district, section

    box_title, box_general, box_district = '', False, None
    for position, body in templates(text, 0):
        name = re.split(r'\|', body, maxsplit=1)[0].strip().lower()
        fields = template_fields(body)
        if name.startswith('election box begin'):
            box_title = plain(fields.get('title', ''))
            district, section = context(position)
            lowered = (box_title + ' ' + section).lower()
            excluded = re.search(r'primary|runoff|run-off|convention|caucus|poll|straw', box_title.lower())
            in_general = 'general election' in section.lower()
            # Louisiana and top-two states: the November election is in the General election section.
            box_general = bool(in_general and not excluded)
            if not box_general and not excluded and re.search(r'results', section.lower()) and \
                    not re.search(r'primary|runoff|convention', section.lower()) and \
                    re.search(r'\b(district|senate|at-large|delegate)\b.*\belection\b', box_title.lower()):
                box_general = True
            m = re.search(r'(\d+)(?:st|nd|rd|th) congressional district', lowered)
            # The section heading controls. Some table titles carry the number of another district.
            box_district = district if district is not None else (int(m.group(1)) if m else None)
            continue
        if not box_general or 'candidate' not in name or 'candidate' not in fields:
            continue
        candidate = plain(fields['candidate'])
        incumbent = bool(re.search(r'\(incumbent\)', candidate, flags=re.I))
        candidate = re.sub(r'\((?:incumbent|write-in)\)', '', candidate, flags=re.I).strip()
        if not candidate or candidate.lower() in {'write-in', 'write-ins', 'scattering', 'other', 'others', 'tbd', 'tba'}:
            continue
        votes = re.sub(r'[^\d]', '', plain(fields.get('votes', '')))
        rows.append(dict(state=state, office=office, kind=kind,
                         district=(0 if state in AT_LARGE else box_district) if office == 'house' else None,
                         party=party_code(fields.get('party', '')), party_text=plain(fields.get('party', '')),
                         candidate=candidate, incumbent=incumbent, votes=int(votes) if votes else None,
                         table_title=box_title))
    return rows


def parse_nominees(text, state, office, kind, skip):
    """Fallback for a contest with no general-election table: the Nominee and
    Advanced to general lists of the party sections. `skip` holds the districts
    that already have a table."""
    rows, stack = [], {}
    lines = text.split('\n')
    for line in lines:
        m = re.match(r'^(={2,6})\s*(.*?)\s*\1\s*$', line)
        if m:
            level = len(m.group(1))
            stack = {k: v for k, v in stack.items() if k < level}
            stack[level] = plain(m.group(2))
            continue
        if not line.startswith('*') or line.startswith('**') or not stack:
            continue
        titles_now = list(stack.values())
        last = titles_now[-1].lower()
        if not re.fullmatch(r'nominee|nominees|advanced to general|advanced to the general election', last):
            continue
        district = None
        for title in titles_now:
            d = re.match(r'District (\d+)', title)
            if d:
                district = int(d.group(1))
        if office == 'house':
            district = 0 if state in AT_LARGE else district
            if district is None or district in skip:
                continue
        elif skip:
            continue
        section = ' / '.join(titles_now).lower()
        entry = plain(re.split(r',|<ref|\{\{efn', line.lstrip('* '), maxsplit=1)[0])
        entry = re.sub(r'\((?:incumbent|write-in)\)', '', entry, flags=re.I).strip()
        party = re.search(r'\((D|R|DFL|Democratic|Republican)\)\s*$', entry)
        if party:
            code = 'R' if party.group(1).startswith('R') else 'D'
            entry = entry[:party.start()].strip()
        elif 'democrat' in section:
            code = 'D'
        elif 'republican' in section:
            code = 'R'
        else:
            code = 'other'
        if entry and len(entry) < 60:
            rows.append(dict(state=state, office=office, kind=kind, district=district if office == 'house' else None,
                             party=code, party_text='', candidate=entry, incumbent='(incumbent)' in line.lower(),
                             votes=None, table_title='', list_source='nominee_list'))
    return rows


def tokens(value):
    value = unicodedata.normalize('NFKD', str(value)).encode('ascii', 'ignore').decode().lower()
    return [t for t in re.findall(r'[a-z]+', value) if t not in SUFFIXES]


def surname_of_roster(raw, display):
    if isinstance(raw, str) and ',' in raw:
        return ''.join(tokens(raw.split(',', 1)[0]))
    t = tokens(display)
    return t[-1] if t else ''


def build(args):
    folder, out = Path(args.dir), Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    index = json.loads((folder / 'pages.json').read_text())
    rows = []
    for entry in index:
        if entry['found']:
            text = (folder / 'wikitext' / entry['file']).read_text()
            kind = 'general' if entry['kind'].startswith('general') else entry['kind']
            found = parse_page(text, entry['state'], entry['office'], kind)
            for row in found:
                row['list_source'] = 'general_table'
            have = {row['district'] for row in found} if entry['office'] == 'house' else set(found and [1])
            extra = parse_nominees(text, entry['state'], entry['office'], kind, have)
            entry['n_rows_nominee_list'] = len(extra)
            found += extra
            for row in found:
                row.update(source_title=entry['title'], source_revision_id=entry['revision_id'])
            rows += found
            entry['n_rows'] = len(found)
    ballot = pd.DataFrame(rows).drop_duplicates(['state', 'office', 'kind', 'district', 'party', 'candidate'])
    roster = pd.read_parquet(Path(args.input) / 'exports/candidate_roster.parquet')
    roster['district_number'] = pd.to_numeric(roster.district, errors='coerce')
    roster['surname'] = [surname_of_roster(r, c) for r, c in zip(roster.fec_raw_name, roster.candidate)]

    matched = []
    for i, row in ballot[ballot.party.isin(['D', 'R'])].iterrows():
        pool = roster[(roster.state == row.state) & (roster.office == row.office)]
        if row.office == 'house' and row.state not in AT_LARGE:  # FEC files some at-large candidates in district 01
            pool = pool[pool.district_number == row.district]
        name_tokens = tokens(ALIASES.get((row.state, row.candidate), row.candidate))
        best, best_score, best_kind = None, 0, ''
        for r in pool.itertuples():
            surname_hit = r.surname and r.surname in ''.join(name_tokens) and any(
                r.surname.endswith(t) or r.surname.startswith(t) for t in name_tokens[-2:])
            score = fuzz.token_set_ratio(' '.join(name_tokens), ' '.join(tokens(r.fec_raw_name or r.candidate)))
            score += 100 if surname_hit else 0
            score += 5 if r.party == row.party else 0
            if score > best_score:
                best, best_score, best_kind = r, score, 'surname' if surname_hit else 'fuzzy'
        accept = best is not None and (best_score >= 150 or (best_kind == 'fuzzy' and best_score >= 80 and best.party == row.party))
        district_differs = False
        if not accept and row.office == 'house':
            # Redistricting: the FEC record can have another district. Accept only a near-exact name in the state.
            for r in roster[(roster.state == row.state) & (roster.office == 'house')].itertuples():
                roster_tokens = tokens(r.fec_raw_name or r.candidate)
                if r.surname and name_tokens and r.surname == name_tokens[-1] and name_tokens[0] in roster_tokens \
                        and r.party == row.party:
                    best, best_score, accept, district_differs = r, 200, True, True
                    break
        matched.append(dict(row.to_dict(), candidate_id=best.candidate_id if accept else None,
                            roster_name=best.fec_raw_name if best is not None else None,
                            roster_party=best.party if best is not None else None,
                            match_score=best_score, matched=bool(accept),
                            party_differs=bool(accept and best.party != row.party),
                            district_differs=district_differs))
    result = pd.DataFrame(matched)
    ballot.to_csv(out / 'ballot_2026_all_candidates.csv', index=False)
    result.to_csv(out / 'ballot_2026_major_party_match.csv', index=False)

    contests = result.groupby(['state', 'office', 'kind', 'district'], dropna=False).size()
    expected_house = roster[roster.office == 'house'].groupby('state').district_number.nunique()
    summary = dict(pages=len(index), pages_missing=[e['requested_title'] for e in index if not e['found']],
                   pages_without_rows=[e['title'] for e in index if e['found'] and not e.get('n_rows')],
                   candidates_all_parties=int(len(ballot)), major_party_candidates=int(len(result)),
                   matched=int(result.matched.sum()), unmatched=int((~result.matched).sum()),
                   duplicate_candidate_ids=int(result.candidate_id.dropna().duplicated().sum()),
                   party_differs=int(result.party_differs.sum()),
                   district_differs=int(result.district_differs.sum()),
                   rows_by_list_source=result.list_source.value_counts().to_dict(),
                   house_contests=int(sum(k[1] == 'house' for k in contests.index)),
                   senate_contests=int(sum(k[1] == 'senate' for k in contests.index)),
                   house_districts_in_roster=int(expected_house.sum()),
                   contests_with_more_than_two_major=int((contests > 2).sum()),
                   contests_with_one_major=int((contests == 1).sum()))
    (out / 'ballot_2026_summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('command', choices=['fetch', 'build'])
    ap.add_argument('--dir', required=True)
    ap.add_argument('--input', default='data/collection_2026/final_20261109')
    ap.add_argument('--out', default='build/release_v5_0_2026')
    args = ap.parse_args()
    fetch(args) if args.command == 'fetch' else build(args)


if __name__ == '__main__':
    main()
