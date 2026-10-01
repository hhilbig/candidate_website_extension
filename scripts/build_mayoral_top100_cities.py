#!/usr/bin/env python3
"""Freeze Census 2020 city populations and crosswalk the largest 100 places.

The city list includes Urban Honolulu, as Census national city tables do.
Louisville combines the two non-overlapping 2020 TIGER place components.
This script does not select or evaluate candidates.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROSTER = ROOT / 'data/municipal_discovery/exhaustive_source_expansion_20260908/ownership_repair_20260910/combined/candidate_roster.csv'
DEFAULT_OUT = ROOT / 'data/municipal_discovery/top100_coverage/inputs'
BASE = 'https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/tigerWMS_Census2020/MapServer'
STATE_ABB = dict(x.split(':') for x in '01:AL 02:AK 04:AZ 05:AR 06:CA 08:CO 09:CT 10:DE 11:DC 12:FL 13:GA 15:HI 16:ID 17:IL 18:IN 19:IA 20:KS 21:KY 22:LA 23:ME 24:MD 25:MA 26:MI 27:MN 28:MS 29:MO 30:MT 31:NE 32:NV 33:NH 34:NJ 35:NM 36:NY 37:NC 38:ND 39:OH 40:OK 41:OR 42:PA 44:RI 45:SC 46:SD 47:TN 48:TX 49:UT 50:VT 51:VA 53:WA 54:WV 55:WI 56:WY'.split())
ALIASES = {'1836003': '1836000', '2148006': '2148000', '1571550': '1517000'}
NOTES = {
    '2148006': 'Census city population combines old Louisville city (246161) and balance component (386884), totaling 633045. Metro mayor serves all Jefferson County, including separately incorporated cities. Roster uses old city code.',
    '1836003': 'City rank uses Indianapolis balance population. Mayor jurisdiction and excluded municipalities require election-source verification. Roster uses Indianapolis consolidated-city code.',
    '4752006': 'City rank uses Nashville-Davidson balance population, excluding separately incorporated municipalities. Metro mayor serves the consolidated jurisdiction.',
    '1571550': 'Census national city tables include Urban Honolulu CDP. Rank uses this CDP, not Honolulu County population. Mayor serves City and County of Honolulu. Roster uses historical Honolulu place code.',
    '1235000': 'Jacksonville city population excludes separately incorporated beach cities and Baldwin. Mayor serves the consolidated city-county jurisdiction.',
    '2205000': 'City rank uses Baton Rouge city population. Mayor-president serves the consolidated city-parish jurisdiction, not only the city.',
    '2146027': 'Lexington-Fayette urban county is a consolidated city-county government.',
    '0203000': 'Anchorage municipality is a consolidated municipal jurisdiction.',
    '0669088': 'Santa Clarita city is absent from the existing roster. Council selects the mayor; retain the city in the population universe.',
}
NOTE_SOURCES = {
    '2148006': 'https://louisvilleky.gov/sites/default/files/2022-08/2022-2023%20LOUISVILLE%20METRO%20APPROVED%20BUDGET.pdf',
    '1571550': 'https://www.honolulu.gov/cor/wp-content/uploads/sites/17/2025/01/Charter-FINAL-1-16-2025.pdf',
    '4752006': 'https://www.nashville.gov/departments/mayor/what-can-your-mayor-do',
    '2205000': 'https://www.brla.gov/1062/Our-Government',
    '0669088': 'https://santaclarita.gov/city-council/',
}


def query_url(layer, where):
    return f'{BASE}/{layer}/query?' + urllib.parse.urlencode({
        'f': 'json', 'where': where, 'outFields': 'GEOID,NAME,STATE,PLACE,POP100',
        'returnGeometry': 'false', 'orderByFields': 'POP100 DESC',
    })


def cached_query(path, url):
    if not path.exists():
        with urllib.request.urlopen(url, timeout=60) as response:
            raw = response.read()
        data = json.loads(raw)
        if data.get('error') or data.get('exceededTransferLimit'):
            raise ValueError(data)
        path.write_bytes(raw)
    data = json.loads(path.read_bytes())
    if data.get('error') or data.get('exceededTransferLimit'):
        raise ValueError('Invalid or incomplete frozen Census response')
    return [x['attributes'] for x in data['features']]


def build(incorporated, honolulu, roster):
    places = {r['GEOID']: dict(r) for r in incorporated if r['STATE'] in STATE_ABB}
    if len(places) != len([r for r in incorporated if r['STATE'] in STATE_ABB]):
        raise ValueError('Duplicate Census place identifiers')
    old = places.pop('2148000')
    places['2148006']['POP100'] += old['POP100']
    if places['2148006']['POP100'] != 633045:
        raise ValueError('Louisville components changed; inspect before rebuilding')
    assert len(honolulu) == 1 and honolulu[0]['GEOID'] == '1571550'
    places['1571550'] = dict(honolulu[0])
    selected = sorted(places.values(), key=lambda r: (-r['POP100'], r['GEOID']))[:100]
    assert len(selected) == 100
    assert selected[-1]['POP100'] > 200000, 'Query threshold is too high'
    roster_ids = {r['place_fips'].zfill(7) for r in roster}
    output = []
    for rank, row in enumerate(selected, 1):
        geoid = row['GEOID']
        roster_id = ALIASES.get(geoid, geoid)
        output.append(dict(
            city_id=geoid, city_name=row['NAME'], state_abb=STATE_ABB[row['STATE']],
            population_2020=int(row['POP100']), census_geoid=geoid,
            roster_place_fips=roster_id if roster_id in roster_ids else '',
            source_url=f'{BASE}/{28 if geoid == "1571550" else 26}',
            jurisdiction_note=NOTES.get(geoid, 'Census incorporated-place population. Verify election jurisdiction in official results.'),
            population_rank_2020=rank, jurisdiction_source_url=NOTE_SOURCES.get(geoid, ''),
            crosswalk_status='explicit_historical_alias' if geoid in ALIASES else ('exact_geoid' if roster_id in roster_ids else 'missing_roster_city'),
        ))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--roster', type=Path, default=DEFAULT_ROSTER)
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    sources = [
        ('cities_census2020_incorporated_raw.json', query_url(26, 'POP100 > 200000')),
        ('cities_census2020_honolulu_raw.json', query_url(28, "STATE='15' AND POP100>200000")),
    ]
    data = [cached_query(args.output_dir / name, url) for name, url in sources]
    with args.roster.open(newline='') as handle:
        roster = list(csv.DictReader(handle))
    rows = build(*data, roster)
    output = args.output_dir / 'cities_2020_top100.csv'
    with output.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest = {'population_reference_date': '2020-04-01', 'geography_vintage': '2020-01-01',
        'selection': 'Largest 100 Census incorporated places in 50 states and DC, plus Urban Honolulu; one consolidated Louisville record.',
        'source_population_threshold': 200000, 'city_count': len(rows),
        'roster_matched_cities': sum(bool(x['roster_place_fips']) for x in rows),
        'inputs': [{'file': name, 'url': url, 'sha256': hashlib.sha256((args.output_dir/name).read_bytes()).hexdigest()} for name, url in sources],
        'roster_sha256': hashlib.sha256(args.roster.read_bytes()).hexdigest(),
        'output_sha256': hashlib.sha256(output.read_bytes()).hexdigest()}
    (args.output_dir / 'cities_manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
