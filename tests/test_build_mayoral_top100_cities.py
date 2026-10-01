import csv
import json
from pathlib import Path

import pytest

from scripts.build_mayoral_top100_cities import build

INPUTS = Path('data/municipal_discovery/top100_coverage/inputs')


def sources():
    return [
        [x['attributes'] for x in json.loads((INPUTS / filename).read_text())['features']]
        for filename in ['cities_census2020_incorporated_raw.json', 'cities_census2020_honolulu_raw.json']
    ]


def test_complete_fixed_city_population_list():
    rows = build(*sources(), [])
    assert len(rows) == len({r['city_id'] for r in rows}) == 100
    assert rows[0]['city_name'] == 'New York city'
    assert rows[-1]['city_name'] == 'Richmond city'
    assert rows[-1]['population_2020'] == 226610
    assert 'Santa Clarita city' in {r['city_name'] for r in rows}
    assert 'Hialeah city' not in {r['city_name'] for r in rows}


def test_consolidated_louisville_and_honolulu_aliases():
    rows = {r['city_id']: r for r in build(*sources(), [
        {'place_fips': '2148000'}, {'place_fips': '1517000'}, {'place_fips': '1836000'},
    ])}
    assert '2148000' not in rows
    assert rows['2148006']['population_2020'] == 633045
    assert rows['2148006']['roster_place_fips'] == '2148000'
    assert rows['1571550']['population_2020'] == 350964
    assert rows['1571550']['roster_place_fips'] == '1517000'
    assert rows['1836003']['roster_place_fips'] == '1836000'


def test_duplicate_geoid_rejected():
    incorporated, honolulu = sources()
    with pytest.raises(ValueError, match='Duplicate'):
        build(incorporated + [incorporated[0]], honolulu, [])


def test_frozen_output_100_unique_cities():
    with (INPUTS / 'cities_2020_top100.csv').open() as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 100
    assert len({r['city_id'] for r in rows}) == 100
    assert sum(bool(r['roster_place_fips']) for r in rows) == 99
    assert [r['city_name'] for r in rows if not r['roster_place_fips']] == ['Santa Clarita city']
