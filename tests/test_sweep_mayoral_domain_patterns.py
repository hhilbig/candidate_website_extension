"""Pattern generation, WARC body extraction, and the name/mayor screen."""
import gzip
from datetime import date

from scripts.sweep_mayoral_domain_patterns import (
    city_word, crawl_dates, domain_patterns, name_parts, screen_text, warc_body, window,
)


def test_name_parts_handles_nickname_suffix_and_initials():
    assert name_parts('James W. (Jim) Merritt Jr.') == (['james', 'jim'], 'merritt')
    assert name_parts('George "Eddie" Lorton') == (['george', 'eddie'], 'lorton')
    assert name_parts('Edwin B. Peacock III') == (['edwin'], 'peacock')


def test_city_word_shortens_census_names():
    assert city_word('Indianapolis city (balance)') == 'indianapolis'
    assert city_word('Nashville-Davidson') == 'nashville'
    assert city_word('Louisville/Jefferson County metro government (balance)') == 'louisville'
    assert city_word('Urban Honolulu') == 'honolulu'


def test_patterns_include_known_missed_sites():
    assert 'johnwhitmire.com' in domain_patterns('John Whitmire', 'Houston')
    assert 'suthersformayor.com' in domain_patterns('John Suthers', 'Colorado Springs')
    assert 'electfloydbonner.com' in domain_patterns('Floyd Bonner', 'Memphis')
    assert 'joehogsett.com' in domain_patterns('Joseph H. "Joe" Hogsett', 'Indianapolis city (balance)')


def test_window_is_365_days_up_to_election():
    assert window('2020-11-03') == (date(2019, 11, 4), date(2020, 11, 3))


def test_crawl_dates_parse_week_ids_only():
    assert crawl_dates([{'id': 'CC-MAIN-2023-40'}, {'id': 'CC-MAIN-2012'}]) == [('CC-MAIN-2023-40', date(2023, 10, 2))]


def test_warc_body_and_screen():
    record = gzip.compress(b'WARC/1.0\r\nWARC-Type: response\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>x</html>')
    assert warc_body(record) == b'<html>x</html>'
    assert screen_text('Floyd Bonner', 'Elect Floyd Bonner Mayor of Memphis', 'bonner')[0] is True
    assert screen_text('Bonner Law', 'Attorney Floyd Bonner', 'bonner')[0] is False


def test_failed_queries_stay_retryable(tmp_path):
    import json
    from scripts.sweep_mayoral_domain_patterns import done_keys
    path = tmp_path / 'hits.jsonl'
    path.write_text('\n'.join(json.dumps(r) for r in [
        {'key': 'a', 'status': 'error'}, {'key': 'b', 'status': 'ok'},
        {'key': 'c', 'status': 'error'}, {'key': 'c', 'status': 'ok'}]) + '\n')
    assert done_keys(path) == {'b', 'c'}
