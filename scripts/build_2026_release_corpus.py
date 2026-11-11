"""Build the 2026 rows of raw_corpus.parquet from the final 2026 collection exports.

One output row is one usable page of one candidate at one copy time. The
columns are the 28 columns of the published raw_corpus.parquet, plus three
columns that the 2026 data needs (and two page type columns, see page_types):

  capture_source  'live' (copied from the campaign server) or 'wayback'
  snapshot_id     one crawl of one site; the pair table refers to it
  page_id         one page observation; the pair table refers to it

The fields on_ballot, stage, data_source and candidate_year_stage stay missing
here. A later step fills them from the general-election candidate lists.

Usage:
  python scripts/build_2026_release_corpus.py \
      --input data/collection_2026/final_20261109 \
      --crosswalk build/release_v4_0_candidate/candidate_crosswalk.csv \
      --out build/release_v5_0_2026
"""
import argparse
import gzip
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.harmonize_deliverable import district_id, icpsr_name, name_key, norm_district  # noqa: E402
from scripts.icpsr_replicate_coding import icpsr_clean_text  # noqa: E402
from src.extract_text import classify_page_type  # noqa: E402

PARTY = {'D': 'democrat', 'R': 'republican'}
SCHEMA = pa.schema([
    ('candidate_icpsr', pa.string()), ('name_key', pa.string()), ('cand_id', pa.string()),
    ('bonica_rid', pa.string()), ('candidate_year_stage', pa.string()), ('state', pa.string()),
    ('district', pa.float64()), ('district_id', pa.string()), ('office', pa.string()), ('year', pa.int64()),
    ('party', pa.string()), ('stage', pa.int64()), ('data_source', pa.string()), ('date', pa.string()),
    ('urlkey', pa.string()), ('snap_url', pa.string()), ('page_type', pa.string()), ('n_char', pa.int64()),
    ('n_words', pa.int64()), ('n_tags', pa.int64()), ('n_clean_tags', pa.int64()), ('n_snap', pa.int64()),
    ('text_snap_content', pa.string()), ('candidate_cycle_id', pa.string()), ('cand_election_yr', pa.string()),
    ('cand_status', pa.string()), ('universe_source', pa.string()), ('on_ballot', pa.bool_()),
    ('capture_source', pa.string()), ('snapshot_id', pa.string()), ('page_id', pa.string()),
    ('page_type_context', pa.string()), ('extraction_route', pa.string()),
])


NON_CONTENT = re.compile(
    r'/(privacy|terms|cart|shop|store|checkout|donate|contribute|volunteer|contact|login|sign-?up|subscribe|'
    r'sitemap|cdn-cgi|events?|join[^/]*|search|tag|category|author|feed|wp-[a-z]+)(/|$|[-_.?])', re.I)


# An archived body that the collector did not decompress (zstd) is binary data, not page
# text. In such a row more than half of the characters are control or replacement
# characters; in each other row of the November 9 corpus the share is below 2 percent.
NOT_TEXT = re.compile('[\ufffd\x00-\x08\x0b\x0c\x0e-\x1f]')


def undecoded(text):
    return len(NOT_TEXT.findall(text)) > 0.25 * max(len(text), 1)


# Hosts that are not a campaign website (decision of November 11, 2026, Hanno). The FEC
# record of a few candidates names a news article, a social profile, or an office page.
NOT_CAMPAIGN_HOSTS = {'linktr.ee', 'foxnews.com', 'linkedin.com', 'fedscoop.com', 'chicagotribune.com'}
# No campaign page has more than 140,000 characters. Two pages of one hacked site hold
# 1.9 million characters of injected link text each.
MAX_PAGE_CHARS = 500_000


def removal_reason(page):
    """Return why the release omits a usable page, or None."""
    host = urlsplit(page.original_url).netloc.lower().removeprefix('www.')
    if host in NOT_CAMPAIGN_HOSTS or host.endswith('.gov'):
        return 'not_campaign_host'
    # The archive pass stored each archived address below the site root as a homepage.
    # An address with a query string (`/?p=12`) is a post or a tracking link, not the homepage.
    if page.page_type == 'homepage' and '?' in page.original_url:
        return 'query_string_homepage'
    if int(page.n_char) > MAX_PAGE_CHARS:
        return 'injected_text'
    return None


SIDE_FIELDS = ('snapshot_id', 'homepage_page_id', 'captured_at', 'source', 'distance_days', 'local_date_min',
               'local_date_max', 'page_ids', 'n_pages', 'page_types', 'page_sources', 'page_time_min',
               'page_time_max', 'page_span_hours', 'snapshot_complete')


def release_pairs(pairs, pages, removed):
    """Select each pair side again without the removed pages.

    The selection is the collector's own (`_snapshot_choice`). With no removed page it
    returns the collector's table; the function checks this for each row it does not change.
    """
    from src.collection_2026_outputs import _datetime, _missing_reason, _snapshot_choice
    by_candidate = {}
    for record in pages.to_dict('records'):
        if record['page_id'] in removed:
            record['status'] = 'removed_from_release'
        by_candidate.setdefault(record['candidate_id'], []).append(record)
    pairs = pairs.fillna('').copy()
    changed = 0
    for i, row in pairs.iterrows():
        event = dict(event_id=row.event_id, date=row.event_date, timezones=json.loads(row.timezones))
        candidate_pages = by_candidate.get(row.candidate_id, [])
        choices = {}
        for side in ('pre', 'post'):
            choice = _snapshot_choice(candidate_pages, event, side)
            choices[side] = choice
            same = (str(bool(choice)) == row[f'{side}_available']
                    and (not choice or (choice['homepage_page_id'] == row[f'{side}_homepage_page_id']
                                        and choice['page_ids'] == json.loads(row[f'{side}_page_ids']))))
            if same:
                continue
            changed += 1
            pairs.at[i, f'{side}_available'] = str(bool(choice))
            pairs.at[i, f'{side}_missing_reason'] = '' if choice else _missing_reason(
                dict(candidate_id=row.candidate_id, website_url=row.website_url), candidate_pages, event, side)
            for field in SIDE_FIELDS:
                value = choice.get(field, '')
                pairs.at[i, f'{side}_{field}'] = json.dumps(value) if isinstance(value, list) else str(value)
        pre, post = choices['pre'], choices['post']
        status = 'paired' if pre and post else 'pre_only' if pre else 'post_only' if post else 'uncovered'
        if status != row.coverage_status or (pre and post and (
                pre['homepage_page_id'] != row.pre_homepage_page_id or post['homepage_page_id'] != row.post_homepage_page_id)):
            pairs.at[i, 'coverage_status'] = status
            pairs.at[i, 'mixed_source_pair'] = str(bool(pre and post and len(set(pre['page_sources'] + post['page_sources'])) > 1))
            pairs.at[i, 'pair_homepage_span_days'] = str(
                (_datetime(post['captured_at']) - _datetime(pre['captured_at'])).total_seconds() / 86400) if pre and post else ''
    return pairs, changed


def page_types(collector_type, url):
    """Return (page_type, extraction_route) for one 2026 page.

    Decision of November 9, 2026 (Hanno). `page_type` follows the URL rule of the
    published files; the collector's homepage stays `homepage`. The collector's own
    type uses the link context (a page reached from the issues page is `issues`) and
    stays in `page_type_context`. The extraction reads issue and biography pages by
    context when the URL rule says `other` and the URL is not a service page.
    Endorsements use the URL rule only, because the endorsement contract requires it.
    """
    by_url = 'homepage' if collector_type == 'homepage' else classify_page_type(url)
    route = None
    if by_url in ('issues', 'biography', 'endorsements'):
        route = by_url
    elif by_url == 'other' and collector_type in ('issues', 'biography') and not NON_CONTENT.search(urlsplit(url).path):
        route = collector_type
    return by_url, route


def stamp(captured_at):
    return datetime.fromisoformat(captured_at).astimezone(timezone.utc).strftime('%Y%m%d%H%M%S')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True)
    ap.add_argument('--crosswalk', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    src, out = Path(args.input), Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    roster = pd.read_parquet(src / 'exports/candidate_roster.parquet')
    assert roster.candidate_id.is_unique
    roster = roster.set_index('candidate_id')
    pages = pd.read_parquet(src / 'exports/page_corpus.parquet')
    usable = pages[pages.status == 'usable'].copy()
    assert usable.page_id.is_unique and usable.text_path.ne('').all()
    assert (usable.identity_status == 'confirmed').all()
    usable['date'] = usable.captured_at.map(stamp)
    n_usable = len(usable)

    # The collector made a new crawl for each retry. For Wayback, two rows with the
    # same candidate, capture time, URL and text are one archived capture. Keep the first.
    # A row that the pair table uses is the one that stays.
    reasons = {page.page_id: reason for page in usable.itertuples() if (reason := removal_reason(page))}
    removed_pages = usable[usable.page_id.isin(reasons)][['candidate_id', 'page_id', 'date', 'original_url', 'source', 'n_char']].copy()
    removed_pages['reason'] = removed_pages.page_id.map(reasons)
    removed_pages.rename(columns={'candidate_id': 'cand_id', 'original_url': 'url'}).to_csv(
        out / 'pages_removed_2026.csv', index=False)
    pairs, pair_sides_changed = release_pairs(
        pd.read_parquet(src / 'exports/candidate_event_pairs.parquet'), pages, set(reasons))
    pairs.to_parquet(out / 'candidate_event_pairs_release.parquet', index=False)
    candidates_before = usable.candidate_id.nunique()
    usable = usable[~usable.page_id.isin(reasons)]
    in_pairs = set()
    for column in ('pre_page_ids', 'post_page_ids'):
        for value in pairs[column]:
            if isinstance(value, str) and value.startswith('['):
                in_pairs.update(json.loads(value))
    in_pairs.update(pairs.pre_homepage_page_id.dropna())
    in_pairs.update(pairs.post_homepage_page_id.dropna())
    usable['in_pairs'] = usable.page_id.isin(in_pairs)
    usable = usable.sort_values('in_pairs', ascending=False, kind='stable')
    wayback = usable.source == 'wayback'
    # Two pairs can use two copies of one capture (after one event, before the next). Both stay.
    duplicate = (wayback & ~usable.in_pairs
                 & usable.duplicated(['candidate_id', 'date', 'original_url', 'text_path'], keep='first'))
    usable = usable[~duplicate]

    # DIME identifiers end in 2022. Carry the identifier of an earlier year for the same FEC ID.
    xwalk = pd.read_csv(args.crosswalk, dtype=str)
    rid = (xwalk.dropna(subset=['bonica_rid']).sort_values('year').drop_duplicates('cand_id', keep='last')
           .set_index('cand_id').bonica_rid.to_dict())

    writer = pq.ParquetWriter(out / 'raw_corpus_2026.parquet', SCHEMA, compression='zstd')
    n_rows, text_cache_hits = 0, 0
    removed_undecoded = []
    type_counts = Counter()
    for cid, group in usable.groupby('candidate_id', sort=True):
        r = roster.loc[cid]
        district = norm_district(r.district) if r.office == 'house' else float('nan')
        fixed = dict(
            candidate_icpsr=icpsr_name(r.fec_raw_name, r.candidate), name_key=name_key(r.fec_raw_name, r.candidate),
            cand_id=cid, bonica_rid=rid.get(cid), candidate_year_stage=None, state=r.state,
            district=None if pd.isna(district) else float(district),
            district_id=None if r.office != 'house' or pd.isna(district) else district_id(r.state, district, r.office),
            office=r.office, year=2026, party=PARTY[r.party], stage=None, data_source=None,
            candidate_cycle_id=f'{cid}-2026', cand_election_yr='2026', cand_status=r.cand_status or None,
            universe_source='same_year_fec', on_ballot=None, n_snap=int(group.snapshot_id.nunique()))
        rows, cache = [], {}
        for page in group.sort_values(['date', 'snapshot_id', 'depth', 'original_url']).itertuples():
            if page.text_path not in cache:
                text = gzip.open(src / page.text_path, 'rt', encoding='utf-8').read()
                assert hashlib.sha256(text.encode()).hexdigest() == Path(page.text_path).name.split('.')[0]
                cleaned = icpsr_clean_text(text)
                cache[page.text_path] = (text, len(text), len(text.split()), cleaned['n_tags'], cleaned['n_clean_tags'])
            else:
                text_cache_hits += 1
            text, n_char, n_words, n_tags, n_clean = cache[page.text_path]
            if undecoded(text):
                removed_undecoded.append(dict(cand_id=cid, page_id=page.page_id, date=page.date, url=page.original_url,
                                              source=page.source, in_pairs=bool(page.in_pairs)))
                continue
            live = page.source == 'live'
            by_url, route = page_types(page.page_type, page.original_url)
            type_counts[(page.page_type, by_url, route or '')] += 1
            rows.append(dict(
                fixed, date=page.date, urlkey=page.original_url,
                snap_url=None if live else f'https://web.archive.org/web/{page.date}/{page.original_url}',
                page_type=by_url, page_type_context=page.page_type, extraction_route=route, n_char=n_char, n_words=n_words, n_tags=n_tags, n_clean_tags=n_clean,
                text_snap_content=text, capture_source=page.source, snapshot_id=page.snapshot_id,
                page_id=page.page_id))
        assert rows, f'{cid} has no decoded page'
        n_snap = len({row['snapshot_id'] for row in rows})
        for row in rows:
            row['n_snap'] = n_snap
        writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
        n_rows += len(rows)
    writer.close()

    pd.DataFrame(removed_undecoded).to_csv(out / 'undecoded_pages_removed_2026.csv', index=False)
    report = dict(usable_pages=n_usable, pages_removed_by_rule=removed_pages.reason.value_counts().to_dict(),
                  candidates_with_no_page_after_rules=int(candidates_before - usable.candidate_id.nunique()),
                  pair_sides_selected_again=pair_sides_changed, undecoded_pages_removed=len(removed_undecoded),
                  undecoded_pages_removed_in_pairs=sum(r['in_pairs'] for r in removed_undecoded), wayback_duplicates_removed=int(duplicate.sum()), rows_written=n_rows,
                  candidates=int(usable.candidate_id.nunique()),
                  rows_by_source=usable.source.value_counts().to_dict(),
                  rows_by_context_type_url_type_route={' | '.join(k): v for k, v in sorted(type_counts.items())},
                  repeated_text_rows=text_cache_hits,
                  input_coverage_report_generated_at=json.loads(
                      (src / 'exports/coverage_report.json').read_text()).get('generated_at'))
    (out / 'raw_corpus_2026_build.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
