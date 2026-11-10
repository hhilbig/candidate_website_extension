"""Build the 2026 rows of raw_corpus.parquet from the final 2026 collection exports.

One output row is one usable page of one candidate at one copy time. The
columns are the 28 columns of the published raw_corpus.parquet, plus three
columns that the 2026 data needs:

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
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.harmonize_deliverable import district_id, icpsr_name, name_key, norm_district  # noqa: E402
from scripts.icpsr_replicate_coding import icpsr_clean_text  # noqa: E402

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
])


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
    pairs = pd.read_parquet(src / 'exports/candidate_event_pairs.parquet')
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
            live = page.source == 'live'
            rows.append(dict(
                fixed, date=page.date, urlkey=page.original_url,
                snap_url=None if live else f'https://web.archive.org/web/{page.date}/{page.original_url}',
                page_type=page.page_type, n_char=n_char, n_words=n_words, n_tags=n_tags, n_clean_tags=n_clean,
                text_snap_content=text, capture_source=page.source, snapshot_id=page.snapshot_id,
                page_id=page.page_id))
        writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
        n_rows += len(rows)
    writer.close()

    report = dict(usable_pages=n_usable, wayback_duplicates_removed=int(duplicate.sum()), rows_written=n_rows,
                  candidates=int(usable.candidate_id.nunique()),
                  rows_by_source=usable.source.value_counts().to_dict(),
                  rows_by_page_type=usable.page_type.value_counts().to_dict(),
                  repeated_text_rows=text_cache_hits,
                  input_coverage_report_generated_at=json.loads(
                      (src / 'exports/coverage_report.json').read_text()).get('generated_at'))
    (out / 'raw_corpus_2026_build.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
