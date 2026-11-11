"""Build event_pairs_2026.csv: one row for each 2026 candidate and election event.

The collector's pair table names, for each candidate and event, the nearest
complete website copy before and after the event. This script makes the table
agree with the release corpus:

  * A candidate whose text is under another FEC ID of the same person has no
    pages in the corpus. Both sides become unavailable, with the reason
    `captured_under_other_fec_id`.
  * A page that the corpus build removed (undecoded archive body) leaves the
    page list of its side. No removed page is a homepage, so each side stays
    available. `*_n_pages` is the new count; the other side fields stay.

Each page identifier in the output is a `page_id` of raw_corpus.parquet, and
each snapshot identifier is a `snapshot_id` of that file. The script fails if
this is not true.

Usage:
  python scripts/build_2026_release_pairs.py \
      --input data/collection_2026/final_20261109 --release build/release_v5_0_2026
"""
import argparse
import json
from pathlib import Path

import pandas as pd

SIDE_FIELDS = ['snapshot_id', 'homepage_page_id', 'captured_at', 'source', 'distance_days', 'local_date_min',
               'local_date_max', 'page_ids', 'n_pages', 'page_types', 'page_sources', 'page_time_min',
               'page_time_max', 'page_span_hours', 'snapshot_complete']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True)
    ap.add_argument('--release', required=True)
    args = ap.parse_args()
    release = Path(args.release)
    pairs = pd.read_parquet(Path(args.input) / 'exports/candidate_event_pairs.parquet').fillna('')
    corpus = pd.read_parquet(release / 'raw_corpus_2026.parquet', columns=['cand_id', 'page_id', 'snapshot_id'])
    universe = pd.read_csv(release / 'candidate_year_universe_2026.csv', dtype=str)
    provenance = pd.read_csv(release / 'candidate_year_provenance_2026.csv', dtype=str)
    pages, snapshots = set(corpus.page_id), set(corpus.snapshot_id)
    other_id = set(provenance[provenance.capture_status == 'captured_under_other_fec_id'].cand_id)
    assert pairs.candidate_id.isin(universe.cand_id).all()
    assert not pairs.duplicated(['candidate_id', 'event_id']).any()

    moved = pairs.candidate_id.isin(other_id)
    pages_removed = 0
    for side in ('pre', 'post'):
        hit = moved & (pairs[f'{side}_available'] == 'True')
        pairs.loc[hit, f'{side}_available'] = 'False'
        pairs.loc[hit, f'{side}_missing_reason'] = 'captured_under_other_fec_id'
        for field in SIDE_FIELDS:
            pairs.loc[hit, f'{side}_{field}'] = ''
        available = pairs[f'{side}_available'] == 'True'
        for i in pairs.index[available]:
            ids = json.loads(pairs.at[i, f'{side}_page_ids'])
            kept = [x for x in ids if x in pages]
            if len(kept) != len(ids):
                pages_removed += len(ids) - len(kept)
                pairs.at[i, f'{side}_page_ids'] = json.dumps(kept)
                pairs.at[i, f'{side}_n_pages'] = str(len(kept))
            assert kept and pairs.at[i, f'{side}_homepage_page_id'] in kept
            assert pairs.at[i, f'{side}_snapshot_id'] in snapshots
    pairs.loc[moved, ['mixed_source_pair', 'pair_homepage_span_days']] = ['False', '']
    pre, post = pairs.pre_available == 'True', pairs.post_available == 'True'
    status = pd.Series('uncovered', index=pairs.index)
    status[pre & post], status[pre & ~post], status[~pre & post] = 'paired', 'pre_only', 'post_only'
    changed = int((status != pairs.coverage_status).sum())
    assert (status[~moved] == pairs.coverage_status[~moved]).all()
    pairs['coverage_status'] = status

    pairs = pairs.rename(columns={'candidate_id': 'cand_id'})
    pairs.insert(1, 'candidate_cycle_id', pairs.cand_id + '-2026')
    assert pairs.candidate_cycle_id.isin(universe.candidate_cycle_id).all()
    pairs.to_csv(release / 'event_pairs_2026.csv', index=False)
    report = dict(rows=len(pairs), columns=pairs.shape[1], candidates=int(pairs.cand_id.nunique()),
                  rows_of_candidates_under_other_fec_id=int(moved.sum()), coverage_status_changed=changed,
                  pages_removed_from_lists=pages_removed, coverage_status=pairs.coverage_status.value_counts().to_dict())
    (release / 'event_pairs_2026_build.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
