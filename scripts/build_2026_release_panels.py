"""Build the 2026 rows of the panel and provenance files from the 2026 corpus rows.

Outputs in the release folder:
  panel_candidate_year_2026.csv    one captured candidate, the site copy with the most text
  panel_icpsr_compat_2026.csv      ICPSR-compatible measures and topic shares
  panel_snapshot_day_2026.parquet  daily measures
  candidate_year_provenance_2026.csv

The code is the code of the published files (rebuild_release.py). One rule
differs. In the earlier years all pages of a site copy have one timestamp. In
2026 each page has its own time, so the candidate-year panel groups pages by
`snapshot_id`; `sel_date` is the earliest page time of the selected copy.

Usage:
  python scripts/build_2026_release_panels.py --out build/release_v5_0_2026
"""
import argparse
import json
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.build_panel import collapse_candidate  # noqa: E402
from scripts.build_snapshot_day_panel import build_snapshot_day_panel  # noqa: E402
from scripts.build_universe_provenance import build_candidate_year_provenance, summarize_raw_parquet  # noqa: E402
from scripts.icpsr_replicate_coding import load_ngrams, run_apply  # noqa: E402
from scripts.rebuild_release import NGRAMS, PANEL_COLUMNS, _blank_unsupported_topics, _load_topic_model  # noqa: E402


def candidate_year_panel(corpus, universe):
    info = universe.set_index('cand_id')
    rows = []
    source = pq.ParquetFile(corpus)
    for i in range(source.num_row_groups):
        d = source.read_row_group(i).to_pandas()
        assert d.cand_id.nunique() == 1, 'one row group must hold one candidate'
        start = d.groupby('snapshot_id').date.transform('min')
        d['page_date'] = d.date
        d['date'] = start + '_' + d.snapshot_id  # sorts by time; the latest copy wins a tie
        d['candidate'] = info.at[d.cand_id.iloc[0], 'candidate']
        row = collapse_candidate(d, '#+#')
        if row is None:
            continue
        selected = row['sel_date']
        row.update(sel_date=selected[:14], sel_snapshot_id=selected[15:], cand_id=d.cand_id.iloc[0])
        rows.append(row)
    panel = pd.DataFrame(rows)
    carry = ['source_cand_id', 'candidate_cycle_id', 'cand_election_yr', 'cand_status', 'universe_source',
             'on_ballot', 'district']
    panel = panel.drop(columns=['district', 'stage']).merge(universe[['cand_id'] + carry], on='cand_id', validate='one_to_one')
    panel['stage'] = panel.on_ballot.map({True: 2, False: pd.NA}).astype('Int64')
    panel['data_source'] = panel.on_ballot.map({True: 'general_2026_collection', False: 'election_year_2026_collection'})
    return panel


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    ap.add_argument('--input', default='data/collection_2026/final_20261109')
    args = ap.parse_args()
    out = Path(args.out)
    corpus = out / 'raw_corpus_2026.parquet'
    universe = pd.read_csv(out / 'candidate_year_universe_2026.csv', dtype={'cand_id': str, 'source_cand_id': str})
    crosswalk = pd.read_csv(out / 'candidate_crosswalk_2026.csv', dtype={'cand_id': str})

    panel = candidate_year_panel(corpus, universe)
    panel = panel.merge(crosswalk[['cand_id', 'candidate_year_stage']], on='cand_id', validate='one_to_one')
    assert set(panel.candidate_cycle_id) == set(universe[universe.captured].candidate_cycle_id)
    panel[PANEL_COLUMNS + ['sel_snapshot_id']].to_csv(out / 'panel_candidate_year_2026.csv', index=False)
    print('candidate-year panel', len(panel), flush=True)

    topic_model = _load_topic_model()
    with tempfile.TemporaryDirectory(prefix='release-2026-coding-') as tmp:
        coded_source = Path(tmp) / 'coded.csv'
        status = run_apply(corpus, coded_source, 200, NGRAMS, topic_model)
        if status:
            raise SystemExit(f'coding failed with status {status}')
        coded = pd.read_csv(coded_source, low_memory=False, dtype={'cand_id': str})
    add_columns = ['year', 'cand_id', 'source_cand_id', 'candidate_cycle_id', 'cand_election_yr', 'cand_status',
                   'universe_source', 'on_ballot', 'general_votes', 'stage', 'data_source', 'candidate_year_stage']
    selected = universe.merge(crosswalk[['cand_id', 'stage', 'data_source', 'candidate_year_stage']], on='cand_id')
    add = selected[add_columns]
    coded = coded.drop(columns=[c for c in add if c in coded and c not in ('year', 'cand_id')])
    coded = coded.merge(add, on=['year', 'cand_id'], how='inner', validate='one_to_one')
    assert len(coded) == len(panel), (len(coded), len(panel))
    coded['icpsr_compatible'] = coded.on_ballot
    coded = _blank_unsupported_topics(coded)
    coded.to_csv(out / 'panel_icpsr_compat_2026.csv', index=False)
    print('coded panel', coded.shape, flush=True)

    day_counts = build_snapshot_day_panel(corpus, out / 'panel_snapshot_day_2026.parquet', ngrams=load_ngrams(NGRAMS),
                                          topic_model=topic_model, topic_names=topic_model[2], overwrite=True)
    print('snapshot-day panel', day_counts, flush=True)

    empty = pd.DataFrame(columns=['candidate_cycle_id', 'cand_id', 'year', 'action', 'reason'])
    provenance = build_candidate_year_provenance(universe, summarize_raw_parquet(corpus), panel, coded, empty, None)
    # The 2026 collector records its method and its failures; the earlier years do not.
    provenance['collection_method_version'] = 'collection_2026_live_and_wayback'
    provenance['provenance_evidence_scope'] = 'released_usable_pages_and_2026_collection_roster'
    # The published status values describe the earlier years, where no search record exists.
    # For 2026 the collector searched for each URL and recorded each failed copy.
    no_capture = ~provenance.capture_status.eq('captured')
    no_url = no_capture & ~provenance.has_url.astype(bool)
    provenance.loc[no_url, 'observability_status'] = 'no_url_found'
    provenance.loc[provenance.capture_status.eq('captured_under_other_fec_id'),
                   'observability_status'] = 'captured_under_other_fec_id'
    failed = no_capture & provenance.has_url.astype(bool) & ~provenance.capture_status.eq('captured_under_other_fec_id')
    provenance.loc[failed, 'observability_status'] = 'valid_url_no_usable_capture'
    pages = pd.read_parquet(args.input + '/exports/page_corpus.parquet', columns=['candidate_id', 'depth', 'status'])
    home = pages[pages.depth.astype(str) == '0']
    reason = home.groupby('candidate_id').status.agg(lambda v: v.value_counts().index[0])
    provenance['collection_failure_reason'] = 'not_applicable'
    provenance.loc[failed, 'collection_failure_reason'] = (
        provenance.loc[failed, 'cand_id'].map(reason).fillna('no_copy_attempt_recorded'))
    provenance.to_csv(out / 'candidate_year_provenance_2026.csv', index=False)
    summary = dict(panel_rows=len(panel), coded_rows=len(coded), coded_columns=coded.shape[1],
                   snapshot_day=day_counts, provenance_rows=len(provenance), provenance_columns=provenance.shape[1],
                   text_quality=panel.text_quality.value_counts().to_dict(),
                   observability_status=provenance.observability_status.value_counts().to_dict(),
                   collection_failure_reason=provenance.collection_failure_reason.value_counts().to_dict())
    (out / 'panels_2026_summary.json').write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps(summary, indent=2, default=str))


if __name__ == '__main__':
    main()
