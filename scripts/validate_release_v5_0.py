"""Check the assembled release version 5.0 across its files.

`scripts/validate_release.py` holds the counts and the selection logic of the
published version (2002-2024). `scripts/assemble_release_v5_0.py` and
`scripts/assemble_release_v5_0_extraction.py` already require that each
published row is unchanged. This script checks what those two do not: the keys
and counts that connect the 2026 rows to each other and to the published rows,
and the two manifests.

Usage:
  python scripts/validate_release_v5_0.py build/release_v5_0_candidate
"""
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

RECORD_FILES = ['candidate_issue_statements.parquet', 'candidate_biography_facts.parquet',
                'candidate_endorsements.parquet']
checks = []


def check(condition, label):
    checks.append((bool(condition), label))
    if not condition:
        print('FAIL', label, flush=True)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def column(path, names):
    return pq.read_table(path, columns=names).to_pandas()


def main():
    base = Path(sys.argv[1])
    read = lambda name: pd.read_csv(base / name, dtype=str, keep_default_na=False, low_memory=False)
    universe, roster, provenance = read('candidate_year_universe.csv'), read('release_roster.csv'), read('candidate_year_provenance.csv')
    panel, coded, crosswalk = read('panel_candidate_year.csv'), read('panel_icpsr_compat.csv'), read('candidate_crosswalk.csv')
    for frame in (panel, coded, crosswalk):
        frame['candidate_cycle_id'] = frame.cand_id + '-' + frame.year

    ids = set(universe.candidate_cycle_id)
    check(universe.candidate_cycle_id.is_unique, 'universe key is unique')
    check(roster.equals(universe), 'roster equals universe')
    check(list(provenance.candidate_cycle_id) == list(universe.candidate_cycle_id), 'provenance has the universe keys in order')
    captured = set(universe.candidate_cycle_id[universe.captured.str.lower() == 'true'])
    for label, frame in (('panel', panel), ('coded panel', coded), ('crosswalk', crosswalk)):
        check(frame.candidate_cycle_id.is_unique, f'{label} key is unique')
        check(set(frame.candidate_cycle_id) == captured, f'{label} keys equal the captured universe rows')

    corpus = column(base / 'raw_corpus.parquet', ['cand_id', 'year', 'capture_source', 'snapshot_id', 'page_id', 'snap_url'])
    corpus['candidate_cycle_id'] = corpus.cand_id + '-' + corpus.year.astype(str)
    check(set(corpus.candidate_cycle_id) == captured, 'corpus candidate-years equal the captured universe rows')
    new = corpus.year == 2026
    check((corpus.capture_source[~new] == 'wayback').all(), 'earlier corpus rows are wayback')
    check(corpus.capture_source[new].isin(['live', 'wayback']).all(), '2026 capture_source is live or wayback')
    check(corpus.page_id[new].notna().all() and corpus.page_id[new].is_unique, '2026 page_id is present and unique')
    check(corpus.snapshot_id[new].notna().all(), '2026 snapshot_id is present')
    check(corpus.page_id[~new].isna().all() and corpus.snapshot_id[~new].isna().all(), 'earlier rows have no page_id or snapshot_id')
    check(corpus.snap_url[new & (corpus.capture_source == 'live')].isna().all(), 'live rows have no snap_url')
    check(corpus.snap_url[corpus.capture_source == 'wayback'].notna().all(), 'wayback rows have a snap_url')

    days = column(base / 'panel_snapshot_day.parquet', ['cand_id', 'year'])
    check(set(days.cand_id + '-' + days.year.astype(str)) == captured, 'snapshot-day candidate-years equal the captured rows')
    selected = panel[panel.year == '2026']
    check(selected.sel_snapshot_id.isin(set(corpus.snapshot_id[new])).all(), 'each 2026 sel_snapshot_id is in the corpus')
    check((panel.sel_snapshot_id[panel.year != '2026'] == '').all(), 'sel_snapshot_id is missing before 2026')

    pairs = read('event_pairs_2026.csv')
    check(not pairs.duplicated(['cand_id', 'event_id']).any(), 'pair key is unique')
    check(pairs.candidate_cycle_id.isin(ids).all(), 'pair candidates are in the universe')
    pages, snapshots = set(corpus.page_id[new]), set(corpus.snapshot_id[new])
    for side in ('pre', 'post'):
        have = pairs[pairs[f'{side}_available'] == 'True']
        check(have[f'{side}_snapshot_id'].isin(snapshots).all(), f'{side} snapshot IDs are in the corpus')
        check(have[f'{side}_homepage_page_id'].isin(pages).all(), f'{side} homepage page IDs are in the corpus')
        listed = {page for value in have[f'{side}_page_ids'] for page in json.loads(value)}
        check(listed <= pages, f'{side} page lists are in the corpus')

    status = read('candidate_extraction_status.csv')
    check(len(status) == 3 * len(universe) and not status.duplicated(['candidate_cycle_id', 'route']).any(),
          'status has three rows for each universe row')
    check(set(status.candidate_cycle_id) == ids, 'status keys equal the universe keys')
    check(not (status.extraction_status == 'failed').any(), 'no failed extraction status')
    documents = column(base / 'candidate_extraction_documents.parquet', ['parent_document_id', 'candidate_cycle_id', 'route'])
    segments = column(base / 'candidate_extraction_segments.parquet', ['segment_id', 'parent_document_id'])
    check(documents.parent_document_id.is_unique and segments.segment_id.is_unique, 'document and segment keys are unique')
    check(set(documents.candidate_cycle_id) <= captured, 'documents belong to captured candidate-years')
    check(segments.parent_document_id.isin(set(documents.parent_document_id)).all(), 'segments join to documents')
    counts = documents.groupby(['candidate_cycle_id', 'route']).size()
    stated = status.set_index(['candidate_cycle_id', 'route']).n_documents.astype(int)
    check((stated[stated > 0].sort_index() == counts.sort_index()).all() and len(counts) == (stated > 0).sum(),
          'status document counts equal the documents table')
    record_ids = []
    for name in RECORD_FILES:
        records = column(base / name, ['record_id', 'parent_document_id', 'segment_id', 'candidate_cycle_id'])
        record_ids.append(records.record_id)
        check(records.parent_document_id.isin(set(documents.parent_document_id)).all(), f'{name}: records join to documents')
        check(records.segment_id.isin(set(segments.segment_id)).all(), f'{name}: records join to segments')
        owner = documents.set_index('parent_document_id').candidate_cycle_id
        check((records.parent_document_id.map(owner) == records.candidate_cycle_id).all(), f'{name}: candidate key equals the document')
    check(pd.concat(record_ids).is_unique, 'record_id is unique across the record tables')

    for manifest_name in ('manifest.json', 'candidate_extraction_manifest.json'):
        for item in json.loads((base / manifest_name).read_text()):
            path = base / item['file']
            if path.suffix == '.parquet':
                meta = pq.ParquetFile(path).metadata
                shape = (meta.num_rows, meta.num_columns)
            else:
                shape = read(item['file']).shape
            check(shape == (item['rows'], item['columns']), f"{item['file']}: manifest rows and columns")
            check(path.stat().st_size == item['bytes'] and sha256(path) == item['sha256'], f"{item['file']}: manifest size and hash")

    failed = [label for ok, label in checks if not ok]
    print(json.dumps({'checks': len(checks), 'failed': failed}, indent=2))
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
