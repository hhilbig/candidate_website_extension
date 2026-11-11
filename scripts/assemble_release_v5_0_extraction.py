"""Add the 2026 extracted-variable files to release version 5.0.

Each output file holds the rows of the published version 4.0 file, unchanged
and in the published order, and then the 2026 rows of the extraction packager.
The two inputs must have the same columns and types. No candidate-year,
document, segment, or record identifier can occur in both.

The 2026 extraction ran on the corpus of November 10. The release corpus omits some of
those pages (decision of November 11: pages of other hosts, one hacked page). The script
keeps a 2026 document only if the release corpus still holds its text for that candidate
and route, and it computes the 2026 status rows again from the kept rows.

The documents table gets one new column, `route_source`: `url` if a page of the document
has `page_type` equal to the route, else `link_context` (2026 only).

The script writes `candidate_extraction_manifest.json` for the six files, and
adds each `--also` file to `manifest.json`.

Usage:
  python scripts/assemble_release_v5_0_extraction.py \
      --published build/release_v4_0_candidate \
      --new build/hive_candidate_extraction/release_2026_20261111_v22_model \
      --corpus-2026 build/release_v5_0_2026/raw_corpus_2026.parquet \
      --out build/release_v5_0_candidate --also event_pairs_2026.csv
"""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PARQUET_KEYS = {
    'candidate_issue_statements.parquet': 'record_id',
    'candidate_biography_facts.parquet': 'record_id',
    'candidate_endorsements.parquet': 'record_id',
    'candidate_extraction_documents.parquet': 'parent_document_id',
    'candidate_extraction_segments.parquet': 'segment_id',
}
STATUS = 'candidate_extraction_status.csv'


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def corpus_documents(corpus_path):
    """Return, for each (candidate_cycle_id, route, text hash) of the 2026 corpus, the row count and the route source."""
    rows = pq.read_table(corpus_path, columns=['candidate_cycle_id', 'extraction_route', 'page_type',
                                                'text_snap_content']).to_pandas()
    rows = rows[rows.extraction_route.notna()]
    rows['sha'] = [hashlib.sha256(text.encode()).hexdigest() for text in rows.text_snap_content]
    rows['by_url'] = rows.page_type == rows.extraction_route
    grouped = rows.groupby(['candidate_cycle_id', 'extraction_route', 'sha']).agg(n=('sha', 'size'), by_url=('by_url', 'any'))
    pages = rows.groupby(['candidate_cycle_id', 'extraction_route']).size()
    return grouped, pages


def merge_parquet(name, key, published, new, out, keep=None, documents=None):
    old, add = pq.read_table(published / name), pq.read_table(new / name)
    if keep is not None:
        add = add.filter(pa.compute.is_in(add.column('parent_document_id'), value_set=pa.array(sorted(keep))))
    assert old.schema.names == add.schema.names, (name, 'columns differ')
    add = add.cast(old.schema)
    assert key in old.schema.names, (name, key)
    shared = set(old.column(key).to_pylist()) & set(add.column(key).to_pylist())
    assert not shared, (name, f'{len(shared)} identifiers occur in both inputs')
    years = set(pa.compute.utf8_slice_codeunits(add.column('candidate_cycle_id'), -4).to_pylist())
    assert years == {'2026'}, (name, years)
    both = pa.concat_tables([old, add])
    if documents is not None:
        # documents: parent_document_id -> (rows in the release corpus, route source)
        ids = add.column('parent_document_id').to_pylist()
        counts = old.column('n_raw_occurrences').to_pylist() + [documents[i][0] for i in ids]
        source = ['url'] * len(old) + [documents[i][1] for i in ids]
        position = both.schema.names.index('n_raw_occurrences')
        both = both.set_column(position, both.schema.field(position), pa.array(counts, type=both.schema.field(position).type))
        both = both.append_column('route_source', pa.array(source, type=pa.string()))
    pq.write_table(both, out / name, compression='zstd')
    written = pq.read_table(out / name).slice(0, len(old))
    assert (written.drop_columns(['route_source']) if documents is not None else written).equals(old), name
    return len(old), len(add), len(both.schema.names)


def status_2026(add, out, pages):
    """Compute the 2026 status rows again from the kept documents, segments, and records."""
    documents = pq.read_table(out / 'candidate_extraction_documents.parquet', columns=[
        'candidate_cycle_id', 'route', 'withheld_overlap_conflict_group_count']).to_pandas()
    documents = documents[documents.candidate_cycle_id.str.endswith('-2026')]
    segments = pq.read_table(out / 'candidate_extraction_segments.parquet', columns=['candidate_cycle_id', 'route']).to_pandas()
    segments = segments[segments.candidate_cycle_id.str.endswith('-2026')]
    records = []
    for name, route in (('candidate_issue_statements.parquet', 'issues'), ('candidate_biography_facts.parquet', 'biography'),
                        ('candidate_endorsements.parquet', 'endorsements')):
        ids = pq.read_table(out / name, columns=['candidate_cycle_id']).to_pandas()
        records.append(ids[ids.candidate_cycle_id.str.endswith('-2026')].assign(route=route))
    key = ['candidate_cycle_id', 'route']
    counts = pd.DataFrame(dict(
        n_documents=documents.groupby(key).size(), n_segments=segments.groupby(key).size(),
        n_records=pd.concat(records).groupby(key).size(),
        n_withheld_overlap_conflict_groups=documents.groupby(key).withheld_overlap_conflict_group_count.sum()))
    index = pd.MultiIndex.from_frame(add[key])
    new = add.copy()
    new['n_raw_pages'] = pages.reindex(index).fillna(0).astype(int).astype(str).values
    for column in counts:
        new[column] = counts[column].reindex(index).fillna(0).astype(int).astype(str).values
    same = (new[['n_raw_pages', 'n_documents', 'n_segments', 'n_records', 'n_withheld_overlap_conflict_groups']]
            == add[['n_raw_pages', 'n_documents', 'n_segments', 'n_records', 'n_withheld_overlap_conflict_groups']]).all(axis=1)
    for i in new.index[~same]:
        if new.at[i, 'n_raw_pages'] == '0':
            assert new.at[i, 'n_documents'] == '0'
            new.at[i, 'source_observability'], new.at[i, 'extraction_status'] = 'not_observed', 'not_extracted_no_source'
        elif new.at[i, 'n_records'] != '0':
            new.at[i, 'extraction_status'] = 'observed_positive'
        else:
            # Pages remain and no record remains. Only a row that had this state before can keep its status.
            assert add.at[i, 'n_records'] == '0' and new.at[i, 'n_documents'] == add.at[i, 'n_documents'], new.loc[i].to_dict()
    return new, int((~same).sum())


def merge_status(published, new, out, pages):
    old = pd.read_csv(published / STATUS, dtype=str, keep_default_na=False)
    add = pd.read_csv(new / STATUS, dtype=str, keep_default_na=False)
    add, changed = status_2026(add, out, pages)
    print('2026 status rows computed again with a different count:', changed, flush=True)
    assert list(old.columns) == list(add.columns), 'status columns differ'
    assert not set(old.candidate_cycle_id) & set(add.candidate_cycle_id)
    assert add.candidate_cycle_id.str.endswith('-2026').all()
    both = pd.concat([old, add], ignore_index=True)
    assert not both.duplicated(['candidate_cycle_id', 'route']).any()
    both.to_csv(out / STATUS, index=False)
    check = pd.read_csv(out / STATUS, dtype=str, keep_default_na=False, nrows=len(old))
    assert check.equals(old)
    return len(old), len(add), both.shape[1]


def entry(path):
    if path.suffix == '.parquet':
        meta = pq.ParquetFile(path).metadata
        rows, columns = meta.num_rows, meta.num_columns
    else:
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        rows, columns = frame.shape
    return dict(file=path.name, rows=int(rows), columns=int(columns), bytes=path.stat().st_size, sha256=sha256(path))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--published', required=True)
    ap.add_argument('--new', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--corpus-2026', required=True)
    ap.add_argument('--also', action='append', default=[])
    args = ap.parse_args()
    published, new, out = Path(args.published), Path(args.new), Path(args.out)
    report = {}
    in_corpus, pages = corpus_documents(Path(args.corpus_2026))
    packaged = pq.read_table(new / 'candidate_extraction_documents.parquet', columns=[
        'parent_document_id', 'candidate_cycle_id', 'route', 'raw_text_sha256', 'n_raw_occurrences']).to_pandas()
    index = pd.MultiIndex.from_frame(packaged[['candidate_cycle_id', 'route', 'raw_text_sha256']])
    packaged['n'] = in_corpus.n.reindex(index).values
    packaged['by_url'] = in_corpus.by_url.reindex(index).values
    kept = packaged[packaged.n.notna()]
    documents = {r.parent_document_id: (int(r.n), 'url' if r.by_url else 'link_context') for r in kept.itertuples()}
    report['documents_2026'] = dict(packaged=len(packaged), kept=len(kept),
                                    occurrence_count_changed=int((kept.n != kept.n_raw_occurrences).sum()),
                                    route_source=kept.by_url.map({True: 'url', False: 'link_context'}).value_counts().to_dict())
    print(report['documents_2026'], flush=True)
    for name, key in PARQUET_KEYS.items():
        report[name] = dict(zip(('published_rows', 'rows_2026', 'columns'), merge_parquet(
            name, key, published, new, out, keep=set(documents),
            documents=documents if name == 'candidate_extraction_documents.parquet' else None)))
        print(name, report[name], flush=True)
    report[STATUS] = dict(zip(('published_rows', 'rows_2026', 'columns'), merge_status(published, new, out, pages)))
    print(STATUS, report[STATUS], flush=True)
    # As in version 4.0, the extracted-variable files have their own manifest.
    order = [STATUS, 'candidate_extraction_documents.parquet', 'candidate_extraction_segments.parquet',
             'candidate_issue_statements.parquet', 'candidate_biography_facts.parquet',
             'candidate_endorsements.parquet']
    extraction = [dict(sorted(entry(out / name).items())) for name in order]
    (out / 'candidate_extraction_manifest.json').write_text(json.dumps(extraction, indent=2) + '\n')
    manifest = json.loads((out / 'manifest.json').read_text())
    manifest = [m for m in manifest if m['file'] not in args.also] + [entry(out / name) for name in args.also]
    manifest.sort(key=lambda m: m['file'])
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    (out.parent / 'release_v5_0_reports').mkdir(exist_ok=True)
    (out.parent / 'release_v5_0_reports' / 'assemble_extraction_report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
