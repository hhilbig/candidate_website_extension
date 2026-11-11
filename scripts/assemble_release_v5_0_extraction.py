"""Add the 2026 extracted-variable files to release version 5.0.

Each output file holds the rows of the published version 4.0 file, unchanged
and in the published order, and then the 2026 rows of the extraction packager.
The two inputs must have the same columns and types. No candidate-year,
document, segment, or record identifier can occur in both.

The script writes `candidate_extraction_manifest.json` for the six files, and
adds each `--also` file to `manifest.json`.

Usage:
  python scripts/assemble_release_v5_0_extraction.py \
      --published build/release_v4_0_candidate \
      --new build/hive_candidate_extraction/release_2026_20261111_v22_model \
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


def merge_parquet(name, key, published, new, out):
    old, add = pq.read_table(published / name), pq.read_table(new / name)
    assert old.schema.names == add.schema.names, (name, 'columns differ')
    add = add.cast(old.schema)
    assert key in old.schema.names, (name, key)
    shared = set(old.column(key).to_pylist()) & set(add.column(key).to_pylist())
    assert not shared, (name, f'{len(shared)} identifiers occur in both inputs')
    years = set(pa.compute.utf8_slice_codeunits(add.column('candidate_cycle_id'), -4).to_pylist())
    assert years == {'2026'}, (name, years)
    both = pa.concat_tables([old, add])
    pq.write_table(both, out / name, compression='zstd')
    assert pq.read_table(out / name).slice(0, len(old)).equals(old), name
    return len(old), len(add), len(both.schema.names)


def merge_status(published, new, out):
    old = pd.read_csv(published / STATUS, dtype=str, keep_default_na=False)
    add = pd.read_csv(new / STATUS, dtype=str, keep_default_na=False)
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
    ap.add_argument('--also', action='append', default=[])
    args = ap.parse_args()
    published, new, out = Path(args.published), Path(args.new), Path(args.out)
    report = {}
    for name, key in PARQUET_KEYS.items():
        report[name] = dict(zip(('published_rows', 'rows_2026', 'columns'), merge_parquet(name, key, published, new, out)))
        print(name, report[name], flush=True)
    report[STATUS] = dict(zip(('published_rows', 'rows_2026', 'columns'), merge_status(published, new, out)))
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
    (out / 'assemble_extraction_report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
