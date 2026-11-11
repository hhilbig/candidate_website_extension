"""Assemble the base files of release version 5.0: version 4.0 rows plus the 2026 rows.

Each output file holds the rows of the published file, unchanged and in the
published order, and then the 2026 rows. A column that only the 2026 data has
is missing for the earlier years, with one exception: `capture_source` is
`wayback` for each earlier row, because all earlier text comes from the
Internet Archive.

The extracted-variable files are not part of this script; the extraction
packager writes them.

Usage:
  python scripts/assemble_release_v5_0.py \
      --published build/release_v4_0_candidate --new build/release_v5_0_2026 \
      --out build/release_v5_0_candidate
"""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CSV_FILES = ['candidate_crosswalk', 'candidate_year_provenance', 'candidate_year_universe',
             'panel_candidate_year', 'panel_icpsr_compat']
PARQUET_FILES = ['panel_snapshot_day', 'raw_corpus']
KEYS = {'candidate_crosswalk': ['cand_id', 'year'], 'candidate_year_provenance': ['candidate_cycle_id'],
        'candidate_year_universe': ['candidate_cycle_id'], 'panel_candidate_year': ['cand_id', 'year'],
        'panel_icpsr_compat': ['cand_id', 'year']}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def merge_csv(name, published, new, out):
    old = pd.read_csv(published / f'{name}.csv', dtype=str, keep_default_na=False)
    add = pd.read_csv(new / f'{name}_2026.csv', dtype=str, keep_default_na=False)
    assert [c for c in add.columns if c in old.columns] == list(old.columns), name
    assert set(old.year) & set(add.year) == set() and set(add.year) == {'2026'}, name
    extra = [c for c in add.columns if c not in old.columns]
    both = pd.concat([old, add], ignore_index=True)[list(old.columns) + extra].fillna('')
    assert not both.duplicated(KEYS[name]).any(), name
    both.to_csv(out / f'{name}.csv', index=False)
    # The published rows must come back unchanged.
    check = pd.read_csv(out / f'{name}.csv', dtype=str, keep_default_na=False, nrows=len(old))
    assert check[list(old.columns)].equals(old), name
    return len(both), both.shape[1], extra


def merge_parquet(name, published, new, out):
    old, add = pq.ParquetFile(published / f'{name}.parquet'), pq.ParquetFile(new / f'{name}_2026.parquet')
    schema = add.schema_arrow
    for field in old.schema_arrow:
        assert schema.field(field.name).type == field.type, (name, field.name)
    assert schema.names[:len(old.schema_arrow.names)] == old.schema_arrow.names, name
    extra = schema.names[len(old.schema_arrow.names):]
    rows = 0
    with pq.ParquetWriter(out / f'{name}.parquet', schema, compression='zstd') as writer:
        for i in range(old.num_row_groups):
            table = old.read_row_group(i)
            for column in extra:
                value = 'wayback' if column == 'capture_source' else None
                table = table.append_column(schema.field(column), pa.array([value] * len(table), schema.field(column).type))
            writer.write_table(table)
            rows += len(table)
        for i in range(add.num_row_groups):
            table = add.read_row_group(i).cast(schema)
            assert set(table.column('year').to_pylist()) == {2026}, name
            writer.write_table(table)
            rows += len(table)
    assert rows == old.metadata.num_rows + add.metadata.num_rows
    return rows, len(schema.names), extra


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--published', required=True)
    ap.add_argument('--new', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    published, new, out = Path(args.published), Path(args.new), Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest, report = [], {}
    for name in CSV_FILES + PARQUET_FILES:
        suffix = 'csv' if name in CSV_FILES else 'parquet'
        rows, columns, extra = (merge_csv if suffix == 'csv' else merge_parquet)(name, published, new, out)
        path = out / f'{name}.{suffix}'
        manifest.append(dict(file=path.name, rows=rows, columns=columns, bytes=path.stat().st_size, sha256=sha256(path)))
        report[path.name] = dict(rows=rows, columns=columns, new_columns=extra)
        print(path.name, report[path.name], flush=True)
    # release_roster.csv is a copy of the universe file, as in version 4.0.
    shutil.copyfile(out / 'candidate_year_universe.csv', out / 'release_roster.csv')
    entry = dict(next(m for m in manifest if m['file'] == 'candidate_year_universe.csv'), file='release_roster.csv')
    manifest.append(entry)
    manifest.sort(key=lambda m: m['file'])
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    (out / 'assemble_report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
