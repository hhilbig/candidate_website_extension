"""Correct `on_ballot` in the rows for 2002 through 2024 of release version 5.0.

Decision of November 11, 2026 (Hanno), after the first-user review. The published flag
has three defects:

  1. A real general-election candidate has no flag. The published match did not read the
     returns rows with the party labels of Minnesota and North Dakota
     (DEMOCRATIC-FARMER-LABOR, DEMOCRATIC-NPL), and most at-large House seats have no flag.
  2. A flag has no returns row of the candidate's party. The published match did not
     compare the party, so a candidate got the votes of a namesake of the other party.
  3. One returns row gives a flag to two or three FEC IDs (one person with several IDs,
     or two persons with one family name).

The script matches the universe to the MIT returns again, with the party in the key and
with the party labels above. It adds each new match (1), removes each flag that has no
match (2), and keeps one FEC ID for each returns row (3). Order for (3): the ID with
text in the corpus; then the given name that equals the name in the returns; then more
page rows; then FEC status C; then the lower ID. Each other flag stays as published.

The script changes `on_ballot`, `general_votes`, `stage`, `data_source`,
`candidate_year_stage`, and `icpsr_compatible` in each file that holds them. The 2026 rows do not change.

Usage:
  python scripts/correct_on_ballot_v5_0.py build/release_v5_0_candidate \
      --report quality_reports/on_ballot_correction_v5_0.csv
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import coverage_vs_returns as returns  # noqa: E402

PARTY_LABELS = {
    'DEMOCRATIC-FARMER-LABOR': 'DEMOCRAT', 'DEMOCRATIC-FARM-LABOR': 'DEMOCRAT',
    'DEMOCRATIC-NONPARTISAN LEAGUE': 'DEMOCRAT', 'DEMOCRATIC-NPL': 'DEMOCRAT',
    'REPUBLICAN, LIBERTARIAN': 'REPUBLICAN',
}
CSV_FILES = ['candidate_year_universe.csv', 'release_roster.csv', 'candidate_year_provenance.csv',
             'panel_candidate_year.csv', 'panel_icpsr_compat.csv', 'candidate_crosswalk.csv']
PARQUET_FILES = ['raw_corpus.parquet', 'panel_snapshot_day.parquet']


def ballot():
    """MIT returns, one row for each major-party general-election candidate."""
    read = pd.read_csv

    def with_labels(path, *args, **kwargs):
        frame = read(path, *args, **kwargs)
        if str(path).endswith('house.tab'):
            frame['party'] = frame.party.replace(PARTY_LABELS)
        return frame
    returns.pd.read_csv = with_labels
    try:
        return returns.ballot()
    finally:
        returns.pd.read_csv = read


def decide(universe, page_rows):
    """Return the corrected flag and votes for the rows before 2026, and the list of changes."""
    old = universe[universe.year.astype(int) <= 2024].copy()
    old['flag'] = old.on_ballot.str.lower() == 'true'
    work = old[['candidate_cycle_id', 'year', 'office', 'state', 'fec_raw_name', 'party']].copy()
    work['year'] = work.year.astype(int)
    work['state_po'] = work.state
    work['district'] = pd.to_numeric(old.district.replace('', None), errors='coerce')
    work.loc[work.office == 'senate', 'district'] = 0
    work['district'] = work.district.fillna(-1).astype(int)
    work = returns.keys(work, 'fec_raw_name', comma=True)
    source = ballot()
    matched = []
    for label, party in (('DEMOCRAT', 'D'), ('REPUBLICAN', 'R')):
        side = work[work.party == party]
        result = returns.reverse_match(side, source[source.party == label])
        result['candidate_cycle_id'] = side.candidate_cycle_id.values
        matched.append(result)
    matched = pd.concat(matched).set_index('candidate_cycle_id')
    assert set(old.party) <= {'D', 'R'}
    old['new_flag'] = old.candidate_cycle_id.map(matched.on_ballot).fillna(False).astype(bool)
    old['new_votes'] = pd.to_numeric(old.candidate_cycle_id.map(matched.general_votes), errors='coerce')
    old['votes'] = pd.to_numeric(old.general_votes.replace('', None), errors='coerce')

    old['change'] = ''
    added = ~old.flag & old.new_flag
    removed = old.flag & ~old.new_flag
    old.loc[added, 'votes'] = old.loc[added, 'new_votes']
    # A flag with no returns row of the candidate's party is wrong only if another flagged
    # candidate of the race has the same vote total (the returns row belongs to that
    # candidate). Else the FEC party code differs from the ballot party, and the flag stays.
    race = ['year', 'office', 'state', 'district']
    claimed = old[(old.flag | added) & ~removed].groupby(race + ['votes'], dropna=False).size()
    for index in old.index[removed]:
        row = old.loc[index]
        if (row.year, row.office, row.state, row.district, row.votes) not in claimed.index:
            removed[index] = False
    old.loc[added, 'change'] = 'added_match_in_returns'
    old.loc[removed, 'change'] = 'removed_returns_row_of_other_candidate'
    old.loc[added, 'votes'] = old.loc[added, 'new_votes']
    old['final'] = (old.flag | added) & ~removed

    # One returns row, several FEC IDs.
    old['first'] = work['first'].values
    old['rows'] = old.candidate_cycle_id.map(page_rows).fillna(0).astype(int)
    old['has_text'] = old.captured.str.lower() == 'true'
    names = {(r.year, r.office, r.state_po, int(r.candidatevotes), r.party[0]): {first for _, first in r.lasts}
             for r in source.itertuples()}
    old['kept_cand_id'] = ''
    flagged = old[old.final]
    key = ['year', 'office', 'state', 'district', 'party', 'votes']
    for values, group in flagged.groupby(key, dropna=False):
        if len(group) < 2:
            continue
        official = names.get((int(values[0]), values[1], values[2], int(values[5]), values[4]), set())
        ranked = group.assign(exact=group['first'].isin(official), status_c=group.cand_status == 'C').sort_values(
            ['has_text', 'exact', 'rows', 'status_c', 'cand_id'], ascending=[False, False, False, False, True])
        keep = ranked.cand_id.iloc[0]
        for index in ranked.index[1:]:
            old.at[index, 'final'] = False
            old.at[index, 'change'] = 'removed_other_fec_id_of_one_returns_row'
            old.at[index, 'kept_cand_id'] = keep
    old.loc[~old.final, 'votes'] = float('nan')
    assert old.loc[old.final, 'votes'].notna().all()
    assert not old[old.final].duplicated(key).any()
    changes = old[old.change != ''][['candidate_cycle_id', 'candidate', 'office', 'year', 'state', 'district', 'party',
                                     'captured', 'change', 'kept_cand_id', 'votes']]
    return old.set_index('candidate_cycle_id')[['final', 'votes']], changes


def patterns(frame, columns):
    """Values that the file uses for a flagged and an unflagged row before 2026."""
    early = frame[frame.year.astype(int) <= 2024]
    flag = early.on_ballot.astype(str).str.lower() == 'true'
    return ({c: early.loc[flag, c].iloc[0] for c in columns}, {c: early.loc[~flag, c].iloc[0] for c in columns})


def cycle_ids(frame):
    if 'candidate_cycle_id' in frame:
        return frame.candidate_cycle_id
    return frame.cand_id + '-' + frame.year.astype(str)


ICPSR_NAME = {}  # candidate_cycle_id -> candidate_icpsr, from the crosswalk


def stage_name(frame, flagged):
    name = (frame.loc[flagged, 'candidate_icpsr'] if 'candidate_icpsr' in frame
            else cycle_ids(frame)[flagged].map(ICPSR_NAME))
    assert name.notna().all()
    return name.astype(str) + '_' + frame.loc[flagged, 'year'].astype(str) + '_2'


def correct_csv(path, decision, changed):
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)
    ids = cycle_ids(frame)
    hit = ids.isin(changed)
    if not hit.any():
        return 0
    fixed = [c for c in ('on_ballot', 'stage', 'data_source', 'icpsr_compatible') if c in frame]
    yes, no = patterns(frame, fixed)
    flag = ids.map(decision.final)
    for column in fixed:
        frame.loc[hit & (flag == True), column] = yes[column]  # noqa: E712
        frame.loc[hit & (flag == False), column] = no[column]  # noqa: E712
    if 'general_votes' in frame:
        votes = ids.map(decision.votes)
        frame.loc[hit, 'general_votes'] = [('' if pd.isna(v) else f'{v:.1f}') for v in votes[hit]]
    if 'candidate_year_stage' in frame:
        on = hit & (flag == True)  # noqa: E712
        frame.loc[hit, 'candidate_year_stage'] = ''
        frame.loc[on, 'candidate_year_stage'] = stage_name(frame, on)
    frame.to_csv(path, index=False)
    return int(hit.sum())


def correct_parquet(path, decision, changed):
    source = pq.ParquetFile(path)
    names = source.schema_arrow.names
    probe = source.read(columns=[c for c in ('candidate_cycle_id', 'cand_id', 'year', 'on_ballot', 'candidate_year_stage')
                                 if c in names]).to_pandas()
    early = probe[probe.year <= 2024]
    blank = early.loc[~early.on_ballot, 'candidate_year_stage'].iloc[0]
    del probe, early
    temporary = path.with_suffix('.tmp.parquet')
    writer = pq.ParquetWriter(temporary, source.schema_arrow, compression='zstd')
    rows = 0
    for group in range(source.num_row_groups):
        table = source.read_row_group(group)
        frame = table.select([c for c in ('candidate_cycle_id', 'cand_id', 'year', 'candidate_icpsr', 'on_ballot', 'stage',
                                          'data_source', 'candidate_year_stage') if c in names]).to_pandas()
        ids = cycle_ids(frame)
        hit = ids.isin(changed)
        if hit.any():
            rows += int(hit.sum())
            flag = ids.map(decision.final)
            on, off = hit & (flag == True), hit & (flag == False)  # noqa: E712
            frame.loc[on, 'on_ballot'], frame.loc[off, 'on_ballot'] = True, False
            stage = frame.stage.astype('Int64')
            stage[on], stage[off] = 2, pd.NA
            frame['stage'] = stage
            frame.loc[on, 'data_source'], frame.loc[off, 'data_source'] = 'general_wayback', 'election_year_wayback'
            frame.loc[off, 'candidate_year_stage'] = blank
            frame.loc[on, 'candidate_year_stage'] = stage_name(frame, on)
            for column in ('on_ballot', 'stage', 'data_source', 'candidate_year_stage'):
                field = source.schema_arrow.field(column)
                table = table.set_column(names.index(column), field, pa.array(frame[column], type=field.type, from_pandas=True))
        writer.write_table(table)
    writer.close()
    temporary.replace(path)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('folder')
    ap.add_argument('--report', required=True)
    args = ap.parse_args()
    base = Path(args.folder)
    universe = pd.read_csv(base / 'candidate_year_universe.csv', dtype=str, keep_default_na=False)
    page_rows = pd.Series(pq.read_table(base / 'raw_corpus.parquet', columns=['candidate_cycle_id'])
                          .column('candidate_cycle_id').to_pylist()).value_counts()
    crosswalk = pd.read_csv(base / 'candidate_crosswalk.csv', dtype=str, keep_default_na=False)
    ICPSR_NAME.update(zip(crosswalk.cand_id + '-' + crosswalk.year, crosswalk.candidate_icpsr))
    decision, changes = decide(universe, page_rows)
    changes.to_csv(args.report, index=False)
    changed = set(changes.candidate_cycle_id)
    print(changes.change.value_counts().to_dict(), flush=True)
    for name in CSV_FILES:
        print(name, correct_csv(base / name, decision, changed), flush=True)
    for name in PARQUET_FILES:
        print(name, correct_parquet(base / name, decision, changed), flush=True)
    manifest = json.loads((base / 'manifest.json').read_text())
    for item in manifest:
        if item['file'] in CSV_FILES + PARQUET_FILES:
            path = base / item['file']
            item['bytes'], item['sha256'] = path.stat().st_size, hashlib.sha256(path.read_bytes()).hexdigest()
    (base / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    after = pd.read_csv(base / 'candidate_year_universe.csv', dtype=str, keep_default_na=False)
    early = after[after.year.astype(int) <= 2024]
    assert (early.set_index('candidate_cycle_id').on_ballot.str.lower() == 'true').equals(
        decision.final.reindex(early.candidate_cycle_id))
    assert after[after.year == '2026'].equals(universe[universe.year == '2026'])
    print('on_ballot before 2026:', int((early.on_ballot.str.lower() == 'true').sum()))


if __name__ == '__main__':
    main()
