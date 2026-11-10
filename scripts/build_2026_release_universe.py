"""Build the 2026 rows of the universe and crosswalk files, and fill the ballot fields of the 2026 corpus rows.

Population (same rule as the published files): Democratic and Republican FEC
candidates with election year 2026; all House records; Senate records only in
states with a Senate election on November 3, 2026. General-election candidates
with an FEC record outside that population enter as `ballot_override` rows.

`on_ballot` comes from the provisional list of build_2026_ballot_list.py.
`general_votes` stays missing for 2026: the available counts are not final.

Usage:
  python scripts/build_2026_release_universe.py \
      --input data/collection_2026/final_20261109 --out build/release_v5_0_2026 \
      --crosswalk build/release_v4_0_candidate/candidate_crosswalk.csv
"""
import argparse
import json
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.build_universe_provenance import (  # noqa: E402
    normalize_website_domain, normalize_website_url, website_cycle_id)
from scripts.harmonize_deliverable import district_id, icpsr_name, name_key  # noqa: E402
from src.name_utils import clean_name  # noqa: E402

FEC_COLUMNS = ['cand_id', 'cand_name', 'cand_pty_affiliation', 'cand_election_yr', 'cand_office_st', 'cand_office',
               'cand_office_district', 'cand_ici', 'cand_status', 'cand_pcc', 'st1', 'st2', 'city', 'st', 'zip']
# General-election candidates (provisional list) whose FEC record is outside the population rule.
# cand_id: (party, district or None, reason)
BALLOT_OVERRIDES = {
    'S8ID00092': ('R', None, 'FEC party code UNK; Republican nominee, Idaho Senate'),
    'H4MD05169': ('R', 5, 'FEC party code REO; Republican nominee, Maryland 5'),
    'H2MA08090': ('R', 8, 'FEC party field holds 08; Republican nominee, Massachusetts 8'),
    'H6VI01065': ('D', 0, 'FEC party code IND; Democratic nominee, Virgin Islands delegate'),
    'H2MN06214': ('D', 8, 'FEC election year 2024 and district 06; DFL nominee, Minnesota 8'),
}
UNIVERSE_COLUMNS = [
    'candidate_cycle_id', 'cand_id', 'source_cand_id', 'cand_election_yr', 'cand_status', 'cand_ici', 'candidate',
    'fec_raw_name', 'state', 'source_district', 'district', 'geography_status', 'office', 'year', 'party',
    'universe_source', 'on_ballot', 'general_votes', 'historical_attempt_record', 'website_url',
    'normalized_website_url', 'website_domain', 'website_cycle_id', 'url_discovery_status', 'url_validation_status',
    'url_source', 'has_url', 'captured', 'capture_status']
CROSSWALK_COLUMNS = [
    'candidate', 'state', 'district', 'office', 'year', 'party', 'district_i', 'cand_id', 'fec_raw_name', 'cand_ici',
    'bonica_rid', 'ICPSR', 'NID', 'cfscore', 'seat', 'lname', 'fname', 'cfscore_dyn', 'dwnom1', 'dwnom2',
    'dime_district', 'dime_ico', 'candidate_icpsr', 'name_key', 'party_full', 'stage', 'data_source', 'district_id',
    'candidate_year_stage', 'on_ballot', 'general_votes', 'cand_election_yr', 'cand_status', 'source_cand_id',
    'universe_source', 'candidate_cycle_id']
DATA_SOURCE = {True: 'general_2026_collection', False: 'election_year_2026_collection'}


def geography(frame):
    frame['district'] = pd.to_numeric(frame.source_district, errors='coerce')
    frame['geography_status'] = 'valid_house_district'
    senate = frame.office == 'senate'
    frame.loc[senate, 'district'] = pd.NA
    frame.loc[senate, 'geography_status'] = 'senate_not_applicable'
    missing = (frame.office == 'house') & frame.district.isna()
    frame.loc[missing, 'geography_status'] = 'missing_house_district'
    invalid = (frame.office == 'house') & frame.district.notna() & ~(frame.district.eq(0) | frame.district.between(1, 53))
    frame.loc[invalid, 'district'] = pd.NA
    frame.loc[invalid, 'geography_status'] = 'invalid_house_district'
    return frame


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--crosswalk', required=True)
    args = ap.parse_args()
    src, out = Path(args.input), Path(args.out)

    roster = pd.read_parquet(src / 'exports/candidate_roster.parquet')
    fec_zip = sorted((src / 'fec').glob('cn26_*.zip'))[-1]
    fec = pd.read_csv(zipfile.ZipFile(fec_zip).open('cn.txt'), sep='|', header=None, names=FEC_COLUMNS, dtype=str)
    fec = fec.drop_duplicates('cand_id').set_index('cand_id')
    ballot = pd.read_csv(out / 'ballot_2026_major_party_match.csv')
    senate_states = set(ballot[ballot.office == 'senate'].state)

    base = pd.DataFrame(dict(
        cand_id=roster.candidate_id, candidate=roster.candidate, fec_raw_name=roster.fec_raw_name,
        state=roster.state, source_district=roster.district, office=roster.office, party=roster.party,
        cand_status=roster.cand_status.replace('', pd.NA), website_url=roster.website_url.replace('', pd.NA),
        url_source=roster.url_source.replace('', pd.NA), url_lookup_status=roster.url_lookup_status,
        universe_source='same_year_fec', historical_attempt_record=True))
    base['cand_ici'] = base.cand_id.map(fec.cand_ici)
    n_roster = len(base)
    outside = (base.office == 'senate') & ~base.state.isin(senate_states)
    base = base[~outside]

    extra = []
    for cand_id, (party, district, reason) in BALLOT_OVERRIDES.items():
        assert cand_id not in set(base.cand_id), cand_id
        r = fec.loc[cand_id]
        extra.append(dict(
            cand_id=cand_id, candidate=clean_name(r.cand_name), fec_raw_name=r.cand_name, state=r.cand_office_st,
            source_district=r.cand_office_district if district is None else f'{district:02d}',
            office={'H': 'house', 'S': 'senate'}[r.cand_office], party=party, cand_status=r.cand_status,
            website_url=pd.NA, url_source=pd.NA, url_lookup_status='not_attempted', universe_source='ballot_override',
            historical_attempt_record=False, cand_ici=r.cand_ici, override_reason=reason))
    u = pd.concat([base, pd.DataFrame(extra)], ignore_index=True)
    u['year'] = 2026
    u['cand_election_yr'] = '2026'
    u['source_cand_id'] = u.cand_id
    u['candidate_cycle_id'] = u.cand_id + '-2026'
    assert u.candidate_cycle_id.is_unique
    u = geography(u)

    on_ballot = set(ballot.candidate_id.dropna()) | set(BALLOT_OVERRIDES)
    u['on_ballot'] = u.cand_id.isin(on_ballot)
    assert on_ballot <= set(u.cand_id), sorted(on_ballot - set(u.cand_id))[:10]
    u['general_votes'] = pd.NA

    nonblank = u.website_url.notna()
    u['url_discovery_status'] = 'search_found_no_url'
    u.loc[u.url_lookup_status == 'source_error', 'url_discovery_status'] = 'search_failed'
    u.loc[u.url_lookup_status == 'not_attempted', 'url_discovery_status'] = 'not_searched'
    u.loc[nonblank, 'url_discovery_status'] = 'url_recorded'
    u['normalized_website_url'] = u.website_url.map(lambda v: normalize_website_url(v) if pd.notna(v) else None)
    u['has_url'] = u.normalized_website_url.notna()
    u['url_validation_status'] = 'not_evaluated_no_url_record'
    u.loc[nonblank, 'url_validation_status'] = 'invalid_or_placeholder'
    u.loc[u.has_url, 'url_validation_status'] = 'valid'
    u['website_domain'] = [normalize_website_domain(v) if ok else None for v, ok in zip(u.website_url, u.has_url)]
    u['website_cycle_id'] = [website_cycle_id(v, 2026) if ok else None for v, ok in zip(u.website_url, u.has_url)]

    corpus_path = out / 'raw_corpus_2026.parquet'
    captured = set(pq.read_table(corpus_path, columns=['cand_id']).column('cand_id').to_pylist())
    assert captured <= set(u.cand_id) | set(roster.candidate_id[outside.reindex(roster.index, fill_value=False)])
    u['captured'] = u.cand_id.isin(captured)
    u['capture_status'] = u.captured.map({True: 'captured', False: 'not_captured'})
    u = u.sort_values(['office', 'year', 'state', 'district', 'candidate_cycle_id'], na_position='last')
    u[UNIVERSE_COLUMNS].to_csv(out / 'candidate_year_universe_2026.csv', index=False)
    pd.DataFrame(extra)[['cand_id', 'fec_raw_name', 'state', 'office', 'party', 'override_reason']].to_csv(
        out / 'ballot_override_2026.csv', index=False)

    # Crosswalk: one row for each captured candidate. DIME ends in 2022, so only the
    # person identifier of an earlier year is carried; year-specific DIME fields stay missing.
    old = pd.read_csv(args.crosswalk, dtype=str)
    rid = (old.dropna(subset=['bonica_rid']).sort_values('year').drop_duplicates('cand_id', keep='last')
           .set_index('cand_id').bonica_rid)
    x = u[u.captured].copy()
    x['district_i'] = x.district
    x['bonica_rid'] = x.cand_id.map(rid)
    for column in ('ICPSR', 'NID', 'cfscore', 'seat', 'lname', 'fname', 'cfscore_dyn', 'dwnom1', 'dwnom2',
                   'dime_district', 'dime_ico'):
        x[column] = pd.NA
    x['candidate_icpsr'] = [icpsr_name(r, c) for r, c in zip(x.fec_raw_name, x.candidate)]
    x['name_key'] = [name_key(r, c) for r, c in zip(x.fec_raw_name, x.candidate)]
    x['party_full'] = x.party.map({'D': 'democrat', 'R': 'republican'})
    x['stage'] = x.on_ballot.map({True: 2, False: pd.NA}).astype('Int64')
    x['data_source'] = x.on_ballot.map(DATA_SOURCE)
    x['district_id'] = [district_id(s, d, o) for s, d, o in zip(x.state, x.district, x.office)]
    x['candidate_year_stage'] = [f'{n}_2026_2' if b else '' for n, b in zip(x.candidate_icpsr, x.on_ballot)]
    x[CROSSWALK_COLUMNS].to_csv(out / 'candidate_crosswalk_2026.csv', index=False)

    # Fill the ballot fields of the corpus rows. Rows of Senate candidates outside the population are removed.
    fields = x.set_index('cand_id')[['on_ballot', 'stage', 'data_source', 'candidate_year_stage']]
    source = pq.ParquetFile(corpus_path)
    temporary = out / 'raw_corpus_2026.tmp.parquet'
    writer = pq.ParquetWriter(temporary, source.schema_arrow, compression='zstd')
    kept, removed = 0, 0
    for batch in source.iter_batches(batch_size=5000):
        frame = batch.to_pandas()
        inside = frame.cand_id.isin(fields.index)
        removed += int((~inside).sum())
        frame = frame[inside].copy()
        frame['on_ballot'] = frame.cand_id.map(fields.on_ballot).astype(bool)
        frame['stage'] = frame.cand_id.map(fields.stage).astype('Int64')
        frame['data_source'] = frame.cand_id.map(fields.data_source)
        frame['candidate_year_stage'] = frame.cand_id.map(fields.candidate_year_stage)
        writer.write_table(pa.Table.from_pandas(frame, schema=source.schema_arrow, preserve_index=False))
        kept += len(frame)
    writer.close()
    temporary.replace(corpus_path)

    summary = dict(
        fec_file=fec_zip.name, roster_rows=n_roster, senate_rows_outside_election_states=int(outside.sum()),
        ballot_override_rows=len(extra), universe_rows=int(len(u)),
        by_office=u.office.value_counts().to_dict(), by_party=u.party.value_counts().to_dict(),
        geography_status=u.geography_status.value_counts().to_dict(),
        on_ballot=int(u.on_ballot.sum()), has_url=int(u.has_url.sum()), captured=int(u.captured.sum()),
        on_ballot_with_url=int((u.on_ballot & u.has_url).sum()), on_ballot_captured=int((u.on_ballot & u.captured).sum()),
        url_validation_status=u.url_validation_status.value_counts().to_dict(),
        crosswalk_rows=int(len(x)), crosswalk_with_bonica_rid=int(x.bonica_rid.notna().sum()),
        corpus_rows=kept, corpus_rows_removed_outside_population=removed)
    (out / 'universe_2026_summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
