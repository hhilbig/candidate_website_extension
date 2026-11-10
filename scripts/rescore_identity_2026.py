"""Score candidate identity again on the stored 2026 homepage text.

The collector confirms a homepage only if the text has the FEC family name and
an FEC given-name word. Sites that use a nickname fail that check. This script
reads the final exports and the stored text. It does not change the collection.
It writes one row for each candidate with an unresolved homepage, and a summary.

Evidence classes for one homepage text (the first class that applies):
  given_variant  family name in text, plus a short form of a given name (the collector's nickname rule)
  host_surname   family name in text, and family name in the website host
  surname_only   family name in text, no other evidence
  no_surname     family name not in text

Usage (on the droplet):
  python rescore_identity_2026.py --data /path/to/data --out /path/to/outdir
  python rescore_identity_2026.py --data /path/to/data --out /path/to/outdir --apply

Without --apply the script only reads. With --apply it uses the collector's
identity_match on each stored homepage with status identity_unresolved. A
homepage that passes becomes usable and confirmed in collection.sqlite, with
identity_rule set. Back up collection.sqlite first. Run `collect_2026.py export`
after it. Subpages do not change: the collector did not follow links from an
unresolved homepage.
"""
import argparse
import gzip
import json
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.collection_2026 import identity_match, short_form_match  # noqa: E402

SUFFIXES = {'jr', 'sr', 'ii', 'iii', 'iv'}
TITLES = {'mr', 'mrs', 'ms', 'dr', 'hon', 'rev'}

def words(value):
    value = unicodedata.normalize('NFKD', str(value)).encode('ascii', 'ignore').decode().lower()
    return set(re.findall(r'[a-z]+', value))


def name_parts(raw, display):
    """Return (surname_words, given_words) with the collector's rule."""
    if ',' in raw:
        surname, given = raw.split(',', 1)
        return words(surname) - SUFFIXES, words(given) - SUFFIXES - TITLES
    tokens = re.findall(r'[A-Za-z]+', display)
    return (words(tokens[-1]) if tokens else set()), (words(tokens[0]) if len(tokens) > 1 else set())


def classify(text, surname_words, given_words, host):
    observed = words(text)
    if not surname_words or not surname_words <= observed:
        return 'no_surname'
    if given_words & observed:
        return 'collector_rule'  # the present rule accepts this text
    if short_form_match(surname_words, given_words, text):
        return 'given_variant'
    if ''.join(sorted(surname_words, key=len, reverse=True)[:1]) in re.sub(r'[^a-z]', '', host.lower()):
        return 'host_surname'
    return 'surname_only'


def apply(data, out, roster):
    import sqlite3
    db = sqlite3.connect(data / 'collection.sqlite')
    info = roster.set_index('candidate_id')
    rows = db.execute("SELECT page_id,data FROM pages WHERE json_extract(data,'$.status')='identity_unresolved' "
                      "AND json_extract(data,'$.depth')=0").fetchall()
    verdicts, changed = {}, []
    for page_id, raw in rows:
        page = json.loads(raw)
        cid = page['candidate_id']
        if cid not in info.index or not page.get('text_path'):
            continue
        key = (cid, page['text_path'])
        if key not in verdicts:
            text = gzip.open(data / page['text_path'], 'rt', encoding='utf-8', errors='replace').read()
            r = info.loc[cid]
            verdicts[key] = identity_match(dict(fec_raw_name=r.get('fec_raw_name', '') or '',
                                                candidate=r.get('candidate', '') or ''), text)
        if verdicts[key]:
            page.update(status='usable', identity_status='confirmed', identity_rule='short_form_rescore_20261109')
            changed.append((json.dumps(page), page_id, cid))
    with db:
        db.executemany('UPDATE pages SET data=? WHERE page_id=?', [(d, p) for d, p, _ in changed])
    report = dict(unresolved_homepage_rows=len(rows), rows_changed=len(changed),
                  candidates_changed=len({c for _, _, c in changed}))
    (out / 'identity_rescore_applied.json').write_text(json.dumps(report, indent=2))
    pd.DataFrame([(p, c) for _, p, c in changed], columns=['page_id', 'candidate_id']).to_csv(
        out / 'identity_rescore_applied_pages.csv', index=False)
    print(json.dumps(report, indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    data, out = Path(args.data), Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    roster = pd.read_parquet(data / 'exports/candidate_roster.parquet').drop_duplicates('candidate_id')
    if args.apply:
        apply(data, out, roster)
        return
    pages = pd.read_parquet(data / 'exports/page_corpus.parquet')
    pairs = pd.read_parquet(data / 'exports/candidate_event_pairs.parquet')
    home = pages[pages.depth.astype(str) == '0']
    unresolved = home[home.status == 'identity_unresolved']
    confirmed_ids = set(home[home.status == 'usable'].candidate_id)
    info = roster.set_index('candidate_id')

    cache, rows = {}, []
    for cid, group in unresolved.groupby('candidate_id'):
        r = info.loc[cid]
        raw = r.get('fec_raw_name', '') or ''
        surname_words, given_words = name_parts(raw, r.get('candidate', '') or '')
        counts, example = Counter(), {}
        for _, page in group.iterrows():
            key = (cid, page.text_path)
            if key not in cache:
                try:
                    text = gzip.open(data / page.text_path, 'rt', encoding='utf-8', errors='replace').read()
                except OSError:
                    text = None
                host = urlsplit(page.effective_url or page.original_url).netloc
                cache[key] = ('text_missing' if text is None else classify(text, surname_words, given_words, host),
                              '' if text is None else re.sub(r'\s+', ' ', text)[:160])
            cls, snippet = cache[key]
            counts[cls] += 1
            example.setdefault(cls, snippet)
        n = sum(counts.values())
        strong = counts['given_variant'] + counts['host_surname']
        if counts['given_variant'] / n >= 0.5:
            verdict = 'given_variant'
        elif strong / n >= 0.5:
            verdict = 'host_surname'
        elif (strong + counts['surname_only']) / n >= 0.5:
            verdict = 'surname_only'
        else:
            verdict = 'no_surname'
        rows.append(dict(candidate_id=cid, candidate=r.get('candidate', ''), fec_raw_name=raw,
                         office=r.get('office', ''), state=r.get('state', ''), website_url=r.get('website_url', ''),
                         had_confirmed_homepage=cid in confirmed_ids, n_unresolved_homepages=n,
                         verdict=verdict, **{f'n_{k}': counts[k] for k in
                                             ('given_variant', 'host_surname', 'surname_only', 'no_surname',
                                              'collector_rule', 'text_missing')},
                         example_text=example.get(verdict, next(iter(example.values()), ''))))
    result = pd.DataFrame(rows)
    result.to_csv(out / 'identity_rescore_candidates.csv', index=False)

    # Upper bound on new pairs: a candidate-event row can become paired only if
    # each side is available now or is missing because of the identity check.
    def fixable(side):
        return (pairs[f'{side}_available'].astype(str) == 'True') | (pairs[f'{side}_missing_reason'] == 'identity_unresolved')
    blocked = pairs[(pairs.coverage_status != 'paired') & fixable('pre') & fixable('post')]
    general = pairs.event_date.astype(str).str[:10] == '2026-11-03'
    summary = dict(candidates_with_unresolved_homepage=int(len(result)),
                   never_confirmed=int((~result.had_confirmed_homepage).sum()),
                   verdict_counts=result.verdict.value_counts().to_dict(),
                   verdict_counts_never_confirmed=result[~result.had_confirmed_homepage].verdict.value_counts().to_dict(),
                   unresolved_homepage_rows_by_class={k: int(result[f'n_{k}'].sum()) for k in
                                                      ('given_variant', 'host_surname', 'surname_only', 'no_surname',
                                                       'collector_rule', 'text_missing')},
                   pair_rows_blocked_only_by_identity=int(len(blocked)),
                   general_pair_rows_blocked_only_by_identity=int((general.loc[blocked.index]).sum()))
    for label, verdicts in (('given_variant', {'given_variant'}),
                            ('given_variant_or_host_surname', {'given_variant', 'host_surname'})):
        ids = set(result[result.verdict.isin(verdicts)].candidate_id)
        hit = blocked[blocked.candidate_id.isin(ids)]
        summary[f'max_new_pairs_{label}'] = int(len(hit))
        summary[f'max_new_general_pairs_{label}'] = int(general.loc[hit.index].sum())
    (out / 'identity_rescore_summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
