"""Attach capture reviews to corrected discovery rows, preserving acceptance."""
import argparse
import csv
import hashlib
import json
from pathlib import Path


def index(rows):
    result = {r['candidate_url_id']: r for r in rows}
    if len(result) != len(rows) or '' in result:
        raise ValueError('Duplicate or empty key')
    return result


def attach(main, references, captures):
    kept, excluded, reviews = index(main), index(references), index(captures)
    if kept.keys() & excluded.keys() or reviews.keys() - (kept.keys() | excluded.keys()):
        raise ValueError('Review keys outside discovery/reference partition')
    fields = sorted({k for r in captures for k in r if k.startswith('ai_capture_')})
    evidence_fields = {'ai_capture_evidence_path':'evidence_path',
                       'ai_capture_evidence_sha256':'evidence_sha256',
                       'ai_capture_timestamp':'effective_timestamp',
                       'ai_capture_effective_url':'effective_original_url'}
    if any(set(r) & (set(fields)|set(evidence_fields)) for r in main):
        raise ValueError('Refusing to overwrite capture fields')
    for key, review in reviews.items():
        source = kept.get(key, excluded.get(key))
        if (review['candidate_year_id'] != source['candidate_year_id']
                or review['exact_normalized_url'] != source['exact_normalized_url']
                or review['ai_capture_reviewed'] != 'True'):
            raise ValueError('Review identity or completion mismatch')
        if review['ai_capture_review_decision']=='reject_candidate_attribution' and key in kept:
            raise ValueError('Unapplied attribution rejection')
    result = []
    for row in main:
        review = reviews.get(row['candidate_url_id'])
        extra = dict.fromkeys([*fields,*evidence_fields], '')
        extra['ai_capture_reviewed'] = 'False'
        if review:
            extra.update({k:review[k] for k in fields})
            extra.update({k:review[v] for k,v in evidence_fields.items()})
        result.append({**row, **extra})
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['main','references','captures','capture-summary','output']:
        p.add_argument('--'+name,type=Path,required=True)
    args=p.parse_args()
    summary=json.loads(args.capture_summary.read_text())
    digest=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
    if summary['rows_output_sha256']!=digest(args.captures) or summary['remaining_rows']!=0:
        raise ValueError('Incomplete or changed capture table')
    def read(path):
        with path.open() as f:return list(csv.DictReader(f))
    source=read(args.main)
    result=attach(source,read(args.references),read(args.captures))
    with args.output.open('x',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(result[0]));w.writeheader();w.writerows(result)
    audit=dict(rows=len(result),capture_reviewed_rows=sum(r['ai_capture_reviewed']=='True' for r in result),
               original_fields_preserved=all(all(r[k]==v for k,v in old.items()) for r,old in zip(result,source)),
               input_hashes={str(p):digest(p) for p in [args.main,args.references,args.captures,args.capture_summary]},
               output_sha256=digest(args.output),script_sha256=digest(Path(__file__)),acceptance_changes=0)
    with args.output.with_suffix('.audit.json').open('x') as f:json.dump(audit,f,indent=2);f.write('\n')
    print(json.dumps(audit,indent=2))


if __name__=='__main__':main()
