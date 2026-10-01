"""Attach source-period reviews without changing website acceptance or claims."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

FIELDS=('source_period_ai_reviewed','source_period_ai_decision','source_period_ai_reason',
        'source_period_ai_ownership_status','source_period_archive_validation',
        'source_period_review_model','source_period_review_sha256')


def attach(rows,packet,review_hash):
    by_id={r['candidate_url_id']:r for r in rows}
    if len(by_id)!=len(rows):raise ValueError('Duplicate candidate URL IDs')
    if any(set(FIELDS)&set(r) for r in rows):raise ValueError('Review columns already exist')
    reviews=packet['reviews_by_candidate_url_id']
    if set(reviews)-set(by_id):raise ValueError('Review IDs absent from main table')
    if len(reviews)!=packet['selection']['selected_rows']:raise ValueError('Review count differs')
    for key,review in reviews.items():
        row=by_id[key]
        if (review['row_key']['candidate_year_id']!=row['candidate_year_id'] or
                review['raw_url']!=row['raw_url'] or review['candidate_url_id']!=key):
            raise ValueError('Review identity differs')
        if not set(review['source_claim_ids'])<=set(json.loads(row['source_expansion_claim_ids_json'])):
            raise ValueError('Review claim bindings differ')
        if review['overall_judgment'] not in {'supported_discovery_lead','retain_pending_period_evidence'}:
            raise ValueError('Unexpected review decision')
    output=[]
    for row in rows:
        extra=dict.fromkeys(FIELDS,'')
        extra['source_period_ai_reviewed']='False'
        if row['candidate_url_id'] in reviews:
            review=reviews[row['candidate_url_id']]
            extra.update(source_period_ai_reviewed='True',
                source_period_ai_decision=review['overall_judgment'],
                source_period_ai_reason=review['office_year_period_support']['reason'],
                source_period_ai_ownership_status=review['source_ownership']['status'],
                source_period_archive_validation=review['archive_validation']['status'],
                source_period_review_model=packet['requested_review_model'],
                source_period_review_sha256=review_hash)
        output.append(dict(row,**extra))
    return output


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('main','review','output'):p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args()
    sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
    packet=json.loads(a.review.read_text())
    if packet['inputs'].get(str(a.main))!=sha(a.main):raise ValueError('Main input hash differs')
    with a.main.open(newline='') as f:original=list(csv.DictReader(f))
    output=attach(original,packet,sha(a.review))
    if a.output.exists():raise ValueError('Refusing to replace an output')
    a.output.parent.mkdir(parents=True,exist_ok=True)
    with a.output.open('x',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(output[0]));w.writeheader();w.writerows(output)
    with a.output.open(newline='') as f:verified=list(csv.DictReader(f))
    assert len(verified)==len(original)
    assert all(all(new[k]==v for k,v in old.items()) for old,new in zip(original,verified))
    audit=dict(rows=len(output),reviewed_rows=sum(r['source_period_ai_reviewed']=='True' for r in output),
        supported_leads=sum(r['source_period_ai_decision']=='supported_discovery_lead' for r in output),
        pending_period_evidence=sum(r['source_period_ai_decision']=='retain_pending_period_evidence' for r in output),
        original_fields_preserved=True,website_acceptance_changes=0,
        ai_accepted_rows=sum(r.get('ai_accepted_website','').lower()=='true' for r in output),
        inputs={str(path):sha(path) for path in (a.main,a.review)},output_sha256=sha(a.output),
        script_sha256=sha(Path(__file__)))
    a.output.with_suffix('.audit.json').write_text(json.dumps(audit,indent=2)+'\n')
    print(json.dumps(audit,indent=2))


if __name__=='__main__':main()
