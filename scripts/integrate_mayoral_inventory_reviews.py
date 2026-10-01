"""Attach verified capture decisions to every index row without deleting evidence."""
import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path


def integrate(inventory,packet):
    by_id={r['row_id']:r for r in inventory}
    if len(by_id)!=len(inventory):raise ValueError('Duplicate index row IDs')
    reviewed={}
    for raw_hash,review in packet['reviews_by_raw_sha256'].items():
        if review['raw_sha256']!=raw_hash:raise ValueError('Review hash key differs')
        if review['decision'] not in {'support','caveat','reject_capture','uncertain'}:
            raise ValueError('Unknown review decision')
        bindings={r['row_id']:r for r in review.get('capture_bindings',[])}
        if len(bindings)!=len(review.get('capture_bindings',[])) or set(bindings)!=set(review['row_ids']):
            raise ValueError('Review lacks unique per-row capture bindings')
        for row_id in review['row_ids']:
            if row_id in reviewed or row_id not in by_id:raise ValueError('Duplicate or absent review row ID')
            row=by_id[row_id]
            binding=bindings[row_id]
            if binding.get('date_validity_decision') not in {'support','caveat','reject_capture','uncertain'}:
                raise ValueError('Missing or invalid per-capture date decision')
            if (row['candidate_url_id']!=binding['candidate_url_id'] or
                row['candidate_year_id']!=binding['candidate_year_id'] or
                row['timestamp']!=binding['capture_timestamp'] or row['original']!=binding['capture_url']):
                raise ValueError('Review capture identity differs')
            if not any(e['raw_verified'] and e['raw_sha256']==raw_hash for e in row['exact_evidence']):
                raise ValueError('Review is not bound to verified exact evidence')
            reviewed[row_id]=dict(review,date_validity_decision=binding['date_validity_decision'])
    expected={r['row_id'] for r in inventory if r['saved_exact_response']}
    if set(reviewed)!=expected:raise ValueError('Review row coverage differs')
    output=[]
    for row in inventory:
        review=reviewed.get(row['row_id'])
        extra=dict(ai_capture_decision='not_reviewed',ai_capture_date_decision='not_reviewed',ai_capture_reason='',raw_sha256='',
            text_sha256='',usable_static_text=False,ai_supported_mayoral_text=False)
        if review:
            extra.update(ai_capture_decision=review['decision'],ai_capture_reason=review['reason'],
                ai_capture_date_decision=review['date_validity_decision'],
                raw_sha256=review['raw_sha256'],text_sha256=review['text_sha256'],
                usable_static_text=review['usable_static_text'],
                ai_supported_mayoral_text=review['decision']=='support' and review['date_validity_decision']=='support' and review['usable_static_text'])
        output.append(dict({k:row[k] for k in ('row_id','query_key','candidate_url_id','candidate_year_id',
            'timestamp','original','mimetype','statuscode','duplicate_of_row_id','saved_exact_response')},**extra))
    return output


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('inventory','reviews','output'):p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args();sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
    packet=json.loads(a.reviews.read_text())
    if packet['input_source_sha256']!=sha(a.inventory):raise ValueError('Source hash differs')
    result=integrate(json.loads(a.inventory.read_text()),packet)
    with a.output.open('x',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(result[0]));w.writeheader();w.writerows(result)
    audit=dict(inventory_rows=len(result),reviewed_rows=sum(r['ai_capture_decision']!='not_reviewed' for r in result),
        decisions_by_inventory_row=dict(collections.Counter(r['ai_capture_decision'] for r in result)),
        ai_supported_mayoral_text_rows=sum(r['ai_supported_mayoral_text'] for r in result),
        unique_ai_supported_capture_keys=len({(r['candidate_year_id'],r['timestamp'],r['original']) for r in result if r['ai_supported_mayoral_text']}),
        rows_deleted=0,website_acceptance_changes=0,inputs={str(path):sha(path) for path in (a.inventory,a.reviews)},
        output_sha256=sha(a.output),script_sha256=sha(Path(__file__)),
        definition='AI-supported text requires content and per-capture date support plus nonempty static text. Caveats require further review; wrong-office captures cannot contribute. This is not human approval or archive completeness.')
    a.output.with_suffix('.audit.json').write_text(json.dumps(audit,indent=2)+'\n')
    print(json.dumps(audit,indent=2))


if __name__=='__main__':main()
