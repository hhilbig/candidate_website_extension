"""Prepare the remaining exact-URL all-timestamp inventory without requests."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

from scripts.inventory_mayoral_accepted_urls import query_key, LIMITS


def rows(path):
    with path.open(newline='') as f:
        return list(csv.DictReader(f))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(audit, completed, accepted):
    accepted = [r for r in accepted if r.get('ai_accepted_website','').lower()=='true']
    by_id={r['candidate_url_id']:r for r in accepted}
    if len(by_id)!=len(accepted):
        raise ValueError('Duplicate accepted URL IDs')
    if {r['candidate_url_id'] for r in audit}!=set(by_id) or len(audit)!=len(by_id):
        raise ValueError('Audit and accepted URL universe differ')
    done={r['candidate_url_id'] for r in completed}
    if len(done)!=len(completed) or not done<=set(by_id):
        raise ValueError('Completed inventory IDs differ')
    output=[]
    for r in audit:
        current=by_id[r['candidate_url_id']]
        for field in ('candidate_year_id','exact_normalized_url'):
            if current[field]!=r[field]:
                raise ValueError('Current target differs from audit')
        if r['candidate_url_id'] in done:
            continue
        windows=json.loads(r['inventory_windows_json'])
        if len(windows)!=1 or len(windows[0])!=2:
            raise ValueError('A remaining target needs an explicit window decision')
        start,end=windows[0]
        row=dict(candidate_url_id=r['candidate_url_id'],candidate_year_id=r['candidate_year_id'],
            full_name=r['full_name'],exact_normalized_url=r['exact_normalized_url'],
            proposed_window_start=start,proposed_window_end=end,
            window_rule='preserve_existing_inventory_window_without_extension',
            action='all_timestamp_exact_url_inventory_no_digest_or_status_filter',
            proposal_status='awaiting_metadata_batch_approval')
        row['query_key']=query_key(row)
        if start>end:
            raise ValueError('Reversed window')
        output.append(row)
    return sorted(output,key=lambda r:r['query_key'])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('audit','completed','accepted','output'):
        p.add_argument('--'+name,type=Path,required=True)
    args=p.parse_args()
    if args.output.exists():
        raise ValueError('Refusing to replace proposal')
    output=prepare(rows(args.audit),rows(args.completed),rows(args.accepted))
    args.output.mkdir(parents=True)
    queue=args.output/'targeted_inventory_queue.csv'
    with queue.open('x',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(output[0]))
        writer.writeheader();writer.writerows(output)
    manifest=dict(status='proposal_only_not_launched',query_count=len(output),
        candidate_years=len({r['candidate_year_id'] for r in output}),
        limits=LIMITS,scope='Exact accepted URLs and all timestamps within existing windows. No new paths or expanded windows.',
        byte_estimate='Unknown until queried. The 32 MiB metadata limit and 300-request limit may stop the batch before completion.',
        existing_inventory_limitation='Older prefix inventories collapsed repeated content digests; they do not enumerate every timestamp.',
        approval='Requires explicit metadata-batch approval and fresh remote preflight.',
        inputs={str(path):sha(path) for path in (args.audit,args.completed,args.accepted)},
        queue_sha256=sha(queue),script_sha256=sha(Path(__file__)),archive_requests=0)
    (args.output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps(manifest,indent=2))


if __name__=='__main__':
    main()
