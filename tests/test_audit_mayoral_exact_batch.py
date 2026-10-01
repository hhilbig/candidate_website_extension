import csv
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

import scripts.audit_mayoral_capture_completeness as audit


def fixture(tmp_path, count=1):
    root = tmp_path/'exact_capture_20260914'
    frozen, run = root/'inputs', root/'run'
    frozen.mkdir(parents=True); (run/'raw').mkdir(parents=True); (run/'text').mkdir()
    queue = []
    captures = []
    results = []
    for i in range(count):
        row = {'candidate_year_id':f'c{i}__2020','timestamp':'20200101000000',
               'original_url':f'https://example.org/{i}'}
        row['proposed_capture_key'] = audit._exact_capture_key(row)
        queue.append(row)
        raw, text = f'<p>text {i}</p>'.encode(), f'text {i}'.encode()
        raw_rel, text_rel = f'raw/{i}.gz', f'text/{i}.txt'
        (run/raw_rel).write_bytes(gzip.compress(raw)); (run/text_rel).write_bytes(text)
        result = {'requested_timestamp':row['timestamp'],'requested_original_url':row['original_url'],
                  'effective_timestamp':row['timestamp'],'effective_original_url':row['original_url'],
                  'effective_replay_url':f"https://web.archive.org/web/{row['timestamp']}id_/{row['original_url']}",
                  'redirects':[{'raw_path':raw_rel,'raw_sha256':hashlib.sha256(raw).hexdigest(),
                                'decoded_bytes':len(raw),'oversized':False,'status_code':200}],
                  'text_path':text_rel,'text_sha256':hashlib.sha256(text).hexdigest(),'text_bytes':len(text)}
        results.append(result)
        captures.append({'candidate_year_id':row['candidate_year_id'],'timestamp':row['timestamp'],
                         'original_url':row['original_url'],'inventory_sources':['old'],
                         'queue_sources':[], 'fetches':[]})
    queue_path = frozen/'exact_capture_queue.csv'
    with queue_path.open('w',newline='') as handle:
        writer=csv.DictWriter(handle,fieldnames=list(queue[0])); writer.writeheader(); writer.writerows(queue)
    (frozen/'manifest.json').write_text(json.dumps({'output_sha256':audit.sha(queue_path),'selected_capture_keys':count}))
    db=sqlite3.connect(run/'ledger.sqlite3')
    db.execute('CREATE TABLE current(key TEXT PRIMARY KEY,status TEXT,result TEXT)')
    db.execute('CREATE TABLE contract(id INTEGER PRIMARY KEY,value TEXT)')
    db.execute('CREATE TABLE attempts(id TEXT,key TEXT,started REAL,finished REAL,status TEXT,result TEXT)')
    db.executemany('INSERT INTO current VALUES (?,?,?)',[(r['proposed_capture_key'],'fetched',json.dumps(v)) for r,v in zip(queue,results)])
    db.execute('INSERT INTO contract VALUES (1,?)',(json.dumps({'queue_sha256':audit.sha(queue_path)}),))
    db.executemany('INSERT INTO attempts VALUES (?,?,?,?,?,?)',[(str(i),r['proposed_capture_key'],1,2,'fetched',json.dumps(v)) for i,(r,v) in enumerate(zip(queue,results))])
    db.commit(); db.close()
    remote={'captures':captures,'hashes':{},'raw_files_checked':0,'raw_files_verified':0}
    return root,remote,queue,results


def test_success_removes_retry_condition_and_preserves_prior_fetch(tmp_path):
    root,remote,queue,_=fixture(tmp_path)
    remote['captures'][0]['fetches'].append({'fetch_status':'request_error','raw_verified':False})
    merged=audit.merge_exact_batch(remote,root)
    capture=merged['captures'][0]
    assert [f['fetch_status'] for f in capture['fetches']] == ['request_error','fetched']
    assert capture['fetches'][-1]['raw_verified']
    assert capture['queue_sources'] == ['exact_capture_20260914']
    assert merged['raw_files_checked'] == merged['raw_files_verified'] == 1
    assert str(root/'run'/'ledger.sqlite3') in merged['hashes']


@pytest.mark.parametrize('target', ['raw','text'])
def test_tampered_evidence_fails_without_mutation(tmp_path,target):
    root,remote,_,results=fixture(tmp_path,2)
    relative = results[1]['redirects'][0]['raw_path'] if target == 'raw' else results[1]['text_path']
    path=root/'run'/relative
    path.write_bytes(b'tampered')
    before=json.dumps(remote,sort_keys=True)
    with pytest.raises(ValueError,match='evidence'):
        audit.merge_exact_batch(remote,root)
    assert json.dumps(remote,sort_keys=True) == before


@pytest.mark.parametrize('field,value',[
    ('effective_timestamp','20200202000000'),('effective_original_url','https://other.org/')])
def test_effective_identity_mismatch_fails(tmp_path,field,value):
    root,remote,queue,results=fixture(tmp_path)
    results[0][field]=value
    db=sqlite3.connect(root/'run'/'ledger.sqlite3')
    db.execute('UPDATE current SET result=?',(json.dumps(results[0]),)); db.commit(); db.close()
    with pytest.raises(ValueError,match='identity mismatch'):
        audit.merge_exact_batch(remote,root)


@pytest.mark.parametrize('change',['missing','stale','duplicate_queue'])
def test_key_agreement_required(tmp_path,change):
    root,remote,queue,_=fixture(tmp_path)
    db=sqlite3.connect(root/'run'/'ledger.sqlite3')
    if change=='missing': db.execute('DELETE FROM current')
    elif change=='stale': db.execute("INSERT INTO current VALUES ('stale','fetched','{}')")
    else:
        queue_path=root/'inputs'/'exact_capture_queue.csv'
        with queue_path.open('a') as handle: handle.write(queue_path.read_text().splitlines()[1]+'\n')
        manifest=root/'inputs'/'manifest.json'
        data=json.loads(manifest.read_text()); data['output_sha256']=audit.sha(queue_path); data['selected_capture_keys']=2
        manifest.write_text(json.dumps(data))
    db.commit(); db.close()
    with pytest.raises(ValueError): audit.merge_exact_batch(remote,root)


def test_unmatched_upstream_key_fails(tmp_path):
    root,remote,_,_=fixture(tmp_path)
    remote['captures'].clear()
    with pytest.raises(ValueError,match='no upstream'):
        audit.merge_exact_batch(remote,root)


def test_duplicate_merge_fails(tmp_path):
    root,remote,_,_=fixture(tmp_path)
    audit.merge_exact_batch(remote,root)
    with pytest.raises(ValueError,match='already merged'):
        audit.merge_exact_batch(remote,root)


@pytest.mark.parametrize('change',['http_status','empty_text','replay_url','contract'])
def test_completed_evidence_contract_required(tmp_path,change):
    root,remote,queue,results=fixture(tmp_path)
    db=sqlite3.connect(root/'run'/'ledger.sqlite3')
    if change == 'http_status': results[0]['redirects'][-1]['status_code']=404
    elif change == 'empty_text':
        text=root/'run'/results[0]['text_path']; text.write_bytes(b'')
        results[0]['text_bytes']=0; results[0]['text_sha256']=hashlib.sha256(b'').hexdigest()
    elif change == 'replay_url': results[0]['effective_replay_url']=results[0]['effective_replay_url'].replace('example.org','other.org')
    else:
        db.execute("UPDATE contract SET value='{}'")
    if change != 'contract':
        db.execute('UPDATE current SET result=?',(json.dumps(results[0]),))
    db.commit(); db.close()
    with pytest.raises(ValueError): audit.merge_exact_batch(remote,root)
