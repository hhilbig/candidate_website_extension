import csv
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

import scripts.fetch_mayoral_exact_capture_batch as runner

CLOCK = [1000000.0]


class Response:
    encoding = 'utf-8'

    def __init__(self, status=200, body=b'<p>saved text</p>', headers=None, crash=False):
        self.status_code = status
        self.body = body
        self.headers = headers or {'Content-Type': 'text/html'}
        self.crash = crash
        self.closed = False

    def iter_content(self, chunk_size):
        for offset in range(0,len(self.body),chunk_size):
            yield self.body[offset:offset+chunk_size]
        if self.crash:
            raise KeyboardInterrupt()

    def close(self):
        self.closed = True


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def get(self, url, **kwargs):
        assert kwargs['allow_redirects'] is False
        assert kwargs['stream'] is True
        self.urls.append(url)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture
def setup(tmp_path, monkeypatch):
    CLOCK[0] = 1000000.0
    monkeypatch.setattr(runner.time, 'time', lambda: CLOCK[0])
    monkeypatch.setattr(runner.shutil, 'disk_usage', lambda _: type('Usage', (), {'free': 10**10})())
    def create(count=1):
        rows = []
        for i in range(count):
            row = dict(candidate_year_id=f'{i}__2020', timestamp='20200101000000',
                       original_url=f'https://example.org/{i}', url_relation='exact', reason='not_queued')
            row['requested_replay_url'] = f"https://web.archive.org/web/{row['timestamp']}id_/{row['original_url']}"
            row['proposed_capture_key'] = runner.capture_key(row)
            rows.append(row)
        queue = tmp_path/'queue.csv'
        with queue.open('w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        manifest = tmp_path/'manifest.json'
        manifest.write_text(json.dumps(dict(output_sha256=hashlib.sha256(queue.read_bytes()).hexdigest(),
                                           proposed_limits=runner.LIMITS, selected_capture_keys=count)))
        return queue, manifest, tmp_path/'output', rows
    return create


def run(args, session, count=None):
    def advance(seconds):
        CLOCK[0] += seconds
    return runner.run_batch(*args[:3], max_new_attempts=count, session=session, sleep=advance)


def test_success_and_resume(setup):
    args = setup()
    result = run(args, Session([Response()]))
    assert result['statuses'] == {'fetched': 1}
    assert result['http_requests'] == result['attempts'] == 1
    assert result['acquisition_complete']
    assert run(args, Session([]))['attempts'] == 1
    evidence = json.loads(result['current'][0]['result'])
    assert (args[2]/evidence['text_path']).read_text() == 'saved text'
    import gzip
    raw = gzip.decompress((args[2]/evidence['redirects'][0]['raw_path']).read_bytes())
    assert hashlib.sha256(raw).hexdigest() == evidence['redirects'][0]['raw_sha256']
    db = sqlite3.connect(args[2]/'ledger.sqlite3')
    contract = json.loads(db.execute('SELECT value FROM contract').fetchone()[0])
    db.close()
    assert 'contract_version' not in contract


def test_external_redirect_never_requested(setup):
    args = setup()
    session = Session([Response(302, b'live redirect', {'Location':'https://example.org/'})])
    result = run(args, session)
    assert result['statuses'] == {'redirect_blocked': 1}
    assert len(session.urls) == 1
    assert result['decoded_bytes'] == len(b'live redirect')


def test_mismatch_saved_unresolved_and_redirect_bytes_count(setup):
    args = setup()
    redirect = args[3][0]['requested_replay_url'].replace('20200101000000', '20200201000000')
    result = run(args, Session([Response(302, b'abc', {'Location':redirect}), Response()]))
    assert result['statuses'] == {'capture_mismatch': 1}
    assert result['decoded_bytes'] == 3 + len(b'<p>saved text</p>')
    evidence = json.loads(result['current'][0]['result'])
    assert evidence['effective_timestamp'] == '20200201000000'
    assert (args[2]/evidence['text_path']).exists()


def test_crash_reservation_survives_restart(setup):
    args = setup()
    with pytest.raises(KeyboardInterrupt):
        run(args, Session([Response(crash=True)]))
    result = run(args, Session([]), 0)
    assert result['charged_decoded_bytes'] == runner.LIMITS['max_response_decoded_bytes']
    assert result['statuses'] == {'retryable': 1}
    db = sqlite3.connect(args[2]/'ledger.sqlite3')
    assert db.execute('SELECT status FROM attempts').fetchone()[0] == 'interrupted'
    db.close()


def test_oversize_and_total_budget(setup, monkeypatch):
    monkeypatch.setitem(runner.LIMITS, 'max_response_decoded_bytes', 8)
    monkeypatch.setitem(runner.LIMITS, 'max_tranche_decoded_bytes', 8)
    args = setup(2)
    session = Session([Response(body=b'x'*9)])
    result = run(args, session)
    assert result['charged_decoded_bytes'] == 8
    assert result['decoded_bytes'] == 8
    assert result['stop_reason'] == 'decoded_budget'
    assert len(session.urls) == 1


def test_fair_retries_and_three_attempt_cap(setup):
    args = setup(2)
    result = run(args, Session([Response(503), Response()]), 2)
    assert result['attempts'] == 2
    assert result['statuses'] == {'retryable':1, 'fetched':1}
    for _ in range(2):
        db = sqlite3.connect(args[2]/'ledger.sqlite3')
        db.execute('UPDATE current SET next_due=0')
        db.commit()
        db.close()
        result = run(args, Session([Response(503)]), 1)
    assert result['attempts'] == 4
    assert run(args, Session([]))['attempts'] == 4


def test_frozen_hash_and_resume_contract(setup):
    args = setup()
    run(args, Session([]), 0)
    args[0].write_text(args[0].read_text()+'\n')
    with pytest.raises(ValueError, match='hash'):
        run(args, Session([]), 0)


def test_unexpected_keys_rejected(setup):
    args = setup()
    run(args, Session([]), 0)
    db = sqlite3.connect(args[2]/'ledger.sqlite3')
    db.execute("INSERT INTO current(key,status) VALUES ('stale','fetched')")
    db.commit()
    db.close()
    with pytest.raises(ValueError, match='keys'):
        run(args, Session([]), 0)


def test_archive_redirect_limit_and_loop(setup):
    args = setup()
    result = run(args, Session([Response(302, headers={'Location':args[3][0]['requested_replay_url']})]))
    assert result['statuses'] == {'redirect_limit':1}


def test_redirect_chain_bounded_and_original_mismatch(setup):
    args = setup()
    start = args[3][0]['requested_replay_url']
    redirects = [Response(302, b'abc', {'Location':start.replace('/0',f'/path{i}')}) for i in range(6)]
    session = Session(redirects)
    result = run(args,session)
    assert result['http_requests'] == 6
    assert result['statuses'] == {'redirect_limit':1}
    assert result['decoded_bytes'] == 18


def test_crash_charge_can_block_next_request(setup, monkeypatch):
    monkeypatch.setitem(runner.LIMITS,'max_tranche_decoded_bytes',runner.LIMITS['max_response_decoded_bytes'])
    args = setup(2)
    with pytest.raises(KeyboardInterrupt):
        run(args,Session([Response(crash=True)]))
    session = Session([])
    result = run(args,session)
    assert not session.urls
    assert result['stop_reason'] == 'decoded_budget'


def test_persistent_circuit_stops_before_next_request(setup):
    args = setup(6)
    result = run(args,Session([Response(503) for _ in range(5)]))
    assert result['attempts'] == 5
    assert result['stop_reason'] == 'circuit_open'
    assert run(args,Session([]))['attempts'] == 5


def test_flock_excludes_writer(setup):
    import fcntl
    args = setup()
    args[2].mkdir()
    with (args[2]/'writer.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            run(args,Session([]),0)


def test_short_stream_chunks_never_consume_beyond_ceiling(setup,monkeypatch):
    monkeypatch.setitem(runner.LIMITS,'max_response_decoded_bytes',10)
    args = setup()
    class VariableResponse(Response):
        consumed = 0
        def iter_content(self,chunk_size):
            for chunk in (b'123456',b'abcdef'):
                self.consumed += len(chunk)
                yield chunk
    response = VariableResponse()
    result = run(args,Session([response]))
    assert response.consumed == result['decoded_bytes'] == 6
    assert result['charged_decoded_bytes'] == 10
    assert result['statuses'] == {'response_too_large':1}


def test_exact_ceiling_conservatively_unresolved(setup,monkeypatch):
    monkeypatch.setitem(runner.LIMITS,'max_response_decoded_bytes',8)
    args = setup()
    result = run(args,Session([Response(body=b'12345678')]))
    assert result['statuses'] == {'response_too_large':1}
    assert result['decoded_bytes'] <= 8


def test_changed_original_saved_but_unresolved(setup):
    args = setup()
    redirect = args[3][0]['requested_replay_url'].replace('example.org','other.org')
    result = run(args,Session([Response(302,b'abc',{'Location':redirect}),Response()]))
    assert result['statuses'] == {'capture_mismatch':1}
    evidence = json.loads(result['current'][0]['result'])
    assert evidence['effective_original_url'] == 'https://other.org/0'


def test_volume_headroom_no_attempt(setup, monkeypatch):
    args = setup()
    monkeypatch.setattr(runner.shutil,'disk_usage',lambda _: type('Usage', (), {'free': 7*10**9})())
    result = run(args, Session([]))
    assert result['attempts'] == result['http_requests'] == 0
    assert result['stop_reason'] == 'storage_headroom'


def indexed_setup(tmp_path, relation='index_returned_url_variant'):
    row = dict(candidate_year_id='1__2020', timestamp='20200101000000',
               original_url='http://example.org:80/', provider='web.archive.org',
               candidate_url_id='candidate_url_1', inventory_requested_url='https://www.example.org/',
               url_relation=relation, reason='no_verified_exact_response_in_inspected_stores',
               selection_basis='propose_html_capture', proposal_status='awaiting_approval_not_executable',
               inventory_row_ids_json='[1]')
    row['requested_replay_url'] = f"https://web.archive.org/web/{row['timestamp']}id_/{row['original_url']}"
    row['proposed_capture_key'] = runner.capture_key(row)
    queue = tmp_path/'indexed.csv'
    with queue.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    manifest = tmp_path/'approved.json'
    manifest.write_text(json.dumps({
        'contract_version': runner.INDEXED_CONTRACT_VERSION,
        'approved_queue_sha256': hashlib.sha256(queue.read_bytes()).hexdigest(),
        'proposal_manifest_sha256': 'a'*64,
        'selected_capture_keys': 1,
        'approved_limits': runner.LIMITS,
        'approved_url_relations': ['exact', 'index_returned_url_variant'],
        'preserve_original_proposal_bytes': True,
        'approval': {'status':'approved', 'approved_by':'user',
                     'approved_batches': {'exact':0, 'index_returned_url_variant':1}},
    }))
    monkey_pair = (hashlib.sha256(queue.read_bytes()).hexdigest(), 'a'*64)
    runner.APPROVED_INDEXED_QUEUES.add(monkey_pair)
    return queue, manifest, tmp_path/'indexed-output', row


def test_indexed_contract_accepts_only_frozen_approved_variants(tmp_path):
    queue, manifest, output, row = indexed_setup(tmp_path)
    rows, digest, contract = runner.load_queue(queue, manifest)
    assert rows == [row]
    assert contract['contract_version'] == runner.INDEXED_CONTRACT_VERSION
    assert contract['approval_manifest_sha256'] == hashlib.sha256(manifest.read_bytes()).hexdigest()
    result = runner.run_batch(queue, manifest, output, max_new_attempts=0, session=Session([]))
    db = sqlite3.connect(output/'ledger.sqlite3')
    saved = json.loads(db.execute('SELECT value FROM contract').fetchone()[0])
    db.close()
    assert saved['contract_version'] == runner.INDEXED_CONTRACT_VERSION
    assert saved['approval_manifest_sha256'] == contract['approval_manifest_sha256']
    assert result['expected_keys'] == 1


def test_indexed_contract_rejects_unapproved_or_non_equivalent_variant(tmp_path):
    queue, manifest, _, _ = indexed_setup(tmp_path)
    approval = json.loads(manifest.read_text())
    approval['approval']['status'] = 'proposal'
    manifest.write_text(json.dumps(approval))
    with pytest.raises(ValueError, match='explicitly approved'):
        runner.load_queue(queue, manifest)

    queue, manifest, _, _ = indexed_setup(tmp_path)
    rows = list(csv.DictReader(queue.open()))
    rows[0]['inventory_requested_url'] = 'https://different.example/path'
    with queue.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    approval = json.loads(manifest.read_text())
    approval['approved_queue_sha256'] = hashlib.sha256(queue.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(approval))
    runner.APPROVED_INDEXED_QUEUES.add((approval['approved_queue_sha256'],
                                        approval['proposal_manifest_sha256']))
    with pytest.raises(ValueError, match='differs beyond'):
        runner.load_queue(queue, manifest)


def test_indexed_success_requires_requested_replay_identity(tmp_path, monkeypatch):
    CLOCK[0] = 1000000.0
    monkeypatch.setattr(runner.time, 'time', lambda: CLOCK[0])
    monkeypatch.setattr(runner.shutil, 'disk_usage', lambda _: type('Usage', (), {'free': 10**10})())
    queue, manifest, output, row = indexed_setup(tmp_path)
    result = run((queue, manifest, output), Session([Response()]))
    assert result['statuses'] == {'fetched': 1}
    evidence = json.loads(result['current'][0]['result'])
    assert evidence['effective_timestamp'] == row['timestamp']
    assert evidence['effective_original_url'] == row['original_url']


@pytest.mark.parametrize(('relation', 'original', 'accepted', 'message'), [
    ('exact', 'http://example.org/', 'https://example.org/', 'exact URL relation'),
    ('unknown', 'http://example.org/', 'http://example.org/', 'invalid URL relation'),
    ('index_returned_url_variant', 'http://example.org:443/', 'https://example.org/',
     'non-default port'),
])
def test_indexed_contract_rejects_wrong_relation_labels_and_ports(
        tmp_path, relation, original, accepted, message):
    queue, manifest, _, _ = indexed_setup(tmp_path)
    rows = list(csv.DictReader(queue.open()))
    rows[0].update(url_relation=relation, original_url=original,
                   inventory_requested_url=accepted)
    rows[0]['requested_replay_url'] = (
        f"https://web.archive.org/web/{rows[0]['timestamp']}id_/{original}")
    rows[0]['proposed_capture_key'] = runner.capture_key(rows[0])
    with queue.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    approval = json.loads(manifest.read_text())
    approval['approved_queue_sha256'] = hashlib.sha256(queue.read_bytes()).hexdigest()
    approval['approval']['approved_batches'] = {
        'exact': int(relation == 'exact'),
        'index_returned_url_variant': int(relation == 'index_returned_url_variant'),
    }
    manifest.write_text(json.dumps(approval))
    runner.APPROVED_INDEXED_QUEUES.add((approval['approved_queue_sha256'],
                                        approval['proposal_manifest_sha256']))
    with pytest.raises(ValueError, match=message):
        runner.load_queue(queue, manifest)


def test_indexed_resume_rejects_changed_approval_manifest(tmp_path):
    queue, manifest, output, _ = indexed_setup(tmp_path)
    runner.run_batch(queue, manifest, output, max_new_attempts=0, session=Session([]))
    approval = json.loads(manifest.read_text())
    approval['success_contract'] = 'changed after ledger creation'
    manifest.write_text(json.dumps(approval))
    with pytest.raises(ValueError, match='resume contract differs'):
        runner.run_batch(queue, manifest, output, max_new_attempts=0, session=Session([]))
