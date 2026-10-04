"""Offline checks for preserved acquisition evidence and recovery."""
import gzip
import json
from pathlib import Path
from unittest.mock import Mock, MagicMock

import pytest

from src.collection_2026 import (
    Store, Client, parse_cdx, clean_url, identity_match, inventory,
    acquire_page, crawl_snapshot, collect_live, available_captures,
)


@pytest.fixture
def config():
    return dict(user_agent='test',max_run_seconds=60,minimum_free_bytes=0,
                request_attempts=1,live_delay_seconds=0,wayback_delay_seconds=0,
                connect_timeout_seconds=1,read_timeout_seconds=1,
                max_response_bytes=10000,max_pages_per_snapshot=3,max_depth=2,
                render_fallback=False)


@pytest.fixture
def candidate():
    return dict(candidate_id='H00000001',candidate='Jane Example',fec_raw_name='EXAMPLE, JANE',
                website_url='https://example.org/',state='NY',office='house',party='D',year=2026)


@pytest.fixture
def event():
    return dict(event_id='test-primary',date='2026-06-02',timezones=['America/New_York'])


def reply(body, url, **extra):
    return dict(status='ok',body=body,url=url,http_status=200,
                headers={'Content-Type':'text/html'},fetched_at='2026-09-14T12:00:00+00:00',**extra)


def test_store_preserves_attempts_and_recovers(tmp_path):
    store=Store(tmp_path)
    store.add_work('w','live_snapshot','c',{})
    store.start('w')
    aid=store.attempt('w','https://example.org')
    store.recover()
    assert store.rows('work')[0]['status']=='retryable'
    assert store.rows('attempts')[0]['status']=='interrupted'
    aid2=store.attempt('w','https://example.org')
    assert aid!=aid2
    store.finish_attempt(aid2,'ok',{'http_status':200})
    assert len(store.rows('attempts'))==2


def test_blob_dedup_preserves_observations(tmp_path):
    store=Store(tmp_path)
    path,digest=store.blob(b'<p>same</p>')
    assert store.blob(b'<p>same</p>')==(path,digest)
    assert gzip.decompress((tmp_path/path).read_bytes())==b'<p>same</p>'
    store.page({'page_id':'one','captured_at':'2026-01-01','sha256':digest})
    store.page({'page_id':'two','captured_at':'2026-01-02','sha256':digest})
    assert len(store.rows('pages'))==2


@pytest.mark.parametrize('mime',['image/png','application/javascript','text/css','application/pdf','font/woff2'])
def test_page_fetch_rejects_assets_before_reading_or_saving(tmp_path,config,monkeypatch,mime):
    monkeypatch.setattr('src.collection_2026.public_url',lambda url:True)
    store=Store(tmp_path)
    store.add_work('w','live_snapshot','c',{})
    client=Client(store,config)
    response=MagicMock()
    response.__enter__.return_value=response
    response.is_redirect=False
    response.status_code=200
    response.headers={'Content-Type':mime}
    client.session.get=Mock(return_value=response)
    result=client.get('https://example.org/opaque','w',page=True)
    assert result['status']=='non_html'
    response.iter_content.assert_not_called()
    assert not list((tmp_path/'html').rglob('*.gz'))
    assert store.rows('attempts')[0]['detail']['error']=='excluded_non_html_payload'


def test_inventory_json_is_preserved_but_json_page_payload_is_not(tmp_path,config,monkeypatch):
    monkeypatch.setattr('src.collection_2026.public_url',lambda url:True)
    store=Store(tmp_path)
    store.add_work('w','cdx_inventory','c',{})
    client=Client(store,config)
    response=MagicMock()
    response.__enter__.return_value=response
    response.is_redirect=False
    response.status_code=200
    response.headers={'Content-Type':'application/json'}
    response.iter_content.return_value=[b'[]']
    client.session.get=Mock(return_value=response)
    result=client.get('https://web.archive.org/cdx/search/cdx','w',archive=True)
    assert result['status']=='ok'
    assert gzip.decompress((tmp_path/result['body_path']).read_bytes())==b'[]'


def test_cdx_parser_requires_valid_header_and_keeps_resume():
    body=json.dumps([['timestamp','original','statuscode','mimetype'],
                     ['20260601120000','https://example.org','200','text/html']]).encode()+b'\n\nresume-key\n'
    rows,key=parse_cdx(body)
    assert key=='resume-key'
    assert len(rows)==1
    with pytest.raises(ValueError):
        parse_cdx(b'<html>error</html>')
    with pytest.raises(ValueError):
        parse_cdx(b'[["timestamp"],["2026"]]')
    assert parse_cdx(b'[]')==([], '')


def test_cdx_parser_accepts_json_embedded_continuation():
    body=json.dumps([['timestamp','original','statuscode','mimetype'],
                     ['20260601120000','https://example.org','200','text/html'],
                     [],['next-page-key']]).encode()
    rows,key=parse_cdx(body)
    assert len(rows)==1
    assert key=='next-page-key'


def test_inventory_error_never_empty(tmp_path,config,candidate):
    store=Store(tmp_path)
    client=Client(store,config)
    client.get=Mock(return_value={'status':'request_error','body':b''})
    key=inventory(store,client,candidate,candidate['website_url'],'20260914')
    assert store.rows('work')[0]['status']=='retryable'
    assert store.rows('captures')==[]
    client.get=Mock(return_value={'status':'ok','body':b'[]'})
    assert inventory(store,client,candidate,candidate['website_url'],'20260914')==key
    assert store.rows('work')[0]['status']=='empty'


def test_inventory_retry_resumes_after_completed_cdx_page(tmp_path,config,candidate):
    store=Store(tmp_path)
    client=Client(store,config)
    first=json.dumps([['timestamp','original','statuscode','mimetype'],
        ['20260601120000',candidate['website_url'],'200','text/html'],[],['second-page']]).encode()
    client.get=Mock(side_effect=[{'status':'ok','body':first},{'status':'request_error','body':b''}])
    key=inventory(store,client,candidate,candidate['website_url'],'20260914')
    assert store.rows('work')[0]['result']['resume_key']=='second-page'
    client.get=Mock(return_value={'status':'ok','body':b'[]'})
    assert inventory(store,client,candidate,candidate['website_url'],'20260914')==key
    assert 'resumeKey=second-page' in client.get.call_args.args[0]
    assert len(store.rows('captures'))==1
    assert store.rows('work')[0]['status']=='complete'
    assert store.rows('work')[0]['result']['capture_rows']==1


def test_inventory_pagination_preserves_full_rows(tmp_path,config,candidate):
    store=Store(tmp_path)
    client=Client(store,config)
    header=['timestamp','original','statuscode','mimetype']
    page=lambda stamp: json.dumps([header,[stamp,'https://example.org/','200','text/html']]).encode()
    client.get=Mock(side_effect=[{'status':'ok','body':page('20260601120000')+b'\n\nnext'},
                                {'status':'ok','body':page('20260603120000')}])
    inventory(store,client,candidate,candidate['website_url'],'20260914')
    assert len(store.rows('captures'))==2
    assert store.rows('work')[0]['status']=='complete'
    assert 'matchType=prefix' in client.get.call_args_list[0].args[0]
    from urllib.parse import parse_qs,urlsplit
    assert '*' not in parse_qs(urlsplit(client.get.call_args_list[0].args[0]).query)['url'][0]


def test_effective_timestamp_controls_side(tmp_path,config,candidate,event):
    store=Store(tmp_path)
    client=Client(store,config)
    requested='https://web.archive.org/web/20260601120000id_/https://example.org/'
    effective='https://web.archive.org/web/20260603120000id_/https://example.org/'
    client.get=Mock(return_value=reply(b'<html><p>Jane Example campaign</p></html>',effective))
    row,_=acquire_page(store,client,candidate,'s',requested,'homepage',0,'wayback','w',event,'pre')
    assert row['status']=='timestamp_mismatch'
    assert row['captured_at'].startswith('2026-06-03')
    assert row['requested_timestamp']=='20260601120000'


def test_unrelated_identity_not_usable(tmp_path,config,candidate):
    store=Store(tmp_path)
    client=Client(store,config)
    client.get=Mock(return_value=reply(b'<p>Other person website</p>',candidate['website_url']))
    row,_=acquire_page(store,client,candidate,'s',candidate['website_url'],'homepage',0,'live','w')
    assert row['status']=='identity_unresolved'
    assert row['text_path']


def test_crawl_limits_are_recorded(tmp_path,config,candidate):
    store=Store(tmp_path)
    client=Client(store,dict(config,max_pages_per_snapshot=1))
    html=b'<p>Jane Example</p><a href="/issues">Issues</a><a href="/about">About</a>'
    client.get=Mock(return_value=reply(html,candidate['website_url']))
    result=crawl_snapshot(store,client,candidate,candidate['website_url'],'w','live')
    manifest=json.loads((tmp_path/'snapshots'/f"{result['snapshot_id']}.json").read_text())
    assert len(manifest['omissions'])==2
    assert all(r['reason']=='page_limit' for r in manifest['omissions'])
    assert store.rows('pages')[0]['snapshot_complete'] is True


def test_missing_url_keeps_work_row(tmp_path,config,candidate):
    store=Store(tmp_path)
    client=Client(store,config)
    collect_live(store,client,dict(candidate,website_url=''),'2026-09-14')
    assert store.rows('work')[0]['result']['reason']=='missing_url'
    assert store.rows('work')[0]['status']=='blocked'


def test_url_and_identity_checks(candidate):
    assert not clean_url('https://user:pass@example.org')
    assert not clean_url('file:///tmp/secret')
    assert not clean_url('https://example.org:9090')
    assert identity_match(candidate,'The campaign of Jane Example')
    assert not identity_match(candidate,'Jane Different')


def test_completed_inventory_reused_within_week(tmp_path,config,candidate):
    store=Store(tmp_path)
    client=Client(store,config)
    client.get=Mock(return_value={'status':'ok','body':b'[]'})
    first=inventory(store,client,candidate,candidate['website_url'],'20260914')
    assert inventory(store,client,candidate,candidate['website_url'],'20260915')==first
    assert client.get.call_count==1
    assert inventory(store,client,candidate,candidate['website_url'],'20260921')!=first
    assert client.get.call_count==2


def test_successful_wider_inventory_supersedes_old_failure(tmp_path,config,candidate):
    store=Store(tmp_path)
    client=Client(store,config)
    client.get=Mock(return_value={'status':'request_error','body':b''})
    old=inventory(store,client,candidate,candidate['website_url'],'20260914')
    client.get=Mock(return_value={'status':'ok','body':b'[]'})
    new=inventory(store,client,candidate,candidate['website_url'],'20260922')
    statuses={w['key']:w for w in store.rows('work')}
    assert statuses[old]['status']=='superseded'
    assert statuses[old]['result']['superseded_by']==new


def test_section_links_keep_article_paths_and_query_page_roles():
    from src.collection_2026 import page_links
    links=page_links(b'<a href="/?page_id=23">Issues</a><a href="/2026/update">Read more</a><a href="/contact">Contact</a>',
                     'https://example.org/news/','live',2,parent_type='news')
    assert ('https://example.org/?page_id=23','issues') in links
    assert ('https://example.org/2026/update','news') in links
    assert not any('/contact' in url for url,_ in links)


def test_social_and_payment_subdomains_are_excluded():
    assert not clean_url('https://www.facebook.com/campaign')
    assert not clean_url('https://secure.actblue.com/donate/example')
    assert clean_url('https://example.org/issues')


def test_memento_header_controls_actual_capture_date(tmp_path,config,candidate,event):
    store=Store(tmp_path); client=Client(store,config)
    url='https://web.archive.org/web/20260601120000id_/https://example.org/'
    value=reply(b'<p>Jane Example</p>',url)
    value['headers']={'content-type':'text/html','memento-datetime':'Wed, 03 Jun 2026 12:00:00 GMT'}
    client.get=Mock(return_value=value)
    row,_=acquire_page(store,client,candidate,'s',url,'homepage',0,'wayback','w',event,'pre')
    assert row['status']=='timestamp_mismatch'
    assert row['captured_at'].startswith('2026-06-03')
    assert row['effective_url_timestamp']=='20260601120000'
    assert row['timestamp_source']=='memento_header'
