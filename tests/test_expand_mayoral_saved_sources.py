import json
from pathlib import Path

import pytest

from scripts import expand_mayoral_saved_sources as m


def seed(db, name="Jane Doe", url="https://news.example/guide", cid="c1"):
    candidate = {"candidate_year_id": cid, "full_name": name, "contest": "race1", "geo_name": "Boston", "state_abb": "MA", "year": "2017", "month": "11"}
    db.execute("INSERT OR REPLACE INTO candidates VALUES(?,?)", (cid, json.dumps(candidate)))
    source_id = m.add_source(db, url=url, candidate_id=cid, origin="test", input_path="fixture", input_row=1)
    db.commit()
    return dict(db.execute("SELECT * FROM sources WHERE source_id=?", (source_id,)).fetchone()), candidate


def test_source_transport_keeps_archive_wrapper_path_case_and_query():
    u = "https://web.archive.org/web/20170401000000/https://VoteJane.com/MyPage?a=B#top"
    assert m.source_url(u) == u.split("#")[0]
    assert m.source_url("HTTPS://NEWS.Example/Profile?Candidate=Jane") == "https://news.example/Profile?Candidate=Jane"


@pytest.mark.parametrize("url", ["http://localhost/test", "http://127.0.0.1/", "http://169.254.169.254/latest/", "file:///etc/passwd", "https://user:secret@example.org", "https://example.org:8080/x"])
def test_reject_non_public_and_credential_urls(url):
    assert m.disposition(m.source_url(url)) != "pending"


def test_dns_resolution_cannot_access_private_network():
    def resolver(*args, **kwargs):
        return [(2, 1, 6, "", ("192.168.1.1", 443))]
    with pytest.raises(ValueError, match="non-public address"):
        m.assert_public("https://public-name.example/", resolver)


def test_one_source_preserves_independent_claim_contexts(tmp_path):
    db = m.connect(tmp_path)
    source, _ = seed(db)
    m.add_source(db, url=source["url"], candidate_id="c2", origin="independent", input_path="other", input_row=2)
    assert db.execute("SELECT COUNT(*) FROM sources").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM contexts").fetchone()[0] == 2


def test_started_attempt_survives_restart_and_consumes_retry_count(tmp_path):
    db = m.connect(tmp_path)
    source, _ = seed(db)
    first = m.begin_attempt(db, source["source_id"])
    db.close()
    db = m.connect(tmp_path)
    assert db.execute("SELECT attempt_id FROM attempts").fetchone()[0] == first
    assert len(m.pending_sources(db, 2)) == 1
    m.begin_attempt(db, source["source_id"])
    assert m.pending_sources(db, 2) == []


def test_cached_pdf_or_html_reparsed_without_network(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    saved = root / "saved.html"
    saved.write_bytes(b'<p>Jane Doe campaign website <a href="https://BetterBoston.org/Issues">website</a></p>')
    source = {"source_id": "s1", "url": "https://news.example/guide", "cache_path": "saved.html", "cache_content_type": "text/html", "cache_sha256": m.sha(saved)}
    class NoNetwork:
        def get(self, *args, **kwargs):
            raise AssertionError("network request from saved source")
    result, parsed = m.fetch_document(source, root, tmp_path / "run", session=NoNetwork())
    assert result["used_cache"]
    assert result["request_count"] == 0
    assert result["status"] == "fetched"
    assert any(x["exact_normalized_url"] == "https://betterboston.org/Issues" and x["raw_url"] == "https://BetterBoston.org/Issues" for x in parsed["links"])
    assert (tmp_path / "run" / result["raw_path"]).is_file()


def test_terminal_success_not_downgraded_by_error(tmp_path):
    db = m.connect(tmp_path)
    source, candidate = seed(db)
    roster, contests = {"c1": candidate}, {"race1": ["c1"]}
    result = {"ended_at": m.now(), "status": "fetched", "retryable": False, "effective_url": source["url"], "raw_sha256": "hash", "raw_path": "raw/x", "parsed_path": "parsed/x"}
    a = m.begin_attempt(db, source["source_id"])
    m.complete_attempt(db, a, source, result, {"text": "", "links": []}, roster, contests, 2)
    b = m.begin_attempt(db, source["source_id"])
    m.complete_attempt(db, b, source, {**result, "status": "request_error", "retryable": True}, None, roster, contests, 2)
    assert db.execute("SELECT status FROM current").fetchone()[0] == "fetched"
    assert db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 2


def test_all_contest_candidates_receive_document_context(tmp_path):
    db = m.connect(tmp_path)
    source, candidate = seed(db)
    other = {**candidate, "candidate_year_id": "c2", "full_name": "John Roe"}
    selected = m.document_candidates(db, source["source_id"], {"c1": candidate, "c2": other}, {"race1": ["c1", "c2"]})
    assert {r["candidate_year_id"] for r in selected} == {"c1", "c2"}


def test_ownership_repair_preserves_acquisition_and_claim_keys(tmp_path):
    from src.mayoral_source_expansion import parse_document
    db = m.connect(tmp_path)
    source, candidate = seed(db)
    parsed = parse_document(b'<p>Web site: <a href="https://newsdaily.org/jane-doe-wins">Jane Doe wins</a>. 2017.</p>', "text/html", source["url"])
    parsed_path, _ = m.save_parsed(tmp_path, parsed)
    result = {"ended_at": m.now(), "status": "fetched", "retryable": False, "effective_url": source["url"], "raw_sha256": "hash", "raw_path": "raw/x", "parsed_path": parsed_path}
    attempt = m.begin_attempt(db, source["source_id"])
    m.complete_attempt(db, attempt, source, result, parsed, {"c1": candidate}, {"race1": ["c1"]}, 2)
    row = db.execute("SELECT claim_id,payload FROM claims").fetchone()
    payload = json.loads(row[1])
    payload.update(attribution_status="identity_supported", url_type="custom_domain")
    payload.pop("candidate_site_ownership_supported")
    db.execute("UPDATE claims SET payload=?", (json.dumps(payload),))
    tables = ["sources", "contexts", "attempts", "current", "deferred_links"]
    before = {name: [tuple(r) for r in db.execute("SELECT * FROM " + name)] for name in tables}
    audit = m.repair_saved_ownership(db, tmp_path)
    assert audit["claims_updated"] == 1 and audit["unmatched_claims"] == 0
    assert db.execute("SELECT claim_id FROM claims").fetchone()[0] == row[0]
    assert db.execute("SELECT attribution_status FROM claims").fetchone()[0] == "reference_lead"
    assert before == {name: [tuple(r) for r in db.execute("SELECT * FROM " + name)] for name in tables}


def test_ownership_repair_cannot_promote_an_unresolved_identity(tmp_path):
    from src.mayoral_source_expansion import parse_document
    db = m.connect(tmp_path)
    source, candidate = seed(db)
    parsed = parse_document(b'<p>Jane Doe <a href="https://janedoe.com">Website</a></p>', "text/html", source["url"])
    parsed_path, _ = m.save_parsed(tmp_path, parsed)
    result = {"ended_at": m.now(), "status": "fetched", "retryable": False, "effective_url": source["url"], "raw_sha256": "hash", "raw_path": "raw/x", "parsed_path": parsed_path}
    attempt = m.begin_attempt(db, source["source_id"])
    m.complete_attempt(db, attempt, source, result, parsed, {"c1": candidate}, {"race1": ["c1"]}, 2)
    payload = json.loads(db.execute("SELECT payload FROM claims").fetchone()[0])
    payload["attribution_status"] = "match_unresolved"
    db.execute("UPDATE claims SET attribution_status='match_unresolved',payload=?", (json.dumps(payload),))
    m.repair_saved_ownership(db, tmp_path)
    payload = json.loads(db.execute("SELECT payload FROM claims").fetchone()[0])
    assert payload["attribution_status"] == "match_unresolved"
    assert payload["input_attribution_status"] == "match_unresolved"


def test_export_retains_nonattempted_and_excluded_keys(tmp_path):
    db = m.connect(tmp_path)
    seed(db)
    m.add_source(db, url="https://www.vote411.org/ballot", candidate_id="c1", origin="test", input_path="x", input_row=2)
    db.commit()
    audit = m.export(tmp_path, db)
    rows = m.read_csv(tmp_path / "source_candidate_dispositions.csv")
    assert len(rows) == 2
    assert {r["source_status"] for r in rows} == {"not_attempted", "permission_required"}
    assert audit["exact_source_disposition_key_agreement"]
    assert audit["exact_relation_disposition_key_agreement"]
    assert audit["stage"] == "source_pass_incomplete"


def test_cached_only_runner_keeps_remote_sources_pending(tmp_path):
    db = m.connect(tmp_path / "run")
    source, _ = seed(db)
    cache = tmp_path / "page.html"
    cache.write_text("<p>No campaign link.</p>")
    db.execute("UPDATE sources SET cache_path=?,cache_content_type='text/html' WHERE source_id=?", (str(cache), source["source_id"]))
    m.add_source(db, url="https://other.example/source", candidate_id="c1", origin="test", input_path="x", input_row=3)
    db.commit()
    db.close()
    result = m.run_fetch(tmp_path, tmp_path / "run", workers=1, cache_only=True, min_free_bytes=0, host_delay=0)
    assert result["processed_sources"] == 1
    assert result["never_attempted_sources"] == 1
    assert result["stage"] == "source_pass_incomplete"


def test_cache_uses_saved_final_url_for_relative_links(tmp_path):
    saved = tmp_path / "saved.html"
    saved.write_text('<p>Jane Doe campaign website <a href="Candidate/Issues">website</a></p>')
    source = {"source_id":"s", "url":"https://news.example/old", "cache_path":str(saved),
              "cache_content_type":"text/html", "cache_effective_url":"https://news.example/2017/guide/"}
    result, parsed = m.fetch_document(source, tmp_path, tmp_path/"run")
    assert result["effective_url"] == source["cache_effective_url"]
    assert parsed["links"][0]["exact_normalized_url"] == "https://news.example/2017/guide/Candidate/Issues"


def test_parsed_artifacts_are_immutable_and_content_addressed(tmp_path):
    first = {"text":"old", "links":[], "parser_version":"v1"}
    old_json, old_text = m.save_parsed(tmp_path,first)
    old_bytes = (tmp_path/old_json).read_bytes()
    new_json, new_text = m.save_parsed(tmp_path,{**first,"text":"new","parser_version":"v2"})
    assert old_json != new_json and old_text != new_text
    assert (tmp_path/old_json).read_bytes() == old_bytes
    assert m.save_parsed(tmp_path,first) == (old_json,old_text)


def test_reparse_retires_stale_followups_and_keeps_original_sources(tmp_path):
    from src.mayoral_source_expansion import parse_document
    db = m.connect(tmp_path)
    source, candidate = seed(db)
    roster, contests = {"c1":candidate}, {"race1":["c1"]}
    parsed = parse_document(b'<p>Jane Doe campaign website <a href="https://BetterBoston.org/">website</a></p>',"text/html",source["url"])
    result = {"ended_at":m.now(),"status":"fetched","retryable":False,"effective_url":source["url"],"raw_sha256":"x","raw_path":"raw/x","parsed_path":"parsed/x"}
    m.complete_attempt(db,m.begin_attempt(db,source["source_id"]),source,result,parsed,roster,contests,2)
    assert db.execute("SELECT COUNT(*) FROM contexts WHERE origin='candidate_link_followup'").fetchone()[0] == 1
    m.complete_attempt(db,m.begin_attempt(db,source["source_id"]),source,{**result,"operation":"reparse"},{"text":"","links":[]},roster,contests,2)
    assert db.execute("SELECT COUNT(*) FROM contexts WHERE origin='candidate_link_followup'").fetchone()[0] == 0
    assert db.execute("SELECT disposition FROM sources WHERE url='https://betterboston.org/'").fetchone()[0] == "stale_followup"
    assert db.execute("SELECT COUNT(*) FROM retired_contexts").fetchone()[0] == 1
    assert db.execute("SELECT disposition FROM sources WHERE source_id=?",(source["source_id"],)).fetchone()[0] == "pending"


def test_prior_rejection_and_candidate_flags_cannot_approve_new_claim(tmp_path):
    from src.mayoral_source_expansion import parse_document
    db = m.connect(tmp_path)
    source, candidate = seed(db)
    candidate.update(human_accepted=True,covered=True,sampled_audit_accepted=True,archive_capture_available=True)
    db.execute("INSERT INTO review_decisions VALUES(?,?,?)",("c1","betterboston.org/","human_rejected"))
    parsed = parse_document(b'<p>Jane Doe campaign website <a href="https://BetterBoston.org/">website</a></p>',"text/html",source["url"])
    result = {"ended_at":m.now(),"status":"fetched","retryable":False,"effective_url":source["url"],"raw_sha256":"x","raw_path":"raw/x","parsed_path":"parsed/x"}
    m.complete_attempt(db,m.begin_attempt(db,source["source_id"]),source,result,parsed,{"c1":candidate},{"race1":["c1"]},2)
    claims = [json.loads(r[0]) for r in db.execute("SELECT payload FROM claims")]
    assert claims and all(c["attribution_status"]=="prior_rejection_review" for c in claims)
    for claim in claims:
        assert not any(m.truth(claim.get(f,False)) for f in ("human_accepted","covered","sampled_audit_accepted","archive_capture_available"))
    assert db.execute("SELECT COUNT(*) FROM contexts WHERE origin='candidate_link_followup'").fetchone()[0] == 0


def test_reparse_does_not_consume_network_retry_allowance_or_erase_byte_total(tmp_path):
    db = m.connect(tmp_path)
    source,candidate = seed(db)
    result = {"ended_at":m.now(),"status":"request_error","retryable":True,"effective_url":source["url"],"content_bytes":1200,"request_count":1}
    m.complete_attempt(db,m.begin_attempt(db,source["source_id"]),source,result,None,{"c1":candidate},{"race1":["c1"]},2)
    m.complete_attempt(db,m.begin_attempt(db,source["source_id"]),source,{**result,"operation":"reparse","request_count":0,"used_cache":True},None,{"c1":candidate},{"race1":["c1"]},2)
    assert len(m.pending_sources(db,2)) == 1
    assert m.status(db)["downloaded_raw_bytes"] == 1200


def test_saved_snippet_extracted_when_source_page_unavailable(tmp_path):
    db = m.connect(tmp_path)
    source,candidate = seed(db)
    db.execute("UPDATE contexts SET snippet='Jane Doe campaign website: https://janedoe.org/Issues'")
    db.commit()
    assert m.extract_saved_snippets(db,tmp_path,{"c1":candidate},{"race1":["c1"]},2) == 1
    assert m.extract_saved_snippets(db,tmp_path,{"c1":candidate},{"race1":["c1"]},2) == 0
    assert db.execute("SELECT COUNT(*) FROM claims WHERE exact_url='https://janedoe.org/Issues'").fetchone()[0] == 1
    assert m.status(db)["downloaded_raw_bytes"] == 0


def test_retry_after_seconds_and_http_date():
    from datetime import datetime,timezone,timedelta
    from email.utils import format_datetime
    assert m.retry_seconds("120") == 120
    assert 58 < m.retry_seconds(format_datetime(datetime.now(timezone.utc)+timedelta(seconds=60))) <= 60
    assert m.retry_seconds("invalid") == 0


def test_new_candidate_context_reuses_saved_parse(tmp_path):
    from src.mayoral_source_expansion import parse_document
    db = m.connect(tmp_path)
    source,candidate = seed(db)
    other = {**candidate,"candidate_year_id":"c2","full_name":"John Roe","contest":"other"}
    roster,contests = {"c1":candidate,"c2":other},{"race1":["c1"],"other":["c2"]}
    parsed = parse_document(b'<p>John Roe campaign website: https://johnroe.org</p>',"text/html",source["url"])
    parsed_path,text_path = m.save_parsed(tmp_path,parsed)
    result = {"ended_at":m.now(),"status":"fetched","retryable":False,"effective_url":source["url"],"raw_sha256":"x","raw_path":"raw/x","parsed_path":parsed_path,"text_path":text_path}
    m.complete_attempt(db,m.begin_attempt(db,source["source_id"]),source,result,parsed,roster,contests,0)
    assert db.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
    m.add_source(db,url=source["url"],candidate_id="c2",origin="later_source",input_path="y",input_row=1)
    assert m.refresh_attribution(db,tmp_path,roster,contests,0) == 1
    assert db.execute("SELECT COUNT(*) FROM claims WHERE candidate_year_id='c2'").fetchone()[0] == 1
    assert m.status(db)["downloaded_raw_bytes"] == 0


def test_repeated_empty_202_responses_are_unavailable_and_defer_host(tmp_path):
    db=m.connect(tmp_path)
    roster={}
    for i in range(4):
        source,candidate=seed(db,url=f"https://news.example/page{i}")
        roster["c1"]=candidate
        if i<3:
            result={"source_id":source["source_id"],"ended_at":m.now(),"status":"fetched","retryable":False,"effective_url":source["url"],
                    "http_status":202,"content_bytes":0,"request_count":1}
            m.complete_attempt(db,m.begin_attempt(db,source["source_id"]),source,result,None,roster,{"race1":["c1"]},2)
    m.repair_empty_response_statuses(db,roster,{"race1":["c1"]},2)
    assert db.execute("SELECT COUNT(*) FROM current WHERE status='source_unavailable'").fetchone()[0]==3
    assert db.execute("SELECT disposition FROM sources WHERE url='https://news.example/page3'").fetchone()[0]=='host_deferred_access'
    assert db.execute("SELECT COUNT(*) FROM host_restrictions").fetchone()[0]==1
    assert len(m.pending_sources(db,3))==0
