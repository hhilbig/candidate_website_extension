import io
import json
import zipfile

import pytest

import src.mayoral_source_expansion as module


SOURCE = "https://springfieldnews.com/elections/2017"
CANDIDATES = [
    {"candidate_year_id": "alice__2017", "full_name": "Alice Smith", "contest_id": "city__2017", "year": 2017, "month": 5},
    {"candidate_year_id": "bob__2017", "full_name": "Bob Jones", "contest_id": "city__2017", "year": 2017, "month": 5},
]


def parse(value, mime="text/html", **kwargs):
    return module.parse_document(value.encode() if isinstance(value, str) else value, mime, SOURCE, **kwargs)


def claims(value):
    return module.attribute_links(parse(value), CANDIDATES, SOURCE)


@pytest.mark.parametrize("url,anchor", [
    ("https://dailyjournal.com/news/alice-smith-for-mayor", "Alice Smith for mayor"),
    ("https://dailyjournal.com", "Alice Smith wins election"),
    ("https://cityrecords.com/auditor?id=123", "Filing of candidacy Alice Smith"),
    ("https://awards.org/awards/alice-smith/", "Alice Smith"),
])
def test_bibliographic_website_label_does_not_support_ownership(url, anchor):
    result = claims(f'<p>Web site: <a href="{url}">{anchor}</a>. Daily Journal. 2017.</p>')
    assert result
    assert all(not module.eligible_site_claim(row) for row in result)


def test_login_route_requires_review():
    result = claims('<p><a href="https://alicesmith.nationbuilder.com/forms/user_sessions/new">Alice Smith</a></p>')
    assert result and not module.eligible_site_claim(result[0])
    assert result[0]["candidate_site_ownership_reason"] == "account_route_requires_site_root_review"


def test_named_publisher_website_is_not_candidate_ownership():
    result = claims('<p>Alice Smith, mayor, appeared in a discussion. '
                    '<a href="https://publicvideo.org/video/?123">Public Video website</a> Retrieved May 2019.</p>')
    assert result and all(not module.eligible_site_claim(row) for row in result)
    assert result[0]['candidate_site_ownership_reason'] == 'named_third_party_website_label'


@pytest.mark.parametrize('anchor', ['Website', 'Visit the campaign website', 'Alice Smith website',
                                  "Follow this link to Alice Smith's campaign website.",
                                  'Alice Smith for Mayor of Springfield Campaign Website'])
def test_generic_or_candidate_website_label_remains_supported(anchor):
    result = claims(f'<h2>Alice Smith</h2><p><a href="https://abettercity.org">{anchor}</a></p>')
    assert result and module.eligible_site_claim(result[0])


def test_official_press_release_label_is_reference_evidence():
    result = claims('<p><a href="https://assemblyparty.org/press-releases/20190101-bill">'
                    'Assemblymember Alice Smith signs bill | Official Website</a></p>')
    assert result and not module.eligible_site_claim(result[0])
    assert result[0]['candidate_site_ownership_reason'] == 'government_or_officeholder_website_context'


@pytest.mark.parametrize('content', [
    'County Clerk website, <a href="https://civicrecords.org/person">Alice Smith</a>',
    '<a href="https://civicrecords.org/person">Mayor Alice Smith</a> on City of Springfield website',
    'Website <a href="https://civicrecords.org/person">Supervisor Alice Smith</a>',
])
def test_government_context_is_not_campaign_ownership(content):
    result = claims(f'<p>{content}</p>')
    assert result and not module.eligible_site_claim(result[0])
    assert result[0]['candidate_site_ownership_reason'] == 'government_or_officeholder_website_context'


@pytest.mark.parametrize('office', ['Congress', 'Senate', 'State Representative', 'State Assembly'])
def test_linked_campaign_office_overrides_mayoral_page_context(office):
    result = claims(f'<title>Alice Smith, former mayor</title><p>'
                    f'<a href="https://alicesmith.com">Alice Smith for {office}</a> campaign website</p>')
    assert result and result[0]['wrong_office_flag']
    assert not module.eligible_site_claim(result[0])


def test_unrelated_office_mention_does_not_override_candidate_campaign_link():
    result = claims('<p>Bob Jones ran for Senate.</p><h2>Alice Smith</h2>'
                    '<p><a href="https://alicesmith.com">Alice Smith for Mayor</a></p>')
    alice = next(r for r in result if r['candidate_year_id']=='alice__2017')
    assert module.eligible_site_claim(alice)


def test_explicit_government_website_context_is_reference():
    result = claims('<p><a href="https://legislativeparty.org/alice">Alice Smith</a> official government website</p>')
    assert result and not module.eligible_site_claim(result[0])


def test_website_label_does_not_override_an_unowned_article_target():
    result = claims('<p>Alice Smith <a href="https://localdaily.com/news/alice-smith">Campaign website</a></p>')
    assert result and not module.eligible_site_claim(result[0])


def test_shared_host_article_path_cannot_establish_candidate_ownership():
    assert not module._candidate_shaped_target(CANDIDATES[0], {
        "exact_normalized_url": "https://medium.com/local-news/alice-smith-for-mayor",
        "url_type": "shared_host_path",
    })
    assert module._candidate_shaped_target(CANDIDATES[0], {
        "exact_normalized_url": "https://sites.google.com/site/alicesmithformayor/home",
        "url_type": "shared_host_path",
    })


def test_undated_personal_site_and_council_source_require_review():
    result = claims('<p>Alice Smith website, undated: <a href="https://alicesmith.com">website</a></p>')
    assert result and not module.eligible_site_claim(result[0])
    result = claims('<title>Meet the candidates for city council</title><p>Alice Smith <a href="https://alicesmith.com">Website</a></p>')
    assert result and result[0]["wrong_office_flag"] and not module.eligible_site_claim(result[0])


def test_party_website_and_wrong_office_target_do_not_add_mayoral_sites():
    result = claims('<p>Visit the official Party for Socialism website for Alice Smith and other candidates: <a href="https://partyexample.org">website</a></p>')
    assert result and not module.eligible_site_claim(result[0])
    result = claims('<p><a href="https://alicesmithforgovernor.com">Alice Smith</a></p>')
    assert result and result[0]["wrong_office_flag"] and not module.eligible_site_claim(result[0])


def test_election_title_with_city_preserves_year_conflict():
    assert module._temporal({"year": 2019}, "", "https://elections.org", "2021 Fort Worth mayoral election") == "date_conflict_review"
    assert module._temporal({"year": 2021}, "", "https://elections.org", "2021 Fort Worth mayoral election") == "not_assessed"


@pytest.mark.parametrize("url,anchor", [
    ("https://alicesmith.com", "Alice Smith"),
    ("https://alicesmith.wordpress.com/about", "Alice Smith"),
    ("https://abettercity.org", "Campaign website"),
])
def test_supported_ownership_is_exportable(url, anchor):
    result = claims(f'<h2>Alice Smith</h2><p><a href="{url}">{anchor}</a></p>')
    assert result and module.eligible_site_claim(result[0])


def test_multiple_candidates_slogan_domains_heading_and_no_footer_attribution():
    result = claims("""<html><body><h1>2017 mayoral candidates</h1>
    <h2>Alice Smith</h2><p><a href='https://abettercity.org/Issues?Ref=Mail'>Campaign website</a></p>
    <h2>Bob Jones</h2><p><a href='https://OurFutureTogether.com'>Website</a></p>
    <footer><a href='https://designcompany.com'>Website design</a></footer></body></html>""")
    assert {(r["candidate_year_id"], r["exact_normalized_url"]) for r in result} == {
        ("alice__2017", "https://abettercity.org/Issues?Ref=Mail"),
        ("bob__2017", "https://ourfuturetogether.com"),
    }
    assert all(r["attribution_status"] == "identity_supported" for r in result)
    assert all(r["month"] == 5 and r["contest_id"] == "city__2017" for r in result)


def test_table_and_nearby_candidate_identity_are_not_assigned_to_every_name():
    result = claims("""<table><tr><td>Alice Smith</td><td><a href='https://citytogether.org'>Website</a></td></tr>
    <tr><td>Bob Jones</td><td><a href='https://forwardcity.net'>Website</a></td></tr></table>""")
    assert [(r["candidate_year_id"], r["registrable_domain"]) for r in result] == [
        ("alice__2017", "citytogether.org"), ("bob__2017", "forwardcity.net")]


def test_exact_case_paths_query_wrappers_bare_urls_and_raw_preserved():
    raw = "https://web.archive.org/web/20170502120000/http://VOTEALICE.com/Campaign/About?Ref=Email"
    document = parse(f"<p>Alice Smith campaign website <a href='{raw}'>Website</a>. Also AbetterCity.org/Issues?Ref=Mail.</p>")
    links = {r["exact_normalized_url"]: r for r in document["links"]}
    assert links["http://votealice.com/Campaign/About?Ref=Email"]["raw_url"] == raw
    assert "http://abettercity.org/Issues?Ref=Mail" in links
    assert module._normalized_target("https://wayback.archive-it.org/5903/20170501/http://VoteAlice.com/Path", SOURCE) == "http://votealice.com/Path"
    assert module._normalized_target("https://wayback.library.ucla.edu/20170501/http://VoteAlice.com/Path", SOURCE) == "http://votealice.com/Path"


@pytest.mark.parametrize("target", ["mailto:person@gmail.com", "person@gmail.com", "javascript:alert(1)", "data:text/html,test", "https://", "http://localhost/a", "http://127.0.0.1/a", "http://user:pass@site.com", "http://bad host.com", "https://good.com:bad/"])
def test_invalid_targets_are_counted_and_never_emitted(target):
    document = parse(f'<p>Alice Smith campaign <a href="{target}">Website</a></p>')
    assert not [row for row in document["links"] if row["extraction_method"] == "html_hyperlink"]
    assert document["invalid_url_count"] >= 1


def test_json_csv_and_jsonld_attribution():
    for document in [
        parse(json.dumps([{"name": "Alice Smith", "website": "citytogether.org/Campaign"}, {"name": "Bob Jones", "website": "forwardcity.net"}]), "application/json"),
        parse("name,website\nAlice Smith,citytogether.org/Campaign\nBob Jones,forwardcity.net\n", "text/csv"),
        parse('<script type="application/ld+json">{"name":"Alice Smith","url":"https://citytogether.org/Campaign"}</script>'),
    ]:
        result = module.attribute_links(document, CANDIDATES, SOURCE)
        assert result
        assert {r["candidate_year_id"] for r in result if r["registrable_domain"] == "citytogether.org"} == {"alice__2017"}


def test_nested_json_preserves_parent_identity_without_crossing_siblings():
    document = parse(json.dumps({"candidates": [
        {"name": "Alice Smith", "links": {"website": "https://citytogether.org"}},
        {"name": "Bob Jones", "links": {"website": "https://forwardcity.net"}},
    ]}), "application/json")
    result = module.attribute_links(document, CANDIDATES, SOURCE)
    assert [(r["candidate_year_id"], r["registrable_domain"]) for r in result] == [
        ("alice__2017", "citytogether.org"), ("bob__2017", "forwardcity.net")]


def test_date_conflict_is_retained_and_not_autoapproved():
    result = claims("<p>Alice Smith 2021 campaign website: <a href='https://citytogether.org'>Website</a></p>")
    assert result[0]["temporal_status"] == "date_conflict_review"
    assert result[0]["review_status"] == "pending"
    assert result[0]["evidence_grade"] == "D"
    assert result[0]["strict_coverage_eligible"] is False


def test_generic_news_and_uncertain_multi_name_links_remain_unresolved():
    result = claims("<p>Alice Smith discussed the budget <a href='https://accountingfirm.org'>report</a>.</p>")
    assert result[0]["attribution_status"] == "match_unresolved"
    result = claims("<p><a href='https://citytogether.org'>Website</a> Alice Smith and Bob Jones are candidates.</p>")
    assert len(result) == 2
    assert all(r["attribution_status"] == "match_unresolved" for r in result)
    result = claims("<p>Alice Smith and Bob Jones <a href='https://citytogether.org'>campaign website</a></p>")
    assert len(result) == 2
    assert all(r["attribution_status"] == "match_unresolved" for r in result)


def test_domain_shape_and_profiles_are_preserved():
    result = claims("<p><a href='https://alicesmithformayor.com'>More</a></p><p><a href='https://ballotpedia.org/Bob_Jones'>Bob Jones</a></p>")
    assert result[0]["attribution_status"] == "domain_shape_only"
    assert result[1]["url_type"] == "third_party_profile"


def _pdf(text="Alice Smith campaign website", uri="https://CityTogether.org/Issues"):
    # A real one-page PDF fixture with a link annotation and a valid xref table.
    stream = f"BT /F1 14 Tf 30 750 Td ({text}) Tj ET".encode()
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
               b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R /Annots [6 0 R] >>",
               b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
               b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
               f"<< /Type /Annot /Subtype /Link /Rect [30 745 300 765] /A << /S /URI /URI ({uri}) >> /Contents (Alice Smith campaign website) >>".encode()]
    data, offsets = b"%PDF-1.4\n", [0]
    for number, obj in enumerate(objects, 1):
        offsets.append(len(data))
        data += f"{number} 0 obj\n".encode() + obj + b"\nendobj\n"
    xref = len(data)
    data += f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode()
    data += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:])
    return data + f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()


@pytest.mark.skipif(not module._tool("pdftotext"), reason="Poppler unavailable")
def test_real_pdf_hyperlink_annotation_without_visible_url():
    document = parse(_pdf(), "application/pdf", ocr=False)
    assert any(r["extraction_method"] == "pdf_hyperlink_annotation" and r["exact_normalized_url"] == "https://citytogether.org/Issues" for r in document["links"])
    result = module.attribute_links(document, CANDIDATES, SOURCE)
    assert result[0]["candidate_year_id"] == "alice__2017"


@pytest.mark.skipif(not module._tool("pdftotext"), reason="Poppler unavailable")
def test_ocr_extracts_urls_and_budget_residuals_are_explicit(monkeypatch):
    monkeypatch.setattr(module, "_ocr_page", lambda *args: "Alice Smith campaign website citytogether.org/Issues")
    document = parse(_pdf(text=""), "application/pdf")
    assert document["ocr_used"] is True
    assert document["ocr_pages"] == [1]
    assert any(r["extraction_method"] == "pdf_ocr_text" for r in document["links"])
    monkeypatch.setattr(module, "OCR_MAX_PAGES", 0)
    document = parse(_pdf(text=""), "application/pdf")
    assert document["parse_status"] == "partial"
    assert document["unprocessed_pages"] == [{"page": 1, "reason": "ocr_page_limit"}]


def test_meta_redirects_and_source_relative_paths():
    document = parse("<html><head><title>Alice Smith campaign website</title><meta http-equiv='refresh' content='0; url=https://CityTogether.org/Campaign'></head><body><a href='/candidate/AliceSmith'>Alice Smith</a></body></html>")
    assert any(r["extraction_method"] == "html_meta_refresh" for r in document["links"])
    assert any(r["exact_normalized_url"] == "https://springfieldnews.com/candidate/AliceSmith" and r["url_type"] == "source_internal_page" for r in document["links"])


def test_bad_documents_return_explicit_parse_errors():
    assert parse(b"not a pdf", "application/pdf")["parse_status"] == "parse_error"
    assert parse("{broken", "application/json")["parse_status"] == "parse_error"
    assert parse(b"binary", "application/octet-stream")["parse_status"] == "unsupported"


def test_xlsx_cell_hyperlinks_and_rows_without_extra_dependencies():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("xl/worksheets/sheet1.xml", '''<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
        <sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Name</t></is></c><c r="B1" t="inlineStr"><is><t>Website</t></is></c></row>
        <row r="2"><c r="A2" t="inlineStr"><is><t>Alice Smith</t></is></c><c r="B2" t="inlineStr"><is><t>Campaign website</t></is></c></row>
        <row r="3"><c r="A3" t="inlineStr"><is><t>Bob Jones</t></is></c><c r="B3" t="inlineStr"><is><t>citytogether.vote</t></is></c></row>
        <row r="4"><c r="A4" t="inlineStr"><is><t>Other source text without URLs</t></is></c></row></sheetData>
        <hyperlinks><hyperlink ref="B2" r:id="rId1"/></hyperlinks></worksheet>''')
        archive.writestr("xl/worksheets/_rels/sheet1.xml.rels", '''<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="https://CityTogether.org/Campaign"/></Relationships>''')
    document = parse(stream.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    assert document["parse_status"] == "parsed"
    result = module.attribute_links(document, CANDIDATES, SOURCE)
    assert any(row["candidate_year_id"] == "alice__2017" and row["exact_normalized_url"] == "https://citytogether.org/Campaign" for row in result)
    assert any(row["candidate_year_id"] == "bob__2017" and row["exact_normalized_url"] == "http://citytogether.vote" for row in result)
    assert "Other source text without URLs" in document["text"]


@pytest.mark.skipif(not all(module._tool(tool) for tool in ("pdftotext", "pdftoppm", "tesseract")), reason="OCR tools unavailable")
def test_actual_local_ocr_engine_recovers_website(monkeypatch):
    # Force OCR on a text-poor parse, then run the actual rasterizer and OCR engine.
    monkeypatch.setattr(module, "_pdf_pages", lambda path: [""])
    document = parse(_pdf(text="Alice Smith website CityTogether.org"), "application/pdf")
    assert document["ocr_pages"] == [1]
    assert any(r["extraction_method"] == "pdf_ocr_text" and r["registrable_domain"] == "citytogether.org" for r in document["links"])


def test_newsarticle_metadata_does_not_become_campaign_ownership():
    article = {
        "@context": "https://schema.org", "@type": "NewsArticle",
        "name": "Alice Smith launches mayoral campaign", "headline": "Alice Smith launches mayoral campaign",
        "url": "https://othernewspaper.com/story/alice-smith",
        "articleBody": "Alice Smith starts her campaign for mayor. " * 30,
        "author": {"@type": "Person", "name": "Staff Writer", "url": "https://reporterportfolio.org"},
        "publisher": {"@type": "Organization", "name": "City News", "url": "https://citynewspaper.org",
                      "sameAs": ["https://facebook.com/citynews"],
                      "logo": {"@type": "ImageObject", "url": "https://ca-times.brightspotcdn.com/dims4/default/resize/quality/75/?url=someimage"}},
        "image": "https://anotherimageservice.com/render?id=22",
    }
    document = parse('<script type="application/ld+json">' + json.dumps(article) + '</script>')
    assert len(document["links"]) == 7
    assert any(r["json_property"] == "@context" and r["url_type"] == "non_page_resource" for r in document["links"])
    assert any(r["structured_owner_name"] == "Staff Writer" and "Alice Smith" not in r["surrounding_context"] for r in document["links"])
    result = module.attribute_links(document, CANDIDATES, SOURCE)
    assert not any(r["attribution_status"] == "identity_supported" for r in result)
    assert any(r["url_type"] == "source_reference" and r["attribution_status"] == "reference_lead" for r in result)


@pytest.mark.parametrize("url,kind", [
    ("https://youtu.be/clip", "social_profile"),
    ("https://smartvoter.org/2017/05/02/ca/vote/smith/", "third_party_profile"),
    ("https://lp.org/", "source_reference"),
    ("https://www.sos.state.mn.us/candidates/alice", "officeholder_or_government_profile"),
    ("https://ci.springfield.or.us/candidates/alice", "officeholder_or_government_profile"),
    ("https://university.edu/profile/alice", "source_reference"),
    ("https://ca-times.brightspotcdn.com/dims4/id", "non_page_resource"),
])
def test_non_candidate_source_classes_cannot_count_as_campaign_sites(url, kind):
    document = parse(f'<p>Alice Smith <a href="{url}">campaign website</a></p>')
    assert document["links"][0]["url_type"] == kind
    result = module.attribute_links(document, CANDIDATES, SOURCE)
    assert not any(r["attribution_status"] == "identity_supported" for r in result)


def test_henderson_named_anchors_preserve_real_candidate_domains():
    candidates = [{"candidate_year_id": name.replace(" ", "_") + "__2017", "full_name": name, "year": 2017}
                  for name in ["Joe Scala", "Rick Workman", "Kyler Robinson"]]
    document = parse('''<article><h1>Henderson mayoral candidates</h1>
      <p><a href="https://electjoescala.com/Issues">Joe Scala</a> will compete against
      <a href="http://rickworkman.com/">Rick Workman</a> and
      <a href="http://kylerrobinsonformayor.com">Kyler Robinson</a>.</p></article>''')
    result = module.attribute_links(document, candidates, SOURCE)
    assert len(result) == 3
    assert all(r["attribution_status"] == "identity_supported" for r in result)
    assert {r["registrable_domain"] for r in result} == {"electjoescala.com", "rickworkman.com", "kylerrobinsonformayor.com"}


def test_named_news_headline_is_reference_even_when_it_mentions_campaign():
    result = claims('<p><a href="https://localjournal.org/story/alice-campaign">Alice Smith launches her campaign for mayor</a></p>')
    assert result[0]["attribution_status"] == "reference_lead"
    assert result[0]["url_type"] == "source_reference"
    result = claims('<p>Alice Smith announced her campaign and cited <a href="https://budgetoffice.org">the budget report</a>.</p>')
    assert result[0]["attribution_status"] == "match_unresolved"


def test_json_person_url_and_nested_explicit_website_support_identity():
    document = parse(json.dumps({"@type": "Person", "name": "Alice Smith", "url": "https://citytogether.org"}), "application/ld+json")
    result = module.attribute_links(document, CANDIDATES, SOURCE)
    assert result[0]["attribution_status"] == "identity_supported"
    assert result[0]["relation_role"] == "person_url"
    assert result[0]["structured_owner_name"] == "Alice Smith"


def test_bare_domain_website_labels_remain_supported_in_candidate_paragraphs():
    result = claims('<p>Alice Smith: website citytogether.org/Issues</p><p>Bob Jones: website forwardcity.net</p>')
    assert len(result) == 2
    assert all(r["attribution_status"] == "identity_supported" for r in result)
    assert {r["candidate_year_id"] for r in result if r["registrable_domain"] == "citytogether.org"} == {"alice__2017"}


@pytest.mark.parametrize("page,raw,transport,exact", [
    ("https://web.archive.org/web/20170501id_/http://citynews.org/guide/index.html", "/candidate/Alice?Year=2017",
     "https://web.archive.org/web/20170501id_/http://citynews.org/candidate/Alice?Year=2017", "http://citynews.org/candidate/Alice?Year=2017"),
    ("https://wayback.archive-it.org/5903/20170501/http://citynews.org/guide/", "../candidate/Alice",
     "https://wayback.archive-it.org/5903/20170501/http://citynews.org/candidate/Alice", "http://citynews.org/candidate/Alice"),
    ("https://web.archive.org/web/20170501/http://citynews.org/guide/", "/web/20160502/http://VoteAlice.com/Path",
     "https://web.archive.org/web/20160502/http://VoteAlice.com/Path", "http://votealice.com/Path"),
    (SOURCE, "CityTogether.org/Issues", "http://citytogether.org/Issues", "http://citytogether.org/Issues"),
    (SOURCE, "candidate/Alice.html", "https://springfieldnews.com/elections/candidate/Alice.html", "https://springfieldnews.com/elections/candidate/Alice.html"),
])
def test_transport_keeps_archive_routes_and_exact_campaign_target(page, raw, transport, exact):
    document = module.parse_document(f'<p>Alice Smith <a href="{raw}">Website</a></p>'.encode(), "text/html", page)
    hyperlink = next(row for row in document["links"] if row["extraction_method"] == "html_hyperlink")
    assert hyperlink["transport_url"] == transport
    assert hyperlink["exact_normalized_url"] == exact
    assert hyperlink["raw_url"] == raw


def test_new_claim_does_not_inherit_other_url_approval_or_archive_success():
    candidate = {**CANDIDATES[0], "human_accepted": True, "sampled_audit_accepted": True,
                 "archive_capture_available": True, "archive_capture_within_headline_window": True,
                 "downloaded_content_available": True, "historical_url_identified": True,
                 "coverage_basis": "old_site_archive", "covered": True, "has_url_lead": True,
                 "prior_attempt_refs_json": "large unrelated prior attempt history",
                 "strict_custom_domain": True, "review_decision": "human_accepted"}
    result = module.attribute_links(parse('<p>Alice Smith website <a href="https://citytogether.org">Website</a></p>'), [candidate], SOURCE)
    assert result[0]["candidate_year_id"] == candidate["candidate_year_id"]
    for key in ("human_accepted", "sampled_audit_accepted", "archive_capture_available", "archive_capture_within_headline_window", "downloaded_content_available", "historical_url_identified"):
        assert result[0][key] is False
    for key in ("coverage_basis", "covered", "has_url_lead", "prior_attempt_refs_json", "strict_custom_domain", "review_decision"):
        assert key not in result[0]


def test_plain_text_snippet_at_pdf_url_uses_authoritative_mime():
    document = module.parse_document(b"Alice Smith campaign website citytogether.org/Issues", "text/plain", "https://citynews.org/guide.pdf")
    assert document["parse_status"] == "parsed"
    assert document["links"][0]["exact_normalized_url"] == "http://citytogether.org/Issues"
    assert document["links"][0]["extraction_method"] == "plain_text_url"


@pytest.mark.skipif(not module._tool("pdftotext"), reason="Poppler unavailable")
def test_actual_pdf_signature_wins_over_wrong_text_mime():
    document = module.parse_document(_pdf(), "text/plain", "https://citynews.org/plain.txt", ocr=False)
    assert document["page_count"] == 1
    assert any(row["extraction_method"] == "pdf_hyperlink_annotation" for row in document["links"])


@pytest.mark.parametrize("mime", ["text/html", "application/pdf", "application/octet-stream"])
def test_html_body_at_pdf_url_is_parsed_as_html(mime):
    document = module.parse_document(b'<html><body><p>Source unavailable. <a href="https://citynews.org/help">Help</a></p></body></html>', mime, "https://citynews.org/guide.pdf")
    assert document["parse_status"] == "parsed"
    assert document["links"][0]["extraction_method"] == "html_hyperlink"
    assert "page_count" not in document


def test_fulop_governor_campaign_publication_date_conflicts_with_2013_mayoral_year():
    document = module.parse_document(b'''<html><head><title>Meet Democratic candidate for NJ governor Steven Fulop</title>
        <meta property="article:published_time" content="2025-06-09T12:00:00Z"></head>
        <body><p>Steven Fulop's campaign website: <a href="https://stevenfulop.com">Website</a></p></body></html>''',
        "text/html", "https://fox5ny.com/news/meet-steven-fulop")
    candidate = {"candidate_year_id": "fulop__2013", "full_name": "Steven Fulop", "year": 2013}
    result = module.attribute_links(document, [candidate], "https://fox5ny.com/news/meet-steven-fulop", "Steven Fulop Jersey City mayor website 2013")
    assert document["source_publication_date"] == "2025-06-09T12:00:00Z"
    assert result[0]["temporal_status"] == "date_conflict_review"
    assert result[0]["wrong_office_flag"] is True
    assert result[0]["review_status"] == "pending"
    assert result[0]["exact_normalized_url"] == "https://stevenfulop.com"


def test_explicit_historical_year_in_local_context_keeps_old_site_as_review_lead():
    document = parse('''<html><head><title>Alice Smith historical campaigns</title></head>
      <body><time datetime="2025-06-09">June 9, 2025</time>
      <p>Alice Smith 2017 campaign website: <a href="https://citytogether.org">Website</a></p></body></html>''')
    result = module.attribute_links(document, CANDIDATES, SOURCE)
    assert document["source_publication_date"] == "2025-06-09"
    assert result[0]["temporal_status"] == "not_assessed"


def test_jsonld_publication_date_and_governor_endorsement_are_distinct():
    document = parse('''<html><head><title>Governor endorses Alice Smith for mayor</title>
      <script type="application/ld+json">{"@type":"NewsArticle","datePublished":"2017-02-11","headline":"Governor endorses Alice Smith for mayor"}</script>
      </head><body><p>Alice Smith campaign website: <a href="https://citytogether.org">Website</a></p></body></html>''')
    result = module.attribute_links(document, CANDIDATES, SOURCE)
    assert document["source_publication_date"] == "2017-02-11"
    assert result[0]["wrong_office_flag"] is False
    assert result[0]["temporal_status"] == "not_assessed"


def test_onbase_static_download_target_is_extracted_without_javascript_execution():
    page = "https://ob.wvc-ut.gov/OnBaseAgendaOnline/Documents/DownloadFile/Meeting.pdf?documentType=5&meetingId=5407&isAttachment=True"
    body = b'''<html><head><title>Downloading, Please wait...</title></head><body>
      <script>window.location = window.location.toString().replace("DownloadFile", "DownloadFileBytes");</script></body></html>'''
    document = module.parse_document(body, "text/html", page)
    assert document["parse_status"] == "parsed"
    assert document["links"][0]["relation_role"] == "document_download"
    assert document["links"][0]["transport_url"] == page.replace("/DownloadFile/", "/DownloadFileBytes/")
    assert document["links"][0]["exact_normalized_url"].endswith("Meeting.pdf?documentType=5&meetingId=5407&isAttachment=True")
    again = module.parse_document(body, "text/html", page.replace("/DownloadFile/", "/DownloadFileBytes/"))
    assert not again["links"]


def test_existing_flattened_mayor_council_links_do_not_assign_unknown_people_to_mayor():
    # These are the fields from the saved Patch parser output, without another HTML parse.
    parts = ["Alexandria Mayor Bill Euille Democrat billeuille.com Andrew Macdonald Independent",
             "Alexandria City Council John Chapman Democrat",
             "Glenda Davis Independent (currently no website) Frank Fannon Republican",
             "Alicia Hughes Republican"]
    domains = ["andrewmacdonaldformayor.com", "chapman4council.com", "frankfannon.com", "aliciahughes.com"]
    context = " ".join(f"{part} {domain}" for part, domain in zip(parts, domains))
    links = []
    for index, domain in enumerate(domains):
        before, after = context.split(domain, 1)
        links.append({"raw_url": "http://" + domain + "/", "exact_normalized_url": "http://" + domain + "/",
                      "anchor_text": domain, "surrounding_context": context, "preceding_text": before,
                      "following_text": after, "heading_context": "Local updates",
                      "url_type": "custom_domain", "registrable_domain": domain, "source_locator": str(index),
                      "extraction_method": "html_hyperlink"})
    candidate = {"candidate_year_id": "andrew__2012", "full_name": "Andrew Macdonald", "year": 2012}
    result = module.attribute_links({"links": links, "title": "Alexandria Mayor and City Council Candidate Websites"}, [candidate], SOURCE)
    supported = [row["registrable_domain"] for row in result if row["attribution_status"] == "identity_supported"]
    assert supported == ["andrewmacdonaldformayor.com"]
    assert len(result) == 4
    assert all(row["attribution_version"] == module.ATTRIBUTION_VERSION for row in result)


def test_unknown_named_owner_blocks_known_candidate_without_prior_link_boundary():
    result = claims('<p>Alice Smith supports John Doe. John Doe campaign website <a href="https://forwardcity.net">Website</a></p>')
    assert not any(row["attribution_status"] == "identity_supported" for row in result)
    result = claims('<h2>Alice Smith</h2><p>John Doe <a href="https://forwardcity.net">Website</a></p>')
    assert not any(row["attribution_status"] == "identity_supported" for row in result)
