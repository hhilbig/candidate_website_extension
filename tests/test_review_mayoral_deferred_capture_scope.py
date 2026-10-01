import csv
import json
from pathlib import Path

from scripts.review_mayoral_deferred_capture_scope import classify_url


def test_ordinary_same_domain_path_is_lead():
    decision, _, _ = classify_url("https://www.example.org/issues/housing", "example.org")
    assert decision == "eligible_download_lead"


def test_host_variant_normalizes_but_offsite_requires_review():
    assert classify_url("http://example.org/about", "example.org")[0] == "eligible_download_lead"
    assert classify_url("https://forms.example.net/signup", "example.org")[0] == "needs_scope_review"


def test_explicit_technical_endpoints_are_not_page_targets():
    assert classify_url("https://example.org/wp-login.php", "example.org")[0] == "not_campaign_page_target"
    assert classify_url("https://example.org/crmapi/subscribe", "example.org")[0] == "not_campaign_page_target"


def test_same_host_index_and_embed_page_roles_are_leads():
    urls = [
        "https://example.org/author/admin/",
        "https://example.org/2021/03/",
        "https://example.org/event/embed/",
    ]
    assert all(classify_url(url, "example.org")[0] == "eligible_download_lead" for url in urls)


def test_individually_adjudicated_queries():
    assert classify_url("https://bevandufty.com/support-bevan?v=endorse", "bevandufty.com")[0] == "eligible_download_lead"
    assert classify_url("https://indiawalton.com/news?offset=1632", "indiawalton.com")[0] == "eligible_download_lead"
    decision, reason, _ = classify_url(
        "http://lelandyee.com/take-action/support-our-bills?/take-action/rush-boycott",
        "lelandyee.com",
    )
    assert decision == "needs_scope_review"
    assert "malformed route-as-query" in reason


def test_template_and_unadjudicated_query_require_review():
    urls = [
        "https://example.org/template-item/page-template/",
        "https://example.org/news?offset=3",
    ]
    assert all(classify_url(url, "example.org")[0] == "needs_scope_review" for url in urls)


def test_v4_saved_evidence_overlay_changes_only_exact_keys():
    base = Path("quality_reports/mayoral_deferred_scope_review_2026-09-14")
    evidence = json.loads((base / "leland_saved_route_evidence.json").read_text())
    with (base / "v3/deferred_capture_scope_reviews.csv").open(newline="") as f:
        v3 = {(r["timestamp"], r["original_url"]): r for r in csv.DictReader(f)}
    with (base / "v4/deferred_capture_scope_reviews.csv").open(newline="") as f:
        v4 = {(r["timestamp"], r["original_url"]): r for r in csv.DictReader(f)}
    expected = {(str(r["timestamp"]), r["original_url"]) for r in evidence["pending_capture_keys"]}
    assert v3.keys() == v4.keys()
    changed = {key for key in v3 if v3[key]["scope_decision"] != v4[key]["scope_decision"]}
    assert changed == expected
    for key in expected:
        assert v3[key]["scope_decision"] == "needs_scope_review"
        assert v4[key]["scope_decision"] == "eligible_download_lead"
        assert v4[key]["saved_evidence_path"].endswith("leland_saved_route_evidence.json")
