from pathlib import Path
from scripts.reconcile_mayoral_completed_scrape import exact, reconcile


def test_remote_source_is_valid_python():
    text = Path("scripts/reconcile_mayoral_completed_scrape.py").read_text()
    compile(text.split("\ndef main():")[0], "<remote>", "exec")


def test_exact_preserves_path_scheme_query_and_host():
    assert exact("https://EXAMPLE.com") == "https://example.com/"
    assert exact("http://example.com/Case?a=1") != exact("https://example.com/case?a=1")
    assert exact("https://www.example.com/") != exact("https://example.com/")


def test_domain_is_not_exact_and_candidate_identity_is_required():
    urls = [{"candidate_year_id": c, "candidate_url_id": c, "exact_normalized_url": "https://example.com/Case", "registrable_domain": "example.com"} for c in ["a", "b"]]
    roster = [{"candidate_year_id": c,"active_2010_2021":"True"} for c in ["a","b"]]
    remote = {"targets":[{"target_id":"t","candidate_year_id":"a","registrable_domain":"example.com","saved_response":2,"inventory":2,"inventory_target_status":"found"}],"exact":[]}
    result = reconcile(urls,roster,remote,[])
    assert result[0]["completed_batch_status"] == "same_candidate_domain_response_saved"
    assert result[0]["exact_url_saved_response"] == 0
    assert result[1]["completed_batch_status"] == "not_in_completed_frozen_batch"
    assert not result[0]["prior_validation_campaign_supported"]


def test_metadata_and_completed_empty_are_distinct():
    urls = [{"candidate_year_id": c,"candidate_url_id": c,"exact_normalized_url":"http://e.com/", "registrable_domain":"e.com"} for c in ["a","b"]]
    roster = [{"candidate_year_id": c,"active_2010_2021":"True"} for c in ["a","b"]]
    targets = [{"target_id":c,"candidate_year_id":c,"registrable_domain":"e.com","inventory":n,"inventory_target_status":s} for c,n,s in [("a",1,"found"),("b",0,"no_capture")]]
    results = reconcile(urls,roster,{"targets":targets,"exact":[]},[])
    assert results[0]["completed_batch_status"] == "same_candidate_domain_metadata_only"
    assert results[1]["completed_batch_status"] == "frozen_target_no_capture"
