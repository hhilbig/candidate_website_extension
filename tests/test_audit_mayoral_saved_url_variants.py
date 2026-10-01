from pathlib import Path
import pytest
from scripts.audit_mayoral_saved_url_variants import exact, original, relation, variant_key, distance, summarize, context_matches, validate_keys, validate_cache_provenance


def test_wrappers_and_sensitive_paths():
    assert original("https://web.archive.org/web/20101103000100id_/http://www.a.org/Case?Q=X") == "http://www.a.org/Case?Q=X"
    assert original("https://wayback.archive-it.org/5903/20101103000100id_/http://a.org/") == "http://a.org/"
    assert exact("HTTP://A.ORG") == "http://a.org/"
    assert variant_key("https://www.a.org:443/Case?Q=X") == variant_key("http://a.org:80/Case?Q=X")
    assert variant_key("https://a.org/Case?Q=X") != variant_key("https://a.org/case?Q=X")
    assert variant_key("https://a.org/Case?Q=X") != variant_key("https://a.org/Case?Q=x")
    assert variant_key("https://a.org:80/") != variant_key("http://a.org/")


def test_syntax_is_not_redirect_evidence():
    assert relation("https://a.org/", "http://www.a.org/", "http://www.a.org/") == "saved_syntactic_variant_only"
    assert relation("https://a.org/", "https://a.org/", "http://www.a.org/") == "saved_request_to_variant_redirect"
    assert relation("https://a.org/", "https://a.org/Old", "https://a.org/") == "saved_effective_exact_url"
    assert relation("https://a.org/Case", "https://a.org/Case", "https://a.org/case") == "different_path_query_or_host"
    assert relation("https://a.org/", "https://a.org/", "") == "effective_url_unknown"
    assert distance("20101114000000", "2010-11-15") == 1


def test_failed_hash_does_not_promote():
    r = {"candidate_url_id": "x", "candidate_year_id": "c", "exact_normalized_url": "https://a.org", "review_action": "a"}
    remote = {"results": {"x": {"counts": {}, "best": {"saved_effective_exact_url": {"raw_hash_check": "hash_mismatch", "within_365_days": True}}}}}
    result = summarize([r], remote)[0]
    assert result["classification"] == "no_verified_saved_response"
    assert result["same_path_saved_within_365_days"] is False
    assert result["strict_coverage_newly_accepted"] is False


def test_candidate_year_and_domain_isolation():
    r = {"candidate_year_id": "a__2014", "registrable_domain": "site.org"}
    assert context_matches(r, {"candidate_year_id": "a__2014"}, r)
    assert not context_matches(r, {"candidate_year_id": "a__2018"}, r)
    assert not context_matches(r, {"candidate_year_id": "b__2014"}, r)
    assert not context_matches(r, {"candidate_year_id": "a__2014"}, {**r, "registrable_domain": "other.org"})


def test_complete_requested_key_contract():
    requested = [{"candidate_url_id": str(i)} for i in range(521)]
    result = {str(i): {} for i in range(521)}
    validate_keys(requested, result, 521)
    with pytest.raises(ValueError):
        validate_keys(requested[:-1], result, 521)
    with pytest.raises(ValueError):
        validate_keys(requested + requested[:1], result)
    with pytest.raises(ValueError):
        validate_keys(requested, {**result, "unexpected": {}}, 521)
    with pytest.raises(ValueError):
        validate_keys(requested, {str(i): {} for i in range(520)}, 521)


def test_remote_payload_is_valid_python():
    source = Path("scripts/audit_mayoral_saved_url_variants.py").read_text().rsplit('if __name__ == "__main__":', 1)[0]
    compile(source + "\nprint(scan([], []))\n", "remote", "exec")


def test_cache_rejects_changed_or_missing_input_hashes():
    expected = {"queue": "abc", "validation": "def"}
    validate_cache_provenance({"local_input_hashes": expected}, expected)
    with pytest.raises(ValueError):
        validate_cache_provenance({}, expected)
    with pytest.raises(ValueError):
        validate_cache_provenance({"local_input_hashes": {**expected, "queue": "changed"}}, expected)
