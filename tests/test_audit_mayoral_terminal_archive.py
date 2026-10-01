import importlib.util
from pathlib import Path


spec = importlib.util.spec_from_file_location("terminal_audit", Path(__file__).parents[1] / "scripts/audit_mayoral_terminal_archive.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def test_empty_requires_complete_verified_queries():
    query = {"inventory_status": "no_capture", "raw_verified": True, "raw_rows": 0}
    assert audit.classify([query], 1, [], [])[0] == "completed_empty_inventory"
    assert audit.classify([query], 2, [], [])[0] == "inventory_error_or_incomplete"
    assert audit.classify([{**query, "raw_verified": False}], 1, [], [])[0] == "inventory_error_or_incomplete"
    assert audit.classify([{**query, "inventory_status": "request_error"}], 1, [], [])[0] == "inventory_error_or_incomplete"


def test_fetch_failure_is_not_empty_archive():
    query = {"inventory_status": "found", "raw_verified": True, "raw_rows": 1}
    assert audit.classify([query], 1, [{"retryable": "False"}], [])[0] == "terminal_fetch_without_saved_response"
    assert audit.classify([query], 1, [{"retryable": "True"}], [])[0] == "retryable_fetch"
    assert audit.classify([query], 1, [{"retryable": "False", "fetch_status": "no_content"}], [])[0] == "saved_html_without_extracted_text"


def test_variant_matching_preserves_path_and_query():
    assert audit.equivalent("http://www.example.com/a/") == audit.equivalent("https://example.com/a")
    assert audit.equivalent("https://example.com/a") != audit.equivalent("https://example.com/b")
    assert audit.equivalent("https://example.com/a?q=1") != audit.equivalent("https://example.com/a?q=2")
