from pathlib import Path

import pandas as pd

from scripts.build_municipal_curated_acquisition_contract import (
    audit_post_shard_directories,
)
from scripts.build_municipal_curated_acquisition_shard_1 import (
    AUDIT_DIR,
    OUTPUT_DIR,
    parse_html,
    records_from_pdf_page_text,
    source_label_matches_candidate,
)


FIXTURES = Path(__file__).parent / "fixtures" / "municipal_curated_shard_1"


def test_cbs_candidate_labels_recover_middle_names_and_party_suffixes():
    records = parse_html(
        FIXTURES / "cbs_candidate_links.html",
        "https://www.cbsnews.com/minnesota/news/minneapolis-mayoral-election-candidates/",
    )
    anchors = [record for record in records if record.method == "html_anchor"]
    expected = {
        "kurtis hanna": "http://captainkurtis.com/",
        "john hartwig": "http://candidates.sos.state.mn.us/CandidateInformation.aspx?candidateid=8502",
        "bill kahn": "http://lastminneapolismayor.org/",
        "tony lane": "http://www.themilitant.com/",
        "don samuels": "http://samuelsformayor.com/",
        "captain jack sparrow": "http://occupirate.blogspot.com/",
        "oloveuse 'ole' savior": "http://www.olesavior.org/",
        "tony benjegerdes": "http://mayortroy.com/",
        "mark anderson": "http://markvandersonformayor.org/",
    }
    for candidate, url in expected.items():
        matches = [
            record.raw_url
            for record in anchors
            if source_label_matches_candidate(candidate, record.owner_name)
        ]
        assert matches == [url]


def test_alameda_urls_attach_to_statement_owner_not_endorser():
    pages = (FIXTURES / "alameda_statement_pages.txt").read_text().split("\f")
    candidates = ["barbara halliday", "mark salinas", "libby schaaf"]
    records = [
        record
        for page_number, page in enumerate(pages, start=1)
        for record in records_from_pdf_page_text(page, page_number, candidates)
    ]
    owners = {record.raw_url: record.owner_name.casefold() for record in records}
    assert owners["www.BarbaraHalliday.com"] == "barbara halliday"
    assert owners["www.salinasforhayward.com"] == "mark salinas"
    assert owners["www.charlie.vote"] == "charlie michelson"
    assert owners["www.LorenTaylor.org"] == "loren taylor"
    assert owners["Aimee4Schools.com"] == "aimee eng"
    assert all(owner != "libby schaaf" for owner in owners.values())


def test_final_shard_known_claims_and_no_libby_false_assignment():
    evidence = pd.read_csv(OUTPUT_DIR / "evidence.csv", dtype=str, keep_default_na=False)
    candidates = pd.read_csv(
        AUDIT_DIR / "shard_1_candidate_targets.csv", dtype=str, keep_default_na=False
    )
    claims = evidence.merge(
        candidates[["target_id", "candidate_year_id", "full_name"]],
        on=["target_id", "candidate_year_id"],
        validate="many_to_one",
    )

    def urls(name: str) -> set[str]:
        return set(claims.loc[claims["full_name"].eq(name), "exact_normalized_url"])

    assert "http://captainkurtis.com/" in urls("kurtis  hanna")
    assert "http://lastminneapolismayor.org/" in urls("bill kahn")
    assert "http://samuelsformayor.com/" in urls("don samuels")
    assert "http://occupirate.blogspot.com/" in urls("captain jack sparrow")
    assert urls("mark salinas") == {"http://www.salinasforhayward.com"}
    assert urls("barbara halliday") == {"http://www.barbarahalliday.com"}
    assert not urls("libby schaaf")


def test_final_shard_passes_frozen_offline_contract_audit():
    targets = pd.read_csv(AUDIT_DIR / "shard_1_targets.csv", keep_default_na=False)
    candidates = pd.read_csv(
        AUDIT_DIR / "shard_1_candidate_targets.csv", keep_default_na=False
    )
    parsers = pd.read_csv(AUDIT_DIR / "parser_support_matrix.csv", keep_default_na=False)
    result = audit_post_shard_directories(
        targets, candidates, parsers, {1: OUTPUT_DIR}
    )
    assert result["verdict"] == "pass"
    assert result["expected_attempts"] == 22
    assert result["terminal_attempts"] == 22
