import pandas as pd

import scripts.freeze_mayoral_search_temporal_validation as freeze
from scripts.run_mayoral_search_source_expansion_2010_2021 import (
    PAGE_LINK_COLUMNS,
    URL_LEAD_COLUMNS,
)


def lead(**updates):
    row = {column: "" for column in URL_LEAD_COLUMNS}
    row.update(
        contract_version="mayoral-search-source-expansion-2010-2021-v1",
        url_claim_id="claim-1",
        url_entity_id="entity-1",
        result_claim_id="result-1",
        query_attempt_id="query-1",
        discovery_candidate_year_id="a__2013",
        discovery_candidate_contest_id="contest__a",
        candidate_year_id="a__2013",
        candidate_contest_id="contest__a",
        contest="contest",
        full_name="Betsy Hodges",
        geo_name="Minneapolis",
        state_abb="MN",
        year="2013",
        month="11",
        exact_normalized_url="http://www.betsyhodges.org/",
        raw_url="http://www.betsyhodges.org/",
        site_root_url="http://www.betsyhodges.org/",
        registrable_domain="betsyhodges.org",
        url_type="custom_domain",
        candidate_attribution_status="identity_supported",
        evidence_grade="D",
        strict_coverage_eligible=False,
        review_status="pending",
        parser_version="v1",
        extraction_method="html_hyperlink",
        page_link_id="link-1",
    )
    row.update(updates)
    return row


def links(anchor="Betsy Hodges"):
    row = {column: "" for column in PAGE_LINK_COLUMNS}
    row.update(page_link_id="link-1", anchor_text=anchor)
    return pd.DataFrame([row], columns=PAGE_LINK_COLUMNS)


def test_candidate_named_link_and_name_domain_is_frozen_but_not_approved():
    eligible, decisions, audit = freeze.freeze_leads(
        pd.DataFrame([lead()], columns=URL_LEAD_COLUMNS), links()
    )
    assert len(eligible) == 1
    assert eligible.iloc[0].candidate_year_id == "a__2013"
    assert eligible.iloc[0].discovery_candidate_year_id == "a__2013"
    assert bool(eligible.iloc[0].eligible_for_temporal_validation)
    assert not bool(eligible.iloc[0].strict_coverage_eligible)
    assert eligible.iloc[0].acceptance == ""
    assert audit["strict_or_accepted_claims_created"] == 0
    assert bool(decisions.iloc[0].candidate_token_in_host)


def test_direct_result_is_retained_for_content_validation():
    row = lead(
        extraction_method="serper_direct_candidate_site_result",
        page_link_id="",
        exact_normalized_url="https://electalvey.com/",
        raw_url="https://electalvey.com/",
    )
    eligible, _, _ = freeze.freeze_leads(
        pd.DataFrame([row], columns=URL_LEAD_COLUMNS),
        pd.DataFrame(columns=PAGE_LINK_COLUMNS),
    )
    assert len(eligible) == 1
    assert eligible.iloc[0].eligibility_reason == "direct_serper_candidate_site_result"


def test_news_url_and_government_profile_fail_the_ownership_gate():
    rows = [
        lead(
            url_claim_id="news",
            url_entity_id="news-entity",
            exact_normalized_url="https://example-news.com/story",
            raw_url="https://example-news.com/story",
        ),
        lead(
            url_claim_id="government",
            url_entity_id="government-entity",
            url_type="officeholder_or_government_profile",
        ),
    ]
    eligible, decisions, audit = freeze.freeze_leads(
        pd.DataFrame(rows, columns=URL_LEAD_COLUMNS), links()
    )
    assert eligible.empty
    assert len(decisions) == 2
    assert audit["eligible_rows"] == 0


def test_validation_key_changes_with_attributed_candidate_not_discovery_candidate():
    first = lead(discovery_candidate_year_id="searcher__2013")
    second = lead(
        url_claim_id="claim-2",
        url_entity_id="entity-2",
        discovery_candidate_year_id="other-searcher__2013",
    )
    eligible, _, _ = freeze.freeze_leads(
        pd.DataFrame([first, second], columns=URL_LEAD_COLUMNS), links()
    )
    assert eligible.validation_key.nunique() == 2
    assert eligible.validation_hash.nunique() == 2


def test_verified_contest_directory_rescues_candidate_named_opaque_domain():
    row = lead(
        full_name="Christopher Robin Zimmerman",
        exact_normalized_url="http://crz.net/",
        raw_url="http://crz.net/",
        source_page_url=(
            "https://www.cbsnews.com/minnesota/news/"
            "minneapolis-mayoral-election-candidates/"
        ),
    )
    eligible, _, _ = freeze.freeze_leads(
        pd.DataFrame([row], columns=URL_LEAD_COLUMNS),
        links("Christopher Robin Zimmerman (Libertarian)"),
    )
    assert len(eligible) == 1
    assert (
        eligible.iloc[0].eligibility_reason
        == "candidate_named_link_in_verified_contest_campaign_directory"
    )


def test_contest_directory_does_not_rescue_known_party_or_publisher_domain():
    row = lead(
        full_name="Christopher Clark",
        exact_normalized_url="http://www.lp.org/",
        raw_url="http://www.lp.org/",
        registrable_domain="lp.org",
        source_page_url=(
            "https://www.cbsnews.com/minnesota/news/"
            "minneapolis-mayoral-election-candidates/"
        ),
    )
    eligible, _, _ = freeze.freeze_leads(
        pd.DataFrame([row], columns=URL_LEAD_COLUMNS),
        links("Christopher Clark (Libertarian Party)"),
    )
    assert eligible.empty
