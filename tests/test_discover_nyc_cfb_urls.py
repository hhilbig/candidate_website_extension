import pandas as pd

from scripts.discover_nyc_cfb_urls import (
    match_candidate, mayor_profile_links, profile_campaign_urls, profile_candidate_name,
)


def test_mayor_profile_links_are_limited_to_cycle_and_mayor_prefix():
    html = '''<a href="/public/voter-guide/general_2009/cd_profile/M_Bloomberg_605.aspx#key">Mike</a>
    <a href="/public/voter-guide/general_2009/cd_profile/PA_other.aspx">Other</a>'''
    assert mayor_profile_links(html, "https://www.nyccfb.info/start", 2009) == [
        "https://www.nyccfb.info/public/voter-guide/general_2009/cd_profile/M_Bloomberg_605.aspx"
    ]


def test_extracts_plain_text_and_icon_candidate_supplied_websites():
    html = '''<p><strong>Web site:</strong> www.ferrer2005.com </p>
    <li><a href="http://www.billdeblasio.com" alt="Web Site"><img></a></li>
    <p><strong>Website:</strong><a href="https://facebook.com/not-a-site">social</a></p>'''
    assert profile_campaign_urls(html) == [
        ("http://www.billdeblasio.com", "Candidate-supplied Web Site link"),
        ("http://www.ferrer2005.com", "Web site: www.ferrer2005.com"),
    ]


def test_profile_title_and_mojibake_tolerant_candidate_match():
    assert profile_candidate_name("<title>2013 NYC Voter Guide: Adolfo Carrión, Jr.</title>") == "Adolfo Carrión, Jr."
    candidates = pd.DataFrame([{"full_name":"adolfo carriÃ³n"},{"full_name":"bill de blasio"}])
    matched, score = match_candidate("Adolfo Carrión, Jr.", candidates)
    assert matched.full_name == "adolfo carriÃ³n"
    assert score >= 0.86


def test_unique_surname_match_handles_common_first_name_variants():
    candidates = pd.DataFrame([
        {"full_name":"michael mike bloomberg"}, {"full_name":"william bill thompson"}
    ])
    matched, score = match_candidate("Mike Bloomberg", candidates)
    assert matched.full_name == "michael mike bloomberg"
    assert score >= 0.90
    matched, score = match_candidate("William C. Thompson, Jr.", candidates)
    assert matched.full_name == "william bill thompson"
    assert score >= 0.90
