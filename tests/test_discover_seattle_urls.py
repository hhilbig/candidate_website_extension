import pandas as pd

from scripts.discover_seattle_urls import (
    best_candidate_match,
    extract_campaign_contact,
    mayor_campaign_ids,
    normalize_reported_url,
    parse_contact_csv,
)


def test_extracts_only_mayor_campaign_ids_from_seattle_homepage():
    html = """
    <div>
      <span class="tabLeftColumn"><strong>Mayor</strong></span>
      <span class="tabLeftColumnItem" onclick="jsLoadDashboard('2021','campaign',774,'collapsed');">A</span>
      <span class="tabLeftColumnItem" onclick="jsLoadDashboard('2021','campaign',758,'collapsed');">B</span>
      <span class="tabLeftColumn"><strong>City Attorney</strong></span>
      <span class="tabLeftColumnItem" onclick="jsLoadDashboard('2021','campaign',999,'collapsed');">C</span>
    </div>
    """
    assert mayor_campaign_ids(html, 2021) == [774, 758]


def test_extracts_candidate_site_from_official_contact_block():
    html = """
    <div style="text-align: left; font-size: 1em; overflow: visible;">
      <strong>Bruce Harrell</strong><br>
      <a href="mailto:info@example.com">email</a>
      <a href="http://bruceforseattle.com">bruceforseattle.com</a>
    </div>
    """
    name, links = extract_campaign_contact(html)
    assert name == "Bruce Harrell"
    assert links == [("http://bruceforseattle.com", "bruceforseattle.com")]


def test_name_match_handles_initial_and_accent():
    candidates = pd.DataFrame([
        {"full_name": "m lorena gonzalez"}, {"full_name": "bruce harrell"}
    ])
    matched, score = best_candidate_match("M. Lorena González", candidates)
    assert matched["full_name"] == "m lorena gonzalez"
    assert score == 1


def test_name_match_handles_first_name_variant_and_middle_initial():
    candidates = pd.DataFrame([
        {"full_name": "ed murray"}, {"full_name": "mike mcginn"}
    ])
    matched, score = best_candidate_match("Edward B. Murray", candidates)
    assert matched["full_name"] == "ed murray"
    assert score >= 0.90


def test_parse_contact_csv_and_normalize_reported_domain():
    text = (
        "intCampaignID,strContestType,strCommitteeType,c14CampaignName,strCampaignBallotStatus,c30CampaignName,c100CommitteeName,c30OrderName,c200StreetAddress,c200Email,c200Web,vc3ParticipationStatus,strTreasurerName\n"
        '"774","mayor","candidates","Harrell","1","Bruce Harrell","Bruce for Seattle","Harrell","address","email","bruceforseattle.com","No","Treasurer"\n'
        '"999","council","candidates","Other","1","Other Person","Other","Other","other.example","No","NA"\n'
    )
    result = parse_contact_csv(text)
    assert result["intCampaignID"].tolist() == ["774"]
    assert normalize_reported_url(result.iloc[0]["c200Web"]) == (
        "http://bruceforseattle.com"
    )


def test_parse_contact_csv_repairs_short_legacy_row_from_tail_marker():
    text = (
        "intCampaignID,strContestType,strCommitteeType,c14CampaignName,strCampaignBallotStatus,c30CampaignName,c100CommitteeName,c30OrderName,many,changing,columns\n"
        '"114","mayor","candidates","Williams","5","Luke Williams","Luke for Mayor","Williams","206-555","email@example.com","peopleforluke.org","No","","NA"\n'
    )
    result = parse_contact_csv(text)
    assert result.iloc[0]["c200Web"] == "peopleforluke.org"
    assert result.iloc[0]["source_field_count"] == 14
