"""Mayoral release: text status rule and validator detection of inconsistent release files."""
import csv
import json

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.build_mayoral_ledb_crosswalk import match_basis
from scripts.build_mayoral_release import text_status
from scripts.validate_mayoral_release import validate, write_manifest


def person(**kw):
    return {"candidate_election_id": "a", "downloaded_content": "", "url_lead": "", **kw}


def test_text_status_precedence():
    assert text_status(person(), 3, ["pass"], set()) == "text"
    assert text_status(person(downloaded_content="True"), 0, ["excluded"], set()) == "excluded_site_check"
    assert text_status(person(downloaded_content="True"), 0, ["pass"], set()) == "no_usable_pages"
    assert text_status(person(), 0, [], {"a"}) == "check_incomplete"
    assert text_status(person(url_lead="True"), 0, [], set()) == "site_not_archived"
    assert text_status(person(), 0, [], set()) == "no_url_lead"


def write(path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def release(tmp_path):
    cities = [{"city_id": f"c{i}", "county_fips": f"c{i}"[:2] + "001", "county_fips_all": f"c{i}"[:2] + "001"} for i in range(100)]
    years = [{"city_id": f"c{i}", "year": str(y)} for i in range(100) for y in range(2010, 2027)]
    write(tmp_path / "mayoral_cities.csv", cities)
    write(tmp_path / "mayoral_city_years.csv", years)
    write(tmp_path / "mayoral_elections.csv", [{"election_id": "e1"}])
    finalists = [
        {"candidate_election_id": "f1", "election_id": "e1", "city_id": "c0", "eligible": "True", "text_status": "text",
         "url_lead": "True", "n_pages_usable": "2"},
        {"candidate_election_id": "f2", "election_id": "e1", "city_id": "c0", "eligible": "True",
         "text_status": "no_url_lead", "url_lead": "", "n_pages_usable": "0"}]
    write(tmp_path / "mayoral_finalists.csv", finalists)
    page = {"candidate_election_id": "f1", "target_id": "t", "capture_timestamp": "20200101000000",
            "days_from_election": -10, "in_text_window": True, "excluded": False, "duplicate_of": "",
            "site_check_passed": True, "release_usable": True}
    pages = [dict(page, page_id="p1", original_url="https://a.test/", text="Vote A", text_sha256="a"),
             dict(page, page_id="p2", original_url="https://a.test/issues/", text="Issues", text_sha256="i"),
             dict(page, page_id="p3", original_url="https://a.test/old/", text="Old", text_sha256="o",
                  days_from_election=-900, in_text_window=False, release_usable=False)]
    pq.write_table(pa.Table.from_pylist(pages), tmp_path / "mayoral_page_corpus.parquet")
    write(tmp_path / "mayoral_finalist_panel.csv", [{"candidate_election_id": "f1", "n_pages": "2", "text": "Vote A#+#Issues"}])
    write(tmp_path / "mayoral_ledb_crosswalk.csv", [
        {"candidate_election_id": "f1", "ledb_candid": "7", "ledb_contest": "k1", "ledb_match": "name_same_month"},
        {"candidate_election_id": "f2", "ledb_candid": "8", "ledb_contest": "k1", "ledb_match": "last_name_same_month"}])
    write_manifest(tmp_path)
    return tmp_path


def test_valid_release_passes(tmp_path):
    report = validate(release(tmp_path))
    assert report["valid"], report["errors"]


def test_validator_catches_panel_text_and_usable_rule_changes(tmp_path):
    root = release(tmp_path)
    write(root / "mayoral_finalist_panel.csv", [{"candidate_election_id": "f1", "n_pages": "2", "text": "Vote A"}])
    report = validate(root)
    assert not report["valid"] and any("panel text differs" in e for e in report["errors"])
    table = pq.read_table(root / "mayoral_page_corpus.parquet").to_pylist()
    table[2]["release_usable"] = True
    pq.write_table(pa.Table.from_pylist(table), root / "mayoral_page_corpus.parquet")
    errors = validate(root)["errors"]
    assert any("release_usable differs" in e for e in errors) and any("SHA-256 differs" in e for e in errors)


def test_validator_rejects_status_without_panel(tmp_path):
    root = release(tmp_path)
    rows = list(csv.DictReader((root / "mayoral_finalists.csv").open()))
    rows[1]["text_status"] = "text"
    write(root / "mayoral_finalists.csv", rows)
    assert any("panel rows differ" in e for e in validate(root)["errors"])


def test_site_rejected_status_and_person_matching():
    from scripts.build_mayoral_release import name_parts, person_ids
    assert text_status(person(url_lead="True"), 0, [], set(), frozenset({"a"})) == "site_rejected"
    assert name_parts("Ed Lee")[1] == name_parts("Edwin M. Lee")[1] == "lee"
    people = [{"candidate_election_id": i, "city_id": "c", "candidate_name": n} for i, n in
              [("1", "Bill Bell"), ("2", "William V. (Bill) Bell"), ("3", "Anjanee Bell"), ("4", "Ed Lee"), ("5", "Edwin M. Lee")]]
    ids = person_ids(people)
    assert ids["1"] == ids["2"] != ids["3"] and ids["4"] == ids["5"]
    people = [{"candidate_election_id": i, "city_id": "c", "candidate_name": n} for i, n in
              [("6", "G T Bynum"), ("7", "GT Bynum"), ("8", "Eddie Lorton"), ("9", 'George "Eddie" Lorton'),
               ("10", "Sylvester Williams"), ("11", "Leonardo (Leo) Williams")]]
    ids = person_ids(people)
    assert ids["6"] == ids["7"] and ids["8"] == ids["9"] and ids["10"] != ids["11"]


def test_pass4_status_replaces_earlier_incomplete(tmp_path, monkeypatch):
    import scripts.build_mayoral_release as release
    sweep = tmp_path / "sweep_after_20261007"
    sweep.mkdir()
    (sweep / "sweep_check_verdicts.json").write_text(json.dumps(
        [{"candidate_election_id": "a", "status": "incomplete"}, {"candidate_election_id": "b", "status": "incomplete"}]))
    pass4 = tmp_path / "pass4_20261010"
    pass4.mkdir()
    (pass4 / "x_url_leads.json").write_text(json.dumps(
        [{"candidate_election_id": "a", "status": "not_found"}, {"candidate_election_id": "c", "status": "incomplete"}]))
    monkeypatch.setattr(release, "DISCOVERY", tmp_path)
    monkeypatch.setattr(release, "REPO", tmp_path)
    assert release.incomplete_checks() == {"b", "c"}


def test_unique_segments_keeps_first_occurrence():
    from scripts.build_mayoral_release import unique_segments
    assert unique_segments("Home#+#About#+#Plan text#+#Home #+#  About#+#Donate") == "Home#+#About#+#Plan text#+#Donate"
    assert unique_segments("") == ""


def test_ledb_name_match():
    assert match_basis("James 'Jim' Kenney", "james 'jim' kenney", False) == "name"
    assert match_basis("Jim Kenney", "james kenney", False) == "name"
    assert match_basis("Marty J Walsh", "martin walsh", False) == "name"
    assert match_basis("Lily Bao", "leilei bao", True) == "last_name"
    assert match_basis("Lily Bao", "leilei bao", False) == ""
    assert match_basis("Joe Adame", "adame", True) == "last_name"
    assert match_basis("John Smith", "jane doe", True) == ""


def test_validator_rejects_split_ledb_contest(tmp_path):
    root = release(tmp_path)
    write(root / "mayoral_ledb_crosswalk.csv", [
        {"candidate_election_id": "f1", "ledb_candid": "7", "ledb_contest": "k1", "ledb_match": "name_same_month"},
        {"candidate_election_id": "f2", "ledb_candid": "8", "ledb_contest": "k2", "ledb_match": "name_same_month"}])
    write_manifest(root)
    assert any("different ALGED contests" in e for e in validate(root)["errors"])


def test_primary_pages_window_and_nearest_copy():
    from scripts.build_mayoral_release import primary_pages
    pool = [{"capture_timestamp": "20250601000000", "text_sha256": "a", "original_url": "https://x.com/"},
            {"capture_timestamp": "20250301000000", "text_sha256": "b", "original_url": "https://www.x.com"},
            {"capture_timestamp": "20250302000000", "text_sha256": "c", "original_url": "https://x.com/issues"},
            {"capture_timestamp": "20250701000000", "text_sha256": "d", "original_url": "https://x.com/later"},
            {"capture_timestamp": "20240101000000", "text_sha256": "e", "original_url": "https://x.com/old"}]
    kept = primary_pages(pool, "2025-06-24")
    # The June copy of the home page wins over the March copy; the July page is after the primary, the 2024 page too early.
    assert [r["text_sha256"] for r in kept] == ["a", "c"]


def test_campaign_periods():
    from scripts.build_mayoral_release import campaign_periods
    people = [{"candidate_election_id": "f1", "election_id": "g1"}, {"candidate_election_id": "f2", "election_id": "g2"}]
    primary = [{"candidate_election_id": "p1", "general_election_id": "g1", "election_date": "2025-06-24"}]
    rows = [{"candidate_election_id": "f1", "capture_timestamp": "20250624120000"},
            {"candidate_election_id": "f1", "capture_timestamp": "20250625000000"},
            {"candidate_election_id": "f2", "capture_timestamp": "20250101000000"},
            {"candidate_election_id": "p1", "capture_timestamp": "20250701000000"}]
    first = {"g1": {"rounds": "two_rounds", "first_round_date": "2025-06-24"}, "g2": {"rounds": "unknown"}}
    assert campaign_periods(rows, people, primary, first) == ["first_round", "final_round", "", "after_primary"]
    first["g2"] = {"rounds": "single_round", "first_round_date": "2025-11-04"}
    assert campaign_periods(rows, people, primary, first)[2] == "single_round"


def test_panel_keeps_one_page_per_text():
    from scripts.build_mayoral_release import panel_pages
    own = [{"page_id": "a1", "text_sha256": "x", "phase": "before", "days_from_election": -200},
           {"page_id": "a2", "text_sha256": "x", "phase": "after", "days_from_election": 5},
           {"page_id": "b1", "text_sha256": "y", "phase": "after", "days_from_election": 30},
           {"page_id": "b2", "text_sha256": "y", "phase": "election_day", "days_from_election": 0},
           {"page_id": "c1", "text_sha256": "z", "phase": "before", "days_from_election": -3}]
    # A pre-election copy wins over a nearer later copy; without one, the copy nearest the election wins; order kept.
    assert [p["page_id"] for p in panel_pages(own)] == ["a1", "b2", "c1"]


def test_validator_rejects_repeated_text_in_panel(tmp_path):
    root = release(tmp_path)
    table = pq.read_table(root / "mayoral_page_corpus.parquet").to_pylist()
    table[1]["text_sha256"] = "a"
    pq.write_table(pa.Table.from_pylist(table), root / "mayoral_page_corpus.parquet")
    write_manifest(root)
    assert any("panel page count differs" in e for e in validate(root)["errors"])
