"""Mayoral page corpus: page rules added on October 16, 2026, and the pre-election queue key."""
from scripts.build_mayoral_page_corpus import flags
from scripts.build_mayoral_prefetch_queue import page_key


def reasons(text, url):
    page = {"original_url": url, "effective_replay_url": ""}
    return flags(text, page, "smith", 2020)[0]


CAMPAIGN = "Jane Smith for Mayor. " * 20


def test_not_a_page_addresses():
    for url in ["https://smithformayor.com/wp-content/x/app.js", "https://smithformayor.com/wp-admin/post.php",
                "https://smithformayor.com/post-sitemap.xml", "https://smithformayor.com/_Incapsula_Resource",
                "https://smithformayor.com/flyer.pdf", "https://smithformayor.com/index.php?option=com_mailto&x=1"]:
        assert "not_a_page" in reasons(CAMPAIGN, url), url
    for url in ["https://smithformayor.com/", "https://smithformayor.com/issues.html", "https://smithformayor.com/index.php"]:
        assert "not_a_page" not in reasons(CAMPAIGN, url), url


def test_bot_check_and_hosting_placeholder():
    assert "error_or_challenge" in reasons("Please wait while your request is being verified...", "https://a.com/about/")
    assert "parked_or_placeholder" in reasons("Affordable, Reliable Web Hosting Solutions. Web Hosting - courtesy of "
                                              "www.bluehost.com. Help Center. Contact Us.", "https://a.com/")
    assert "parked_or_placeholder" not in reasons(CAMPAIGN, "https://smithformayor.com/")


def test_page_key_matches_address_variants():
    assert page_key("http://www.Smith.com:80/issues/") == page_key("https://smith.com/issues") == "smith.com/issues"
    assert page_key("http://smith.com/a#top") == "smith.com/a"


def test_manifest_skips_prefetch_copy_already_fetched(tmp_path, monkeypatch):
    import csv, gzip, json, sys
    from scripts import build_mayoral_page_manifest as manifest
    batch = tmp_path / "b1"
    (batch / "candidate_site_targets.csv").parent.mkdir(parents=True)
    (batch / "candidate_site_targets.csv").write_text("target_id,election_date\nt1,2020-11-03\n")
    columns = ["capture_id", "target_id", "fetch_status", "text_characters", "timestamp", "effective_timestamp",
               "original_url", "effective_replay_url", "text_path", "text_sha256", "raw_html_path", "raw_html_sha256"]
    row = dict(zip(columns, ["c1", "t1", "fetched", "100", "20201001000000", "", "https://a.com/", "", "t", "h", "r", "h"]))
    for run, rows in {"production": [row], "prefetch": [row, {**row, "capture_id": "c2"}]}.items():
        (batch / "runs" / run).mkdir(parents=True)
        with (batch / "runs" / run / "fetch_attempts.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
    (tmp_path / "map.json").write_text(json.dumps({"b1": {"t1": "f1"}}))
    monkeypatch.setattr(manifest, "ACQ", tmp_path)
    monkeypatch.setattr(sys, "argv", ["x", "--map", str(tmp_path / "map.json"), "--output", str(tmp_path / "m.csv.gz")])
    manifest.main()
    with gzip.open(tmp_path / "m.csv.gz", "rt") as handle:
        assert [(r["capture_id"], r["run"]) for r in csv.DictReader(handle)] == [("c1", "production"), ("c2", "prefetch")]


def test_not_a_page_system_folders():
    for url in ["https://a.com/wp-content/themes/born-to-give/", "https://a.com/cdn-cgi/l/email-protection",
                "https://a.com/x/jd.gallery.js.php", "https://a.com/wordpress-resources-at-siteground/",
                "https://a.com/cgi-bin/FormMail.pl"]:
        assert "not_a_page" in reasons(CAMPAIGN, url), url
    assert "not_a_page" not in reasons(CAMPAIGN, "https://a.com/content/plan/")


def test_first_round_copies_are_not_duplicates_of_final_round_copies(tmp_path, monkeypatch):
    import csv
    import gzip
    import hashlib
    import json
    import sys

    import pyarrow.parquet as pq

    from scripts import build_mayoral_page_corpus as corpus

    def page(capture, stamp, body):
        raw = gzip.compress(f"<html><body><p>{body}</p></body></html>".encode())
        path = tmp_path / f"{capture}.html.gz"
        path.write_bytes(raw)
        return {"candidate_election_id": "f1", "target_id": "t1", "capture_id": capture, "batch": "b", "run": "production",
                "registrable_domain": "x.com", "original_url": "https://x.com/issues", "timestamp": stamp,
                "effective_replay_url": "", "effective_timestamp": "", "days_from_election": "", "in_text_window": "",
                "text_path": "", "text_sha256": "", "text_characters": "1", "raw_html_path": str(path),
                "raw_html_sha256": hashlib.sha256(gzip.decompress(raw)).hexdigest()}

    body = "Smith for Mayor. Our plan for housing and safer streets in every neighborhood of the city. " * 3
    rows = [page("c1", "20190201000000", body + "Vote February 26."), page("c2", "20190320000000", body + "Vote April 2."),
            page("c3", "20190110000000", body + "Vote February 26.")]
    manifest = tmp_path / "manifest.csv.gz"
    with gzip.open(manifest, "wt", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    people = tmp_path / "people.csv"
    people.write_text("candidate_election_id,election_id,candidate_name,election_date\nf1,e1,Jane Smith,2019-04-02\n")
    first = tmp_path / "first.csv"
    first.write_text("election_id,rounds,first_round_date\ne1,two_rounds,2019-02-26\n")
    out = tmp_path / "corpus.parquet"
    monkeypatch.setattr(sys, "argv", ["x", "--manifest", str(manifest), "--candidates", str(people), "--output", str(out),
                                      "--first-rounds", str(first)])
    corpus.main()
    dup = {r["page_id"]: r["duplicate_of"] for r in pq.read_table(out).to_pylist()}
    # c1 and c2 share the address but lie on either side of the first round; c3 repeats c1's text in the first round.
    assert dup == {"c1": "", "c2": "", "c3": "c1"}
    assert json.loads(out.with_suffix(".audit.json").read_text())["duplicate_pages"] == 1
