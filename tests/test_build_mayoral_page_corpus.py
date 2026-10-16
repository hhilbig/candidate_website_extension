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
