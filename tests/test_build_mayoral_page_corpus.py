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
