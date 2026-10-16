"""Build the mayoral page corpus from the page manifest: text, counts and page flags (release stages 2 and 3).

Input: the page manifest (scripts/build_mayoral_page_manifest.py), one row per saved page of a credited
campaign site, and the coverage build's candidate table for names and election dates.

For every page the script re-extracts the text from the stored raw HTML with the House/Senate extractor
(src/extract_text.extract_visible_text, segments joined by "#+#"), so both datasets read the same way,
and computes the ICPSR cleaning counts (scripts/icpsr_replicate_coding.icpsr_clean_text). It then sets
rule-based flags. Three flags exclude a page from the finalist panel; the others are information only:

  excluded  parked_or_placeholder  a directory listing, or a page under 1,200 characters that says the domain is
                                   for sale, parked, expired or suspended, or that the site is under construction
  excluded  spam                   a platform user-profile page with posts, or five or more gambling, drug or loan
                                   spam terms on a page that does not name the candidate
  excluded  off_domain             the archive served the page from a different registrable domain
  excluded  empty                  fewer than 50 characters of visible text
  excluded  binary_or_garbled      NUL bytes, or more than 5 percent replacement or control characters (an image or
                                   a broken page served as HTML)
  excluded  error_or_challenge     a page under 1,500 characters that is a bot challenge, an access-denied or a
                                   "page not found" page
  excluded  listing_page           an address that lists other pages: tag, author, category, feed, comments,
                                   search, paging (/page/2/), date archives without a post, calendar views
  excluded  not_a_page             an address of a file that is not a page (script, style, font, image, PDF, XML),
                                   or a platform, admin, theme, plugin, CGI, sitemap, mail-form or bot-check path
  flag      app_shell              fewer than 40 words and a request for JavaScript or a loading message
  flag      other_year_only        the page names years, but not the election year or the year before
  flag      names_candidate        the page names the candidate's last name
  flag      non_latin_text         mostly non-Latin text (the House/Senate CJK rule); campaigns publish such pages
  flag      duplicate_of           a kept page of the same finalist has the same extracted text, or the same
                                   address after removing the scheme, "www.", a trailing slash and the query; only a
                                   page that passes the page rules inside the 365-day window can be the kept page
Days from the election are computed from the finalist's election date (October 10, 2026: the August batches'
target files hold only the election month, which had shifted the August pages by up to two weeks).

The corpus keeps every page with its flags. Nothing is deleted; exclusion is a column.
Run on mac2 from ~/candidate_website_extension_stage3 (the page files live there).
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

csv.field_size_limit(sys.maxsize)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bs4 import BeautifulSoup  # noqa: E402

from scripts.icpsr_replicate_coding import icpsr_clean_text  # noqa: E402
from src.extract_text import classify_page_type, extract_visible_text, is_cjk_spam  # noqa: E402

# Phrases of parked, expired, suspended and placeholder pages. They count only on short pages, because a
# campaign page can mention road construction or an event "coming soon" (checked October 9, 2026).
PARKED = re.compile(
    r"(domain (name )?(is|may be) for sale|buy this domain|this domain is parked|parked free|"
    r"domain parking|hosting account (has been )?suspended|account (has been )?suspended|"
    r"this account has been suspended|website coming soon|site coming soon|under construction|"
    r"future home of|this domain has expired|domain has expired|domain name expired|renew (this|your) domain|"
    r"recently been registered|related searches|sedo\b|web hosting - courtesy of|just another wordpress site|"
    r"welcome to wordpress\. this is your first post)", re.I)
PARKED_MAX_CHARS = 1200
SPAM = re.compile(r"\b(casino|viagra|cialis|payday loans?|slot online|judi|poker online|replica watches|"
                  r"cbd gummies|essay writing service)\b", re.I)
SHELL = re.compile(r"(enable javascript|requires javascript|javascript is (disabled|required)|loading\W{0,3}$|"
                   r"you need to enable javascript)", re.I | re.M)
CHALLENGE = re.compile(
    r"(just a moment|checking your browser|attention required|access denied|verify you are (a )?human|"
    r"ddos protection|403 forbidden|404 not found|error 404|page not found|page cannot be found|"
    r"could not be found|nothing (was )?found|the page you (are looking for|requested)|this page (does not|doesn't) exist|"
    r"website is currently not available|request is being verified)", re.I)
CHALLENGE_MAX_CHARS = 1500
LISTING_PATH = re.compile(r"/(tag|tags|author|category|categories|feed|rss|comments|search)(/|$)|/page/\d+/?$|"
                          r"^/\d{4}(/\d{1,2}){0,2}/?$|/calendar/action|/events/(list|month|day|week|photo)(/|$)", re.I)
LISTING_QUERY = re.compile(r"(^|&)(ajaxcalendar|replytocom|share|s|paged|range|tribe-bar-date|eventdisplay)=", re.I)
# Addresses that are not content pages: files of a non-page type, and platform, admin, sitemap and bot-check paths
# (added October 16, 2026 after the first-user review found such pages among the usable ones).
NOT_A_PAGE = re.compile(r"\.(css|js|mjs|json|xml|txt|ico|png|jpe?g|gif|svg|webp|woff2?|ttf|eot|otf|map|pdf|docx?|zip)$|"
                        r"/(wp-admin|wp-content|wp-includes|wp-login\.php|wp-json|xmlrpc\.php|_Incapsula_Resource|cdn-cgi|cgi-bin)(/|$)|"
                        r"\.js\.php$|/wordpress-resources-at-siteground/?$|sitemap[^/]*$", re.I)
NOT_A_PAGE_QUERY = re.compile(r"(^|&)(option=com_mailto|tmpl=component)", re.I)
YEAR = re.compile(r"\b(20[0-3]\d)\b")
APOSTROPHE = re.compile(r"['’`]")
BOOLEAN = ("in_text_window", "names_candidate", "app_shell", "other_year_only", "non_latin_text", "excluded")
COLUMNS = ["page_id", "candidate_election_id", "target_id", "batch", "run", "site_domain", "original_url",
           "snap_url", "capture_timestamp", "days_from_election", "in_text_window", "page_type", "text",
           "n_char", "n_words", "n_tags", "n_clean_tags", "text_sha256", "raw_html_sha256",
           "names_candidate", "app_shell", "other_year_only", "non_latin_text", "duplicate_of", "excluded",
           "exclusion_reason"]


def registrable(host: str) -> str:
    parts = host.lower().removeprefix("www.").split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host.lower()


def last_name(name: str) -> str:
    words = [w for w in re.split(r"[\s,]+", unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode())
             if w and w.lower().strip(".") not in {"jr", "sr", "ii", "iii", "iv"} and not w.startswith(("(", "\"", "'"))]
    return APOSTROPHE.sub("", words[-1].lower().strip(".")) if words else ""


def address_key(url: str) -> str:
    parts = urlsplit(url)
    return (parts.hostname or "").lower().removeprefix("www.") + (parts.path or "/").rstrip("/").lower()


def flags(text: str, page: dict, last: str, election_year: int) -> tuple[list[str], dict]:
    reasons = []
    eff = re.sub(r"^https?://web\.archive\.org/web/\d{14}(id_)?/", "", page["effective_replay_url"] or "")
    if eff and registrable(urlsplit(eff).hostname or "") != registrable(urlsplit(page["original_url"]).hostname or ""):
        reasons.append("off_domain")
    plain = text.replace("#+#", "\n")
    if len(plain.strip()) < 50:
        reasons.append("empty")
    odd = sum(1 for ch in plain if ch == "\ufffd" or (unicodedata.category(ch) == "Cc" and ch not in "\n\r\t"))
    if page.get("_had_nul") or (plain and odd / len(plain) > 0.05):
        reasons.append("binary_or_garbled")
    if len(plain) < CHALLENGE_MAX_CHARS and CHALLENGE.search(plain):
        reasons.append("error_or_challenge")
    address = urlsplit(page["original_url"])
    if LISTING_PATH.search(address.path or "/") or LISTING_QUERY.search(address.query or ""):
        reasons.append("listing_page")
    if NOT_A_PAGE.search(address.path or "/") or NOT_A_PAGE_QUERY.search(address.query or ""):
        reasons.append("not_a_page")
    directory_listing = plain.lstrip().lower().startswith("index of /")
    if directory_listing or (len(plain) < PARKED_MAX_CHARS and PARKED.search(plain)):
        reasons.append("parked_or_placeholder")
    named = bool(last) and last in APOSTROPHE.sub("", plain.lower())
    # Spam terms count only on pages that do not name the candidate: campaigns discuss casinos as a policy topic.
    # User profile pages of site platforms (Wix "/profile/... Join date") carry spam posts. Non-Latin text is not
    # spam by itself, because campaigns publish pages in Chinese, Spanish and other languages (checked October 9).
    profile_spam = "/profile/" in page["original_url"] and "join date" in plain[:300].lower()
    if profile_spam or (len(SPAM.findall(plain)) >= 5 and not named):
        reasons.append("spam")
    years = {int(y) for y in YEAR.findall(plain)}
    info = {"names_candidate": named,
            "app_shell": len(plain.split()) < 40 and bool(SHELL.search(plain)),
            "other_year_only": bool(years) and not years & {election_year, election_year - 1},
            "non_latin_text": is_cjk_spam(plain)}
    return reasons, info


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--candidates", type=Path, required=True, help="candidate_coverage.csv from the coverage build")
    parser.add_argument("--output", type=Path, required=True, help="parquet file")
    parser.add_argument("--status-id", default="mayoral-page-corpus")
    args = parser.parse_args()
    import pyarrow as pa
    import pyarrow.parquet as pq

    people = {r["candidate_election_id"]: r for r in csv.DictReader(args.candidates.open())}
    pages = list(csv.DictReader(gzip.open(args.manifest, "rt")))
    from datetime import date, datetime
    for page in pages:
        stamp = page["effective_timestamp"] or page["timestamp"]
        election = date.fromisoformat(people[page["candidate_election_id"]]["election_date"])
        page["days_from_election"] = (datetime.strptime(stamp[:8], "%Y%m%d").date() - election).days
    # The first copy of a page wins the duplicate check: copies from before election day come first (nearest the
    # election first), then copies from election day or later (October 16, 2026: pre-election copies fetched).
    pages.sort(key=lambda p: (p["candidate_election_id"], p["days_from_election"] >= 0, abs(p["days_from_election"]),
                              p["capture_id"]))
    _status("set", args.status_id, "--project", "Candidate websites, mayoral extension",
            "--task", "Build the mayoral page corpus with page checks", "--total", str(len(pages)), "--unit", "pages")
    schema = pa.schema([(c, pa.string()) if c not in {"days_from_election", "n_char", "n_words", "n_tags", "n_clean_tags"}
                        else (c, pa.int64()) for c in COLUMNS if c not in BOOLEAN]
                       + [(c, pa.bool_()) for c in BOOLEAN])
    schema = pa.schema([schema.field(c) for c in COLUMNS])
    writer = pq.ParquetWriter(args.output, schema, compression="zstd")
    seen_text: dict[tuple[str, str], str] = {}
    seen_address: dict[tuple[str, str], str] = {}
    counts: Counter = Counter()
    batch = []
    for i, page in enumerate(pages, 1):
        person = people[page["candidate_election_id"]]
        raw = gzip.decompress(Path(page["raw_html_path"]).read_bytes())
        if hashlib.sha256(raw).hexdigest() != page["raw_html_sha256"]:
            raise ValueError(f"raw HTML hash differs for {page['capture_id']}")
        text = extract_visible_text(BeautifulSoup(raw, "html.parser"))
        page["_had_nul"] = "\x00" in text
        text = text.replace("\x00", "")
        clean = icpsr_clean_text(text)
        reasons, info = flags(text, page, last_name(person["candidate_name"]), int(person["election_date"][:4]))
        text_sha = hashlib.sha256(text.encode()).hexdigest()
        # Only a page that passes the page rules inside the text window can be the kept copy, so a duplicate never
        # points to an excluded or out-of-window copy (second first-user review, October 16, 2026).
        text_key = (page["candidate_election_id"], text_sha)
        address = (page["candidate_election_id"], address_key(page["original_url"]))
        dup = seen_text.get(text_key) or seen_address.get(address) or page["capture_id"]
        if dup == page["capture_id"] and not reasons and abs(page["days_from_election"]) <= 365:
            seen_text[text_key] = seen_address[address] = page["capture_id"]
        for r in reasons:
            counts[r] += 1
        counts["pages"] += 1
        stamp = page["effective_timestamp"] or page["timestamp"]
        batch.append({
            "page_id": page["capture_id"], "candidate_election_id": page["candidate_election_id"],
            "target_id": page["target_id"], "batch": page["batch"], "run": page["run"],
            "site_domain": registrable(urlsplit(page["original_url"]).hostname or ""),
            "original_url": page["original_url"], "snap_url": f"https://web.archive.org/web/{stamp}/{page['original_url']}",
            "capture_timestamp": stamp, "days_from_election": page["days_from_election"],
            "in_text_window": abs(page["days_from_election"]) <= 365, "page_type": classify_page_type(page["original_url"]),
            "text": text, "n_char": len(text), "n_words": len(text.split()), "n_tags": clean["n_tags"],
            "n_clean_tags": clean["n_clean_tags"], "text_sha256": text_sha, "raw_html_sha256": page["raw_html_sha256"],
            **info, "duplicate_of": "" if dup == page["capture_id"] else dup,
            "excluded": bool(reasons), "exclusion_reason": ";".join(reasons)})
        if len(batch) == 2000 or i == len(pages):
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            batch = []
            _status("set", args.status_id, "--done", str(i), "--note", f"{i:,} of {len(pages):,} pages processed.")
    writer.close()
    table = pq.read_table(args.output, columns=["excluded", "duplicate_of", "in_text_window", "candidate_election_id"])
    data = table.to_pydict()
    counts["excluded_pages"] = sum(data["excluded"])
    counts["duplicate_pages"] = sum(1 for d in data["duplicate_of"] if d)
    usable = {c for c, e, w, d in zip(data["candidate_election_id"], data["excluded"], data["in_text_window"],
                                      data["duplicate_of"]) if not e and w and not d}
    audit = {"pages": counts["pages"], "excluded_pages": counts["excluded_pages"],
             "exclusion_reasons": {k: v for k, v in counts.items() if k in {"off_domain", "empty", "parked_or_placeholder", "spam",
                                                                     "binary_or_garbled", "error_or_challenge", "listing_page", "not_a_page"}},
             "duplicate_pages": counts["duplicate_pages"], "finalists": len(set(data["candidate_election_id"])),
             "finalists_with_usable_in_window_pages": len(usable)}
    args.output.with_suffix(".audit.json").write_text(json.dumps(audit, indent=1) + "\n")
    _status("set", args.status_id, "--done", str(len(pages)), "--state", "finished",
            "--note", f"{audit['excluded_pages']:,} pages excluded by rule.")
    print(json.dumps(audit, indent=1))


def _status(*args: str) -> None:
    import subprocess
    subprocess.run(["jobstatus", *args], check=False, capture_output=True,
                   env={"PATH": str(Path.home() / ".local/bin") + ":/usr/bin:/bin"})


if __name__ == "__main__":
    main()
