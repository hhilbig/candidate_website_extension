"""Domain-pattern sweep for mayoral finalists without a campaign URL lead.

Stage 2 search agents missed some sites (independent checkers found 11 in about
170 spot checks, one because a host-only query skipped the www host). This script
tries the usual campaign domain patterns for every finalist without a URL lead.
It searches the Wayback CDX index (whole domain, 365 days up to the election) and,
for 2022-2026 elections, the Common Crawl index for crawls in the same window.
A hit only becomes a lead after its page text passes the screen (candidate last
name and "mayor") and a separate check applies the stage 2 acceptance rule.

Commands (run from the repository root, or a copy with the same layout):
  patterns     write sweep/patterns.csv from the coverage build
  wayback      query Wayback CDX for every pattern (resumable)
  commoncrawl  query Common Crawl indexes for 2022-2026 elections (resumable)
  screen       fetch each hit's page text and apply the name/mayor screen (resumable)
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import re
import sys
import time
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

COVERAGE = Path("data/municipal_discovery/top100_coverage/outputs/candidate_coverage.csv")
SWEEP = Path("data/municipal_discovery/top100_coverage/url_discovery/sweep_20261002")
WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"
CC_INDEX = "https://index.commoncrawl.org"
CC_DATA = "https://data.commoncrawl.org/"
USER_AGENT = "CandidateWebsiteExtension/1.0 (academic research; contact: hhilbig@ucdavis.edu)"
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "dr", "md", "phd"}
CC_FIRST_YEAR = 2022


def ascii_word(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", text.lower())


def name_parts(name: str) -> tuple[list[str], str]:
    """First-name variants (formal and nickname) and the last name."""
    nicknames = [ascii_word(n) for n in re.findall(r"[\"“(]([^\"”)]+)[\"”)]", name)]
    plain = re.sub(r"[\"“(][^\"”)]*[\"”)]", " ", name)
    words = [w for w in (ascii_word(t) for t in re.split(r"[\s,]+", plain)) if w and w not in SUFFIXES]
    words = [w for w in words if len(w) > 1]  # drop initials
    if not words:
        raise ValueError(f"cannot parse name: {name!r}")
    last = words[-1]
    firsts = [words[0]] if len(words) > 1 else []
    firsts += [n for n in nicknames if n and n not in firsts]
    return firsts, last


def city_word(city: str) -> str:
    city = re.sub(r"\(.*?\)|\bcity\b|\bmetro government\b|\bUrban\b", " ", city, flags=re.I)
    return ascii_word(re.split(r"[/-]", city.strip())[0])


def domain_patterns(name: str, city: str) -> list[str]:
    firsts, last = name_parts(name)
    town = city_word(city)
    stems = {last + "formayor", last + "4mayor", "mayor" + last, "vote" + last, "elect" + last,
             last + "for" + town, last + town}
    for first in firsts:
        stems |= {first + last, first + last + "formayor", first + last + "4mayor", first + "formayor",
                  "elect" + first + last, "vote" + first + last, first + "for" + town, first + last + "for" + town}
    domains = {stem + ".com" for stem in stems}
    domains |= {stem + ".org" for stem in stems if stem.endswith(("formayor", "for" + town)) or stem in {f + last for f in firsts}}
    return sorted(d for d in domains if len(d) > 6)


def window(election_date: str) -> tuple[date, date]:
    day = date.fromisoformat(election_date)
    return day - timedelta(days=365), day


# ---------------------------------------------------------------- patterns

def cmd_patterns(args) -> None:
    rows = list(csv.DictReader(COVERAGE.open()))
    targets = [r for r in rows if r["eligible"] == "True" and r["url_lead"] != "True"]
    out = []
    for r in targets:
        start, end = window(r["election_date"])
        for domain in domain_patterns(r["candidate_name"], r["city_name"]):
            out.append({"candidate_election_id": r["candidate_election_id"], "candidate_name": r["candidate_name"],
                        "city_name": r["city_name"], "state_abb": r["state_abb"], "election_date": r["election_date"],
                        "window_start": start.isoformat(), "window_end": end.isoformat(), "domain": domain})
    SWEEP.mkdir(parents=True, exist_ok=True)
    write_csv(SWEEP / "patterns.csv", out)
    print(json.dumps({"finalists": len(targets), "pattern_rows": len(out),
                      "unique_domain_windows": len({(o["domain"], o["window_start"]) for o in out})}))


# ---------------------------------------------------------------- shared

def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def done_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    # Failed queries stay retryable: a later run queries them again (last record per key wins).
    latest = {}
    for line in path.open():
        if line.strip():
            record = json.loads(line)
            latest[record["key"]] = record["status"]
    return {key for key, status in latest.items() if status == "ok"}


class Client:
    """One request at a time, a fixed gap, retries with backoff, and a pause after repeated failures."""

    def __init__(self, gap: float, attempts: int = 4):
        import requests
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.gap, self.attempts, self.last, self.failures = gap, attempts, 0.0, 0

    def get(self, url: str, **kwargs):
        error = None
        for attempt in range(self.attempts):
            wait = self.gap - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            try:
                response = self.session.get(url, timeout=(20, 90), **kwargs)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise RuntimeError(f"HTTP {response.status_code}")
                self.failures = 0
                return response
            except Exception as exc:  # network errors and retryable statuses
                error = exc
                self.failures += 1
                if self.failures >= 8:
                    print(f"{datetime.now():%H:%M:%S} pause after {self.failures} failures: {exc}", flush=True)
                    time.sleep(600)
                    self.failures = 0
                else:
                    time.sleep(min(120, 10 * 2 ** attempt))
        raise RuntimeError(f"failed after {self.attempts} attempts: {error}")


def domain_windows(year_min: int = 0, patterns: str = "patterns.csv") -> list[dict]:
    seen, out = set(), []
    for r in csv.DictReader((SWEEP / patterns).open()):
        key = f"{r['domain']}|{r['window_start']}|{r['window_end']}"
        if int(r["election_date"][:4]) >= year_min and key not in seen:
            seen.add(key)
            out.append({"key": key, "domain": r["domain"], "start": r["window_start"], "end": r["window_end"]})
    return out


# ---------------------------------------------------------------- wayback

def cmd_wayback(args) -> None:
    out = SWEEP / "wayback_hits.jsonl"
    done = done_keys(out)
    client = Client(args.gap)
    todo = [w for w in domain_windows() if w["key"] not in done]
    print(f"{datetime.now():%H:%M:%S} wayback: {len(todo)} domain windows to query", flush=True)
    with out.open("a") as handle:
        for i, w in enumerate(todo, 1):
            params = {"url": w["domain"], "matchType": "domain", "from": w["start"].replace("-", ""),
                      "to": w["end"].replace("-", ""), "output": "json", "fl": "timestamp,original,statuscode,mimetype",
                      "filter": ["statuscode:200", "mimetype:text/html"], "collapse": "urlkey", "limit": "50"}
            try:
                rows = client.get(WAYBACK_CDX, params=params).json() or []
                record = {"key": w["key"], "status": "ok", "captures": [dict(zip(rows[0], r)) for r in rows[1:]]}
            except Exception as exc:
                record = {"key": w["key"], "status": "error", "error": str(exc)[:300], "captures": []}
            handle.write(json.dumps(record) + "\n")
            handle.flush()
            if i % 100 == 0:
                print(f"{datetime.now():%H:%M:%S} wayback {i}/{len(todo)}", flush=True)
    print("wayback done", flush=True)


# ---------------------------------------------------------------- common crawl

def crawl_dates(collections: list[dict]) -> list[tuple[str, date]]:
    """Approximate each CC-MAIN-YYYY-WW crawl by the Monday of its ISO week."""
    out = []
    for c in collections:
        match = re.fullmatch(r"CC-MAIN-(\d{4})-(\d{2})", c["id"])
        if match:
            year, week = map(int, match.groups())
            out.append((c["id"], date.fromisocalendar(year, week, 1)))
    return out


def cmd_commoncrawl(args) -> None:
    out = SWEEP / "commoncrawl_hits.jsonl"
    done = done_keys(out)
    client = Client(args.gap)
    crawls = crawl_dates(client.get(CC_INDEX + "/collinfo.json").json())
    todo = []
    for w in domain_windows(CC_FIRST_YEAR, args.patterns):
        start, end = date.fromisoformat(w["start"]), date.fromisoformat(w["end"])
        # A crawl runs for about two weeks after its start; include crawls that began up to 14 days before the window.
        # Only the latest --max-crawls crawls before the election are queried (the CC index answers in about 8 s).
        in_window = sorted((began, crawl) for crawl, began in crawls if start - timedelta(days=14) <= began <= end)
        for began, crawl in in_window[-args.max_crawls:]:
            key = w["key"] + "|" + crawl
            if key not in done:
                todo.append((key, w["domain"], crawl))
    print(f"{datetime.now():%H:%M:%S} commoncrawl: {len(todo)} domain-crawl queries", flush=True)
    with out.open("a") as handle:
        for i, (key, domain, crawl) in enumerate(todo, 1):
            params = {"url": domain, "matchType": "domain", "output": "json", "limit": "50",
                      "filter": ["=status:200", "~mime:text/html"]}
            try:
                response = client.get(f"{CC_INDEX}/{crawl}-index", params=params)
                lines = [] if response.status_code == 404 else response.text.splitlines()
                record = {"key": key, "status": "ok", "captures": [json.loads(l) for l in lines if l.startswith("{")]}
            except Exception as exc:
                record = {"key": key, "status": "error", "error": str(exc)[:300], "captures": []}
            handle.write(json.dumps(record) + "\n")
            handle.flush()
            if i % 100 == 0:
                print(f"{datetime.now():%H:%M:%S} commoncrawl {i}/{len(todo)}", flush=True)
    print("commoncrawl done", flush=True)


# ---------------------------------------------------------------- screen

def page_text(html: bytes) -> tuple[str, str]:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return title, re.sub(r"\s+", " ", soup.get_text(" ", strip=True))


def warc_body(record: bytes) -> bytes:
    """HTTP body from one gzip-compressed WARC response record."""
    raw = gzip.decompress(record)
    _, _, http = raw.partition(b"\r\n\r\n")
    _, _, body = http.partition(b"\r\n\r\n")
    return body


def screen_text(title: str, text: str, last: str) -> tuple[bool, str]:
    folded = ascii_word_spaces(title + " " + text)
    passed = last in folded and "mayor" in folded
    at = folded.find("mayor")
    return passed, (title + " | " + text[max(0, at - 150): at + 150]) if at >= 0 else title


def ascii_word_spaces(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()


def best_captures(path: Path, source: str) -> list[dict]:
    """Root-page capture preferred; otherwise the shortest URL. One per domain window (and crawl)."""
    out = []
    if not path.exists():
        return out
    for line in path.open():
        record = json.loads(line)
        if not record["captures"]:
            continue
        caps = record["captures"]
        if source == "wayback":
            pick = min(caps, key=lambda c: (len(c["original"]), c["timestamp"]))
            out.append({"key": record["key"], "source": source, "url": pick["original"], "timestamp": pick["timestamp"],
                        "fetch": f"https://web.archive.org/web/{pick['timestamp']}id_/{pick['original']}"})
        else:
            pick = min(caps, key=lambda c: (len(c["url"]), c["timestamp"]))
            out.append({"key": record["key"], "source": source, "url": pick["url"], "timestamp": pick["timestamp"],
                        "fetch": json.dumps({"filename": pick["filename"], "offset": int(pick["offset"]),
                                             "length": int(pick["length"])})})
    return out


def cmd_screen(args) -> None:
    out = SWEEP / "screened.jsonl"
    done = done_keys(out)
    lasts = {}
    for r in csv.DictReader((SWEEP / "patterns.csv").open()):
        key = f"{r['domain']}|{r['window_start']}|{r['window_end']}"
        lasts.setdefault(key, set()).add(name_parts(r["candidate_name"])[1])
    hits = best_captures(SWEEP / "wayback_hits.jsonl", "wayback") + best_captures(SWEEP / "commoncrawl_hits.jsonl", "commoncrawl")
    clients = {"wayback": Client(args.gap), "commoncrawl": Client(1.0)}
    with out.open("a") as handle:
        for hit in hits:
            key = hit["key"] + "|" + hit["source"]
            if key in done:
                continue
            base = "|".join(hit["key"].split("|")[:3])
            try:
                if hit["source"] == "wayback":
                    html = clients["wayback"].get(hit["fetch"]).content
                else:
                    loc = json.loads(hit["fetch"])
                    rng = f"bytes={loc['offset']}-{loc['offset'] + loc['length'] - 1}"
                    html = warc_body(clients["commoncrawl"].get(CC_DATA + loc["filename"], headers={"Range": rng}).content)
                title, text = page_text(html)
                results = {last: screen_text(title, text, last) for last in lasts[base]}
                record = {"key": key, "status": "ok", **{k: v for k, v in hit.items() if k != "key"},
                          "title": title[:200], "chars": len(text),
                          "screen": {last: passed for last, (passed, _) in results.items()},
                          "snippet": next(iter(results.values()))[1][:400]}
            except Exception as exc:
                record = {"key": key, "status": "error", "error": str(exc)[:300], **{k: v for k, v in hit.items() if k != "key"}}
            handle.write(json.dumps(record) + "\n")
            handle.flush()
    print("screen done", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["patterns", "wayback", "commoncrawl", "screen"])
    parser.add_argument("--patterns", default="patterns.csv", help="pattern file in the sweep folder")
    parser.add_argument("--max-crawls", type=int, default=2, help="Common Crawl: latest crawls per window")
    parser.add_argument("--gap", type=float, default=2.0, help="seconds between requests to one service")
    args = parser.parse_args()
    {"patterns": cmd_patterns, "wayback": cmd_wayback, "commoncrawl": cmd_commoncrawl, "screen": cmd_screen}[args.command](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
