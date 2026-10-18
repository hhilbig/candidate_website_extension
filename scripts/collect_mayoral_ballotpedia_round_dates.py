"""Read the round dates of mayoral elections from archived Ballotpedia election pages (v6 campaign_period flag).

Ballotpedia has one page per mayoral election of the largest cities, for example
"Mayoral_election_in_Seattle,_Washington_(2017)", with fixed fields such as "Primary election: August 1, 2017" and
"General election: November 7, 2017". For each city the script lists the archived pages whose address starts with
"ballotpedia.org/Mayoral_election_in_<City>" (one Wayback CDX query), reads the latest 200 capture of each page,
and records every "<Primary|General|Runoff> election: <date>" field. Approved by Hanno on October 18, 2026 as the
script step before any agent search. Standard library only, so it runs on the droplet (the archive limits the home
network after heavy traffic).

Input:  a JSON list of {"city_id", "ballotpedia_city"} (the city name as Ballotpedia writes it, without the state)
Output: JSON lines, one per page: city_id, page, year, snapshot, fields [{"round", "date"}], or an error

Usage:
  python3 collect_mayoral_ballotpedia_round_dates.py --cities cities.json --output ballotpedia_rounds.jsonl
"""
from __future__ import annotations

import argparse
import html
import json
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime

MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
FIELD = re.compile(rf"\b(Primary|General|Runoff|Primary runoff|General runoff) election:\s*(({MONTHS}) \d{{1,2}}, \d{{4}})")
YEAR = re.compile(r"\((\d{4})\)$")


def get(url: str, tries: int = 4) -> str:
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "research-archive-reader"}),
                                        timeout=90) as response:
                body = response.read().decode("utf-8", "replace")
            if "Temporarily Offline" in body[:3000]:
                raise OSError("archive offline")
            return body
        except Exception:  # noqa: BLE001 - network errors are retried, then raised
            if attempt == tries - 1:
                raise
            time.sleep(30 * (attempt + 1))
    return ""


def pages_for(city: str) -> dict[str, str]:
    """Latest 200 capture timestamp of each archived page whose title starts with the city's election prefix."""
    prefix = "ballotpedia.org/Mayoral_election_in_" + city.replace(" ", "_")
    query = urllib.parse.urlencode({"url": prefix, "matchType": "prefix", "filter": "statuscode:200",
                                    "fl": "original,timestamp", "output": "json", "limit": "5000"})
    rows = json.loads(get("https://web.archive.org/cdx/search/cdx?" + query) or "[]")[1:]
    latest = {}
    for original, stamp in rows:
        title = urllib.parse.unquote(original.split("ballotpedia.org/", 1)[1].split("?")[0].split("#")[0])
        if YEAR.search(title) and stamp > latest.get(title, ""):
            latest[title] = stamp
    return latest


def fields(page_html: str) -> list[dict]:
    text = re.sub(r"<script.*?</script>|<style.*?</style>", " ", page_html, flags=re.S)
    text = html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text)))
    out, seen = [], set()
    for match in FIELD.finditer(text):
        item = (match.group(1).lower(), datetime.strptime(match.group(2), "%B %d, %Y").strftime("%Y-%m-%d"))
        if item not in seen:
            seen.add(item)
            out.append({"round": item[0], "date": item[1]})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cities", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    with open(args.output, "a") as out:
        for city in json.load(open(args.cities)):
            try:
                pages = pages_for(city["ballotpedia_city"])
            except Exception as error:  # noqa: BLE001
                out.write(json.dumps({"city_id": city["city_id"], "error": f"cdx: {error}"}) + "\n")
                continue
            for title, stamp in sorted(pages.items()):
                snapshot = f"https://web.archive.org/web/{stamp}/https://ballotpedia.org/{urllib.parse.quote(title)}"
                row = {"city_id": city["city_id"], "page": title, "year": int(YEAR.search(title).group(1)),
                       "snapshot": snapshot}
                try:
                    row["fields"] = fields(get(snapshot))
                except Exception as error:  # noqa: BLE001
                    row["error"] = f"page: {error}"
                out.write(json.dumps(row) + "\n")
                out.flush()
                time.sleep(2)
            time.sleep(2)


if __name__ == "__main__":
    main()
