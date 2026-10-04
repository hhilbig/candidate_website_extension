#!/usr/bin/env python3
"""Build the index of Maricopa County official election result documents.

The county publishes an Election Results Archives page. The page looks like a
JavaScript application, but the year list is a plain query parameter and the
server returns complete HTML for each year. This script reads those pages and
writes one index of every published result document.

The index gives the document name and its `/asset/` path. It does not download
the documents. Those paths sit behind a bot challenge, so only a real browser
session reaches them. Use the browser for the documents themselves and this
script for the index, which stays reproducible.

The five in-scope cities in this county are Chandler, Gilbert, Glendale,
Phoenix and Scottsdale. Mesa publishes its own results and is already complete.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

BASE = "https://elections.maricopa.gov"
PAGE = BASE + "/results-and-data/historic-results.html"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
LINK = re.compile(
    r'href="(/asset/[^"]+\.(?:txt|pdf))"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
TAGS = re.compile(r"<[^>]+>")


def fetch_year(year: int, timeout: int = 60) -> str:
    result = subprocess.run(
        ["curl", "-sS", "--max-time", str(timeout), "-A", USER_AGENT,
         f"{PAGE}?year={year}"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout


def documents(html: str) -> list[dict]:
    rows = []
    for href, label in LINK.findall(html):
        name = TAGS.sub("", label).strip()
        if name:
            rows.append({"name": name, "path": href})
    return rows


def build(first: int, last: int) -> dict:
    index = {}
    for year in range(first, last + 1):
        rows = documents(fetch_year(year))
        index[str(year)] = rows
        print(f"{year}: {len(rows)} documents")
    return {
        "version": "maricopa_official_results_index_v1",
        "source_page": PAGE,
        "note": (
            "Paths are relative to " + BASE + ". A plain fetch of an /asset/ "
            "path returns 403 because of a bot challenge. Read the documents "
            "through a browser session."
        ),
        "years": index,
        "document_count": sum(len(v) for v in index.values()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first-year", type=int, default=2010)
    parser.add_argument("--last-year", type=int, default=2026)
    parser.add_argument(
        "--out", type=Path,
        default=Path("quality_reports/mayoral_maricopa_results_index.json"),
    )
    args = parser.parse_args()
    index = build(args.first_year, args.last_year)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(index, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {args.out} with {index['document_count']} documents")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
