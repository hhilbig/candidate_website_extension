#!/usr/bin/env python3
"""Read official contest results from a Clarity Elections results site.

Many county election authorities publish their official results through
Clarity (`results.enr.clarityelections.com`). The public page is a JavaScript
application, but the same site serves a static, versioned summary document
that holds every contest, choice, and vote total.

This reader fetches that document and reports the contests that match a
search term. It reads only what the election authority published. It never
infers a winner, and it never edits a total.

The version string identifies the exact published revision, so a result can be
re-fetched and compared later.
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import urllib.request
import zlib
from typing import Any

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
BASE = "https://results.enr.clarityelections.com"


def _get(url: str, timeout: int = 60) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def _decompress(payload: bytes) -> bytes:
    for decode in (
        gzip.decompress,
        zlib.decompress,
        lambda value: zlib.decompress(value, -15),
    ):
        try:
            return decode(payload)
        except Exception:
            continue
    return payload


def current_version(state: str, county: str, election_id: str) -> str:
    url = f"{BASE}/{state}/{county}/{election_id}/current_ver.txt"
    version = _get(url).decode("utf-8", "replace").strip()
    if not re.fullmatch(r"\d+", version):
        raise ValueError(f"unexpected Clarity version response: {version[:80]!r}")
    return version


def summary(state: str, county: str, election_id: str, version: str) -> dict:
    url = f"{BASE}/{state}/{county}/{election_id}/{version}/json/sum.json"
    return json.loads(_decompress(_get(url)))


def contest_rows(document: dict, pattern: str) -> list[dict[str, Any]]:
    """Return every contest whose name matches, with choices and totals."""
    regex = re.compile(pattern, re.IGNORECASE)
    rows: list[dict[str, Any]] = []
    for contest in document.get("Contests") or []:
        name = str(contest.get("C") or "")
        if not regex.search(name):
            continue
        choices = list(contest.get("CH") or [])
        votes = list(contest.get("V") or [])
        parties = list(contest.get("P") or [])
        entries = [
            {
                "choice": choices[index],
                "party": parties[index] if index < len(parties) else None,
                "votes": int(votes[index]) if index < len(votes) else None,
            }
            for index in range(len(choices))
        ]
        # Report the authority's own ordering and totals. Sorting is only a
        # convenience for reading; the recorded votes are unchanged.
        ranked = sorted(
            (e for e in entries if isinstance(e["votes"], int)),
            key=lambda e: -e["votes"],
        )
        rows.append({
            "contest": name,
            "total_votes_cast_in_contest": contest.get("T"),
            "precincts_reporting": contest.get("PR"),
            "precincts_total": contest.get("TP"),
            "choices": entries,
            "ranked": ranked,
        })
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, help="for example PA")
    parser.add_argument("--county", required=True, help="for example Allegheny")
    parser.add_argument(
        "--election-id", required=True,
        help="the numeric id in the Clarity results URL",
    )
    parser.add_argument(
        "--contest-pattern", required=True,
        help="regular expression matched against the contest name",
    )
    parser.add_argument("--version", help="pin an exact published revision")
    args = parser.parse_args()

    version = args.version or current_version(
        args.state, args.county, args.election_id
    )
    document = summary(args.state, args.county, args.election_id, version)
    rows = contest_rows(document, args.contest_pattern)
    print(json.dumps({
        "source": (
            f"{BASE}/{args.state}/{args.county}/{args.election_id}/"
        ),
        "published_version": version,
        "summary_document": (
            f"{BASE}/{args.state}/{args.county}/{args.election_id}/"
            f"{version}/json/sum.json"
        ),
        "matched_contests": len(rows),
        "contests": rows,
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
