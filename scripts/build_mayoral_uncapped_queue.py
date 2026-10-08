"""Build the fetch queue for the pages a mayoral site scrape left out because of its page cap.

Hanno decided on October 8, 2026 to collect every page the Wayback Machine holds for each
campaign site in the scrape window, not only the first 150. The capture inventory of a finished
batch already lists every archived HTML page, so no new CDX query is needed. This script keeps
the scraper's own rule (one capture per distinct archived page, the one nearest the election),
removes the pages the capped run already queued, and drops URLs that are not content pages:

  asset          the path ends in a file type that is not a page (.css, .js, .json, images, fonts)
  system_path    the path is a CMS or platform internal path (wp-content, wp-json, Wix internals, feeds)
  token_path     a path segment is longer than 100 characters (session or account tokens)
  loop_or_template  a crawler loop or an unfilled link template: a path segment repeats, the path holds a
                 template placeholder ({...}, gtm.js), a quoted URL, a search-results path, or more than 8 levels
  query_variant  the URL differs from a kept page only by query parameters that carry no content
                 (tracking, calendar views, directory sorting, replies, search), or by a trailing /embed/

The inventory and the dropped rows stay on disk, with counts in the audit. The output is a new
run directory with fetch_queue.csv, which `scrape_mayoral_wayback_batch.py fetch --run-dir` reads.

Usage:
  python scripts/build_mayoral_uncapped_queue.py --batch-dir <batch> [--allowed-targets ids.json] [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit

csv.field_size_limit(sys.maxsize)

ASSET = re.compile(r"\.(css|js|mjs|json|xml|txt|ico|png|jpe?g|gif|svg|webp|woff2?|ttf|eot|otf|map|rss|atom|zip|mp[34]|mov)$", re.I)
SYSTEM = re.compile(r"/(wp-content|wp-includes|wp-json|wp-admin|xmlrpc\.php|feed|comments/feed|_api|_partials|"
                    r"wix-thunderbolt|_files|cdn-cgi|static/js|static/css|sbbi|htsrv|wp-login\.php|login\.php|cart|checkout|my-account|"
                    r"forms/shares)(/|$)", re.I)
NON_CONTENT_KEYS = re.compile(r"^(utm_.*|fbclid|gclid|mc_cid|mc_eid|replytocom|share|amp|instance_id|time|yr|month|dy|cid|"
                              r"mcat|ical|outlook-ical|tribe-bar-date|eventdisplay|c|o|s|ver|lang|format|output|nocache|"
                              r"_ga|ref|source|from|range|date|view|tribe_eventcategory|recruiter_id|switch_lang|ajaxcalendar|add-to-cart|"
                              r"orderby|product_orderby|_c|redirect_to|phpsessid|sid|sessionid)$", re.I)
LOOP = re.compile(r"(%7B|\{|%22|gtm\.js|gtm\.start|siteassets|parastorage|/search/|/page/\d+/page/|"
                  r"/product-page/(fit|fill|crop|version)/)", re.I)


def classify(page_identity: str) -> tuple[str, str]:
    """Return (reason or '', the page key after removing non-content query parameters)."""
    parts = urlsplit(page_identity)
    path = parts.path or "/"
    if ASSET.search(path):
        return "asset", ""
    if SYSTEM.search(path):
        return "system_path", ""
    if any(len(segment) > 100 for segment in path.split("/")):
        return "token_path", ""
    segments = [segment for segment in path.split("/") if segment]
    if LOOP.search(path) or len(segments) > 8 or len(segments) != len(set(segments)):
        return "loop_or_template", ""
    if path.endswith("/embed/"):
        path = path[: -len("embed/")]
    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not NON_CONTENT_KEYS.match(k)]
    return "", path + ("?" + urlencode(kept) if kept else "")


def select(captures: list[dict], queued: set[tuple[str, str]]) -> tuple[list[dict], Counter]:
    """One capture per target and page key, nearest the election; skip pages the capped run queued."""
    counts: Counter = Counter()
    best: dict[tuple[str, str], dict] = {}
    for row in captures:
        if (row["target_id"], row["page_identity"]) in queued:
            continue
        reason, key = classify(row["page_identity"])
        if reason:
            counts[reason] += 1
            continue
        slot = (row["target_id"], key)
        rank = (int(row["capture_days_absolute"]), row["timestamp"], row["capture_id"])
        if slot not in best or rank < best[slot][0]:
            best[slot] = (rank, row)
    queued_keys = {(t, classify(p)[1]) for t, p in queued}
    out = []
    for slot, (_, row) in sorted(best.items(), key=lambda item: (item[0][0], item[1][0])):
        if slot in queued_keys:
            counts["query_variant"] += 1
            continue
        out.append(row)
    return out, counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-dir", type=Path, required=True)
    parser.add_argument("--source-run", default="production")
    parser.add_argument("--output-run", default="uncapped")
    parser.add_argument("--allowed-targets", type=Path,
                        help="JSON list of target_ids to include: only campaign sites the coverage build credits")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    source = args.batch_dir / "runs" / args.source_run
    with (source / "fetch_queue.csv").open(newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames
        queue = list(reader)
    queued = {(r["target_id"], r["page_identity"]) for r in queue}
    allowed = set(json.loads(args.allowed_targets.read_text())) if args.allowed_targets else None
    with (source / "capture_inventory.csv").open(newline="") as handle:
        captures = [r for r in csv.DictReader(handle) if allowed is None or r["target_id"] in allowed]
    if allowed is not None:
        queue = [r for r in queue if r["target_id"] in allowed]
    unique = {(r["target_id"], r["page_identity"]) for r in captures}
    rows, dropped = select(captures, queued)
    # query_variant counts every inventory row whose page key repeats a kept page; report unique pages too.
    audit = {
        "batch": args.batch_dir.name, "source_run": args.source_run,
        "allowed_targets": len(allowed) if allowed is not None else "all",
        "inventory_rows": len(captures), "unique_pages": len(unique), "already_queued_pages": len(queued),
        "uncapped_pages_before_filter": len(unique - queued), "uncapped_pages_selected": len(rows),
        "dropped_inventory_rows_by_reason": dict(dropped),
        "targets_with_new_pages": len({r["target_id"] for r in rows}),
        "filter_drop_share_of_uncapped_pages": round(1 - len(rows) / max(1, len(unique - queued)), 3),
    }
    print(json.dumps(audit, indent=1))
    if args.dry_run:
        return
    out_dir = args.batch_dir / "runs" / args.output_run
    out_dir.mkdir(parents=True, exist_ok=True)
    by_target: Counter = Counter()
    with (out_dir / "fetch_queue.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            by_target[row["target_id"]] += 1
            path = urlsplit(row["page_identity"]).path
            writer.writerow({**{c: row.get(c, "") for c in columns},
                             "path_depth": len([p for p in path.split("/") if p]),
                             "target_queue_rank": by_target[row["target_id"]],
                             "selection_reason": "uncapped_nearest_election_capture_per_content_page",
                             "max_pages_per_target": ""})
    (out_dir / "uncapped_queue_audit.json").write_text(json.dumps(audit, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
