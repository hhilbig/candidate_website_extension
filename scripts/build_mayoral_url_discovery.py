#!/usr/bin/env python3
"""Build the large-city mayoral universe and discover campaign URL evidence."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import subprocess
import sys
import tempfile
import time
import zlib
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.mayoral_discovery import (
    build_mayoral_universe,
    choose_near_election_snapshots,
    extract_ballotpedia_campaign_links,
    verify_known_source_file,
)


LOGGER = logging.getLogger(__name__)
USER_AGENT = "CandidateWebsiteExtension/1.0 (academic research; contact via repository)"
CDX_URL = "https://web.archive.org/cdx/search/cdx"
EVIDENCE_COLUMNS = [
    "candidate_year_id", "ledb_candid", "full_name", "geo_name", "state_abb",
    "year", "candidate_source", "candidate_source_doi", "url_source",
    "source_profile_url", "source_snapshot_timestamp", "retrieved_on",
    "evidence_url", "evidence_text", "discovery_method", "confidence",
]
STATUS_COLUMNS = [
    "candidate_year_id", "ledb_candid", "full_name", "geo_name", "state_abb",
    "year", "source_profile_url", "source_status", "source_snapshot_count",
    "source_snapshots_checked", "source_snapshot_timestamps_checked",
    "campaign_urls_found", "error",
]


@contextmanager
def hard_deadline(seconds: float):
    """Enforce a wall-clock deadline around archive requests on Unix."""
    if seconds <= 0 or not hasattr(signal, "setitimer"):
        yield
        return

    def raise_timeout(_signum, _frame):
        raise TimeoutError(f"archive request exceeded {seconds} seconds")

    previous_handler = signal.signal(signal.SIGALRM, raise_timeout)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        signal.signal(signal.SIGALRM, previous_handler)


def write_csv_atomic(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    """Write a stable-schema CSV without leaving a partial checkpoint."""
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = frame.reindex(columns=columns)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", dir=path.parent, delete=False, encoding="utf-8", newline=""
    ) as handle:
        temporary = Path(handle.name)
        normalized.to_csv(handle, index=False)
    temporary.replace(path)


class ArchiveRequestError(RuntimeError):
    """A bounded archive request failed."""


def fetch_text_with_curl(url: str, timeout: int, attempts: int = 2) -> str:
    """Fetch text with two independent wall-clock limits."""
    command = [
        "curl", "--location", "--silent", "--show-error", "--fail",
        "--compressed", "--max-time", str(timeout), "--user-agent", USER_AGENT, url,
    ]
    last_error = "curl request failed"
    for attempt in range(1, attempts + 1):
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                timeout=timeout + 5,
            )
            return result.stdout.decode("utf-8", errors="replace")
        except subprocess.TimeoutExpired:
            last_error = f"curl subprocess exceeded {timeout + 5} seconds"
        except subprocess.CalledProcessError as error:
            last_error = (error.stderr or b"curl request failed").decode(
                "utf-8", errors="replace"
            ).strip()[:500]
        if attempt < attempts:
            time.sleep(3)
    raise ArchiveRequestError(last_error)


def candidate_shard(candidate_year_id: str, shard_count: int) -> int:
    """Assign a candidate-year stably across independent workers."""
    if shard_count < 1:
        raise ValueError("shard_count must be at least 1")
    return zlib.crc32(str(candidate_year_id).encode("utf-8")) % shard_count


def query_profile_snapshots(
    profile_url: str, year: int, timeout: int = 60, attempts: int = 2
) -> list[str]:
    query = urlencode(
        [
            ("url", profile_url), ("from", year), ("to", year + 1),
            ("output", "json"), ("filter", "statuscode:200"),
            ("filter", "mimetype:text/html"), ("fl", "timestamp,original,digest"),
            ("collapse", "digest"),
        ]
    )
    payload = json.loads(fetch_text_with_curl(f"{CDX_URL}?{query}", timeout, attempts))
    if not payload or len(payload) == 1:
        return []
    return [row[0] for row in payload[1:] if row]


def fetch_archived_profile(
    timestamp: str, profile_url: str, timeout: int = 60, attempts: int = 2
) -> str:
    archive_url = f"https://web.archive.org/web/{timestamp}id_/{profile_url}"
    return fetch_text_with_curl(archive_url, timeout, attempts)


def discover_ballotpedia_archive_urls(
    universe: pd.DataFrame,
    delay_seconds: float = 1.0,
    max_profiles: int | None = None,
    output_dir: Path | None = None,
    resume: bool = True,
    recent_first: bool = False,
    timeout: int = 60,
    shard_count: int = 1,
    shard_index: int = 0,
    attempts: int = 2,
    snapshot_limit: int = 3,
    retry_no_url: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Discover campaign links from archived Ballotpedia candidate profiles."""
    profiles = universe[
        universe["in_discovery_scope"] & universe["ballotpedia_url"].notna()
    ].copy()
    profiles = profiles.drop_duplicates(["ledb_candid", "year", "ballotpedia_url"])
    profiles = profiles.sort_values(
        ["year", "state_abb", "geo_name", "full_name"],
        ascending=[not recent_first, True, True, True],
    )
    if not 0 <= shard_index < shard_count:
        raise ValueError("shard_index must be between 0 and shard_count - 1")
    profiles = profiles[
        profiles["candidate_year_id"].map(
            lambda value: candidate_shard(value, shard_count) == shard_index
        )
    ]
    evidence_path = output_dir / "mayoral_url_evidence.csv" if output_dir else None
    status_path = output_dir / "mayoral_source_status.csv" if output_dir else None
    prior_evidence = (
        pd.read_csv(evidence_path) if resume and evidence_path and evidence_path.exists() else pd.DataFrame(columns=EVIDENCE_COLUMNS)
    )
    prior_status = (
        pd.read_csv(
            status_path,
            dtype={
                "candidate_year_id": "string",
                "source_snapshot_timestamps_checked": "string",
            },
        ) if resume and status_path and status_path.exists() else pd.DataFrame(columns=STATUS_COLUMNS)
    )
    previously_checked: dict[str, set[str]] = {}
    if "source_snapshot_timestamps_checked" in prior_status.columns:
        for item in prior_status.itertuples(index=False):
            raw = getattr(item, "source_snapshot_timestamps_checked", "")
            timestamps = set(str(raw).split(" | ")) if pd.notna(raw) and str(raw) else set()
            previously_checked[str(item.candidate_year_id)] = timestamps
    completed_mask = prior_status["source_status"].ne("error")
    if retry_no_url:
        completed_mask &= ~(
            prior_status["source_status"].eq("profile_checked")
            & prior_status["campaign_urls_found"].fillna(0).eq(0)
        )
    completed = set(prior_status.loc[completed_mask, "candidate_year_id"].astype(str))
    profiles = profiles[~profiles["candidate_year_id"].astype(str).isin(completed)]
    if max_profiles is not None:
        profiles = profiles.head(max_profiles)
    pending_ids = set(profiles["candidate_year_id"].astype(str))
    prior_status = prior_status[
        ~prior_status["candidate_year_id"].astype(str).isin(pending_ids)
    ].copy()
    prior_evidence = prior_evidence[
        ~prior_evidence["candidate_year_id"].astype(str).isin(pending_ids)
    ].copy()

    evidence_rows: list[dict] = prior_evidence.to_dict("records")
    status_rows: list[dict] = prior_status.to_dict("records")

    for number, row in enumerate(profiles.itertuples(index=False), start=1):
        prior_timestamps = sorted(
            previously_checked.get(str(row.candidate_year_id), set())
        ) if retry_no_url else []
        status = {
            "candidate_year_id": row.candidate_year_id,
            "ledb_candid": row.ledb_candid,
            "full_name": row.full_name,
            "geo_name": row.geo_name,
            "state_abb": row.state_abb,
            "year": row.year,
            "source_profile_url": row.ballotpedia_url,
            "source_status": "not_attempted",
            "source_snapshot_count": 0,
            "source_snapshots_checked": len(prior_timestamps),
            "source_snapshot_timestamps_checked": " | ".join(prior_timestamps),
            "campaign_urls_found": 0,
            "error": "",
        }
        LOGGER.info("Checking %s/%s: %s (%s, %s)", number, len(profiles), row.full_name, row.geo_name, row.year)
        try:
            timestamps = query_profile_snapshots(
                row.ballotpedia_url, int(row.year), timeout=timeout, attempts=attempts
            )
            status["source_snapshot_count"] = len(timestamps)
            chosen = choose_near_election_snapshots(
                timestamps, int(row.year), row.month, limit=snapshot_limit
            )
            if retry_no_url:
                chosen = [
                    timestamp for timestamp in chosen
                    if timestamp not in previously_checked.get(str(row.candidate_year_id), set())
                ]
            if not timestamps:
                status["source_status"] = "no_archived_profile"
            elif not chosen:
                status["source_status"] = "no_additional_snapshots"
            else:
                status["source_status"] = "profile_checked"
            found_for_candidate: set[str] = set()
            for timestamp in chosen:
                time.sleep(delay_seconds)
                html = fetch_archived_profile(
                    timestamp, row.ballotpedia_url, timeout=timeout, attempts=attempts
                )
                status["source_snapshots_checked"] += 1
                checked = status["source_snapshot_timestamps_checked"].split(" | ")
                checked = [value for value in checked if value]
                checked.append(timestamp)
                status["source_snapshot_timestamps_checked"] = " | ".join(checked)
                for item in extract_ballotpedia_campaign_links(html):
                    url = item["evidence_url"]
                    if url in found_for_candidate:
                        continue
                    found_for_candidate.add(url)
                    evidence_rows.append(
                        {
                            "candidate_year_id": row.candidate_year_id,
                            "ledb_candid": row.ledb_candid,
                            "full_name": row.full_name,
                            "geo_name": row.geo_name,
                            "state_abb": row.state_abb,
                            "year": row.year,
                            "candidate_source": "American Local Government Elections Database",
                            "candidate_source_doi": "10.17605/OSF.IO/MV5E6",
                            "url_source": "Ballotpedia archived profile",
                            "source_profile_url": row.ballotpedia_url,
                            "source_snapshot_timestamp": timestamp,
                            "retrieved_on": date.today().isoformat(),
                            **item,
                        }
                    )
            status["campaign_urls_found"] = len(found_for_candidate)
        except (ArchiveRequestError, requests.RequestException, TimeoutError, ValueError, json.JSONDecodeError) as error:
            status["source_status"] = "error"
            status["error"] = f"{type(error).__name__}: {error}"[:500]
        status_rows.append(status)
        if output_dir:
            write_csv_atomic(pd.DataFrame(evidence_rows), evidence_path, EVIDENCE_COLUMNS)
            write_csv_atomic(pd.DataFrame(status_rows), status_path, STATUS_COLUMNS)
        time.sleep(delay_seconds)
        if number % 25 == 0:
            LOGGER.info("Checked %s/%s archived profiles", number, len(profiles))

    return (
        pd.DataFrame(evidence_rows).reindex(columns=EVIDENCE_COLUMNS),
        pd.DataFrame(status_rows).reindex(columns=STATUS_COLUMNS),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--candidate-file",
        default="data/external/municipal/ledb_candidatelevel.csv",
    )
    parser.add_argument(
        "--city-file",
        default="data/external/municipal/cities_constituency_data.csv",
    )
    parser.add_argument("--output-dir", default="data/municipal_discovery")
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year", type=int, default=2021)
    parser.add_argument("--min-population", type=int, default=100_000)
    parser.add_argument(
        "--stage-annotations", default="config/curated_mayoral_contest_stages.json"
    )
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument("--max-profiles", type=int)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--recent-first", action="store_true")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--attempts", type=int, default=2)
    parser.add_argument("--snapshot-limit", type=int, default=3)
    parser.add_argument("--retry-no-url", action="store_true")
    parser.add_argument("--universe-only", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    candidate_hash = verify_known_source_file(args.candidate_file)
    city_hash = verify_known_source_file(args.city_file)
    stage_hash = verify_known_source_file(args.stage_annotations)

    universe = build_mayoral_universe(
        args.candidate_file,
        args.city_file,
        start_year=args.start_year,
        end_year=args.end_year,
        min_population=args.min_population,
        stage_annotations_path=args.stage_annotations,
    )
    universe.to_csv(output_dir / "mayoral_candidate_universe.csv", index=False)
    metadata = {
        "generated_on": date.today().isoformat(),
        "candidate_source": {
            "name": "American Local Government Elections Database candidate-level CSV",
            "doi": "10.17605/OSF.IO/MV5E6",
            "article_doi": "10.1038/s41597-023-02792-x",
            "license": "CC BY 4.0",
            "path": str(args.candidate_file),
            "sha256": candidate_hash,
        },
        "population_source": {
            "name": "ALGEL cities constituency data with documented Census supplements",
            "path": str(args.city_file),
            "sha256": city_hash,
        },
        "stage_adjudication_source": {
            "path": str(args.stage_annotations),
            "sha256": stage_hash,
            "rule": "Official or explicit publisher evidence takes priority; otherwise stage remains unresolved until a date-matched election-page source is combined.",
        },
        "scope": {
            "office": "Mayor",
            "start_year": args.start_year,
            "end_year": args.end_year,
            "minimum_2020_population": args.min_population,
            "contested_rule": "at least two distinct candidates in the recorded contest",
            "stage_caveat": "ALGEL records one mayoral contest per jurisdiction-year but has no explicit primary/general/runoff field",
        },
        "counts": {
            "mayor_rows": len(universe),
            "candidate_years_in_scope": int(universe["in_discovery_scope"].sum()),
            "contests_in_scope": int(universe.loc[universe["in_discovery_scope"], "contest"].nunique()),
            "jurisdictions_in_scope": int(universe.loc[universe["in_discovery_scope"], "place_fips"].nunique()),
            "missing_population_rows": int(universe["population_2020"].isna().sum()),
            "candidate_years_by_stage_scope_status": {
                str(key): int(value) for key, value in universe.loc[
                    universe["in_discovery_scope"], "stage_scope_status"
                ].value_counts().to_dict().items()
            },
        },
    }
    (output_dir / "mayoral_discovery_metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    LOGGER.info(
        "Universe: %s mayor rows; %s in discovery scope; %s missing population",
        len(universe),
        int(universe["in_discovery_scope"].sum()),
        int(universe["population_2020"].isna().sum()),
    )
    if args.universe_only:
        return

    evidence, status = discover_ballotpedia_archive_urls(
        universe,
        delay_seconds=args.delay_seconds,
        max_profiles=args.max_profiles,
        output_dir=output_dir,
        resume=not args.no_resume,
        recent_first=args.recent_first,
        timeout=args.timeout,
        shard_count=args.shard_count,
        shard_index=args.shard_index,
        attempts=args.attempts,
        snapshot_limit=args.snapshot_limit,
        retry_no_url=args.retry_no_url,
    )
    write_csv_atomic(evidence, output_dir / "mayoral_url_evidence.csv", EVIDENCE_COLUMNS)
    write_csv_atomic(status, output_dir / "mayoral_source_status.csv", STATUS_COLUMNS)
    LOGGER.info(
        "Discovery: %s URL claims for %s candidate-years; %s source errors",
        len(evidence),
        evidence["candidate_year_id"].nunique() if not evidence.empty else 0,
        int(status["source_status"].eq("error").sum()) if not status.empty else 0,
    )


if __name__ == "__main__":
    main()
