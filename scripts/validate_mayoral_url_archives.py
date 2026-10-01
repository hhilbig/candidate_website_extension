#!/usr/bin/env python3
"""Validate discovered campaign URLs using archive metadata, without fetching sites."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_mayoral_url_discovery import ArchiveRequestError, CDX_URL, fetch_text_with_curl, write_csv_atomic
from src.mayoral_discovery import site_root_url


VALIDATION_COLUMNS = [
    "candidate_year_id", "candidate_contest_id", "contest", "full_name",
    "geo_name", "state_abb", "year", "month",
    "validated_campaign_url", "campaign_host", "archive_query_status",
    "archive_query_urls", "validation_method_version",
    "archive_capture_count", "nearest_capture_timestamp", "nearest_capture_days",
    "temporal_validation_status", "error",
]

VALIDATION_METHOD_VERSION = "exact_root_and_html_site_wildcard_v4"

VALIDATION_KEY_COLUMNS = ["candidate_year_id", "validated_campaign_url"]

MERGE_STATUS_PRIORITY = {
    "error": 0,
    "not_attempted_missing_election_month": 1,
    "checked_with_partial_error": 2,
    "checked": 3,
}


def campaign_archive_query_urls(url: str) -> list[str]:
    """Query the cited path, homepage, and any archived subpage on the site."""
    root = site_root_url(url)
    return list(dict.fromkeys(value for value in [url, root, f"{root}*" if root else ""] if value))


def query_campaign_url_captures(url: str, year: int, timeout: int, attempts: int) -> list[str]:
    query = urlencode([
        ("url", url), ("from", year - 1), ("to", year + 1), ("output", "json"),
        ("filter", "statuscode:200"), ("filter", "mimetype:text/html"),
        ("fl", "timestamp,original,digest"),
        ("collapse", "digest"),
    ])
    payload = json.loads(fetch_text_with_curl(f"{CDX_URL}?{query}", timeout, attempts))
    if not payload or len(payload) == 1:
        return []
    return [row[0] for row in payload[1:] if row]


def nearest_capture(timestamps: list[str], year: int, month: int) -> tuple[str, int | None]:
    target = datetime(year, month, 15)
    parsed = []
    for timestamp in set(timestamps):
        try:
            capture = datetime.strptime(timestamp[:14], "%Y%m%d%H%M%S")
        except (TypeError, ValueError):
            continue
        parsed.append((abs((capture - target).days), timestamp))
    if not parsed:
        return "", None
    days, timestamp = min(parsed)
    return timestamp, days


def _validate_target(row, timeout: int, attempts: int) -> dict:
    result = {
        "candidate_year_id": row.candidate_year_id,
        "candidate_contest_id": row.candidate_contest_id,
        "contest": row.contest,
        "full_name": row.full_name,
        "geo_name": row.geo_name,
        "state_abb": row.state_abb,
        "year": row.year,
        "month": int(row.month) if not pd.isna(row.month) else pd.NA,
        "validated_campaign_url": row.validated_campaign_url,
        "campaign_host": row.campaign_host,
        "archive_query_status": "not_attempted",
        "archive_query_urls": " | ".join(
            campaign_archive_query_urls(row.validated_campaign_url)
        ),
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "archive_capture_count": 0,
        "nearest_capture_timestamp": "",
        "nearest_capture_days": pd.NA,
        "temporal_validation_status": "unresolved",
        "error": "",
    }
    if pd.isna(row.month):
        result["archive_query_status"] = "not_attempted_missing_election_month"
        result["temporal_validation_status"] = "missing_election_month"
        result["error"] = "Recorded election month is missing; no month was imputed"
        return result
    try:
        timestamps = []
        query_errors = []
        successful_queries = 0
        for query_url in campaign_archive_query_urls(row.validated_campaign_url):
            try:
                timestamps.extend(query_campaign_url_captures(
                    query_url, int(row.year), timeout, attempts
                ))
                successful_queries += 1
            except (ArchiveRequestError, ValueError, json.JSONDecodeError) as error:
                query_errors.append(
                    f"{query_url}: {type(error).__name__}: {error}"[:500]
                )
        if not successful_queries:
            raise ArchiveRequestError(" | ".join(query_errors))
        nearest, days = nearest_capture(timestamps, int(row.year), result["month"])
        result["archive_capture_count"] = len(timestamps)
        result["nearest_capture_timestamp"] = nearest
        result["nearest_capture_days"] = days if days is not None else pd.NA
        result["archive_query_status"] = (
            "checked_with_partial_error" if query_errors else "checked"
        )
        result["error"] = " | ".join(query_errors)[:500]
        if days is None:
            result["temporal_validation_status"] = "no_archive_capture_in_year_window"
        elif days <= 365:
            result["temporal_validation_status"] = "archive_capture_within_365_days"
        else:
            result["temporal_validation_status"] = "archive_capture_outside_365_days"
    except (ArchiveRequestError, ValueError, json.JSONDecodeError) as error:
        result["archive_query_status"] = "error"
        result["error"] = f"{type(error).__name__}: {error}"[:500]
    return result


def archive_validation_targets(
    evidence: pd.DataFrame,
    universe: pd.DataFrame,
) -> pd.DataFrame:
    """Return the unique current evidence keys and their contest context."""
    context_columns = ["candidate_year_id", "candidate_contest_id", "contest", "month"]
    missing_context = [column for column in context_columns if column not in universe.columns]
    if missing_context:
        raise ValueError(f"Universe missing archive context columns: {missing_context}")
    context = universe.sort_values("candidate_contest_id").drop_duplicates("candidate_year_id")[
        context_columns
    ]
    targets = evidence[
        evidence["validation_status"].eq("valid_source_backed_campaign_url")
    ].drop(columns=[
        column for column in ["candidate_contest_id", "contest", "month"]
        if column in evidence.columns
    ]).merge(context, on="candidate_year_id", how="left", validate="many_to_one")
    targets = targets.drop_duplicates(VALIDATION_KEY_COLUMNS)
    month_values = pd.to_numeric(targets["month"], errors="coerce")
    invalid_month = month_values.notna() & ~month_values.between(1, 12)
    if invalid_month.any():
        identifiers = targets.loc[invalid_month, "candidate_year_id"].astype(str).tolist()
        raise ValueError(f"Invalid election month for archive targets: {identifiers[:5]}")
    targets["month"] = month_values.astype("Int64")
    return targets


def _validation_keys(frame: pd.DataFrame) -> set[tuple[str, str]]:
    return set(zip(
        frame["candidate_year_id"].astype(str),
        frame["validated_campaign_url"].astype(str),
    ))


def validation_audit(
    validation: pd.DataFrame,
    evidence: pd.DataFrame,
    universe: pd.DataFrame,
    output_path: Path,
) -> dict:
    """Summarize archive-validation coverage and remaining retryable rows."""
    target_keys = _validation_keys(archive_validation_targets(evidence, universe))
    validation_keys = _validation_keys(validation)

    def counts(column: str) -> dict[str, int]:
        values = validation[column].fillna("<missing>").astype(str).value_counts()
        return {key: int(values[key]) for key in sorted(values.index)}

    duplicate_rows = int(validation.duplicated(VALIDATION_KEY_COLUMNS, keep=False).sum())
    return {
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "output_path": str(output_path),
        "row_count": int(len(validation)),
        "key_count": int(len(validation_keys)),
        "evidence_key_count": int(len(target_keys)),
        "duplicate_key_rows": duplicate_rows,
        "missing_evidence_keys": int(len(target_keys - validation_keys)),
        "unexpected_validation_keys": int(len(validation_keys - target_keys)),
        "exact_evidence_key_agreement": (
            validation_keys == target_keys
            and len(validation) == len(validation_keys)
        ),
        "archive_query_status_counts": counts("archive_query_status"),
        "temporal_validation_status_counts": counts("temporal_validation_status"),
        "retryable_rows": int(validation["archive_query_status"].isin({
            "error", "checked_with_partial_error",
        }).sum()),
    }


def write_json_atomic(payload: dict, path: Path) -> None:
    """Write deterministic JSON without exposing a partial audit file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", dir=path.parent, delete=False, encoding="utf-8"
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temporary.replace(path)


def merge_validation_csvs(
    evidence: pd.DataFrame,
    universe: pd.DataFrame,
    validation_paths: list[Path],
    output_path: Path,
) -> pd.DataFrame:
    """Merge offline v4 checkpoints and require exact agreement with current evidence.

    A later input wins only after status, populated nearest capture, and capture
    count are tied. Each input may cover a shard of the target keys, but it may
    not contain duplicate or unexpected keys. The union must cover every target.
    """
    if not validation_paths:
        raise ValueError("At least one archive validation CSV is required")

    targets = archive_validation_targets(evidence, universe)
    target_keys = _validation_keys(targets)
    frames = []
    for source_order, path in enumerate(validation_paths):
        frame = pd.read_csv(path)
        missing_columns = [column for column in VALIDATION_COLUMNS if column not in frame.columns]
        if missing_columns:
            raise ValueError(f"Archive validation file {path} missing columns: {missing_columns}")
        if frame[VALIDATION_KEY_COLUMNS].isna().any(axis=None):
            raise ValueError(f"Archive validation file {path} contains a missing key")
        duplicate_keys = frame.duplicated(VALIDATION_KEY_COLUMNS, keep=False)
        if duplicate_keys.any():
            examples = frame.loc[duplicate_keys, VALIDATION_KEY_COLUMNS].head(5).to_dict("records")
            raise ValueError(f"Archive validation file {path} contains duplicate keys: {examples}")

        methods = set(frame["validation_method_version"].dropna().astype(str))
        if (
            methods != {VALIDATION_METHOD_VERSION}
            or frame["validation_method_version"].isna().any()
        ):
            raise ValueError(
                f"Archive validation file {path} is not exclusively v4: {sorted(methods)}"
            )
        statuses = set(frame["archive_query_status"].dropna().astype(str))
        unsupported_statuses = statuses - set(MERGE_STATUS_PRIORITY)
        if unsupported_statuses or frame["archive_query_status"].isna().any():
            raise ValueError(
                f"Archive validation file {path} has unsupported statuses: "
                f"{sorted(unsupported_statuses)}"
            )

        unexpected = _validation_keys(frame) - target_keys
        if unexpected:
            raise ValueError(
                f"Archive validation/evidence key mismatch in {path}: "
                f"unexpected={len(unexpected)}"
            )
        frame = frame.reindex(columns=VALIDATION_COLUMNS).copy()
        frame["_source_order"] = source_order
        frame["_status_priority"] = frame["archive_query_status"].map(MERGE_STATUS_PRIORITY)
        frame["_has_nearest_capture"] = (
            frame["nearest_capture_timestamp"].notna()
            & frame["nearest_capture_timestamp"].astype(str).str.strip().ne("")
        ).astype(int)
        frame["_capture_count_priority"] = pd.to_numeric(
            frame["archive_capture_count"], errors="coerce"
        ).fillna(-1)
        frames.append(frame)

    combined = pd.concat(frames, ignore_index=True)
    combined_keys = _validation_keys(combined)
    missing = target_keys - combined_keys
    unexpected = combined_keys - target_keys
    if missing or unexpected:
        raise ValueError(
            "Archive validation/evidence key mismatch: "
            f"missing={len(missing)}, unexpected={len(unexpected)}"
        )

    ranking_columns = [
        *VALIDATION_KEY_COLUMNS,
        "_status_priority",
        "_has_nearest_capture",
        "_capture_count_priority",
        "_source_order",
    ]
    merged = (
        combined.sort_values(ranking_columns, kind="mergesort")
        .drop_duplicates(VALIDATION_KEY_COLUMNS, keep="last")
        .sort_values(VALIDATION_KEY_COLUMNS, kind="mergesort")
        .reindex(columns=VALIDATION_COLUMNS)
        .reset_index(drop=True)
    )
    result_keys = _validation_keys(merged)
    if result_keys != target_keys or len(merged) != len(target_keys):
        raise ValueError(
            "Merged archive validation/evidence key mismatch: "
            f"missing={len(target_keys - result_keys)}, "
            f"unexpected={len(result_keys - target_keys)}, "
            f"duplicate_rows={len(merged) - len(result_keys)}"
        )
    write_csv_atomic(merged, output_path, VALIDATION_COLUMNS)
    return merged


def validate_urls(
    evidence: pd.DataFrame,
    universe: pd.DataFrame,
    output_path: Path,
    timeout: int = 20,
    attempts: int = 2,
    delay_seconds: float = 1,
    require_key_equality: bool = True,
    workers: int = 1,
) -> pd.DataFrame:
    if workers < 1:
        raise ValueError("workers must be at least 1")
    targets = archive_validation_targets(evidence, universe)
    target_keys = _validation_keys(targets)
    prior = pd.read_csv(output_path) if output_path.exists() else pd.DataFrame(columns=VALIDATION_COLUMNS)
    for column in VALIDATION_COLUMNS:
        if column not in prior:
            prior[column] = pd.NA
    current_method = prior["validation_method_version"].eq(VALIDATION_METHOD_VERSION)
    prior_keys = list(zip(
        prior["candidate_year_id"].astype(str),
        prior["validated_campaign_url"].astype(str),
    ))
    prior = prior.loc[
        current_method & pd.Series(
            [key in target_keys for key in prior_keys], index=prior.index
        )
    ].drop_duplicates(
        ["candidate_year_id", "validated_campaign_url"], keep="last"
    )
    completed_statuses = {"checked"}
    completed = set(
        zip(
            prior.loc[prior["archive_query_status"].isin(completed_statuses), "candidate_year_id"].astype(str),
            prior.loc[prior["archive_query_status"].isin(completed_statuses), "validated_campaign_url"].astype(str),
        )
    )
    rows = prior.to_dict("records")
    write_csv_atomic(pd.DataFrame(rows), output_path, VALIDATION_COLUMNS)
    pending = []
    for row in targets.itertuples(index=False):
        key = (str(row.candidate_year_id), str(row.validated_campaign_url))
        if key in completed:
            continue
        pending.append((key, row))

    def persist(key, result):
        nonlocal rows
        rows = [
            item for item in rows
            if (
                str(item["candidate_year_id"]),
                str(item["validated_campaign_url"]),
            ) != key
        ]
        rows.append(result)
        write_csv_atomic(pd.DataFrame(rows), output_path, VALIDATION_COLUMNS)

    if workers == 1:
        for key, row in pending:
            persist(key, _validate_target(row, timeout, attempts))
            time.sleep(delay_seconds)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_keys = {
                executor.submit(_validate_target, row, timeout, attempts): key
                for key, row in pending
            }
            for future in as_completed(future_keys):
                persist(future_keys[future], future.result())
                time.sleep(delay_seconds)
    result = pd.DataFrame(rows).reindex(columns=VALIDATION_COLUMNS)
    if require_key_equality:
        result_keys = set(zip(
            result["candidate_year_id"].astype(str),
            result["validated_campaign_url"].astype(str),
        ))
        if result_keys != target_keys or len(result) != len(target_keys):
            raise ValueError(
                "Archive validation/evidence key mismatch: "
                f"missing={len(target_keys - result_keys)}, "
                f"unexpected={len(result_keys - target_keys)}, "
                f"duplicate_rows={len(result) - len(result_keys)}"
            )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/municipal_discovery/final")
    parser.add_argument(
        "--merge-input", action="append", default=[], metavar="CSV",
        help=(
            "Offline merge mode: add one v4 validation CSV per occurrence. "
            "The ordered inputs are tie-broken in favor of the last file."
        ),
    )
    parser.add_argument(
        "--output", help="Output CSV (defaults to INPUT_DIR/mayoral_url_archive_validation.csv)"
    )
    parser.add_argument(
        "--audit-output", help="Optional JSON audit written after live validation or offline merge"
    )
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--attempts", type=int, default=2)
    parser.add_argument("--delay-seconds", type=float, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--allow-key-mismatch", action="store_true")
    args = parser.parse_args()
    directory = Path(args.input_dir)
    output = Path(args.output) if args.output else directory / "mayoral_url_archive_validation.csv"
    evidence = pd.read_csv(directory / "mayoral_url_evidence.csv")
    universe = pd.read_csv(directory / "mayoral_candidate_universe.csv", low_memory=False)
    if args.merge_input:
        if args.allow_key_mismatch:
            parser.error("--allow-key-mismatch is not permitted with --merge-input")
        result = merge_validation_csvs(
            evidence,
            universe,
            [Path(path) for path in args.merge_input],
            output,
        )
    else:
        result = validate_urls(
            evidence,
            universe,
            output,
            timeout=args.timeout,
            attempts=args.attempts,
            delay_seconds=args.delay_seconds,
            require_key_equality=not args.allow_key_mismatch,
            workers=args.workers,
        )
    if args.audit_output:
        write_json_atomic(
            validation_audit(result, evidence, universe, output),
            Path(args.audit_output),
        )
    print(result["temporal_validation_status"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
