#!/usr/bin/env python3
"""Validate the corrected, extended release across all public products.

The checks are deliberately cross-product rather than schema-only. They
reconstruct selected candidate-year text from raw pages, reconstruct daily
coverage/timing/digests, re-aggregate the seven ICPSR-compatible measures, and
verify that provenance explains every universe row without inventing evidence
about unobserved collection attempts.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.build_panel import collapse_candidate  # noqa: E402


DATA_FILES = {
    "raw_corpus.parquet",
    "panel_candidate_year.csv",
    "panel_icpsr_compat.csv",
    "candidate_crosswalk.csv",
    "release_roster.csv",
    "candidate_year_universe.csv",
    "candidate_year_provenance.csv",
    "panel_snapshot_day.parquet",
}

# Frozen after the approved 2026-08-22 adjudications and extension build.
EXPECTED_CANDIDATE_YEARS = 7_348
EXPECTED_UNIVERSE_ROWS = 15_041
EXPECTED_BALLOT_CANDIDATES = 3_032
EXPECTED_RAW_ROWS = 797_554
EXPECTED_SNAPSHOT_ROWS = 299_185
EXPECTED_ALL_SCOPE_DAYS = 74_355

INVALID_CAPTURE_IDS = {
    "H4OR06013-2024",
    "S4MN00023-2024",
    "H0MI07135-2020",
}
UNIVERSE_EXCLUDED_IDS = {"H2TN92014-2022", "H4TN57013-2024"}
DAVID_YOUNG_ID = "S4IA00103-2014"
DAVID_YOUNG_LAST_VALID_DAY = "20140105"

PAGE_TYPES = (
    "homepage",
    "issues",
    "biography",
    "news",
    "endorsements",
    "constituent_services",
    "action",
    "other",
)
PAGE_TYPE_ORDER = {value: index for index, value in enumerate(PAGE_TYPES)}
PAGE_SCOPES = {"all", *PAGE_TYPES}
MEASURE_COLUMNS = (
    "icpsr_n_char",
    "icpsr_n_words",
    "icpsr_n_tags",
    "icpsr_n_clean_tags",
    "icpsr_ttr_approx",
    "icpsr_mattr_approx",
    "icpsr_entropy_approx",
)
OBSERVABILITY_STATUSES = {
    "captured_usable_text",
    "captured_thin_text",
    "captured_empty_text",
    "valid_url_no_capture_unknown_reason",
    "invalid_or_placeholder_url",
    "legacy_no_url_record",
    "invalid_capture_excluded",
}
MEASURE_STATUSES = {
    "observed",
    "observed_zero",
    "insufficient_clean_text",
    "scope_not_observed",
    "not_computed",
}
HEX64 = re.compile(r"[0-9a-f]{64}")
WAYBACK_TARGET = re.compile(r"/web/[0-9A-Za-z_*]+/(https?://.+)$")
PROVENANCE_WAYBACK_TARGET = re.compile(
    r"^https?://web\.archive\.org/web/\d+(?:[a-z_]+)?/(.+)$",
    flags=re.IGNORECASE,
)


def _check(condition: bool, message: str) -> None:
    if not bool(condition):
        raise AssertionError(message)


def _require_columns(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = columns - set(frame.columns)
    _check(not missing, f"{label} is missing columns: {sorted(missing)}")


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        low_memory=False,
        dtype={
            "candidate_cycle_id": "string",
            "cand_id": "string",
            "source_cand_id": "string",
        },
    )


def _as_bool(values: pd.Series, label: str) -> pd.Series:
    """Return strict nullable booleans without treating text as truthy."""
    mapping = {
        "true": True,
        "t": True,
        "1": True,
        "yes": True,
        "false": False,
        "f": False,
        "0": False,
        "no": False,
    }
    result = values.map(
        lambda value: pd.NA
        if value is None or pd.isna(value) or str(value).strip() == ""
        else mapping.get(str(value).strip().lower(), "INVALID")
    )
    invalid = result.eq("INVALID").fillna(False)
    _check(not invalid.any(), f"{label} contains non-boolean values")
    return result.astype("boolean")


def _clean_string(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _timestamp_string(value: Any) -> str:
    text = _clean_string(value).removesuffix(".0")
    _check(
        bool(re.fullmatch(r"[0-9]{8,14}", text)),
        f"invalid Wayback timestamp {value!r}",
    )
    try:
        dt.datetime.strptime(text[:8], "%Y%m%d")
    except ValueError as exc:
        raise AssertionError(f"invalid Wayback day {text[:8]}") from exc
    return text


def _provenance_scalar_equal(column: str, actual: Any, expected: Any) -> bool:
    if column in {
        "first_capture_timestamp",
        "last_capture_timestamp",
        "first_capture_day",
        "last_capture_day",
    }:
        return _timestamp_string(actual) == _timestamp_string(expected)
    return _clean_string(actual) == _clean_string(expected)


def _election_day(year: int) -> int:
    day = dt.date(int(year), 11, 1)
    while day.weekday() != 1:
        day += dt.timedelta(days=1)
    if day.day == 1:
        day += dt.timedelta(days=7)
    return int(day.strftime("%Y%m%d"))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _page_url(snap_url: Any, urlkey: Any) -> str | None:
    for value in (snap_url, urlkey):
        text = _clean_string(value)
        if not text:
            continue
        match = WAYBACK_TARGET.search(text)
        return match.group(1) if match else text
    return None


def _snapshot_host(snap_url: Any, urlkey: Any) -> str | None:
    for value in (snap_url, urlkey):
        text = _clean_string(value)
        if not text:
            continue
        match = WAYBACK_TARGET.search(text)
        if match:
            text = match.group(1)
        parsed = urlsplit(text if "://" in text else f"//{text}")
        if parsed.hostname:
            return parsed.hostname.lower().rstrip(".")
    return None


def _normalize_provenance_url(value: Any) -> str | None:
    raw = _clean_string(value)
    if not raw:
        return None
    match = PROVENANCE_WAYBACK_TARGET.match(raw)
    if match:
        raw = match.group(1)
    parsed_value = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlsplit(parsed_value)
    except ValueError:
        return None
    host = (parsed.hostname or "").lower().strip(".")
    host = re.sub(r"^www\d*\.", "", host)
    if not host:
        return None
    try:
        port_value = parsed.port
    except ValueError:
        port_value = None
    port = f":{port_value}" if port_value else ""
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), host + port, path, parsed.query, ""))


def _provenance_page_url(snap_url: Any, urlkey: Any) -> str | None:
    return _normalize_provenance_url(snap_url) or _normalize_provenance_url(urlkey)


def _row_fingerprint(row: pd.Series, text_hash: str) -> str:
    urlkey = row.get("urlkey")
    payload = [
        _timestamp_string(row["date"]),
        str(row["page_type"]),
        _page_url(row.get("snap_url"), row.get("urlkey")) or "",
        "" if urlkey is None or pd.isna(urlkey) else str(urlkey),
        text_hash,
        int(row["n_char"]),
        int(row["n_words"]),
    ]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _list_tuple(value: Any) -> tuple[str, ...]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ()
    if isinstance(value, np.ndarray):
        value = value.tolist()
    return tuple(str(item) for item in value)


def validate_topics(coded: pd.DataFrame) -> None:
    """Validate coded topic support without freezing a missingness count."""
    base = [
        column
        for column in coded
        if column.startswith("icpsr_topic_") and not column.endswith("_home")
    ]
    home = [
        column
        for column in coded
        if column.startswith("icpsr_topic_") and column.endswith("_home")
    ]
    _check(
        len(base) == len(home) == 31,
        "coded panel must contain 31 base and 31 homepage topics",
    )
    for label, columns, support_columns in (
        ("all-site", base, ["icpsr_n_char", "icpsr_n_words"]),
        ("homepage", home, ["icpsr_n_char_home", "icpsr_n_words_home"]),
    ):
        _require_columns(coded, set(support_columns), "coded panel")
        partial = coded[support_columns].notna().nunique(axis=1).ne(1)
        _check(not partial.any(), f"{label} character/word support is partially missing")
        support = coded[support_columns].notna().all(axis=1)
        _check(
            coded.loc[support, columns].notna().all(axis=None),
            f"{label} topic vector is missing despite supported text",
        )
        _check(
            coded.loc[~support, columns].isna().all(axis=None),
            f"{label} topic vector exists without supported text",
        )
        values = coded.loc[support, columns].apply(pd.to_numeric, errors="coerce")
        _check(np.isfinite(values.to_numpy()).all(), f"{label} topics contain non-finite values")
        _check(((values >= 0) & (values <= 1)).all(axis=None), f"{label} topics fall outside [0, 1]")
        _check(
            np.allclose(values.sum(axis=1), 1, atol=1e-8, rtol=0),
            f"{label} topics do not sum to one",
        )


def _validate_geography(
    frame: pd.DataFrame,
    label: str,
    *,
    captured_only: pd.Series | None = None,
) -> None:
    _require_columns(frame, {"office", "district"}, label)
    office = frame["office"].astype("string").str.lower()
    district = pd.to_numeric(frame["district"], errors="coerce")
    _check(
        district.loc[office.eq("senate")].isna().all(),
        f"{label} has a nonmissing Senate district",
    )
    house = office.eq("house")
    if captured_only is not None:
        house &= captured_only.fillna(False)
    _check(district.loc[house].notna().all(), f"{label} has a missing captured House district")
    _check(district.loc[house].between(0, 53).all(), f"{label} has a captured House district outside 0--53")


def _scope_summary(scope: pd.DataFrame, n_pages_all: int) -> dict[str, Any]:
    timestamps = [_timestamp_string(value) for value in scope["date"]]
    texts = scope["text_snap_content"].fillna("").astype(str)
    hashes = [_text_sha256(text) for text in texts]
    fingerprints = [
        _row_fingerprint(row, text_hash)
        for (_, row), text_hash in zip(scope.iterrows(), hashes)
    ]
    urls = {
        value
        for value in (
            _page_url(row.get("snap_url"), row.get("urlkey"))
            for _, row in scope.iterrows()
        )
        if value
    }
    hosts = {
        value
        for value in (
            _snapshot_host(row.get("snap_url"), row.get("urlkey"))
            for _, row in scope.iterrows()
        )
        if value
    }
    page_types = sorted(
        scope["page_type"].astype(str).unique(), key=PAGE_TYPE_ORDER.__getitem__
    )
    return {
        "first_capture_timestamp": min(timestamps),
        "last_capture_timestamp": max(timestamps),
        "n_capture_timestamps": len(set(timestamps)),
        "n_pages_observed": len(scope),
        "n_pages_all_scopes": n_pages_all,
        "n_distinct_urls": len(urls),
        "n_distinct_page_hashes": len(set(hashes)),
        "n_page_types": len(page_types),
        "page_types_present": tuple(page_types),
        "source_hosts": tuple(sorted(hosts)),
        "page_hashes_digest": _text_sha256("\n".join(sorted(hashes))),
        "day_digest_sha256": _text_sha256("\n".join(sorted(fingerprints))),
        "raw_n_char_total": int(pd.to_numeric(scope["n_char"]).sum()),
        "raw_n_words_total": int(pd.to_numeric(scope["n_words"]).sum()),
    }


def _raw_provenance_summary(frame: pd.DataFrame) -> dict[str, Any]:
    timestamps = sorted({_timestamp_string(value) for value in frame["date"]})
    days = sorted({value[:8] for value in timestamps})
    work = frame.assign(_day=frame["date"].map(_timestamp_string).str[:8])
    day_counts = work.groupby("_day").size()
    texts = frame["text_snap_content"].fillna("").astype(str)
    hashes = [_text_sha256(text) for text in texts]
    urls = {
        value
        for value in (
            _provenance_page_url(row.get("snap_url"), row.get("urlkey"))
            for _, row in frame.iterrows()
        )
        if value
    }
    hosts = {(urlsplit(url).hostname or "").lower() for url in urls}
    first_day, last_day = days[0], days[-1]
    span = (
        dt.datetime.strptime(last_day, "%Y%m%d")
        - dt.datetime.strptime(first_day, "%Y%m%d")
    ).days
    election = _election_day(int(frame["year"].iloc[0]))
    raw_days = work["_day"].astype(int)
    runaway_pages = int(day_counts.loc[day_counts.gt(100)].sum())
    return {
        "n_observed_snapshot_timestamps": len(timestamps),
        "n_observed_snapshot_days": len(days),
        "first_capture_timestamp": timestamps[0],
        "last_capture_timestamp": timestamps[-1],
        "first_capture_day": first_day,
        "last_capture_day": last_day,
        "capture_span_days": span,
        "n_raw_page_rows": len(frame),
        "n_distinct_page_urls": len(urls),
        "n_distinct_page_texts": len(set(hashes)),
        "page_text_digest_sha256": _text_sha256("\n".join(sorted(hashes))),
        "n_page_types_observed": frame["page_type"].nunique(),
        "page_types_observed": ";".join(sorted(frame["page_type"].astype(str).unique())),
        "n_source_hosts": len(hosts),
        "source_hosts": ";".join(sorted(hosts)),
        "max_raw_pages_one_day": int(day_counts.max()),
        "n_runaway_days": int(day_counts.gt(100).sum()),
        "share_raw_pages_on_runaway_days": runaway_pages / len(frame),
        "n_homepage_raw_page_rows": int(frame["page_type"].eq("homepage").sum()),
        "n_preelection_raw_page_rows": int(raw_days.le(election).sum()),
    }


def _validate_raw(
    path: Path,
    panel: pd.DataFrame,
    captured_ids: set[str],
) -> tuple[dict[tuple[str, int, str], dict[str, Any]], pd.DataFrame]:
    raw = pq.ParquetFile(path)
    _check(
        raw.metadata.num_rows == EXPECTED_RAW_ROWS,
        f"raw corpus has {raw.metadata.num_rows:,} rows; expected {EXPECTED_RAW_ROWS:,}",
    )
    required = {
        "candidate_cycle_id", "year", "date", "page_type", "n_char",
        "n_words", "n_tags", "n_clean_tags", "n_snap", "text_snap_content",
        "urlkey", "snap_url", "office", "district",
    }
    missing = required - set(raw.schema_arrow.names)
    _check(not missing, f"raw corpus is missing columns: {sorted(missing)}")

    panel_index = panel.set_index("candidate_cycle_id")
    seen: set[str] = set()
    reconstructed: list[dict[str, Any]] = []
    expected_snapshot: dict[tuple[str, int, str], dict[str, Any]] = {}
    provenance_rows: list[dict[str, Any]] = []
    total_rows = 0

    for group_index in range(raw.num_row_groups):
        frame = raw.read_row_group(group_index).to_pandas()
        if frame.empty:
            continue
        total_rows += len(frame)
        ids = frame["candidate_cycle_id"].dropna().astype(str).unique()
        _check(len(ids) == 1, f"raw row group {group_index} contains {len(ids)} candidate IDs")
        candidate_id = ids[0]
        _check(candidate_id not in seen, f"{candidate_id} is split across raw row groups")
        _check(candidate_id in captured_ids, f"raw candidate {candidate_id} is not in captured panels")
        seen.add(candidate_id)

        text = frame["text_snap_content"].fillna("").astype(str)
        _check(
            pd.to_numeric(frame["n_char"], errors="coerce").eq(text.map(len)).all(),
            f"{candidate_id}: raw n_char does not reconstruct from text",
        )
        _check(
            pd.to_numeric(frame["n_char"], errors="coerce").gt(0).all(),
            f"{candidate_id}: raw corpus contains an empty page row",
        )
        _check(
            pd.to_numeric(frame["n_words"], errors="coerce")
            .eq(text.map(lambda value: len(value.split()))).all(),
            f"{candidate_id}: raw n_words does not reconstruct from text",
        )
        tags = pd.to_numeric(frame["n_tags"], errors="coerce")
        clean_tags = pd.to_numeric(frame["n_clean_tags"], errors="coerce")
        _check(tags.notna().all() and tags.ge(1).all(), f"{candidate_id}: invalid n_tags")
        _check(
            clean_tags.notna().all() and clean_tags.ge(0).all() and clean_tags.le(tags).all(),
            f"{candidate_id}: invalid n_clean_tags",
        )
        timestamps = frame["date"].map(_timestamp_string)
        expected_n_snap = timestamps.nunique()
        n_snap = pd.to_numeric(frame["n_snap"], errors="coerce")
        _check(
            n_snap.notna().all() and n_snap.eq(expected_n_snap).all(),
            f"{candidate_id}: n_snap is not the post-adjudication distinct full timestamp count",
        )
        if candidate_id == DAVID_YOUNG_ID:
            _check(expected_n_snap == 1, "David Young must retain exactly one full Wayback timestamp")
        _check(
            set(frame["page_type"].dropna().astype(str)).issubset(PAGE_TYPES)
            and frame["page_type"].notna().all(),
            f"{candidate_id}: unexpected or missing page_type",
        )
        _validate_geography(frame, f"raw corpus row group for {candidate_id}")

        meta = panel_index.loc[candidate_id]
        panel_input = frame.copy()
        for column in ("candidate", "state", "district", "office", "party"):
            panel_input[column] = meta[column]
        rebuilt = collapse_candidate(panel_input, "#+#")
        _check(rebuilt is not None, f"{candidate_id}: selected panel cannot be reconstructed")
        rebuilt["candidate_cycle_id"] = candidate_id
        reconstructed.append(rebuilt)

        days = timestamps.str[:8].astype(int)
        for snap_day, day_frame in frame.groupby(days, sort=True):
            n_all = len(day_frame)
            all_key = (candidate_id, int(snap_day), "all")
            _check(all_key not in expected_snapshot, f"duplicate raw snapshot key {all_key}")
            expected_snapshot[all_key] = _scope_summary(day_frame, n_all)
            for page_type, scope in day_frame.groupby("page_type", sort=False):
                key = (candidate_id, int(snap_day), str(page_type))
                _check(key not in expected_snapshot, f"duplicate raw snapshot key {key}")
                expected_snapshot[key] = _scope_summary(scope, n_all)

        provenance_rows.append({
            "candidate_cycle_id": candidate_id,
            **_raw_provenance_summary(frame),
        })

    _check(total_rows == raw.metadata.num_rows, "raw row-group counts do not sum to metadata")
    _check(
        seen == captured_ids,
        f"raw/panel ID mismatch: {len(captured_ids - seen)} panel-only and {len(seen - captured_ids)} raw-only IDs",
    )
    _check(not (seen & INVALID_CAPTURE_IDS), "an excluded invalid capture remains in raw data")
    _check(not (seen & UNIVERSE_EXCLUDED_IDS), "a universe-excluded candidate remains in raw data")

    rebuilt_frame = pd.DataFrame(reconstructed).set_index("candidate_cycle_id")
    check_columns = [
        "sel_date", "n_snapshots_available", "n_pages", "page_types", "urlkey",
        "text", "n_char", "n_words", "text_quality",
    ]
    expected = panel_index.loc[rebuilt_frame.index, check_columns].copy()
    actual = rebuilt_frame[check_columns].copy()
    expected["sel_date"] = expected["sel_date"].map(_timestamp_string)
    actual["sel_date"] = actual["sel_date"].map(_timestamp_string)
    _assert_frame_values_equal(expected, actual)

    david_days = {
        str(key[1])
        for key in expected_snapshot
        if key[0] == DAVID_YOUNG_ID and key[2] == "all"
    }
    _check(david_days, f"{DAVID_YOUNG_ID} is absent from corrected raw data")
    _check(
        max(david_days) == DAVID_YOUNG_LAST_VALID_DAY,
        f"{DAVID_YOUNG_ID} ends on {max(david_days)}, expected {DAVID_YOUNG_LAST_VALID_DAY}",
    )
    return expected_snapshot, pd.DataFrame(provenance_rows)


def _assert_frame_values_equal(expected: pd.DataFrame, actual: pd.DataFrame) -> None:
    """Compare release values exactly while ignoring pandas storage dtypes."""
    pd.testing.assert_frame_equal(
        expected,
        actual,
        check_dtype=False,
        check_index_type=False,
    )


def _compare_optional_number(actual: Any, expected: Any, label: str) -> None:
    actual_missing = actual is None or pd.isna(actual)
    expected_missing = expected is None or pd.isna(expected)
    if actual_missing or expected_missing:
        _check(actual_missing and expected_missing, f"{label}: {actual!r} != {expected!r}")
        return
    _check(
        np.isclose(float(actual), float(expected), atol=1e-8, rtol=1e-9),
        f"{label}: {actual!r} != {expected!r}",
    )


def _validate_daily_topic_rows(frame: pd.DataFrame, topic_columns: list[str]) -> None:
    status = frame["topic_status"].astype("string")
    unknown = set(status.dropna()) - {"observed", "insufficient_clean_text"}
    _check(not unknown, f"snapshot panel has incomplete/unknown topic statuses: {sorted(unknown)}")
    supported = status.eq("observed")
    _check(
        frame.loc[supported, topic_columns].notna().all(axis=None),
        "snapshot topic vector is missing despite topic_status=observed",
    )
    _check(
        frame.loc[~supported, topic_columns].isna().all(axis=None),
        "snapshot topic vector exists without topic_status=observed",
    )
    if supported.any():
        values = frame.loc[supported, topic_columns].apply(pd.to_numeric, errors="coerce")
        _check(np.isfinite(values.to_numpy()).all(), "snapshot topics contain non-finite values")
        _check(((values >= 0) & (values <= 1)).all(axis=None), "snapshot topics fall outside [0, 1]")
        _check(
            np.allclose(values.sum(axis=1), 1, atol=1e-8, rtol=0),
            "snapshot topic vectors do not sum to one",
        )


def _reaggregate_snapshot_measures(
    rows_by_variant: dict[str, list[tuple[Any, ...]]],
    coded: pd.DataFrame,
    daily_stats: dict[str, dict[str, dict[str, Any]]],
) -> None:
    coded_index = coded.set_index("candidate_cycle_id")
    suffixes = {"all": "", "home": "_home", "preelec": "_preelec"}
    for variant, suffix in suffixes.items():
        columns = ["candidate_cycle_id", *MEASURE_COLUMNS]
        rows = pd.DataFrame(rows_by_variant[variant], columns=columns)
        means = rows.groupby("candidate_cycle_id", sort=False)[list(MEASURE_COLUMNS)].mean()
        for candidate_id, coded_row in coded_index.iterrows():
            for column in MEASURE_COLUMNS:
                output_column = f"{column}{suffix}"
                _check(output_column in coded, f"coded panel lacks {output_column}")
                actual = means.at[candidate_id, column] if candidate_id in means.index else np.nan
                _compare_optional_number(
                    actual,
                    coded_row[output_column],
                    f"{candidate_id} snapshot mean -> {output_column}",
                )
            stats = daily_stats[variant].get(candidate_id, {})
            valid_days = int(stats.get("valid_days", 0))
            valid_pages = int(stats.get("valid_pages", 0))
            for name, expected in (
                (f"icpsr_n_valid_snap{suffix}", valid_days),
                (f"icpsr_n_valid_pages{suffix}", valid_pages),
            ):
                if name not in coded:
                    continue
                expected_value = expected if expected else np.nan
                _compare_optional_number(coded_row[name], expected_value, f"{candidate_id} {name}")

    all_stats = daily_stats["all"]
    for candidate_id, coded_row in coded_index.iterrows():
        stats = all_stats[candidate_id]
        max_pages = int(stats["max_pages"])
        total_pages = int(stats["total_pages"])
        runaway_pages = int(stats["runaway_pages"])
        _compare_optional_number(
            coded_row["icpsr_max_pages_1day"], max_pages,
            f"{candidate_id} icpsr_max_pages_1day",
        )
        _compare_optional_number(
            coded_row["icpsr_share_pages_runaway"], runaway_pages / total_pages,
            f"{candidate_id} icpsr_share_pages_runaway",
        )
        flag = _as_bool(pd.Series([coded_row["icpsr_runaway_flag"]]), "icpsr_runaway_flag").iloc[0]
        _check(bool(flag) == (max_pages > 100), f"{candidate_id}: icpsr_runaway_flag disagrees with daily pages")
        first_day, last_day = stats["first_day"], stats["last_day"]
        span = (
            dt.datetime.strptime(str(last_day), "%Y%m%d")
            - dt.datetime.strptime(str(first_day), "%Y%m%d")
        ).days
        _compare_optional_number(coded_row["icpsr_first_snap_day"], first_day, f"{candidate_id} first snapshot day")
        _compare_optional_number(coded_row["icpsr_last_snap_day"], last_day, f"{candidate_id} last snapshot day")
        _compare_optional_number(coded_row["icpsr_snap_span_days"], span, f"{candidate_id} snapshot span")


def _validate_snapshot(
    path: Path,
    expected_rows: dict[tuple[str, int, str], dict[str, Any]],
    coded: pd.DataFrame,
    captured_ids: set[str],
) -> None:
    parquet = pq.ParquetFile(path)
    _check(
        parquet.metadata.num_rows == EXPECTED_SNAPSHOT_ROWS,
        f"snapshot panel has {parquet.metadata.num_rows:,} rows; expected {EXPECTED_SNAPSHOT_ROWS:,}",
    )
    required = {
        "candidate_cycle_id", "year", "snap_day", "page_scope",
        "first_capture_timestamp", "last_capture_timestamp", "n_capture_timestamps",
        "election_day", "days_to_election", "pre_election", "post_election",
        "n_pages_observed", "n_pages_all_scopes", "n_clean_valid_pages",
        "n_distinct_urls", "n_distinct_page_hashes", "n_page_types",
        "page_types_present", "source_hosts", "page_hashes_sha256",
        "day_digest_sha256", "runaway_day", "raw_n_char_total",
        "raw_n_words_total", "clean_n_char_total", "clean_n_words_total",
        "clean_n_tags_total", "clean_n_clean_tags_total", "measure_status",
        "topic_status", *MEASURE_COLUMNS,
    }
    missing = required - set(parquet.schema_arrow.names)
    _check(not missing, f"snapshot panel is missing columns: {sorted(missing)}")
    topic_columns = [column for column in parquet.schema_arrow.names if column.startswith("day_topic_")]
    _check(len(topic_columns) == 31, f"snapshot panel has {len(topic_columns)} daily topics, expected 31")

    # The raw-derived dictionary is no longer needed after this pass. Popping
    # in place bounds peak memory while also detecting duplicate output keys.
    remaining = expected_rows
    all_count = 0
    snapshot_ids: set[str] = set()
    all_totals: dict[tuple[str, int], tuple[int, ...]] = {}
    detail_totals: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0] * 8)
    rows_by_variant: dict[str, list[tuple[Any, ...]]] = {"all": [], "home": [], "preelec": []}
    daily_stats: dict[str, dict[str, dict[str, Any]]] = {
        "all": defaultdict(dict),
        "home": defaultdict(dict),
        "preelec": defaultdict(dict),
    }

    for batch in parquet.iter_batches(batch_size=25_000):
        frame = batch.to_pandas()
        _validate_daily_topic_rows(frame, topic_columns)
        scopes = set(frame["page_scope"].dropna().astype(str))
        _check(scopes.issubset(PAGE_SCOPES), f"snapshot panel has unknown scopes: {sorted(scopes - PAGE_SCOPES)}")
        measure_status = frame["measure_status"].astype("string")
        _check(
            set(measure_status.dropna()).issubset({"observed", "insufficient_clean_text"}),
            "snapshot panel has unknown measure_status",
        )
        clean_pages = pd.to_numeric(frame["n_clean_valid_pages"], errors="coerce")
        observed_pages = pd.to_numeric(frame["n_pages_observed"], errors="coerce")
        total_columns = [
            "raw_n_char_total", "raw_n_words_total", "clean_n_char_total",
            "clean_n_words_total", "clean_n_tags_total",
            "clean_n_clean_tags_total",
        ]
        totals_numeric = frame[total_columns].apply(pd.to_numeric, errors="coerce")
        _check(
            clean_pages.notna().all()
            and observed_pages.notna().all()
            and clean_pages.ge(0).all()
            and clean_pages.le(observed_pages).all(),
            "snapshot clean-page counts fall outside [0, n_pages_observed]",
        )
        _check(
            totals_numeric.notna().all(axis=None)
            and totals_numeric.ge(0).all(axis=None),
            "snapshot totals contain missing or negative values",
        )
        _check(
            totals_numeric["clean_n_clean_tags_total"]
            .le(totals_numeric["clean_n_tags_total"]).all(),
            "snapshot clean-tag totals exceed tag totals",
        )
        _check(
            (measure_status.eq("observed").to_numpy() == clean_pages.gt(0).to_numpy()).all(),
            "snapshot measure_status disagrees with n_clean_valid_pages",
        )
        unsupported = measure_status.eq("insufficient_clean_text")
        _check(
            frame.loc[unsupported, list(MEASURE_COLUMNS)].isna().all(axis=None),
            "unsupported snapshot measures are not entirely missing",
        )

        for row in frame.to_dict("records"):
            candidate_id = str(row["candidate_cycle_id"])
            snap_day = int(row["snap_day"])
            page_scope = str(row["page_scope"])
            key = (candidate_id, snap_day, page_scope)
            expected = remaining.pop(key, None)
            _check(expected is not None, f"snapshot key is duplicate or absent from raw: {key}")
            snapshot_ids.add(candidate_id)
            if page_scope == "all":
                all_count += 1

            for column in (
                "n_capture_timestamps", "n_pages_observed", "n_pages_all_scopes",
                "n_distinct_urls", "n_distinct_page_hashes", "n_page_types",
                "raw_n_char_total", "raw_n_words_total",
            ):
                _check(
                    int(row[column]) == int(expected[column]),
                    f"{key}: {column}={row[column]!r}, expected {expected[column]!r}",
                )
            for column in ("first_capture_timestamp", "last_capture_timestamp", "day_digest_sha256"):
                _check(str(row[column]) == str(expected[column]), f"{key}: {column} does not reconstruct from raw")
            _check(_list_tuple(row["page_types_present"]) == expected["page_types_present"], f"{key}: page_types_present does not reconstruct from raw")
            _check(_list_tuple(row["source_hosts"]) == expected["source_hosts"], f"{key}: source_hosts does not reconstruct from raw")
            page_hashes = _list_tuple(row["page_hashes_sha256"])
            _check(len(page_hashes) == int(row["n_pages_observed"]), f"{key}: page-hash count differs from pages")
            _check(all(HEX64.fullmatch(value) for value in page_hashes), f"{key}: invalid page digest")
            _check(
                _text_sha256("\n".join(sorted(page_hashes))) == expected["page_hashes_digest"],
                f"{key}: page hashes do not reconstruct from raw text",
            )
            _check(bool(HEX64.fullmatch(str(row["day_digest_sha256"]))), f"{key}: invalid day digest")

            election = _election_day(int(row["year"]))
            snapshot_date = dt.datetime.strptime(str(snap_day), "%Y%m%d").date()
            election_date = dt.datetime.strptime(str(election), "%Y%m%d").date()
            days_to_election = (election_date - snapshot_date).days
            _check(int(row["election_day"]) == election, f"{key}: wrong election_day")
            _check(int(row["days_to_election"]) == days_to_election, f"{key}: wrong days_to_election")
            pre = bool(_as_bool(pd.Series([row["pre_election"]]), "pre_election").iloc[0])
            post = bool(_as_bool(pd.Series([row["post_election"]]), "post_election").iloc[0])
            _check(pre == (days_to_election >= 0) and post == (days_to_election < 0), f"{key}: inconsistent pre/post flags")
            runaway = bool(_as_bool(pd.Series([row["runaway_day"]]), "runaway_day").iloc[0])
            _check(runaway == (int(row["n_pages_all_scopes"]) > 100), f"{key}: inconsistent runaway flag")

            day_key = (candidate_id, snap_day)
            totals = (
                int(row["n_pages_observed"]),
                int(row["n_clean_valid_pages"]),
                int(row["raw_n_char_total"]),
                int(row["raw_n_words_total"]),
                int(row["clean_n_char_total"]),
                int(row["clean_n_words_total"]),
                int(row["clean_n_tags_total"]),
                int(row["clean_n_clean_tags_total"]),
            )
            if page_scope == "all":
                all_totals[day_key] = totals
            else:
                detail_totals[day_key] = [
                    current + value for current, value in zip(detail_totals[day_key], totals)
                ]

            values = tuple(row[column] for column in MEASURE_COLUMNS)
            if page_scope == "all":
                rows_by_variant["all"].append((candidate_id, *values))
                if pre:
                    rows_by_variant["preelec"].append((candidate_id, *values))
            if page_scope == "homepage":
                rows_by_variant["home"].append((candidate_id, *values))

            variants = []
            if page_scope == "all":
                variants.append("all")
                if pre:
                    variants.append("preelec")
            elif page_scope == "homepage":
                variants.append("home")
            for variant in variants:
                stats = daily_stats[variant][candidate_id]
                # The legacy coded count fields count every raw-nonempty page
                # and its day, even if cleaning later removes all substantive
                # text. Raw n_char > 0 is enforced above.
                stats["valid_days"] = stats.get("valid_days", 0) + 1
                stats["valid_pages"] = stats.get("valid_pages", 0) + int(row["n_pages_observed"])
                if variant == "all":
                    pages = int(row["n_pages_observed"])
                    stats["max_pages"] = max(stats.get("max_pages", 0), pages)
                    stats["total_pages"] = stats.get("total_pages", 0) + pages
                    stats["runaway_pages"] = stats.get("runaway_pages", 0) + (pages if pages > 100 else 0)
                    stats["first_day"] = min(stats.get("first_day", snap_day), snap_day)
                    stats["last_day"] = max(stats.get("last_day", snap_day), snap_day)

    _check(not remaining, f"snapshot panel is missing {len(remaining):,} raw-derived keys")
    _check(snapshot_ids == captured_ids, "snapshot/panel candidate ID sets differ")
    _check(all_count == EXPECTED_ALL_SCOPE_DAYS, f"snapshot panel has {all_count:,} all-scope days; expected {EXPECTED_ALL_SCOPE_DAYS:,}")
    _check(set(all_totals) == set(detail_totals), "all/detail snapshot day keys differ")
    for key, totals in all_totals.items():
        _check(tuple(detail_totals[key]) == totals, f"{key}: detail scopes do not sum to all scope")
    _reaggregate_snapshot_measures(rows_by_variant, coded, daily_stats)


def _same_universe(roster: pd.DataFrame, universe: pd.DataFrame) -> None:
    _check(
        list(roster.columns) == list(universe.columns),
        "release_roster.csv and candidate_year_universe.csv have different schemas",
    )
    left = roster.sort_values("candidate_cycle_id").reset_index(drop=True)
    right = universe.sort_values("candidate_cycle_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_dtype=False)


def _validate_provenance(
    provenance: pd.DataFrame,
    universe: pd.DataFrame,
    raw_summary: pd.DataFrame,
    panel: pd.DataFrame,
    coded: pd.DataFrame,
) -> None:
    _check(len(provenance) == len(universe), "provenance is not rectangular with universe")
    _check(provenance["candidate_cycle_id"].is_unique, "provenance key is not unique")
    _check(set(provenance["candidate_cycle_id"]) == set(universe["candidate_cycle_id"]), "provenance/universe IDs differ")
    common = list(universe.columns)
    _require_columns(provenance, set(common), "provenance")
    pd.testing.assert_frame_equal(
        universe[common].sort_values("candidate_cycle_id").reset_index(drop=True),
        provenance[common].sort_values("candidate_cycle_id").reset_index(drop=True),
        check_dtype=False,
    )

    required = {
        "observability_status", "all_text_measure_status",
        "homepage_measure_status", "preelection_measure_status",
        "topic_measure_status", "selected_panel_timestamp", "selected_panel_rule",
        "capture_adjudication_status", "capture_adjudication_reason",
        "collection_method_version", "cdx_availability_status",
        "collection_failure_reason", "snapshot_cap_binding_status",
        "provenance_evidence_scope", "n_raw_page_rows",
    }
    _require_columns(provenance, required, "provenance")
    _check(set(provenance["observability_status"]).issubset(OBSERVABILITY_STATUSES), "unknown observability_status")
    family_columns = [
        "all_text_measure_status", "homepage_measure_status",
        "preelection_measure_status", "topic_measure_status",
    ]
    for column in family_columns:
        _check(set(provenance[column]).issubset(MEASURE_STATUSES), f"unknown {column}")
    for column in ("cdx_availability_status", "collection_failure_reason", "snapshot_cap_binding_status"):
        _check(provenance[column].eq("unknown").all(), f"{column} infers unavailable evidence")
    _check(provenance["collection_method_version"].eq("legacy_mixed_unknown").all(), "collection method overstates evidence")
    _check(
        provenance["provenance_evidence_scope"].eq("released_successful_pages_and_legacy_roster").all(),
        "unexpected provenance_evidence_scope",
    )

    prov_index = provenance.set_index("candidate_cycle_id")
    raw_index = raw_summary.set_index("candidate_cycle_id")
    count_columns = [
        "n_observed_snapshot_timestamps", "n_observed_snapshot_days",
        "n_raw_page_rows", "n_distinct_page_urls", "n_distinct_page_texts",
        "n_page_types_observed", "n_source_hosts", "max_raw_pages_one_day",
        "n_runaway_days", "n_homepage_raw_page_rows", "n_preelection_raw_page_rows",
    ]
    scalar_columns = [
        "first_capture_timestamp", "last_capture_timestamp", "first_capture_day",
        "last_capture_day", "capture_span_days", "page_text_digest_sha256",
        "page_types_observed", "source_hosts", "share_raw_pages_on_runaway_days",
    ]
    for candidate_id, expected in raw_index.iterrows():
        actual = prov_index.loc[candidate_id]
        for column in count_columns:
            _check(int(actual[column]) == int(expected[column]), f"{candidate_id}: provenance {column} differs from raw")
        for column in scalar_columns:
            if column in {"share_raw_pages_on_runaway_days", "capture_span_days"}:
                _compare_optional_number(actual[column], expected[column], f"{candidate_id} provenance {column}")
            else:
                _check(
                    _provenance_scalar_equal(column, actual[column], expected[column]),
                    f"{candidate_id}: provenance {column} differs from raw",
                )
    uncaptured = prov_index.index.difference(raw_index.index)
    _check(prov_index.loc[uncaptured, count_columns].fillna(0).eq(0).all(axis=None), "uncaptured rows contain observed raw counts")
    absent_evidence_columns = [
        "first_capture_timestamp", "last_capture_timestamp", "first_capture_day",
        "last_capture_day", "capture_span_days", "page_text_digest_sha256",
        "share_raw_pages_on_runaway_days",
    ]
    _check(
        prov_index.loc[uncaptured, absent_evidence_columns].isna().all(axis=None),
        "uncaptured rows contain successful-capture evidence",
    )

    panel_index = panel.set_index("candidate_cycle_id")
    for candidate_id, row in panel_index.iterrows():
        selected = _timestamp_string(prov_index.at[candidate_id, "selected_panel_timestamp"])
        _check(selected == _timestamp_string(row["sel_date"]), f"{candidate_id}: provenance selected date differs from panel")
        _check(
            prov_index.at[candidate_id, "selected_panel_rule"] == "longest_text_snapshot_latest_timestamp_tie",
            f"{candidate_id}: incorrect selected-panel rule",
        )
    _check(prov_index.loc[uncaptured, "selected_panel_timestamp"].isna().all(), "uncaptured rows have selected timestamps")

    coded_index = coded.set_index("candidate_cycle_id")
    topic_columns = [column for column in coded if column.startswith("icpsr_topic_") and not column.endswith("_home")]
    for candidate_id, row in prov_index.iterrows():
        observed = int(row["n_raw_page_rows"]) > 0
        invalid = row["capture_adjudication_status"] == "excluded_invalid_capture"
        if invalid:
            _check(row["observability_status"] == "invalid_capture_excluded", f"{candidate_id}: invalid capture has wrong status")
            _check(not observed, f"{candidate_id}: invalid capture contributes raw pages")
        elif observed:
            _check(str(row["observability_status"]).startswith("captured_"), f"{candidate_id}: observed pages lack captured status")
            quality = _clean_string(row.get("selected_text_quality"))
            expected_status = {
                "usable": "captured_usable_text",
                "thin": "captured_thin_text",
            }.get(quality, "captured_empty_text")
            _check(
                row["observability_status"] == expected_status,
                f"{candidate_id}: observability status disagrees with selected text quality",
            )
        elif row["url_discovery_status"] == "legacy_no_url_record":
            _check(row["observability_status"] == "legacy_no_url_record",
                   f"{candidate_id}: missing legacy URL record has the wrong status")
        elif bool(_as_bool(pd.Series([row["has_url"]]), "provenance.has_url").iloc[0]):
            _check(row["observability_status"] == "valid_url_no_capture_unknown_reason",
                   f"{candidate_id}: valid URL without capture has the wrong status")
        else:
            _check(row["observability_status"] == "invalid_or_placeholder_url",
                   f"{candidate_id}: invalid/placeholder URL has the wrong status")

        if candidate_id not in coded_index.index:
            _check(all(row[column] == "not_computed" for column in family_columns), f"{candidate_id}: uncoded row has computed status")
            continue
        measures = coded_index.loc[candidate_id]
        families = {
            "all_text_measure_status": list(MEASURE_COLUMNS),
            "homepage_measure_status": [f"{column}_home" for column in MEASURE_COLUMNS],
            "preelection_measure_status": [f"{column}_preelec" for column in MEASURE_COLUMNS],
            "topic_measure_status": topic_columns,
        }
        for status_column, columns in families.items():
            status = row[status_column]
            values = pd.to_numeric(measures[columns], errors="coerce")
            if status in {"observed", "observed_zero"}:
                _check(values.notna().any(), f"{candidate_id}: {status_column} permits values but none exist")
                if status == "observed_zero":
                    _check(values.dropna().eq(0).all(), f"{candidate_id}: observed_zero has nonzero values")
            else:
                _check(values.isna().all(), f"{candidate_id}: {status_column}={status} but values exist")

    for candidate_id in INVALID_CAPTURE_IDS:
        _check(candidate_id in prov_index.index, f"invalid capture {candidate_id} is missing from provenance")
        _check(
            prov_index.at[candidate_id, "capture_adjudication_status"] == "excluded_invalid_capture",
            f"{candidate_id} lacks its approved capture adjudication",
        )
    _require_columns(
        provenance,
        {"capture_date_adjudication_status", "capture_date_max_snap_day"},
        "provenance",
    )
    david = prov_index.loc[DAVID_YOUNG_ID]
    _check(david["capture_date_adjudication_status"] == "retained_approved_date_range", "David Young lacks date adjudication")
    _check(
        _clean_string(david["capture_date_max_snap_day"]).removesuffix(".0") == DAVID_YOUNG_LAST_VALID_DAY,
        "David Young provenance has the wrong maximum retained day",
    )


def _validate_manifest(base: Path, loaded_csvs: dict[str, pd.DataFrame]) -> None:
    path = base / "manifest.json"
    _check(path.exists(), "missing release file: manifest.json")
    manifest = json.loads(path.read_text())
    _check(isinstance(manifest, list), "manifest.json must contain a list")
    names = [item.get("file") for item in manifest]
    _check(len(names) == len(set(names)), "manifest contains duplicate entries")
    _check(
        set(names) == DATA_FILES,
        f"manifest inventory mismatch: missing={sorted(DATA_FILES - set(names))}, extra={sorted(set(names) - DATA_FILES)}",
    )
    for item in manifest:
        file_path = base / item["file"]
        _check(file_path.exists(), f"manifest entry is absent: {item['file']}")
        _check(int(item["bytes"]) == file_path.stat().st_size, f"wrong manifest bytes for {item['file']}")
        _check(item["sha256"] == sha256(file_path), f"wrong manifest SHA-256 for {item['file']}")
        if file_path.suffix == ".csv":
            rows, columns = loaded_csvs[file_path.name].shape
        else:
            metadata = pq.ParquetFile(file_path).metadata
            rows, columns = metadata.num_rows, metadata.num_columns
        _check(int(item["rows"]) == rows, f"wrong manifest rows for {item['file']}")
        _check(int(item["columns"]) == columns, f"wrong manifest columns for {item['file']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release_dir", type=Path)
    args = parser.parse_args(argv)
    base = args.release_dir
    _check(base.is_dir(), f"release directory does not exist: {base}")
    disk_files = {path.name for path in base.iterdir() if path.is_file()}
    missing = (DATA_FILES | {"manifest.json"}) - disk_files
    _check(not missing, f"missing release files: {sorted(missing)}")

    csv_names = sorted(name for name in DATA_FILES if name.endswith(".csv"))
    csvs = {name: _read_csv(base / name) for name in csv_names}
    panel = csvs["panel_candidate_year.csv"]
    coded = csvs["panel_icpsr_compat.csv"]
    crosswalk = csvs["candidate_crosswalk.csv"]
    roster = csvs["release_roster.csv"]
    universe = csvs["candidate_year_universe.csv"]
    provenance = csvs["candidate_year_provenance.csv"]

    products = {"panel": panel, "coded panel": coded, "crosswalk": crosswalk}
    for label, frame in products.items():
        _require_columns(frame, {"candidate_cycle_id"}, label)
        _check(len(frame) == EXPECTED_CANDIDATE_YEARS, f"{label} has {len(frame):,} rows; expected {EXPECTED_CANDIDATE_YEARS:,}")
        _check(frame["candidate_cycle_id"].is_unique, f"{label} key is not unique")
    captured_ids = set(panel["candidate_cycle_id"].astype(str))
    for label, frame in products.items():
        _check(set(frame["candidate_cycle_id"].astype(str)) == captured_ids, f"{label} candidate ID set differs from panel")

    _check(len(roster) == len(universe) == EXPECTED_UNIVERSE_ROWS, f"universe/roster must each have {EXPECTED_UNIVERSE_ROWS:,} rows")
    _check(roster["candidate_cycle_id"].is_unique, "release roster key is not unique")
    _check(universe["candidate_cycle_id"].is_unique, "candidate universe key is not unique")
    _same_universe(roster, universe)
    universe_ids = set(universe["candidate_cycle_id"].astype(str))
    _check(captured_ids <= universe_ids, "captured panels contain candidates outside universe")
    _check(not (universe_ids & UNIVERSE_EXCLUDED_IDS), "state-legislative exclusions remain in federal universe")
    _check(INVALID_CAPTURE_IDS <= universe_ids, "invalid captures must remain visible in universe")
    office = universe["office"].astype("string").str.lower()
    years = pd.to_numeric(universe["year"], errors="coerce")
    _check(set(office.dropna()) == {"house", "senate"}, "universe contains an unsupported office")
    _check(set(years.loc[office.eq("house")].astype(int)).issubset({2018, 2020, 2022, 2024}),
           "universe contains a House year outside 2018--2024")
    _check(set(years.loc[office.eq("senate")].astype(int)).issubset(set(range(2002, 2025, 2))),
           "universe contains a Senate year outside 2002--2024")

    captured = _as_bool(universe["captured"], "universe.captured")
    _check(not captured.isna().any(), "universe.captured contains missing values")
    _check(int(captured.sum()) == EXPECTED_CANDIDATE_YEARS, "universe captured count differs from panels")
    _check(set(universe.loc[captured, "candidate_cycle_id"].astype(str)) == captured_ids, "captured flags disagree with panels")
    capture_status = universe["capture_status"].astype("string")
    _check(capture_status.loc[captured].eq("captured").all(),
           "captured universe rows do not have capture_status=captured")
    invalid_universe = universe["candidate_cycle_id"].isin(INVALID_CAPTURE_IDS)
    _check(capture_status.loc[invalid_universe].eq("invalid_capture_excluded").all(),
           "approved invalid captures have the wrong universe capture_status")
    has_url = _as_bool(universe["has_url"], "universe.has_url")
    _check(not has_url.isna().any(), "universe.has_url contains missing values")
    accepted_url = universe["url_validation_status"].isin(["valid", "valid_manual_override"])
    _check(has_url.astype(bool).equals(accepted_url), "has_url disagrees with accepted URL states")
    on_ballot_universe = _as_bool(universe["on_ballot"], "universe.on_ballot")
    votes = pd.to_numeric(universe["general_votes"], errors="coerce")
    _check(votes.loc[on_ballot_universe].notna().all(), "ballot universe rows lack votes")
    _check(votes.loc[~on_ballot_universe].isna().all(), "off-ballot universe rows have votes")
    _validate_geography(universe, "candidate universe", captured_only=captured)

    on_ballot = _as_bool(coded["on_ballot"], "coded.on_ballot")
    _check(int(on_ballot.sum()) == EXPECTED_BALLOT_CANDIDATES, f"coded panel has {int(on_ballot.sum()):,} ballot rows; expected {EXPECTED_BALLOT_CANDIDATES:,}")
    compatible = _as_bool(coded["icpsr_compatible"], "coded.icpsr_compatible")
    _check(compatible.equals(on_ballot), "icpsr_compatible disagrees with on_ballot")
    captured_universe = universe.set_index("candidate_cycle_id").loc[
        coded["candidate_cycle_id"]
    ]
    universe_ballot = _as_bool(
        captured_universe["on_ballot"].reset_index(drop=True),
        "captured universe.on_ballot",
    )
    _check(universe_ballot.equals(on_ballot.reset_index(drop=True)),
           "captured universe ballot flags differ from coded panel")
    universe_votes = pd.to_numeric(
        captured_universe["general_votes"].reset_index(drop=True), errors="coerce"
    )
    coded_votes = pd.to_numeric(coded["general_votes"], errors="coerce")
    _check(
        universe_votes.fillna(-1).eq(coded_votes.fillna(-1)).all(),
        "captured universe vote totals differ from coded panel",
    )
    panel_ballot = _as_bool(panel["on_ballot"], "panel.on_ballot")
    _check(panel.loc[~panel_ballot, "stage"].isna().all(), "off-ballot panel rows have stage")
    _check(panel.loc[~panel_ballot, "data_source"].eq("election_year_wayback").all(), "off-ballot rows have wrong source")
    _check(pd.to_numeric(panel.loc[panel_ballot, "stage"]).eq(2).all(), "ballot rows do not have stage 2")
    _check(panel.loc[panel_ballot, "data_source"].eq("general_wayback").all(), "ballot rows have wrong source")
    regular = panel["universe_source"].eq("same_year_fec")
    _check(
        pd.to_numeric(panel.loc[regular, "cand_election_yr"], errors="coerce")
        .eq(pd.to_numeric(panel.loc[regular, "year"], errors="coerce")).all(),
        "same-year FEC rows have inconsistent election year",
    )
    shared = [
        "candidate_cycle_id", "cand_id", "cand_election_yr", "cand_status",
        "universe_source", "on_ballot", "stage", "data_source",
    ]
    reference = panel[shared].sort_values("candidate_cycle_id").reset_index(drop=True)
    for label, product in (("coded panel", coded), ("crosswalk", crosswalk)):
        _require_columns(product, set(shared), label)
        other = product[shared].sort_values("candidate_cycle_id").reset_index(drop=True)
        pd.testing.assert_frame_equal(reference, other, check_dtype=False)
    _validate_geography(panel, "candidate-year panel")
    _validate_geography(crosswalk, "candidate crosswalk")
    validate_topics(coded)

    david = panel.set_index("candidate_cycle_id").loc[DAVID_YOUNG_ID]
    _check(
        _timestamp_string(david["sel_date"])[:8] == DAVID_YOUNG_LAST_VALID_DAY,
        "David Young selected date is not on 20140105",
    )
    for excluded in INVALID_CAPTURE_IDS | UNIVERSE_EXCLUDED_IDS:
        _check(excluded not in captured_ids, f"excluded candidate remains in panels: {excluded}")

    expected_snapshot, raw_summary = _validate_raw(base / "raw_corpus.parquet", panel, captured_ids)
    _validate_snapshot(base / "panel_snapshot_day.parquet", expected_snapshot, coded, captured_ids)
    _validate_provenance(provenance, universe, raw_summary, panel, coded)
    _validate_manifest(base, csvs)

    print(
        f"PASS: {len(panel):,} captured candidate-years; {EXPECTED_RAW_ROWS:,} raw rows; "
        f"{len(universe):,} universe rows; {EXPECTED_SNAPSHOT_ROWS:,} snapshot-day-scope rows"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
