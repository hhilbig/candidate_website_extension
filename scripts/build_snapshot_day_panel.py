#!/usr/bin/env python3
"""Build the text-free snapshot-day panel from a corrected raw corpus.

The output grain is ``candidate_cycle_id x snap_day x page_scope``.  Each
candidate-day has an ``all`` row plus one row for every observed page type.
ICPSR-compatible measures are page means within that day and scope; candidate
means across the ``all`` rows therefore reproduce the existing two-level
page-then-day aggregation.  Daily topics are different: the frozen classifier
is applied once to one cleaned document per day and scope.

The builder reads one Parquet row group at a time and refuses input where one
candidate is split across row groups.  The corrected release currently writes
one candidate-year per row group.  Refusing a split is preferable to silently
emitting duplicate day keys while keeping the raw page text out of memory.

Examples
--------
Build the complete product, including the frozen 31-topic classifier::

    python scripts/build_snapshot_day_panel.py --corpus CORPUS --out OUTPUT

For a quick structural build that deliberately leaves topics uncomputed::

    python scripts/build_snapshot_day_panel.py --skip-topics ...
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Sequence
from urllib.parse import urlsplit

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.icpsr_replicate_coding import (  # noqa: E402
    RUNAWAY_PAGES,
    election_day,
    icpsr_clean_text,
    load_ngrams,
    page_measures,
    train_topic_model,
)

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

REQUIRED_RAW_COLUMNS = {
    "candidate_cycle_id",
    "year",
    "date",
    "page_type",
    "n_char",
    "n_words",
    "text_snap_content",
}

# These fields are copied when present.  ``candidate`` is included for future
# corrected corpora; the current corpus carries the display name in
# ``candidate_icpsr`` instead.
CARRY_STRING_COLUMNS = (
    "candidate_cycle_id",
    "candidate",
    "candidate_icpsr",
    "name_key",
    "cand_id",
    "cand_election_yr",
    "cand_status",
    "state",
    "district",
    "district_id",
    "office",
    "party",
    "data_source",
    "candidate_year_stage",
    "universe_source",
)
CARRY_INT_COLUMNS = ("stage",)
CARRY_BOOL_COLUMNS = ("on_ballot",)

MEAN_COLUMNS = (
    "icpsr_n_char",
    "icpsr_n_words",
    "icpsr_n_tags",
    "icpsr_n_clean_tags",
    "icpsr_ttr_approx",
    "icpsr_mattr_approx",
    "icpsr_entropy_approx",
)

BASE_OUTPUT_COLUMNS = (
    *CARRY_STRING_COLUMNS,
    *CARRY_INT_COLUMNS,
    *CARRY_BOOL_COLUMNS,
    "year",
    "snap_day",
    "page_scope",
    "first_capture_timestamp",
    "last_capture_timestamp",
    "n_capture_timestamps",
    "election_day",
    "days_to_election",
    "pre_election",
    "post_election",
    "n_pages_observed",
    "n_pages_all_scopes",
    "n_clean_valid_pages",
    "n_distinct_urls",
    "n_distinct_page_hashes",
    "n_page_types",
    "page_types_present",
    "source_hosts",
    "page_hashes_sha256",
    "day_digest_sha256",
    "runaway_day",
    "raw_n_char_total",
    "raw_n_words_total",
    "clean_n_char_total",
    "clean_n_words_total",
    "clean_n_tags_total",
    "clean_n_clean_tags_total",
    *MEAN_COLUMNS,
    "measure_status",
    "topic_status",
)

TopicModel = tuple[Any, Callable[[str], str], Sequence[str]]


def topic_names_from_mapping(mapping_path: Path) -> list[str]:
    """Return the frozen classifier's 31 class names in model order."""
    mapping = pd.read_csv(mapping_path, dtype={"code": object})
    if "domain_name_consolidated" not in mapping:
        raise ValueError(
            f"{mapping_path} lacks domain_name_consolidated"
        )
    names = sorted(mapping.domain_name_consolidated.dropna().astype(str).unique())
    if len(names) != 31:
        raise ValueError(
            f"expected 31 topic classes in {mapping_path}, found {len(names)}"
        )
    return names


def topic_columns(topic_names: Sequence[str]) -> list[str]:
    """Name daily probabilities without implying ICPSR topic equivalence."""
    return [f"day_topic_{name}" for name in topic_names]


def output_schema(topic_names: Sequence[str]) -> pa.Schema:
    """Stable Arrow schema, including list-valued provenance fields."""
    fields: list[pa.Field] = []
    for name in CARRY_STRING_COLUMNS:
        fields.append(pa.field(name, pa.string()))
    for name in CARRY_INT_COLUMNS:
        fields.append(pa.field(name, pa.int8()))
    for name in CARRY_BOOL_COLUMNS:
        fields.append(pa.field(name, pa.bool_()))
    fields.extend(
        [
            pa.field("year", pa.int32()),
            pa.field("snap_day", pa.int32()),
            pa.field("page_scope", pa.string()),
            pa.field("first_capture_timestamp", pa.string()),
            pa.field("last_capture_timestamp", pa.string()),
            pa.field("n_capture_timestamps", pa.int32()),
            pa.field("election_day", pa.int32()),
            pa.field("days_to_election", pa.int32()),
            pa.field("pre_election", pa.bool_()),
            pa.field("post_election", pa.bool_()),
            pa.field("n_pages_observed", pa.int32()),
            pa.field("n_pages_all_scopes", pa.int32()),
            pa.field("n_clean_valid_pages", pa.int32()),
            pa.field("n_distinct_urls", pa.int32()),
            pa.field("n_distinct_page_hashes", pa.int32()),
            pa.field("n_page_types", pa.int8()),
            pa.field("page_types_present", pa.list_(pa.string())),
            pa.field("source_hosts", pa.list_(pa.string())),
            pa.field("page_hashes_sha256", pa.list_(pa.string())),
            pa.field("day_digest_sha256", pa.string()),
            pa.field("runaway_day", pa.bool_()),
            pa.field("raw_n_char_total", pa.int64()),
            pa.field("raw_n_words_total", pa.int64()),
            pa.field("clean_n_char_total", pa.int64()),
            pa.field("clean_n_words_total", pa.int64()),
            pa.field("clean_n_tags_total", pa.int64()),
            pa.field("clean_n_clean_tags_total", pa.int64()),
        ]
    )
    fields.extend(pa.field(name, pa.float64()) for name in MEAN_COLUMNS)
    fields.extend(
        [
            pa.field("measure_status", pa.string()),
            pa.field("topic_status", pa.string()),
        ]
    )
    fields.extend(pa.field(name, pa.float64()) for name in topic_columns(topic_names))
    return pa.schema(fields)


def _plain_value(value: Any) -> Any:
    """Convert pandas scalars to Arrow-friendly Python values."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def _single_value(frame: pd.DataFrame, column: str, candidate_id: str) -> Any:
    """Return invariant candidate metadata, rejecting conflicting values."""
    if column not in frame:
        return None
    values = [_plain_value(value) for value in frame[column].tolist()]
    nonmissing = []
    for value in values:
        if value is not None and value not in nonmissing:
            nonmissing.append(value)
    if len(nonmissing) > 1:
        preview = ", ".join(repr(value) for value in nonmissing[:4])
        raise ValueError(
            f"{candidate_id}: conflicting {column} values ({preview})"
        )
    return nonmissing[0] if nonmissing else None


def _coerce_raw_count(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    bad = values.isna() | values.lt(0) | values.mod(1).ne(0)
    if bad.any():
        rows = frame.loc[bad, ["candidate_cycle_id", "date", column]].head(5)
        raise ValueError(
            f"invalid nonnegative integer {column}: {rows.to_dict('records')}"
        )
    return values.astype("int64")


def _validated_timestamps(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    values = frame["date"].astype("string")
    syntax_ok = values.str.fullmatch(r"[0-9]{8,14}", na=False)
    days = values.str.slice(0, 8)
    parsed = pd.to_datetime(days, format="%Y%m%d", errors="coerce")
    bad = ~syntax_ok | parsed.isna()
    if bad.any():
        rows = frame.loc[bad, ["candidate_cycle_id", "date"]].head(5)
        raise ValueError(f"invalid Wayback timestamp: {rows.to_dict('records')}")
    return values.astype(str), days.astype(int)


def _source_host(snap_url: Any, urlkey: Any) -> str | None:
    """Extract the page host, falling back to the candidate's URL key."""
    candidates = []
    for value in (snap_url, urlkey):
        if value is None or pd.isna(value):
            continue
        text = str(value).strip()
        if text:
            candidates.append(text)
    for value in candidates:
        # When only a Wayback URL is present, unwrap the original target.
        match = re.search(r"/web/[0-9A-Za-z_*]+/(https?://.+)$", value)
        if match:
            value = match.group(1)
        parsed = urlsplit(value if "://" in value else f"//{value}")
        if parsed.hostname:
            return parsed.hostname.lower().rstrip(".")
    return None


def _page_url(snap_url: Any, urlkey: Any) -> str | None:
    for value in (snap_url, urlkey):
        if value is None or pd.isna(value):
            continue
        text = str(value).strip()
        if text:
            match = re.search(r"/web/[0-9A-Za-z_*]+/(https?://.+)$", text)
            return match.group(1) if match else text
    return None


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _prepare_pages(
    frame: pd.DataFrame,
    *,
    mattr_window: int,
    ngrams: tuple[dict, set] | None,
    validate_raw_counts: bool,
) -> pd.DataFrame:
    """Validate raw fields and compute each page's cleaned measures once."""
    missing = REQUIRED_RAW_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"raw corpus lacks required columns: {sorted(missing)}")
    if frame.empty:
        return frame.copy()

    out = frame.copy()
    if "candidate" not in out and "candidate_icpsr" in out:
        # Preserve the available display label under the plain public name.
        # This is an alias, not a new identity match.
        out["candidate"] = out["candidate_icpsr"]
    if out.candidate_cycle_id.isna().any() or out.candidate_cycle_id.eq("").any():
        raise ValueError("candidate_cycle_id must be nonmissing")
    out["candidate_cycle_id"] = out.candidate_cycle_id.astype(str)
    out["_timestamp"], out["_snap_day"] = _validated_timestamps(out)
    out["year"] = pd.to_numeric(out.year, errors="coerce")
    if out.year.isna().any() or out.year.mod(1).ne(0).any():
        raise ValueError("year must be an integer")
    out["year"] = out.year.astype(int)
    wrong_year = out._snap_day.astype(str).str.slice(0, 4).astype(int).ne(out.year)
    if wrong_year.any():
        rows = out.loc[
            wrong_year, ["candidate_cycle_id", "year", "date"]
        ].head(5)
        raise ValueError(
            "snapshot year differs from candidate year: "
            f"{rows.to_dict('records')}"
        )

    out["page_type"] = out.page_type.astype("string")
    invalid_types = sorted(
        set(out.loc[~out.page_type.isin(PAGE_TYPES), "page_type"].dropna())
    )
    if out.page_type.isna().any() or invalid_types:
        raise ValueError(
            "page_type must be one of the eight frozen scopes; "
            f"unexpected={invalid_types}, missing={int(out.page_type.isna().sum())}"
        )

    out["_raw_n_char"] = _coerce_raw_count(out, "n_char")
    out["_raw_n_words"] = _coerce_raw_count(out, "n_words")
    out["_raw_text"] = out.text_snap_content.fillna("").astype(str)
    if validate_raw_counts:
        actual_char = out._raw_text.map(len).astype("int64")
        actual_words = out._raw_text.map(lambda text: len(text.split())).astype("int64")
        mismatch = actual_char.ne(out._raw_n_char) | actual_words.ne(out._raw_n_words)
        if mismatch.any():
            rows = out.loc[
                mismatch,
                ["candidate_cycle_id", "date", "n_char", "n_words"],
            ].head(5)
            raise ValueError(
                "raw count columns do not reconstruct from text: "
                f"{rows.to_dict('records')}"
            )

    # Campaign pages are often byte-identical across many snapshot dates.
    # Cache within the candidate row group so repeated text is cleaned and
    # measured once without retaining the whole corpus in memory.
    page_cache: dict[str, tuple[dict, dict, str]] = {}
    cleaned = []
    measures = []
    page_hashes = []
    for text in out._raw_text:
        cached = page_cache.get(text)
        if cached is None:
            clean_result = icpsr_clean_text(text)
            measure_result = page_measures(
                clean_result["text"], mattr_window, ngrams, clean=False
            )
            cached = (clean_result, measure_result, _sha256(text))
            page_cache[text] = cached
        clean_result, measure_result, page_hash = cached
        cleaned.append(clean_result)
        measures.append(measure_result)
        page_hashes.append(page_hash)
    out["_clean_text"] = [item["text"] for item in cleaned]
    out["_clean_n_tags"] = [int(item["n_tags"]) for item in cleaned]
    out["_clean_n_clean_tags"] = [int(item["n_clean_tags"]) for item in cleaned]
    out["_clean_n_char"] = [int(item["n_char"]) for item in measures]
    out["_clean_n_words"] = [int(item["n_words"]) for item in measures]
    out["_clean_ttr"] = [item.get("ttr", np.nan) for item in measures]
    out["_clean_mattr"] = [item.get("mattr", np.nan) for item in measures]
    out["_clean_entropy"] = [item.get("entropy", np.nan) for item in measures]
    out["_page_hash"] = page_hashes
    out["_page_url"] = [
        _page_url(snap, key)
        for snap, key in zip(
            out.get("snap_url", pd.Series([None] * len(out), index=out.index)),
            out.get("urlkey", pd.Series([None] * len(out), index=out.index)),
        )
    ]
    out["_source_host"] = [
        _source_host(snap, key)
        for snap, key in zip(
            out.get("snap_url", pd.Series([None] * len(out), index=out.index)),
            out.get("urlkey", pd.Series([None] * len(out), index=out.index)),
        )
    ]

    fingerprints = []
    columns = [
        "_timestamp",
        "page_type",
        "_page_url",
        "urlkey",
        "_page_hash",
        "_raw_n_char",
        "_raw_n_words",
    ]
    values = {
        column: (
            out[column]
            if column in out
            else pd.Series([None] * len(out), index=out.index)
        )
        for column in columns
    }
    for position in range(len(out)):
        def at(column: str) -> Any:
            return values[column].iloc[position]

        urlkey = at("urlkey")
        payload = [
            str(at("_timestamp")),
            str(at("page_type")),
            at("_page_url") or "",
            "" if urlkey is None or pd.isna(urlkey) else str(urlkey),
            at("_page_hash"),
            int(at("_raw_n_char")),
            int(at("_raw_n_words")),
        ]
        fingerprints.append(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        )
    out["_row_fingerprint"] = fingerprints
    return out


def _mean_or_nan(frame: pd.DataFrame, column: str) -> float:
    values = pd.to_numeric(frame[column], errors="coerce")
    return float(values.mean()) if values.notna().any() else float("nan")


def _scope_row(
    scope: pd.DataFrame,
    *,
    candidate_meta: dict[str, Any],
    page_scope: str,
    snap_day: int,
    n_pages_all_scopes: int,
) -> dict[str, Any]:
    year = int(candidate_meta["year"])
    election = election_day(year)
    snapshot_date = dt.datetime.strptime(str(snap_day), "%Y%m%d").date()
    election_date = dt.datetime.strptime(str(election), "%Y%m%d").date()
    days_to_election = (election_date - snapshot_date).days

    valid = scope[scope._clean_n_char.gt(0)]
    page_hashes = sorted(scope._page_hash.astype(str).tolist())
    digest_input = "\n".join(sorted(scope._row_fingerprint.astype(str)))
    page_types_present = sorted(
        scope.page_type.astype(str).unique(), key=PAGE_TYPE_ORDER.__getitem__
    )
    hosts = sorted(value for value in scope._source_host.dropna().unique() if value)
    urls = {value for value in scope._page_url.dropna() if value}

    row = {
        **candidate_meta,
        "snap_day": int(snap_day),
        "page_scope": page_scope,
        "first_capture_timestamp": str(scope._timestamp.min()),
        "last_capture_timestamp": str(scope._timestamp.max()),
        "n_capture_timestamps": int(scope._timestamp.nunique()),
        "election_day": int(election),
        "days_to_election": int(days_to_election),
        # Match the existing ICPSR-compatible ``_preelec`` variant, which
        # includes pages captured on election day itself.
        "pre_election": bool(days_to_election >= 0),
        "post_election": bool(days_to_election < 0),
        "n_pages_observed": int(len(scope)),
        "n_pages_all_scopes": int(n_pages_all_scopes),
        "n_clean_valid_pages": int(len(valid)),
        "n_distinct_urls": int(len(urls)),
        "n_distinct_page_hashes": int(len(set(page_hashes))),
        "n_page_types": int(len(page_types_present)),
        "page_types_present": page_types_present,
        "source_hosts": hosts,
        "page_hashes_sha256": page_hashes,
        "day_digest_sha256": _sha256(digest_input),
        "runaway_day": bool(n_pages_all_scopes > RUNAWAY_PAGES),
        "raw_n_char_total": int(scope._raw_n_char.sum()),
        "raw_n_words_total": int(scope._raw_n_words.sum()),
        "clean_n_char_total": int(scope._clean_n_char.sum()),
        "clean_n_words_total": int(scope._clean_n_words.sum()),
        "clean_n_tags_total": int(scope._clean_n_tags.sum()),
        "clean_n_clean_tags_total": int(scope._clean_n_clean_tags.sum()),
        "measure_status": "observed" if len(valid) else "insufficient_clean_text",
    }
    source_columns = {
        "icpsr_n_char": "_clean_n_char",
        "icpsr_n_words": "_clean_n_words",
        "icpsr_n_tags": "_clean_n_tags",
        "icpsr_n_clean_tags": "_clean_n_clean_tags",
        "icpsr_ttr_approx": "_clean_ttr",
        "icpsr_mattr_approx": "_clean_mattr",
        "icpsr_entropy_approx": "_clean_entropy",
    }
    for output, source in source_columns.items():
        row[output] = _mean_or_nan(valid, source) if len(valid) else np.nan

    # The classifier document is deterministic under input-row permutation.
    # Each page-type document is disjoint; ``all`` is built directly from all
    # pages, not by averaging or concatenating page-type predictions.
    ordered = valid.sort_values(
        ["_timestamp", "page_type", "_page_url", "_page_hash"],
        kind="mergesort",
        na_position="last",
    )
    document = " ".join(text for text in ordered._clean_text if text.strip())
    row["_topic_document"] = document if document.strip() else None
    return row


def _unscored_snapshot_rows(prepared: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate_id, candidate in prepared.groupby(
        "candidate_cycle_id", sort=False, dropna=False
    ):
        meta = {
            column: _single_value(candidate, column, str(candidate_id))
            for column in (
                *CARRY_STRING_COLUMNS,
                *CARRY_INT_COLUMNS,
                *CARRY_BOOL_COLUMNS,
            )
        }
        for column in CARRY_STRING_COLUMNS:
            if meta[column] is not None:
                meta[column] = str(meta[column])
        for column in CARRY_INT_COLUMNS:
            if meta[column] is not None:
                meta[column] = int(meta[column])
        for column in CARRY_BOOL_COLUMNS:
            if meta[column] is not None:
                meta[column] = bool(meta[column])
        meta["candidate_cycle_id"] = str(candidate_id)
        meta["year"] = int(_single_value(candidate, "year", str(candidate_id)))
        for snap_day, day in candidate.groupby("_snap_day", sort=True):
            n_all = int(len(day))
            rows.append(
                _scope_row(
                    day,
                    candidate_meta=meta,
                    page_scope="all",
                    snap_day=int(snap_day),
                    n_pages_all_scopes=n_all,
                )
            )
            observed = set(day.page_type.astype(str))
            for page_type in PAGE_TYPES:
                if page_type not in observed:
                    continue
                rows.append(
                    _scope_row(
                        day[day.page_type.eq(page_type)],
                        candidate_meta=meta,
                        page_scope=page_type,
                        snap_day=int(snap_day),
                        n_pages_all_scopes=n_all,
                    )
                )
    return pd.DataFrame(rows)


def _score_topics(
    rows: pd.DataFrame,
    *,
    topic_model: TopicModel | None,
    topic_names: Sequence[str],
    topic_cache: dict[tuple[bytes, int], np.ndarray | None] | None = None,
) -> pd.DataFrame:
    out = rows.copy()
    columns = topic_columns(topic_names)
    for column in columns:
        out[column] = np.nan

    supported = out._topic_document.notna() & out._topic_document.astype("string").str.strip().ne("")
    out["topic_status"] = "insufficient_clean_text"
    if topic_model is None:
        out.loc[supported, "topic_status"] = "not_computed"
        return out.drop(columns="_topic_document")

    pipe, cleaner, model_names = topic_model
    if list(model_names) != list(topic_names):
        raise ValueError("topic model class names do not match output topic names")
    cache = topic_cache if topic_cache is not None else {}
    supported_index = list(out.index[supported])
    entries: list[tuple[Any, tuple[bytes, int], str]] = []
    unseen: dict[tuple[bytes, int], str] = {}
    for index in supported_index:
        document = str(out.at[index, "_topic_document"])
        encoded = document.encode("utf-8")
        key = (hashlib.sha256(encoded).digest(), len(encoded))
        entries.append((index, key, document))
        if key not in cache and key not in unseen:
            unseen[key] = document

    # Exact campaign pages and whole-day documents repeat heavily across
    # adjacent snapshots. Clean and classify each distinct document once for
    # the entire streaming build, then reuse its immutable probability vector.
    cleaned_groups: dict[str, list[tuple[bytes, int]]] = {}
    for key, document in unseen.items():
        cleaned = cleaner(document)
        if not cleaned.strip():
            cache[key] = None
        else:
            cleaned_groups.setdefault(cleaned, []).append(key)
    score_docs = list(cleaned_groups)
    if score_docs:
        probabilities = np.asarray(pipe.predict_proba(score_docs), dtype=float)
        expected = (len(cleaned_groups), len(topic_names))
        if probabilities.shape != expected:
            raise ValueError(
                f"topic model returned {probabilities.shape}, expected {expected}"
            )
        if not np.isfinite(probabilities).all() or (probabilities < 0).any():
            raise ValueError("topic model returned invalid probabilities")
        sums = probabilities.sum(axis=1)
        if not np.allclose(sums, 1.0, atol=1e-9, rtol=0):
            raise ValueError("topic probabilities do not sum to one")
        for cleaned, probability in zip(score_docs, probabilities):
            for key in cleaned_groups[cleaned]:
                cache[key] = probability.copy()

    for index, key, _ in entries:
        probability = cache[key]
        if probability is None:
            continue
        out.loc[index, columns] = probability
        out.loc[index, "topic_status"] = "observed"
    return out.drop(columns="_topic_document")


def build_snapshot_day_rows(
    frame: pd.DataFrame,
    *,
    mattr_window: int = 200,
    ngrams: tuple[dict, set] | None = None,
    topic_model: TopicModel | None = None,
    topic_names: Sequence[str] = (),
    validate_raw_counts: bool = True,
) -> pd.DataFrame:
    """Build snapshot-day rows from a small in-memory raw-page frame.

    This function is the deterministic unit-test surface.  The file builder
    below applies it row-group by row-group and batches classifier calls.
    """
    prepared = _prepare_pages(
        frame,
        mattr_window=mattr_window,
        ngrams=ngrams,
        validate_raw_counts=validate_raw_counts,
    )
    if prepared.empty:
        return pd.DataFrame(columns=[*BASE_OUTPUT_COLUMNS, *topic_columns(topic_names)])
    rows = _unscored_snapshot_rows(prepared)
    rows = _score_topics(rows, topic_model=topic_model, topic_names=topic_names)
    return rows[[*BASE_OUTPUT_COLUMNS, *topic_columns(topic_names)]]


def _validate_and_write(
    rows: pd.DataFrame,
    *,
    writer: pq.ParquetWriter,
    schema: pa.Schema,
    seen_keys: set[tuple[str, int, str]],
) -> None:
    keys = list(
        rows[["candidate_cycle_id", "snap_day", "page_scope"]]
        .itertuples(index=False, name=None)
    )
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate snapshot-day key within an output batch")
    overlap = set(keys) & seen_keys
    if overlap:
        raise ValueError(f"duplicate snapshot-day key across batches: {next(iter(overlap))}")
    all_keys = {(candidate, day) for candidate, day, scope in keys if scope == "all"}
    orphan = [
        key for key in keys
        if key[2] != "all" and (key[0], key[1]) not in all_keys
    ]
    if orphan:
        raise ValueError(f"page-type row lacks an all-scope row: {orphan[0]}")
    seen_keys.update(keys)
    table = pa.Table.from_pandas(rows, schema=schema, preserve_index=False, safe=True)
    writer.write_table(table)


def build_snapshot_day_panel(
    corpus: Path,
    out: Path,
    *,
    mattr_window: int = 200,
    ngrams: tuple[dict, set] | None = None,
    topic_model: TopicModel | None = None,
    topic_names: Sequence[str] = (),
    topic_batch_size: int = 400,
    validate_raw_counts: bool = True,
    compression: str = "zstd",
    overwrite: bool = False,
) -> dict[str, int]:
    """Stream a raw Parquet corpus into a validated snapshot-day Parquet."""
    corpus = Path(corpus)
    out = Path(out)
    if not corpus.exists():
        raise FileNotFoundError(corpus)
    if out.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite existing output: {out}")
    if topic_batch_size < 1:
        raise ValueError("topic_batch_size must be positive")

    pf = pq.ParquetFile(corpus)
    missing = REQUIRED_RAW_COLUMNS - set(pf.schema_arrow.names)
    if missing:
        raise ValueError(f"raw corpus lacks required columns: {sorted(missing)}")
    read_columns = sorted(
        REQUIRED_RAW_COLUMNS
        | set(CARRY_STRING_COLUMNS)
        | set(CARRY_INT_COLUMNS)
        | set(CARRY_BOOL_COLUMNS)
        | {"urlkey", "snap_url"}
    )
    read_columns = [column for column in read_columns if column in pf.schema_arrow.names]

    schema = output_schema(topic_names)
    out.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=out.parent, prefix=f".{out.name}.", suffix=".tmp", delete=False
    )
    temp_path = Path(handle.name)
    handle.close()

    writer: pq.ParquetWriter | None = None
    seen_candidates: set[str] = set()
    seen_keys: set[tuple[str, int, str]] = set()
    pending: list[pd.DataFrame] = []
    pending_documents = 0
    output_rows = 0
    input_pages = 0
    topic_cache: dict[tuple[bytes, int], np.ndarray | None] | None = (
        {} if topic_model is not None else None
    )

    def flush() -> None:
        nonlocal pending, pending_documents, output_rows
        if not pending:
            return
        batch = pd.concat(pending, ignore_index=True)
        batch = _score_topics(
            batch,
            topic_model=topic_model,
            topic_names=topic_names,
            topic_cache=topic_cache,
        )
        batch = batch[[*BASE_OUTPUT_COLUMNS, *topic_columns(topic_names)]]
        assert writer is not None
        _validate_and_write(
            batch, writer=writer, schema=schema, seen_keys=seen_keys
        )
        output_rows += len(batch)
        pending = []
        pending_documents = 0

    try:
        writer = pq.ParquetWriter(temp_path, schema=schema, compression=compression)
        for index in range(pf.num_row_groups):
            frame = pf.read_row_group(index, columns=read_columns).to_pandas()
            if frame.empty:
                continue
            candidate_ids = set(frame.candidate_cycle_id.dropna().astype(str))
            overlap = candidate_ids & seen_candidates
            if overlap:
                candidate_id = sorted(overlap)[0]
                raise ValueError(
                    f"{candidate_id} is split across Parquet row groups; "
                    "rewrite the raw corpus clustered by candidate_cycle_id"
                )
            seen_candidates.update(candidate_ids)
            input_pages += len(frame)
            prepared = _prepare_pages(
                frame,
                mattr_window=mattr_window,
                ngrams=ngrams,
                validate_raw_counts=validate_raw_counts,
            )
            rows = _unscored_snapshot_rows(prepared)
            pending.append(rows)
            pending_documents += int(rows._topic_document.notna().sum())
            if topic_model is None or pending_documents >= topic_batch_size:
                flush()
            if (index + 1) % 500 == 0 or index + 1 == pf.num_row_groups:
                print(
                    f"  row groups {index + 1:,}/{pf.num_row_groups:,}; "
                    f"raw pages {input_pages:,}; output rows {output_rows:,}",
                    flush=True,
                )
        flush()
        if not seen_keys:
            raise ValueError("raw corpus contains no snapshot pages")
        writer.close()
        writer = None
        temp_path.chmod(0o644)
        os.replace(temp_path, out)
    except Exception:
        if writer is not None:
            writer.close()
        temp_path.unlink(missing_ok=True)
        raise

    stats = {
        "input_pages": input_pages,
        "candidate_years": len(seen_candidates),
        "snapshot_rows": output_rows,
    }
    print(
        f"wrote {out}: {output_rows:,} snapshot-day-scope rows from "
        f"{input_pages:,} raw pages and {len(seen_candidates):,} candidate-years"
    )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        default=Path("build/release_candidate/raw_corpus.parquet"),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("build/release_candidate/panel_snapshot_day.parquet"),
    )
    parser.add_argument("--mattr-window", type=int, default=200)
    parser.add_argument(
        "--ngrams",
        type=Path,
        default=Path("data/external/ngrams_en_2008.csv"),
    )
    parser.add_argument(
        "--no-entropy",
        action="store_true",
        help="leave daily entropy missing instead of loading the n-gram table",
    )
    parser.add_argument(
        "--manifesto",
        type=Path,
        default=Path("data/external/us_manifesto_2021-1.json"),
    )
    parser.add_argument(
        "--mapping",
        type=Path,
        default=Path("data/external/sub_topics_mapping.csv"),
    )
    parser.add_argument(
        "--skip-topics",
        action="store_true",
        help="retain 31 missing day_topic columns with topic_status=not_computed",
    )
    parser.add_argument("--topic-batch-size", type=int, default=400)
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    names = topic_names_from_mapping(args.mapping)
    ngrams = None
    if not args.no_entropy:
        if not args.ngrams.exists():
            parser.error(f"n-gram entropy file is absent: {args.ngrams}")
        ngrams = load_ngrams(args.ngrams)

    model = None
    if not args.skip_topics:
        if not args.manifesto.exists():
            parser.error(f"Manifesto training corpus is absent: {args.manifesto}")
        model = train_topic_model(args.manifesto, args.mapping)

    build_snapshot_day_panel(
        args.corpus,
        args.out,
        mattr_window=args.mattr_window,
        ngrams=ngrams,
        topic_model=model,
        topic_names=names,
        topic_batch_size=args.topic_batch_size,
        compression=args.compression,
        overwrite=args.overwrite,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
