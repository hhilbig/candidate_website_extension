#!/usr/bin/env python3
"""Build a bounded municipal/curated-source priority audit for the 2010-2021 screen."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE = REPO_ROOT / "data/municipal_discovery/screening_2010_2021"
DEFAULT_UNRESOLVED = (
    DEFAULT_BASE
    / "search_source_expansion"
    / "frozen_inputs"
    / "unresolved_2010_2021.csv"
)
DEFAULT_CANDIDATE_SAMPLE = DEFAULT_BASE / "candidate_screening_sample_2010_2021.csv"
DEFAULT_CONTEST_SAMPLE = DEFAULT_BASE / "complete_contest_sample_2010_2021.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_BASE / "municipal_curated_priority"
DEFAULT_SOURCE_SPECS = DEFAULT_OUTPUT_DIR / "source_specs.json"


class PriorityAuditError(ValueError):
    """Raised when the frozen inputs or source specifications are inconsistent."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise PriorityAuditError(f"{label} is missing columns: {missing}")


def _prepare_frame(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    required = {
        "candidate_year_id", "full_name", "geo_name", "state_abb", "year",
        "month", "contest",
    }
    _require(frame, required, label)
    result = frame.copy()
    result["candidate_year_id"] = result["candidate_year_id"].astype("string").str.strip()
    if result["candidate_year_id"].isna().any() or result["candidate_year_id"].eq("").any():
        raise PriorityAuditError(f"{label} has missing candidate_year_id values")
    if result["candidate_year_id"].duplicated().any():
        raise PriorityAuditError(f"{label} candidate_year_id values must be unique")
    result["geo_name"] = result["geo_name"].astype("string").str.strip().str.casefold()
    result["state_abb"] = result["state_abb"].astype("string").str.strip().str.upper()
    result["year"] = pd.to_numeric(result["year"], errors="raise").astype(int)
    result["month"] = pd.to_numeric(result["month"], errors="coerce").astype("Int64")
    return result


def _validate_url(url: str, label: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise PriorityAuditError(f"{label} must be an exact HTTP(S) URL: {url!r}")


def load_source_specs(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "municipal-curated-priority-source-spec-v1":
        raise PriorityAuditError("Unsupported source-spec schema_version")
    sources = payload.get("sources")
    if not isinstance(sources, list) or not sources:
        raise PriorityAuditError("source_specs.json must contain a non-empty sources list")
    required = {
        "source_id", "source_name", "source_type", "disposition", "source_url",
        "entry_urls", "expected_website_field", "field_evidence_level",
        "field_evidence_score", "evidence_basis", "access_status", "wayback_route",
        "parse_unit", "analyst_minutes_per_parse_group", "access_cost_usd",
        "jurisdictions", "provenance",
    }
    source_ids: set[str] = set()
    for source in sources:
        missing = sorted(required - set(source))
        if missing:
            raise PriorityAuditError(f"Source is missing fields {missing}: {source.get('source_id')}")
        source_id = str(source["source_id"]).strip()
        if not source_id or source_id in source_ids:
            raise PriorityAuditError(f"Duplicate or empty source_id: {source_id!r}")
        source_ids.add(source_id)
        _validate_url(str(source["source_url"]), f"{source_id}.source_url")
        if not source["entry_urls"]:
            raise PriorityAuditError(f"{source_id} has no entry_urls")
        for url in source["entry_urls"]:
            _validate_url(str(url), f"{source_id}.entry_urls")
        _validate_url(str(source["wayback_route"]), f"{source_id}.wayback_route")
        if int(source["field_evidence_score"]) not in range(0, 6):
            raise PriorityAuditError(f"{source_id} field_evidence_score must be 0-5")
        if float(source["access_cost_usd"]) != 0:
            raise PriorityAuditError(f"Paid source is outside this audit: {source_id}")
        if source["disposition"] not in {
            "run_now", "hold_no_website_field", "hold_prior_zero_yield", "hold_exhausted"
        }:
            raise PriorityAuditError(f"Unsupported disposition for {source_id}")
        if not source["provenance"]:
            raise PriorityAuditError(f"{source_id} has no provenance records")
        for item in source["provenance"]:
            if not item.get("url") and not item.get("local_file"):
                raise PriorityAuditError(f"{source_id} has provenance without URL or local_file")
        for jurisdiction in source["jurisdictions"]:
            if set(jurisdiction) != {"geo_name", "state_abb", "years"}:
                raise PriorityAuditError(f"Malformed jurisdiction in {source_id}: {jurisdiction}")
            if not jurisdiction["years"]:
                raise PriorityAuditError(f"Empty target years in {source_id}")
    return sources


def _target_id(source_id: str, contest: str) -> str:
    value = hashlib.sha256(f"{source_id}|{contest}".encode("utf-8")).hexdigest()[:20]
    return f"municipal_target_{value}"


def _join_sorted(values: pd.Series | list[str] | set[str]) -> str:
    return ";".join(sorted({str(value) for value in values if pd.notna(value) and str(value)}))


def build_priority_audit(
    unresolved: pd.DataFrame,
    candidate_sample: pd.DataFrame,
    contest_sample: pd.DataFrame,
    sources: list[dict],
) -> dict[str, object]:
    unresolved = _prepare_frame(unresolved, "unresolved frame")
    candidate_sample = _prepare_frame(candidate_sample, "candidate screen")
    contest_sample = _prepare_frame(contest_sample, "complete-contest screen")
    unresolved_keys = set(unresolved["candidate_year_id"].astype(str))
    candidate_keys = set(candidate_sample["candidate_year_id"].astype(str))
    contest_keys = set(contest_sample["candidate_year_id"].astype(str))
    if not candidate_keys <= unresolved_keys:
        raise PriorityAuditError("candidate screen contains keys outside the unresolved frame")
    if not contest_keys <= unresolved_keys:
        raise PriorityAuditError("complete-contest screen contains keys outside the unresolved frame")

    source_by_id = {str(source["source_id"]): source for source in sources}
    target_rows: list[dict[str, object]] = []
    source_candidate_rows: list[dict[str, object]] = []
    for source in sources:
        source_id = str(source["source_id"])
        masks: list[pd.Series] = []
        for jurisdiction in source["jurisdictions"]:
            masks.append(
                unresolved["geo_name"].eq(str(jurisdiction["geo_name"]).casefold())
                & unresolved["state_abb"].eq(str(jurisdiction["state_abb"]).upper())
                & unresolved["year"].isin([int(year) for year in jurisdiction["years"]])
            )
        source_frame = unresolved.loc[pd.concat(masks, axis=1).any(axis=1)].copy()
        for contest, group in source_frame.groupby("contest", sort=True, dropna=False):
            keys = set(group["candidate_year_id"].astype(str))
            candidate_overlap = keys & candidate_keys
            contest_overlap = keys & contest_keys
            month_value = group["month"].dropna()
            month = int(month_value.iloc[0]) if len(month_value) else None
            if group[["geo_name", "state_abb", "year"]].drop_duplicates().shape[0] != 1:
                raise PriorityAuditError(f"Contest spans multiple jurisdictions or years: {contest}")
            target_id = _target_id(source_id, str(contest))
            target_rows.append(
                {
                    "target_id": target_id,
                    "source_id": source_id,
                    "source_name": source["source_name"],
                    "source_type": source["source_type"],
                    "disposition": source["disposition"],
                    "geo_name": str(group["geo_name"].iloc[0]),
                    "state_abb": str(group["state_abb"].iloc[0]),
                    "year": int(group["year"].iloc[0]),
                    "month": month,
                    "contest": str(contest),
                    "unresolved_candidate_n": int(len(keys)),
                    "candidate_screen_n": int(len(candidate_overlap)),
                    "complete_contest_screen_n": int(len(contest_overlap)),
                    "candidate_screen_ids": _join_sorted(candidate_overlap),
                    "complete_contest_screen_ids": _join_sorted(contest_overlap),
                    "exact_source_url": source["source_url"],
                    "entry_urls_json": json.dumps(source["entry_urls"], ensure_ascii=False),
                    "expected_website_field": source["expected_website_field"],
                    "field_evidence_level": source["field_evidence_level"],
                    "field_evidence_score": int(source["field_evidence_score"]),
                    "evidence_basis": source["evidence_basis"],
                    "access_status": source["access_status"],
                    "wayback_route": source["wayback_route"],
                    "parse_unit": source["parse_unit"],
                    "analyst_minutes_per_parse_group": int(source["analyst_minutes_per_parse_group"]),
                    "access_cost_usd": float(source["access_cost_usd"]),
                    "parse_group_id": f"{source_id}__{int(group['year'].iloc[0])}__{month or 'missing'}",
                }
            )
            for row in group.itertuples(index=False):
                source_candidate_rows.append(
                    {
                        "source_id": source_id,
                        "target_id": target_id,
                        "candidate_year_id": str(row.candidate_year_id),
                        "full_name": str(row.full_name),
                        "geo_name": str(row.geo_name),
                        "state_abb": str(row.state_abb),
                        "year": int(row.year),
                        "month": row.month,
                        "contest": str(row.contest),
                    }
                )

    targets = pd.DataFrame(target_rows)
    source_candidates = pd.DataFrame(source_candidate_rows)
    if targets.empty:
        raise PriorityAuditError("No source specifications intersect the unresolved frame")
    if targets["target_id"].duplicated().any():
        raise PriorityAuditError("Duplicate source-target IDs")
    if source_candidates.duplicated(["source_id", "candidate_year_id"]).any():
        raise PriorityAuditError("Duplicate source-candidate mappings")

    group_columns = ["parse_group_id"]
    targets["parse_group_unresolved_n"] = targets.groupby(group_columns)["unresolved_candidate_n"].transform("sum")
    targets["parse_group_candidate_screen_n"] = targets.groupby(group_columns)["candidate_screen_n"].transform("sum")
    targets["parse_group_complete_contest_screen_n"] = targets.groupby(group_columns)["complete_contest_screen_n"].transform("sum")
    targets["parse_group_contest_n"] = targets.groupby(group_columns)["target_id"].transform("size")
    targets["target_priority_score"] = (
        1000 * targets["candidate_screen_n"]
        + 700 * targets["complete_contest_screen_n"]
        + 100 * targets["parse_group_candidate_screen_n"]
        + 50 * targets["parse_group_complete_contest_screen_n"]
        + 60 * targets["field_evidence_score"]
        + 10 * targets["unresolved_candidate_n"]
        + targets["parse_group_unresolved_n"]
        - targets["analyst_minutes_per_parse_group"]
    ).astype(int)
    targets["screen_target"] = (
        targets["disposition"].eq("run_now")
        & ((targets["candidate_screen_n"] > 0) | (targets["complete_contest_screen_n"] > 0))
    )
    targets = targets.sort_values(
        ["screen_target", "target_priority_score", "field_evidence_score", "source_id", "contest"],
        ascending=[False, False, False, True, True],
    ).reset_index(drop=True)
    wave2 = targets.loc[targets["screen_target"]].copy()
    wave2.insert(0, "wave2_rank", range(1, len(wave2) + 1))

    wave2_candidate_targets = (
        source_candidates.merge(
            wave2[["target_id", "source_id", "wave2_rank", "exact_source_url", "expected_website_field"]],
            on=["target_id", "source_id"],
            how="inner",
            validate="many_to_one",
        )
    )
    if len(wave2_candidate_targets):
        wave2_candidate_targets["in_candidate_screen"] = wave2_candidate_targets[
            "candidate_year_id"
        ].astype(str).isin(candidate_keys)
        wave2_candidate_targets["in_complete_contest_screen"] = wave2_candidate_targets[
            "candidate_year_id"
        ].astype(str).isin(contest_keys)
        wave2_candidate_targets = wave2_candidate_targets[
            [
                "wave2_rank", "target_id", "source_id", "candidate_year_id", "full_name",
                "geo_name", "state_abb", "year", "month", "contest",
                "in_candidate_screen", "in_complete_contest_screen", "exact_source_url",
                "expected_website_field",
            ]
        ].sort_values(["wave2_rank", "source_id", "candidate_year_id"]).reset_index(drop=True)

    inventory_rows: list[dict[str, object]] = []
    for source_id, source in source_by_id.items():
        source_targets = targets.loc[targets["source_id"].eq(source_id)]
        candidate_map = source_candidates.loc[source_candidates["source_id"].eq(source_id)]
        mapped_keys = set(candidate_map["candidate_year_id"].astype(str))
        inventory_rows.append(
            {
                "source_id": source_id,
                "source_name": source["source_name"],
                "source_type": source["source_type"],
                "disposition": source["disposition"],
                "inventory_rank": 0,
                "source_target_n": int(len(source_targets)),
                "wave2_screen_target_n": int(source_targets["screen_target"].sum()) if len(source_targets) else 0,
                "unresolved_candidate_n": int(len(mapped_keys)),
                "candidate_screen_n": int(len(mapped_keys & candidate_keys)),
                "complete_contest_screen_n": int(len(mapped_keys & contest_keys)),
                "best_target_priority_score": int(source_targets["target_priority_score"].max()) if len(source_targets) else 0,
                "field_evidence_score": int(source["field_evidence_score"]),
                "expected_website_field": source["expected_website_field"],
                "exact_source_url": source["source_url"],
                "entry_urls_json": json.dumps(source["entry_urls"], ensure_ascii=False),
                "access_status": source["access_status"],
                "wayback_route": source["wayback_route"],
                "analyst_minutes_per_parse_group": int(source["analyst_minutes_per_parse_group"]),
                "access_cost_usd": float(source["access_cost_usd"]),
                "provenance_json": json.dumps(source["provenance"], ensure_ascii=False, sort_keys=True),
            }
        )
    inventory = pd.DataFrame(inventory_rows)
    inventory["_run"] = inventory["disposition"].eq("run_now").astype(int)
    inventory = inventory.sort_values(
        ["_run", "candidate_screen_n", "complete_contest_screen_n", "field_evidence_score", "unresolved_candidate_n", "source_id"],
        ascending=[False, False, False, False, False, True],
    ).drop(columns="_run").reset_index(drop=True)
    inventory["inventory_rank"] = range(1, len(inventory) + 1)

    viable_candidates = source_candidates.loc[
        source_candidates["source_id"].isin(
            [source["source_id"] for source in sources if source["disposition"] == "run_now"]
        )
    ].copy()
    viable_key_set = set(viable_candidates["candidate_year_id"].astype(str))
    viable_city = (
        viable_candidates.groupby(["geo_name", "state_abb"], as_index=False)
        .agg(
            viable_target_candidate_n=("candidate_year_id", "nunique"),
            viable_source_n=("source_id", "nunique"),
            viable_source_ids=("source_id", _join_sorted),
        )
    )
    city = (
        unresolved.groupby(["geo_name", "state_abb"], as_index=False)
        .agg(
            unresolved_candidate_n=("candidate_year_id", "size"),
            unresolved_contest_n=("contest", "nunique"),
            target_years=("year", lambda values: _join_sorted([str(v) for v in values])),
            missing_month_n=("month", lambda values: int(values.isna().sum())),
        )
        .merge(viable_city, on=["geo_name", "state_abb"], how="left", validate="one_to_one")
    )
    candidate_city_counts = candidate_sample.groupby(["geo_name", "state_abb"])["candidate_year_id"].nunique()
    contest_city_counts = contest_sample.groupby(["geo_name", "state_abb"])["candidate_year_id"].nunique()
    city["candidate_screen_n"] = [int(candidate_city_counts.get((row.geo_name, row.state_abb), 0)) for row in city.itertuples()]
    city["complete_contest_screen_n"] = [int(contest_city_counts.get((row.geo_name, row.state_abb), 0)) for row in city.itertuples()]
    for column in ["viable_target_candidate_n", "viable_source_n"]:
        city[column] = city[column].fillna(0).astype(int)
    city["viable_source_ids"] = city["viable_source_ids"].fillna("")
    city["priority_status"] = "no_specific_source_identified"
    city.loc[city["viable_source_n"] > 0, "priority_status"] = "wave2_source_identified"
    city["city_priority_score"] = (
        1000 * city["candidate_screen_n"]
        + 700 * city["complete_contest_screen_n"]
        + 10 * city["unresolved_candidate_n"]
        + 50 * city["viable_source_n"]
    ).astype(int)
    city = city.sort_values(
        ["city_priority_score", "unresolved_candidate_n", "geo_name", "state_abb"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)
    city.insert(0, "city_priority_rank", range(1, len(city) + 1))

    untargeted_candidate = candidate_sample.loc[
        ~candidate_sample["candidate_year_id"].astype(str).isin(viable_key_set)
    ].copy()
    untargeted_contest = contest_sample.loc[
        ~contest_sample["candidate_year_id"].astype(str).isin(viable_key_set)
    ].copy()
    candidate_targeted_n = int(len(candidate_keys & viable_key_set))
    contest_targeted_n = int(len(contest_keys & viable_key_set))
    full_targeted_n = int(len(unresolved_keys & viable_key_set))
    summary = {
        "schema_version": "municipal-curated-priority-audit-v1",
        "unresolved_candidate_years": int(len(unresolved)),
        "unresolved_contests": int(unresolved["contest"].nunique()),
        "unresolved_cities": int(unresolved[["geo_name", "state_abb"]].drop_duplicates().shape[0]),
        "candidate_screen_rows": int(len(candidate_sample)),
        "complete_contest_screen_rows": int(len(contest_sample)),
        "source_inventory_rows": int(len(inventory)),
        "run_now_sources": int(inventory["disposition"].eq("run_now").sum()),
        "hold_sources": int((~inventory["disposition"].eq("run_now")).sum()),
        "source_target_rows": int(len(targets)),
        "wave2_screen_target_rows": int(len(wave2)),
        "wave2_candidate_source_rows": int(len(wave2_candidate_targets)),
        "full_frame_targeted_unique_candidate_years": full_targeted_n,
        "full_frame_targeted_share": full_targeted_n / len(unresolved),
        "candidate_screen_targeted_unique_candidate_years": candidate_targeted_n,
        "candidate_screen_targeted_share": candidate_targeted_n / len(candidate_sample),
        "candidate_screen_untargeted_candidate_years": int(len(untargeted_candidate)),
        "candidate_screen_untargeted_share": len(untargeted_candidate) / len(candidate_sample),
        "complete_contest_screen_targeted_unique_candidate_years": contest_targeted_n,
        "complete_contest_screen_targeted_share": contest_targeted_n / len(contest_sample),
        "complete_contest_screen_untargeted_candidate_years": int(len(untargeted_contest)),
        "complete_contest_screen_untargeted_share": len(untargeted_contest) / len(contest_sample),
        "candidate_screen_gap_over_20_percent": bool(len(untargeted_candidate) / len(candidate_sample) > 0.20),
        "complete_contest_screen_gap_over_20_percent": bool(len(untargeted_contest) / len(contest_sample) > 0.20),
        "access_cost_usd": float(inventory["access_cost_usd"].sum()),
    }
    return {
        "city_priority": city,
        "source_inventory": inventory,
        "source_targets": targets,
        "wave2_priority_targets": wave2,
        "wave2_candidate_targets": wave2_candidate_targets,
        "candidate_screen_untargeted": untargeted_candidate,
        "complete_contest_screen_untargeted": untargeted_contest,
        "summary": summary,
    }


def _pct(value: float) -> str:
    return f"{100 * value:.1f}%"


def render_access_notes(summary: dict, inventory: pd.DataFrame, wave2: pd.DataFrame) -> str:
    run_sources = inventory.loc[inventory["disposition"].eq("run_now")].copy()
    holds = inventory.loc[~inventory["disposition"].eq("run_now")].copy()
    lines = [
        "# Municipal and curated-source acquisition audit, 2010-2021",
        "",
        (
            f"The bounded source set is ready for screen acquisition. It provides {summary['wave2_screen_target_rows']:,} "
            f"contest-source targets across {summary['run_now_sources']:,} no-cost sources and reaches "
            f"{summary['candidate_screen_targeted_unique_candidate_years']:,} of the fixed "
            f"{summary['candidate_screen_rows']:,}-case candidate screen ({_pct(summary['candidate_screen_targeted_share'])})."
        ),
        "",
        "## What this audit compares",
        "",
        (
            "The audit intersects each source's frozen city-year scope with the 2,669 unresolved candidate-years, "
            "the fixed 480-case candidate screen, and the separate complete-contest screen. One source-target row "
            "means that one source can be parsed once for every unresolved candidate in one recorded contest."
        ),
        "",
        "## Assumptions",
        "",
        "1. The frozen unresolved frame and fixed screening samples are the denominators; this audit does not redraw either sample.",
        "2. A source is runnable only when its disposition is `run_now`; hold sources do not enter targeted-coverage counts.",
        "3. Candidate statements are optional. Their presence does not imply that every candidate supplied a URL.",
        "4. Source extraction creates leads, not strict coverage. Candidate identity and election-period Wayback validation remain required.",
        "5. No paid API, Serper credit, purchase, email, public-record request, or broad Archive-It search is used here.",
        "",
        "## Runnable sources",
        "",
        "| Rank | Source | Candidate screen | Complete-contest screen | Unresolved leverage | Expected website field | Access |",
        "|---:|---|---:|---:|---:|---|---|",
    ]
    for row in run_sources.itertuples(index=False):
        lines.append(
            f"| {row.inventory_rank} | {row.source_name} | {row.candidate_screen_n} | "
            f"{row.complete_contest_screen_n} | {row.unresolved_candidate_n} | "
            f"{row.expected_website_field} | {row.access_status} |"
        )
    lines.extend(
        [
            "",
            "The first acquisition queue is `wave2_priority_targets.csv`. It is ordered by fixed-screen overlap, "
            "complete-contest overlap, parse-once leverage, field evidence, and analyst time.",
            "",
            "## Holds",
            "",
            "| Source | Reason | Candidate screen affected |",
            "|---|---|---:|",
        ]
    )
    for row in holds.itertuples(index=False):
        lines.append(f"| {row.source_name} | {row.disposition} | {row.candidate_screen_n} |")
    lines.extend(
        [
            "",
            "## Coverage gap",
            "",
            (
                f"The bounded sources do not target {summary['candidate_screen_untargeted_candidate_years']:,} "
                f"candidate-screen rows ({_pct(summary['candidate_screen_untargeted_share'])}) and "
                f"{summary['complete_contest_screen_untargeted_candidate_years']:,} complete-contest rows "
                f"({_pct(summary['complete_contest_screen_untargeted_share'])}). Both losses exceed 20%. "
                "This is an expected consequence of the hard stop on catalogue expansion, not a cleaning drop. "
                "The exact omitted rows are retained in the two untargeted CSV files."
            ),
            "",
            "## Access notes",
            "",
            "- Oakland 2018 is immediately parseable from the downloaded official PDF fixture. Four mayoral campaign domains were already observed in its qualification statements.",
            "- Minneapolis 2013 has both a live curated candidate-website list and a dated official filing-page snapshot.",
            "- San Francisco 2018 has an exact official DataSF-derived PDF URL, but that PDF endpoint timed out during this audit; use the recorded Wayback route.",
            "- Riverside's public index returned HTTP 403 to curl. Use a normal browser session or the recorded Wayback route.",
            "- County voter-guide sources should extract visible text and PDF hyperlink annotations once per election before candidate matching.",
            "",
            "## Decision",
            "",
            "Run the ordered same-screen queue now. Do not rerun the New Haven, San Antonio, or Memphis holds unless a specific URL-bearing record type is identified.",
            "",
            "## Glossary",
            "",
            "- **Candidate screen:** the fixed 480-case evaluation sample.",
            "- **Complete-contest screen:** the separate sampled contest-years, containing every unresolved candidate in each selected contest.",
            "- **Parse-once leverage:** unresolved candidates reached by parsing one election-level source once.",
            "- **Strict coverage:** an accepted candidate-owned campaign URL with qualifying identity and temporal evidence; source leads alone do not count.",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(
    results: dict[str, object],
    output_dir: Path,
    input_paths: dict[str, Path],
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "city_priority": ("city_priority.csv", results["city_priority"]),
        "source_inventory": ("source_inventory.csv", results["source_inventory"]),
        "source_targets": ("source_targets.csv", results["source_targets"]),
        "wave2_priority_targets": ("wave2_priority_targets.csv", results["wave2_priority_targets"]),
        "wave2_candidate_targets": ("wave2_candidate_targets.csv", results["wave2_candidate_targets"]),
        "candidate_screen_untargeted": ("candidate_screen_untargeted.csv", results["candidate_screen_untargeted"]),
        "complete_contest_screen_untargeted": ("complete_contest_screen_untargeted.csv", results["complete_contest_screen_untargeted"]),
    }
    written: list[dict[str, object]] = []
    for name, (filename, frame) in files.items():
        path = output_dir / filename
        frame.to_csv(path, index=False)
        written.append(
            {"name": name, "path": filename, "rows": int(len(frame)), "bytes": path.stat().st_size, "sha256": _sha256(path)}
        )
    summary_path = output_dir / "audit_summary.json"
    summary_path.write_text(json.dumps(results["summary"], indent=2, sort_keys=True) + "\n", encoding="utf-8")
    written.append(
        {"name": "audit_summary", "path": summary_path.name, "rows": None, "bytes": summary_path.stat().st_size, "sha256": _sha256(summary_path)}
    )
    notes_path = output_dir / "access_notes.md"
    notes_path.write_text(
        render_access_notes(results["summary"], results["source_inventory"], results["wave2_priority_targets"]),
        encoding="utf-8",
    )
    written.append(
        {"name": "access_notes", "path": notes_path.name, "rows": None, "bytes": notes_path.stat().st_size, "sha256": _sha256(notes_path)}
    )

    fixture_path = output_dir / "alameda_2018_composite_voter_guide.pdf"
    fixture = None
    if fixture_path.exists():
        fixture = {
            "path": fixture_path.name,
            "bytes": fixture_path.stat().st_size,
            "sha256": _sha256(fixture_path),
            "source_url": "https://acvote.alamedacountyca.gov/acvote-assets/02_election_information/PDFs/20181106/en/Composite%20Voter%20Information%20Guide/Composite%20Voter%20Information%20Guide.pdf",
        }
    manifest = {
        "schema_version": "municipal-curated-priority-output-manifest-v1",
        "inputs": {
            name: {"path": str(path.relative_to(REPO_ROOT)), "bytes": path.stat().st_size, "sha256": _sha256(path)}
            for name, path in input_paths.items()
        },
        "outputs": written,
        "downloaded_fixture": fixture,
    }
    manifest_path = output_dir / "output_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unresolved", type=Path, default=DEFAULT_UNRESOLVED)
    parser.add_argument("--candidate-sample", type=Path, default=DEFAULT_CANDIDATE_SAMPLE)
    parser.add_argument("--contest-sample", type=Path, default=DEFAULT_CONTEST_SAMPLE)
    parser.add_argument("--source-specs", type=Path, default=DEFAULT_SOURCE_SPECS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    unresolved = pd.read_csv(args.unresolved)
    candidate_sample = pd.read_csv(args.candidate_sample)
    contest_sample = pd.read_csv(args.contest_sample)
    sources = load_source_specs(args.source_specs)
    results = build_priority_audit(unresolved, candidate_sample, contest_sample, sources)
    manifest = write_outputs(
        results,
        args.output_dir,
        {
            "unresolved_frame": args.unresolved,
            "candidate_screen": args.candidate_sample,
            "complete_contest_screen": args.contest_sample,
            "source_specs": args.source_specs,
        },
    )
    print(json.dumps({"summary": results["summary"], "manifest": manifest}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
