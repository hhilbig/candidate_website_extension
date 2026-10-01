#!/usr/bin/env python3
"""Add contest context to frozen search-pilot tables without new API calls."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import pandas as pd

from scripts.expand_mayoral_search_evidence import merge_evidence_queue


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTEXT_COLUMNS = ["candidate_contest_id", "contest", "month"]


def candidate_context(universe: pd.DataFrame) -> pd.DataFrame:
    required = {"candidate_year_id", *CONTEXT_COLUMNS}
    missing = sorted(required - set(universe.columns))
    if missing:
        raise ValueError(f"Candidate universe lacks context columns: {missing}")
    context = universe[["candidate_year_id", *CONTEXT_COLUMNS]].copy()
    if context["candidate_year_id"].isna().any():
        raise ValueError("Candidate universe has missing candidate_year_id values")
    conflicts = context.groupby("candidate_year_id", dropna=False)[CONTEXT_COLUMNS].nunique(
        dropna=False
    )
    if conflicts.gt(1).any(axis=None):
        bad = conflicts.index[conflicts.gt(1).any(axis=1)].tolist()[:5]
        raise ValueError(f"Candidate universe has conflicting contest context: {bad}")
    return context.drop_duplicates("candidate_year_id")


def attach_context(frame: pd.DataFrame, context: pd.DataFrame, label: str) -> pd.DataFrame:
    if "candidate_year_id" not in frame:
        raise ValueError(f"{label} lacks candidate_year_id")
    original_columns = frame.columns.tolist()
    existing = [column for column in CONTEXT_COLUMNS if column in frame]
    renamed = frame.rename(columns={column: f"{column}__existing" for column in existing})
    merged = renamed.merge(
        context,
        on="candidate_year_id",
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    missing_matches = merged["_merge"].ne("both")
    if missing_matches.any():
        examples = merged.loc[missing_matches, "candidate_year_id"].head(5).tolist()
        raise ValueError(f"{label} has {int(missing_matches.sum())} unmatched candidates: {examples}")
    merged = merged.drop(columns="_merge")
    for column in existing:
        old_column = f"{column}__existing"
        old = merged[old_column]
        new = merged[column]
        old_present = old.notna() & old.astype(str).str.strip().ne("")
        conflicts = old_present & old.astype(str).ne(new.astype(str))
        if conflicts.any():
            examples = merged.loc[conflicts, "candidate_year_id"].head(5).tolist()
            raise ValueError(f"{label} has conflicting {column} values: {examples}")
        merged = merged.drop(columns=old_column)
    ordered = [
        "candidate_year_id",
        *CONTEXT_COLUMNS,
        *[column for column in original_columns if column not in {"candidate_year_id", *CONTEXT_COLUMNS}],
    ]
    return merged[ordered]


def attach_search_metadata(expansion: pd.DataFrame, queue: pd.DataFrame) -> pd.DataFrame:
    """Backfill search-result context that old expansion files did not store."""
    keys = ["candidate_year_id", "evidence_source_url"]
    required = set(keys)
    if not required.issubset(expansion.columns) or not required.issubset(queue.columns):
        raise ValueError("Lead and expansion tables need candidate and evidence-source keys")
    metadata_columns = [
        "query_variant", "result_rank", "exact_year_context", "mentioned_years",
        "temporal_conflict", "office_mismatch_signal", "result_title", "result_snippet",
    ]

    def combine_text(values: pd.Series) -> str:
        clean = [str(value).strip() for value in values if pd.notna(value) and str(value).strip()]
        return " | ".join(dict.fromkeys(clean))

    aggregations = {
        "query_variant": combine_text,
        "result_rank": "min",
        "exact_year_context": "max",
        "mentioned_years": combine_text,
        "temporal_conflict": "max",
        "office_mismatch_signal": "max",
        "result_title": combine_text,
        "result_snippet": combine_text,
    }
    available = [column for column in metadata_columns if column in queue.columns]
    lookup = queue.groupby(keys, as_index=False, dropna=False).agg(
        {column: aggregations[column] for column in available}
    )
    existing = [column for column in available if column in expansion.columns]
    renamed = expansion.rename(columns={column: f"{column}__existing" for column in existing})
    merged = renamed.merge(lookup, on=keys, how="left", validate="many_to_one")
    for column in existing:
        old_column = f"{column}__existing"
        old = merged[old_column]
        old_present = old.notna() & old.astype(str).str.strip().ne("")
        merged.loc[old_present, column] = old.loc[old_present]
        merged = merged.drop(columns=old_column)
    return merged


def repair_pilot_tables(
    query_manifest: pd.DataFrame,
    lead_queue: pd.DataFrame,
    expansion: pd.DataFrame,
    universe: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    context = candidate_context(universe)
    repaired_manifest = attach_context(query_manifest, context, "query manifest")
    repaired_queue = attach_context(lead_queue, context, "lead queue")
    repaired_expansion = attach_context(expansion, context, "source expansion")
    repaired_expansion = attach_search_metadata(repaired_expansion, repaired_queue)
    unified = merge_evidence_queue(repaired_queue, repaired_expansion)
    if len(repaired_manifest) != len(query_manifest):
        raise ValueError("Query-manifest row count changed during context repair")
    if len(repaired_queue) != len(lead_queue):
        raise ValueError("Lead-queue row count changed during context repair")
    if len(repaired_expansion) != len(expansion):
        raise ValueError("Expansion row count changed during context repair")
    missing_month = {
        name: int(frame["month"].isna().sum())
        for name, frame in {
            "query_manifest": repaired_manifest,
            "lead_queue": repaired_queue,
            "source_expansion": repaired_expansion,
        }.items()
    }
    return {
        "query_manifest": repaired_manifest,
        "lead_queue": repaired_queue,
        "source_expansion": repaired_expansion,
        "unified_evidence": unified,
        "missing_month": pd.DataFrame([missing_month]),
    }


def write_csv_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", dir=path.parent, delete=False, encoding="utf-8", newline=""
    ) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False)
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot-dir", type=Path, default=Path("data/municipal_discovery/search_pilot"))
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("data/municipal_discovery/final/mayoral_candidate_universe.csv"),
    )
    args = parser.parse_args()
    outputs = repair_pilot_tables(
        pd.read_csv(args.pilot_dir / "query_manifest.csv"),
        pd.read_csv(args.pilot_dir / "search_lead_review.csv"),
        pd.read_csv(args.pilot_dir / "source_page_expansion.csv"),
        pd.read_csv(args.universe, low_memory=False),
    )
    write_csv_atomic(outputs["query_manifest"], args.pilot_dir / "query_manifest.csv")
    write_csv_atomic(outputs["lead_queue"], args.pilot_dir / "search_lead_review.csv")
    write_csv_atomic(outputs["source_expansion"], args.pilot_dir / "source_page_expansion.csv")
    write_csv_atomic(outputs["unified_evidence"], args.pilot_dir / "search_evidence_review.csv")
    print(json.dumps({
        "query_rows": len(outputs["query_manifest"]),
        "lead_rows": len(outputs["lead_queue"]),
        "expansion_rows": len(outputs["source_expansion"]),
        "unified_rows": len(outputs["unified_evidence"]),
        "unified_candidate_years": int(outputs["unified_evidence"]["candidate_year_id"].nunique()),
        "date_conflict_rows_retained": int(
            outputs["unified_evidence"]["temporal_conflict"].fillna(False).astype(bool).sum()
        ),
        "missing_election_months": outputs["missing_month"].iloc[0].to_dict(),
    }, indent=2))


if __name__ == "__main__":
    main()
