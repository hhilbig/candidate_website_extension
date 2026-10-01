#!/usr/bin/env python3
"""Build the explicit local-parser manifest from the municipal download ledger."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd


SUPPORTED_TYPES = {"csv", "html", "htm", "json", "pdf", "tsv", "xlsx"}


def safe_id(value: object) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "_", str(value).casefold()).strip("_")


def file_type(path: str) -> str:
    kind = Path(path).suffix.casefold().removeprefix(".")
    return "html" if kind == "htm" else kind


def build_manifest(
    inventory: pd.DataFrame,
    downloads: pd.DataFrame,
    universe: pd.DataFrame,
    universe_path: str,
    start_year: int = 2000,
    end_year: int = 2007,
) -> dict:
    required_downloads = {
        "source_id", "source_target_id", "city", "state", "download_status",
        "local_file", "sha256", "final_url",
    }
    missing = required_downloads - set(downloads.columns)
    if missing:
        raise ValueError(f"Download manifest missing columns: {sorted(missing)}")
    downloaded = downloads[downloads["download_status"].eq("downloaded")].copy()
    downloaded["type"] = downloaded["local_file"].map(file_type)
    downloaded = downloaded[downloaded["type"].isin(SUPPORTED_TYPES)]
    inventory_keys = inventory[["source_id", "city", "state"]].astype(str)
    if inventory_keys.duplicated().any():
        raise ValueError("Inventory has duplicate source-city-state targets")

    sources: list[dict] = []
    for inventory_row in inventory.sort_values(["priority_rank", "source_id"]).to_dict("records"):
        source_id = str(inventory_row["source_id"])
        city, state = str(inventory_row["city"]), str(inventory_row["state"])
        source_target_id = f"{source_id}__{safe_id(city)}__{state.casefold()}"
        files = downloaded[downloaded["source_target_id"].eq(source_target_id)]
        target_downloads = downloads[downloads["source_target_id"].eq(source_target_id)]
        upstream_status = ""
        upstream_error = ""
        if files.empty:
            upstream_status = (
                "request_error"
                if target_downloads["download_status"].eq("request_error").any()
                else "source_unavailable"
            )
            if "error" in target_downloads.columns:
                upstream_error = " | ".join(
                    dict.fromkeys(
                        str(value).strip()
                        for value in target_downloads["error"].dropna()
                        if str(value).strip()
                    )
                )[:2000]
        scoped = universe[
            universe["geo_name"].astype(str).str.casefold().eq(city.casefold())
            & universe["state_abb"].astype(str).str.casefold().eq(state.casefold())
            & universe["year"].between(start_year, end_year)
            & universe["in_discovery_scope"].astype("boolean").fillna(False)
        ]
        if pd.notna(inventory_row.get("target_years")) and str(inventory_row.get("target_years")).strip():
            from scripts.fetch_municipal_record_sources import _parse_target_years
            requested_years = _parse_target_years(inventory_row["target_years"])
            available_years = set(scoped["year"].astype(int))
            unavailable = requested_years - available_years
            if unavailable:
                raise ValueError(
                    f"Target years absent from scoped universe for {city}, {state}: {sorted(unavailable)}"
                )
            scoped = scoped[scoped["year"].astype(int).isin(requested_years)]
        for year in sorted(scoped["year"].astype(int).unique()):
            local_files = [
                {
                    "path": str(row.local_file),
                    "type": str(row.type),
                    "file_id": f"sha256:{row.sha256}",
                }
                for row in files.sort_values(["local_file", "final_url"]).itertuples()
            ]
            source = {
                "source_id": f"{safe_id(source_id)}.{safe_id(city)}.{state.casefold()}.{year}",
                "source_name": f"{source_id} ({city}, {state})",
                "source_url": str(inventory_row["official_url"]),
                "source_date": "",
                "geo_name": city,
                "state_abb": state,
                "year": int(year),
                "local_files": local_files,
            }
            if upstream_status:
                source["upstream_status"] = upstream_status
                source["upstream_error"] = upstream_error or (
                    f"No supported local file was recovered for {source_target_id}"
                )
            sources.append(source)
    ids = [source["source_id"] for source in sources]
    if len(ids) != len(set(ids)):
        raise ValueError("Generated municipal parser source IDs are not unique")
    return {
        "manifest_version": "official-municipal-extraction-v1",
        "candidate_universe": universe_path,
        "sources": sources,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--downloads", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year", type=int, default=2007)
    args = parser.parse_args()
    if args.end_year < args.start_year:
        parser.error("--end-year must be at least --start-year")
    manifest = build_manifest(
        pd.read_csv(args.inventory, low_memory=False),
        pd.read_csv(args.downloads, low_memory=False),
        pd.read_csv(args.universe, low_memory=False),
        args.universe,
        args.start_year,
        args.end_year,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "sources": len(manifest["sources"]),
        "unique_local_files": len({
            item["file_id"]
            for source in manifest["sources"] for item in source["local_files"]
        }),
        "output": str(output),
    }, indent=2))


if __name__ == "__main__":
    main()
