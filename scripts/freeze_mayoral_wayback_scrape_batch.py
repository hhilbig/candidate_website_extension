#!/usr/bin/env python3
"""Freeze discovered mayoral domains for a separate Wayback scrape pipeline."""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TARGETS = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/openai_search_residual_pilot/production/extracted/candidate_site_targets.csv"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "data/municipal_archive_acquisition/openai_residual_100_20260826/frozen"
)
FREEZE_VERSION = "mayoral-wayback-scrape-freeze-v1"
SCRAPE_METHOD_VERSION = "mayoral-wayback-site-scrape-v1"
EXPECTED_TARGETS = 28
MAX_PAGES_PER_TARGET = 150

FROZEN_COLUMNS = [
    "freeze_contract_version",
    "scrape_method_version",
    "scrape_key",
    "scrape_hash",
    "target_record_hash",
    "target_id",
    "candidate_year_id",
    "candidate_contest_id",
    "contest",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "period",
    "registrable_domain",
    "site_root_url",
    "primary_exact_url",
    "url_type",
    "campaign_claim_count",
    "campaign_claim_ids_json",
    "campaign_urls_json",
    "evidence_urls_json",
    "election_reference_date",
    "scrape_window_start",
    "scrape_window_end",
    "url_variants_json",
    "max_pages_per_target",
]


class MayoralScrapeFreezeError(RuntimeError):
    pass


def _text(value: object) -> str:
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return ""
    return str(value).strip()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def _canonical_hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False, lineterminator="\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_json(value: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _shift_months(value: date, months: int) -> date:
    index = value.year * 12 + value.month - 1 + months
    year, month0 = divmod(index, 12)
    month = month0 + 1
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def _variants(site_root_url: str, registrable_domain: str) -> list[str]:
    parsed = urlsplit(site_root_url)
    host = (parsed.hostname or "").casefold()
    domain = registrable_domain.casefold()
    if not domain or not host:
        raise MayoralScrapeFreezeError(f"invalid site root: {site_root_url!r}")
    hosts = [host]
    if host in {domain, f"www.{domain}"}:
        hosts = [domain, f"www.{domain}"]
    return [f"{scheme}://{candidate_host}/" for scheme in ("http", "https") for candidate_host in hosts]


def freeze_targets(
    targets: pd.DataFrame,
    *,
    expected_targets: int = EXPECTED_TARGETS,
) -> tuple[pd.DataFrame, dict[str, object]]:
    required = {
        "target_id", "candidate_year_id", "candidate_contest_id", "contest",
        "full_name", "geo_name", "state_abb", "year", "month", "period",
        "registrable_domain", "site_root_url", "primary_exact_url", "url_type",
        "campaign_claim_count", "campaign_claim_ids_json", "campaign_urls_json",
        "evidence_urls_json",
    }
    missing = sorted(required - set(targets.columns))
    if missing:
        raise MayoralScrapeFreezeError(f"candidate targets lack columns: {missing}")
    if len(targets) != expected_targets or targets["target_id"].map(_text).duplicated().any():
        raise MayoralScrapeFreezeError(f"expected exactly {expected_targets} unique discovery targets")
    rows: list[dict[str, object]] = []
    for target in targets.to_dict("records"):
        year = int(target["year"])
        month_text = _text(target["month"])
        if not month_text:
            raise MayoralScrapeFreezeError(f"missing election month for {target['target_id']}")
        month = int(float(month_text))
        if not 1 <= month <= 12:
            raise MayoralScrapeFreezeError(f"invalid election month for {target['target_id']}")
        reference = date(year, month, 15)
        month_start = date(year, month, 1)
        month_end = date(year, month, calendar.monthrange(year, month)[1])
        window_start = _shift_months(month_start, -18)
        window_end = month_end + timedelta(days=120)
        scrape_key = "\x1f".join([
            _text(target["target_id"]),
            _text(target["candidate_year_id"]),
            _text(target["registrable_domain"]),
        ])
        target_payload = {key: _text(value) for key, value in target.items()}
        rows.append({
            "freeze_contract_version": FREEZE_VERSION,
            "scrape_method_version": SCRAPE_METHOD_VERSION,
            "scrape_key": scrape_key,
            "scrape_hash": hashlib.sha256(scrape_key.encode("utf-8")).hexdigest(),
            "target_record_hash": _canonical_hash(target_payload),
            **{column: target.get(column, "") for column in FROZEN_COLUMNS if column in target},
            "year": year,
            "month": month,
            "election_reference_date": reference.isoformat(),
            "scrape_window_start": window_start.isoformat(),
            "scrape_window_end": window_end.isoformat(),
            "url_variants_json": json.dumps(
                _variants(_text(target["site_root_url"]), _text(target["registrable_domain"])),
                separators=(",", ":"),
            ),
            "max_pages_per_target": MAX_PAGES_PER_TARGET,
        })
    frozen = pd.DataFrame(rows).reindex(columns=FROZEN_COLUMNS)
    frozen = frozen.sort_values(["scrape_hash", "target_id"], kind="stable").reset_index(drop=True)
    if frozen["scrape_key"].duplicated().any() or frozen["scrape_hash"].duplicated().any():
        raise MayoralScrapeFreezeError("scrape keys are not unique")
    audit = {
        "freeze_contract_version": FREEZE_VERSION,
        "scrape_method_version": SCRAPE_METHOD_VERSION,
        "target_rows": len(frozen),
        "candidate_years": int(frozen["candidate_year_id"].nunique()),
        "registrable_domains": int(frozen["registrable_domain"].nunique()),
        "missing_election_months": int(frozen["month"].map(_text).eq("").sum()),
        "max_pages_per_target": MAX_PAGES_PER_TARGET,
        "handoff_unit": "candidate-year x discovered registrable domain",
        "discovery_fields_are_not_recomputed": True,
    }
    return frozen, audit


def build(
    targets_path: Path = DEFAULT_TARGETS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    *,
    expected_targets: int = EXPECTED_TARGETS,
    defer_missing_months: bool = False,
) -> dict[str, object]:
    targets_path = targets_path.resolve()
    output_dir = output_dir.resolve()
    targets = pd.read_csv(targets_path, keep_default_na=False, low_memory=False)
    deferred = targets.loc[targets["month"].map(_text).eq("")].copy()
    if len(deferred) and not defer_missing_months:
        raise MayoralScrapeFreezeError(
            f"missing election month for {deferred.iloc[0]['target_id']}"
        )
    active = targets.loc[~targets["month"].map(_text).eq("")].copy() if defer_missing_months else targets
    frozen, audit = freeze_targets(active, expected_targets=expected_targets)
    frozen_path = output_dir / "targets.csv"
    _atomic_csv(frozen, frozen_path)
    deferred_path = output_dir / "deferred_missing_month.csv"
    if defer_missing_months:
        deferred = deferred.copy()
        deferred["deferred_reason"] = "missing_election_month_no_date_imputation"
        _atomic_csv(deferred, deferred_path)
    audit.update({
        "input": {"path": _display_path(targets_path), "rows": len(targets), "sha256": _sha256_file(targets_path)},
        "output": {"path": _display_path(frozen_path), "rows": len(frozen), "bytes": frozen_path.stat().st_size, "sha256": _sha256_file(frozen_path), "columns": FROZEN_COLUMNS},
        "deferred_missing_month_rows": len(deferred),
        "deferred_missing_month_output": (
            {
                "path": _display_path(deferred_path),
                "rows": len(deferred),
                "bytes": deferred_path.stat().st_size,
                "sha256": _sha256_file(deferred_path),
            }
            if defer_missing_months else None
        ),
        "key_definition": "target_id + U+001F + candidate_year_id + U+001F + registrable_domain",
    })
    _atomic_json(audit, output_dir / "manifest.json")
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=Path, default=DEFAULT_TARGETS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--expected-targets", type=int, default=EXPECTED_TARGETS)
    parser.add_argument("--defer-missing-months", action="store_true")
    args = parser.parse_args()
    print(json.dumps(
        build(
            args.targets,
            args.output_dir,
            expected_targets=args.expected_targets,
            defer_missing_months=args.defer_missing_months,
        ),
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
