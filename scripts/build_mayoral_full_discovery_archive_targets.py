#!/usr/bin/env python3
"""Build one archive-acquisition target per newly discovered candidate-domain."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLAIMS = (
    REPO_ROOT / "data/municipal_discovery/screening_2010_2021/"
    "full_discovery_2010_2021/combined_candidate_list/candidate_url_claims.csv"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT / "data/municipal_archive_acquisition/full_discovery_740_20260826"
)
SOURCE_COLUMNS = [
    "target_id", "candidate_year_id", "candidate_contest_id", "contest",
    "full_name", "geo_name", "state_abb", "year", "month", "period",
    "registrable_domain", "site_root_url", "primary_exact_url", "url_type",
    "campaign_claim_count", "campaign_claim_ids_json", "campaign_urls_json",
    "evidence_urls_json",
]


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _stable_id(*values: object) -> str:
    payload = "\x1f".join(_text(value) for value in values)
    return "full_discovery_target_" + hashlib.sha256(payload.encode()).hexdigest()[:24]


def _period(year: object) -> str:
    value = int(float(_text(year)))
    if 2010 <= value <= 2013:
        return "2010_2013"
    if 2014 <= value <= 2017:
        return "2014_2017"
    if 2018 <= value <= 2021:
        return "2018_2021"
    raise ValueError(f"archive target year is outside 2010--2021: {value}")


def build_targets(claims: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    required = {
        "discovery_claim_id", "source_channel", "candidate_year_id",
        "candidate_contest_id", "contest", "full_name", "geo_name", "state_abb",
        "year", "month", "registrable_domain", "site_root_url",
        "exact_normalized_url", "url_type",
    }
    if missing := sorted(required - set(claims.columns)):
        raise ValueError(f"combined discovery claims lack columns: {missing}")
    new = claims.loc[claims["source_channel"].ne("existing_unified_claim")].copy()
    if new.empty:
        raise ValueError("combined discovery claims contain no new acquisition targets")
    if new["discovery_claim_id"].map(_text).duplicated().any():
        raise ValueError("new discovery claim IDs are duplicated")
    if new["candidate_year_id"].map(_text).eq("").any() or new["registrable_domain"].map(_text).eq("").any():
        raise ValueError("new discovery claims have blank candidate/domain keys")
    rows: list[dict[str, object]] = []
    for (candidate_id, domain), group in new.groupby(
        ["candidate_year_id", "registrable_domain"], sort=True
    ):
        context_columns = [
            "candidate_contest_id", "contest", "full_name", "geo_name",
            "state_abb", "year", "month",
        ]
        for column in context_columns:
            if group[column].map(_text).nunique() != 1:
                raise ValueError(f"candidate-domain context conflict in {column}: {candidate_id} / {domain}")
        first = group.sort_values(
            ["source_channel", "discovery_claim_id"], kind="stable"
        ).iloc[0]
        urls = list(dict.fromkeys(group["exact_normalized_url"].map(_text)))
        roots = list(dict.fromkeys(group["site_root_url"].map(_text)))
        claim_ids = list(dict.fromkeys(group["discovery_claim_id"].map(_text)))
        url_types = list(dict.fromkeys(group["url_type"].map(_text)))
        preferred_type = next(
            (value for value in ("custom_domain", "candidate_subdomain", "shared_host_path") if value in url_types),
            url_types[0],
        )
        rows.append({
            "target_id": _stable_id(candidate_id, domain),
            "candidate_year_id": candidate_id,
            "candidate_contest_id": first["candidate_contest_id"],
            "contest": first["contest"], "full_name": first["full_name"],
            "geo_name": first["geo_name"], "state_abb": first["state_abb"],
            "year": int(float(_text(first["year"]))), "month": first["month"],
            "period": _period(first["year"]), "registrable_domain": domain,
            "site_root_url": roots[0], "primary_exact_url": urls[0],
            "url_type": preferred_type, "campaign_claim_count": len(group),
            "campaign_claim_ids_json": json.dumps(claim_ids, separators=(",", ":")),
            "campaign_urls_json": json.dumps(urls, ensure_ascii=False, separators=(",", ":")),
            "evidence_urls_json": "[]",
        })
    targets = pd.DataFrame(rows).reindex(columns=SOURCE_COLUMNS)
    if targets["target_id"].duplicated().any() or targets.duplicated(
        ["candidate_year_id", "registrable_domain"]
    ).any():
        raise ValueError("archive target keys are duplicated")
    recovered_claims: set[str] = set()
    for value in targets["campaign_claim_ids_json"]:
        recovered_claims.update(json.loads(value))
    if recovered_claims != set(new["discovery_claim_id"].map(_text)):
        raise ValueError("archive target grouping lost an independent discovery claim")
    audit = {
        "input_claim_rows": len(claims), "new_discovery_claim_rows": len(new),
        "excluded_existing_claim_rows": len(claims) - len(new),
        "excluded_existing_fraction": float((len(claims) - len(new)) / len(claims)),
        "large_drop_explanation": (
            "Existing unified claims are excluded because they already belong to prior "
            "archive-acquisition batches; this follow-on batch consumes only the 740 newly found candidates."
        ),
        "target_rows": len(targets),
        "candidate_years": int(targets["candidate_year_id"].nunique()),
        "candidate_domains": int(targets[["candidate_year_id", "registrable_domain"]].drop_duplicates().shape[0]),
        "source_claims_recovered": len(recovered_claims),
        "missing_election_month_targets": int(targets["month"].map(_text).eq("").sum()),
        "targets_by_period": targets.groupby("period").size().sort_index().astype(int).to_dict(),
        "exact_new_claim_key_agreement": True,
    }
    return targets.sort_values(["candidate_year_id", "registrable_domain"], kind="stable").reset_index(drop=True), audit


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False, lineterminator="\n")
    temporary.replace(path)


def run(claims_path: Path = DEFAULT_CLAIMS, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, object]:
    claims = pd.read_csv(claims_path, keep_default_na=False, low_memory=False)
    targets, audit = build_targets(claims)
    output_dir.mkdir(parents=True, exist_ok=True)
    target_path = output_dir / "source_targets.csv"
    _atomic_csv(targets, target_path)
    audit.update({
        "input_sha256": hashlib.sha256(claims_path.read_bytes()).hexdigest(),
        "output_sha256": hashlib.sha256(target_path.read_bytes()).hexdigest(),
        "output_bytes": target_path.stat().st_size,
    })
    (output_dir / "source_targets_audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claims", type=Path, default=DEFAULT_CLAIMS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    print(json.dumps(run(args.claims, args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
