"""Version the 2026 FEC roster without excluding candidates who lack websites.

URL records are source leads. The collector must check website ownership.
The FEC election year defines the population; ballot participation stays separate.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import tempfile
import time
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests

from .build_candidate_roster import FEC_CANDIDATE_COLUMNS, PARTY_MAP
from .name_utils import clean_name

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FEC_URL = "https://www.fec.gov/files/bulk-downloads/2026/cn26.zip"
PILOT_EVIDENCE = PROJECT_ROOT / "config/collection_2026_pilot_evidence.json"
FIELDS = [
    "candidate_id", "candidate", "fec_raw_name", "committee_id", "state",
    "district", "office", "party", "year", "website_url", "website_urls",
    "url_source", "url_checked_at", "url_lookup_status", "url_lookup_detail",
    "roster_status", "participation_status", "identity_status", "cand_status",
    "first_seen_at", "last_seen_at", "roster_version",
    "pilot_member", "pilot_reason",
]
USER_AGENT = "CandidateWebsiteResearch/2026 (public academic website archive)"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _json(path: Path, value: object) -> None:
    _atomic(path, (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode())


def _record_attempt(path: Path, value: dict) -> None:
    """Keep completed source requests if a later request interrupts the refresh."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _csv_bytes(rows: list[dict]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def load_roster(root: Path) -> list[dict]:
    """Read the last completed roster. Return an empty list before first refresh."""
    path = Path(root) / "rosters" / "current.csv"
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    ids = [row["candidate_id"] for row in rows]
    if len(ids) != len(set(ids)) or not all(ids):
        raise ValueError("Current roster must contain one row per nonempty FEC candidate ID")
    if PILOT_EVIDENCE.exists():
        cases={row['candidate_id']:row for row in json.loads(PILOT_EVIDENCE.read_text()).get('candidates',[])}
        for row in rows:
            if row['candidate_id'] in cases:
                row['pilot_evidence']=cases[row['candidate_id']]
    return rows


def _parse_fec(content: bytes) -> tuple[list[dict], dict]:
    """Reject malformed records. Count each explicit population exclusion."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        members = [name for name in archive.namelist() if Path(name).name.lower() == "cn.txt"]
        if len(members) != 1:
            raise ValueError("FEC ZIP must contain exactly one cn.txt")
        source = archive.read(members[0]).decode("latin-1")
    rows, counts, seen = [], Counter(), {}
    for line, fields in enumerate(csv.reader(io.StringIO(source), delimiter="|"), 1):
        if not fields:
            continue
        counts["raw_rows"] += 1
        if len(fields) != len(FEC_CANDIDATE_COLUMNS):
            raise ValueError(f"FEC row {line} has {len(fields)} fields; expected 15")
        raw = dict(zip(FEC_CANDIDATE_COLUMNS, fields))
        if raw["cand_election_yr"] != "2026":
            counts["other_election_year"] += 1
            continue
        if raw["cand_office"] not in ("H", "S"):
            counts["other_office"] += 1
            continue
        if raw["cand_pty_affiliation"] not in PARTY_MAP:
            counts["other_party"] += 1
            continue
        cid = raw["cand_id"]
        if not re.fullmatch(r"[HS][A-Z0-9]{8}", cid):
            raise ValueError(f"FEC row {line} has an invalid candidate ID")
        if cid in seen:
            if raw != seen[cid]:
                raise ValueError(f"FEC ID {cid} has conflicting duplicate records")
            counts["identical_duplicate_rows"] += 1
            continue
        seen[cid] = raw
        rows.append({
            "candidate_id": cid, "candidate": clean_name(raw["cand_name"]),
            "fec_raw_name": raw["cand_name"], "committee_id": raw["cand_pcc"],
            "state": raw["cand_office_st"], "district": raw["cand_office_district"],
            "office": {"H": "house", "S": "senate"}[raw["cand_office"]],
            "party": PARTY_MAP[raw["cand_pty_affiliation"]], "year": "2026",
            "cand_status": raw["cand_status"],
        })
    counts["eligible_candidates"] = len(rows)
    if not rows:
        raise ValueError("FEC download contains no eligible 2026 candidates")
    return rows, dict(counts)


def _normal_url(value: str) -> str:
    value = (value or "").strip()
    if not value or re.search(r"\s", value) or "@" in value:
        return ""
    value = re.sub(r"^(https?://){2,}", "https://", value, flags=re.I)
    if not re.match(r"https?://", value, flags=re.I):
        value = "https://" + value
    try:
        parts = urlsplit(value)
        host = (parts.hostname or "").lower()
        if parts.scheme.lower() not in ("http", "https") or "." not in host:
            return ""
        if host.endswith(".gov") or any(host == x or host.endswith("." + x) for x in (
            "actblue.com", "winred.com", "anedot.com", "facebook.com", "twitter.com", "x.com",
            "instagram.com", "youtube.com", "youtu.be", "tiktok.com", "linkedin.com",
        )):
            return ""
        # Preserve path case. Some campaign servers use case-sensitive routes.
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, ""))
    except ValueError:
        return ""


def _alternatives(row: dict) -> list[str]:
    value = row.get("website_urls") or "[]"
    urls = json.loads(value) if isinstance(value, str) else value
    if not isinstance(urls, list):
        raise ValueError("website_urls must contain a JSON list")
    return list(dict.fromkeys(url for value in [row.get("website_url", ""), *urls]
                              if (url := _normal_url(str(value)))))


def _legacy_leads() -> dict[str, list[dict]]:
    """Match existing URL leads by FEC ID. Names never establish identity."""
    paths = [PROJECT_ROOT / "build/release_v2_1_candidate/release_roster.csv",
             PROJECT_ROOT / "build/website_v1_1/release_roster.csv",
             PROJECT_ROOT / "build/release_candidate/release_roster.csv"]
    paths += sorted((PROJECT_ROOT / "data/rosters").glob("*.csv"))
    leads: dict[str, list[dict]] = {}
    for path in paths:
        if not path.exists():
            continue
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not {"cand_id", "candidate_id"}.intersection(reader.fieldnames or []):
                continue
            for row in reader:
                cid = row.get("cand_id") or row.get("candidate_id", "")
                url = _normal_url(row.get("website_url", ""))
                if cid and url:
                    leads.setdefault(cid, []).append({
                        "url": url, "source": str(path.relative_to(PROJECT_ROOT)),
                        "year": row.get("year", ""), "status": "historical_lead_unvalidated",
                    })
    # The most recent candidate year comes first. Earlier alternatives remain.
    for values in leads.values():
        values.sort(key=lambda value: value["year"], reverse=True)
    return leads


def _api_key() -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env", override=False)
    except ImportError:
        pass
    value = os.environ.get("OPENFEC_API_KEY", "")
    return "" if value == "DEMO_KEY" else value


def _source_result(response: requests.Response) -> dict:
    """Do not include exception URLs. OpenFEC query strings contain an API key."""
    if response.status_code != 200:
        return {"status": "rate_limited" if response.status_code == 429 else "http_error",
                "http_status": response.status_code, "urls": []}
    try:
        return {"status": "ok", "data": response.json(), "urls": []}
    except ValueError:
        return {"status": "invalid_response", "urls": []}


def _openfec(session: requests.Session, row: dict, key: str) -> dict:
    if not key:
        return {"source": "openfec", "status": "no_registered_key", "urls": []}
    committee = row.get("committee_id", "")
    if not re.fullmatch(r"C[A-Z0-9]{8}", committee):
        return {"source": "openfec", "status": "no_committee_id", "urls": []}
    try:
        result = _source_result(session.get(
            f"https://api.open.fec.gov/v1/committee/{committee}/",
            params={"api_key": key}, timeout=30))
    except requests.RequestException as error:
        return {"source": "openfec", "status": "request_error",
                "error_type": type(error).__name__, "urls": []}
    result["source"] = "openfec"
    result["committee_id"] = committee
    if result["status"] != "ok":
        return result
    payload = result.pop("data")
    records = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(records, list) or any(not isinstance(item, dict) for item in records):
        result["status"] = "invalid_response"
        return result
    for record in records:
        if record.get("committee_id") and record["committee_id"] != committee:
            result["status"] = "identity_conflict"
            return result
        ids = record.get("candidate_ids") or []
        if ids and row["candidate_id"] not in ids:
            result["status"] = "identity_conflict"
            return result
    result["urls"] = list(dict.fromkeys(url for record in records
                                        if (url := _normal_url(record.get("website", "")))))
    result["status"] = "found" if result["urls"] else "no_result"
    return result


def _wikidata(session: requests.Session, rows: list[dict]) -> dict[str, dict]:
    """Use P1839, the FEC identifier. Do not match candidates by name."""
    if not rows:
        return {}
    ids = [row["candidate_id"] for row in rows]
    values = " ".join(json.dumps(cid) for cid in ids)
    query = ("SELECT ?fec ?person ?website WHERE { VALUES ?fec { " + values +
             " } ?person wdt:P1839 ?fec . ?person wdt:P856 ?website . }")
    base = {cid: {"source": "wikidata", "status": "no_result", "urls": [], "entities": []}
            for cid in ids}
    try:
        result = _source_result(session.get("https://query.wikidata.org/sparql",
                                params={"query": query, "format": "json"}, timeout=60))
    except requests.RequestException as error:
        result = {"status": "request_error", "error_type": type(error).__name__}
    if result["status"] != "ok":
        return {cid: {"source": "wikidata", "urls": [], **result} for cid in ids}
    payload = result.get("data")
    bindings = payload.get("results", {}).get("bindings") if isinstance(payload, dict) else None
    if not isinstance(bindings, list):
        return {cid: {"source": "wikidata", "status": "invalid_response", "urls": []}
                for cid in ids}
    for binding in bindings:
        try:
            cid, entity = binding["fec"]["value"], binding["person"]["value"]
            url = _normal_url(binding["website"]["value"])
        except (KeyError, TypeError):
            return {cid: {"source": "wikidata", "status": "invalid_response", "urls": []}
                    for cid in ids}
        if cid not in base:
            continue
        base[cid]["entities"].append(entity)
        if url:
            base[cid]["urls"].append(url)
    for result in base.values():
        result["entities"] = sorted(set(result["entities"]))
        result["urls"] = list(dict.fromkeys(result["urls"]))
        if len(result["entities"]) > 1:
            result["status"] = "identity_conflict"
            result["urls"] = []
        else:
            result["status"] = "found" if result["urls"] else "no_result"
    return base


def _lookup_urls(rows: list[dict], limit: int | None, root: Path, version: str) -> dict:
    # Oldest unattempted candidates come first after the initial pilot.
    if limit is not None and limit < 0:
        raise ValueError("lookup_limit must be nonnegative or None")
    candidates = sorted(rows, key=lambda row: (row.get("url_checked_at", ""), row["candidate_id"]))
    if not any(row.get("url_checked_at") for row in rows) and len(rows) >= 40 and limit:
        pilot = select_pilot(rows)
        pids = {row["candidate_id"] for row in pilot}
        reasons = {row["candidate_id"]: row["pilot_reason"] for row in pilot}
        for row in rows:
            if row["candidate_id"] in pids:
                row["pilot_member"] = "true"
                row["pilot_reason"] = reasons[row["candidate_id"]]
        candidates = [row for row in candidates if row["candidate_id"] in pids] + [
            row for row in candidates if row["candidate_id"] not in pids]
    selected = candidates if limit is None else candidates[:limit]
    selected_ids = {row["candidate_id"] for row in selected}
    for row in rows:
        if row["candidate_id"] not in selected_ids:
            row["url_lookup_status"] = "budget_deferred"
    attempts, checked = [], _now()
    attempt_journal = root / "rosters" / "url_attempts" / f"{version}.jsonl"
    key = _api_key()
    with requests.Session() as session:
        session.headers["User-Agent"] = USER_AGENT
        last_fec = 0.0
        for row in selected:
            if key and row.get("committee_id"):
                time.sleep(max(0.0, 4.0 - (time.monotonic() - last_fec)))
                last_fec = time.monotonic()
            result = _openfec(session, row, key)
            attempts.append({"candidate_id": row["candidate_id"], "checked_at": _now(), **result})
            _record_attempt(attempt_journal, attempts[-1])
            row["_source_results"] = [result]
        unresolved = [row for row in selected if not row["_source_results"][0].get("urls")]
        for offset in range(0, len(unresolved), 50):
            batch = unresolved[offset:offset + 50]
            results = _wikidata(session, batch)
            for row in batch:
                result = results[row["candidate_id"]]
                attempts.append({"candidate_id": row["candidate_id"], "checked_at": _now(), **result})
                _record_attempt(attempt_journal, attempts[-1])
                row["_source_results"].append(result)
            if offset + 50 < len(unresolved):
                time.sleep(1)
    errors = {"http_error", "request_error", "rate_limited", "invalid_response", "identity_conflict"}
    for row in selected:
        results = row.pop("_source_results")
        found = next((result for result in results if result.get("urls")), None)
        urls = _alternatives(row)
        if found:
            urls = list(dict.fromkeys([*found["urls"], *urls]))
            row["website_url"], row["url_source"] = urls[0], found["source"]
            row["identity_status"] = "source_id_matched_site_unverified"
            row["url_lookup_status"] = "found"
        elif any(result["status"] in errors for result in results):
            row["url_lookup_status"] = "source_error"
        elif any(result["status"] == "no_registered_key" for result in results):
            row["url_lookup_status"] = "incomplete_no_registered_key"
        else:
            row["url_lookup_status"] = "no_result"
        row["website_urls"] = json.dumps(urls)
        row["url_checked_at"] = checked
        row["url_lookup_detail"] = json.dumps(results, sort_keys=True)
    _json(root / "rosters" / "url_attempts" / f"{version}.json", attempts)
    return {"candidates_attempted": len(selected), "lookup_limit": limit,
            "openfec_registered_key_available": bool(key),
            "source_status_counts": dict(Counter(x["source"] + ":" + x["status"] for x in attempts))}


def refresh_roster(root: Path, lookup_limit: int | None = 40) -> list[dict]:
    """Download a fresh FEC file and publish a complete, versioned roster.

    A zero limit performs no URL requests. None permits all candidate lookups.
    Missing candidates remain under retained_absent_from_current_fec status.
    A failed FEC download or parse leaves the previous current.csv intact.
    """
    root = Path(root)
    if lookup_limit is not None and lookup_limit < 0:
        raise ValueError("lookup_limit must be nonnegative or None")
    previous = {row["candidate_id"]: row for row in load_roster(root)}
    timestamp = _now()
    version = re.sub(r"[^0-9]", "", timestamp) + "Z"
    response = requests.get(FEC_URL, headers={"User-Agent": USER_AGENT, "Cache-Control": "no-cache"}, timeout=60)
    response.raise_for_status()
    content = response.content
    _atomic(root / "rosters" / "fec" / f"cn26_{version}.zip", content)
    fresh, counts = _parse_fec(content)
    leads = _legacy_leads()
    rows = []
    for record in fresh:
        cid = record["candidate_id"]
        prior = previous.pop(cid, {})
        row = {field: prior.get(field, "") for field in FIELDS}
        row.update(record)
        urls = _alternatives(prior)
        candidate_leads = leads.get(cid, [])
        urls = list(dict.fromkeys([*urls, *(lead["url"] for lead in candidate_leads)]))
        row.update(website_urls=json.dumps(urls), website_url=prior.get("website_url") or (urls[0] if urls else ""),
                   roster_status="current_fec", participation_status=prior.get("participation_status") or "unknown",
                   identity_status=prior.get("identity_status") or "unresolved",
                   first_seen_at=prior.get("first_seen_at") or timestamp, last_seen_at=timestamp,
                   roster_version=version)
        if not row["url_source"] and urls:
            row["url_source"] = "historical_fec_id_lead"
            row["url_lookup_detail"] = json.dumps(candidate_leads)
        rows.append(row)
    for row in previous.values():
        row.update(roster_status="retained_absent_from_current_fec", roster_version=version)
        rows.append(row)
    rows.sort(key=lambda row: row["candidate_id"])
    lookup = _lookup_urls(rows, lookup_limit, root, version)
    report = {"version": version, "fetched_at": timestamp, "source_url": FEC_URL,
              "source_sha256": hashlib.sha256(content).hexdigest(), "source_bytes": len(content),
              "source_last_modified": response.headers.get("Last-Modified", ""),
              "source_etag": response.headers.get("ETag", ""), "population_counts": counts,
              "previous_candidates": len(load_roster(root)), "retained_absent_candidates": len(previous),
              "total_candidates": len(rows), "missing_website_urls": sum(not row["website_url"] for row in rows),
              "groups": dict(Counter(row["office"] + ":" + row["party"] for row in rows)),
              "url_lookup": lookup}
    content_csv = _csv_bytes(rows)
    _atomic(root / "rosters" / "versions" / f"roster_{version}.csv", content_csv)
    _json(root / "rosters" / "versions" / f"roster_{version}.json", report)
    _atomic(root / "rosters" / "current.csv", content_csv)
    _json(root / "rosters" / "latest_refresh.json", report)
    return rows


def lookup_candidate_urls(root: Path, candidates: list[dict], limit: int = 40) -> list[dict]:
    """Resolve only specified FEC IDs. Preserve every row in the saved roster.

    Return the complete roster. Use select_pilot afterward for the frozen subset.
    A complete balanced pilot freezes membership before the first source request.
    """
    root = Path(root)
    rows = load_roster(root)
    lookup = {row["candidate_id"]: row for row in rows}
    ids = [row["candidate_id"] for row in candidates]
    if len(ids) != len(set(ids)):
        raise ValueError("URL lookup input contains duplicate FEC IDs")
    if any(cid not in lookup for cid in ids):
        raise ValueError("URL lookup requested a candidate outside the saved roster")
    if limit < 0:
        raise ValueError("URL lookup limit must be nonnegative")
    if len(ids) == 40:
        groups = Counter((lookup[cid]["office"], lookup[cid]["party"]) for cid in ids)
        if groups != Counter({(office, party): 10 for office in ("house", "senate") for party in ("D", "R")}):
            raise ValueError("A 40-candidate pilot must contain 10 candidates per office and party")
        old = {row["candidate_id"] for row in rows if row.get("pilot_member") == "true"}
        if old and old != set(ids):
            raise ValueError("Cannot replace an existing frozen pilot through URL refresh")
        for candidate in candidates:
            lookup[candidate["candidate_id"]]["pilot_member"] = "true"
            lookup[candidate["candidate_id"]]["pilot_reason"] = candidate.get("pilot_reason", "office_party_cross_section")
    version = re.sub(r"[^0-9]", "", _now()) + "Z"
    # Save pilot membership before any request so an interrupted job can resume it.
    _atomic(root / "rosters/current.csv", _csv_bytes(rows))
    summary = _lookup_urls([lookup[cid] for cid in ids], limit, root, version)
    for row in rows:
        row["roster_version"] = version
    prior = root / "rosters/latest_refresh.json"
    report = json.loads(prior.read_text()) if prior.exists() else {}
    report.update(version=version, url_refreshed_at=_now(), url_lookup=summary,
                  total_candidates=len(rows), missing_website_urls=sum(not row["website_url"] for row in rows))
    content = _csv_bytes(rows)
    _atomic(root / "rosters/versions" / f"roster_{version}.csv", content)
    _json(root / "rosters/versions" / f"roster_{version}.json", report)
    _atomic(root / "rosters/current.csv", content)
    _json(prior, report)
    return rows


def select_pilot(roster: list[dict], evidence_path: str | Path = PILOT_EVIDENCE) -> list[dict]:
    """Select 10 candidates per office and party, with sourced election examples.

    This is an operational pilot, not a representative research sample.
    Fixed hash order breaks ties without using candidate positions or outcomes.
    """
    path = Path(evidence_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    evidence = json.loads(path.read_text()) if path.exists() else {"candidates": []}
    examples = {row["candidate_id"]: row for row in evidence.get("candidates", [])}
    seen = set()
    for row in roster:
        if row["candidate_id"] in seen:
            raise ValueError("Pilot input contains duplicate FEC candidate IDs")
        seen.add(row["candidate_id"])
    selected = []
    frozen_ids = {row["candidate_id"] for row in roster if row.get("pilot_member") == "true"}
    if frozen_ids and len(frozen_ids) != 40:
        raise ValueError("Frozen pilot membership must contain exactly 40 candidates")
    for office in ("house", "senate"):
        for party in ("D", "R"):
            group = [dict(row) for row in roster if row["office"] == office and row["party"] == party]
            if len(group) < 10:
                raise ValueError(f"Pilot requires at least 10 {office} {party} candidates")
            group.sort(key=lambda row: hashlib.sha256(("20260914:" + row["candidate_id"]).encode()).hexdigest())
            required = [row for row in group if row["candidate_id"] in (frozen_ids or examples)]
            if frozen_ids and len(required) != 10:
                raise ValueError("Frozen pilot must contain 10 candidates per office and party")
            if len(required) > 10:
                raise ValueError(f"More than 10 evidence candidates in {office} {party}")
            for row in required:
                example = examples.get(row["candidate_id"], {})
                for field in ("office", "party", "state"):
                    if example.get(field) and example[field] != row[field]:
                        raise ValueError(f"Pilot evidence conflicts with FEC {field} for {row['candidate_id']}")
            chosen = required[:]
            # Include missing-URL cases explicitly; fill from known URL leads first.
            missing = next((row for row in group if not row.get("website_url") and row not in chosen), None)
            if missing and len(chosen) < 10:
                chosen.append(missing)
            available = [row for row in group if row.get("website_url") and row not in chosen]
            available += [row for row in group if not row.get("website_url") and row not in chosen]
            chosen += available[:10 - len(chosen)]
            for row in chosen:
                example = examples.get(row["candidate_id"])
                row["pilot_reason"] = row.get("pilot_reason") or ("sourced_election_example" if example else (
                    "missing_url" if not row.get("website_url") else "office_party_cross_section"))
                row["pilot_evidence"] = json.dumps(example or {}, sort_keys=True)
            selected.extend(chosen)
    return selected
