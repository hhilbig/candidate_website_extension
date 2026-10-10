#!/usr/bin/env python3
"""Build election coverage from official result roles and retained URL evidence.

This module does not infer finalist roles from first-round vote totals.
It consumes source-verified winner and runner-up roles instead.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
BASE = Path("data/municipal_discovery/top100_coverage")
ROSTER = Path("data/municipal_discovery/exhaustive_source_expansion_20260908/ownership_repair_20260910/combined/candidate_roster.csv")
URLS = Path("quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_complete/candidate_urls_with_capture_reviews.csv")
REVIEWS = Path("quality_reports/mayoral_variant_content_review_2026-09-14/capture_reviews.csv")
VALIDATION = Path("quality_reports/mayoral_archive_validation_2026-09-10/final/validation.csv")
CUTOFF = date(2026, 9, 14)
METRICS = ("url_lead", "archive_discovery", "downloaded_content")


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def truth(value):
    token = str(value).strip().casefold()
    if token in {"true", "1", "yes", "t"}:
        return True
    if token in {"false", "0", "no", "f"}:
        return False
    if token in {"", "none", "unknown", "nan"}:
        return None
    raise ValueError(f"Invalid boolean: {value!r}")


def name_key(value):
    return re.sub(r"[^a-z0-9]", "", str(value).casefold())


def city_key(value):
    return name_key(re.sub(r"\s+(?:city|town|municipality|metropolitan government|urban county)(?:\s*\(balance\))?$", "", value, flags=re.I))


def fips_key(value):
    return str(value).zfill(7) if str(value) else ""


def campaign_lead(row):
    return bool(exact_url(row.get("exact_normalized_url", ""))) and row.get("url_type") in {
        "custom_domain", "candidate_subdomain", "shared_host_path", "shared_host", "subdomain", "campaign_website",
        "candidate_subdomain_or_shared_host_path"}


def exact_url(value):
    value = re.sub(r"^https?://(?:web\.archive\.org/web|wayback\.archive-it\.org/[^/]+)/\d{1,14}(?:[a-z_]+)?/", "", str(value))
    try:
        parts = urlsplit(value)
        if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username:
            return ""
        parts.port  # Reject malformed ports without changing valid literal ports.
    except ValueError:
        return ""
    # Scheme and hostname are case insensitive; path, query, and www are not collapsed.
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", parts.query, ""))


def unique(rows, key, label):
    values = [r[key] for r in rows]
    if any(not v for v in values) or len(set(values)) != len(values):
        raise ValueError(f"Missing or duplicate {label} {key}")


def capture_date(value):
    if not re.fullmatch(r"\d{14}", str(value)):
        return None
    try:
        return datetime.strptime(str(value), "%Y%m%d%H%M%S").date()
    except ValueError:
        return None


def period_match(timestamp, election_date):
    captured = capture_date(timestamp)
    return captured is not None and abs((captured - date.fromisoformat(election_date)).days) <= 365


def aggregate(rows):
    contests = defaultdict(list)
    for row in rows:
        contests[row["election_id"]].append(row)
    result = {"candidate_records": len(rows), "contested_elections": len(contests)}
    for metric in METRICS:
        numerator = sum(row[metric] is True for row in rows)
        both = sum(len(group) == 2 and all(r[metric] is True for r in group) for group in contests.values())
        result.update({metric + "_covered": numerator,
                       metric + "_unknown": sum(r[metric] is None for r in rows),
                       metric + "_share": numerator / len(rows) if rows else None,
                       metric + "_both": both,
                       metric + "_both_share": both / len(contests) if contests else None})
    return result


def by_candidate_rows(urls, candidate_year_id):
    return [u for u in urls if u["candidate_year_id"] == candidate_year_id]


def evidence_for(urls, election_date, reviews, validations, saved=None, attributed_keys=frozenset()):
    """Positive evidence only; missing inventories remain unknown, not absent."""
    archive = False
    download = False
    substantive = False
    refs = set()
    for row in urls:
        target = exact_url(row["exact_normalized_url"])
        if row.get("url_type") in {"social", "social_media", "fundraising", "officeholder_profile"}:
            continue
        accepted = any(truth(row.get(k, "")) is True for k in ("human_accepted", "sampled_audit_accepted", "ai_accepted_website"))
        # Checked legacy attribution (legacy_attribution/agent_verdicts.csv): this URL is the candidate's own site.
        accepted = accepted or (row["candidate_url_id"], row["candidate_year_id"]) in attributed_keys
        key = (row["candidate_url_id"], row["candidate_year_id"])
        attributed = accepted
        for evidence in reviews.get(key, []):
            same = exact_url(evidence.get("effective_original_url", "")) == target
            support = evidence.get("ai_capture_review_decision") == "support_capture"
            timestamp = evidence.get("effective_timestamp", "")
            if target and same and support and period_match(timestamp, election_date):
                archive = True
                substantive = True
                attributed = True
                refs.add("capture_review:" + key[0] + ":" + timestamp)
                # A reviewed raw page does not alone establish a saved text file.
        for evidence in validations.get(key, []):
            identity = accepted or all(truth(evidence.get(k, "")) is True for k in (
                "candidate_identity_match", "city_match", "mayoral_campaign_match"))
            for timestamp, original in (
                (evidence.get("nearest_capture_timestamp", ""), evidence.get("nearest_capture_url", "")),
                (evidence.get("capture_effective_timestamp", ""), evidence.get("effective_original_url", "")),
            ):
                if identity and target and exact_url(original) == target and period_match(timestamp, election_date):
                    archive = True
                    attributed = True
                    refs.add("validation:" + key[0] + ":" + timestamp)
        # Attached AI evidence is URL-specific; capture support is not inferred from domain shape.
        if target and accepted and exact_url(row.get("ai_review_capture_url", "")) == target:
            timestamp = row.get("ai_review_capture_timestamp", "")
            if period_match(timestamp, election_date):
                archive = True
                refs.add("accepted_capture:" + key[0] + ":" + timestamp)
        for evidence in (saved or {}).get(key, []):
            if not attributed or not target or exact_url(evidence.get("capture_original_url", "")) != target:
                continue
            if exact_url(evidence.get("exact_normalized_url", "")) != target or not period_match(evidence.get("capture_timestamp", ""), election_date):
                continue
            archive = True
            refs.add("saved_inventory:" + evidence["source_ref"] + ":" + evidence["capture_timestamp"])
            if all(truth(evidence.get(k, "")) is True for k in ("raw_saved", "text_saved")):
                if all(evidence.get(k) for k in ("raw_path", "text_path", "raw_sha256", "text_sha256")):
                    download = True
        # Legacy flags are reported separately, not promoted without dated evidence.
    return archive or None, download or None, substantive or None, sorted(refs)


def recorded_period_metadata(urls, election_date, saved, reviews, validations):
    """Record metadata independently of campaign attribution or replay success."""
    for row in urls:
        target = exact_url(row["exact_normalized_url"])
        key = row["candidate_url_id"], row["candidate_year_id"]
        pairs = []
        pairs.extend((e.get("capture_original_url", ""), e.get("capture_timestamp", "")) for e in saved.get(key, []))
        pairs.extend((e.get("effective_original_url", ""), e.get("effective_timestamp", "")) for e in reviews.get(key, []))
        pairs.extend((e.get("nearest_capture_url", ""), e.get("nearest_capture_timestamp", "")) for e in validations.get(key, []))
        if target and any(exact_url(url) == target and period_match(stamp, election_date) for url, stamp in pairs):
            return True
    return None


def build(cities, roster, urls, results, reviews=(), validations=(), city_year_overrides=(), saved_evidence=(), identity_links=(), discovered_leads=(), stage3_pages=(),
          legacy_attribution=(), legacy_site_pages=(), legacy_site_attribution=()):
    unique(cities, "city_id", "city")
    unique(roster, "candidate_year_id", "legacy candidate-year")
    unique(urls, "candidate_url_id", "URL")
    cities_by_id = {r["city_id"]: r for r in cities}
    city_names = {(city_key(r["city_name"]), r["state_abb"]): r for r in cities}
    fips_map = {fips_key(r["roster_place_fips"]): r for r in cities if r.get("roster_place_fips")}
    if len(fips_map) != sum(bool(r.get("roster_place_fips")) for r in cities):
        raise ValueError("Ambiguous roster-city crosswalk")
    roster_ids = {r["candidate_year_id"]: r for r in roster}
    old_contests = defaultdict(list)
    city_year_old = defaultdict(list)
    exact_names = defaultdict(list)
    for row in roster:
        year = int(float(row["year"]))
        city = fips_map.get(fips_key(row["place_fips"]))
        if city and 2010 <= year <= CUTOFF.year:
            old_contests[row["contest"]].append(row)
            city_year_old[(city["city_id"], year)].append(row)
            exact_names[(city["city_id"], year, name_key(row["full_name"]))].append(row)
    # Reviewed name-variant links (scripts/build_mayoral_identity_links.py). They
    # apply only where no exact or source-verified link exists, and they keep
    # their own basis label so provenance stays visible.
    reviewed_links = {}
    for link in identity_links:
        key = (link["election_id"], name_key(link["candidate_name"]))
        if key in reviewed_links:
            raise ValueError("Duplicate reviewed identity link")
        reviewed_links[key] = link
    # Checked URL discovery (scripts/build_mayoral_url_leads.py). It applies only to
    # finalists without a legacy URL lead and keeps its own basis label.
    discovered = {}
    for lead in discovered_leads:
        if lead["candidate_election_id"] in discovered:
            raise ValueError("Duplicate discovered URL lead")
        discovered[lead["candidate_election_id"]] = lead
    used_discovered = set()
    # Legacy-lead site scrape (stage 3L): a target's saved pages count only after its own attribution check.
    accepted_targets = {r["target_id"] for r in legacy_site_attribution if r["status"] == "accepted"}
    legacy_pages = defaultdict(list)
    for row in legacy_site_pages:
        legacy_pages[row["candidate_election_id"]].append(row)
    if accepted_targets - {r["target_id"] for r in legacy_site_pages}:
        raise ValueError("Legacy site attribution names a target without a page summary")
    attributed_keys = set()
    for row in legacy_attribution:
        if row["status"] == "accepted":
            if row["candidate_url_id"] not in {u["candidate_url_id"] for u in by_candidate_rows(urls, row["candidate_year_id"])}:
                raise ValueError("Legacy attribution names a URL that is not linked to its candidate-year")
            attributed_keys.add((row["candidate_url_id"], row["candidate_year_id"]))
    # Saved site pages from the stage 3 scrape of discovered URLs (scripts/summarize_mayoral_stage3_pages.py).
    pages = {}
    for row in stage3_pages:
        if row["candidate_election_id"] in pages:
            raise ValueError("Duplicate stage 3 page summary")
        pages[row["candidate_election_id"]] = row
    by_candidate = defaultdict(list)
    for row in urls:
        by_candidate[row["candidate_year_id"]].append(row)
    review_index, validation_index, saved_index = defaultdict(list), defaultdict(list), defaultdict(list)
    targets = {(r["candidate_url_id"], r["candidate_year_id"]): exact_url(r["exact_normalized_url"]) for r in urls}
    for row in saved_evidence:
        if targets.get((row["candidate_url_id"], row["candidate_year_id"])) != exact_url(row["exact_normalized_url"]):
            raise ValueError("Saved evidence does not match current candidate and URL")
    for rows, index in ((reviews, review_index), (validations, validation_index), (saved_evidence, saved_index)):
        for row in rows:
            index[(row["candidate_url_id"], row["candidate_year_id"])].append(row)

    elections, candidates, outside = [], [], []
    legacy_election_links = {}
    linked_contests = set()
    for record in results:
        city = cities_by_id.get(record.get("city_id")) or fips_map.get(fips_key(record.get("roster_place_fips", ""))) or city_names.get((city_key(record["city_name"]), record["state_abb"]))
        if not city:
            outside.append(dict(record, exclusion_reason="outside_city_list_or_unresolved_city_match"))
            continue
        election_date = record["election_date"]
        day = date.fromisoformat(election_date)
        if not date(2010, 1, 1) <= day <= CUTOFF:
            outside.append(dict(record, exclusion_reason="outside_date_window"))
            continue
        kind = record.get("election_type", "regular")
        if kind not in {"regular", "special"}:
            raise ValueError("Election type must distinguish regular and special elections")
        election_id = f"{city['city_id']}__{election_date}__{kind}"
        roles = Counter(c.get("role") for c in record.get("candidates", []))
        status = record.get("verification_status", "unresolved")
        if status == "verified_decisive" and record.get("certification_status") in {
            "not_confirmed", "county_only", "report_for_certification", "uncertified", "unresolved"}:
            raise ValueError("Verified election conflicts with unresolved certification")
        if status == "verified_decisive":
            if not record.get("source_url"):
                raise ValueError("Verified election lacks source")
            decisive_primary = record.get("stage") == "primary_nominating" and bool(record.get("decisive_stage_source_url"))
            if not decisive_primary and record.get("stage") not in {"general", "runoff", "ranked_choice", "ranked_choice_final", "special"}:
                raise ValueError("Verified election is not a decisive stage")
            if roles != {"winner": 1, "runner_up": 1}:
                status = "uncontested" if roles == {"winner": 1} else "unresolved_candidate_roles"
        election = {"election_id": election_id, "city_id": city["city_id"], "city_name": city["city_name"],
                    "state_abb": city["state_abb"], "year": day.year, "election_date": election_date,
                    "election_type": kind, "stage": record.get("stage", ""), "verification_status": status,
                    "source_url": record.get("source_url", ""), "verification_note": record.get("verification_note", ""),
                    "certification_status": record.get("certification_status", "source_verified" if status == "verified_decisive" else "unresolved"),
                    "certification_source_url": record.get("certification_source_url", ""),
                    "source_locator": record.get("source_locator", ""),
                    "legacy_contest_ids": []}
        for candidate in record.get("candidates", []):
            legacy_id = candidate.get("candidate_year_id", "")
            matches = exact_names.get((city["city_id"], day.year, name_key(candidate["name"])), [])
            if legacy_id:
                legacy = roster_ids.get(legacy_id)
                if not legacy or fips_map.get(fips_key(legacy["place_fips"]), {}).get("city_id") != city["city_id"] or int(float(legacy["year"])) != day.year or not legacy.get("month") or int(float(legacy["month"])) != day.month:
                    raise ValueError("Candidate identity link conflicts with city/year")
                # A supplied ID is a curated source link. Preserve its provenance.
                link_basis = "source_verified_id"
            else:
                # Names alone cannot link multiple elections in the same year.
                matches = [r for r in matches if r.get("month") and int(float(r["month"])) == day.month]
                legacy = matches[0] if len(matches) == 1 else None
                legacy_id = legacy["candidate_year_id"] if legacy else ""
                link_basis = "exact_name_city_year_month" if legacy else "unresolved_or_new_candidate"
                reviewed = reviewed_links.get((election_id, name_key(candidate["name"]))) if not legacy else None
                if reviewed:
                    legacy = roster_ids.get(reviewed["candidate_year_id"])
                    if (not legacy or fips_map.get(fips_key(legacy["place_fips"]), {}).get("city_id") != city["city_id"]
                            or int(float(legacy["year"])) != day.year or not legacy.get("month")
                            or int(float(legacy["month"])) != day.month):
                        raise ValueError("Reviewed identity link conflicts with city/year/month")
                    legacy_id = legacy["candidate_year_id"]
                    link_basis = reviewed["link_basis"]
            if legacy:
                if legacy_id in legacy_election_links and legacy_election_links[legacy_id] != election_id:
                    raise ValueError("Legacy candidate-year maps to multiple elections; provide election-specific evidence")
                legacy_election_links[legacy_id] = election_id
                election["legacy_contest_ids"].append(legacy["contest"])
                linked_contests.add(legacy["contest"])
            links = [r for r in by_candidate.get(legacy_id, []) if campaign_lead(r)]
            # Never transfer a URL across distinct election months without an explicit claim.
            links = [r for r in links if r.get("month") and int(float(r["month"])) == day.month]
            archive, downloaded, content, refs = evidence_for(links, election_date, review_index, validation_index, saved_index,
                                                              attributed_keys)
            candidate_id = election_id + "__" + hashlib.sha256(name_key(candidate["name"]).encode()).hexdigest()[:16]
            lead = discovered.get(candidate_id)
            url_lead_basis = "legacy_url_inventory" if links else ""
            discovered_url = ""
            if lead:
                used_discovered.add(candidate_id)
                # A checked lead from the legacy-lead search (October 9, 2026) may replace an old roster URL that had
                # no archived copy in the window; the roster URL's own evidence is kept.
                if links and lead.get("replaces_legacy") != "True":
                    raise ValueError("Discovered URL lead for a finalist that already has a legacy URL lead")
                # An outside-window entry without a URL is an unconfirmed domain, not a lead.
                if lead["final_status"] == "accepted" and not lead["campaign_url"]:
                    raise ValueError("Accepted discovered URL lead lacks its URL")
                if lead["final_status"] in {"accepted", "outside_window"} and lead["campaign_url"]:
                    discovered_url = lead["campaign_url"]
                    url_lead_basis = lead["link_basis"]
                if lead["final_status"] == "accepted":
                    stamp = lead["checked_capture_timestamp"]
                    if not (period_match(stamp, election_date) and stamp[:8] <= election_date.replace("-", "")):
                        raise ValueError("Accepted discovered URL lacks a capture before the election")
                    archive = True
                    refs = sorted(set(refs) | {"url_discovery:" + stamp})
                saved_pages = pages.get(candidate_id)
                if discovered_url and saved_pages and int(saved_pages["pages_with_text_in_window"]) > 0:
                    if truth(saved_pages["files_verified"]) is not True:
                        raise ValueError("Stage 3 saved pages failed hash verification")
                    if saved_pages["registrable_domain"].lower() not in discovered_url.lower():
                        raise ValueError("Stage 3 pages are not on the discovered campaign domain")
                    archive, downloaded = True, True
                    refs = sorted(set(refs) | {"stage3_pages:" + saved_pages["target_id"] + ":"
                                               + saved_pages["pages_with_text_in_window"]})
            for site in legacy_pages.get(candidate_id, []):
                if site["target_id"] in accepted_targets and int(site["pages_with_text_in_window"]) > 0:
                    if not links:
                        raise ValueError("Legacy site pages for a finalist without a legacy URL lead")
                    if truth(site["files_verified"]) is not True:
                        raise ValueError("Legacy site pages failed hash verification")
                    archive, downloaded = True, True
                    refs = sorted(set(refs) | {"stage3_legacy_pages:" + site["target_id"] + ":" + site["pages_with_text_in_window"]})
            candidates.append({"candidate_election_id": candidate_id, "election_id": election_id,
                "city_id": city["city_id"], "city_name": city["city_name"], "state_abb": city["state_abb"],
                "year": day.year, "election_date": election_date, "election_type": kind,
                "candidate_name": candidate["name"], "official_result_role": candidate.get("role", ""),
                "official_votes": candidate.get("votes", ""), "result_source_url": record.get("source_url", ""),
                "candidate_year_id": legacy_id, "identity_link_basis": link_basis,
                "eligible": status == "verified_decisive", "verification_status": status,
                "url_lead": bool(links) or bool(discovered_url), "url_lead_basis": url_lead_basis,
                "discovered_campaign_url": discovered_url, "archive_discovery": archive, "downloaded_content": downloaded,
                "recorded_period_capture_before_attribution": recorded_period_metadata(links, election_date, saved_index, review_index, validation_index),
                "substantive_campaign_content_review": content, "candidate_url_ids": [r["candidate_url_id"] for r in links],
                "exact_urls": sorted({r["exact_normalized_url"] for r in links}), "evidence_refs": refs,
                "legacy_archive_flag": any(truth(r.get("archive_capture_within_headline_window", "")) is True for r in links),
                "legacy_download_flag": any(truth(r.get("downloaded_content_available", "")) is True for r in links)})
        election["legacy_contest_ids"] = sorted(set(election["legacy_contest_ids"]))
        elections.append(election)
    if set(discovered) - used_discovered:
        raise ValueError("Discovered URL lead does not match a candidate-election")
    unique(elections, "election_id", "election")
    unique(candidates, "candidate_election_id", "candidate-election")
    # Keep every old contest not positively reconciled. Stage labels alone do not verify results.
    for contest, group in old_contests.items():
        if contest in linked_contests:
            continue
        row = group[0]
        city = fips_map[fips_key(row["place_fips"])]
        elections.append({"election_id": "legacy__" + contest, "city_id": city["city_id"],
            "city_name": city["city_name"], "state_abb": city["state_abb"], "year": int(float(row["year"])),
            "election_date": "", "election_type": "unresolved", "stage": row.get("election_stage_status", ""),
            "verification_status": "legacy_results_unverified", "source_url": row.get("election_stage_source_url", ""),
            "verification_note": "Existing contest retained; official decisive result and exact date require verification.",
            "legacy_contest_ids": [contest]})
    unique(elections, "election_id", "combined election")
    overrides = {(r["city_id"], int(r["year"])): r for r in city_year_overrides}
    if len(overrides) != len(city_year_overrides):
        raise ValueError("Duplicate city-year audit override")
    if any(city not in cities_by_id or not 2010 <= year <= CUTOFF.year for city, year in overrides):
        raise ValueError("City-year audit override is outside the fixed universe")
    city_years = []
    for city in cities:
        for year in range(2010, CUTOFF.year + 1):
            key = city["city_id"], year
            observed = [e for e in elections if (e["city_id"], e["year"]) == key]
            state = "recorded_election_calendar_unverified" if observed else "missing_election_calendar"
            override = overrides.get(key)
            expected = []
            if override:
                if not override.get("source_url"):
                    raise ValueError("City-year disposition needs source")
                state = override["status"]
                if state not in {"complete", "no_election", "indirect_selection", "earlier_round_only", "unresolved"}:
                    raise ValueError("Unknown city-year disposition")
                if state in {"no_election", "indirect_selection", "earlier_round_only"} and any(e["verification_status"] in {"verified_decisive", "uncontested"} for e in observed):
                    raise ValueError("City-year disposition conflicts with verified election")
                expected = override.get("expected_election_dates", [])
                if len(expected) != len(set(expected)):
                    raise ValueError("Duplicate expected election date")
                for value in expected:
                    day = date.fromisoformat(value)
                    if day.year != year or day > CUTOFF:
                        raise ValueError("Expected election date is outside audited year or cutoff")
                if state == "complete" and not expected:
                    raise ValueError("Complete election calendar needs expected election dates")
                if state in {"no_election", "indirect_selection", "earlier_round_only"} and expected:
                    raise ValueError("Non-election calendar cannot list expected elections")
                if state == "complete" and any(e["verification_status"] in {"verified_decisive", "uncontested"}
                                              and e["election_date"] not in expected for e in observed):
                    raise ValueError("Complete calendar omits a verified election date")
                for value in expected:
                    if not any(e["election_date"] == value for e in observed):
                        missing = {"election_id": f"calendar__{city['city_id']}__{value}",
                            "city_id": city["city_id"], "city_name": city["city_name"],
                            "state_abb": city["state_abb"], "year": year, "election_date": value,
                            "election_type": "unresolved", "stage": "unresolved",
                            "verification_status": "calendar_result_missing", "source_url": override["source_url"],
                            "verification_note": "Calendar identifies election; decisive results require verification.",
                            "legacy_contest_ids": []}
                        elections.append(missing)
                        observed.append(missing)
            city_years.append({"city_id": city["city_id"], "city_name": city["city_name"],
                "state_abb": city["state_abb"], "year": year, "status": state,
                "expected_election_dates": expected,
                "source_url": override.get("source_url", "") if override else "",
                "audit_note": override.get("note", "") if override else "No complete election calendar has been verified for this city-year.",
                "election_ids": [e["election_id"] for e in observed], "audit_complete": bool(override and state in {"complete", "no_election", "indirect_selection", "earlier_round_only"})})
    eligible = [r for r in candidates if r["eligible"]]
    summary = aggregate(eligible)
    summary.update({"cutoff": CUTOFF.isoformat(), "city_count": len(cities), "city_years": len(city_years),
        "city_years_audited": sum(r["audit_complete"] for r in city_years),
        "full_universe_complete": all(r["audit_complete"] for r in city_years) and all(e["verification_status"] in {"verified_decisive", "uncontested", "excluded_earlier_round"} for e in elections),
        "denominator_scope": "verified contested decisive elections only; not full 2010-2026 coverage",
        "election_status_counts": dict(Counter(r["verification_status"] for r in elections)),
        "legacy_roster_rows": len(roster), "legacy_roster_city_year_rows_in_scope": sum(map(len, city_year_old.values())),
        "legacy_contests_linked": len(linked_contests), "legacy_contests_unverified": len(old_contests) - len(linked_contests),
        "input_results_outside_scope": len(outside),
        "candidate_identity_links_unresolved": sum(not r["candidate_year_id"] for r in candidates)})
    active_rows = [r for r in roster if 2010 <= int(float(r["year"])) <= CUTOFF.year]
    summary["filter_accounting"] = [
        {"rule": "2010 through cutoff year", "before": len(roster), "after": len(active_rows),
         "reason": "Approved date scope; upstream roster retained."},
        {"rule": "national Census city crosswalk", "before": len(active_rows), "after": sum(map(len, city_year_old.values())),
         "reason": "Approved fixed city universe; upstream roster retained."},
        {"rule": "source-verified decisive contested elections", "before": len(candidates), "after": len(eligible),
         "reason": "Unverified and uncontested source records remain in candidate_coverage.csv."}]
    for change in summary["filter_accounting"]:
        change["over_20_percent"] = bool(change["before"] and change["after"] / change["before"] < 0.8)
    return cities, city_years, elections, candidates, summary, outside


def write_csv(path, rows, fields=None):
    fields = fields or sorted({k for row in rows for k in row})
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v for k, v in row.items()})


def combine_result_sources(payloads):
    """Replace an earlier record only through an explicit, checked disposition."""
    records, superseded = {}, []
    for payload in payloads:
        for record in payload:
            city = fips_key(record.get("roster_place_fips") or record.get("city_id") or "")
            city = city or (city_key(record["city_name"]), record["state_abb"])
            key = (city, record["election_date"], record.get("election_type", "regular"))
            if key in records:
                old = records[key]
                if record.get("supersedes_verification_status") != old.get("verification_status"):
                    raise ValueError("Duplicate election source needs an explicit supersession")
                if old.get("verification_status") == "verified_decisive":
                    raise ValueError("Cannot replace a verified result without a separate correction review")
                superseded.append(old)
            records[key] = record
    return list(records.values()), superseded


def merge_coverage_gaps(missing_urls, missing_period_captures):
    """Merge the two gap tables into one deduplicated list per candidate-election.

    The two inputs stay unchanged. They cannot overlap by construction, because
    one holds finalists with no URL lead and the other holds finalists that have
    one, but this refuses a repeated key rather than assume that stays true.
    """
    gaps: dict[str, dict] = {}
    for row in missing_urls:
        gaps[row["candidate_election_id"]] = dict(
            row, gap_type="no_url_lead", gap_detail=row["queue_group"],
            next_action="find_campaign_url_from_existing_sources")
    for row in missing_period_captures:
        key = row["candidate_election_id"]
        if key in gaps:
            raise ValueError(f"gap key appears in both gap tables: {key}")
        gaps[key] = dict(
            row, gap_type="no_period_capture", gap_detail=row["next_action"])
    return sorted(
        gaps.values(),
        key=lambda r: (r["gap_type"], r["election_date"], r["election_id"],
                       str(r["candidate_name"])))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-config", type=Path, default=Path("config/mayoral_top_two_build.json"))
    parser.add_argument("--cities", type=Path, default=BASE / "inputs/cities_2020_top100.csv")
    parser.add_argument("--results", nargs="+", type=Path)
    parser.add_argument("--city-year-audit", nargs="+", type=Path)
    parser.add_argument("--saved-evidence", type=Path, default=BASE / "inputs/saved_capture_evidence.csv")
    parser.add_argument("--output-dir", type=Path, default=BASE / "outputs")
    args = parser.parse_args()
    config = json.loads((ROOT / args.build_config).read_text())
    if config["cutoff"] != CUTOFF.isoformat():
        raise ValueError("Build config cutoff differs from frozen coverage cutoff")
    args.results = args.results or [Path(p) for p in config["results"]]
    args.city_year_audit = args.city_year_audit or [Path(p) for p in config["city_year_audits"]]
    inputs = [args.build_config, args.cities, ROSTER, URLS, REVIEWS, VALIDATION, *args.results]
    payloads = []
    for path in args.results:
        payload = json.loads((ROOT / path).read_text())
        payloads.append(payload if isinstance(payload, list) else payload["elections"])
    records, superseded = combine_result_sources(payloads)
    overrides = []
    for path in args.city_year_audit or []:
        inputs.append(path)
        overrides.extend(json.loads((ROOT / path).read_text()))
    inputs.append(args.saved_evidence)
    saved_manifest_path = args.saved_evidence.with_suffix(".manifest.json")
    saved_manifest = json.loads((ROOT / saved_manifest_path).read_text())
    if saved_manifest["target_sha256"] != sha(ROOT / URLS) or saved_manifest["output_sha256"] != sha(ROOT / args.saved_evidence):
        raise ValueError("Saved evidence manifest does not match current inputs")
    inputs.append(saved_manifest_path)
    city_manifest_path = args.cities.parent / "cities_manifest.json"
    city_manifest = json.loads((ROOT / city_manifest_path).read_text())
    if city_manifest["output_sha256"] != sha(ROOT / args.cities):
        raise ValueError("City list does not match frozen Census manifest")
    inputs.append(city_manifest_path)
    for source in city_manifest["inputs"]:
        path = args.cities.parent / source["file"]
        if sha(ROOT / path) != source["sha256"]:
            raise ValueError("Frozen Census response changed")
        inputs.append(path)
    links_path = config.get("identity_links")
    identity_links = read_csv(ROOT / links_path) if links_path else []
    if links_path:
        inputs.append(Path(links_path))
    pages_paths = config.get("stage3_saved_pages") or []
    stage3_pages = []
    for pages_path in [pages_paths] if isinstance(pages_paths, str) else pages_paths:
        stage3_pages += read_csv(ROOT / pages_path)
        inputs.append(Path(pages_path))
        pages_manifest = json.loads((ROOT / Path(pages_path).with_suffix(".manifest.json")).read_text())
        if pages_manifest["output_sha256"] != sha(ROOT / pages_path) or not pages_manifest["all_files_verified"]:
            raise ValueError("Stage 3 page summary does not match its verified manifest")
    legacy_site_pages = []
    for pages_path in config.get("stage3_legacy_pages") or []:
        legacy_site_pages += read_csv(ROOT / pages_path)
        inputs.append(Path(pages_path))
        pages_manifest = json.loads((ROOT / Path(pages_path).with_suffix(".manifest.json")).read_text())
        if pages_manifest["output_sha256"] != sha(ROOT / pages_path) or not pages_manifest["all_files_verified"]:
            raise ValueError("Legacy page summary does not match its verified manifest")
    site_attribution_path = config.get("legacy_site_attribution")
    legacy_site_attribution = read_csv(ROOT / site_attribution_path) if site_attribution_path else []
    if site_attribution_path:
        inputs.append(Path(site_attribution_path))
    attribution_path = config.get("legacy_attribution")
    legacy_attribution = read_csv(ROOT / attribution_path) if attribution_path else []
    if attribution_path:
        inputs.append(Path(attribution_path))
    leads_path = config.get("discovered_url_leads")
    discovered_leads = read_csv(ROOT / leads_path) if leads_path else []
    if leads_path:
        inputs.append(Path(leads_path))
    legacy_leads_path = config.get("legacy_search_leads")
    if legacy_leads_path:
        discovered_leads = discovered_leads + read_csv(ROOT / legacy_leads_path)
        inputs.append(Path(legacy_leads_path))
    hashes = {str(p): sha(ROOT / p) for p in inputs}
    result = build(read_csv(ROOT / args.cities), read_csv(ROOT / ROSTER), read_csv(ROOT / URLS), records,
                   read_csv(ROOT / REVIEWS), read_csv(ROOT / VALIDATION), overrides, read_csv(ROOT / args.saved_evidence),
                   identity_links=identity_links, discovered_leads=discovered_leads,
                   stage3_pages=stage3_pages, legacy_attribution=legacy_attribution,
                   legacy_site_pages=legacy_site_pages, legacy_site_attribution=legacy_site_attribution)
    cities, city_years, elections, candidates, summary, outside = result
    if len(cities) != 100:
        raise ValueError("Production build requires exactly 100 cities")
    summary["previous_reported_benchmark"] = {
        "candidate_records": 652, "url_lead_records": 371,
        "recorded_contests": 326, "both_urls": 136,
        "scope": "2010-2021, largest cities within old roster, unresolved rounds included",
        "reproduction_status": "Historical conversation result retained; original selected row-key artifact was not frozen. Not independently reproduced here.",
        "comparison": "Current source-verified denominator is not comparable to the earlier provisional denominator."}
    output = ROOT / args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    eligible = [r for r in candidates if r["eligible"]]
    queues = []
    groups = defaultdict(list)
    for row in eligible:
        groups[row["election_id"]].append(row)
    for group in groups.values():
        missing = sum(not r["url_lead"] for r in group)
        for row in group:
            if not row["url_lead"]:
                queues.append(dict(row, queue_group="one_url_missing" if missing == 1 else "both_urls_missing"))
    queues.sort(key=lambda r: (r["queue_group"] != "one_url_missing", not bool(r["evidence_refs"]), r["election_date"], r["election_id"]))
    breakdowns = []
    for dimension in ("city_id", "year"):
        values = [c["city_id"] for c in cities] if dimension == "city_id" else range(2010, CUTOFF.year + 1)
        for value in values:
            breakdowns.append(dict(dimension=dimension, value=value, **aggregate([r for r in eligible if r[dimension] == value])))
    tables = {"cities.csv": cities, "city_year_audit.csv": city_years, "elections.csv": elections,
        "candidate_coverage.csv": candidates, "coverage_breakdowns.csv": breakdowns,
        "missing_urls.csv": queues,
        "missing_period_captures.csv": [dict(r, next_action="review_attribution_existing_capture" if r["recorded_period_capture_before_attribution"] is True else "inspect_inventory_gap_before_new_request")
                                       for r in eligible if r["url_lead"] and r["archive_discovery"] is not True],
        "missing_election_results.csv": [r for r in elections if r["verification_status"] != "verified_decisive" and r["verification_status"] != "uncontested"],
        "missing_election_calendars.csv": [r for r in city_years if not r["audit_complete"]]}
    tables["coverage_gaps.csv"] = merge_coverage_gaps(
        tables["missing_urls.csv"], tables["missing_period_captures.csv"])
    estimate_specs = [
        ("official_results_indexes", "city", len(cities), 1, 3,
         "Inspect each official results index for calendars, special elections, and recent results."),
        ("official_result_documents", "unresolved election record", len(tables["missing_election_results.csv"]), 1, 3,
         "Read result and certification documents. Source-review and legacy records may overlap."),
        ("public_campaign_url_sources", "known finalist without URL", len(queues), 2, 5,
         "Inspect existing source leads and public candidate lists; no paid search assumption."),
        ("exact_archive_metadata", "known finalist with URL but no recorded period capture", sum(r["next_action"] == "inspect_inventory_gap_before_new_request" for r in tables["missing_period_captures.csv"]), 1, 3,
         "Inspect existing metadata first. New requests require a separate approved collection batch.")]
    tables["source_attempt_estimates.csv"] = [
        {"source_channel": channel, "unit": unit, "known_units": n,
         "planning_requests_min": n * low, "planning_requests_max": n * high,
         "direct_paid_api_cost_usd": 0, "analyst_cost_usd": "not_estimated",
         "planning_analyst_hours_min": round(n * 2 / 60, 2),
         "planning_analyst_hours_max": round(n * 10 / 60, 2),
         "analyst_cost_formula": "planning_analyst_hours * approved_hourly_rate; no hourly rate supplied",
         "basis": f"Planning assumption: {low}-{high} requests per known unit, not measured throughput.",
         "limitations": note + " Analyst time assumes 2-10 minutes per unit; complex cases can exceed this range.", "collection_launched": False}
        for channel, unit, n, low, high, note in estimate_specs]
    for filename, rows in tables.items():
        write_csv(output / filename, rows, fields=None if rows else ["election_id", "status"])
    (output / "outside_scope_results.json").write_text(json.dumps(outside, indent=2) + "\n")
    (output / "superseded_result_records.json").write_text(json.dumps(superseded, indent=2) + "\n")
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if hashes != {str(p): sha(ROOT / p) for p in inputs}:
        raise ValueError("An input changed during the build")
    manifest = {"version": "top_two_coverage_v1", "cutoff": CUTOFF.isoformat(), "inputs": hashes,
                "script_sha256": sha(Path(__file__)),
                "outputs": {name: sha(output / name) for name in [*tables, "summary.json", "outside_scope_results.json", "superseded_result_records.json"]}}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
