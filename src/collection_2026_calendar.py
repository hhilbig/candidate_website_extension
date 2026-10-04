"""Load the sourced 2026 calendar without inferring candidate participation."""

from __future__ import annotations

import copy
import datetime as dt
import json
import re
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_CALENDAR = "config/collection_2026_calendar.json"
VALID_STATES = set(
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI "
    "MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT "
    "VT VA WA WV WI WY AS GU MP PR VI".split()
)
EVENT_TYPES = {
    "primary", "primary_runoff", "general", "general_runoff",
    "special_primary", "special_general", "special_runoff",
    "nonpartisan_primary", "convention",
}
VERIFICATION_STATUSES = {
    "fec_only", "state_verified", "local_authority_verified", "party_verified",
}
PARTICIPATION_STATUSES = {
    "unknown", "confirmed", "participated", "not_participating", "withdrawn",
    "primary_loser", "advanced", "winner", "loser", "not_on_ballot",
}
REQUIRED_FIELDS = {
    "event_id", "date", "state", "office", "districts", "parties",
    "event_type", "round", "source_url", "verified_source_url",
    "verification_status", "notes",
}


def _is_url(value: object) -> bool:
    if not isinstance(value, str):
        return False
    parts = urlsplit(value)
    return parts.scheme in {"http", "https"} and bool(parts.netloc)


def validate_events(events: list[dict]) -> None:
    """Reject malformed dates, duplicate IDs, and unsupported event fields."""
    if not isinstance(events, list):
        raise ValueError("events must be a list")
    seen_ids: set[str] = set()
    seen_events: set[tuple] = set()
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f"event {index} must be an object")
        missing = REQUIRED_FIELDS - event.keys()
        if missing:
            raise ValueError(f"event {index} lacks fields: {sorted(missing)}")
        event_id = event["event_id"]
        if not isinstance(event_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", event_id):
            raise ValueError(f"invalid event_id: {event_id!r}")
        if event_id in seen_ids:
            raise ValueError(f"duplicate event_id: {event_id}")
        seen_ids.add(event_id)
        date = event["date"]
        if not isinstance(date, str) or not re.fullmatch(r"2026-\d{2}-\d{2}", date):
            raise ValueError(f"{event_id}: date must be a 2026 ISO date")
        dt.date.fromisoformat(date)
        if event["state"] not in VALID_STATES:
            raise ValueError(f"{event_id}: invalid state")
        if event["office"] not in {"house", "senate"}:
            raise ValueError(f"{event_id}: invalid office")
        districts = event["districts"]
        if not isinstance(districts, list) or not districts:
            raise ValueError(f"{event_id}: districts must be a nonempty list")
        if len(set(districts)) != len(districts) or any(
            not isinstance(district, str) or not re.fullmatch(r"\*|\d{2}", district)
            for district in districts
        ):
            raise ValueError(f"{event_id}: invalid districts")
        if "*" in districts and districts != ["*"]:
            raise ValueError(f"{event_id}: wildcard cannot accompany districts")
        if event["office"] == "senate" and districts != ["*"]:
            raise ValueError(f"{event_id}: Senate events must use statewide districts")
        parties = event["parties"]
        if not isinstance(parties, list) or not parties or len(set(parties)) != len(parties):
            raise ValueError(f"{event_id}: invalid parties")
        if not set(parties) <= {"D", "R"}:
            raise ValueError(f"{event_id}: only D/R roster parties are supported")
        if event["event_type"] not in EVENT_TYPES:
            raise ValueError(f"{event_id}: invalid event_type")
        if not isinstance(event["round"], str) or not event["round"]:
            raise ValueError(f"{event_id}: round must be a nonempty string")
        if event.get("event_status", "scheduled") not in {"scheduled", "conditional", "held", "cancelled"}:
            raise ValueError(f"{event_id}: invalid event_status")
        if not _is_url(event["source_url"]):
            raise ValueError(f"{event_id}: source_url must be an HTTP URL")
        verification = event["verification_status"]
        if verification not in VERIFICATION_STATUSES:
            raise ValueError(f"{event_id}: invalid verification_status")
        if event["verified_source_url"] and not _is_url(event["verified_source_url"]):
            raise ValueError(f"{event_id}: invalid verified_source_url")
        if verification != "fec_only" and not event["verified_source_url"]:
            raise ValueError(f"{event_id}: verified event lacks verification source")
        if not isinstance(event["notes"], str):
            raise ValueError(f"{event_id}: notes must be text")
        zones = event.get("timezones", [])
        if not isinstance(zones, list):
            raise ValueError(f"{event_id}: timezones must be a list")
        for zone in zones:
            try:
                ZoneInfo(zone)
            except (ZoneInfoNotFoundError, TypeError, ValueError) as exc:
                raise ValueError(f"{event_id}: invalid timezone {zone!r}") from exc
        identity = (event["date"], event["state"], event["office"],
                    tuple(sorted(districts)), tuple(sorted(parties)), event["round"])
        if identity in seen_events:
            raise ValueError(f"{event_id}: duplicate event definition")
        seen_events.add(identity)


def load_calendar(path: str | Path = DEFAULT_CALENDAR) -> dict:
    """Read the full calendar, including cancelled events and unresolved items."""
    with Path(path).open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        payload = {"schema_version": 1, "events": payload, "unresolved_items": []}
    if not isinstance(payload, dict) or "events" not in payload:
        raise ValueError("calendar must contain events")
    validate_events(payload["events"])
    return payload


def load_events(path: str | Path = DEFAULT_CALENDAR) -> list[dict]:
    """Return dated events that were not cancelled, ordered by date and ID."""
    return sorted(
        (event for event in load_calendar(path)["events"]
         if event.get("event_status") != "cancelled"),
        key=lambda event: (event["date"], event["event_id"]),
    )


def _candidate_value(candidate: dict, *keys: str) -> str:
    for key in keys:
        value = candidate.get(key)
        if value is not None and str(value).strip().lower() not in {"", "nan", "none"}:
            return str(value).strip()
    return ""


def events_for_candidate(candidate: dict, events: list[dict]) -> list[dict]:
    """Match event jurisdiction; retain unknown participation without ballot evidence.

    An optional event_participation mapping can supply an event-specific status.
    Each non-unknown status requires a source_url. No event is removed by status.
    """
    year = _candidate_value(candidate, "year", "cand_election_yr")
    if year and year not in {"2026", "2026.0"}:
        return []
    state = _candidate_value(candidate, "state", "cand_office_st").upper()
    office = _candidate_value(candidate, "office", "cand_office").lower()
    office = {"h": "house", "s": "senate"}.get(office, office)
    party = _candidate_value(candidate, "party", "cand_pty_affiliation").upper()
    party = {"DEM": "D", "DFL": "D", "REP": "R"}.get(party, party)
    district = _candidate_value(candidate, "district", "cand_office_district")
    if re.fullmatch(r"\d+(?:\.0)?", district):
        district = str(int(float(district))).zfill(2)
    elif district.upper() in {"AL", "AT LARGE", "AT-LARGE"}:
        district = "00"
    participation = candidate.get("event_participation") or {}
    if not isinstance(participation, dict):
        raise ValueError("event_participation must map event IDs to sourced records")
    matches = []
    for event in events:
        if event.get("event_status") == "cancelled":
            continue
        if event["state"] != state or event["office"] != office or party not in event["parties"]:
            continue
        if event["districts"] != ["*"] and district not in event["districts"]:
            continue
        result = copy.deepcopy(event)
        result["participation"] = "unknown"
        result["participation_source_url"] = ""
        evidence = participation.get(event["event_id"], {})
        case = candidate.get('pilot_evidence') or {}
        if isinstance(case,str):
            case = json.loads(case)
        if (not evidence and isinstance(case,dict) and
                case.get('candidate_id')==_candidate_value(candidate,'candidate_id','cand_id') and
                case.get('event_date')==event['date'] and case.get('election_round')==event['event_type'] and
                case.get('participation_status')=='confirmed' and _is_url(case.get('source_url')) and
                (not case.get('district') or str(case['district']).zfill(2)==district)):
            evidence={'status':'confirmed','source_url':case['source_url']}
        if evidence:
            if not isinstance(evidence, dict):
                raise ValueError("participation evidence must contain status and source_url")
            status = evidence.get("status", "unknown")
            if status not in PARTICIPATION_STATUSES:
                raise ValueError(f"unsupported participation status: {status}")
            if status != "unknown" and not _is_url(evidence.get("source_url")):
                raise ValueError("non-unknown participation requires a source_url")
            result["participation"] = status
            result["participation_source_url"] = evidence.get("source_url", "")
        matches.append(result)
    return sorted(matches, key=lambda event: (event["date"], event["event_id"]))
