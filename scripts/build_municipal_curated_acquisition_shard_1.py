#!/usr/bin/env python3
"""Build the frozen Wave 2 municipal-acquisition shard 1 outputs.

This script is offline. It parses each frozen physical file once, reuses that
parse across all assigned contests, and writes the exact acquisition-contract
interface under ``municipal_curated_acquisition/shard_1``.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import urljoin, urlsplit, urlunsplit

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_municipal_curated_acquisition_contract import (  # noqa: E402
    audit_post_shard_directories,
)


CONTRACT_VERSION = "municipal-curated-acquisition-contract-v1"
PARSER_VERSION = "municipal_curated_shard_1_v1"
SHARD_ID = 1

ACQUISITION_ROOT = (
    REPO_ROOT
    / "data"
    / "municipal_discovery"
    / "screening_2010_2021"
    / "municipal_curated_acquisition"
)
AUDIT_DIR = ACQUISITION_ROOT / "audit"
OUTPUT_DIR = ACQUISITION_ROOT / "shard_1"
FILES_DIR = OUTPUT_DIR / "files"

SCHEMAS_PATH = AUDIT_DIR / "output_schemas.json"
TARGETS_PATH = AUDIT_DIR / "shard_1_targets.csv"
CANDIDATES_PATH = AUDIT_DIR / "shard_1_candidate_targets.csv"
PARSER_MATRIX_PATH = AUDIT_DIR / "parser_support_matrix.csv"


@dataclass(frozen=True)
class FileSpec:
    filename: str
    source_id: str
    parse_group_id: str
    request_url: str
    file_type: str
    content_type: str
    retrieved_at_utc: str


FILE_SPECS = (
    FileSpec(
        "minneapolis_2013_cbs.html",
        "minneapolis_2013_cbs_candidate_websites",
        "minneapolis_2013_cbs_candidate_websites__2013__11",
        "https://www.cbsnews.com/minnesota/news/minneapolis-mayoral-election-candidates/",
        "html",
        "text/html; charset=UTF-8",
        "2026-08-25T16:06:20Z",
    ),
    FileSpec(
        "alameda_2018_composite_voter_guide.pdf",
        "alameda_2018_composite_voter_guide",
        "alameda_2018_composite_voter_guide__2018__11",
        "https://acvote.alamedacountyca.gov/acvote-assets/02_election_information/PDFs/20181106/en/Composite%20Voter%20Information%20Guide/Composite%20Voter%20Information%20Guide.pdf",
        "pdf",
        "application/pdf",
        "2026-08-25T16:06:20Z",
    ),
    FileSpec(
        "san_diego_past_elections.html",
        "san_diego_county_past_sample_ballots",
        "san_diego_county_past_sample_ballots__2020__11",
        "https://www.sdvote.com/content/rov/en/past-election-info.html",
        "html",
        "text/html;charset=utf-8",
        "2026-08-25T16:11:31Z",
    ),
    FileSpec(
        "san_diego_201411_sample_ballots.pdf",
        "san_diego_county_past_sample_ballots",
        "san_diego_county_past_sample_ballots__2014__11",
        "https://www.sdvote.com/content/dam/rov/en/archive/201411%20Sample%20Ballots.pdf",
        "pdf",
        "application/pdf",
        "2026-08-25T16:12:36Z",
    ),
    FileSpec(
        "san_diego_201811_sample_ballots.pdf",
        "san_diego_county_past_sample_ballots",
        "san_diego_county_past_sample_ballots__2018__11",
        "https://www.sdvote.com/content/dam/rov/en/archive/Sample%20Ballots%20201811.pdf",
        "pdf",
        "application/pdf",
        "2026-08-25T16:12:36Z",
    ),
    FileSpec(
        "sacramento_archived_elections.html",
        "sacramento_county_archived_elections",
        "sacramento_county_archived_elections__2016__11",
        "https://elections.saccounty.gov/us/en/election-information/ResultsPrevElect.html",
        "html",
        "text/html;charset=utf-8",
        "2026-08-25T16:11:31Z",
    ),
    FileSpec(
        "sacramento_201206_candidate_list.pdf",
        "sacramento_county_archived_elections",
        "sacramento_county_archived_elections__2012__6",
        "https://elections.saccounty.gov/content/dam/vre/documents/election-information-archive/2012%20June%205%20Presidential%20Primary%20Election%20Qualified%20Candidate%20List.pdf",
        "pdf",
        "application/pdf",
        "2026-08-25T16:16:42Z",
    ),
    FileSpec(
        "sacramento_201211_candidate_list.pdf",
        "sacramento_county_archived_elections",
        "sacramento_county_archived_elections__2012__11",
        "https://elections.saccounty.gov/content/dam/vre/documents/election-information-archive/2012%20November%206%2c%202012%20Presidential%20General%20Election%20Qualified%20Candidate%20List.pdf",
        "pdf",
        "application/pdf",
        "2026-08-25T16:16:42Z",
    ),
    FileSpec(
        "sacramento_201606_candidate_list.pdf",
        "sacramento_county_archived_elections",
        "sacramento_county_archived_elections__2016__11",
        "https://elections.saccounty.gov/content/dam/vre/documents/election-information-archive/Qualified%20Candidate%20List%20-%20060716.pdf",
        "pdf",
        "application/pdf",
        "2026-08-25T16:16:42Z",
    ),
)

SOURCE_DATES = {
    "minneapolis_2013_cbs_candidate_websites": "2013-10-27",
    "alameda_2018_composite_voter_guide": "2018-11-06",
    "san_diego_county_past_sample_ballots": "",
    "sacramento_county_archived_elections": "",
}

SOURCE_ATTEMPT_TIMES = {
    "minneapolis_2013_cbs_candidate_websites": "2026-08-25T16:06:20Z",
    "alameda_2018_composite_voter_guide": "2026-08-25T16:06:20Z",
    "sf_2018_official_voter_pamphlet": "2026-08-25T16:11:31Z",
    "san_diego_county_past_sample_ballots": "2026-08-25T16:12:36Z",
    "sacramento_county_archived_elections": "2026-08-25T16:16:42Z",
    "riverside_county_past_candidate_statements": "2026-08-25T16:11:31Z",
}

TARGET_EXTRA_FILES = {
    ("san_diego_county_past_sample_ballots", "2014", "11"):
        ("san_diego_201411_sample_ballots.pdf",),
    ("san_diego_county_past_sample_ballots", "2018", "11"):
        ("san_diego_201811_sample_ballots.pdf",),
    ("sacramento_county_archived_elections", "2012", "6"):
        ("sacramento_201206_candidate_list.pdf",),
    ("sacramento_county_archived_elections", "2012", "11"):
        ("sacramento_201211_candidate_list.pdf",),
    ("sacramento_county_archived_elections", "2016", "11"):
        ("sacramento_201606_candidate_list.pdf",),
}

SOURCE_BASE_FILES = {
    "minneapolis_2013_cbs_candidate_websites": ("minneapolis_2013_cbs.html",),
    "alameda_2018_composite_voter_guide": ("alameda_2018_composite_voter_guide.pdf",),
    "san_diego_county_past_sample_ballots": ("san_diego_past_elections.html",),
    "sacramento_county_archived_elections": ("sacramento_archived_elections.html",),
}

UNAVAILABLE_TARGETS = {
    "municipal_target_701a368458fa51e4540d": (
        "The official index contains no supported 2020 sample-ballot file for this contest."
    ),
    "municipal_target_969a332321567c5cae38": (
        "The official index exposes the 2013 sample ballots only as a ZIP package, which is outside the frozen HTML/PDF acquisition contract."
    ),
    "municipal_target_7a15059da2076bf8fc22": (
        "The official index contains no supported 2012 sample-ballot file for this contest."
    ),
    "municipal_target_171c6b582f5e715df540": (
        "The official index contains no supported 2012 sample-ballot file for this contest."
    ),
    "municipal_target_ce75f604133d30d87eb2": (
        "The official index contains no supported November 2016 sample-ballot file for this contest."
    ),
    "municipal_target_e89b6575fc408ba8de2b": (
        "The official index exposes the June 2016 nonpartisan sample ballots only as a ZIP package, which is outside the frozen HTML/PDF acquisition contract."
    ),
    "municipal_target_f0700386cc0b709ddac9": (
        "The assigned archive is explicitly limited to elections before 2020 and contains no exact 2020 file."
    ),
    "municipal_target_03ba2ecb19b390ea36b0": "The official Riverside archive returned HTTP 403.",
    "municipal_target_793b9ac39056bd3e0144": "The official Riverside archive returned HTTP 403.",
    "municipal_target_e2d627bcb8154740701b": "The official Riverside archive returned HTTP 403.",
    "municipal_target_8654d9751e047167579e": "The official Riverside archive returned HTTP 403.",
    "municipal_target_d1489238246ec104e46e": "The official Riverside archive returned HTTP 403.",
}

REQUEST_ERROR_TARGETS = {
    "municipal_target_bcd875e81208dd5cb16e": (
        "The exact SFPL PDF and its Wayback CDX route timed out after bounded retries."
    ),
}

DATE_CONFLICT_NOTES = {
    "municipal_target_4c4bd2e96efc8132e680": (
        "The closest official candidate list is dated June 7, 2016, while the frozen target is November 2016; the date conflict is retained here."
    )
}

PDF_TARGET_FILES = {
    "alameda_2018_composite_voter_guide.pdf": {
        "municipal_target_1aa6164622d86733850f",
        "municipal_target_fae50702b488d1c33db3",
    },
    "san_diego_201411_sample_ballots.pdf": {"municipal_target_5d3762d8e9e3e766c294"},
    "san_diego_201811_sample_ballots.pdf": {
        "municipal_target_313c78ee71c1aad7abea",
        "municipal_target_593db4e387ded36f5a41",
    },
    "sacramento_201206_candidate_list.pdf": {"municipal_target_9f817b9486f31f659731"},
    "sacramento_201211_candidate_list.pdf": {"municipal_target_05c1775524c0a4188e40"},
    "sacramento_201606_candidate_list.pdf": {"municipal_target_4c4bd2e96efc8132e680"},
}

URL_PATTERN = re.compile(r"(?i)\b(?:https?://|www\.)[^\s<>\"'\]\[{}]+")
BARE_DOMAIN_PATTERN = re.compile(
    r"(?i)(?<![@\w.-])(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"(?:com|org|net|us|info|biz|co|me|io)(?:/[^\s<>\"']*)?"
)
TRAILING_URL_PUNCTUATION = ".,;:!?)]}"
COMMON_SECOND_LEVEL_SUFFIXES = {"co.uk", "com.au", "com.br", "com.mx", "org.uk", "org.au"}
SOCIAL_DOMAINS = {
    "facebook.com", "instagram.com", "linkedin.com", "tiktok.com", "twitter.com",
    "x.com", "youtube.com",
}
FUNDRAISING_DOMAINS = {"actblue.com", "anedot.com", "donorbox.org", "gofundme.com", "paypal.com", "winred.com"}
SHARED_DOMAINS = {"blogspot.com", "tumblr.com", "weebly.com", "wixsite.com", "wordpress.com"}
NON_CANDIDATE_DOMAINS = {"lp.org", "sos.state.mn.us", "themilitant.com"}
SOURCE_NAVIGATION_DOMAINS = {
    "acgov.org", "alamedacountyca.gov", "cbsnews.com", "saccounty.gov", "sdvote.com",
}


@dataclass(frozen=True)
class URLRecord:
    raw_url: str
    context: str
    locator: str
    method: str
    owner_name: str = ""


class LinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.current_href: str | None = None
        self.current_text: list[str] = []
        self.anchors: list[tuple[str, str, int]] = []
        self.visible_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() == "a":
            self.current_href = dict(attrs).get("href") or ""
            self.current_text = []

    def handle_data(self, data: str) -> None:
        self.visible_parts.append(data)
        if self.current_href is not None:
            self.current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() == "a" and self.current_href is not None:
            label = clean_text(" ".join(self.current_text))
            href = urljoin(self.base_url, html.unescape(self.current_href))
            self.anchors.append((href, label, len(self.anchors) + 1))
            self.current_href = None
            self.current_text = []


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def stable_id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(str(value) for value in values)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_name(value: object) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", clean_text(value).casefold()).split())


def iter_url_matches(value: object, include_bare: bool = True) -> list[tuple[str, int, int]]:
    text = str(value or "")
    found: list[tuple[str, int, int]] = []
    spans: list[tuple[int, int]] = []
    for match in URL_PATTERN.finditer(text):
        found.append((match.group(0).rstrip(TRAILING_URL_PUNCTUATION), match.start(), match.end()))
        spans.append(match.span())
    if include_bare:
        for match in BARE_DOMAIN_PATTERN.finditer(text):
            if any(start <= match.start() < end for start, end in spans):
                continue
            found.append((match.group(0).rstrip(TRAILING_URL_PUNCTUATION), match.start(), match.end()))
    deduplicated: list[tuple[str, int, int]] = []
    seen: set[tuple[str, int]] = set()
    for raw_url, start, end in found:
        key = (raw_url, start)
        if raw_url and key not in seen:
            deduplicated.append((raw_url, start, end))
            seen.add(key)
    return deduplicated


def extract_urls(value: object, include_bare: bool = True) -> list[str]:
    return list(dict.fromkeys(raw_url for raw_url, _, _ in iter_url_matches(value, include_bare)))


def normalize_exact_url(value: str) -> str:
    raw = html.unescape(str(value).strip()).rstrip(TRAILING_URL_PUNCTUATION)
    if not raw.casefold().startswith(("http://", "https://")):
        raw = "http://" + raw
    try:
        parsed = urlsplit(raw)
        if not parsed.hostname:
            return ""
        host = parsed.hostname.casefold()
        port = f":{parsed.port}" if parsed.port else ""
    except ValueError:
        return ""
    return urlunsplit((parsed.scheme.casefold(), host + port, parsed.path, parsed.query, ""))


def registrable_domain(value: str) -> str:
    host = (urlsplit(normalize_exact_url(value)).hostname or "").casefold().removeprefix("www.")
    if not host or re.fullmatch(r"\d+(?:\.\d+){3}", host) or ":" in host:
        return host
    labels = host.split(".")
    if len(labels) <= 2:
        return host
    suffix = ".".join(labels[-2:])
    if suffix in COMMON_SECOND_LEVEL_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix


def site_root_url(value: str) -> str:
    exact = normalize_exact_url(value)
    parsed = urlsplit(exact)
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", "")) if parsed.netloc else ""


def host_matches(host: str, domains: Iterable[str]) -> bool:
    return any(host == domain or host.endswith("." + domain) for domain in domains)


def classify_url(value: str) -> str:
    exact = normalize_exact_url(value)
    parsed = urlsplit(exact)
    host = (parsed.hostname or "").casefold().removeprefix("www.")
    if host_matches(host, SOCIAL_DOMAINS):
        return "social"
    if host_matches(host, FUNDRAISING_DOMAINS):
        return "fundraising"
    if host_matches(host, NON_CANDIDATE_DOMAINS) or host.endswith(".gov"):
        return "other_lead"
    if host_matches(host, SHARED_DOMAINS):
        return "shared_host_path"
    registered = registrable_domain(exact)
    if registered and host != registered:
        return "candidate_subdomain"
    return "custom_domain"


def is_source_navigation_url(value: str) -> bool:
    host = (urlsplit(normalize_exact_url(value)).hostname or "").casefold().removeprefix("www.")
    return host_matches(host, SOURCE_NAVIGATION_DOMAINS)


def parse_html(path: Path, base_url: str) -> list[URLRecord]:
    parser = LinkParser(base_url)
    parser.feed(path.read_text(encoding="utf-8", errors="replace"))
    records = [
        URLRecord(href, label, f"anchor:{number}", "html_anchor", label)
        for href, label, number in parser.anchors
        if normalize_exact_url(href)
    ]
    visible = clean_text(" ".join(parser.visible_parts))
    records.extend(
        URLRecord(raw_url, visible[:2000], "visible_text", "html_visible_text")
        for raw_url in extract_urls(visible)
        if normalize_exact_url(raw_url)
    )
    return records


def _pdf_object(value: object) -> object:
    return value.get_object() if hasattr(value, "get_object") else value


OWNER_DECLARATION_PATTERN = re.compile(
    r"\bI,\s*([A-Z][A-Z .,'’\"-]{1,100}?),\s*hereby\s+declare",
    flags=re.MULTILINE,
)


def candidate_name_pattern(full_name: str) -> re.Pattern[str] | None:
    tokens = re.findall(r"[a-z0-9]+", normalize_name(full_name))
    if not tokens:
        return None
    return re.compile(r"\b" + r"\W+".join(re.escape(token) for token in tokens) + r"\b", re.I)


def infer_pdf_url_owner(page_text: str, url_start: int, candidate_names: Iterable[str]) -> str:
    """Identify the statement owner, never an endorser who merely shares the page."""
    declaration = OWNER_DECLARATION_PATTERN.search(page_text)
    if declaration:
        return clean_text(declaration.group(1))

    # Some composite guides put two short statements on one page without the
    # formal "I, NAME" declaration. In those pages the closest preceding full
    # contest-candidate heading owns the URL.
    normalized_page = normalize_name(page_text)
    has_mayor_office_heading = bool(re.search(r"(?mi)^\s*MAYOR\s*$", page_text))
    if "candidate s statement" not in normalized_page or not has_mayor_office_heading:
        return ""
    nearest_name = ""
    nearest_end = -1
    for full_name in candidate_names:
        pattern = candidate_name_pattern(full_name)
        if pattern is None:
            continue
        for match in pattern.finditer(page_text, 0, url_start):
            if match.end() > nearest_end:
                nearest_name = full_name
                nearest_end = match.end()
    return nearest_name


def infer_pdf_annotation_owner(page_text: str, candidate_names: Iterable[str]) -> str:
    """Attribute an annotation only when the whole page has one clear owner."""
    declaration = OWNER_DECLARATION_PATTERN.search(page_text)
    if declaration:
        return clean_text(declaration.group(1))
    present: list[str] = []
    for full_name in candidate_names:
        pattern = candidate_name_pattern(full_name)
        if pattern is not None and pattern.search(page_text):
            present.append(full_name)
    return present[0] if len(present) == 1 else ""


def records_from_pdf_page_text(
    page_text: str,
    page_number: int,
    candidate_names: Iterable[str],
) -> list[URLRecord]:
    records: list[URLRecord] = []
    for raw_url, start, end in iter_url_matches(page_text):
        if not normalize_exact_url(raw_url):
            continue
        owner_name = infer_pdf_url_owner(page_text, start, candidate_names)
        context_start = max(0, start - 1200)
        context_end = min(len(page_text), end + 160)
        records.append(
            URLRecord(
                raw_url,
                clean_text(page_text[context_start:context_end])[:2000],
                f"page:{page_number}",
                "pdf_visible_text",
                owner_name,
            )
        )
    return records


def parse_pdf(path: Path, candidate_names: Iterable[str]) -> list[URLRecord]:
    """Extract visible and annotation URLs while preserving page context."""
    try:
        from pypdf import PdfReader
    except ImportError as error:
        raise RuntimeError("pypdf is required for PDF hyperlink-annotation extraction") from error

    reader = PdfReader(str(path))
    if path.name == "alameda_2018_composite_voter_guide.pdf":
        # The Hayward statements use two columns. Poppler's layout order puts
        # the second candidate heading before the first candidate's URL;
        # pypdf preserves the statement stream order needed for attribution.
        page_texts = [(page.extract_text() or "") for page in reader.pages]
    else:
        with tempfile.TemporaryDirectory(prefix="municipal_shard_1_pdf_") as temp_dir:
            text_path = Path(temp_dir) / "visible.txt"
            subprocess.run(
                ["pdftotext", "-layout", str(path), str(text_path)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            page_texts = text_path.read_text(encoding="utf-8", errors="replace").split("\f")

    records: list[URLRecord] = []
    for page_number, page_text in enumerate(page_texts, start=1):
        records.extend(records_from_pdf_page_text(page_text, page_number, candidate_names))

    for page_number, page in enumerate(reader.pages, start=1):
        page_text = clean_text(page_texts[page_number - 1]) if page_number <= len(page_texts) else ""
        for annotation_reference in page.get("/Annots") or []:
            annotation = _pdf_object(annotation_reference)
            if not isinstance(annotation, Mapping) or str(annotation.get("/Subtype", "")) != "/Link":
                continue
            action = _pdf_object(annotation.get("/A", {}))
            raw_url = action.get("/URI", "") if isinstance(action, Mapping) else ""
            if raw_url and normalize_exact_url(str(raw_url)):
                context = clean_text(annotation.get("/Contents", "")) or page_text
                owner_name = infer_pdf_annotation_owner(page_text, candidate_names)
                records.append(
                    URLRecord(
                        str(raw_url), context[:2000], f"page:{page_number}:annotation",
                        "pdf_hyperlink_annotation", owner_name,
                    )
                )
    return records


def parser_id(file_type: str) -> str:
    return "html_links_text_v1" if file_type == "html" else "pdf_text_annotations_v1"


def load_contract_frames() -> tuple[dict[str, list[str]], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    schemas = json.loads(SCHEMAS_PATH.read_text(encoding="utf-8"))
    targets = pd.read_csv(TARGETS_PATH, dtype=str, keep_default_na=False)
    candidates = pd.read_csv(CANDIDATES_PATH, dtype=str, keep_default_na=False)
    parser_matrix = pd.read_csv(PARSER_MATRIX_PATH, keep_default_na=False)
    return schemas, targets, candidates, parser_matrix


def build_file_manifest(targets: pd.DataFrame, file_columns: list[str]) -> tuple[pd.DataFrame, dict[str, dict[str, str]]]:
    source_contract = targets.groupby("source_id", sort=False).first()
    rows: list[dict[str, object]] = []
    lookup: dict[str, dict[str, str]] = {}
    for spec in FILE_SPECS:
        path = FILES_DIR / spec.filename
        if not path.is_file() or path.stat().st_size <= 0:
            raise FileNotFoundError(f"Missing or empty frozen source: {path}")
        digest = sha256_file(path)
        file_id = "sha256:" + digest
        partition_hostname = source_contract.loc[spec.source_id, "partition_hostname"]
        hostname = (urlsplit(spec.request_url).hostname or "").casefold()
        row = {
            "contract_version": CONTRACT_VERSION,
            "file_id": file_id,
            "source_id": spec.source_id,
            "parse_group_id": spec.parse_group_id,
            "shard_id": str(SHARD_ID),
            "partition_hostname": partition_hostname,
            "request_url": spec.request_url,
            "final_url": spec.request_url,
            "request_hostname": hostname,
            "final_hostname": hostname,
            "local_path": "files/" + spec.filename,
            "file_type": spec.file_type,
            "content_type": spec.content_type,
            "http_status": "200",
            "bytes": str(path.stat().st_size),
            "sha256": digest,
            "retrieved_at_utc": spec.retrieved_at_utc,
            "parser_id": parser_id(spec.file_type),
            "parser_version": PARSER_VERSION,
        }
        rows.append(row)
        lookup[spec.filename] = {key: str(value) for key, value in row.items()}
    frame = pd.DataFrame(rows, columns=file_columns)
    if frame["sha256"].duplicated().any():
        raise RuntimeError("Frozen files contain duplicate content")
    return frame, lookup


def parse_frozen_files(candidates: pd.DataFrame) -> dict[str, list[URLRecord]]:
    parsed: dict[str, list[URLRecord]] = {}
    for spec in FILE_SPECS:
        path = FILES_DIR / spec.filename
        if spec.file_type == "html":
            parsed[spec.filename] = parse_html(path, spec.request_url)
            continue
        target_ids = PDF_TARGET_FILES.get(spec.filename, set())
        candidate_names = candidates.loc[candidates["target_id"].isin(target_ids), "full_name"].tolist()
        parsed[spec.filename] = parse_pdf(path, candidate_names)
    return parsed


def candidate_in_context(full_name: str, context: str) -> bool:
    candidate = normalize_name(full_name)
    normalized_context = normalize_name(context)
    return bool(candidate and re.search(r"(?:^| )" + re.escape(candidate) + r"(?: |$)", normalized_context))


def source_label_match_method(
    full_name: str,
    source_label: str,
    contest_candidate_names: Iterable[str],
) -> str:
    """Return an explicit CBS label-match method, including source-name conflicts."""
    candidate_tokens = normalize_name(full_name).split()
    label_tokens = normalize_name(source_label).split()
    if not candidate_tokens or not label_tokens:
        return ""
    if normalize_name(full_name) == normalize_name(source_label):
        return "source_label_exact"
    if len(candidate_tokens) == 1:
        return "source_label_exact" if candidate_tokens[0] == label_tokens[0] else ""
    surname = candidate_tokens[-1]
    if candidate_tokens[0] == label_tokens[0] and surname in label_tokens[1:]:
        return "source_label_middle_name_tolerant"
    if surname in label_tokens[1:] and label_tokens[0] in candidate_tokens[:-1]:
        return "source_label_nickname"
    contest_surnames = [normalize_name(name).split()[-1] for name in contest_candidate_names if normalize_name(name)]
    if (
        surname in label_tokens[1:]
        and contest_surnames.count(surname) == 1
        and edit_distance(candidate_tokens[0], label_tokens[0]) <= 2
    ):
        return "source_label_unique_surname_first_name_conflict"
    return ""


def source_label_matches_candidate(full_name: str, source_label: str) -> bool:
    """Compatibility wrapper used by focused fixtures with one candidate."""
    return bool(source_label_match_method(full_name, source_label, [full_name]))


def edit_distance(left: str, right: str) -> int:
    """Small deterministic Levenshtein distance for source-label typo checks."""
    previous = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current = [left_index]
        for right_index, right_char in enumerate(right, start=1):
            current.append(
                min(
                    current[-1] + 1,
                    previous[right_index] + 1,
                    previous[right_index - 1] + (left_char != right_char),
                )
            )
        previous = current
    return previous[-1]


def build_evidence(
    targets: pd.DataFrame,
    candidates: pd.DataFrame,
    parsed: dict[str, list[URLRecord]],
    file_lookup: dict[str, dict[str, str]],
    evidence_columns: list[str],
) -> pd.DataFrame:
    target_lookup = targets.set_index("target_id")
    rows_by_key: dict[tuple[str, str, str, str], dict[str, object]] = {}

    # CBS explicitly labels each candidate and links the campaign website or profile.
    cbs_filename = "minneapolis_2013_cbs.html"
    cbs_target_id = "municipal_target_c607e0e08192f9ab9ea7"
    cbs_candidates = candidates.loc[candidates["target_id"].eq(cbs_target_id)]
    cbs_candidate_names = cbs_candidates["full_name"].tolist()
    for candidate in cbs_candidates.itertuples(index=False):
        for record in parsed[cbs_filename]:
            if record.method != "html_anchor":
                continue
            identity_match = source_label_match_method(
                candidate.full_name, record.owner_name, cbs_candidate_names
            )
            if not identity_match:
                continue
            add_evidence_row(
                rows_by_key, target_lookup.loc[cbs_target_id], candidate, record,
                file_lookup[cbs_filename], evidence_columns, identity_match,
            )

    # Candidate names and URLs must occur on the same PDF page. Generic source-navigation
    # URLs are excluded because they are not claims about candidate-owned sites.
    for filename, target_ids in PDF_TARGET_FILES.items():
        for target_id in target_ids:
            for candidate in candidates.loc[candidates["target_id"].eq(target_id)].itertuples(index=False):
                for record in parsed[filename]:
                    if is_source_navigation_url(record.raw_url):
                        continue
                    if normalize_name(record.owner_name) != normalize_name(candidate.full_name):
                        continue
                    add_evidence_row(
                        rows_by_key, target_lookup.loc[target_id], candidate, record,
                        file_lookup[filename], evidence_columns, "candidate_statement_owner_exact",
                    )

    rows = sorted(
        rows_by_key.values(),
        key=lambda row: (str(row["target_id"]), str(row["candidate_year_id"]), str(row["exact_normalized_url"])),
    )
    return pd.DataFrame(rows, columns=evidence_columns)


def add_evidence_row(
    rows_by_key: dict[tuple[str, str, str, str], dict[str, object]],
    target: pd.Series,
    candidate: object,
    record: URLRecord,
    file_row: dict[str, str],
    evidence_columns: list[str],
    identity_match: str,
) -> None:
    exact = normalize_exact_url(record.raw_url)
    if not exact:
        return
    target_id = str(target.name)
    key = (target_id, str(candidate.candidate_year_id), file_row["file_id"], exact)
    method_locator = f"{record.method} {record.locator}"
    if key in rows_by_key:
        prior = str(rows_by_key[key]["evidence_text"])
        if method_locator not in prior:
            rows_by_key[key]["evidence_text"] = prior + f"; also extracted by {method_locator}"
        return
    source_id = str(target.source_id)
    evidence_text = (
        f"{method_locator}: source labels {record.owner_name or candidate.full_name} and supplies "
        f"{record.raw_url}; matched to {candidate.full_name} via {identity_match}."
    )
    evidence_id = stable_id(
        "municipal_evidence", target_id, candidate.candidate_year_id,
        file_row["file_id"], exact,
    )
    row = {
        "contract_version": CONTRACT_VERSION,
        "evidence_id": evidence_id,
        "attempt_key": target.attempt_key,
        "target_id": target_id,
        "source_id": source_id,
        "parse_group_id": target.parse_group_id,
        "shard_id": str(SHARD_ID),
        "candidate_year_id": candidate.candidate_year_id,
        "contest": target.contest,
        "raw_url": record.raw_url,
        "exact_normalized_url": exact,
        "site_root_url": site_root_url(exact),
        "registrable_domain": registrable_domain(exact),
        "url_type": classify_url(exact),
        "source_url": file_row["request_url"],
        "source_date": SOURCE_DATES.get(source_id, ""),
        "source_file_id": file_row["file_id"],
        "source_file_sha256": file_row["sha256"],
        "source_snapshot_timestamp": "",
        "evidence_text": evidence_text,
        "expected_website_field": target.expected_website_field,
        "parser_id": file_row["parser_id"],
        "parser_version": PARSER_VERSION,
        "identity_match": identity_match,
        "temporal_match": "source_date_in_election_period_unarchived",
        "evidence_grade": "C",
        "review_status": "pending",
    }
    rows_by_key[key] = {column: row.get(column, "") for column in evidence_columns}


def files_for_target(target: object, file_lookup: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    filenames = list(SOURCE_BASE_FILES.get(str(target.source_id), ()))
    filenames.extend(TARGET_EXTRA_FILES.get((str(target.source_id), str(target.year), str(target.month)), ()))
    return [file_lookup[filename] for filename in filenames]


def attempt_failure(target_id: str) -> tuple[str, str, int, str]:
    if target_id in REQUEST_ERROR_TARGETS:
        return "request_error", REQUEST_ERROR_TARGETS[target_id], 2, "000"
    if target_id in UNAVAILABLE_TARGETS:
        error = UNAVAILABLE_TARGETS[target_id]
        status = "403" if "HTTP 403" in error else "200"
        return "source_unavailable", error, 0, status
    return "", "", 0, "200"


def build_attempts(
    targets: pd.DataFrame,
    evidence: pd.DataFrame,
    file_lookup: dict[str, dict[str, str]],
    attempt_columns: list[str],
) -> pd.DataFrame:
    evidence_counts = evidence.groupby("target_id").size().to_dict() if len(evidence) else {}
    rows: list[dict[str, object]] = []
    for target in targets.sort_values("wave2_rank", key=lambda values: values.astype(int)).itertuples(index=False):
        target_files = files_for_target(target, file_lookup)
        evidence_n = int(evidence_counts.get(str(target.target_id), 0))
        failure_status, error, retry_count, http_status = attempt_failure(str(target.target_id))
        if evidence_n:
            terminal_status = "found"
            error = ""
            match_note = f"Matched {evidence_n} candidate URL evidence row(s) in the frozen source."
        elif failure_status:
            terminal_status = failure_status
            match_note = "No evidence emitted; the acquisition limitation is explicit and retained for retry/review."
        else:
            terminal_status = "no_result"
            match_note = "The frozen source was parsed for visible and embedded links but supplied no URL for a contracted candidate."
        if str(target.target_id) in DATE_CONFLICT_NOTES:
            match_note += " " + DATE_CONFLICT_NOTES[str(target.target_id)]

        file_ids = [row["file_id"] for row in target_files]
        parser_ids = list(dict.fromkeys(row["parser_id"] for row in target_files))
        row = {
            "contract_version": CONTRACT_VERSION,
            "attempt_key": target.attempt_key,
            "target_id": target.target_id,
            "source_id": target.source_id,
            "parse_group_id": target.parse_group_id,
            "shard_id": str(SHARD_ID),
            "partition_hostname": target.partition_hostname,
            "terminal_status": terminal_status,
            "attempted_at_utc": SOURCE_ATTEMPT_TIMES[str(target.source_id)],
            "retry_count": str(retry_count),
            "http_status": http_status,
            "request_url": target.exact_source_url,
            "final_url": target.exact_source_url,
            "downloaded_file_n": str(len(file_ids)),
            "downloaded_file_ids_json": json.dumps(file_ids, separators=(",", ":")),
            "evidence_row_n": str(evidence_n),
            "parser_ids_json": json.dumps(parser_ids, separators=(",", ":")),
            "error": error,
            "match_note": match_note,
        }
        rows.append({column: row.get(column, "") for column in attempt_columns})
    return pd.DataFrame(rows, columns=attempt_columns)


def write_outputs() -> dict[str, object]:
    schemas, targets, candidates, parser_matrix = load_contract_frames()
    if set(targets["target_id"]) != set(pd.read_csv(TARGETS_PATH, dtype=str)["target_id"]):
        raise RuntimeError("Target contract changed while building the shard")

    file_frame, file_lookup = build_file_manifest(targets, schemas["downloaded_file_columns"])
    parsed = parse_frozen_files(candidates)
    evidence = build_evidence(
        targets, candidates, parsed, file_lookup, schemas["evidence_columns"]
    )
    attempts = build_attempts(targets, evidence, file_lookup, schemas["attempt_columns"])

    attempts.to_csv(OUTPUT_DIR / "attempts.csv", index=False, lineterminator="\n")
    file_frame.to_csv(OUTPUT_DIR / "downloaded_files.csv", index=False, lineterminator="\n")
    evidence.to_csv(OUTPUT_DIR / "evidence.csv", index=False, lineterminator="\n")

    summary = audit_post_shard_directories(
        targets,
        candidates,
        parser_matrix,
        {SHARD_ID: OUTPUT_DIR},
    )
    summary.update(
        {
            "candidate_screen_evidence_rows": int(
                evidence["candidate_year_id"].isin(
                    set(candidates.loc[candidates["in_candidate_screen"].str.casefold().eq("true"), "candidate_year_id"])
                ).sum()
            ),
            "complete_contest_screen_evidence_rows": int(
                evidence["candidate_year_id"].isin(
                    set(candidates.loc[candidates["in_complete_contest_screen"].str.casefold().eq("true"), "candidate_year_id"])
                ).sum()
            ),
            "strict_evidence_rows": int(evidence["evidence_grade"].isin(["A", "B"]).sum()),
        }
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Build and run the frozen post-shard contract audit")
    parser.parse_args()
    summary = write_outputs()
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
