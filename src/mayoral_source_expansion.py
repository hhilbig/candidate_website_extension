"""Extract and attribute website leads from saved documents, without network calls.

The parser preserves every link occurrence. Attribution supports discovery only;
it never approves historical ownership or gives a claim strict coverage credit.
"""

from __future__ import annotations

import csv
import html
import io
import ipaddress
import json
import re
import shutil
import subprocess
import tempfile
import time
import unicodedata
import zipfile
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlsplit, urlunsplit
from xml.etree import ElementTree

from bs4 import BeautifulSoup, Comment

from scripts.extract_municipal_mayoral_urls import (
    classify_url, excel_column_index, normalize_exact_url,
    registrable_domain, site_root_url, xlsx_cell_text,
)
PARSER_VERSION = "mayoral-source-expansion-v3"
ATTRIBUTION_VERSION = "mayoral-source-attribution-v7"
OCR_SECONDS = 120
OCR_MAX_PAGES = 20
OCR_MAX_CONTENT_BYTES = 30_000_000
OCR_MIN_TEXT_CHARACTERS = 60
CONTEXT_CHARACTERS = 6000
HEADINGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
URL_RE = re.compile(
    r"(?i)(?<![\w@./-])(?:https?://[^\s<>\"'\[\]{}]+|"
    r"(?:www\.)?(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"(?:com|org|net|us|info|biz|co|me|io|gov|edu|ca|uk|vote|votes|city|"
    r"democrat|republican|campaign|nyc|sf|la|cc|tv|name|online|site|website)"
    r"(?::\d+)?(?:[/#?][^\s<>\"'\[\]{}]*)?(?![\w.-]))"
)
CAMPAIGN_RE = re.compile(r"\b(campaign|official website|web\s*site|homepage|for mayor|elect|vote|personal site)\b", re.I)
WEBSITE_RE = re.compile(r"\b(web\s*site|home\s*page|(?:campaign|official|personal)\s+(?:web\s*)?site)\b", re.I)
PROFILE_HOSTS = {"ballotpedia.org", "votesmart.org", "vote411.org", "ballotready.org", "civicengine.com", "wikipedia.org", "smartvoter.org", "votersedge.org", "lwv.org", "dnet.org", "onyourballot.vote411.org", "ourcampaigns.com"}
REFERENCE_HOSTS = {"lp.org", "gop.com", "democrats.org", "gp.org", "gpus.org", "nytimes.com", "latimes.com", "dallasnews.com", "reviewjournal.com", "washingtonpost.com", "apnews.com", "reuters.com", "usatoday.com", "patch.com", "politico.com", "ballot-access.org"}
TECHNICAL_HOSTS = {"schema.org", "w3.org", "w3schools.com", "purl.org", "brightspotcdn.com", "cloudfront.net", "googleusercontent.com", "gstatic.com", "wp.com", "gravatar.com", "ytimg.com", "twimg.com"}
METADATA_PROPERTIES = {"@context", "image", "logo", "thumbnail", "thumbnailurl", "thumbnailimage", "publisher", "author", "creator", "copyrightholder", "ispartof", "potentialaction"}
SITE_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}
CLAIM_CANDIDATE_FIELDS = {
    "candidate_year_id", "candidate_contest_id", "contest", "contest_id", "ledb_candid",
    "full_name", "candidate_name", "name", "query_name", "aliases", "firstname", "lastname",
    "place_fips", "geo_name", "state_abb", "year", "month", "election_round", "round", "office",
    "contest_n_candidates", "contest_validation_status", "direct_election_status",
    "election_stage_status", "election_stage_source_url", "election_stage_note", "stage_scope_status",
    "in_verified_general_scope", "in_discovery_scope", "source_dataset", "source_doi",
    "period", "region", "population_2020", "population_group", "population_status", "population_source",
}
BAD_SUFFIXES = {"jpg", "jpeg", "png", "gif", "webp", "svg", "ico", "css", "js", "woff", "woff2", "ttf", "mp4", "mp3", "zip"}


def _clean(value: object) -> str:
    return " ".join(str(value or "").split())


def _decode(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "windows-1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            pass
    return content.decode("utf-8", errors="replace")


def _tool(name: str) -> str | None:
    return shutil.which(name) or next((str(p) for p in (Path("/opt/homebrew/bin") / name, Path("/usr/local/bin") / name) if p.is_file()), None)


def _archive_host(host: str) -> bool:
    return host in {"web.archive.org", "wayback.archive-it.org", "wayback.archive.org"} or "wayback" in host or host.startswith("webarchive.")


def _transport_target(raw: str, page_url: str) -> str:
    """Resolve source links while retaining historical replay routes."""
    value = html.unescape(str(raw or "").strip())
    if not value or re.search(r"[\x00-\x20\x7f]", value) or value.startswith("#"):
        return ""
    try:
        parsed = urlsplit(value)
        if parsed.scheme and parsed.scheme.casefold() not in {"http", "https"}:
            return ""
        if parsed.scheme and not parsed.hostname:
            return ""
        if not parsed.scheme and not value.startswith(("/", "?", "./", "../")):
            if "@" in value.split("/")[0]:
                return ""
            if URL_RE.fullmatch(value):
                value = "http://" + value
        base = urlsplit(page_url)
        embedded = re.search(r"/(https?://.+)$", page_url, re.I) if _archive_host((base.hostname or "").casefold()) else None
        if embedded and not urlsplit(value).scheme and not value.startswith("//"):
            explicit_replay = bool(re.search(r"/(?:https?://|https?%3A%2F%2F)", value, re.I))
            if explicit_replay:
                value = urljoin(page_url, value)
            else:
                value = page_url[:embedded.start(1)] + urljoin(embedded.group(1), value)
        else:
            value = urljoin(page_url, value)
        p = urlsplit(value)
        if p.scheme.casefold() not in {"http", "https"} or not p.hostname or p.username or p.password:
            return ""
        port = f":{p.port}" if p.port else ""
        host = p.hostname.casefold()
        if ":" in host:
            host = "[" + host + "]"
        return urlunsplit((p.scheme.casefold(), host + port, p.path, p.query, ""))
    except (ValueError, UnicodeError):
        return ""


def _normalized_target(raw: str, page_url: str) -> str:
    """Resolve links without converting emails, script targets, or paths to hosts."""
    value = _transport_target(raw, page_url)
    if not value:
        return ""
    try:
        # UCLA and other institutional replay hosts use the same embedded target.
        host = (urlsplit(value).hostname or "").casefold()
        if _archive_host(host):
            match = re.search(r"/(https?://.+)$", value, re.I)
            if match:
                value = match.group(1)
            else:
                decoded = unquote(value)
                match = re.search(r"/(https?://.+)$", decoded, re.I)
                if match:
                    value = match.group(1)
        # Search redirect wrappers are references, not candidate domains.
        p = urlsplit(value)
        if (p.hostname or "").casefold() in {"www.google.com", "google.com", "duckduckgo.com"}:
            query = parse_qs(p.query)
            for key in ("url", "q", "uddg"):
                target = query.get(key, [""])[0]
                if target.startswith(("https://", "http://")):
                    value = target
                    break
        exact = normalize_exact_url(value)
        p = urlsplit(exact)
        host = p.hostname or ""
        if not exact or p.username or p.password or "." not in host or "@" in host:
            return ""
        try:
            if ipaddress.ip_address(host).is_global is False:
                return ""
        except ValueError:
            pass
        if host.casefold() in {"localhost", "example.com", "example.org", "example.net"} or host.casefold().endswith((".localhost", ".local", ".invalid", ".test")):
            return ""
        if not all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label, re.I) for label in host.encode("idna").decode().split(".")):
            return ""
        return exact
    except (ValueError, UnicodeError):
        return ""


def _url_type(exact: str, source_url: str) -> str:
    parsed = urlsplit(exact)
    host = (parsed.hostname or "").casefold().removeprefix("www.")
    if Path(parsed.path).suffix.casefold().lstrip(".") in BAD_SUFFIXES:
        return "non_page_resource"
    if any(host == h or host.endswith("." + h) for h in TECHNICAL_HOSTS):
        return "non_page_resource"
    if host in {"youtu.be", "vimeo.com", "flickr.com", "threads.net", "bsky.app"} or host.endswith(".youtu.be"):
        return "social_profile"
    if host.endswith(".gov") or re.search(r"(?:^|\.)(?:state|ci|co|city|county|town|village|k12)\.(?:[a-z0-9-]+\.)?[a-z]{2}\.us$", host):
        return "officeholder_or_government_profile"
    if host.endswith(".edu") or any(host == h or host.endswith("." + h) for h in REFERENCE_HOSTS):
        return "source_reference"
    if any(host == h or host.endswith("." + h) for h in PROFILE_HOSTS):
        return "third_party_profile"
    kind = classify_url(exact)
    if kind == "shared_host_root":
        return "shared_host_root"
    if kind in {"custom_domain", "candidate_subdomain"} and registrable_domain(exact) == registrable_domain(source_url):
        return "source_internal_page"
    return kind


def _link(raw: str, page_url: str, context: str, locator: str, method: str, anchor: str = "", **extra: object) -> dict | None:
    exact = _normalized_target(raw, page_url)
    if not exact:
        return None
    return {
        "raw_url": raw, "exact_normalized_url": exact, "transport_url": _transport_target(raw, page_url),
        "anchor_text": _clean(anchor), "surrounding_context": _clean(context)[:CONTEXT_CHARACTERS],
        "source_locator": locator, "extraction_method": method,
        "url_type": _url_type(exact, page_url), "registrable_domain": registrable_domain(exact),
        "site_root_url": site_root_url(exact), "parser_version": PARSER_VERSION,
        "context_truncated": len(_clean(context)) > CONTEXT_CHARACTERS, **extra,
    }


def _append(result: dict, *args: object, **kwargs: object) -> None:
    link = _link(*args, **kwargs)
    if link:
        result["links"].append(link)
    else:
        result["invalid_url_count"] += 1


def _text_links(result: dict, text: str, page_url: str, locator: str, method: str, **extra: object) -> None:
    for number, match in enumerate(URL_RE.finditer(text), 1):
        raw = match.group().rstrip(".,;:!)]}")
        # Keep the local line and one prior line for PDF name/website rows.
        before = text[:match.start()]
        after = text[match.end():]
        preceding = "\n".join(before.splitlines()[-2:])[-300:]
        following = "\n".join(after.splitlines()[:1])[:180]
        _append(result, raw, page_url, preceding + raw + following,
                f"{locator}:url:{number}", method, preceding_text=preceding,
                following_text=following, **extra)


def _html_scope(node) -> dict:
    containers = {"tr", "li", "p", "dd", "dt", "section", "div", "article"}
    container = node if node.name in containers else node.find_parent(list(containers)) or node.parent or node
    context = container.get_text(" ", strip=True)
    headings = []
    for heading in node.find_all_previous(list(HEADINGS)):
        scope = node.find_parent(["section", "article", "td", "li"])
        if scope and scope not in heading.parents and heading is not scope:
            continue
        headings.append(heading.get_text(" ", strip=True))
        break
    # Nearby anchor offsets retain the sequence within multi-candidate paragraphs.
    before, after, seen = [], [], False
    for text_node in container.find_all(string=True):
        if node is text_node.parent or node in text_node.parents:
            seen = True
            continue
        (after if seen else before).append(str(text_node))
    ancestors = [node, *list(node.parents)]
    boilerplate = any(p.name in {"footer", "nav", "header", "aside"} or re.search(r"(?:^|[ _-])(footer|navigation|social-share|share-tools)(?:$|[ _-])", " ".join([str(p.get("id", "")), *p.get("class", [])]), re.I) for p in ancestors if getattr(p, "attrs", None) is not None)
    return {"context": context, "heading_context": " | ".join(headings),
            "preceding_text": _clean(" ".join(before))[-CONTEXT_CHARACTERS:],
            "following_text": _clean(" ".join(after))[:CONTEXT_CHARACTERS],
            "is_boilerplate": boilerplate}


def _parse_html(content: bytes, page_url: str, result: dict) -> None:
    soup = BeautifulSoup(content, "html.parser")
    result["title"] = soup.title.get_text(" ", strip=True) if soup.title else ""
    for node in soup.find_all("meta"):
        field = str(node.get("property", node.get("name", node.get("itemprop", "")))).casefold()
        if field in {"article:published_time", "datepublished", "date", "pubdate", "publishdate", "dc.date.issued"} and node.get("content"):
            result["source_publication_date"] = _clean(node["content"])
            result["source_publication_date_locator"] = "meta:" + field
            break
    if not result.get("source_publication_date"):
        times = soup.find_all("time", datetime=True)
        published = [node for node in times if "published" in " ".join([str(node.get("itemprop", "")), *node.get("class", [])]).casefold()]
        if published or len(times) == 1:
            result["source_publication_date"] = _clean((published or times)[0]["datetime"])
            result["source_publication_date_locator"] = "time:datetime"
    base = soup.find("base", href=True)
    base_url = _normalized_target(base["href"], page_url) if base else page_url
    for number, node in enumerate(soup.find_all("script"), 1):
        if "ld+json" in str(node.get("type", "")).casefold():
            try:
                _parse_json_value(json.loads(node.string or node.get_text()), page_url, result, f"jsonld:{number}")
            except (ValueError, TypeError) as error:
                result["warnings"].append(f"jsonld:{number}: {error}")
        elif "/DownloadFile/" in page_url and re.search(
            r"window\.location\s*=\s*window\.location\.toString\(\)\.replace\(\s*([\"'])DownloadFile\1\s*,\s*([\"'])DownloadFileBytes\2\s*\)",
            node.string or node.get_text(),
        ):
            _append(result, page_url.replace("/DownloadFile/", "/DownloadFileBytes/", 1), page_url,
                    result["title"], f"static_document_redirect:{number}", "html_static_document_redirect",
                    "Download original document", relation_role="document_download", url_type="source_reference")
    for node in soup(["script", "style", "noscript", "svg", "template"]):
        node.decompose()
    for number, anchor in enumerate(soup.find_all(["a", "area"], href=True), 1):
        scope = _html_scope(anchor)
        context = scope.pop("context")
        _append(result, str(anchor["href"]), base_url or page_url, context, f"anchor:{number}",
                "html_hyperlink", anchor.get_text(" ", strip=True) or anchor.get("title", "") or anchor.get("alt", ""), **scope)
    for number, meta in enumerate(soup.find_all("meta"), 1):
        if str(meta.get("http-equiv", "")).casefold() == "refresh":
            match = re.search(r"(?:^|;)\s*url\s*=\s*(.+)$", str(meta.get("content", "")), re.I)
            if match:
                _append(result, match.group(1).strip(" '\""), page_url, result["title"], f"meta:{number}", "html_meta_refresh")
    for number, node in enumerate(soup.find_all(string=True), 1):
        if not isinstance(node, Comment) and node.parent and node.parent.name not in {"head", "title"} and URL_RE.search(str(node)):
            scope = _html_scope(node.parent)
            context = scope.pop("context")
            # Parent paragraph context helps identify bare URLs after a candidate name.
            for match_number, match in enumerate(URL_RE.finditer(str(node)), 1):
                raw = match.group().rstrip(".,;:!)]}")
                scope["preceding_text"] = str(node)[:match.start()][-CONTEXT_CHARACTERS:]
                scope["following_text"] = str(node)[match.end():][:CONTEXT_CHARACTERS]
                _append(result, raw, page_url, context, f"text:{number}:url:{match_number}",
                        "html_visible_text", **scope)
    result["text"] = soup.get_text("\n", strip=True)


def _parse_json_value(value: object, page_url: str, result: dict, prefix: str = "json") -> None:
    lines = []

    def visit(node: object, locator: str, parent_context: str = "", relation: str = "", owner_name: str = "", owner_type: str = "", metadata: bool = False) -> None:
        if isinstance(node, list):
            for number, item in enumerate(node):
                visit(item, f"{locator}[{number}]", parent_context, relation, owner_name, owner_type, metadata)
            return
        if not isinstance(node, dict):
            if isinstance(node, str):
                # Scan only this value. Parent context supplies identity, never more URLs.
                for number, match in enumerate(URL_RE.finditer(node), 1):
                    extra = {"json_property": relation, "json_object_type": owner_type,
                             "structured_owner_name": owner_name, "relation_role": "metadata_reference" if metadata else "structured_reference"}
                    if metadata:
                        extra["url_type"] = "non_page_resource" if relation in {"image", "logo", "thumbnail", "thumbnailurl", "@context"} else "source_reference"
                    _append(result, match.group().rstrip(".,;:!)]}"), page_url, parent_context + " | " + node,
                            f"{locator}:{number}", "json_field", relation, **extra)
            return
        scalars = {str(key): val for key, val in node.items() if not isinstance(val, (dict, list)) and val is not None}
        local = " | ".join(f"{key}: {val}" for key, val in scalars.items())
        context = " | ".join(part for part in (parent_context, local) if part)
        object_type = _clean(node.get("@type", "")) or owner_type
        object_name = _clean(node.get("name", "")) or owner_name
        if not result.get("source_publication_date") and node.get("datePublished") and not metadata:
            result["source_publication_date"] = _clean(node["datePublished"])
            result["source_publication_date_locator"] = locator + ":datePublished"
        lines.append(local)
        for key, val in scalars.items():
            property_name = key.casefold()
            meta_link = metadata or property_name in METADATA_PROPERTIES
            article_link = bool(re.search(r"article|news|blogposting|webpage|website|organization", object_type, re.I))
            explicit_website = bool(re.search(r"website|homepage|campaign[_ ]?(?:site|url)", property_name))
            relation_role = "metadata_reference" if meta_link else "explicit_website_field" if explicit_website else "article_reference" if article_link and property_name in {"url", "@id", "mainentityofpage"} else "person_url" if property_name == "url" and object_type.casefold() == "person" else "structured_reference"
            extra = {"json_property": key, "json_object_type": object_type,
                     "structured_owner_name": object_name, "relation_role": relation_role}
            if meta_link or relation_role == "article_reference":
                extra["url_type"] = "non_page_resource" if property_name in {"@context", "image", "logo", "thumbnail", "thumbnailurl"} or object_type.casefold() in {"imageobject", "videoobject"} else "source_reference"
            for number, match in enumerate(URL_RE.finditer(str(val or "")), 1):
                raw = match.group().rstrip(".,;:!)]}")
                if property_name in {"articlebody", "description", "text"} and len(str(val)) > 200:
                    before, after = str(val)[:match.start()][-300:], str(val)[match.end():][:180]
                    _append(result, raw, page_url, before + raw + after, f"{locator}:{key}:{number}", "json_text_url",
                            preceding_text=before, following_text=after, **extra)
                else:
                    _append(result, raw, page_url, context, f"{locator}:{key}:{number}", "json_field", str(key), **extra)
        for key, val in node.items():
            if isinstance(val, (list, dict)):
                # Publishers, images, and authors own their own metadata references.
                # The candidate named in a headline does not own those URLs.
                child_metadata = metadata or key.casefold() in METADATA_PROPERTIES
                child_context = "" if child_metadata else context
                visit(val, f"{locator}.{key}", child_context, key.casefold(), "" if child_metadata else object_name,
                      "" if child_metadata else object_type, child_metadata)

    visit(value, prefix)
    result["text"] += "\n".join(lines)


def _parse_xlsx(content: bytes, page_url: str, result: dict) -> None:
    spreadsheet_ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    package_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
    office_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    lines = []
    with zipfile.ZipFile(io.BytesIO(content)) as workbook:
        if sum(info.file_size for info in workbook.infolist()) > 100_000_000:
            raise ValueError("XLSX uncompressed size exceeds 100 MB; source remains unprocessed")
        shared = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            shared = ["".join(node.text or "" for node in item.findall(f".//{{{spreadsheet_ns}}}t"))
                      for item in ElementTree.fromstring(workbook.read("xl/sharedStrings.xml")).findall(f"{{{spreadsheet_ns}}}si")]
        for sheet_name in sorted(name for name in workbook.namelist() if re.fullmatch(r"xl/worksheets/sheet\d+\.xml", name)):
            sheet = ElementTree.fromstring(workbook.read(sheet_name))
            relations = {}
            relationship_name = "xl/worksheets/_rels/" + Path(sheet_name).name + ".rels"
            if relationship_name in workbook.namelist():
                relations = {node.get("Id"): node.get("Target", "") for node in ElementTree.fromstring(workbook.read(relationship_name)).findall(f"{{{package_ns}}}Relationship")}
            hyperlinks = {node.get("ref"): relations.get(node.get(f"{{{office_ns}}}id"), "") for node in sheet.findall(f".//{{{spreadsheet_ns}}}hyperlink")}
            headers = []
            for index, row in enumerate(sheet.findall(f".//{{{spreadsheet_ns}}}row"), 1):
                cells = {node.get("r", ""): xlsx_cell_text(node, shared, spreadsheet_ns) for node in row.findall(f"{{{spreadsheet_ns}}}c")}
                width = max((excel_column_index(reference) + 1 for reference in cells), default=0)
                if len(headers) < width:
                    headers += [f"column_{number + 1}" for number in range(len(headers), width)]
                if index == 1:
                    for reference, value in cells.items():
                        headers[excel_column_index(reference)] = value or headers[excel_column_index(reference)]
                context = " | ".join(f"{headers[excel_column_index(reference)]}: {value}" for reference, value in cells.items())
                locator = f"{Path(sheet_name).stem}:row:{row.get('r', str(index))}"
                lines.append(locator + " | " + context)
                _text_links(result, context, page_url, locator, "xlsx_visible_text")
                for reference, value in cells.items():
                    if hyperlinks.get(reference):
                        _append(result, hyperlinks[reference], page_url, context, locator + ":cell:" + reference,
                                "xlsx_hyperlink", value)
    result["text"] = "\n".join(lines)


def _pdf_pages(path: Path) -> list[str]:
    command = _tool("pdftotext")
    if not command:
        raise RuntimeError("pdftotext unavailable")
    completed = subprocess.run([command, "-layout", str(path), "-"], capture_output=True, timeout=60, check=True)
    pages = _decode(completed.stdout).split("\f")
    if pages and not pages[-1].strip():
        pages.pop()
    return pages


def _pdf_annotations(path: Path, pages: list[str], page_url: str, result: dict) -> None:
    try:
        from pypdf import PdfReader
    except ImportError:
        command = _tool("pdfinfo")
        if not command:
            raise RuntimeError("PDF hyperlink annotations require pypdf or pdfinfo")
        output = subprocess.run([command, "-url", str(path)], capture_output=True, timeout=60, check=True).stdout
        for number, line in enumerate(_decode(output).splitlines(), 1):
            match = re.match(r"\s*(\d+)\s+(\S+)\s+(.+?)\s*$", line)
            if match:
                page = int(match.group(1))
                _append(result, match.group(3), page_url, pages[page - 1] if page <= len(pages) else "",
                        f"pdf:page:{page}:annotation:{number}", "pdf_hyperlink_annotation", page_number=page)
        return
    reader = PdfReader(str(path))
    for page_number, page in enumerate(reader.pages, 1):
        for number, reference in enumerate(page.get("/Annots", []), 1):
            obj = reference.get_object()
            action = obj.get("/A", {})
            action = action.get_object() if hasattr(action, "get_object") else action
            if obj.get("/Subtype") == "/Link" and action.get("/URI"):
                _append(result, str(action["/URI"]), page_url,
                        pages[page_number - 1] if page_number <= len(pages) else "",
                        f"pdf:page:{page_number}:annotation:{number}", "pdf_hyperlink_annotation",
                        str(obj.get("/Contents", "")), page_number=page_number)


def _ocr_page(path: Path, page: int, directory: Path, remaining: float) -> str:
    pdftoppm, tesseract = _tool("pdftoppm"), _tool("tesseract")
    if not pdftoppm or not tesseract:
        raise RuntimeError("OCR requires pdftoppm and tesseract")
    prefix = directory / f"page-{page}"
    started = time.monotonic()
    subprocess.run([pdftoppm, "-f", str(page), "-l", str(page), "-r", "150", "-scale-to", "2200", "-singlefile", "-png", str(path), str(prefix)],
                   capture_output=True, timeout=min(30, remaining), check=True)
    budget = remaining - (time.monotonic() - started)
    if budget <= 0:
        raise TimeoutError("OCR document time budget exceeded")
    completed = subprocess.run([tesseract, str(prefix) + ".png", "stdout", "--psm", "3"],
                               capture_output=True, timeout=min(30, budget), check=True)
    return _decode(completed.stdout)


def _parse_pdf(content: bytes, page_url: str, result: dict, ocr: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="mayoral-source-pdf-") as directory:
        path = Path(directory) / "source.pdf"
        path.write_bytes(content)
        pages = _pdf_pages(path)
        result["page_count"] = len(pages)
        try:
            _pdf_annotations(path, pages, page_url, result)
        except Exception as error:
            result["parse_status"] = "partial"
            result["warnings"].append(f"pdf_annotations: {type(error).__name__}: {error}")
        started, attempted = time.monotonic(), 0
        for number, page_text in enumerate(pages, 1):
            _text_links(result, page_text, page_url, f"pdf:page:{number}", "pdf_visible_text", page_number=number)
            if len(re.sub(r"\W", "", page_text)) >= OCR_MIN_TEXT_CHARACTERS:
                continue
            remaining = OCR_SECONDS - (time.monotonic() - started)
            reason = ("ocr_disabled" if not ocr else "ocr_content_size_limit" if len(content) > OCR_MAX_CONTENT_BYTES else
                      "ocr_page_limit" if attempted >= OCR_MAX_PAGES else "ocr_time_limit" if remaining <= 0 else "")
            if not reason:
                attempted += 1
                result["ocr_used"] = True
                try:
                    ocr_text = _ocr_page(path, number, Path(directory), remaining)
                    pages[number - 1] += "\n" + ocr_text
                    _text_links(result, ocr_text, page_url, f"pdf:page:{number}:ocr", "pdf_ocr_text", page_number=number)
                    result["ocr_pages"].append(number)
                except Exception as error:
                    reason = f"ocr_error:{type(error).__name__}:{error}"
            if reason:
                result["unprocessed_pages"].append({"page": number, "reason": reason})
        result["text"] = "\n\f\n".join(pages)
        if result["unprocessed_pages"]:
            result["parse_status"] = "partial"


def parse_document(content: bytes, content_type: str, page_url: str, *, ocr: bool = True) -> dict:
    """Parse document bytes and retain explicit residuals from bounded OCR."""
    result = {"text": "", "links": [], "parse_status": "parsed", "error": "", "ocr_used": False,
              "ocr_pages": [], "unprocessed_pages": [], "warnings": [], "invalid_url_count": 0,
              "parser_version": PARSER_VERSION, "title": ""}
    mime = str(content_type or "").split(";", 1)[0].casefold().strip()
    suffix = Path(urlsplit(page_url).path).suffix.casefold()
    infer_from_suffix = mime in {"", "application/octet-stream", "binary/octet-stream", "application/unknown"}
    html_signature = bool(re.match(rb"\s*(?:<!doctype\s+html|<html|<head|<body|<article|<div|<p[ >]|<h[1-6][ >])", content, re.I))
    try:
        if content.lstrip().startswith(b"%PDF-"):
            _parse_pdf(content, page_url, result, ocr)
        elif "html" in mime or html_signature:
            _parse_html(content, page_url, result)
        elif mime == "application/pdf" or (infer_from_suffix and suffix == ".pdf"):
            _parse_pdf(content, page_url, result, ocr)
        elif "json" in mime or (infer_from_suffix and suffix in {".json", ".jsonld"}):
            _parse_json_value(json.loads(_decode(content)), page_url, result)
        elif "spreadsheetml" in mime or (infer_from_suffix and suffix == ".xlsx"):
            _parse_xlsx(content, page_url, result)
        elif mime in {"text/csv", "text/tab-separated-values"} or (infer_from_suffix and suffix in {".csv", ".tsv"}):
            text = _decode(content)
            delimiter = "\t" if suffix == ".tsv" or "tab-separated" in mime else ","
            for number, row in enumerate(csv.DictReader(io.StringIO(text), delimiter=delimiter), 2):
                context = " | ".join(f"{key}: {value}" for key, value in row.items())
                _text_links(result, context, page_url, f"row:{number}", "delimited_field")
            result["text"] = text
        elif mime.startswith("text/") or mime in {"application/javascript", "application/xml", ""} or (infer_from_suffix and suffix in {".txt", ".xml"}):
            text = _decode(content)
            if mime == "application/javascript":
                result["warnings"].append("JavaScript parsed as inert text; code was not executed")
            _text_links(result, text, page_url, "text", "plain_text_url")
            result["text"] = text
        else:
            result.update(parse_status="unsupported", error=f"Unsupported content type: {mime or suffix}")
    except Exception as error:
        result.update(parse_status="partial" if result["links"] or result["text"] else "parse_error",
                      error=f"{type(error).__name__}: {error}")
    if result["parse_status"] == "partial" and not result["error"]:
        result["error"] = "; ".join(result["warnings"] + [f"{len(result['unprocessed_pages'])} PDF pages require OCR follow-up"] if result["unprocessed_pages"] else result["warnings"])
    return result


def _norm(value: object) -> str:
    value = unicodedata.normalize("NFKD", _clean(value)).encode("ascii", "ignore").decode().casefold()
    return " ".join(re.findall(r"[a-z0-9]+", value))


def _aliases(candidate: dict) -> list[str]:
    name = _clean(candidate.get("full_name") or candidate.get("candidate_name") or candidate.get("name"))
    aliases = [name, _clean(candidate.get("query_name"))]
    nicknames = re.findall(r'["“‘](.+?)["”’]', name)
    plain = re.sub(r'["“‘].+?["”’]', "", name).strip()
    aliases.append(plain)
    tokens = _norm(plain).split()
    tokens = [x for x in tokens if x not in {"jr", "sr", "ii", "iii", "iv"}]
    if len(tokens) >= 2:
        aliases.append(tokens[0] + " " + tokens[-1])
        aliases.extend(nickname + " " + tokens[-1] for nickname in nicknames)
    other = candidate.get("aliases", [])
    if isinstance(other, str):
        try:
            other = json.loads(other)
        except ValueError:
            other = [other]
    if isinstance(other, list):
        aliases.extend(str(x) for x in other)
    return list(dict.fromkeys(_norm(x) for x in aliases if len(_norm(x).split()) >= 2))


def _matches(text: str, indexed: list[tuple[dict, list[str]]]) -> list[dict]:
    normalized = " " + _norm(text) + " "
    return [candidate for candidate, aliases in indexed if any(" " + alias + " " in normalized for alias in aliases)]


def _temporal(candidate: dict, context: str, source_url: str, title: str, publication_date: str = "") -> str:
    try:
        year = int(float(candidate.get("year", "")))
    except (TypeError, ValueError):
        return "not_assessed"
    # Dates next to election terms flag review. Copyright years do not establish a conflict.
    text = f"{context} {title} {unquote(source_url)}"
    years = {int(m.group(1)) for m in re.finditer(r"\b((?:19|20)\d{2})[\s_/-]{0,12}(?:election|campaign|mayoral|mayor)\b", text, re.I)}
    years |= {int(m.group(1)) for m in re.finditer(r"\b(?:election|campaign|mayoral|mayor)[\s_/-]{0,12}((?:19|20)\d{2})\b", text, re.I)}
    # Election-page titles can put the city between the year and office.
    title_years = {int(m.group(1)) for m in re.finditer(r"\b((?:19|20)\d{2})\s+(?:[a-z]+\s+){0,5}mayoral election\b", _norm(title))}
    if title_years and year not in title_years:
        return "date_conflict_review"
    if years and year not in years:
        return "date_conflict_review"
    published_year = re.search(r"\b((?:19|20)\d{2})\b", publication_date)
    local_target_year = bool(re.search(rf"\b{year}\b", context))
    if published_year and not local_target_year and not year - 2 <= int(published_year.group(1)) <= year + 1:
        return "date_conflict_review"
    return "not_assessed"


def _other_office_campaign(title: str) -> str:
    if re.search(r"\b(?:city council|councilmember|council member)\b", title, re.I) and not re.search(r"\bmayor(?:al)?\b", title, re.I):
        return "city_council_source_without_mayoral_context"
    offices = r"(?:governor|president|senate|senator|congress|congressional|house of representatives|state representative|state assembly)"
    patterns = [
        rf"\bfor\s+(?:[\w'-]+\s+){{0,5}}{offices}\b",
        r"\b(?:gubernatorial|presidential|congressional|senate)\s+(?:candidate|campaign|race|bid)\b",
        rf"\b{offices}\s+(?:candidate|campaign|race)\b",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, title, re.I):
            if not re.search(r"\bmayor(?:al)?\b", match.group(), re.I):
                return match.group()
    return ""


def _candidate_shaped_target(candidate: dict, link: dict) -> bool:
    target = urlsplit(link.get("exact_normalized_url", ""))
    compact = _norm(target.hostname or "").replace(" ", "")
    if link.get("url_type") == "shared_host_path":
        parts = target.path.strip("/").split("/")
        if target.hostname == "sites.google.com" and len(parts) > 1 and parts[0] in {"site", "view"}:
            parts = parts[1:]
        compact += _norm(parts[0]).replace(" ", "")
    return any(alias.replace(" ", "") in compact for alias in _aliases(candidate))


def _name_only_anchor(anchor: str, candidate: dict) -> bool:
    normalized = _norm(anchor)
    normalized = re.sub(r"^(?:mayor|candidate|elect|vote for)\s+|\s+(?:for mayor|campaign)$", "", normalized).strip()
    return normalized in _aliases(candidate)


def ownership_support(candidate: dict, link: dict) -> tuple[bool, str]:
    """Assess target ownership independently of a candidate identity match."""
    if link.get("url_type") not in SITE_TYPES:
        return False, "non_candidate_site_type"
    if link.get("relation_role") in {"metadata_reference", "article_reference"}:
        return False, "reference_relation"
    target = urlsplit(link.get("exact_normalized_url", ""))
    if re.search(r"/(?:forms/user_sessions|login|signin|sign_in)(?:/|$)", target.path, re.I):
        return False, "account_route_requires_site_root_review"
    context = link.get("surrounding_context", "")
    anchor = link.get("anchor_text", "")
    if re.search(r"\bundated\b", context, re.I) and not re.search(r"\b(?:mayor|mayoral|campaign)\b", context, re.I):
        return False, "undated_personal_site_requires_campaign_review"
    citation = bool(re.match(r"\s*Web\s*site\s*:", context, re.I))
    shaped = _candidate_shaped_target(candidate, link)
    government_website = re.search(
        r"\b(?:city of|county clerk|city government|county government|municipal government|state government|official government)\b[^.!?]{0,80}\bweb\s*site\b",
        context, re.I,
    )
    officeholder_label = re.match(r"\s*(?:supervisor|assemblymember|councilmember|council member)\b", anchor, re.I)
    if not shaped and (government_website or officeholder_label) and not re.search(r"\bcampaign\b|\bfor mayor\b", context, re.I):
        return False, "government_or_officeholder_website_context"
    article_path = bool(re.search(r"/(?:news|articles?|stories|politics|awards|auditor|press-releases?)(?:/|$)|/(?:19|20)\d{2}/\d{1,2}/", target.path, re.I))
    if article_path and not shaped:
        return False, "article_or_reference_target_requires_ownership_review"
    if re.search(r"/(?:tag|category)/", target.path, re.I) and not shaped:
        return False, "blog_index_reference"
    if not shaped and re.search(r"\bparty\b.{0,80}\bwebsite\b", context, re.I) and re.search(r"\bother candidates\b", context, re.I):
        return False, "party_website_not_candidate_website"
    # A citation's publication-type label is not a campaign website field.
    if citation:
        branding = bool(re.search(r"\bfor\s+(?:[\w'-]+\s+){0,3}mayor\b", anchor, re.I))
        if (shaped or branding) and target.path in {"", "/", "/index.html"}:
            return True, "candidate_campaign_site_citation"
        return False, "bibliographic_website_reference"
    candidate_named_in_anchor = any(
        " " + alias + " " in " " + _norm(anchor) + " " for alias in _aliases(candidate)
    )
    if not shaped and WEBSITE_RE.search(anchor) and not candidate_named_in_anchor:
        # A named publisher's website is not the candidate's website.
        owner_label = _norm(WEBSITE_RE.sub(" ", anchor))
        for alias in sorted(_aliases(candidate), key=len, reverse=True):
            owner_label = re.sub(r"\b" + re.escape(alias) + r"\b", " ", owner_label)
        owner_label = re.sub(r"\b(?:visit|view|go|to|the|official|campaign|personal|his|her|their|s)\b", " ", owner_label)
        if owner_label.strip():
            return False, "named_third_party_website_label"
    if _website_link_signal(link):
        return True, "explicit_candidate_website_context"
    if shaped:
        return True, "candidate_owned_target_shape"
    return False, "target_ownership_unresolved"


def eligible_site_claim(claim: dict) -> bool:
    """Use the same ownership requirement for exports and summaries."""
    return (
        claim.get("attribution_status") == "identity_supported"
        and claim.get("url_type") in SITE_TYPES
        and str(claim.get("candidate_site_ownership_supported", "")).lower() in {"true", "1", "yes"}
        and bool(claim.get("candidate_site_ownership_reason"))
        and claim.get("temporal_status") != "date_conflict_review"
        and str(claim.get("wrong_office_flag", "")).lower() not in {"true", "1", "yes"}
    )


def _website_link_signal(link: dict) -> bool:
    role = link.get("relation_role", "")
    if role in {"metadata_reference", "article_reference"}:
        return False
    if role in {"explicit_website_field", "person_url"}:
        return True
    anchor = link.get("anchor_text", "")
    if re.search(r"design|powered by|developed by|privacy|terms of", anchor, re.I):
        return False
    def affirmative(value: str) -> bool:
        return bool(WEBSITE_RE.search(value)) and not re.search(r"\b(?:no|without|does not have|not yet|currently no)\s+(?:\w+\s+){0,2}(?:web\s*site|home\s*page)\b", value, re.I)

    if affirmative(anchor):
        return True
    before, after = link.get("preceding_text", "")[-100:], link.get("following_text", "")[:80]
    previous_urls = list(URL_RE.finditer(before))
    if previous_urls:
        before = before[previous_urls[-1].end():]
    if affirmative(before):
        return True
    if re.match(r"^\s*[,(:-]?\s*(?:is\s+)?(?:(?:his|her|their|the|official|campaign|personal)\s+){0,2}(?:web\s*site|home\s*page)\b", after, re.I) and affirmative(after):
        return True
    # Table/field and PDF contexts lack anchor offsets. Keep this check local.
    context = link.get("surrounding_context", "")
    return len(context) <= 220 and affirmative(context)


def _local_owner_match(link: dict, candidate: dict) -> bool:
    """Require the candidate's name next to this link, including unknown people."""
    if link.get("relation_role") in {"person_url", "explicit_website_field"}:
        owner = link.get("structured_owner_name", "")
        if owner:
            return _name_only_anchor(owner, candidate)
    heading = link.get("heading_context", "")
    before = link.get("preceding_text", "")
    context = link.get("surrounding_context", "")
    if not before:
        for marker in (link.get("raw_url", ""), link.get("anchor_text", ""), link.get("exact_normalized_url", "")):
            if marker and context.count(marker) == 1:
                before = context.split(marker, 1)[0]
                break
    previous_urls = list(URL_RE.finditer(before))
    if previous_urls:
        before = before[previous_urls[-1].end():]
    if heading and _name_only_anchor(heading, candidate):
        other_names = re.findall(r"\b[A-Z][a-z][A-Za-z'-]*\s+[A-Z][a-z][A-Za-z'-]*\b", before)
        if not any(not _name_only_anchor(name, candidate) for name in other_names):
            return True
    # Short PDF annotations and structured records can lack a visible URL offset.
    if not before and len(context) <= 220:
        before = context
    for alias in _aliases(candidate):
        tokens = alias.split()
        pattern = r"\b" + r"[\W_]+".join(re.escape(token) for token in tokens) + r"\b"
        matches = list(re.finditer(pattern, unicodedata.normalize("NFKD", before), re.I))
        if not matches:
            continue
        tail = before[matches[-1].end():]
        if len(tail) > 140:
            continue
        # Another named person can own the next URL even if absent from our roster.
        labels = r"\b(?:Democrat|Democratic|Republican|Independent|Libertarian|Green|Party|Nonpartisan|Mayor|Candidate|Campaign|Website|Official|Visit|Elect|Vote|For|City|Council)\b"
        without_labels = re.sub(labels, " ", tail)
        if re.search(r"\b[A-Z][a-z][A-Za-z'-]*\s+[A-Z][a-z][A-Za-z'-]*\b", without_labels):
            continue
        return True
    return False


def attribute_links(parsed: dict, candidates: list[dict], source_url: str, source_title: str = "") -> list[dict]:
    """Attribute local identity evidence and retain unresolved leads for review."""
    indexed = [(candidate, _aliases(candidate)) for candidate in candidates]
    all_titles = parsed.get("title", "") + " " + source_title
    page_named = _matches(all_titles, indexed)
    claims = []
    for link in parsed.get("links", []):
        if link.get("url_type") == "non_page_resource":
            continue
        anchor = link.get("anchor_text", "")
        context = link.get("surrounding_context", "")
        heading = link.get("heading_context", "")
        named_anchor, named_context, named_heading = _matches(anchor, indexed), _matches(context, indexed), _matches(heading, indexed)
        website_signal = _website_link_signal(link)
        metadata = link.get("relation_role") == "metadata_reference"
        selected, status, reason = [], "match_unresolved", ""
        if link.get("is_boilerplate"):
            # A name-shaped footer link remains visible, but never gains support.
            if named_anchor and website_signal:
                selected, reason = named_anchor, "boilerplate_link_requires_review"
            else:
                continue
        elif len(named_anchor) == 1:
            selected = named_anchor
            if website_signal or (_name_only_anchor(anchor, selected[0]) and _candidate_shaped_target(selected[0], link)):
                status, reason = "identity_supported", "explicit_website_label_or_name_only_anchor_to_candidate_domain"
            else:
                status, reason = "reference_lead", "candidate_name_in_reference_anchor_without_website_ownership_signal"
        elif len(named_context) == 1:
            selected, reason = named_context, "candidate_named_in_local_context"
            local_owner = _local_owner_match(link, selected[0])
            named_campaign_domain = _candidate_shaped_target(selected[0], link) and bool(CAMPAIGN_RE.search(all_titles + " " + context))
            if local_owner and (website_signal or named_campaign_domain) and not metadata:
                status = "identity_supported"
            elif link.get("url_type") in {"third_party_profile", "source_internal_page", "source_reference", "officeholder_or_government_profile", "social_profile"} or link.get("relation_role") == "article_reference":
                status, reason = "reference_lead", "candidate_named_in_source_reference"
        elif len(named_context) > 1:
            preceding = _norm(link.get("preceding_text", ""))
            positions = []
            for candidate, aliases in indexed:
                hits = [preceding.rfind(alias) for alias in aliases if " " + alias + " " in " " + preceding + " "]
                if hits:
                    positions.append((max(hits), candidate))
            positions.sort(key=lambda item: item[0], reverse=True)
            # A directly preceding name wins only within a short local clause.
            distinct_clause = len(positions) == 1 or (len(positions) > 1 and bool(WEBSITE_RE.search(preceding[positions[1][0]:positions[0][0]])))
            if positions and website_signal and len(preceding) - positions[0][0] < 150 and distinct_clause and _local_owner_match(link, positions[0][1]):
                selected, status, reason = [positions[0][1]], "identity_supported", "nearest_preceding_name_in_website_clause"
            else:
                selected, reason = named_context, "multiple_candidates_in_local_context"
        elif len(named_heading) == 1 and website_signal and _local_owner_match(link, named_heading[0]):
            selected, status, reason = named_heading, "identity_supported", "website_link_under_candidate_heading"
        elif len(page_named) == 1 and WEBSITE_RE.search(anchor):
            selected, reason = page_named, "candidate_named_in_page_title_only"
        else:
            shaped = [candidate for candidate, _ in indexed if _candidate_shaped_target(candidate, link)]
            if shaped:
                selected, status, reason = shaped, "domain_shape_only", "domain_spelling_only_requires_review"
        if not selected:
            continue
        for candidate in selected:
            claim_link = dict(link)
            claim_status, claim_reason = status, reason
            ownership, ownership_reason = ownership_support(candidate, claim_link)
            if claim_status == "identity_supported" and not ownership:
                claim_status, claim_reason = "reference_lead", ownership_reason
            if metadata:
                claim_status, claim_reason = "reference_lead", "metadata_relation_does_not_support_candidate_website"
            if claim_link.get("url_type") not in SITE_TYPES and claim_status == "identity_supported":
                claim_status, claim_reason = "reference_lead", "candidate_link_on_social_government_civic_or_source_domain"
            # A news headline can identify a candidate without identifying their site.
            if claim_status == "reference_lead" and claim_link.get("url_type") in SITE_TYPES:
                claim_link["url_type"] = "source_reference"
            temporal = _temporal(candidate, context + " " + heading, source_url, all_titles, parsed.get("source_publication_date", ""))
            other_office = _other_office_campaign(link.get("anchor_text", "")) or _other_office_campaign(all_titles)
            target_office = re.search(r"(?:for|4)(?:governor|congress|senate|president|council)", urlsplit(link.get("exact_normalized_url", "")).hostname or "", re.I)
            if target_office:
                other_office = "target_domain_" + target_office.group().lower()
            identity = {key: value for key, value in candidate.items() if key in CLAIM_CANDIDATE_FIELDS}
            claims.append({**identity, **claim_link, "candidate_year_id": candidate.get("candidate_year_id", ""),
                           "source_url": source_url, "source_title": source_title or parsed.get("title", ""),
                           "attribution_status": claim_status, "attribution_reason": claim_reason,
                           "attribution_version": ATTRIBUTION_VERSION,
                           "candidate_site_ownership_supported": ownership,
                           "candidate_site_ownership_reason": ownership_reason,
                           "temporal_status": temporal, "evidence_grade": "D", "review_status": "pending",
                           "source_publication_date": parsed.get("source_publication_date", ""),
                           "wrong_office_flag": bool(other_office), "other_office_evidence": other_office,
                           "acceptance": "pending", "strict_coverage_eligible": False,
                           "human_accepted": False, "sampled_audit_accepted": False,
                           "archive_capture_available": False, "archive_capture_within_headline_window": False,
                           "downloaded_content_available": False, "historical_url_identified": False})
    return claims
