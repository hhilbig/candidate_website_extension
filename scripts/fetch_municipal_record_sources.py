#!/usr/bin/env python3
"""Fetch a bounded set of official municipal election-record files.

The crawler starts from the explicit official-source inventory. It follows only
same-host links whose URL or anchor context names a target election year or a
target candidate. Every request and saved file is recorded; ambient files are
never treated as inputs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import re
import subprocess
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import pandas as pd
from bs4 import BeautifulSoup


PARSER_VERSION = "official_municipal_source_fetch_v1"
USER_AGENT = "candidate-website-extension/1.0 (academic research; public records)"
TRANSIENT_HTTP = {408, 425, 429, 500, 502, 503, 504}
SAVED_EXTENSIONS = {
    ".csv", ".htm", ".html", ".json", ".pdf", ".tsv", ".xls", ".xlsx",
}
CONTENT_TYPE_EXTENSIONS = {
    "application/json": ".json",
    "application/pdf": ".pdf",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "text/csv": ".csv",
    "text/html": ".html",
    "text/tab-separated-values": ".tsv",
}
LINK_TERMS = re.compile(
    r"(?i)candidate|campaign|election|ballot|voter|filing|finance|report|result|"
    r"contact|nomination|declaration|mayor"
)


@dataclass(frozen=True)
class Response:
    url: str
    status: int
    content_type: str
    body: bytes


def canonical_url(value: str) -> str:
    parsed = urlsplit(str(value).strip())
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.hostname:
        return ""
    host = parsed.hostname.casefold()
    port = f":{parsed.port}" if parsed.port else ""
    return urlunsplit((parsed.scheme.casefold(), host + port, parsed.path or "/", parsed.query, ""))


def default_fetcher(url: str, timeout: int = 30, attempts: int = 3) -> Response:
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=timeout) as response:
                content_type = response.headers.get_content_type().casefold()
                return Response(
                    canonical_url(response.geturl()), int(response.status),
                    content_type, response.read(),
                )
        except HTTPError as exc:
            error = exc
            if exc.code not in TRANSIENT_HTTP:
                raise
        except (TimeoutError, URLError) as exc:
            error = exc
        if attempt + 1 < attempts:
            time.sleep(2 ** attempt)
    assert error is not None
    raise error


def curl_fetcher(url: str, timeout: int = 30) -> Response:
    """Fetch with curl when a portal blocks Python's HTTP fingerprint."""
    with tempfile.NamedTemporaryFile() as body_file:
        result = subprocess.run(
            [
                "curl", "-L", "--max-time", str(timeout), "-sS",
                "-o", body_file.name,
                "-w", "%{http_code}\n%{content_type}\n%{url_effective}",
                url,
            ],
            capture_output=True, text=True, check=False,
        )
        metadata = result.stdout.splitlines()
        if result.returncode != 0 or len(metadata) < 3:
            raise URLError(result.stderr.strip() or f"curl exited {result.returncode}")
        status = int(metadata[0])
        content_type = metadata[1].split(";", 1)[0].strip().casefold()
        final_url = canonical_url("\n".join(metadata[2:]))
        if status >= 400:
            raise HTTPError(final_url or url, status, "curl HTTP error", None, None)
        return Response(final_url or canonical_url(url), status, content_type, Path(body_file.name).read_bytes())


def resilient_fetcher(url: str, timeout: int = 30, attempts: int = 3) -> Response:
    try:
        return default_fetcher(url, timeout=timeout, attempts=attempts)
    except (HTTPError, TimeoutError, URLError):
        return curl_fetcher(url, timeout=timeout)


def _extension(url: str, content_type: str) -> str:
    suffix = Path(urlsplit(url).path).suffix.casefold()
    if suffix in SAVED_EXTENSIONS:
        return suffix
    return CONTENT_TYPE_EXTENSIONS.get(content_type, mimetypes.guess_extension(content_type) or ".bin")


def _parse_target_years(value: object) -> set[int]:
    if pd.isna(value) or not str(value).strip():
        return set()
    years: set[int] = set()
    for token in re.split(r"[|,;\s]+", str(value).strip()):
        if not token:
            continue
        if re.fullmatch(r"\d{4}", token):
            years.add(int(token))
            continue
        match = re.fullmatch(r"(\d{4})-(\d{4})", token)
        if match:
            start, end = map(int, match.groups())
            if end < start:
                raise ValueError(f"Invalid target year range: {token}")
            years.update(range(start, end + 1))
            continue
        raise ValueError(f"Invalid target year token: {token}")
    return years


def _source_years(
    universe: pd.DataFrame,
    city: str,
    state: str,
    explicit_years: object,
    start_year: int,
    end_year: int,
) -> set[int]:
    requested = _parse_target_years(explicit_years)
    scoped = universe[
        universe["geo_name"].astype(str).str.casefold().eq(city.casefold())
        & universe["state_abb"].astype(str).str.casefold().eq(state.casefold())
        & universe["year"].between(start_year, end_year)
        & universe["in_discovery_scope"].astype("boolean").fillna(False)
    ]
    available = set(scoped["year"].astype(int))
    if requested:
        unavailable = requested - available
        if unavailable:
            raise ValueError(
                f"Target years absent from the scoped universe for {city}, {state}: {sorted(unavailable)}"
            )
        return requested
    return available


def _candidate_tokens(
    universe: pd.DataFrame, city: str, state: str, years: set[int],
) -> set[str]:
    scoped = universe[
        universe["geo_name"].astype(str).str.casefold().eq(city.casefold())
        & universe["state_abb"].astype(str).str.casefold().eq(state.casefold())
        & universe["year"].astype(int).isin(years)
        & universe["in_discovery_scope"].astype("boolean").fillna(False)
    ]
    tokens: set[str] = set()
    for name in scoped["full_name"].fillna("").astype(str):
        words = re.findall(r"[a-z]{3,}", name.casefold())
        if len(words) >= 2:
            tokens.add(f"{words[0]} {words[-1]}")
    return tokens


def target_link(
    url: str, context: str, root_host: str, years: set[str], candidates: set[str],
) -> bool:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").casefold()
    if host != root_host or parsed.scheme.casefold() not in {"http", "https"}:
        return False
    text = f"{unquote(url)} {context}".casefold()
    has_year = any(year in text for year in years)
    has_candidate = any(re.search(rf"\b{re.escape(token)}\b", text) for token in candidates)
    if not (has_year or has_candidate):
        return False
    return bool(LINK_TERMS.search(text) or Path(parsed.path).suffix.casefold() in SAVED_EXTENSIONS)


def html_links(body: bytes, base_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(body, "html.parser")
    links: list[tuple[str, str]] = []
    for anchor in soup.find_all("a", href=True):
        url = canonical_url(urljoin(base_url, str(anchor["href"])))
        if url:
            links.append((url, " ".join(anchor.get_text(" ", strip=True).split())))
    return links


def crawl_inventory(
    inventory: pd.DataFrame,
    universe: pd.DataFrame,
    output_dir: Path,
    *,
    max_files_per_source: int = 12,
    max_depth: int = 2,
    start_year: int = 2000,
    end_year: int = 2007,
    fetcher: Callable[[str], Response] = default_fetcher,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"source_id", "city", "state", "official_url"}
    missing = required - set(inventory.columns)
    if missing:
        raise ValueError(f"Source inventory missing columns: {sorted(missing)}")
    if inventory["source_id"].isna().any():
        raise ValueError("Source inventory must contain nonmissing source IDs")
    attempt_keys = inventory[["source_id", "city", "state"]].astype(str)
    if attempt_keys.duplicated().any():
        raise ValueError("Source inventory must contain unique source-city-state targets")
    output_dir.mkdir(parents=True, exist_ok=True)
    download_rows: list[dict] = []
    attempt_rows: list[dict] = []

    for source in inventory.sort_values("source_id").to_dict("records"):
        source_id = str(source["source_id"])
        city, state = str(source["city"]), str(source["state"])
        source_target_id = f"{source_id}__{city.casefold().replace(' ', '_')}__{state.casefold()}"
        root_url = canonical_url(str(source["official_url"]))
        root_host = (urlsplit(root_url).hostname or "").casefold()
        numeric_years = _source_years(
            universe, city, state, source.get("target_years"), start_year, end_year,
        )
        years = {str(year) for year in numeric_years}
        candidates = _candidate_tokens(universe, city, state, numeric_years)
        queue = deque([(root_url, 0, "inventory_root")])
        seen: set[str] = set()
        saved = 0
        errors = 0
        unavailable = 0

        while queue and saved < max_files_per_source:
            requested_url, depth, discovery_context = queue.popleft()
            if not requested_url or requested_url in seen:
                continue
            seen.add(requested_url)
            status = "downloaded"
            error_text = ""
            response: Response | None = None
            try:
                response = fetcher(requested_url)
            except HTTPError as exc:
                status = "source_unavailable" if exc.code in {401, 403, 404, 410} else "request_error"
                error_text = f"HTTP {exc.code}: {exc.reason}"
            except Exception as exc:  # the ledger preserves the exact failure
                status = "request_error"
                error_text = f"{type(exc).__name__}: {exc}"

            local_file = ""
            sha256 = ""
            byte_count = 0
            final_url = requested_url
            content_type = ""
            http_status = None
            if response is not None:
                final_url = response.url
                content_type = response.content_type
                http_status = response.status
                digest = hashlib.sha256(response.body).hexdigest()
                extension = _extension(final_url, content_type)
                local_path = output_dir / source_target_id / f"{digest[:20]}{extension}"
                local_path.parent.mkdir(parents=True, exist_ok=True)
                if not local_path.exists():
                    local_path.write_bytes(response.body)
                local_file = local_path.as_posix()
                sha256 = digest
                byte_count = len(response.body)
                saved += 1
                if content_type == "text/html" and depth < max_depth:
                    for link, context in html_links(response.body, final_url):
                        if link not in seen and target_link(
                            link, context, root_host, years, candidates,
                        ):
                            queue.append((link, depth + 1, context[:500]))
            elif status == "request_error":
                errors += 1
            else:
                unavailable += 1

            download_rows.append({
                "source_id": source_id,
                "source_target_id": source_target_id,
                "city": city,
                "state": state,
                "target_years": "|".join(sorted(years)),
                "requested_url": requested_url,
                "final_url": final_url,
                "crawl_depth": depth,
                "discovery_context": discovery_context,
                "http_status": http_status,
                "content_type": content_type,
                "download_status": status,
                "local_file": local_file,
                "bytes": byte_count,
                "sha256": sha256,
                "error": error_text,
                "retrieved_on": date.today().isoformat(),
                "parser_version": PARSER_VERSION,
            })

        if saved:
            terminal = "found"
        elif errors:
            terminal = "request_error"
        else:
            terminal = "source_unavailable"
        attempt_rows.append({
            "source_id": source_id,
            "source_target_id": source_target_id,
            "city": city,
            "state": state,
            "target_years": "|".join(sorted(years)),
            "urls_requested": len(seen),
            "files_downloaded": saved,
            "request_errors": errors,
            "unavailable_requests": unavailable,
            "source_status": terminal,
            "attempted_on": date.today().isoformat(),
            "parser_version": PARSER_VERSION,
        })

    downloads = pd.DataFrame(download_rows).sort_values(
        ["source_target_id", "crawl_depth", "requested_url"],
    ).reset_index(drop=True)
    attempts = pd.DataFrame(attempt_rows).sort_values("source_target_id").reset_index(drop=True)
    return downloads, attempts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-files-per-source", type=int, default=12)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year", type=int, default=2007)
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--attempts", type=int, default=2)
    args = parser.parse_args()
    output = Path(args.output_dir)
    if args.timeout <= 0 or args.attempts <= 0 or args.end_year < args.start_year:
        parser.error("--timeout and --attempts must be positive")
    fetcher = lambda url: resilient_fetcher(
        url, timeout=args.timeout, attempts=args.attempts,
    )
    downloads, attempts = crawl_inventory(
        pd.read_csv(args.inventory, low_memory=False),
        pd.read_csv(args.universe, low_memory=False),
        output / "source_files",
        max_files_per_source=args.max_files_per_source,
        max_depth=args.max_depth,
        start_year=args.start_year,
        end_year=args.end_year,
        fetcher=fetcher,
    )
    downloads.to_csv(output / "download_manifest.csv", index=False)
    attempts.to_csv(output / "source_fetch_attempts.csv", index=False)
    summary = {
        "parser_version": PARSER_VERSION,
        "sources": int(len(attempts)),
        "files_downloaded": int(downloads["download_status"].eq("downloaded").sum()),
        "status_counts": {
            str(key): int(value)
            for key, value in attempts["source_status"].value_counts().sort_index().items()
        },
    }
    (output / "fetch_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
