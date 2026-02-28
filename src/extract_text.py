"""
HTML-to-text extraction for Wayback Machine snapshots.

Strips Wayback toolbar, extracts visible text, handles frames/iframes.
"""

import logging
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from bs4.element import Comment

logger = logging.getLogger(__name__)

# Wayback Machine markers
WAYBACK_TOOLBAR_END = "<!-- END WAYBACK TOOLBAR INSERT -->"
WAYBACK_FILE_ARCHIVED = "<!--\n     FILE ARCHIVED ON"


def strip_wayback_toolbar(html: str) -> str:
    """Remove Wayback Machine toolbar HTML from archived page."""
    if WAYBACK_TOOLBAR_END in html:
        html = html.split(WAYBACK_TOOLBAR_END)[-1]
    if WAYBACK_FILE_ARCHIVED in html:
        html = html.split(WAYBACK_FILE_ARCHIVED)[0]
    return html.strip()


def is_wayback_page(html: str) -> bool:
    """Check if HTML contains Wayback Machine markers."""
    return WAYBACK_TOOLBAR_END in html or "FILE ARCHIVED ON" in html


def _tag_visible(element) -> bool:
    """Filter for visible text elements (exclude scripts, styles, etc.)."""
    if element.parent.name in ["style", "script", "head", "title", "meta", "[document]"]:
        return False
    if isinstance(element, Comment):
        return False
    return True


def _deduplicate_text_segments(segments: list[str], max_repeats: int = 2) -> list[str]:
    """
    Remove boilerplate text segments (nav menus, footers) that repeat excessively.

    Segments appearing more than max_repeats times are removed entirely.
    Consecutive duplicate segments are also collapsed.
    """
    # Count occurrences
    counts: dict[str, int] = {}
    for seg in segments:
        counts[seg] = counts.get(seg, 0) + 1

    # Remove segments appearing > max_repeats times (only for segments >= 5 chars)
    filtered = [seg for seg in segments
                if len(seg) < 5 or counts[seg] <= max_repeats]

    # Remove consecutive duplicates
    deduped = []
    for seg in filtered:
        if not deduped or seg != deduped[-1]:
            deduped.append(seg)

    return deduped


def extract_visible_text(soup: BeautifulSoup, separator: str = "#+#") -> str:
    """
    Extract visible text from parsed HTML.

    Args:
        soup: Parsed BeautifulSoup object.
        separator: String to join text chunks.

    Returns:
        Concatenated visible text.
    """
    texts = soup.find_all(string=True)
    visible = filter(_tag_visible, texts)
    cleaned = [t.strip() for t in visible if len(t.strip()) > 2]
    cleaned = _deduplicate_text_segments(cleaned)
    return separator.join(cleaned)


def get_subpage_urls(soup: BeautifulSoup, base_url: str,
                     exclude_domains: list[str] | None = None) -> list[str]:
    """
    Find internal links (subpages) within the same domain.

    Handles three link formats found in Wayback pages:
    1. Already-rewritten Wayback URLs (web.archive.org/web/TIMESTAMP/...)
    2. Absolute original-domain URLs (https://example.com/about)
    3. Relative URLs (/about, contact.html)

    Args:
        soup: Parsed page HTML.
        base_url: The snapshot's Wayback URL (e.g., https://web.archive.org/web/20200601/https://pelosi.house.gov/).
        exclude_domains: Domains to skip (social media, etc.).

    Returns:
        Deduplicated list of internal Wayback URLs.
    """
    if exclude_domains is None:
        exclude_domains = ["twitter.com", "facebook.com", "instagram.com", "youtube.com"]

    # Extract the original domain and Wayback prefix from the base URL
    original_domain = _extract_domain(base_url)
    if not original_domain:
        return []

    domain_bare = original_domain.replace("www.", "")

    # Extract Wayback prefix and original URL for resolving relative links
    # base_url: https://web.archive.org/web/TIMESTAMP/http://site.com/path/page.html
    parts = base_url.split("/")
    wayback_prefix = None
    original_url = None
    if len(parts) >= 6 and "web.archive.org" in base_url:
        wayback_prefix = "/".join(parts[:5])  # https://web.archive.org/web/TIMESTAMP
        original_url = "/".join(parts[5:])    # http://site.com/path/page.html

    links = set()
    for tag in soup.find_all(["a", "area"], href=True):
        href = tag["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        if any(excl in href for excl in exclude_domains):
            continue

        # Case 1: Already a Wayback URL
        if "web.archive.org" in href:
            if domain_bare in href:
                links.add(href)
            continue

        # Case 2 & 3: Relative or absolute original-domain URL — resolve to Wayback format
        if wayback_prefix and original_url:
            # Check if it's an absolute URL for a different domain
            if href.startswith("http://") or href.startswith("https://"):
                href_domain = href.split("://")[1].split("/")[0].replace("www.", "")
                if domain_bare not in href_domain:
                    continue  # external link
                # Absolute URL for same domain — prepend Wayback prefix
                resolved = wayback_prefix + "/" + href
            else:
                # Relative URL — resolve against the original URL, then prepend Wayback prefix
                resolved = wayback_prefix + "/" + urljoin(original_url, href)

            links.add(resolved)

    return list(links)


def _extract_domain(wayback_url: str) -> Optional[str]:
    """Extract the original domain from a Wayback Machine URL."""
    # https://web.archive.org/web/20200101/http://example.com/page
    parts = wayback_url.split("/")
    if len(parts) < 6:
        return None
    # Reconstruct original URL portion
    original = "/".join(parts[5:])
    if "://" in original:
        domain = original.split("://")[1].split("/")[0]
    elif ":/" in original:
        domain = original.split(":/")[1].split("/")[0]
    else:
        domain = original.split("/")[0]
    return domain


def extract_frame_content(soup: Optional[BeautifulSoup], base_url: str,
                          separator: str = "#+#",
                          fetch_fn=None,
                          max_depth: int = 3) -> tuple[str, list[str]]:
    """
    Recursively extract content from frames/iframes.

    Args:
        soup: Parsed HTML (may contain frames).
        base_url: Wayback URL of this page.
        separator: Text chunk separator.
        fetch_fn: Callable(url) -> BeautifulSoup for fetching frame URLs.
        max_depth: Maximum recursion depth for nested frames.

    Returns:
        (text_content, list_of_subpage_urls)
    """
    if soup is None or max_depth <= 0:
        return "", []

    frames = soup.find_all("frame") + soup.find_all("iframe")
    text = extract_visible_text(soup, separator)
    subpages = get_subpage_urls(soup, base_url)

    if not frames:
        # No frames — return direct text content
        return text, subpages

    # Frame elements found; recurse into frames and combine with page text
    all_text = ""
    all_subpages = list(subpages)

    for frame in frames:
        src = frame.get("src")
        if not src:
            continue

        frame_url = _resolve_frame_url(src, base_url)
        if fetch_fn is None:
            continue

        frame_soup = fetch_fn(frame_url)
        if frame_soup is None:
            logger.warning(f"Could not fetch frame content: {frame_url}")
            continue

        frame_text, frame_subpages = extract_frame_content(
            frame_soup, frame_url, separator, fetch_fn, max_depth - 1
        )
        all_text += (separator if all_text and frame_text else "") + frame_text
        all_subpages.extend(frame_subpages)

    combined_text = text + all_text
    if frames and not combined_text.strip():
        logger.warning(f"Frame-based page yielded no text: {base_url}")
    return combined_text, list(set(all_subpages))


def _resolve_frame_url(frame_src: str, base_url: str) -> str:
    """Resolve a frame src attribute to a full Wayback URL."""
    if "web.archive.org" in frame_src:
        return "https://" + "//".join(frame_src.split("//")[1:])
    if "web/20" in frame_src:
        return "https://web.archive.org" + ("/" if not frame_src.startswith("/") else "") + frame_src

    # Relative URL: extract Wayback prefix and original URL, then use urljoin
    # base_url format: https://web.archive.org/web/TIMESTAMP/http://site.com/path/page.html
    parts = base_url.split("/")
    if len(parts) >= 6 and "web.archive.org" in base_url:
        # parts[0:5] = ['https:', '', 'web.archive.org', 'web', 'TIMESTAMP']
        wayback_prefix = "/".join(parts[:5])  # https://web.archive.org/web/TIMESTAMP
        original_url = "/".join(parts[5:])    # http://site.com/path/page.html
        resolved = urljoin(original_url, frame_src)
        return wayback_prefix + "/" + resolved

    return frame_src
