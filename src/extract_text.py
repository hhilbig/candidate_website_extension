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
    return separator.join(cleaned)


def get_subpage_urls(soup: BeautifulSoup, base_url: str,
                     exclude_domains: list[str] | None = None) -> list[str]:
    """
    Find internal links (subpages) within the same domain.

    Args:
        soup: Parsed page HTML.
        base_url: The snapshot's home URL (Wayback format).
        exclude_domains: Domains to skip (social media, etc.).

    Returns:
        Deduplicated list of internal Wayback URLs.
    """
    if exclude_domains is None:
        exclude_domains = ["twitter.com", "facebook.com", "instagram.com", "youtube.com"]

    # Extract the original domain from the Wayback URL
    # Format: https://web.archive.org/web/TIMESTAMP/http://example.com/page
    original_domain = _extract_domain(base_url)
    if not original_domain:
        return []

    links = set()
    for tag in soup.find_all(["a", "area"], href=True):
        href = tag["href"]
        # Only keep links within the same domain that go through Wayback
        if "web.archive.org" not in href:
            continue
        if original_domain.replace("www.", "") not in href:
            continue
        if any(excl in href for excl in exclude_domains):
            continue
        links.add(href)

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
                          fetch_fn=None) -> tuple[str, list[str]]:
    """
    Recursively extract content from frames/iframes.

    Args:
        soup: Parsed HTML (may contain frames).
        base_url: Wayback URL of this page.
        separator: Text chunk separator.
        fetch_fn: Callable(url) -> BeautifulSoup for fetching frame URLs.

    Returns:
        (text_content, list_of_subpage_urls)
    """
    if soup is None:
        return "", []

    frames = soup.find_all("frame") + soup.find_all("iframe")
    text = extract_visible_text(soup, separator)
    subpages = get_subpage_urls(soup, base_url)

    if not frames or text:
        # No frames, or page has content outside frames
        return text, subpages

    # Page is frame-based with no direct content; recurse into frames
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
            continue

        frame_text, frame_subpages = extract_frame_content(
            frame_soup, frame_url, separator, fetch_fn
        )
        all_text += (separator if all_text and frame_text else "") + frame_text
        all_subpages.extend(frame_subpages)

    return text + all_text, list(set(all_subpages))


def _resolve_frame_url(frame_src: str, base_url: str) -> str:
    """Resolve a frame src attribute to a full Wayback URL."""
    if "web.archive.org" in frame_src:
        return "https://" + "//".join(frame_src.split("//")[1:])
    if "web/20" in frame_src:
        return "https://web.archive.org" + ("/" if not frame_src.startswith("/") else "") + frame_src

    # Relative URL: resolve against base
    domain = _extract_domain(base_url)
    if domain:
        base_prefix = base_url.split(domain)[0] + domain
        return base_prefix + ("/" if not frame_src.startswith("/") else "") + frame_src

    return frame_src
