"""Sitemap fetcher and parser.

Handles:
* Standard ``<urlset>`` sitemaps
* Sitemap index files (``<sitemapindex>``) -  recursively fetches sub-sitemaps
* ``<changefreq>`` and ``<lastmod>`` per-URL metadata (exposed to the caller
  so the scheduler/optimiser can decide whether to re-scrape)
* Sitemap URLs declared in robots.txt (``Sitemap:`` directive)
"""

from __future__ import annotations

import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

import httpx

from .robots import get_sitemaps_from_robots
from .log_utils import jlog


# ── Data structures ──────────────────────────────────────────────────


@dataclass
class SitemapEntry:
    """One ``<url>`` element from a sitemap."""

    loc: str
    lastmod: Optional[datetime] = None
    changefreq: Optional[str] = None  # always|hourly|daily|weekly|monthly|yearly|never
    priority: Optional[float] = None


_CHANGEFREQ_SECONDS: Dict[str, int] = {
    "always": 0,
    "hourly": 3600,
    "daily": 86400,
    "weekly": 604800,
    "monthly": 2592000,
    "yearly": 31536000,
}


def changefreq_to_seconds(freq: Optional[str]) -> Optional[int]:
    """Convert a sitemap ``changefreq`` value to seconds, or ``None``."""
    if not freq:
        return None
    return _CHANGEFREQ_SECONDS.get(freq.lower())


# ── Fetching ─────────────────────────────────────────────────────────


def _fetch_xml(url: str, user_agent: str, max_retries: int = 4) -> Optional[str]:
    """GET *url* with exponential-backoff retry for 429s."""
    headers = {"User-Agent": user_agent}
    wait = 2
    for attempt in range(1, max_retries + 1):
        try:
            resp = httpx.get(url, headers=headers, timeout=60, follow_redirects=True)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 429:
                jlog(
                    "warning",
                    "sitemap_rate_limited",
                    url=url,
                    wait=wait,
                    attempt=attempt,
                )
                time.sleep(wait)
                wait *= 3
                continue
            jlog("warning", "sitemap_http_error", url=url, status=resp.status_code)
            return None
        except Exception as exc:
            jlog("warning", "sitemap_fetch_error", url=url, error=str(exc))
            if attempt < max_retries:
                time.sleep(wait)
                wait *= 2
    return None


def _parse_datetime(text: Optional[str]) -> Optional[datetime]:
    """Best-effort ISO-8601 date parsing (``YYYY-MM-DD`` or full timestamp)."""
    if not text:
        return None
    text = text.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _strip_ns(tag: str) -> str:
    """Remove XML namespace prefix from *tag*."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_urlset(root: ET.Element) -> List[SitemapEntry]:
    """Parse a ``<urlset>`` element into a list of :class:`SitemapEntry`."""
    entries: List[SitemapEntry] = []
    for url_elem in root:
        if not _strip_ns(url_elem.tag) == "url":
            continue
        loc = lastmod_str = changefreq = priority_str = None
        for child in url_elem:
            tag = _strip_ns(child.tag)
            if tag == "loc":
                loc = child.text.strip() if child.text else None
            elif tag == "lastmod":
                lastmod_str = child.text
            elif tag == "changefreq":
                changefreq = child.text.strip() if child.text else None
            elif tag == "priority":
                priority_str = child.text
        if loc and loc.startswith("http"):
            entries.append(
                SitemapEntry(
                    loc=loc,
                    lastmod=_parse_datetime(lastmod_str),
                    changefreq=changefreq,
                    priority=float(priority_str) if priority_str else None,
                )
            )
    return entries


def _parse_sitemap_xml(
    text: str, user_agent: str, visited: Optional[Set[str]] = None
) -> List[SitemapEntry]:
    """Parse a sitemap (or sitemap index) and return all entries."""
    if visited is None:
        visited = set()
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        # Regex fallback
        locs = re.findall(r"<loc>\s*(https?://[^<]+?)\s*</loc>", text)
        return [SitemapEntry(loc=u) for u in locs]

    root_tag = _strip_ns(root.tag)

    if root_tag == "urlset":
        return _parse_urlset(root)

    if root_tag == "sitemapindex":
        entries: List[SitemapEntry] = []
        for child in root:
            if _strip_ns(child.tag) != "sitemap":
                continue
            loc_elem = None
            for sub in child:
                if _strip_ns(sub.tag) == "loc":
                    loc_elem = sub
            if loc_elem is None or not loc_elem.text:
                continue
            sub_url = loc_elem.text.strip()
            if sub_url in visited:
                continue
            visited.add(sub_url)
            sub_text = _fetch_xml(sub_url, user_agent)
            if sub_text:
                entries.extend(_parse_sitemap_xml(sub_text, user_agent, visited))
        return entries

    # Unknown root -  try urlset parse anyway
    return _parse_urlset(root)


# ── Public API ───────────────────────────────────────────────────────


def fetch_sitemap_entries(root_url: str, user_agent: str) -> List[SitemapEntry]:
    """Discover and parse sitemaps for *root_url*.

    1. Check robots.txt for ``Sitemap:`` directives.
    2. If none found, try ``<root_url>/sitemap.xml``.
    3. Parse (possibly recursive sitemap index).

    Returns a list of :class:`SitemapEntry` objects.
    """
    parsed = urlparse(root_url)
    domain = parsed.netloc
    if domain.startswith("www."):
        domain = domain[4:]

    sitemap_urls: List[str] = get_sitemaps_from_robots(domain, user_agent)

    base = f"{parsed.scheme}://{parsed.netloc}"

    if not sitemap_urls:
        # Fallback: conventional location
        sitemap_urls = [f"{base}/sitemap.xml"]
    else:
        # Resolve relative URLs (e.g. /sitemap.xml → https://example.com/sitemap.xml)
        resolved: List[str] = []
        for u in sitemap_urls:
            if u.startswith("http"):
                resolved.append(u)
            elif u.startswith("/"):
                resolved.append(f"{base}{u}")
            else:
                resolved.append(f"{base}/{u}")
        sitemap_urls = resolved

    all_entries: List[SitemapEntry] = []
    visited: Set[str] = set()

    for sm_url in sitemap_urls:
        if sm_url in visited:
            continue
        visited.add(sm_url)
        text = _fetch_xml(sm_url, user_agent)
        if text:
            all_entries.extend(_parse_sitemap_xml(text, user_agent, visited))

    jlog("info", "sitemap_parsed", root_url=root_url, entries=len(all_entries))
    return all_entries
