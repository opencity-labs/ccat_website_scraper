"""Enhanced robots.txt parser.

Beyond standard Allow / Disallow, this module also extracts:
* ``Crawl-delay`` -  respected as a per-domain request delay.
* Per-path refresh hints found in non-standard ``# refresh: <seconds>``
  comment lines (used by some site owners to indicate how often a section
  should be re-scraped).  These are stored in ``RobotsInfo.refresh_rules``
  as ``{path_prefix: interval_seconds}``.
* ``Sitemap`` directives -  returned so the sitemap module can use them.
"""

from __future__ import annotations

import re
import requests
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from urllib.robotparser import RobotFileParser

from .url_utils import normalize_domain
from .log_utils import jlog


@dataclass
class RobotsInfo:
    """Parsed robots.txt data for a single domain."""

    parser: Optional[RobotFileParser] = None
    crawl_delay: Optional[float] = None  # seconds
    sitemaps: List[str] = field(default_factory=list)
    # path_prefix → re-scrape interval in seconds  (non-standard hint)
    refresh_rules: Dict[str, int] = field(default_factory=dict)


# Module-level cache: domain → RobotsInfo
_robots_cache: Dict[str, RobotsInfo] = {}


def _get_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    return s


def _parse_extra_directives(
    text: str, user_agent: str
) -> tuple[Optional[float], List[str], Dict[str, int]]:
    """Extract Crawl-delay, Sitemap, and refresh hints from raw robots.txt text.

    Returns ``(crawl_delay, sitemaps, refresh_rules)``.
    """
    crawl_delay: Optional[float] = None
    sitemaps: List[str] = []
    refresh_rules: Dict[str, int] = {}

    # Track which user-agent block we're in
    in_matching_block = False
    in_wildcard_block = False
    wildcard_crawl_delay: Optional[float] = None

    for line in text.splitlines():
        stripped = line.strip()

        # ── Sitemap (global, outside any UA block) ───────────────
        if stripped.lower().startswith("sitemap:"):
            url = stripped.split(":", 1)[1].strip()
            if url:
                sitemaps.append(url)
            continue

        # ── User-agent line → determines which block we're in ────
        if stripped.lower().startswith("user-agent:"):
            ua_value = stripped.split(":", 1)[1].strip().lower()
            in_matching_block = user_agent.lower() in ua_value or ua_value == "*"
            if ua_value == "*":
                in_wildcard_block = True
            else:
                in_wildcard_block = False
            continue

        # ── Crawl-delay ──────────────────────────────────────────
        if stripped.lower().startswith("crawl-delay:"):
            try:
                delay_val = float(stripped.split(":", 1)[1].strip())
                if in_matching_block and not in_wildcard_block:
                    crawl_delay = delay_val
                elif in_wildcard_block and crawl_delay is None:
                    wildcard_crawl_delay = delay_val
            except ValueError:
                pass
            continue

        # ── Non-standard refresh hint ────────────────────────────
        # Format:  # refresh: /path <seconds>
        # Example: # refresh: /news 3600
        m = re.match(r"#\s*refresh:\s*(\S+)\s+(\d+)", stripped)
        if m:
            refresh_rules[m.group(1)] = int(m.group(2))
            continue

    if crawl_delay is None:
        crawl_delay = wildcard_crawl_delay

    return crawl_delay, sitemaps, refresh_rules


def load_robots(domain: str, user_agent: str) -> RobotsInfo:
    """Fetch and parse robots.txt for *domain*.  Results are cached."""
    if domain in _robots_cache:
        return _robots_cache[domain]

    info = RobotsInfo()
    session = _get_session(user_agent)

    for proto in ("https", "http"):
        robots_url = f"{proto}://{domain}/robots.txt"
        try:
            resp = session.get(robots_url, timeout=10)
            if resp.status_code == 200:
                rp = RobotFileParser()
                rp.set_url(robots_url)
                rp.parse(resp.text.splitlines())
                info.parser = rp

                delay, sitemaps, refresh = _parse_extra_directives(
                    resp.text, user_agent
                )
                info.crawl_delay = delay
                info.sitemaps = sitemaps
                info.refresh_rules = refresh

                jlog(
                    "info",
                    "robots_loaded",
                    domain=domain,
                    crawl_delay=delay,
                    sitemaps=len(sitemaps),
                    refresh_rules=len(refresh),
                )
                break
        except Exception as exc:
            jlog("warning", "robots_fetch_failed", url=robots_url, error=str(exc))

    _robots_cache[domain] = info
    return info


def is_url_allowed(
    domain: str, url: str, user_agent: str, follow_robots: bool = True
) -> bool:
    """Check whether *url* is allowed by robots.txt.  Always ``True`` when
    ``follow_robots`` is ``False`` or no robots.txt was found."""
    if not follow_robots:
        return True
    info = load_robots(domain, user_agent)
    if info.parser is None:
        return True
    return info.parser.can_fetch(user_agent, url)


def get_crawl_delay(domain: str, user_agent: str) -> Optional[float]:
    """Return the Crawl-delay for *domain* (or ``None``)."""
    info = load_robots(domain, user_agent)
    return info.crawl_delay


def get_sitemaps_from_robots(domain: str, user_agent: str) -> List[str]:
    """Return Sitemap URLs declared in robots.txt for *domain*."""
    info = load_robots(domain, user_agent)
    return info.sitemaps


def get_refresh_interval(domain: str, path: str, user_agent: str) -> Optional[int]:
    """Return the refresh interval (seconds) for *path* under *domain*,
    or ``None`` if no hint exists."""
    info = load_robots(domain, user_agent)
    # Longest-prefix match
    best: Optional[int] = None
    best_len = 0
    for prefix, interval in info.refresh_rules.items():
        if path.startswith(prefix) and len(prefix) > best_len:
            best = interval
            best_len = len(prefix)
    return best


def clear_cache() -> None:
    """Clear the module-level robots cache (useful on settings reset)."""
    _robots_cache.clear()
