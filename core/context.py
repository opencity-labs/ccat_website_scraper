"""Scraping session context — shared mutable state for one scraping run."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class ScrapeContext:
    """Holds all mutable state for a single scraping session.

    Thread-safe where needed: ``visited_pages``, ``scraped_pages``,
    ``scraped_contents``, and UI throttling are protected by locks.
    """

    # ── Identity ─────────────────────────────────────────────────────
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    scheduled: bool = False

    # ── Domain / path scoping ────────────────────────────────────────
    root_domains: Set[str] = field(default_factory=set)
    # domain → set of allowed path prefixes (per-domain scoping)
    allowed_paths: Dict[str, Set[str]] = field(default_factory=dict)
    allowed_domains: Set[str] = field(default_factory=set)  # single-page only

    # ── Crawl settings (copied from plugin settings for convenience) ─
    max_depth: int = -1
    max_pages: int = -1
    max_workers: int = 4
    request_delay: float = 0.5
    page_timeout: int = 30
    skip_get_params: bool = False
    ingest_pdf: bool = False
    skip_extensions: List[str] = field(default_factory=list)
    cache_content: bool = True
    follow_robots_txt: bool = True
    obey_crawl_delay: bool = True
    only_sitemap: bool = False
    use_scrapling: bool = False
    user_agent: str = "CheshireCatAI WebsitesScraper/1.0"

    # ── Rate-limiting & anti-detection ───────────────────────────────
    batch_size: int = 50
    batch_pause_seconds: float = 5.0
    adaptive_backoff: bool = True
    backoff_initial_seconds: float = 10.0
    backoff_max_seconds: float = 120.0
    backoff_threshold: int = 3

    # ── Proxy ────────────────────────────────────────────────────────
    proxy_enabled: bool = False
    proxy_list: List[str] = field(default_factory=list)
    proxy_rotate_every: int = 10

    # ── Chunking ─────────────────────────────────────────────────────
    chunk_size: int = 1024
    chunk_overlap: int = 256

    # ── Collected data ───────────────────────────────────────────────
    visited_pages: Set[str] = field(default_factory=set)
    scraped_pages: List[str] = field(default_factory=list)
    failed_pages: List[str] = field(default_factory=list)
    ignored_pages: List[str] = field(default_factory=list)

    # url → (raw_bytes, content_type)
    scraped_contents: Dict[str, Tuple[bytes, str]] = field(default_factory=dict)

    # ── Per-domain crawl delay overrides from robots.txt ─────────────
    domain_crawl_delays: Dict[str, float] = field(default_factory=dict)

    # ── Sitemap data ──────────────────────────────────────────────────
    # All URLs discovered from sitemap.xml (populated before BFS)
    sitemap_urls: Set[str] = field(default_factory=set)
    # url → SitemapEntry (for lastmod pre-filter)
    sitemap_meta: Dict[str, Any] = field(default_factory=dict)

    # ── Locks ────────────────────────────────────────────────────────
    visited_lock: Lock = field(default_factory=Lock)
    scraped_lock: Lock = field(default_factory=Lock)
    contents_lock: Lock = field(default_factory=Lock)
    update_lock: Lock = field(default_factory=Lock)

    # UI update throttle
    last_update_time: float = 0.0

    # ── Custom hook fields ───────────────────────────────────────────
    _custom: Dict[str, Any] = field(default_factory=dict)

    # ── Serialisation for hooks ──────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "session_id": self.session_id,
            "scheduled": self.scheduled,
            "scraped_pages": list(self.scraped_pages),
            "failed_pages": list(self.failed_pages),
            "ignored_pages": list(self.ignored_pages),
            "chunk_size": self.chunk_size,
            "chunk_overlap": self.chunk_overlap,
            "page_timeout": self.page_timeout,
            "request_delay": self.request_delay,
            "user_agent": self.user_agent,
            "skip_extensions": list(self.skip_extensions),
        }
        d.update(self._custom)
        return d

    def update_from_dict(self, d: Dict[str, Any]) -> None:
        _KNOWN = {
            "session_id",
            "scheduled",
            "scraped_pages",
            "failed_pages",
            "ignored_pages",
            "chunk_size",
            "chunk_overlap",
            "page_timeout",
            "request_delay",
            "user_agent",
            "skip_extensions",
        }
        for k in _KNOWN:
            if k in d:
                setattr(self, k, d[k])
        for k, v in d.items():
            if k not in _KNOWN:
                self._custom[k] = v
