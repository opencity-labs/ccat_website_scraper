"""Memory manager — deduplication, ingestion, stale-content cleanup.

This module replaces the three-plugin orchestration (Dietician + middleman +
ScrapyCat ingestion loop) with a single, self-contained flow:

1. **Before ingestion** — compare content hash to decide: skip / partial-update / full ingest.
2. **After ingestion** — retry transient failures, remove stale memories.
"""

from __future__ import annotations

import email.utils
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

from .context import ScrapeContext
from .hash_db import HashDB, content_hash
from ..utils.log_utils import jlog


# ── Freshness check (Last-Modified / ETag) ───────────────────────────


def check_url_freshness(url: str, db: HashDB, user_agent: str) -> Tuple[str, bool]:
    """HEAD-request *url* and compare ``Last-Modified`` with the hash DB's
    ``last_seen`` timestamp.  Returns ``(url, should_update)``.

    For URLs **already in the hash DB** (previously ingested):
    * ``Last-Modified`` newer than ``last_seen`` → ``True`` (should update)
    * ``Last-Modified`` absent or older → ``False`` (skip — the hash check
      during ingestion will catch real content changes anyway)

    For URLs **not in the hash DB** (first time):
    * Always ``True`` (must fetch and ingest).
    """
    try:
        existing = db.get(url)
        if existing is None:
            return url, True  # new URL — always process

        last_seen = existing["last_seen"]

        headers = {"User-Agent": user_agent}
        try:
            resp = httpx.head(url, headers=headers, timeout=10, follow_redirects=True)
            if resp.status_code == 405:
                resp = httpx.get(
                    url, headers=headers, timeout=10, follow_redirects=True
                )
                resp.close()
            if resp.status_code >= 400:
                # Server error — URL is known, skip rather than re-ingest
                return url, False
        except Exception:
            # Network error — URL is known, skip
            return url, False

        lm = resp.headers.get("Last-Modified")
        if lm:
            try:
                server_time = email.utils.parsedate_to_datetime(lm)
                if server_time.tzinfo is None:
                    server_time = server_time.replace(tzinfo=timezone.utc)
                last_seen_dt = datetime.fromtimestamp(last_seen, tz=timezone.utc)
                if server_time > last_seen_dt:
                    return url, True  # server says content is newer
            except Exception:
                pass
            return url, False  # Last-Modified is older or equal → skip

        # No Last-Modified header, but URL is already in hash DB.
        # Skip it — the hash check during ingestion catches real changes.
        return url, False
    except Exception:
        return url, True


def filter_unchanged_urls(
    urls: List[str],
    db: HashDB,
    user_agent: str,
    parallel: bool = True,
    max_workers: int = 10,
) -> Tuple[List[str], List[str]]:
    """Split *urls* into ``(to_update, unchanged)``.  Uses HEAD requests +
    ``Last-Modified`` comparison against the hash DB's ``last_seen``."""
    to_update: List[str] = []
    unchanged: List[str] = []

    workers = max_workers if parallel else 1
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {
                pool.submit(check_url_freshness, u, db, user_agent): u for u in urls
            }
            for f in as_completed(futs):
                try:
                    url, should = f.result()
                    (to_update if should else unchanged).append(url)
                except Exception:
                    to_update.append(futs[f])
    except Exception:
        to_update = list(urls)

    jlog(
        "info",
        "freshness_filter_result",
        to_update=len(to_update),
        unchanged=len(unchanged),
    )
    return to_update, unchanged


# ── Deduplication via hash DB ────────────────────────────────────────


def should_ingest(
    db: HashDB, url: str, page_content: str, chunk_count: int, session_id: str
) -> str:
    """Decide how to handle *url* given its current *page_content*.

    Returns one of:
    * ``"full"`` — ingest all chunks (new or changed content)
    * ``"skip"`` — hash + chunk count identical, nothing to do
    * ``"partial"`` — hash changed, caller should diff old vs new chunks

    Side-effect: the DB row is created/updated on ``"full"``; touched on ``"skip"``.
    """
    h = content_hash(page_content)
    existing = db.get(url)

    if existing is None:
        db.upsert(url, h, chunk_count, session_id)
        return "full"

    if existing["content_hash"] == h and existing["chunk_count"] == chunk_count:
        db.touch(url, session_id)
        return "skip"

    # Content or chunk count changed → caller does a diff
    db.upsert(url, h, chunk_count, session_id)
    return "partial"


# ── Partial (diff) ingestion ─────────────────────────────────────────


def diff_and_store(
    cat: Any,
    docs: list,
    source: str,
    metadata: Dict[str, Any],
) -> int:
    """Compare new *docs* against existing Qdrant chunks for *source*.

    * Deletes old chunks whose text is no longer in the new version.
    * Returns only genuinely new chunks (avoids re-embedding unchanged text).
    """
    collection = cat.memory.vectors.declarative
    filt = collection._qdrant_filter_from_dict({"source": source})
    old_points, _ = collection.client.scroll(
        collection_name=collection.collection_name,
        scroll_filter=filt,
        with_payload=True,
        limit=10_000,
    )
    old_texts = {p.payload["page_content"] for p in old_points}
    new_texts = {d.page_content for d in docs}

    # Delete stale
    stale_ids = [p.id for p in old_points if p.payload["page_content"] not in new_texts]
    if stale_ids:
        collection.delete_points(stale_ids)
        jlog("info", "diff_deleted_stale", source=source, count=len(stale_ids))

    # Keep only genuinely new
    new_docs = [d for d in docs if d.page_content not in old_texts]
    if new_docs:
        cat.rabbit_hole.store_documents(
            cat=cat, docs=new_docs, source=source, metadata=metadata
        )
        jlog("info", "diff_stored_new", source=source, count=len(new_docs))
    return len(new_docs)


# ── Retry logic ──────────────────────────────────────────────────────

_PERMANENT_PATTERNS = (
    "404",
    "not found",
    "403",
    "forbidden",
    "400",
    "bad request",
    "not supported",
)


def retry_failed(
    ctx: ScrapeContext,
    cat: Any,
    db: HashDB,
    max_attempts: int = 3,
    delay: int = 5,
) -> Tuple[List[str], List[str]]:
    """Re-attempt ingestion for ``ctx.failed_pages``.

    Returns ``(recovered, still_failed)``.
    """
    remaining = list(ctx.failed_pages)
    recovered: List[str] = []

    for attempt in range(1, max_attempts + 1):
        if not remaining:
            break
        to_try = list(remaining)
        for url in to_try:
            try:
                meta = {
                    "url": url,
                    "source": url,
                    "session_id": ctx.session_id,
                }
                cat.rabbit_hole.ingest_file(
                    cat=cat,
                    file=url,
                    chunk_size=ctx.chunk_size,
                    chunk_overlap=ctx.chunk_overlap,
                    metadata=meta,
                )
                remaining.remove(url)
                recovered.append(url)
                jlog("info", "retry_ok", url=url, attempt=attempt)
            except Exception as exc:
                err = str(exc).lower()
                if any(p in err for p in _PERMANENT_PATTERNS):
                    remaining.remove(url)
                    jlog("warning", "retry_permanent_error", url=url, error=str(exc))
                elif attempt == max_attempts:
                    jlog("error", "retry_exhausted", url=url, error=str(exc))

        if remaining and attempt < max_attempts:
            time.sleep(delay)

    return recovered, remaining


# ── Stale memory cleanup ────────────────────────────────────────────


def cleanup_stale_memories(
    cat: Any,
    db: HashDB,
    session_id: str,
    keep_urls: Set[str],
    qdrant_limit: int = 10_000,
) -> List[str]:
    """Remove vector memories (and DB rows) for URLs that were present in a
    previous session but are no longer found.

    *keep_urls* should be the union of scraped + ignored + still-failed URLs.
    Returns the list of removed URLs.
    """
    stale = db.urls_for_command_not_in(session_id, keep_urls)
    if not stale:
        return []

    removed: List[str] = []
    collection = cat.memory.vectors.declarative

    for url in stale:
        try:
            filt = collection._qdrant_filter_from_dict({"source": url})
            points, _ = collection.client.scroll(
                collection_name=collection.collection_name,
                scroll_filter=filt,
                limit=qdrant_limit,
                with_payload=False,
            )
            if points:
                collection.delete_points([p.id for p in points])
            db.delete(url)
            removed.append(url)
            jlog("info", "stale_url_removed", url=url)
        except Exception as exc:
            jlog("error", "cleanup_error", url=url, error=str(exc))

    jlog("info", "stale_cleanup_done", removed=len(removed))
    return removed


def delete_memories_by_source(source: str, cat_or_ccat: Any) -> int:
    """Delete all vector memory points with ``source`` metadata.
    Works with both ``StrayCat`` and ``CheshireCat`` instances.
    Returns the count of deleted points.
    """
    if not source:
        return 0

    if hasattr(cat_or_ccat, "memory"):
        vectors = cat_or_ccat.memory.vectors
    else:
        vectors = cat_or_ccat.vectors

    collection = vectors.collections["declarative"]
    filt = collection._qdrant_filter_from_dict({"source": source})
    points, _ = collection.client.scroll(
        collection_name=collection.collection_name,
        scroll_filter=filt,
        limit=10_000,
    )
    count = len(points)
    if count:
        collection.delete_points_by_metadata_filter({"source": source})
        jlog("info", "points_deleted", source=source, count=count)
    return count
