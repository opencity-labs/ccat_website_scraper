"""ccat_websites_scraper — main plugin module.

Hooks into the Cheshire Cat lifecycle to provide:
* Scheduled scraping via WhiteRabbit
* Automatic memory deduplication / cleanup
* Custom HTML parser with ``display:none`` filtering
* API endpoint for deleting memories by source

All log output uses structured JSON for easy parsing by log aggregators.
"""

from __future__ import annotations

import gc
import inspect
import json
import os
import re
import sys
import time
import urllib.parse
from typing import Any, Dict, List, Set

from bs4 import BeautifulSoup
from langchain.document_loaders.blob_loaders.schema import Blob
from langchain_community.document_loaders.parsers.html.bs4 import BS4HTMLParser

from cat.looking_glass.stray_cat import StrayCat
from cat.mad_hatter.decorators import hook, plugin, endpoint
from cat.rabbit_hole import RabbitHole
from cat.utils import singleton
from cat.auth.permissions import AuthPermission, AuthResource, check_permissions
from pydantic import BaseModel

from .core.context import ScrapeContext
from .core.crawler import crawl, batch_fetch
from .core.hash_db import HashDB, content_hash
from .core.memory_manager import (
    filter_unchanged_urls,
    should_ingest,
    diff_and_store,
    retry_failed,
    cleanup_stale_memories,
    delete_memories_by_source,
)
from .utils.url_utils import (
    clean_url,
    normalize_url_with_protocol,
    normalize_domain,
    validate_url,
    random_sleep,
)
from .utils.log_utils import jlog
from .utils.robots import (
    load_robots,
    is_url_allowed,
    get_crawl_delay,
    clear_cache as clear_robots_cache,
)
from .utils.sitemap import fetch_sitemap_entries
from .settings import ScrapingEngine, ScheduleMode


# ═══════════════════════════════════════════════════════════════════════
# Monkey-patch RabbitHole.ingest_file to propagate metadata
# ═══════════════════════════════════════════════════════════════════════

_original_ingest_file = None
_RabbitHoleClass = None

for cls in singleton.instances:
    if cls.__name__ == "RabbitHole":
        _RabbitHoleClass = cls
        _original_ingest_file = cls.ingest_file
        break

if (
    not _RabbitHoleClass
    and hasattr(RabbitHole, "__closure__")
    and RabbitHole.__closure__
):
    for cell in RabbitHole.__closure__:
        if (
            inspect.isclass(cell.cell_contents)
            and cell.cell_contents.__name__ == "RabbitHole"
        ):
            _RabbitHoleClass = cell.cell_contents
            _original_ingest_file = _RabbitHoleClass.ingest_file
            break

if not _RabbitHoleClass:
    cat_module = sys.modules.get("cat.rabbit_hole")
    if cat_module:
        for name, obj in inspect.getmembers(cat_module):
            if inspect.isclass(obj) and name == "RabbitHole" and obj is not RabbitHole:
                _RabbitHoleClass = obj
                _original_ingest_file = obj.ingest_file
                break

if _original_ingest_file and _RabbitHoleClass:

    def _patched_ingest_file(
        self, cat, file, chunk_size=None, chunk_overlap=None, metadata=None
    ):
        if metadata is None:
            metadata = {}
        if hasattr(cat, "working_memory"):
            cat.working_memory.temp_ingest_metadata = metadata
        else:
            cat._temp_ingest_metadata = metadata
        try:
            return _original_ingest_file(
                self, cat, file, chunk_size, chunk_overlap, metadata
            )
        finally:
            if hasattr(cat, "working_memory") and hasattr(
                cat.working_memory, "temp_ingest_metadata"
            ):
                del cat.working_memory.temp_ingest_metadata
            elif hasattr(cat, "_temp_ingest_metadata"):
                delattr(cat, "_temp_ingest_metadata")

    _RabbitHoleClass.ingest_file = _patched_ingest_file


# ═══════════════════════════════════════════════════════════════════════
# Custom HTML parser — strips display:none elements
# ═══════════════════════════════════════════════════════════════════════


class _CleanHTMLParser(BS4HTMLParser):
    """BS4 HTML parser that removes ``display: none`` divs before parsing."""

    def __init__(self, strip_hidden: bool = False):
        self._strip_hidden = strip_hidden
        super().__init__()

    def lazy_parse(self, blob: Blob):
        with blob.as_bytes_io() as f:
            raw = f.read()
        soup = BeautifulSoup(raw, "html.parser")
        if self._strip_hidden:
            for div in soup.find_all("div", style=True):
                style = div["style"].lower().replace(" ", "").replace(";", "")
                if "display:none" in style:
                    div.decompose()
        new_blob = Blob.from_data(
            data=str(soup).encode("utf-8"),
            mime_type="text/html",
            path=blob.source,
        )
        yield from super().lazy_parse(new_blob)

    def parse(self, blob: Blob) -> list:
        return list(self.lazy_parse(blob))


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════


def _load_settings(cat: Any) -> Dict[str, Any]:
    return cat.mad_hatter.get_plugin().load_settings()


def _get_db(settings: Dict[str, Any]) -> HashDB:
    return HashDB(settings.get("db_path", "cat/data/websites_scraper.db"))


def _build_context(settings: Dict[str, Any], scheduled: bool = False) -> ScrapeContext:
    """Build a :class:`ScrapeContext` from plugin settings."""
    ctx = ScrapeContext()
    ctx.scheduled = scheduled
    ctx.max_depth = settings.get("max_depth", -1)
    ctx.max_pages = settings.get("max_pages", -1)
    ctx.max_workers = settings.get("max_workers", 4)
    ctx.request_delay = settings.get("request_delay", 0.5)
    ctx.page_timeout = settings.get("page_timeout", 30)
    ctx.skip_get_params = settings.get("skip_get_params", False)
    ctx.ingest_pdf = settings.get("ingest_pdf", False)
    ctx.cache_content = settings.get("cache_content", True)
    ctx.follow_robots_txt = settings.get("follow_robots_txt", True)
    ctx.obey_crawl_delay = settings.get("obey_crawl_delay", True)
    ctx.only_sitemap = settings.get("only_sitemap", False)
    ctx.chunk_size = settings.get("chunk_size", 1024)
    ctx.chunk_overlap = settings.get("chunk_overlap", 256)
    ctx.user_agent = settings.get("user_agent", "CheshireCatAI WebsitesScraper/1.0")

    engine = settings.get("scraping_engine", ScrapingEngine.STANDARD)
    if isinstance(engine, str):
        engine = ScrapingEngine(engine)
    ctx.use_scrapling = engine == ScrapingEngine.SCRAPLING

    ext_str = settings.get(
        "skip_extensions",
        ".jpg,.jpeg,.png,.gif,.bmp,.svg,.webp,.ico,.zip,.ods,.odt,.xls,.p7m,.rar,.mp3,.xml,.7z,.exe,.doc,.m4a,.crdownload,.odp,.ppt,.pptx",
    )
    ctx.skip_extensions = [
        e.strip() if e.strip().startswith(".") else f".{e.strip()}"
        for e in ext_str.split(",")
        if e.strip()
    ]

    # Anti-detection / rate-limiting
    ctx.batch_size = settings.get("batch_size", 50)
    ctx.batch_pause_seconds = settings.get("batch_pause_seconds", 5.0)
    ctx.adaptive_backoff = settings.get("adaptive_backoff", True)
    ctx.backoff_initial_seconds = settings.get("backoff_initial_seconds", 10.0)
    ctx.backoff_max_seconds = settings.get("backoff_max_seconds", 120.0)
    ctx.backoff_threshold = settings.get("backoff_threshold", 3)

    # Proxy
    ctx.proxy_enabled = settings.get("proxy_enabled", False)
    raw_proxies = settings.get("proxy_list", "").strip()
    ctx.proxy_list = (
        [p.strip() for p in raw_proxies.splitlines() if p.strip()]
        if raw_proxies
        else []
    )
    ctx.proxy_rotate_every = settings.get("proxy_rotate_every", 10)

    return ctx


def _batched(lst: List, n: int):
    """Yield successive *n*-sized slices from *lst*."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# ═══════════════════════════════════════════════════════════════════════
# Core scraping orchestration
# ═══════════════════════════════════════════════════════════════════════


def run_scrape(cat: Any, scheduled: bool = False) -> str:
    """Crawl, deduplicate, ingest, and clean up.  Returns a human-readable summary string."""

    settings = _load_settings(cat)

    # ── Load URLs from settings ──────────────────────────────────
    urls_raw = settings.get("starting_urls", "").strip()
    starting_urls = [
        normalize_url_with_protocol(clean_url(u))
        for u in urls_raw.split(",")
        if u.strip() and validate_url(u.strip())
    ]
    if not starting_urls:
        return "Error: no valid starting URLs in plugin settings."

    ctx = _build_context(settings, scheduled)

    jlog(
        "info",
        "scrape_start",
        starting_urls=starting_urls,
        scheduled=scheduled,
        engine="scrapling" if ctx.use_scrapling else "standard",
        only_sitemap=ctx.only_sitemap,
    )

    # Allowed extra domains from settings
    for u in settings.get("allowed_extra_domains", "").split(","):
        u = u.strip()
        if u and validate_url(u):
            ctx.allowed_domains.add(normalize_domain(normalize_url_with_protocol(u)))

    # Root domains + paths (per-domain)
    for url in starting_urls:
        parsed = urllib.parse.urlparse(url)
        domain = normalize_domain(parsed.netloc)
        ctx.root_domains.add(domain)
        ctx.allowed_paths.setdefault(domain, set()).add(parsed.path or "/")

    # Preload robots.txt
    if ctx.follow_robots_txt:
        for d in ctx.root_domains | ctx.allowed_domains:
            load_robots(d, ctx.user_agent)

    # ── Sitemap discovery ────────────────────────────────────────
    # Always populate ctx.sitemap_urls and ctx.sitemap_meta.
    # These are kept separate from ctx.scraped_pages intentionally:
    # the merge with BFS results happens below.
    if settings.get("use_sitemap", True):
        for url in starting_urls:
            try:
                entries = fetch_sitemap_entries(url, ctx.user_agent)
                for e in entries:
                    ctx.sitemap_urls.add(e.loc)
                    ctx.sitemap_meta[e.loc] = e
                jlog("info", "sitemap_loaded", root_url=url, entries=len(entries))
            except Exception as exc:
                jlog("warning", "sitemap_fetch_failed", url=url, error=str(exc))

    # ── Crawl (BFS) ─────────────────────────────────────────────
    start_time = time.time()

    if not ctx.only_sitemap:
        crawl(ctx, cat, starting_urls)

        # Merge BFS results + sitemap URLs (set union = no duplicates).
        ctx.scraped_pages = list(set(ctx.scraped_pages) | ctx.sitemap_urls)
    else:
        # only_sitemap=True: use exactly the sitemap URLs, no BFS.
        ctx.scraped_pages = list(ctx.sitemap_urls)
        ctx.visited_pages.update(ctx.sitemap_urls)

    jlog(
        "info",
        "crawl_phase_done",
        sitemap_urls=len(ctx.sitemap_urls),
        bfs_only=(
            len(set(ctx.scraped_pages) - ctx.sitemap_urls)
            if not ctx.only_sitemap
            else 0
        ),
        total=len(ctx.scraped_pages),
        failed=len(ctx.failed_pages),
    )

    if not ctx.scraped_pages:
        elapsed = round((time.time() - start_time) / 60, 2)
        if ctx.failed_pages:
            return f"No pages scraped. {len(ctx.failed_pages)} failed ({elapsed} min)."
        return f"No pages scraped ({elapsed} min)."

    # ── Sitemap lastmod pre-filter ───────────────────────────────
    # If the sitemap provides a <lastmod> for a URL and our hash DB
    # already has that URL with a last_seen >= lastmod, skip the URL
    # entirely (no fetch, no HEAD request, just touch the DB row).
    db = _get_db(settings)
    if ctx.sitemap_meta:
        sitemap_skipped: List[str] = []
        remaining: List[str] = []
        for url in ctx.scraped_pages:
            entry = ctx.sitemap_meta.get(url)
            if entry and entry.lastmod is not None:
                existing = db.get(url)
                if existing is not None:
                    from datetime import datetime, timezone

                    try:
                        last_seen_dt = datetime.fromtimestamp(
                            existing["last_seen"], tz=timezone.utc
                        )
                        if entry.lastmod <= last_seen_dt:
                            # Sitemap says page hasn't changed since we last saw it
                            db.touch(url, ctx.session_id)
                            sitemap_skipped.append(url)
                            continue
                    except Exception:
                        pass  # fall through to normal processing
            remaining.append(url)

        if sitemap_skipped:
            ctx.scraped_pages = remaining
            ctx.ignored_pages.extend(sitemap_skipped)
            jlog(
                "info",
                "sitemap_lastmod_filter",
                skipped=len(sitemap_skipped),
                remaining=len(remaining),
            )

    # ── Freshness filter (Last-Modified HEAD vs hash DB) ─────────
    to_update, unchanged = filter_unchanged_urls(
        ctx.scraped_pages,
        db,
        ctx.user_agent,
        parallel=True,
        max_workers=ctx.max_workers,
    )
    ctx.scraped_pages = to_update
    ctx.ignored_pages.extend(unchanged)

    jlog(
        "info",
        "freshness_filter_done",
        to_update=len(to_update),
        unchanged=len(unchanged),
    )

    if not ctx.scraped_pages:
        elapsed = round((time.time() - start_time) / 60, 2)
        jlog("info", "nothing_to_update", elapsed_min=elapsed)
        db.close()
        return f"All {len(unchanged)} pages unchanged ({elapsed} min)."

    # ── Batched fetch + ingest ───────────────────────────────────
    ingested = 0
    skipped = 0
    total_to_process = len(ctx.scraped_pages)
    all_pages = list(ctx.scraped_pages)  # snapshot — we mutate ctx below
    batch_sz = ctx.batch_size

    jlog(
        "info",
        "ingest_phase_start",
        total=total_to_process,
        batch_size=batch_sz,
        batch_pause=ctx.batch_pause_seconds,
        proxy_enabled=ctx.proxy_enabled,
        adaptive_backoff=ctx.adaptive_backoff,
    )

    for batch_num, batch in enumerate(_batched(all_pages, batch_sz), 1):
        # ── Inter-batch pause (skip first batch) ─────────────────────
        if batch_num > 1 and ctx.batch_pause_seconds > 0:
            jlog(
                "info", "batch_pause", seconds=ctx.batch_pause_seconds, batch=batch_num
            )
            time.sleep(ctx.batch_pause_seconds)

        # ── 1. Fetch content for this batch (if not already cached) ──
        urls_needing_content = [u for u in batch if u not in ctx.scraped_contents]
        if urls_needing_content:
            jlog(
                "info",
                "batch_content_fetch",
                batch=batch_num,
                to_fetch=len(urls_needing_content),
            )
            try:
                batch_fetch(ctx, cat, urls_needing_content)
            except Exception as fetch_exc:
                jlog(
                    "error",
                    "batch_fetch_crashed",
                    batch=batch_num,
                    error=str(fetch_exc),
                )
                # Mark every URL that wasn't fetched as failed so they
                # can be retried later — do NOT abort the whole run.
                for u in urls_needing_content:
                    if u not in ctx.scraped_contents and u not in ctx.failed_pages:
                        ctx.failed_pages.append(u)

        # ── 2. Ingest each URL in the batch ─────────────────────────
        for url in batch:
            processed_so_far = ingested + skipped
            global_idx = processed_so_far + 1
            pct = round(global_idx / total_to_process * 100, 1)

            try:
                metadata: Dict[str, str] = {
                    "url": url,
                    "source": url,
                    "session_id": ctx.session_id,
                }

                if url in ctx.scraped_contents:
                    raw_bytes, ctype = ctx.scraped_contents[url]
                    docs = cat.rabbit_hole.string_to_docs(
                        cat=cat,
                        file_bytes=raw_bytes,
                        source=url,
                        content_type=ctype,
                        chunk_size=ctx.chunk_size,
                        chunk_overlap=ctx.chunk_overlap,
                    )
                elif not ctx.use_scrapling:
                    # Standard engine fallback: let rabbit_hole fetch the page
                    if ctx.request_delay > 0:
                        time.sleep(random_sleep(ctx.request_delay))
                    try:
                        cat.rabbit_hole.ingest_file(
                            cat=cat,
                            file=url,
                            chunk_size=ctx.chunk_size,
                            chunk_overlap=ctx.chunk_overlap,
                            metadata=metadata,
                        )
                        db.upsert(url, "ingested-via-rabbithole", 0, ctx.session_id)
                        ingested += 1
                        jlog(
                            "info",
                            "ingest_ok_rabbithole",
                            url=url,
                            progress=f"{global_idx}/{total_to_process}",
                            percentage=pct,
                        )
                        continue
                    except Exception as e:
                        ctx.failed_pages.append(url)
                        jlog("error", "ingest_failed_rabbithole", url=url, error=str(e))
                        continue
                else:
                    # Scrapling engine but URL failed content fetch — mark
                    # as *failed* so the retry logic picks it up.
                    jlog(
                        "warning",
                        "ingest_skip_no_content",
                        url=url,
                        msg="Content not fetched (Scrapling fetch failed?)",
                    )
                    if url not in ctx.failed_pages:
                        ctx.failed_pages.append(url)
                    continue

                # We have docs — run hash-based dedup
                full_text = "\n".join(d.page_content for d in docs)
                decision = should_ingest(db, url, full_text, len(docs), ctx.session_id)

                if decision == "skip":
                    jlog(
                        "info",
                        "ingest_skip_unchanged",
                        url=url,
                        progress=f"{global_idx}/{total_to_process}",
                        percentage=pct,
                    )
                    ctx.ignored_pages.append(url)
                    skipped += 1
                    continue

                if decision == "partial":
                    n_new = diff_and_store(cat, docs, url, metadata)
                    if n_new == 0:
                        # Hash changed (e.g. dynamic HTML) but actual
                        # chunk content is identical — treat as skip.
                        jlog(
                            "info",
                            "ingest_skip_no_diff",
                            url=url,
                            progress=f"{global_idx}/{total_to_process}",
                            percentage=pct,
                        )
                        ctx.ignored_pages.append(url)
                        skipped += 1
                        continue
                else:
                    # full
                    cat.rabbit_hole.store_documents(
                        cat=cat, docs=docs, source=url, metadata=metadata
                    )

                ingested += 1
                jlog(
                    "info",
                    "ingest_ok",
                    url=url,
                    decision=decision,
                    progress=f"{global_idx}/{total_to_process}",
                    percentage=pct,
                )

            except Exception as exc:
                ctx.failed_pages.append(url)
                jlog("error", "ingest_error", url=url, error=str(exc))

        # ── 3. Free memory for this batch ────────────────────────────
        for url in batch:
            ctx.scraped_contents.pop(url, None)
        gc.collect()

        jlog(
            "info",
            "batch_done",
            batch=batch_num,
            ingested=ingested,
            skipped=skipped,
            failed=len(ctx.failed_pages),
            overall_progress=f"{min(ingested + skipped, total_to_process)}/{total_to_process}",
        )

    jlog(
        "info",
        "ingest_phase_done",
        ingested=ingested,
        skipped=skipped,
        failed=len(ctx.failed_pages),
    )

    # ── Retry failures ───────────────────────────────────────────
    if ctx.failed_pages and settings.get("retry_failed_urls", True):
        jlog("info", "retry_start", count=len(ctx.failed_pages))
        recovered, still_failed = retry_failed(
            ctx,
            cat,
            db,
            max_attempts=settings.get("max_retry_attempts", 3),
            delay=settings.get("retry_delay_seconds", 5),
        )
        ctx.scraped_pages.extend(recovered)
        ctx.failed_pages = still_failed
        jlog(
            "info",
            "retry_done",
            recovered=len(recovered),
            still_failed=len(still_failed),
        )

    # ── Stale cleanup ────────────────────────────────────────────
    keep: Set[str] = (
        set(ctx.scraped_pages) | set(ctx.ignored_pages) | set(ctx.failed_pages)
    )
    removed = cleanup_stale_memories(cat, db, ctx.session_id, keep)
    if removed:
        jlog("info", "stale_cleanup", removed=len(removed))

    db.close()

    elapsed = round((time.time() - start_time) / 60, 2)
    summary_parts = [f"{ingested} URLs ingested"]
    if skipped:
        summary_parts.append(f"{skipped} skipped (unchanged)")
    if ctx.failed_pages:
        summary_parts.append(f"{len(ctx.failed_pages)} failed")
    if removed:
        summary_parts.append(f"{len(removed)} stale removed")
    summary_parts.append(f"{elapsed} min")
    summary = ", ".join(summary_parts)

    jlog(
        "info",
        "scrape_done",
        summary=summary,
        elapsed_min=elapsed,
        ingested=ingested,
        skipped=skipped,
        failed=len(ctx.failed_pages),
        stale_removed=len(removed or []),
    )

    return summary


# ═══════════════════════════════════════════════════════════════════════
# Scheduling
# ═══════════════════════════════════════════════════════════════════════


def _setup_schedule(ccat: Any, settings: Dict[str, Any] | None = None) -> None:
    """Create / update / remove the scheduled scraping job."""
    try:
        if settings is None:
            settings = ccat.mad_hatter.get_plugin().load_settings()

        job_id = "websites_scraper_scheduled"

        # Always remove existing
        try:
            ccat.white_rabbit.scheduler.remove_job(job_id)
        except Exception:
            pass

        if not settings.get("schedule_enabled", False):
            jlog("info", "schedule_disabled")
            return

        urls = settings.get("starting_urls", "").strip()
        if not urls:
            jlog("info", "schedule_no_urls")
            return

        from cat.looking_glass.stray_cat import StrayCat
        from cat.auth.permissions import AuthUserInfo

        def _scheduled_job(cat_ref: Any = None) -> str:
            system_user = AuthUserInfo(id="system", name="system")
            stray = StrayCat(system_user)
            return run_scrape(stray, scheduled=True)

        mode = settings.get("schedule_mode", ScheduleMode.DAILY_AT)
        if isinstance(mode, str):
            mode = ScheduleMode(mode)

        hour = settings.get("schedule_hour", 3)
        minute = settings.get("schedule_minute", 0)

        if mode == ScheduleMode.DAILY_AT:
            ccat.white_rabbit.schedule_cron_job(
                job=_scheduled_job,
                job_id=job_id,
                hour=hour,
                minute=minute,
            )
            jlog("info", "schedule_set", mode="daily_at", hour=hour, minute=minute)

        elif mode == ScheduleMode.EVERY_N_HOURS:
            n = settings.get("schedule_interval_hours", 6)
            ccat.white_rabbit.schedule_interval_job(
                job=_scheduled_job,
                job_id=job_id,
                hours=n,
            )
            jlog("info", "schedule_set", mode="every_n_hours", interval_hours=n)

        elif mode == ScheduleMode.EVERY_N_DAYS_AT:
            n = settings.get("schedule_interval_days", 1)
            ccat.white_rabbit.schedule_cron_job(
                job=_scheduled_job,
                job_id=job_id,
                day=f"*/{n}",
                hour=hour,
                minute=minute,
            )
            jlog(
                "info",
                "schedule_set",
                mode="every_n_days_at",
                interval_days=n,
                hour=hour,
                minute=minute,
            )

        elif mode == ScheduleMode.CRON:
            expr = settings.get("schedule_cron_expression", "").strip()
            if expr:
                parts = expr.split()
                kw: Dict[str, str] = {}
                fields = ["minute", "hour", "day", "month", "day_of_week"]
                for i, f in enumerate(fields):
                    if i < len(parts):
                        kw[f] = parts[i]
                ccat.white_rabbit.schedule_cron_job(
                    job=_scheduled_job,
                    job_id=job_id,
                    **kw,
                )
                jlog("info", "schedule_set", mode="cron", expression=expr)

    except Exception as exc:
        jlog("error", "schedule_setup_error", error=str(exc))


# ═══════════════════════════════════════════════════════════════════════
# Hooks
# ═══════════════════════════════════════════════════════════════════════


@hook()
def after_cat_bootstrap(cat) -> None:
    """Set up the schedule when the Cat starts."""
    _setup_schedule(cat)


@hook(priority=20)
def before_rabbithole_splits_text(doc, cat):
    """Propagate source URL metadata from the monkey-patched ingest_file."""
    metadata = {}
    if hasattr(cat, "working_memory") and hasattr(
        cat.working_memory, "temp_ingest_metadata"
    ):
        metadata = cat.working_memory.temp_ingest_metadata
    elif hasattr(cat, "_temp_ingest_metadata"):
        metadata = cat._temp_ingest_metadata

    if metadata and "source" in metadata:
        for d in doc:
            d.metadata["source"] = metadata["source"]
    return doc


@hook(priority=10)
def rabbithole_instantiates_parsers(file_handlers: dict, cat) -> dict:
    """Swap in the ``display:none``-stripping HTML parser when configured."""
    try:
        settings = _load_settings(cat)
        if settings.get("ignore_display_none", False):
            file_handlers["text/html"] = _CleanHTMLParser(strip_hidden=True)
    except Exception as exc:
        jlog("error", "parser_hook_error", error=str(exc))
    return file_handlers


@hook
def after_rabbithole_splitted_text(chunks, cat):
    """Filter out garbage chunks (too short, CID artefacts, etc.)."""
    settings = _load_settings(cat)
    min_len = settings.get("min_chunk_length", 50)
    cid_re = re.compile(r"(\(cid:\d+\)\s*){2,}")

    kept = []
    for c in chunks:
        if len(c.page_content) < min_len:
            continue
        src = c.metadata.get("source", "")
        if src.lower().endswith(".pdf"):
            s = c.page_content.replace(" ", "").strip()
            if s and s.count(s[0]) == len(s):
                continue
            if cid_re.search(c.page_content):
                continue
        kept.append(c)
    if not kept:
        jlog(
            "warning", "all_chunks_filtered", msg="All chunks removed by quality filter"
        )
    return kept


# ═══════════════════════════════════════════════════════════════════════
# Plugin settings save handler
# ═══════════════════════════════════════════════════════════════════════


def _save_settings_to_file(settings: Dict, plugin_path: str) -> Dict:
    path = os.path.join(plugin_path, "settings.json")
    old = {}
    if os.path.exists(path):
        try:
            with open(path) as f:
                old = json.load(f)
        except Exception:
            pass
    merged = {**old, **settings}
    try:
        with open(path, "w") as f:
            json.dump(merged, f, indent=4)
        return merged
    except Exception as exc:
        jlog("error", "settings_save_error", error=str(exc))
        return {}


@plugin
def save_settings(settings: Dict) -> Dict:
    # Handle DB deletion request
    if settings.get("delete_db", False):
        db_path = settings.get("db_path", "cat/data/websites_scraper.db")
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
                jlog("info", "hash_db_deleted", path=db_path)
        except Exception as exc:
            jlog("error", "hash_db_delete_error", error=str(exc))
        settings["delete_db"] = False
        clear_robots_cache()

    # Update schedule
    try:
        from cat.looking_glass.cheshire_cat import CheshireCat

        ccat = CheshireCat()
        _setup_schedule(ccat, settings)
    except Exception as exc:
        jlog("error", "schedule_update_error", error=str(exc))

    plugin_path = os.path.dirname(os.path.abspath(__file__))
    return _save_settings_to_file(settings, plugin_path)


# ═══════════════════════════════════════════════════════════════════════
# API endpoint
# ═══════════════════════════════════════════════════════════════════════


class _DeleteRequest(BaseModel):
    source: str


@endpoint.delete(path="/memory/scraper-delete-by-source", tags=["Websites Scraper"])
def api_delete_by_source(
    request: _DeleteRequest,
    cat: StrayCat = check_permissions(AuthResource.MEMORY, AuthPermission.DELETE),
) -> Dict[str, str]:
    """Delete all declarative memories matching *source*."""
    if not request.source:
        return {"error": "source is required"}
    n = delete_memories_by_source(request.source, cat)
    # Also remove from hash DB
    try:
        settings = _load_settings(cat)
        db = _get_db(settings)
        db.delete(request.source)
        db.close()
    except Exception:
        pass
    jlog("info", "api_delete_by_source", source=request.source, deleted=n)
    return {"message": f"Deleted {n} memories for source '{request.source}'"}
