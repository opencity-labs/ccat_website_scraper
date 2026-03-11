"""Multi-threaded BFS web crawler with batch-fetch support.

Supports two backends:
* **standard** — ``requests`` + BeautifulSoup (default, lightweight)
* **scrapling** — Camoufox stealth browser (JS rendering, anti-detection)

For the *Scrapling* backend the module reuses a **single Camoufox browser
session** across an entire batch of URLs, avoiding the per-page browser
launch/teardown that would otherwise crash the container on large sitemaps.

Anti-detection features:
* **Proxy rotation** — round-robin switching through a configurable proxy list
* **Adaptive backoff** — exponential pause on 429 / empty-content responses
* **Browser restart** on consecutive failures (Camoufox)
* **Randomised delays** — jitter between requests
"""

from __future__ import annotations

import gc
import math
import random
import threading
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from .context import ScrapeContext
from ..utils.log_utils import jlog
from ..utils.url_utils import normalize_domain, random_sleep
from ..utils.robots import is_url_allowed, get_crawl_delay

# ── Thread-local HTTP sessions ───────────────────────────────────────

_tls = threading.local()


def _get_session(user_agent: str, proxy: Optional[str] = None) -> requests.Session:
    """Return a thread-local ``requests.Session``, optionally via *proxy*."""
    key = f"session_{proxy or 'direct'}"
    if not hasattr(_tls, key):
        s = requests.Session()
        s.headers.update({"User-Agent": user_agent})
        if proxy:
            s.proxies = {"http": proxy, "https": proxy}
        setattr(_tls, key, s)
    return getattr(_tls, key)


# ── Proxy helper ─────────────────────────────────────────────────────


class _ProxyRotator:
    """Thread-safe round-robin proxy selector."""

    def __init__(self, proxies: List[str], rotate_every: int = 10):
        self._proxies = proxies or []
        self._rotate_every = max(rotate_every, 1) if rotate_every else 0
        self._idx = 0
        self._count = 0
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self._proxies)

    def next(self) -> Optional[str]:
        if not self._proxies:
            return None
        with self._lock:
            proxy = self._proxies[self._idx % len(self._proxies)]
            self._count += 1
            if self._rotate_every and self._count >= self._rotate_every:
                self._count = 0
                self._idx += 1
                jlog(
                    "info",
                    "proxy_rotated",
                    new_proxy=self._proxies[self._idx % len(self._proxies)],
                    after_pages=self._rotate_every,
                )
            return proxy

    def force_rotate(self) -> Optional[str]:
        """Immediately advance to the next proxy (e.g. after repeated failures)."""
        if not self._proxies:
            return None
        with self._lock:
            self._idx += 1
            self._count = 0
            proxy = self._proxies[self._idx % len(self._proxies)]
            jlog("info", "proxy_force_rotated", new_proxy=proxy)
            return proxy


def _build_proxy_rotator(ctx: ScrapeContext) -> _ProxyRotator:
    if ctx.proxy_enabled and ctx.proxy_list:
        return _ProxyRotator(ctx.proxy_list, ctx.proxy_rotate_every)
    return _ProxyRotator([], 0)


# ── Adaptive backoff ─────────────────────────────────────────────────


class _BackoffTracker:
    """Tracks consecutive failures and computes exponential backoff waits."""

    def __init__(self, ctx: ScrapeContext):
        self.enabled = ctx.adaptive_backoff
        self.initial = ctx.backoff_initial_seconds
        self.maximum = ctx.backoff_max_seconds
        self.threshold = ctx.backoff_threshold
        self.consecutive_fails = 0

    def record_success(self) -> None:
        self.consecutive_fails = 0

    def record_failure(self, url: str, reason: str) -> float:
        """Record a failure and return the seconds to wait (0 if disabled)."""
        if not self.enabled:
            return 0.0
        self.consecutive_fails += 1
        wait_secs = min(
            self.initial * (2 ** (self.consecutive_fails - 1)),
            self.maximum,
        )
        # Add jitter ±20%
        wait_secs *= random.uniform(0.8, 1.2)
        jlog(
            "warning",
            "adaptive_backoff",
            url=url,
            reason=reason,
            consecutive_fails=self.consecutive_fails,
            wait_seconds=round(wait_secs, 1),
        )
        return wait_secs

    @property
    def should_restart_browser(self) -> bool:
        """True when consecutive failures hit the threshold — signals the
        caller to restart the browser session and/or rotate proxy."""
        return self.enabled and self.consecutive_fails >= self.threshold


def _is_empty_content(html: str) -> bool:
    """Detect responses that are technically 200 but have no real content
    (soft blocks, captcha pages, empty shells)."""
    if not html or len(html.strip()) < 100:
        return True
    # Common block indicators
    lower = html.lower()
    if "<title>access denied</title>" in lower:
        return True
    if "captcha" in lower and len(html) < 3000:
        return True
    return False


# ── Scrapling / Camoufox helpers ─────────────────────────────────────

_SCRAPLING_AVAILABLE: bool | None = None
_CAMOUFOX_AVAILABLE: bool | None = None


def _check_scrapling() -> bool:
    global _SCRAPLING_AVAILABLE
    if _SCRAPLING_AVAILABLE is None:
        try:
            from scrapling import StealthyFetcher  # noqa: F401

            _SCRAPLING_AVAILABLE = True
        except ImportError as exc:
            _SCRAPLING_AVAILABLE = False
            jlog("error", "scrapling_import_failed", error=str(exc))
    return _SCRAPLING_AVAILABLE


def _check_camoufox() -> bool:
    global _CAMOUFOX_AVAILABLE
    if _CAMOUFOX_AVAILABLE is None:
        try:
            from camoufox.sync_api import Camoufox  # noqa: F401

            _CAMOUFOX_AVAILABLE = True
        except ImportError as exc:
            _CAMOUFOX_AVAILABLE = False
            jlog("error", "camoufox_import_failed", error=str(exc))
    return _CAMOUFOX_AVAILABLE


def _fetch_with_scrapling_single(
    url: str, timeout: int, proxy: Optional[str] = None
) -> Tuple[str, bytes, str]:
    """Fetch *url* via Scrapling StealthyFetcher (single-page, launches a new
    browser).  Kept as a fallback when ``camoufox.sync_api`` is unavailable."""
    from scrapling import StealthyFetcher

    kwargs: Dict[str, Any] = {"headless": True, "timeout": timeout * 1000}
    if proxy:
        kwargs["network_idle"] = True
        # Scrapling StealthyFetcher accepts proxy as a kwarg
        kwargs["proxy"] = {"server": proxy}

    fetcher = StealthyFetcher()
    resp = fetcher.fetch(url, **kwargs)
    html = resp.html if hasattr(resp, "html") else str(resp)
    raw = html.encode("utf-8") if isinstance(html, str) else html
    return html, raw, "text/html"


# ── Batch content fetch ─────────────────────────────────────────────


def batch_fetch(ctx: ScrapeContext, cat: Any, urls: List[str]) -> int:
    """Fetch content for *urls* using the configured engine.

    Stores results in ``ctx.scraped_contents``.  Failed URLs are appended to
    ``ctx.failed_pages``.  Returns the count of successfully fetched URLs.
    """
    if not urls:
        return 0
    if ctx.use_scrapling:
        if _check_scrapling():
            return _batch_fetch_scrapling(ctx, cat, urls)
        jlog(
            "error",
            "scrapling_not_available",
            msg="scrapling engine selected but import failed — falling back to standard. "
            "Make sure 'scrapling[camoufox]' is installed.",
        )
    return _batch_fetch_standard(ctx, cat, urls)


def _apply_crawl_delay(ctx: ScrapeContext, url: str) -> None:
    """Sleep for the appropriate delay (configured + robots.txt) with jitter."""
    parsed = urllib.parse.urlparse(url)
    domain = normalize_domain(parsed.netloc)
    delay = ctx.request_delay
    if ctx.obey_crawl_delay:
        rd = get_crawl_delay(domain, ctx.user_agent)
        if rd is not None:
            delay = max(delay, rd)
            ctx.domain_crawl_delays[domain] = rd
    if delay > 0:
        # Add ±30% jitter to make timing less predictable
        jittered = delay * random.uniform(0.7, 1.3)
        time.sleep(jittered)


def _is_robots_allowed(ctx: ScrapeContext, url: str) -> bool:
    """Return ``False`` if robots.txt blocks *url*."""
    parsed = urllib.parse.urlparse(url)
    domain = normalize_domain(parsed.netloc)
    return is_url_allowed(domain, url, ctx.user_agent, ctx.follow_robots_txt)


def _batch_fetch_scrapling(ctx: ScrapeContext, cat: Any, urls: List[str]) -> int:
    """Fetch URLs with a single Camoufox browser session (stealth, JS-capable).

    Falls back to per-page ``StealthyFetcher`` if the ``camoufox`` sync API
    is not available.
    """
    total = len(urls)
    fetched = 0
    proxy_rot = _build_proxy_rotator(ctx)
    backoff = _BackoffTracker(ctx)

    if _check_camoufox():
        jlog(
            "info",
            "batch_fetch_start",
            engine="camoufox",
            total=total,
            proxy_enabled=proxy_rot.enabled,
        )
        fetched = _batch_fetch_camoufox(ctx, cat, urls, proxy_rot, backoff)
    else:
        # Fallback: per-page StealthyFetcher (slower, higher memory)
        jlog(
            "warning",
            "camoufox_sync_unavailable",
            msg="Falling back to StealthyFetcher per page — this is slower and uses more memory",
        )
        for i, url in enumerate(urls, 1):
            if not _is_robots_allowed(ctx, url):
                jlog("info", "blocked_by_robots", url=url)
                ctx.ignored_pages.append(url)
                continue
            _apply_crawl_delay(ctx, url)
            proxy = proxy_rot.next()
            try:
                html, raw, ctype = _fetch_with_scrapling_single(
                    url, ctx.page_timeout, proxy=proxy
                )

                if _is_empty_content(html):
                    wait = backoff.record_failure(url, "empty_content")
                    ctx.failed_pages.append(url)
                    if wait > 0:
                        time.sleep(wait)
                    if backoff.should_restart_browser:
                        proxy_rot.force_rotate()
                    continue

                backoff.record_success()
                with ctx.contents_lock:
                    ctx.scraped_contents[url] = (raw, ctype)
                fetched += 1
                pct = round(fetched / total * 100, 1)
                jlog(
                    "info",
                    "page_fetched",
                    url=url,
                    progress=f"{fetched}/{total}",
                    percentage=pct,
                )
            except Exception as exc:
                ctx.failed_pages.append(url)
                wait = backoff.record_failure(url, str(exc))
                if wait > 0:
                    time.sleep(wait)
                if backoff.should_restart_browser:
                    proxy_rot.force_rotate()
            # Garbage collect every 10 pages to keep memory bounded
            if i % 10 == 0:
                gc.collect()

    jlog(
        "info",
        "batch_fetch_scrapling_done",
        fetched=fetched,
        failed=total - fetched,
        total=total,
    )
    return fetched


def _batch_fetch_camoufox(
    ctx: ScrapeContext,
    cat: Any,
    urls: List[str],
    proxy_rot: _ProxyRotator,
    backoff: _BackoffTracker,
) -> int:
    """Core Camoufox implementation — reuses ONE browser across all *urls*.

    Restarts the browser session when:
    - Consecutive failures reach the backoff threshold
    - A proxy rotation is forced
    """
    from camoufox.sync_api import Camoufox

    total = len(urls)
    fetched = 0
    url_idx = 0  # tracks position across browser restarts
    _MAX_BROWSER_RESTARTS = 10  # cap to prevent infinite loop

    browser_restarts = 0
    while url_idx < total:
        proxy = proxy_rot.next() if proxy_rot.enabled else None
        cam_kwargs: Dict[str, Any] = {"headless": True}
        if proxy:
            cam_kwargs["proxy"] = {"server": proxy}
            jlog(
                "info", "browser_session_start", proxy=proxy, remaining=total - url_idx
            )
        else:
            jlog(
                "info",
                "browser_session_start",
                proxy="direct",
                remaining=total - url_idx,
            )

        # Remember position before browser starts — if browser creation
        # itself fails, we must still advance past the current URL to
        # prevent an infinite loop.
        idx_before = url_idx

        try:
            with Camoufox(**cam_kwargs) as browser:
                page = browser.new_page()
                browser_restarts = 0  # reset on successful launch

                while url_idx < total:
                    url = urls[url_idx]
                    url_idx += 1

                    if not _is_robots_allowed(ctx, url):
                        jlog("info", "blocked_by_robots", url=url)
                        ctx.ignored_pages.append(url)
                        continue
                    _apply_crawl_delay(ctx, url)

                    try:
                        page.goto(
                            url,
                            timeout=ctx.page_timeout * 1000,
                            wait_until="domcontentloaded",
                        )
                        html = page.content()

                        if _is_empty_content(html):
                            ctx.failed_pages.append(url)
                            wait = backoff.record_failure(url, "empty_content")
                            if wait > 0:
                                time.sleep(wait)
                            if backoff.should_restart_browser:
                                jlog(
                                    "warning",
                                    "browser_restart_threshold",
                                    consecutive_fails=backoff.consecutive_fails,
                                )
                                proxy_rot.force_rotate()
                                break  # restart browser with new proxy
                            continue

                        raw = html.encode("utf-8")
                        with ctx.contents_lock:
                            ctx.scraped_contents[url] = (raw, "text/html")

                        fetched += 1
                        backoff.record_success()
                        pct = round(fetched / total * 100, 1)
                        jlog(
                            "info",
                            "page_fetched",
                            url=url,
                            progress=f"{fetched}/{total}",
                            percentage=pct,
                        )

                    except Exception as exc:
                        ctx.failed_pages.append(url)
                        err_str = str(exc).lower()
                        is_rate_limit = "429" in err_str or "too many" in err_str
                        reason = "429_rate_limit" if is_rate_limit else str(exc)[:200]
                        wait = backoff.record_failure(url, reason)

                        if wait > 0:
                            time.sleep(wait)

                        # Try to recover the page object
                        try:
                            page.close()
                        except Exception:
                            pass
                        try:
                            page = browser.new_page()
                        except Exception as page_exc:
                            jlog(
                                "error",
                                "browser_page_recreate_failed",
                                error=str(page_exc),
                            )
                            break  # browser dead — restart

                        if backoff.should_restart_browser:
                            jlog(
                                "warning",
                                "browser_restart_threshold",
                                consecutive_fails=backoff.consecutive_fails,
                            )
                            proxy_rot.force_rotate()
                            break  # restart browser loop

                    # Garbage collect periodically
                    if fetched % 50 == 0 and fetched > 0:
                        gc.collect()

                try:
                    page.close()
                except Exception:
                    pass

        except Exception as exc:
            jlog(
                "error",
                "browser_session_crash",
                error=str(exc),
                fetched_before_crash=fetched,
                total=total,
                at_url_idx=url_idx,
            )
            # If browser creation itself failed, url_idx hasn't moved.
            # Skip the current URL to guarantee forward progress.
            if url_idx == idx_before and url_idx < total:
                current = urls[url_idx]
                if current not in ctx.failed_pages:
                    ctx.failed_pages.append(current)
                url_idx += 1
            else:
                # Inner loop ran at least once — mark the last URL.
                current = urls[url_idx - 1] if url_idx > 0 else None
                if (
                    current
                    and current not in ctx.scraped_contents
                    and current not in ctx.failed_pages
                ):
                    ctx.failed_pages.append(current)

            browser_restarts += 1
            if browser_restarts >= _MAX_BROWSER_RESTARTS:
                jlog(
                    "error",
                    "max_browser_restarts",
                    restarts=browser_restarts,
                    remaining=total - url_idx,
                )
                # Mark all remaining URLs as failed
                while url_idx < total:
                    u = urls[url_idx]
                    if u not in ctx.failed_pages:
                        ctx.failed_pages.append(u)
                    url_idx += 1
                break

            wait = backoff.record_failure(
                urls[url_idx - 1] if url_idx > 0 else "unknown", "browser_crash"
            )
            if wait > 0:
                time.sleep(wait)
            proxy_rot.force_rotate()

    return fetched


def _batch_fetch_standard(ctx: ScrapeContext, cat: Any, urls: List[str]) -> int:
    """Fetch URLs with plain ``requests`` + thread-local sessions."""
    total = len(urls)
    fetched = 0
    proxy_rot = _build_proxy_rotator(ctx)
    backoff = _BackoffTracker(ctx)

    jlog(
        "info",
        "batch_fetch_start",
        engine="standard",
        total=total,
        proxy_enabled=proxy_rot.enabled,
    )

    for i, url in enumerate(urls, 1):
        if not _is_robots_allowed(ctx, url):
            jlog("info", "blocked_by_robots", url=url)
            ctx.ignored_pages.append(url)
            continue
        _apply_crawl_delay(ctx, url)

        proxy = proxy_rot.next()
        try:
            session = _get_session(ctx.user_agent, proxy)
            resp = session.get(url, timeout=ctx.page_timeout)

            if resp.status_code == 429:
                ctx.failed_pages.append(url)
                wait = backoff.record_failure(url, "429_rate_limit")
                if wait > 0:
                    time.sleep(wait)
                if backoff.should_restart_browser:
                    proxy_rot.force_rotate()
                continue

            if resp.status_code == 200:
                ctype = resp.headers.get("Content-Type", "text/html").split(";")[0]
                html_text = resp.text

                if _is_empty_content(html_text):
                    ctx.failed_pages.append(url)
                    wait = backoff.record_failure(url, "empty_content")
                    if wait > 0:
                        time.sleep(wait)
                    if backoff.should_restart_browser:
                        proxy_rot.force_rotate()
                    continue

                backoff.record_success()
                with ctx.contents_lock:
                    ctx.scraped_contents[url] = (resp.content, ctype)
                fetched += 1
                pct = round(fetched / total * 100, 1)
                jlog(
                    "info",
                    "page_fetched",
                    url=url,
                    progress=f"{fetched}/{total}",
                    percentage=pct,
                )
            else:
                ctx.failed_pages.append(url)
                jlog("warning", "http_error", url=url, status=resp.status_code)
                backoff.record_failure(url, f"http_{resp.status_code}")
        except Exception as exc:
            ctx.failed_pages.append(url)
            wait = backoff.record_failure(url, str(exc)[:200])
            if wait > 0:
                time.sleep(wait)

        if i % 100 == 0:
            gc.collect()

    jlog(
        "info",
        "batch_fetch_standard_done",
        fetched=fetched,
        failed=total - fetched,
        total=total,
    )
    return fetched


# ── URL extraction & validation ──────────────────────────────────────


def _extract_valid_urls(
    raw_urls: List[str], page: str, ctx: ScrapeContext
) -> List[str]:
    """Filter and normalise a list of raw hrefs found on *page*."""
    valid: List[str] = []
    for href in raw_urls:
        if "#" in href:
            href = href.split("#")[0]
        if not href:
            continue

        if href.startswith(("http://", "https://")):
            full_url = href
        else:
            full_url = urllib.parse.urljoin(page, href)

        parsed = urllib.parse.urlparse(full_url)
        domain = normalize_domain(parsed.netloc)

        is_root = domain in ctx.root_domains
        is_allowed = domain in ctx.allowed_domains

        if not (is_root or is_allowed):
            continue

        # Path scope (root domains only — per-domain)
        if is_root and ctx.allowed_paths:
            domain_paths = ctx.allowed_paths.get(domain, set())
            if domain_paths and not any(
                parsed.path.startswith(p) for p in domain_paths
            ):
                continue

        if ctx.skip_get_params and parsed.query:
            continue

        low = full_url.lower()
        if ctx.skip_extensions and low.endswith(tuple(ctx.skip_extensions)):
            continue

        # PDFs
        if low.endswith(".pdf"):
            if ctx.ingest_pdf:
                with ctx.visited_lock:
                    if full_url not in ctx.visited_pages:
                        ctx.visited_pages.add(full_url)
                        with ctx.scraped_lock:
                            ctx.scraped_pages.append(full_url)
            continue

        valid.append(full_url)
    return valid


# ── Per-page crawl (BFS mode) ───────────────────────────────────────


def _crawl_page(
    ctx: ScrapeContext, cat: Any, url: str, depth: int
) -> List[Tuple[str, int]]:
    """Fetch a single page. Returns list of ``(child_url, depth+1)`` for recursion."""
    with ctx.visited_lock:
        if url in ctx.visited_pages:
            return []
        ctx.visited_pages.add(url)

    # robots.txt check
    if not _is_robots_allowed(ctx, url):
        jlog("info", "blocked_by_robots", url=url)
        return []

    _apply_crawl_delay(ctx, url)

    new_urls: List[Tuple[str, int]] = []

    try:
        if ctx.use_scrapling and _check_scrapling():
            html_text, raw_bytes, ctype = _fetch_with_scrapling_single(
                url, ctx.page_timeout
            )
            if ctx.cache_content:
                with ctx.contents_lock:
                    ctx.scraped_contents[url] = (raw_bytes, ctype)
        else:
            session = _get_session(ctx.user_agent)
            resp = session.get(url, timeout=ctx.page_timeout)
            if resp.status_code != 200:
                jlog("warning", "http_error", url=url, status=resp.status_code)
                return []
            html_text = resp.text
            ctype = resp.headers.get("Content-Type", "text/html").split(";")[0]
            if ctx.cache_content:
                with ctx.contents_lock:
                    ctx.scraped_contents[url] = (resp.content, ctype)

        soup = BeautifulSoup(html_text, "html.parser")

        with ctx.scraped_lock:
            ctx.scraped_pages.append(url)
            count = len(ctx.scraped_pages)

        # Throttled progress
        if not ctx.scheduled:
            with ctx.update_lock:
                now = time.time()
                if now - ctx.last_update_time > 0.5:
                    ctx.last_update_time = now
                    try:
                        cat.send_ws_message(f"Scraped {count} pages — currently: {url}")
                    except Exception:
                        pass

        jlog("info", "crawl_page_ok", url=url, depth=depth, total_scraped=count)

        # If only_sitemap, skip link extraction entirely
        if ctx.only_sitemap:
            return []

        hrefs = [a["href"] for a in soup.select("a[href]")]
        valid = _extract_valid_urls(hrefs, url, ctx)

        for child_url in valid:
            child_parsed = urllib.parse.urlparse(child_url)
            child_domain = normalize_domain(child_parsed.netloc)

            if child_domain in ctx.root_domains:
                # Recursive
                if child_url not in ctx.visited_pages:
                    if ctx.max_depth == -1 or (depth + 1) <= ctx.max_depth:
                        new_urls.append((child_url, depth + 1))
            elif child_domain in ctx.allowed_domains:
                # Single-page
                with ctx.visited_lock:
                    if child_url not in ctx.visited_pages:
                        ctx.visited_pages.add(child_url)
                        with ctx.scraped_lock:
                            ctx.scraped_pages.append(child_url)

    except Exception as exc:
        jlog("error", "crawl_page_failed", url=url, error=str(exc))
        ctx.failed_pages.append(url)

    return new_urls


# ── Main BFS entry point ────────────────────────────────────────────


def crawl(ctx: ScrapeContext, cat: Any, start_urls: List[str]) -> None:
    """BFS-crawl *start_urls* using a :class:`ThreadPoolExecutor`. Populates
    ``ctx.scraped_pages`` and ``ctx.failed_pages`` in place."""
    jlog("info", "bfs_crawl_start", start_urls=start_urls, max_workers=ctx.max_workers)

    # ── Timeout tuning ───────────────────────────────────────────
    # wait() timeout must be generous compared to per-page timeout
    # because workers have startup overhead and the page_timeout
    # only covers the HTTP request itself.  Give at least 3× the
    # page timeout so a slow-but-still-alive page doesn't trigger
    # a catastrophic cancel of all queued work.
    _wait_timeout = max(ctx.page_timeout * 3, 120)
    _consecutive_stalls = 0
    _MAX_STALLS = 3

    with ThreadPoolExecutor(max_workers=ctx.max_workers) as pool:
        futures: Dict[Any, Tuple[str, int]] = {}

        for url in start_urls:
            f = pool.submit(_crawl_page, ctx, cat, url, 0)
            futures[f] = (url, 0)

        while futures:
            with ctx.visited_lock:
                if ctx.max_pages != -1 and len(ctx.visited_pages) >= ctx.max_pages:
                    jlog("info", "max_pages_reached", limit=ctx.max_pages)
                    for f in futures:
                        f.cancel()
                    break

            done, _ = wait(
                futures.keys(), timeout=_wait_timeout, return_when=FIRST_COMPLETED
            )

            if not done:
                _consecutive_stalls += 1
                jlog(
                    "warning",
                    "bfs_stall",
                    remaining=len(futures),
                    stall=_consecutive_stalls,
                    max_stalls=_MAX_STALLS,
                )
                if _consecutive_stalls >= _MAX_STALLS:
                    jlog("warning", "bfs_timeout", remaining=len(futures))
                    for f in list(futures):
                        f.cancel()
                        url, _ = futures.pop(f)
                        ctx.failed_pages.append(url)
                    break
                continue

            _consecutive_stalls = 0  # Made progress — reset stall counter.

            for f in done:
                if f not in futures:
                    continue
                url, depth = futures.pop(f)
                try:
                    children = f.result()
                    for child_url, child_depth in children:
                        with ctx.visited_lock:
                            if (
                                ctx.max_pages != -1
                                and len(ctx.visited_pages) >= ctx.max_pages
                            ):
                                break
                        if ctx.max_depth == -1 or child_depth <= ctx.max_depth:
                            nf = pool.submit(
                                _crawl_page, ctx, cat, child_url, child_depth
                            )
                            futures[nf] = (child_url, child_depth)
                except Exception as exc:
                    jlog("error", "bfs_future_failed", url=url, error=str(exc))
                    ctx.failed_pages.append(url)

    jlog(
        "info",
        "bfs_crawl_done",
        scraped=len(ctx.scraped_pages),
        failed=len(ctx.failed_pages),
    )
