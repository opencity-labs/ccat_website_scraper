"""Settings model for the Websites Scraper plugin."""

from enum import Enum
from pydantic import BaseModel, Field, validator
from cat.mad_hatter.decorators import plugin


class ScheduleMode(str, Enum):
    """How the scheduled job should repeat."""

    DAILY_AT = "daily_at"  # every day at HH:MM
    EVERY_N_HOURS = "every_n_hours"  # every N hours
    EVERY_N_DAYS_AT = "every_n_days_at"  # every N days at HH:MM
    CRON = "cron"  # full cron expression


class ScrapingEngine(str, Enum):
    """Which scraping backend to use."""

    STANDARD = "standard"  # requests + BeautifulSoup (lightweight)
    SCRAPLING = "scrapling"  # Scrapling with Camoufox (JS rendering)


class PluginSettings(BaseModel):
    """Websites Scraper — unified settings."""

    # ── Targets ──────────────────────────────────────────────────────
    starting_urls: str = Field(
        default="",
        title="Starting URLs",
        description="Comma-separated list of URLs to scrape (e.g. https://example.com, https://docs.example.com/guide).",
    )
    allowed_extra_domains: str = Field(
        default="",
        title="Allowed extra domains (single-page only)",
        description="Comma-separated domains/URLs whose pages are ingested when linked from a starting URL, but NOT crawled recursively.",
    )

    # ── Scraping engine ──────────────────────────────────────────────
    scraping_engine: ScrapingEngine = Field(
        default=ScrapingEngine.STANDARD,
        title="Scraping engine",
        description="'standard' uses requests + BS4 (fast, lightweight). 'scrapling' uses Scrapling with Camoufox for JS-rendered pages.",
    )

    # ── Crawling behaviour ───────────────────────────────────────────
    max_depth: int = Field(
        default=-1,
        title="Maximum crawling depth",
        description="-1 for unlimited. Limits how many links deep the crawler will go from the starting URL.",
    )
    max_pages: int = Field(
        default=-1,
        title="Maximum pages to crawl",
        description="-1 for unlimited.",
    )
    max_workers: int = Field(
        default=4,
        title="Concurrent workers",
        description="Number of threads for parallel crawling.",
    )
    request_delay: float = Field(
        default=0.5,
        title="Request delay (seconds)",
        description="Random delay up to this value between requests (per worker).",
    )
    page_timeout: int = Field(
        default=30,
        title="Page timeout (seconds)",
        description="Maximum time to wait for a single page load.",
    )
    skip_get_params: bool = Field(
        default=False,
        title="Skip URLs with GET parameters",
        description="Ignore URLs containing query strings (?key=val).",
    )
    ingest_pdf: bool = Field(
        default=False,
        title="Ingest linked PDFs",
        description="Download and ingest PDF files found during crawling.",
    )
    skip_extensions: str = Field(
        default=".jpg,.jpeg,.png,.gif,.bmp,.svg,.webp,.ico,.zip,.ods,.odt,.xls,.p7m,.rar,.mp3,.xml,.7z,.exe,.doc,.m4a,.crdownload,.odp,.ppt,.pptx",
        title="File extensions to skip",
        description="Comma-separated list.",
    )
    cache_content: bool = Field(
        default=True,
        title="Cache fetched content",
        description="Reuse HTML fetched during crawling at ingestion time, avoiding a second request per page.",
    )

    # ── robots.txt ───────────────────────────────────────────────────
    follow_robots_txt: bool = Field(
        default=True,
        title="Follow robots.txt",
        description="Respect robots.txt Allow/Disallow and Crawl-delay directives.",
    )
    obey_crawl_delay: bool = Field(
        default=True,
        title="Obey Crawl-delay from robots.txt",
        description="When enabled and robots.txt specifies a Crawl-delay for our user agent, that delay is used instead of the configured request_delay.",
    )

    # ── Sitemap ──────────────────────────────────────────────────────
    use_sitemap: bool = Field(
        default=True,
        title="Fetch sitemap.xml",
        description="Discover URLs via sitemap.xml before crawling.",
    )
    only_sitemap: bool = Field(
        default=False,
        title="Only scrape URLs from sitemap",
        description="When enabled, ONLY URLs found in sitemap.xml are scraped — the crawler will not follow links on pages.",
    )

    # ── Text chunking ────────────────────────────────────────────────
    chunk_size: int = Field(
        default=1024,
        title="Chunk size",
        description="Character count per text chunk for embedding.",
    )
    chunk_overlap: int = Field(
        default=256,
        title="Chunk overlap",
        description="Character overlap between consecutive chunks.",
    )

    # ── Content filtering ────────────────────────────────────────────
    ignore_display_none: bool = Field(
        default=False,
        title="Strip display:none elements",
        description="Remove hidden <div> elements before parsing HTML.",
    )
    min_chunk_length: int = Field(
        default=50,
        title="Minimum chunk length",
        description="Discard chunks shorter than this (characters).",
    )

    # ── Deduplication & memory ───────────────────────────────────────
    db_path: str = Field(
        default="cat/data/websites_scraper.db",
        title="SQLite database path",
        description="Path for the content-hash tracking database. Change only if you know what you are doing.",
    )
    delete_db: bool = Field(
        default=False,
        title="Delete tracking database",
        description="Set to True, save, and the database file will be deleted. Cannot be undone.",
    )

    # ── Retry logic ──────────────────────────────────────────────────
    retry_failed_urls: bool = Field(
        default=True,
        title="Retry failed URLs",
        description="Re-attempt ingestion for URLs that failed during the scraping session.",
    )
    max_retry_attempts: int = Field(
        default=3,
        title="Max retry attempts",
        description="How many times to retry a transiently-failed URL.",
    )
    retry_delay_seconds: int = Field(
        default=5,
        title="Retry delay (seconds)",
        description="Wait time between retry rounds.",
    )

    # ── Scheduling ───────────────────────────────────────────────────
    schedule_enabled: bool = Field(
        default=False,
        title="Enable scheduled scraping",
        description="Run the scraping job automatically on a schedule.",
    )
    schedule_mode: ScheduleMode = Field(
        default=ScheduleMode.DAILY_AT,
        title="Schedule mode",
        description="How the schedule repeats. 'daily_at': every day at the given time. 'every_n_hours': repeats every N hours. 'every_n_days_at': every N days at the given time. 'cron': full cron expression.",
    )
    schedule_hour: int = Field(
        default=3,
        title="Schedule hour (UTC, 0-23)",
        description="Hour for daily_at / every_n_days_at modes.",
    )
    schedule_minute: int = Field(
        default=0,
        title="Schedule minute (0-59)",
        description="Minute for daily_at / every_n_days_at modes.",
    )
    schedule_interval_hours: int = Field(
        default=6,
        title="Interval hours",
        description="For every_n_hours mode: run every N hours.",
    )
    schedule_interval_days: int = Field(
        default=1,
        title="Interval days",
        description="For every_n_days_at mode: run every N days.",
    )
    schedule_cron_expression: str = Field(
        default="",
        title="Cron expression",
        description="For cron mode: APScheduler cron fields as 'minute hour day month day_of_week' (e.g. '0 3 */2 * *' = 03:00 every 2 days).",
    )

    # ── Anti-detection & rate-limiting ────────────────────────────────
    batch_size: int = Field(
        default=50,
        title="Fetch batch size",
        description="How many URLs to fetch + ingest in one pass before freeing memory and pausing. Lower = safer against rate-limiting.",
    )
    batch_pause_seconds: float = Field(
        default=5.0,
        title="Pause between batches (seconds)",
        description="Sleep this many seconds between consecutive batches. Helps avoid detection on large sitemaps.",
    )
    adaptive_backoff: bool = Field(
        default=True,
        title="Adaptive backoff on 429 / empty pages",
        description="When enabled, automatically increases delay and pauses when the server returns 429 or empty content.",
    )
    backoff_initial_seconds: float = Field(
        default=10.0,
        title="Backoff initial wait (seconds)",
        description="First backoff pause when a 429 or empty page is detected.",
    )
    backoff_max_seconds: float = Field(
        default=120.0,
        title="Backoff maximum wait (seconds)",
        description="Cap for exponential backoff pauses.",
    )
    backoff_threshold: int = Field(
        default=3,
        title="Consecutive failures before long pause",
        description="After this many consecutive 429s or empty pages, pause for backoff_max_seconds and restart the browser (Scrapling).",
    )

    # ── Proxy ─────────────────────────────────────────────────────────
    proxy_enabled: bool = Field(
        default=False,
        title="Enable proxy rotation",
        description="Route requests through proxies. One proxy per line in the list below.",
    )
    proxy_list: str = Field(
        default="",
        title="Proxy list",
        description=(
            "One proxy per line. Formats: http://host:port, socks5://host:port, "
            "http://user:pass@host:port. Proxies are rotated round-robin."
        ),
    )
    proxy_rotate_every: int = Field(
        default=10,
        title="Rotate proxy every N pages",
        description="Switch to the next proxy after this many fetched pages. 0 = use the same proxy for the whole batch.",
    )

    # ── Identity ─────────────────────────────────────────────────────
    user_agent: str = Field(
        default="CheshireCatAI WebsitesScraper/1.0",
        title="User-Agent string",
        description="Sent with every HTTP request. Sites may use this to identify the bot.",
    )

    # ── Validators ───────────────────────────────────────────────────
    @validator("page_timeout")
    def _page_timeout(cls, v):
        if not 5 <= v <= 300:
            raise ValueError("Must be 5-300.")
        return v

    @validator("schedule_hour")
    def _schedule_hour(cls, v):
        if not 0 <= v <= 23:
            raise ValueError("Must be 0-23.")
        return v

    @validator("schedule_minute")
    def _schedule_minute(cls, v):
        if not 0 <= v <= 59:
            raise ValueError("Must be 0-59.")
        return v

    @validator("max_retry_attempts")
    def _max_retry(cls, v):
        if not 1 <= v <= 10:
            raise ValueError("Must be 1-10.")
        return v

    @validator("batch_size")
    def _batch_size(cls, v):
        if not 1 <= v <= 1000:
            raise ValueError("Must be 1-1000.")
        return v


@plugin
def settings_model():
    return PluginSettings
