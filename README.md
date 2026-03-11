# Cheshire Cat Website Scraper Plugin

[![Cheshire Cat](https://custom-icon-badges.demolab.com/static/v1?label=&message=Website+Scraper&color=383938&style=for-the-badge&logo=cheshire_cat_ai)](https://)

A full‑featured web crawler and ingest pipeline for Cheshire Cat AI.  Point it at one or more seed URLs and the plugin will:

* **crawl** the site with polite rate‑limiting, robots.txt / sitemap support, and optional JavaScript rendering
* **deduplicate** content using a lightweight hash database so unchanged pages are skipped
* **ingest** HTML (and PDFs) into Cheshaire Cat memory with configurable chunking and quality filters
* **schedule** scraping runs automatically via WhiteRabbit’s scheduler
* **clean up** stale memories and provide an API for deleting memories by source
* **customize** parsing with hidden‑element stripping and proxy rotation to avoid detection

This repository holds the plugin code; use `setup.py` to bootstrap new copies and manage releases.

---

## Features

| Capability | Description |
|------------|-------------|
| Crawling | BFS crawler with depth/page limits, link filtering, sitemap support, and optional link parameter skipping. |
| Engines | `standard` (requests + BeautifulSoup) or `scrapling` (Scrapling + Camoufox for JS pages). |
| Rate control | Configurable request delay, adaptive backoff on 429/empty, batch pausing, and optional robots.txt crawl‑delay. |
| Anti‑detection | Proxy rotation, custom User‑Agent, random delays. |
| Deduplication | SQLite database stores content hashes; pages unchanged since last run are skipped. |
| Chunking & Filters | Adjustable chunk size/overlap, minimum length, hidden‑element stripping, PDF ingestion. |
| Scheduling | Integrates with WhiteRabbit to run daily / hourly / custom cron jobs. |
| API | Endpoint `DELETE /memory/scraper-delete-by-source` for bulk memory removal by source URL. |

The rest of this README covers installation, configuration, usage, and development tips.

---

## Installation

1. **Clone plugin** into the cat `plugins` directory:
   ```bash
   git clone https://github.com/yourorg/ccat_website_scraper.git \
     /path/to/cheshire_cat/plugins/ccat_website_scraper
   ```
2. **Run setup** to initialize names/metadata:
   ```bash
   cd /path/to/cheshire_cat/plugins/ccat_website_scraper
   python setup.py
   ```
3. **Install requirements** (if not already):
   ```bash
   pip install -r requirements.txt
   ```
4. **Restart Cheshire Cat** or reload plugins to load the scraper code.

> A new release is published any time you bump `version` in `plugin.json`.

---

## Configuration

All settings are stored via Cheshire Cat’s plugin UI or in `settings.json` adjacent to the plugin code.  The schema is defined in `settings.py` and includes:

* **Targets** – `starting_urls`, `allowed_extra_domains`.
* **Crawling** – depth, max pages, workers, delays, timeouts, skip GET params, ingest PDFs, skip extensions, cache content.
* **Robots / Sitemap** – follow robots.txt, obey crawl delay, fetch sitemap, only use sitemap.
* **Chunking & Filtering** – `chunk_size`, `chunk_overlap`, `ignore_display_none`, `min_chunk_length`.
* **Deduplication** – `db_path`, `delete_db`.
* **Retry & cleanup** – `retry_failed_urls`, `max_retry_attempts`, `retry_delay_seconds`.
* **Scheduling** – enable, mode (`daily_at`, `every_n_hours`, `every_n_days_at`, `cron`), time/interval, cron expression.
* **Anti‑detection** – `batch_size`, `batch_pause_seconds`, `adaptive_backoff`, backoff parameters.
* **Proxy** – `proxy_enabled`, `proxy_list`, `proxy_rotate_every`.
* **Identity** – `user_agent` string.

Settings API is merged with any existing `settings.json` when the user saves via the plugin UI.



---

## Running a Scrape

Trigger a manual run from the Cheshire Cat web UI or interactively:
```python
from cat.looking_glass.cheshire_cat import CheshireCat
from cat.plugins import get_plugin  # plugin loader

ccat = CheshireCat()
run_scrape = get_plugin("website_scraper").run_scrape
summary = run_scrape(ccat)
print(summary)
```

Output sample:
```
10 URLs ingested, 2 skipped (unchanged), 1 failed, 3 stale removed, 4.2 min
```

### Scheduling
Enable `schedule_enabled` in settings and choose a mode.  The plugin installs hooks during `after_cat_bootstrap` to keep the WhiteRabbit schedule in sync.  Modes include daily at a time, every N hours, every N days at a time, or a custom cron expression.

---

## Memory Management & API

* **Dedup** – a SQLite hash database at `db_path` tracks page hashes and last‑seen timestamps.  Unchanged pages are skipped, and stale memory entries are purged after each run.
* **Manual cleanup** – set `delete_db` to true and save settings to wipe the hash store.
* **API endpoint** – send a `DELETE` request to `/memory/scraper-delete-by-source` with JSON `{ "source": "https://site.example/page" }` to remove all declarative memories originating from that source.  Permissions are checked automatically.

```bash
curl -X DELETE https://your-ccat/api/plugins/websitescraper/memory/scraper-delete-by-source \
  -H 'Content-Type: application/json' \
  -d '{"source":"https://example.com"}'
```

---

## Development & Testing

* Code lives under `scraper.py`, `core/` and `utils/`.
* `core/` contains the crawler, hash DB, memory managers and context objects.
* `utils/` provides URL cleaning, robots.txt handling, sitemap parsing and structured JSON logging.  All logging in the plugin uses `jlog` to emit structured JSON.
* Hooks extend Cheshire Cat: ingest metadata propagation, parser swapping, chunk filtering, and schedule setup.
* Run `pytest` in the plugin root (if tests exist) or manually import and exercise `run_scrape`.

When editing settings add validators in `settings.py` to enforce ranges.

---

## Troubleshooting

* **No URLs scraped** – ensure `starting_urls` are valid and reachable. The plugin returns an error string if none are found.
* **Too many 429s / bans** – enable proxies, increase delays, lower batch size, or throttle with adaptive backoff.
* **Memory bloat** – reduce `batch_size` or increase `batch_pause_seconds`.  Cleanup runs automatically but you can purge `db_path` and restart.
* **Sitemap not fetched** – set `use_sitemap` false or check that the target site’s sitemap exists and is reachable.

Check the Cheshire Cat logs; all output is structured JSON with events such as `scrape_start`, `ingest_ok`, `batch_done`, etc.

---

## Release & Versioning

Modify `plugin.json` version and push a tag to trigger automated release pipelines.  See the template README for more details.

---


---

Happy scraping!
