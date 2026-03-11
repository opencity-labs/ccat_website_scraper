"""Structured JSON logging helper for the Websites Scraper plugin.

All log lines are emitted as JSON objects with a consistent schema::

    {"component": "ccat_websites_scraper", "event": "<event_name>", "data": {…}}

This makes it easy to filter, parse and aggregate logs in ELK, Loki,
CloudWatch, or any other log management system.
"""

from __future__ import annotations

import json
from typing import Any

from cat.log import log as _cat_log

_COMPONENT = "ccat_websites_scraper"


def jlog(level: str, event: str, **data: Any) -> None:
    """Emit a structured JSON log line.

    Parameters
    ----------
    level : str
        Log level — one of ``"debug"``, ``"info"``, ``"warning"``, ``"error"``,
        ``"critical"``.
    event : str
        Machine-readable event name (e.g. ``"page_fetched"``, ``"ingest_skip"``).
    **data
        Arbitrary key-value pairs to include in the ``data`` payload.

    Example
    -------
    >>> jlog("info", "page_fetched", url="https://…", progress="3/100", percentage=3.0)
    """
    getattr(_cat_log, level)(
        json.dumps({"component": _COMPONENT, "event": event, "data": data})
    )
