"""Lightweight content-hash tracking database (plain sqlite3).

Replaces the SQLAlchemy-based Dietician DB with a simpler, faster approach
using Python's built-in ``sqlite3`` module.  The schema stores:

* **url** -  the canonical source URL
* **content_hash** -  SHA-256 hex digest of the full page content (pre-chunking)
* **chunk_count** -  number of chunks produced at last ingestion
* **last_seen** -  epoch timestamp of last successful scrape
* **session_id** -  session that last touched this URL

This database enables:
1. Skip ingestion when content hash is unchanged.
2. Detect content changes → delete stale chunks, ingest only new ones.
3. Track which URLs belong to which scraping session for cleanup.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import time
from typing import Dict, List, Optional, Set, Tuple

from ..utils.log_utils import jlog

# ── Schema ───────────────────────────────────────────────────────────

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS documents (
    url          TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    chunk_count  INTEGER NOT NULL DEFAULT 0,
    last_seen    REAL NOT NULL,
    session_id   TEXT NOT NULL,
    miss_count   INTEGER NOT NULL DEFAULT 0
);
"""

_IDX_SQL = """
CREATE INDEX IF NOT EXISTS idx_session ON documents(session_id);
"""


# ── Helpers ──────────────────────────────────────────────────────────


def content_hash(text: str) -> str:
    """Return the SHA-256 hex digest of *text*."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class HashDB:
    """Minimal SQLite wrapper for the content-hash tracking DB."""

    def __init__(self, db_path: str):
        self._path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")  # faster concurrent reads
        self._conn.execute(_CREATE_SQL)
        self._conn.execute(_IDX_SQL)
        # Migrate older databases that lack the miss_count column.
        try:
            self._conn.execute(
                "ALTER TABLE documents ADD COLUMN miss_count INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass  # column already exists
        self._conn.commit()

    # ── Queries ──────────────────────────────────────────────────

    def get(self, url: str) -> Optional[Dict]:
        """Return row as dict or ``None``."""
        row = self._conn.execute(
            "SELECT url, content_hash, chunk_count, last_seen, session_id, miss_count FROM documents WHERE url = ?",
            (url,),
        ).fetchone()
        if row is None:
            return None
        return {
            "url": row[0],
            "content_hash": row[1],
            "chunk_count": row[2],
            "last_seen": row[3],
            "session_id": row[4],
            "miss_count": row[5],
        }

    def upsert(
        self, url: str, hash_val: str, chunk_count: int, session_id: str
    ) -> None:
        """Insert or update a document record."""
        self._conn.execute(
            """INSERT INTO documents (url, content_hash, chunk_count, last_seen, session_id, miss_count)
               VALUES (?, ?, ?, ?, ?, 0)
               ON CONFLICT(url) DO UPDATE SET
                   content_hash = excluded.content_hash,
                   chunk_count  = excluded.chunk_count,
                   last_seen    = excluded.last_seen,
                   session_id   = excluded.session_id,
                   miss_count   = 0""",
            (url, hash_val, chunk_count, time.time(), session_id),
        )
        self._conn.commit()

    def touch(self, url: str, session_id: str) -> None:
        """Update ``last_seen`` and ``session_id`` without changing hash."""
        self._conn.execute(
            "UPDATE documents SET last_seen = ?, session_id = ?, miss_count = 0 WHERE url = ?",
            (time.time(), session_id, url),
        )
        self._conn.commit()

    def delete(self, url: str) -> None:
        self._conn.execute("DELETE FROM documents WHERE url = ?", (url,))
        self._conn.commit()

    def increment_miss_count(self, url: str) -> int:
        """Increment ``miss_count`` for *url* and return the new value."""
        self._conn.execute(
            "UPDATE documents SET miss_count = miss_count + 1 WHERE url = ?",
            (url,),
        )
        self._conn.commit()
        row = self._conn.execute(
            "SELECT miss_count FROM documents WHERE url = ?", (url,)
        ).fetchone()
        return row[0] if row else 0

    def urls_for_command_not_in(
        self, session_id: str, keep_urls: Set[str]
    ) -> List[str]:
        """Return URLs that were last seen BEFORE this session for the given
        command pattern -  i.e. stale URLs to clean up.

        We identify "same logical scrape" by looking at rows whose
        ``session_id`` differs from the current one.  The caller passes
        ``keep_urls`` (union of scraped + ignored + failed) to exclude.
        """
        rows = self._conn.execute(
            "SELECT url FROM documents WHERE session_id != ?",
            (session_id,),
        ).fetchall()
        return [r[0] for r in rows if r[0] not in keep_urls]

    def all_urls(self) -> List[str]:
        return [
            r[0] for r in self._conn.execute("SELECT url FROM documents").fetchall()
        ]

    def close(self) -> None:
        self._conn.close()

    def destroy(self) -> None:
        """Close and delete the database file."""
        self.close()
        if os.path.exists(self._path):
            os.remove(self._path)
            jlog("info", "hash_db_destroyed", path=self._path)
