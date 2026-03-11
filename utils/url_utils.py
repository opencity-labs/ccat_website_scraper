"""URL normalisation and validation helpers."""

import re
import random
import urllib.parse


def clean_url(url: str) -> str:
    """Strip whitespace, trailing slashes and fragments."""
    return url.strip().rstrip("/").split("#")[0]


def normalize_url_with_protocol(url: str) -> str:
    """Ensure *url* starts with a scheme; default to ``https://``."""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        return f"https://{url}"
    return url


def normalize_domain(domain_or_url: str) -> str:
    """Return a canonical domain string (lowercase, no www, no scheme).

    >>> normalize_domain("https://www.Example.COM/path")
    'example.com'
    """
    domain = domain_or_url.strip().lower()
    if domain.startswith(("http://", "https://")):
        domain = urllib.parse.urlparse(domain).netloc
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


_DOMAIN_RE = re.compile(r"^([a-z0-9-]+\.)+[a-z]{2,}(/[^\s]*)?$", re.IGNORECASE)
_URL_RE = re.compile(r"^https?://([a-z0-9-]+\.)+[a-z]{2,}(/[^\s]*)?$", re.IGNORECASE)


def validate_url(url: str) -> bool:
    """Return ``True`` when *url* looks like a valid URL or bare domain."""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        return bool(_DOMAIN_RE.match(url))
    return bool(_URL_RE.match(url))


def random_sleep(max_seconds: float, min_seconds: float = 0.0) -> float:
    """Return a random sleep duration in 0.1-second increments."""
    if max_seconds <= 0:
        return 0.0
    lo = int(max(0, min_seconds) * 10)
    hi = int(max_seconds * 10)
    return random.randint(lo, hi) / 10.0
