"""Shared helpers for date-based fallback queries.

Weekly shows that release as date-named torrents (e.g. WWE SmackDown, which
never gets a real S##E## tag from a classifier) need free-text search
queries built from the episode's air date instead of season/episode
numbers. These helpers are pure functions (no I/O) shared by the
BitMagnet, Zilean, and Jackett scrapers.
"""

import re
from datetime import date, timedelta

_PUNCTUATION_RE = re.compile(r"[!?:'\",.()\[\]&]")
_WHITESPACE_RE = re.compile(r"\s+")


def clean_query_title(title: str) -> str:
    """Strip punctuation that breaks full-text search engines and collapse
    whitespace. "WWE Smackdown!" -> "WWE Smackdown"."""
    if not title:
        return ""
    stripped = _PUNCTUATION_RE.sub("", title)
    return _WHITESPACE_RE.sub(" ", stripped).strip()


def air_date_variants(air_date: str) -> list[str]:
    """Return ["YYYY-MM-DD" for air_date, air_date-1, air_date+1] as ISO
    strings, to tolerate the +/-1 day timezone drift between TMDB's stored
    air date and the real release date. Invalid input -> []."""
    if not air_date:
        return []
    try:
        d = date.fromisoformat(air_date)
    except (ValueError, TypeError):
        return []
    return [
        d.isoformat(),
        (d - timedelta(days=1)).isoformat(),
        (d + timedelta(days=1)).isoformat(),
    ]


def build_date_queries(
    title: str,
    air_date: str | None,
    season: int | None,
    episode: int | None,
) -> list[str]:
    """Build a deduped, ordered list of free-text search queries for a
    date-named episode: one per air-date variant (air_date, air_date-1,
    air_date+1) using dot-separated dates, plus a "S##E##" query when
    season/episode are known. Empty list if air_date is None."""
    if air_date is None:
        return []

    clean = clean_query_title(title)

    queries = [
        f"{clean} {variant.replace('-', '.')}" for variant in air_date_variants(air_date)
    ]

    if season is not None and episode is not None:
        queries.append(f"{clean} S{season:02d}E{episode:02d}")

    seen = set()
    deduped = []
    for query in queries:
        if query not in seen:
            seen.add(query)
            deduped.append(query)
    return deduped
