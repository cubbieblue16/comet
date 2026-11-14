"""Wrong-show identity gate.

The title filter is deliberately loose (a release titled ``Ted Lasso`` passes
for the series ``Ted``).  This gate rejects a release only when its parsed
show name is POSITIVELY identified as a different series: a catalog lookup
returns a series whose scrubbed name EQUALS the scrubbed parsed title and
whose IMDb id differs from the requested one.  Anything that cannot be
identified (no exact hit, lookup failure, timeout, budget spent, gate off)
is kept exactly as before: the gate fails open everywhere.
"""

import asyncio
import re
import time
from collections import Counter, OrderedDict
from urllib.parse import quote

import aiohttp

from comet.core.logger import logger
from comet.core.models import database, settings
from comet.services.filtering import _log_exclusion, scrub
from comet.utils.http_client import http_client_manager

_CINEMETA_SEARCH_URL = "https://v3-cinemeta.strem.io/catalog/series/top/search={query}.json"
_IMDB_ID = re.compile(r"tt\d+")
_ARTICLE = re.compile(r"^(?:the|a|an) ")
# first season/episode/year marker in a release name; the text before it is the
# title head as the uploader wrote it
_HEAD_END = re.compile(
    r"(?<![a-z0-9])(?:s\d{1,2} ?e\d{1,3}|s\d{1,2}|\d{1,2}x\d{1,3}|season \d+|(?:19|20)\d{2})(?![a-z0-9])"
)
_COUNTRY = {"us"}  # the catalog default; UK/AU/... editions are other series

MAX_LOOKUPS = 30
CONCURRENCY = 8
LOOKUP_TIMEOUT = 2.5
BUDGET = 4.0
MAX_TITLE_LEN = 120

_POSITIVE_TTL = 30 * 86400
_NEGATIVE_TTL = 7 * 86400
_CACHE_MAX = 20000
_LOCAL_INDEX_TTL = 600

# scrubbed title -> (expires_at, ((imdb_id, name), ...) exact-name hits)
_cache: "OrderedDict[str, tuple[float, tuple]]" = OrderedDict()
_local_index: dict = {}
_local_index_at: float = 0.0


def reset_state():
    """Clear the process-wide caches (tests)."""
    global _local_index, _local_index_at
    _cache.clear()
    _local_index = {}
    _local_index_at = 0.0


def _no_article(key: str) -> str:
    return _ARTICLE.sub("", key, count=1)


def _canon(key: str) -> str:
    """Article-insensitive, with a trailing "usa" read as "us"."""
    key = _no_article(key)
    return key[:-1] if key.endswith(" usa") else key


def _release_head(title) -> list[str]:
    """Scrubbed title head of a release name (before season/episode/year) as
    match candidates: the head itself and, when it ends in "us", the head
    without it. A localized prefix in another script is ignored. [] when the
    name has no season/episode/year marker."""
    if not isinstance(title, str):
        return []
    text = re.sub(r"[._]+", " ", title).lower()
    match = _HEAD_END.search(text)
    if not match:
        return []
    words = scrub(text[: match.start()]).split()
    while words and not words[0].isascii():
        words.pop(0)
    if not words:
        return []
    heads = [" ".join(words)]
    if len(words) > 1 and words[-1] in _COUNTRY:
        heads.append(" ".join(words[:-1]))
    return heads


def _log_rejection(msg: str):
    _log_exclusion(msg)


def _cache_get(key: str):
    entry = _cache.get(key)
    if entry is None:
        return None
    if entry[0] <= time.time():
        _cache.pop(key, None)
        return None
    _cache.move_to_end(key)
    return entry[1]


def _cache_put(key: str, hits: tuple):
    ttl = _POSITIVE_TTL if hits else _NEGATIVE_TTL
    _cache[key] = (time.time() + ttl, hits)
    _cache.move_to_end(key)
    while len(_cache) > _CACHE_MAX:
        _cache.popitem(last=False)


async def _fetch_cinemeta(title: str) -> list[tuple[str, str]]:
    """Cinemeta series search -> [(imdb_id, name)]. Raises on any failure."""
    url = _CINEMETA_SEARCH_URL.format(query=quote(title, safe=""))
    session = await http_client_manager.get_session()
    async with session.get(
        url, timeout=aiohttp.ClientTimeout(total=LOOKUP_TIMEOUT)
    ) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        payload = await response.json(content_type=None)

    if not isinstance(payload, dict) or not isinstance(payload.get("metas"), list):
        raise ValueError("unexpected Cinemeta payload")

    hits = []
    for meta in payload["metas"]:
        if not isinstance(meta, dict):
            continue
        imdb_id, name = meta.get("id"), meta.get("name")
        if (
            isinstance(imdb_id, str)
            and isinstance(name, str)
            and _IMDB_ID.fullmatch(imdb_id)
        ):
            hits.append((imdb_id, name))
    return hits


async def _load_local_index() -> dict:
    """scrubbed series title -> {imdb ids}, from Comet's own metadata cache.

    Titles live under ``imdb:tt<id>`` keys for movies and series alike (scrapers
    also leave season-scoped rows under movie ids), so only ids present in
    ``series_episode_index`` count: a movie must never identify a series.
    """
    rows = await database.fetch_all(
        "SELECT m.media_id, m.title FROM media_metadata_cache m "
        "WHERE m.title IS NOT NULL AND m.media_id LIKE 'imdb:tt%' "
        "AND EXISTS (SELECT 1 FROM series_episode_index s "
        "WHERE s.series_id = SUBSTR(m.media_id, 6))"
    )
    index: dict[str, set] = {}
    for row in rows:
        base = str(row["media_id"])[5:]
        if not _IMDB_ID.fullmatch(base):
            continue
        key = scrub(row["title"])
        if key:
            index.setdefault(key, set()).add(base)
    return index


async def _get_local_index() -> dict:
    global _local_index, _local_index_at
    now = time.time()
    if now - _local_index_at < _LOCAL_INDEX_TTL:
        return _local_index
    try:
        _local_index = await _load_local_index()
    except Exception:
        pass  # keep the previous index (or empty): the network lookup still runs
    _local_index_at = now
    return _local_index


class IdentityGate:
    """Per-request gate. One instance lives on a TorrentManager, so the lookup
    cap and the time budget are shared by every call made for that request."""

    def __init__(
        self,
        title: str,
        aliases,
        media_id,
        media_type: str,
        is_kitsu: bool = False,
        *,
        max_lookups: int = MAX_LOOKUPS,
        concurrency: int = CONCURRENCY,
        lookup_timeout: float = LOOKUP_TIMEOUT,
        budget: float = BUDGET,
    ):
        self.title = title
        self.media_id = media_id
        self.media_type = media_type
        self.is_kitsu = is_kitsu
        self.max_lookups = max_lookups
        self.concurrency = concurrency
        self.lookup_timeout = lookup_timeout
        self.budget = budget
        self.lookups = 0
        self.spent = 0.0
        self.expected = {scrub(title or "")}
        if isinstance(aliases, dict):
            for names in aliases.values():
                if isinstance(names, (list, tuple, set)):
                    for name in names:
                        if isinstance(name, str):
                            self.expected.add(scrub(name))
        self.expected.discard("")
        self.expected_loose = {_canon(k) for k in self.expected}

    @property
    def enabled(self) -> bool:
        return bool(
            settings.IDENTITY_GATE_ENABLED
            and self.media_type == "series"
            and not self.is_kitsu
            and isinstance(self.media_id, str)
            and _IMDB_ID.fullmatch(self.media_id)
        )

    def _verdict(self, key: str, hits) -> tuple[str, str] | None:
        """(imdb_id, name) of the other series, or None (keep)."""
        exact = [(i, n) for i, n in hits if scrub(n) == key]
        if not exact or any(i == self.media_id for i, _ in exact):
            return None
        return exact[0]

    async def _lookup(self, key: str, raw_title: str):
        try:
            hits = await asyncio.wait_for(
                _fetch_cinemeta(raw_title), timeout=self.lookup_timeout
            )
        except Exception:
            return  # failures are never cached and never reject
        _cache_put(key, tuple((i, n) for i, n in hits if scrub(n) == key))

    async def filter(self, items):
        items = list(items)
        if not items or not self.enabled:
            return items
        try:
            return await self._filter(items)
        except Exception as exc:
            logger.warning(f"Identity gate failed open: {exc!r}")
            return items

    async def _filter(self, items):
        groups: dict[str, list] = {}
        raw: dict[str, str] = {}
        for item in items:
            parsed_title = getattr(item.get("parsed"), "parsed_title", None)
            if not isinstance(parsed_title, str):
                continue
            key = scrub(parsed_title)
            if not key or key in self.expected:
                continue
            # "Mentalist" for "The Mentalist": article-insensitive equality
            if _canon(key) in self.expected_loose:
                continue
            # the release name itself leads with the requested title and the
            # parser picked an episode title ("The Office (US) - S06E03 - The Promotion")
            if any(
                _canon(head) in self.expected_loose
                for head in _release_head(item.get("title"))
            ):
                continue
            groups.setdefault(key, []).append(item)
            raw.setdefault(key, parsed_title)
        if not groups:
            return items

        verdicts: dict[str, tuple[str, str] | None] = {}
        pending: list[str] = []
        local = await _get_local_index()
        for key in groups:
            ids = local.get(key)
            if ids and self.media_id not in ids:
                other = sorted(ids)[0]
                verdicts[key] = (other, raw[key])
                continue
            hits = _cache_get(key)
            if hits is not None:
                verdicts[key] = self._verdict(key, hits)
            elif len(raw[key]) <= MAX_TITLE_LEN:
                pending.append(key)

        remaining_lookups = self.max_lookups - self.lookups
        remaining_time = self.budget - self.spent
        if pending and remaining_lookups > 0 and remaining_time > 0:
            pending.sort(key=lambda k: len(groups[k]), reverse=True)
            chosen = pending[:remaining_lookups]
            self.lookups += len(chosen)
            sem = asyncio.Semaphore(self.concurrency)

            async def bounded(key):
                async with sem:
                    await self._lookup(key, raw[key])

            started = time.monotonic()
            tasks = [asyncio.ensure_future(bounded(k)) for k in chosen]
            try:
                _, not_done = await asyncio.wait(tasks, timeout=remaining_time)
            finally:
                self.spent += time.monotonic() - started
            for task in tasks:
                if not task.done():
                    task.cancel()
            for key in chosen:
                hits = _cache_get(key)
                if hits is not None:
                    verdicts[key] = self._verdict(key, hits)

        rejected = {k: v for k, v in verdicts.items() if v is not None}
        if not rejected:
            return items

        kept = []
        counts = Counter()
        for item in items:
            parsed_title = getattr(item.get("parsed"), "parsed_title", None)
            key = scrub(parsed_title) if isinstance(parsed_title, str) else None
            verdict = rejected.get(key)
            if verdict is None:
                kept.append(item)
                continue
            counts[key] += 1
            _log_rejection(
                f"🪪 Rejected (Other Show) | {item.get('title')} | "
                f"Parsed: {parsed_title} | Is: {verdict[1]} ({verdict[0]}) | "
                f"Expected: {self.title} ({self.media_id})"
            )
        summary = ", ".join(
            f"{raw[k]} -> {rejected[k][1]} ({rejected[k][0]}) x{n}"
            for k, n in counts.most_common(5)
        )
        logger.log(
            "FILTER",
            f"🪪 Identity gate: rejected {sum(counts.values())} torrents of other "
            f"shows for {self.title} ({self.media_id}): {summary}",
        )
        return kept
