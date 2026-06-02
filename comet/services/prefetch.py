"""Next-episode prefetch.

When a series episode starts playing, warm the *next* episode in the
background so it plays instantly when the user (or Stremio's binge auto-advance)
moves on:

  Part A - scrape + debrid availability for episode N+1 -> its stream list and
           cache rows are ready before Stremio ever asks for them.
  Part B - pre-resolve the top cached candidate's playback (download) link into
           ``download_links_cache`` -> the click->play spinner is skipped.

This shifts work that would happen at N+1's start to N's start; net debrid load
over a full binge is unchanged (you were going to watch N+1 anyway). The only
genuinely extra calls are for episodes that get prefetched but never watched.

Fire-and-forget: every failure is swallowed so a prefetch problem can never
affect the playback that triggered it. Gated behind ``PREFETCH_NEXT_EPISODE``
(master) and ``PREFETCH_NEXT_EPISODE_RESOLVE_LINK`` (Part B).
"""

import asyncio
import time

from comet.core.database import (DOWNLOAD_LINK_CACHE_TTL,
                                  build_scope_lookup_params, database)
from comet.core.logger import logger
from comet.core.models import settings
from comet.debrid.exceptions import DebridLinkGenerationError
from comet.debrid.manager import build_account_key_hash, get_debrid
from comet.metadata.episode_index import EpisodeIndexService
from comet.metadata.manager import MetadataScraper
from comet.services.date_episode_resolver import DateEpisodeResolver
from comet.services.lock import DistributedLock
from comet.services.orchestration import TorrentManager


# Best-effort in-memory guard against re-warming the same next episode on every
# /playback/ hit. Keyed by "{next_media_id}|{debrid_service}" -> last-warm time.
# Single worker (FASTAPI_WORKERS=1), so a plain dict is sufficient; a miss just
# falls back to the pre-guard behavior.
_RECENTLY_WARMED: dict[str, float] = {}


def _recently_warmed(key: str, now: float, ttl: int) -> bool:
    ts = _RECENTLY_WARMED.get(key)
    return ts is not None and (now - ts) < ttl


def _mark_warmed(key: str, now: float, ttl: int) -> None:
    _RECENTLY_WARMED[key] = now
    # Bound memory: drop entries older than the TTL on each write.
    stale = [k for k, t in _RECENTLY_WARMED.items() if now - t >= ttl]
    for k in stale:
        _RECENTLY_WARMED.pop(k, None)


def _select_target_hash(
    ranked_hashes,
    torrents: dict,
    service_cache_status: dict,
    debrid_service: str,
    played_hash: str | None,
) -> str | None:
    """Pick the info_hash to pre-resolve a download link for.

    Stremio's binge auto-advance reuses the bingeGroup of the episode the user
    just played, and Comet's bingeGroup embeds the info_hash. For a season pack
    (one hash serves every episode) the next episode is therefore requested with
    the SAME hash the user just played. So if that played hash is also a cached
    candidate for N+1, resolve THAT hash -- it's the one auto-advance will ask
    for. Otherwise fall back to the highest-ranked candidate cached on this
    service (the per-episode-torrent / no-played-hash case).
    """

    def _cached(info_hash: str) -> bool:
        status = service_cache_status.get(info_hash)
        return bool(status and status.get(debrid_service))

    if played_hash and played_hash in torrents and _cached(played_hash):
        return played_hash

    for info_hash in ranked_hashes:
        if _cached(info_hash):
            return info_hash
    return None


async def prefetch_next_episode(
    *,
    session,
    config: dict,
    media_only_id: str | None,
    season: int | None,
    episode: int | None,
    debrid_service: str,
    debrid_api_key: str,
    ip: str,
    played_hash: str | None = None,
):
    """Entry point. Decides whether to warm episode N+1, then does it under a lock."""
    if not settings.PREFETCH_NEXT_EPISODE:
        return

    # Series episodes only (season+episode known) on IMDb ids. Kitsu/anime and
    # date-based season rollover are out of scope for v1.
    if season is None or episode is None:
        return
    if not media_only_id or not media_only_id.startswith("tt"):
        return
    if debrid_service == "torrent" or not debrid_api_key:
        return

    debrid_entries = config.get("_debridEntries", [])
    if not debrid_entries:
        return

    next_episode = episode + 1
    next_media_id = f"{media_only_id}:{season}:{next_episode}"

    # Skip if we warmed this exact next episode very recently (repeated /playback/
    # hits, seeks). Checked before the lock so we avoid a forced-primary DB write.
    warm_key = f"{next_media_id}|{debrid_service}"
    rewarm_ttl = settings.PREFETCH_REWARM_TTL
    if _recently_warmed(warm_key, time.time(), rewarm_ttl):
        return

    # Separate lock namespace so a prefetch never blocks a real-time stream
    # request for the same episode (worst case is a rare harmless double scrape).
    lock = DistributedLock(f"prefetch:{next_media_id}")
    if not await lock.acquire():
        return

    try:
        await _warm_next_episode(
            session=session,
            config=config,
            media_only_id=media_only_id,
            next_media_id=next_media_id,
            season=season,
            next_episode=next_episode,
            debrid_entries=debrid_entries,
            debrid_service=debrid_service,
            debrid_api_key=debrid_api_key,
            ip=ip,
            played_hash=played_hash,
        )
        _mark_warmed(warm_key, time.time(), rewarm_ttl)
    except Exception as e:
        logger.warning(f"Next-episode prefetch failed for {next_media_id}: {e}")
    finally:
        await lock.release()


async def _warm_next_episode(
    *,
    session,
    config: dict,
    media_only_id: str,
    next_media_id: str,
    season: int,
    next_episode: int,
    debrid_entries: list,
    debrid_service: str,
    debrid_api_key: str,
    ip: str,
    played_hash: str | None,
):
    metadata_scraper = MetadataScraper(session)
    episode_index = EpisodeIndexService(session)
    (metadata, aliases), target_air_date = await asyncio.gather(
        metadata_scraper.fetch_metadata_and_aliases(
            "series", next_media_id, media_only_id, season, next_episode
        ),
        episode_index.get_target_air_date(media_only_id, season, next_episode),
    )
    # No metadata -> next episode doesn't exist (end of season). Nothing to warm.
    if metadata is None or metadata.get("episode") is None:
        logger.log(
            "SCRAPER", f"⏭️  Prefetch: no next episode for {next_media_id}, skipping"
        )
        return

    title = metadata["title"]

    remove_adult_content = settings.REMOVE_ADULT_CONTENT and config["removeTrash"]
    torrent_manager = TorrentManager(
        "series",
        next_media_id,
        media_only_id,
        title,
        metadata["year"],
        metadata["year_end"],
        metadata["season"],
        metadata["episode"],
        aliases,
        remove_adult_content,
        is_kitsu=False,
        search_episode=next_episode,
        search_season=season,
        cache_media_ids=[media_only_id],
        target_air_date=target_air_date,
        reject_unknown_episode_files=True,
        date_resolver=DateEpisodeResolver(session),
    )

    await torrent_manager.get_cached_torrents()
    if not torrent_manager.primary_cached:
        await torrent_manager.scrape_torrents()

    if not torrent_manager.torrents:
        logger.log("SCRAPER", f"⏭️  Prefetch: no torrents for {next_media_id}")
        return

    # Lazy import avoids an endpoint<->service import cycle (stream.py imports
    # services, this service must not be imported at stream.py load time).
    from comet.api.endpoints.stream import \
        get_and_cache_multi_service_availability

    service_cache_status, _errors = await get_and_cache_multi_service_availability(
        session,
        debrid_entries,
        torrent_manager.torrents,
        next_media_id,
        media_only_id,
        season,
        next_episode,
        ip,
        target_air_date=target_air_date,
    )

    # Diagnostic receipt for prefetch-write / stream-read alignment. Logged in
    # the same shape as stream.py's read-side receipt so the two lines diff
    # visually when a slow re-open happens later. Fields here are exactly what
    # got written to debrid_availability rows.
    logger.log(
        "SCRAPER",
        f"🔍 Availability-cache write for {next_media_id}: "
        f"torrents={len(torrent_manager.torrents)} "
        f"services={[e['service'] for e in debrid_entries]} "
        f"season_norm={season} episode_norm={next_episode} "
        f"account_key_hashes={[build_account_key_hash(e['apiKey'])[:8] for e in debrid_entries]}"
    )

    logger.log(
        "SCRAPER",
        f"🔮 Prefetched next episode {next_media_id}: "
        f"{len(torrent_manager.torrents)} torrents warmed",
    )

    if not settings.PREFETCH_NEXT_EPISODE_RESOLVE_LINK:
        return

    await _resolve_next_episode_link(
        session=session,
        config=config,
        torrent_manager=torrent_manager,
        service_cache_status=service_cache_status,
        media_only_id=media_only_id,
        next_media_id=next_media_id,
        season=season,
        next_episode=next_episode,
        name=title,
        aliases=aliases,
        debrid_service=debrid_service,
        debrid_api_key=debrid_api_key,
        ip=ip,
        played_hash=played_hash,
    )


async def _resolve_next_episode_link(
    *,
    session,
    config: dict,
    torrent_manager: TorrentManager,
    service_cache_status: dict,
    media_only_id: str,
    next_media_id: str,
    season: int,
    next_episode: int,
    name: str,
    aliases: dict,
    debrid_service: str,
    debrid_api_key: str,
    ip: str,
    played_hash: str | None,
):
    """Pre-resolve the download link for the top cached candidate of N+1.

    Picks the highest-ranked torrent that is already cached on the user's debrid
    service (the one Stremio's binge auto-advance is most likely to play), then
    resolves and caches its link. Only ever touches a hash known to be cached, so
    it never wastes a link-gen on an uncached/nonexistent torrent.
    """
    await torrent_manager.rank_torrents(
        config["rtnSettings"],
        config["rtnRanking"],
        0,
        config["maxSize"],
        config["removeTrash"],
    )

    target_hash = _select_target_hash(
        torrent_manager.ranked_torrents,
        torrent_manager.torrents,
        service_cache_status,
        debrid_service,
        played_hash,
    )
    if target_hash is None:
        # No torrent (played_hash or any ranked candidate) is cached on this
        # service for N+1. Logged so we can distinguish this silent path from
        # the "existing link reused" path below when debugging slow auto-advance.
        cached_count = sum(
            1 for h in torrent_manager.torrents
            if (service_cache_status.get(h) or {}).get(debrid_service)
        )
        logger.log(
            "PLAYBACK",
            f"⏭️  Prefetch link-gen skipped for {next_media_id}: "
            f"no cached candidate on {debrid_service} "
            f"(played_hash={played_hash[:8] if played_hash else 'none'}, "
            f"ranked={len(torrent_manager.ranked_torrents)}, "
            f"cached_on_service={cached_count})",
        )
        return

    torrent = torrent_manager.torrents[target_hash]
    account_key_hash = build_account_key_hash(debrid_api_key)

    # Skip if a fresh link for this exact hash/episode is already cached.
    min_timestamp = time.time() - DOWNLOAD_LINK_CACHE_TTL
    scope_params = build_scope_lookup_params(season, next_episode)
    existing = await database.fetch_one(
        """
        SELECT 1
        FROM download_links_cache
        WHERE debrid_service = :debrid_service
        AND account_key_hash = :account_key_hash
        AND info_hash = :info_hash
        AND season_norm = :season_norm
        AND episode_norm = :episode_norm
        AND updated_at >= :min_timestamp
        """,
        {
            "debrid_service": debrid_service,
            "account_key_hash": account_key_hash,
            "info_hash": target_hash,
            "min_timestamp": min_timestamp,
            **scope_params,
        },
    )
    if existing:
        logger.log(
            "PLAYBACK",
            f"♻️  Prefetch link reused for {next_media_id} ({target_hash[:8]}): "
            f"fresh cached link already in download_links_cache",
        )
        return

    debrid = get_debrid(
        session,
        f"{media_only_id}:{season}:{next_episode}",
        media_only_id,
        debrid_service,
        debrid_api_key,
        ip,
    )
    if debrid is None:
        logger.log(
            "PLAYBACK",
            f"⏭️  Prefetch link-gen skipped for {next_media_id}: "
            f"get_debrid returned None for {debrid_service}",
        )
        return

    file_index = torrent.get("fileIndex")
    torrent_title = torrent.get("title") or ""
    try:
        download_url = await debrid.generate_download_link(
            target_hash,
            str(file_index) if file_index is not None else "n",
            name,
            torrent_title,
            season,
            next_episode,
            torrent.get("sources", []),
            aliases,
        )
    except DebridLinkGenerationError as e:
        logger.log("PLAYBACK", f"Prefetch link-gen skipped for {next_media_id}: {e}")
        return

    if not download_url:
        logger.log(
            "PLAYBACK",
            f"⏭️  Prefetch link-gen skipped for {next_media_id} ({target_hash[:8]}): "
            f"generate_download_link returned empty URL",
        )
        return

    from comet.api.endpoints.playback import cache_download_link

    await cache_download_link(
        debrid_service=debrid_service,
        account_key_hash=account_key_hash,
        info_hash=target_hash,
        season=season,
        episode=next_episode,
        download_url=download_url,
    )
    logger.log(
        "PLAYBACK",
        f"🔮 Prefetched playback link for {next_media_id} ({target_hash[:8]})",
    )
