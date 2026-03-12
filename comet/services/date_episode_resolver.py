from cachetools import TTLCache

import aiohttp

from comet.core.logger import logger
from comet.metadata.tmdb import TMDBApi
from comet.utils.parsing import extract_date_from_title

# Module-level TTL caches shared across all resolver instances and requests.
# TMDB data (air dates, season lists) is stable — 1 hour TTL is safe.
_tmdb_id_cache: TTLCache = TTLCache(maxsize=500, ttl=3600)
_season_date_cache: TTLCache = TTLCache(maxsize=500, ttl=3600)
_seasons_list_cache: TTLCache = TTLCache(maxsize=500, ttl=3600)


class DateEpisodeResolver:
    """Resolves date-based torrent filenames to season/episode numbers via TMDB.

    Uses module-level TTL caches so lookups are shared across all requests.
    Creating multiple instances (e.g., one per debrid service) is cheap —
    they all share the same cached TMDB data.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self._tmdb = TMDBApi(session)

    async def resolve(
        self,
        imdb_id: str,
        title: str,
        search_season: int | None = None,
    ) -> tuple[int | None, int | None]:
        """Attempt to resolve a date-based torrent title to (season, episode).

        Returns (season, episode) if resolved, or (None, None) if not.
        """
        air_date = extract_date_from_title(title)
        if air_date is None:
            return None, None

        tmdb_id = await self._get_tmdb_id(imdb_id)
        if tmdb_id is None:
            return None, None

        date_str = air_date.isoformat()

        # If we know the season, check it directly
        if search_season is not None:
            date_map = await self._get_season_date_map(tmdb_id, search_season)
            ep = date_map.get(date_str)
            if ep is not None:
                logger.log(
                    "SCRAPER",
                    f"Date resolver: mapped {date_str} -> S{search_season:02d}E{ep:02d} for {imdb_id}",
                )
                return search_season, ep

        # If season unknown or not found in the given season, search by year
        seasons = await self._get_seasons_list(tmdb_id)
        for season_info in reversed(seasons):  # reverse: most recent first
            season_num = season_info.get("season_number")
            if season_num is None:
                continue
            # Include season 0 (specials) — WWE PPVs are often classified here
            season_air_date = season_info.get("air_date", "")
            if not season_air_date:
                continue
            # Skip seasons from different years (rough filter)
            try:
                season_year = int(season_air_date[:4])
            except (ValueError, IndexError):
                continue
            if abs(season_year - air_date.year) > 1:
                continue

            if season_num == search_season:
                continue  # Already checked above

            date_map = await self._get_season_date_map(tmdb_id, season_num)
            ep = date_map.get(date_str)
            if ep is not None:
                logger.log(
                    "SCRAPER",
                    f"Date resolver: mapped {date_str} -> S{season_num:02d}E{ep:02d} for {imdb_id} (fallback)",
                )
                return season_num, ep

        return None, None

    async def _get_tmdb_id(self, imdb_id: str) -> str | None:
        cached = _tmdb_id_cache.get(imdb_id)
        if cached is not None:
            return cached if cached != "" else None

        tmdb_id = await self._tmdb.get_tmdb_id_from_imdb(imdb_id)

        # Cache the result (use empty string for None to distinguish from cache miss)
        _tmdb_id_cache[imdb_id] = tmdb_id if tmdb_id is not None else ""

        if tmdb_id is None:
            logger.warning(f"DateResolver: Could not find TMDB ID for {imdb_id}")

        return tmdb_id

    async def _get_season_date_map(self, tmdb_id: str, season: int) -> dict[str, int]:
        cache_key = f"{tmdb_id}:{season}"
        cached = _season_date_cache.get(cache_key)
        if cached is not None:
            return cached

        date_map = await self._tmdb.get_season_episodes(tmdb_id, season)
        _season_date_cache[cache_key] = date_map
        return date_map

    async def _get_seasons_list(self, tmdb_id: str) -> list[dict]:
        cached = _seasons_list_cache.get(tmdb_id)
        if cached is not None:
            return cached

        seasons = await self._tmdb.get_seasons_for_show(tmdb_id)
        _seasons_list_cache[tmdb_id] = seasons
        return seasons
