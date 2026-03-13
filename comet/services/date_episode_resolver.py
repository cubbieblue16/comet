import time

import aiohttp
import orjson

from comet.core.logger import logger
from comet.core.models import database, settings
from comet.metadata.tmdb import TMDBApi


class DateEpisodeResolver:
    """Resolves date-based torrent filenames to season/episode numbers via TMDB.

    Uses database-backed caching so lookups persist across restarts and are
    shared across all workers.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self._tmdb = TMDBApi(session)

    async def resolve(
        self,
        imdb_id: str,
        date_str: str | None,
        search_season: int | None = None,
        fallback: bool = True,
    ) -> tuple[int | None, int | None]:
        """Resolve a date string to (season, episode) via TMDB.

        Args:
            imdb_id: IMDb ID of the show (e.g. "tt0115147")
            date_str: ISO date from RTN's parsed.date (e.g. "2026-01-05"), or None
            search_season: Season to check first
            fallback: If False, only checks search_season (fast path).
                      If True, searches other seasons when not found.

        Returns (season, episode) if resolved, or (None, None) if not.
        """
        try:
            if not date_str or not imdb_id:
                return None, None

            tmdb_id = await self._get_tmdb_id(imdb_id)
            if tmdb_id is None:
                return None, None

            # Extract year from date string for fallback year filtering
            try:
                date_year = int(date_str[:4])
            except (ValueError, IndexError):
                return None, None

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

            if not fallback:
                return None, None

            # If season unknown or not found in the given season, search by year
            seasons = await self._get_seasons_list(tmdb_id)
            for season_info in reversed(seasons):  # reverse: most recent first
                season_num = season_info.get("season_number")
                if season_num is None:
                    continue
                season_air_date = season_info.get("air_date", "")
                if not season_air_date:
                    continue
                try:
                    season_year = int(season_air_date[:4])
                except (ValueError, IndexError):
                    continue
                if abs(season_year - date_year) > 1:
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
        except Exception as e:
            logger.error(f"DateResolver: Error resolving {date_str} for {imdb_id}: {e}")
            return None, None

    async def _get_tmdb_id(self, imdb_id: str) -> str | None:
        """Look up TMDB ID from IMDb ID, with DB-backed caching."""
        row = await database.fetch_one(
            """
            SELECT tmdb_id FROM date_episode_cache
            WHERE cache_key = :key AND cache_type = 'tmdb_id'
            AND timestamp >= :min_ts
            """,
            {"key": imdb_id, "min_ts": time.time() - settings.METADATA_CACHE_TTL},
        )
        if row is not None:
            val = row["tmdb_id"]
            return val if val else None

        tmdb_id = await self._tmdb.get_tmdb_id_from_imdb(imdb_id)

        await database.execute(
            """
            INSERT INTO date_episode_cache (cache_key, cache_type, tmdb_id, data, timestamp)
            VALUES (:key, 'tmdb_id', :tmdb_id, NULL, :ts)
            ON CONFLICT (cache_key, cache_type) DO UPDATE SET tmdb_id = :tmdb_id, timestamp = :ts
            """,
            {"key": imdb_id, "tmdb_id": tmdb_id or "", "ts": int(time.time())},
        )

        if tmdb_id is None:
            logger.warning(f"DateResolver: Could not find TMDB ID for {imdb_id}")

        return tmdb_id

    async def _get_season_date_map(self, tmdb_id: str, season: int) -> dict[str, int]:
        """Fetch air_date -> episode mapping for a season, with DB-backed caching."""
        cache_key = f"{tmdb_id}:{season}"

        row = await database.fetch_one(
            """
            SELECT data FROM date_episode_cache
            WHERE cache_key = :key AND cache_type = 'season_dates'
            AND timestamp >= :min_ts
            """,
            {"key": cache_key, "min_ts": time.time() - settings.METADATA_CACHE_TTL},
        )
        if row is not None and row["data"]:
            return orjson.loads(row["data"])

        date_map = await self._tmdb.get_season_episodes(tmdb_id, season)

        await database.execute(
            """
            INSERT INTO date_episode_cache (cache_key, cache_type, tmdb_id, data, timestamp)
            VALUES (:key, 'season_dates', NULL, :data, :ts)
            ON CONFLICT (cache_key, cache_type) DO UPDATE SET data = :data, timestamp = :ts
            """,
            {"key": cache_key, "data": orjson.dumps(date_map).decode(), "ts": int(time.time())},
        )

        return date_map

    async def _get_seasons_list(self, tmdb_id: str) -> list[dict]:
        """Fetch season list for a show, with DB-backed caching."""
        row = await database.fetch_one(
            """
            SELECT data FROM date_episode_cache
            WHERE cache_key = :key AND cache_type = 'seasons_list'
            AND timestamp >= :min_ts
            """,
            {"key": tmdb_id, "min_ts": time.time() - settings.METADATA_CACHE_TTL},
        )
        if row is not None and row["data"]:
            return orjson.loads(row["data"])

        seasons = await self._tmdb.get_seasons_for_show(tmdb_id)

        await database.execute(
            """
            INSERT INTO date_episode_cache (cache_key, cache_type, tmdb_id, data, timestamp)
            VALUES (:key, 'seasons_list', NULL, :data, :ts)
            ON CONFLICT (cache_key, cache_type) DO UPDATE SET data = :data, timestamp = :ts
            """,
            {"key": tmdb_id, "data": orjson.dumps(seasons).decode(), "ts": int(time.time())},
        )

        return seasons
