import aiohttp
import orjson

from comet.core.logger import logger
from comet.core.models import settings

DEFAULT_TMDB_READ_ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNTkxMmVmOWFhM2IxNzg2Zjk3ZTE1NWY1YmQ3ZjY1MSIsInN1YiI6IjY1M2NjNWUyZTg5NGE2MDBmZjE2N2FmYyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.xrIXsMFJpI1o1j5g2QpQcFP1X3AfRjFA5FlBFO5Naw8"


_overrides_cache: dict[str, str] | None = None
_overrides_cache_source: str | None = None


def _load_tmdb_to_imdb_overrides() -> dict[str, str]:
    """Parse TMDB_TO_IMDB_OVERRIDES env var into {tmdb_id: imdb_id}.

    Supports JSON object form only. Invalid entries are logged and skipped
    rather than failing the whole map. Result is cached until the raw env
    value changes (supports live config reload).
    """
    global _overrides_cache, _overrides_cache_source
    raw = (settings.TMDB_TO_IMDB_OVERRIDES or "").strip()

    if _overrides_cache is not None and _overrides_cache_source == raw:
        return _overrides_cache

    parsed: dict[str, str] = {}
    if raw:
        try:
            data = orjson.loads(raw)
        except orjson.JSONDecodeError as e:
            logger.warning(f"TMDB_TO_IMDB_OVERRIDES is not valid JSON: {e}")
            data = None

        if isinstance(data, dict):
            for tmdb_id, imdb_id in data.items():
                tmdb_key = str(tmdb_id).strip()
                imdb_val = str(imdb_id).strip() if imdb_id is not None else ""
                if not tmdb_key or not imdb_val.startswith("tt"):
                    logger.warning(
                        f"TMDB_TO_IMDB_OVERRIDES: skipping invalid entry "
                        f"{tmdb_id!r}={imdb_id!r} (IMDB IDs must start with 'tt')"
                    )
                    continue
                parsed[tmdb_key] = imdb_val
        elif data is not None:
            logger.warning(
                "TMDB_TO_IMDB_OVERRIDES must be a JSON object, got "
                f"{type(data).__name__}"
            )

    _overrides_cache = parsed
    _overrides_cache_source = raw
    return parsed


class TMDBApi:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.base_url = "https://api.themoviedb.org/3"
        self.headers = {
            "Authorization": f"Bearer {settings.TMDB_READ_ACCESS_TOKEN if settings.TMDB_READ_ACCESS_TOKEN else DEFAULT_TMDB_READ_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }

    async def get_upcoming_movie_release_date(self, tmdb_id: str):
        try:
            url = f"{self.base_url}/movie/{tmdb_id}/release_dates"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    return None

                data = await response.json()

            release_dates = []
            for result in data.get("results", []):
                for release in result.get("release_dates", []):
                    # Type 4 = Digital, Type 5 = Physical
                    if release.get("type") in (4, 5):
                        date_str = release.get("release_date", "").split("T")[0]
                        if date_str:
                            release_dates.append(date_str)

            return min(release_dates) if release_dates else None
        except Exception as e:
            logger.error(f"TMDB: Error getting movie release date for {tmdb_id}: {e}")
            return None

    async def get_episode_air_date(self, tmdb_id: str, season: int, episode: int):
        try:
            url = f"{self.base_url}/tv/{tmdb_id}/season/{season}/episode/{episode}"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                return data.get("air_date")
        except Exception as e:
            logger.error(
                f"TMDB: Error getting episode air date for {tmdb_id} S{season}E{episode}: {e}"
            )
            return None

    async def get_tmdb_id_from_imdb(self, imdb_id: str):
        try:
            url = f"{self.base_url}/find/{imdb_id}?external_source=imdb_id"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(
                        f"TMDB: Failed to get TMDB ID from IMDB ID {imdb_id}: {text}"
                    )
                    return None

                data = await response.json()

            movie_results = data.get("movie_results")
            if movie_results:
                return str(movie_results[0]["id"])

            tv_results = data.get("tv_results")
            if tv_results:
                return str(tv_results[0]["id"])

            return None
        except Exception as e:
            logger.error(f"TMDB: Error converting IMDB ID {imdb_id}: {e}")
            return None

    async def get_season_episodes(self, tmdb_id: str, season: int) -> dict[str, int]:
        """Fetch all episodes for a season and return a mapping of air_date -> episode_number.

        Returns: {"2026-01-05": 9, "2026-01-06": 10, ...}
        """
        try:
            url = f"{self.base_url}/tv/{tmdb_id}/season/{season}"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    return {}

                data = await response.json()

            date_to_episode = {}
            for ep in data.get("episodes", []):
                air_date = ep.get("air_date")
                ep_number = ep.get("episode_number")
                if air_date and ep_number is not None:
                    date_to_episode[air_date] = ep_number
            return date_to_episode
        except Exception as e:
            logger.error(f"TMDB: Error getting season episodes for {tmdb_id} S{season}: {e}")
            return {}

    async def get_seasons_for_show(self, tmdb_id: str) -> list[dict]:
        """Fetch the list of seasons for a show with their air dates.

        Returns: [{"season_number": 1, "air_date": "2024-01-01"}, ...]
        """
        try:
            url = f"{self.base_url}/tv/{tmdb_id}"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    return []

                data = await response.json()

            return [
                {"season_number": s.get("season_number"), "air_date": s.get("air_date")}
                for s in data.get("seasons", [])
            ]
        except Exception as e:
            logger.error(f"TMDB: Error getting seasons for {tmdb_id}: {e}")
            return []

    async def has_watch_providers(self, tmdb_id: str):
        try:
            url = f"{self.base_url}/movie/{tmdb_id}/watch/providers"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                return bool(data.get("results"))
        except Exception as e:
            logger.error(f"TMDB: Error getting watch providers for {tmdb_id}: {e}")
            return None

    async def get_external_ids(self, tmdb_id: str, media_type: str):
        """Fetch external IDs (imdb_id, tvdb_id, ...) for a TMDB item.

        media_type: "movie" or "series" (mapped to TMDB's "tv" endpoint).
        Returns dict like {"imdb_id": "tt1234567", "tvdb_id": 1234, ...} or None on failure.
        """
        tmdb_path = "movie" if media_type == "movie" else "tv"
        try:
            url = f"{self.base_url}/{tmdb_path}/{tmdb_id}/external_ids"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    logger.warning(
                        f"TMDB: external_ids for {tmdb_path}/{tmdb_id} returned HTTP {response.status}"
                    )
                    return None
                return await response.json()
        except Exception as e:
            logger.error(
                f"TMDB: Error getting external_ids for {tmdb_path}/{tmdb_id}: {e}"
            )
            return None

    async def get_imdb_from_tmdb(self, tmdb_id: str, media_type: str):
        """Return the IMDB ID (tt...) for a TMDB item, or None if unavailable.

        Checks manual TMDB_TO_IMDB_OVERRIDES first (for items TMDB hasn't
        linked to IMDB), then falls back to TMDB's external_ids lookup.
        """
        overrides = _load_tmdb_to_imdb_overrides()
        override = overrides.get(str(tmdb_id).strip())
        if override:
            logger.log(
                "SCRAPER",
                f"🧷 TMDB_TO_IMDB_OVERRIDES: tmdb:{tmdb_id} -> {override}",
            )
            return override

        external_ids = await self.get_external_ids(tmdb_id, media_type)
        if not external_ids:
            return None
        imdb_id = external_ids.get("imdb_id")
        if not imdb_id or not isinstance(imdb_id, str) or not imdb_id.startswith("tt"):
            return None
        return imdb_id

    async def get_movie_metadata(self, tmdb_id: str):
        """Return (title, year, year_end) for a TMDB movie, or (None, None, None) on failure."""
        try:
            url = f"{self.base_url}/movie/{tmdb_id}"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    logger.warning(
                        f"TMDB: movie/{tmdb_id} returned HTTP {response.status}"
                    )
                    return None, None, None
                data = await response.json()

            title = data.get("title") or data.get("original_title")
            release_date = data.get("release_date") or ""
            year = None
            if release_date[:4].isdigit():
                year = int(release_date[:4])
            return title, year, None
        except Exception as e:
            logger.error(f"TMDB: Error getting movie metadata for {tmdb_id}: {e}")
            return None, None, None

    async def get_tv_metadata(self, tmdb_id: str):
        """Return (title, year_first_aired, year_last_aired) for a TMDB TV show."""
        try:
            url = f"{self.base_url}/tv/{tmdb_id}"
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    logger.warning(
                        f"TMDB: tv/{tmdb_id} returned HTTP {response.status}"
                    )
                    return None, None, None
                data = await response.json()

            title = data.get("name") or data.get("original_name")
            first_air_date = data.get("first_air_date") or ""
            last_air_date = data.get("last_air_date") or ""
            year = int(first_air_date[:4]) if first_air_date[:4].isdigit() else None
            year_end = int(last_air_date[:4]) if last_air_date[:4].isdigit() else None
            # Ongoing shows: don't report a year_end equal to first-air year unless ended
            if data.get("in_production") and year_end == year:
                year_end = None
            return title, year, year_end
        except Exception as e:
            logger.error(f"TMDB: Error getting tv metadata for {tmdb_id}: {e}")
            return None, None, None
