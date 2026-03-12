import aiohttp

from comet.core.logger import logger
from comet.core.models import settings

DEFAULT_TMDB_READ_ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNTkxMmVmOWFhM2IxNzg2Zjk3ZTE1NWY1YmQ3ZjY1MSIsInN1YiI6IjY1M2NjNWUyZTg5NGE2MDBmZjE2N2FmYyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.xrIXsMFJpI1o1j5g2QpQcFP1X3AfRjFA5FlBFO5Naw8"


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

            return data.get("seasons", [])
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
