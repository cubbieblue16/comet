from collections import defaultdict

import aiohttp

from comet.core.logger import logger
from comet.core.models import settings


def _get_trakt_headers():
    return {
        "trakt-api-version": "2",
        "trakt-api-key": settings.TRAKT_API_KEY or "",
    }


async def get_trakt_aliases(
    session: aiohttp.ClientSession, media_type: str, media_id: str
):
    api_key = settings.TRAKT_API_KEY
    if not api_key:
        return {}

    try:
        async with session.get(
            f"https://api.trakt.tv/{'movies' if media_type == 'movie' else 'shows'}/{media_id}/aliases",
            headers=_get_trakt_headers(),
        ) as response:
            if response.status != 200:
                logger.warning(f"Trakt API returned HTTP {response.status} for {media_id}")
                return {}
            data = await response.json()

        result = defaultdict(set)
        for alias_entry in data:
            title = alias_entry.get("title")
            country = alias_entry.get("country")

            if title:
                key = country if country else "ez"
                result[key].add(title)

        return {k: list(v) for k, v in result.items()}
    except Exception as e:
        logger.warning(f"Failed to fetch Trakt aliases for {media_id}: {e}")

    return {}
