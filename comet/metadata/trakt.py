from collections import defaultdict

import aiohttp


_TRAKT_HEADERS = {
    "trakt-api-version": "2",
    "trakt-api-key": "0",
}


async def get_trakt_aliases(
    session: aiohttp.ClientSession, media_type: str, media_id: str
):
    try:
        async with session.get(
            f"https://api.trakt.tv/{'movies' if media_type == 'movie' else 'shows'}/{media_id}/aliases",
            headers=_TRAKT_HEADERS,
        ) as response:
            data = await response.json()

        result = defaultdict(set)
        for alias_entry in data:
            title = alias_entry.get("title")
            country = alias_entry.get("country")

            if title:
                key = country if country else "ez"
                result[key].add(title)

        return {k: list(v) for k, v in result.items()}
    except Exception:
        pass

    return {}
