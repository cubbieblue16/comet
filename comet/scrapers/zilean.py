from comet.core.logger import logger
from comet.scrapers.base import BaseScraper
from comet.scrapers.helpers.date_queries import build_date_queries
from comet.scrapers.models import ScrapeRequest


class ZileanScraper(BaseScraper):
    def __init__(self, manager, session, url: str):
        super().__init__(manager, session, url)

    def _parse_results(self, data):
        torrents = []
        for result in data:
            torrents.append(
                {
                    "title": result["raw_title"],
                    "infoHash": result["info_hash"].lower(),
                    "fileIndex": None,
                    "seeders": None,
                    "size": int(result["size"]),
                    "tracker": "DMM",
                    "sources": [],
                }
            )
        return torrents

    async def scrape(self, request: ScrapeRequest):
        torrents = []
        seen_hashes = set()
        try:
            show = (
                f"&season={request.season}&episode={request.episode}"
                if request.media_type == "series"
                else ""
            )
            data = await self.session.get(
                f"{self.url}/dmm/filtered?query={request.title}{show}"
            )
            data = await data.json()

            for t in self._parse_results(data):
                if t["infoHash"] not in seen_hashes:
                    seen_hashes.add(t["infoHash"])
                    torrents.append(t)

            # Date-based fallback search for shows that use dates instead of
            # S##E## (fans out across air_date-1/air_date/air_date+1 to
            # tolerate TMDB timezone drift, and a cleaned title to avoid
            # punctuation that breaks full-text search)
            date_queries = build_date_queries(
                request.title, request.air_date, request.season, request.episode
            )
            for date_query in date_queries:
                data = await self.session.get(
                    f"{self.url}/dmm/filtered?query={date_query}"
                )
                data = await data.json()

                for t in self._parse_results(data):
                    if t["infoHash"] not in seen_hashes:
                        seen_hashes.add(t["infoHash"])
                        torrents.append(t)

        except Exception as e:
            logger.warning(
                f"Exception while getting torrents for {request.title} with Zilean ({self.url}): {e}"
            )

        return torrents
