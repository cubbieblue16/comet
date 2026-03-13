from comet.core.logger import logger
from comet.scrapers.base import BaseScraper
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

            # Date-based search for shows that use dates instead of S##E##
            if request.air_date:
                date_query = f"{request.title} {request.air_date.replace('-', '.')}"
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
