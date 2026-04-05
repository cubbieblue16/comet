from comet.core.logger import log_scraper_error, logger
from comet.core.models import settings
from comet.scrapers.base import BaseScraper
from comet.scrapers.models import ScrapeRequest
from comet.services.torrent_manager import extract_trackers_from_magnet


class TorboxScraper(BaseScraper):
    def __init__(self, manager, session):
        super().__init__(manager, session)

    async def scrape(self, request: ScrapeRequest):
        if not settings.TORBOX_API_KEY:
            return []

        torrents = []

        try:
            async with self.session.get(
                f"https://search-api.torbox.app/torrents/imdb:{request.media_only_id}",
                headers={"Authorization": f"Bearer {settings.TORBOX_API_KEY}"},
            ) as response:
                if response.status != 200:
                    logger.warning(
                        f"TorBox returned HTTP {response.status} for {request.media_only_id}"
                    )
                    return torrents

                data = await response.json()

            if not data or not data.get("data") or not data["data"].get("torrents"):
                return torrents

            for torrent in data["data"]["torrents"]:
                torrents.append(
                    {
                        "title": torrent["raw_title"],
                        "infoHash": torrent["hash"],
                        "fileIndex": None,
                        "seeders": torrent["last_known_seeders"],
                        "size": torrent["size"],
                        "tracker": f"TorBox|{torrent['tracker']}",
                        "sources": extract_trackers_from_magnet(torrent["magnet"]),
                    }
                )
        except Exception as e:
            log_scraper_error(
                "TorBox", "https://search-api.torbox.app", request.media_only_id, e
            )

        return torrents
