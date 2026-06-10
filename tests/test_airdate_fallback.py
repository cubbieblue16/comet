"""The date-resolver fallback in scrape_torrents must store the resolved air
date on the manager, not just pass it to scrapers — filtering decisions
(reject_unknown overrides) read self.target_air_date."""

import pytest

import comet.services.orchestration as orchestration_module
from comet.services.orchestration import TorrentManager


class FakeDateResolver:
    async def get_air_date(self, media_only_id, season, episode):
        return "2025-01-11"


@pytest.mark.asyncio
async def test_scrape_torrents_stores_fallback_air_date(monkeypatch):
    async def fake_scrape_all(request):
        return
        yield  # async generator that yields nothing

    monkeypatch.setattr(
        orchestration_module.scraper_manager, "scrape_all", fake_scrape_all
    )

    async def fake_cache_torrents(self):
        return None

    monkeypatch.setattr(TorrentManager, "cache_torrents", fake_cache_torrents)

    tm = TorrentManager(
        "series",
        "tt1:1:2",
        "tt1",
        "Show",
        2020,
        None,
        1,
        2,
        aliases={},
        remove_adult_content=False,
        search_episode=2,
        search_season=1,
        target_air_date=None,
        date_resolver=FakeDateResolver(),
    )

    await tm.scrape_torrents()

    assert tm.target_air_date == "2025-01-11"
