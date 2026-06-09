"""The /playback/ request itself is the "user is watching episode N" signal.

The next-episode prefetch must therefore fire even when THIS episode's
link generation fails — those failures are exactly when warming N+1 matters
most (the chain otherwise dies: failed playback -> no trigger -> N+1 cold).
Link-gen failures must also leave a log line; previously the only trace was
a 206 placeholder in the access log.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import comet.api.endpoints.playback as playback_module
from comet.core.models import settings
from comet.debrid.exceptions import DebridLinkGenerationError


@pytest.fixture
def playback_env(monkeypatch):
    """Patch playback()'s collaborators so it runs as a plain coroutine."""
    monkeypatch.setattr(settings, "PREFETCH_NEXT_EPISODE", True)
    monkeypatch.setattr(settings, "PROXY_DEBRID_STREAM", False)

    monkeypatch.setattr(
        playback_module,
        "config_check",
        lambda b64, strict_b64config=True: {"debridStreamProxyPassword": ""},
    )
    monkeypatch.setattr(
        playback_module,
        "get_debrid_credentials",
        lambda config, index: ("realdebrid", "apikey"),
    )
    monkeypatch.setattr(
        playback_module.http_client_manager,
        "get_session",
        AsyncMock(return_value=MagicMock()),
    )

    fake_db = MagicMock()
    fake_db.fetch_one = AsyncMock(return_value=None)
    fake_db.execute = AsyncMock()
    monkeypatch.setattr(playback_module, "database", fake_db)

    monkeypatch.setattr(playback_module, "get_client_ip", lambda request: "1.2.3.4")

    fake_scraper = MagicMock()
    fake_scraper.fetch_metadata_and_aliases = AsyncMock(return_value=(None, {}))
    monkeypatch.setattr(
        playback_module, "MetadataScraper", lambda session: fake_scraper
    )

    prefetch_mock = AsyncMock()
    monkeypatch.setattr(playback_module, "prefetch_next_episode", prefetch_mock)

    status_response = MagicMock(name="status_video_response")
    monkeypatch.setattr(
        playback_module,
        "build_status_video_response",
        lambda *a, **k: status_response,
    )

    debrid = MagicMock()
    monkeypatch.setattr(playback_module, "get_debrid", lambda *a, **k: debrid)

    fake_logger = MagicMock()
    monkeypatch.setattr(playback_module, "logger", fake_logger, raising=False)

    return SimpleNamespace(
        prefetch=prefetch_mock,
        debrid=debrid,
        status_response=status_response,
        logger=fake_logger,
    )


async def _call_playback():
    response = await playback_module.playback(
        MagicMock(),
        "b64config",
        "deadbeefcafe",
        "0",
        "0",
        "1",
        "2",
        torrent_name="Show.S01.Pack",
        name="Show",
        media_id="tt123",
    )
    # Let the fire-and-forget prefetch task (if any) actually run.
    pending = list(playback_module._PREFETCH_TASKS)
    if pending:
        await asyncio.gather(*pending)
    return response


@pytest.mark.asyncio
async def test_prefetch_fires_when_link_generation_raises(playback_env):
    playback_env.debrid.generate_download_link = AsyncMock(
        side_effect=DebridLinkGenerationError("realdebrid", "boom")
    )

    response = await _call_playback()

    playback_env.prefetch.assert_awaited_once()
    assert response is playback_env.status_response


@pytest.mark.asyncio
async def test_prefetch_fires_when_link_generation_returns_empty(playback_env):
    playback_env.debrid.generate_download_link = AsyncMock(return_value="")

    response = await _call_playback()

    playback_env.prefetch.assert_awaited_once()
    assert response is playback_env.status_response


@pytest.mark.asyncio
async def test_prefetch_still_fires_on_successful_link_generation(playback_env):
    playback_env.debrid.generate_download_link = AsyncMock(
        return_value="https://example.com/file.mkv"
    )

    await _call_playback()

    playback_env.prefetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_prefetch_skipped_for_movies(playback_env):
    """season=None (movie) must not trigger a next-episode prefetch."""
    playback_env.debrid.generate_download_link = AsyncMock(
        return_value="https://example.com/file.mkv"
    )

    await playback_module.playback(
        MagicMock(),
        "b64config",
        "deadbeefcafe",
        "0",
        "0",
        "n",
        "n",
        torrent_name="Movie.2026",
        name="Movie",
        media_id="tt123",
    )
    pending = list(playback_module._PREFETCH_TASKS)
    if pending:
        await asyncio.gather(*pending)

    playback_env.prefetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_link_generation_error_is_logged(playback_env):
    playback_env.debrid.generate_download_link = AsyncMock(
        side_effect=DebridLinkGenerationError("realdebrid", "boom")
    )

    await _call_playback()

    logged = str(playback_env.logger.log.call_args_list)
    assert "deadbeefcafe" in logged
    assert "boom" in logged


@pytest.mark.asyncio
async def test_empty_download_url_is_logged(playback_env):
    playback_env.debrid.generate_download_link = AsyncMock(return_value="")

    await _call_playback()

    logged = str(playback_env.logger.log.call_args_list)
    assert "deadbeefcafe" in logged
