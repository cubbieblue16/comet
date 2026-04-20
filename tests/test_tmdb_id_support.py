"""Tests for TMDB ID support: parsing, metadata, and external-id resolution.

These cover the changes that let Comet accept `tmdb:` prefixed media IDs from
Stremio addons (e.g. the TMDB addon used to look up WWE PPVs like
WrestleMania 42 that aren't on IMDB).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest


def _mock_session_returning_json(status: int, json_payload: dict):
    mock_response = AsyncMock()
    mock_response.status = status
    mock_response.json = AsyncMock(return_value=json_payload)

    mock_session = MagicMock()
    mock_session.get = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_response),
            __aexit__=AsyncMock(return_value=False),
        )
    )
    return mock_session


def test_parse_media_id_tmdb_movie():
    from comet.utils.parsing import parse_media_id

    assert parse_media_id("movie", "tmdb:1433456") == ("1433456", None, None)


def test_parse_media_id_tmdb_series_with_season_and_episode():
    from comet.utils.parsing import parse_media_id

    assert parse_media_id("series", "tmdb:55555:3:7") == ("55555", 3, 7)


def test_parse_media_id_tmdb_series_season_only():
    from comet.utils.parsing import parse_media_id

    assert parse_media_id("series", "tmdb:55555:3") == ("55555", 3, None)


def test_parse_media_id_imdb_still_works():
    from comet.utils.parsing import parse_media_id

    assert parse_media_id("movie", "tt1234567") == ("tt1234567", None, None)
    assert parse_media_id("series", "tt1234567:2:5") == ("tt1234567", 2, 5)


def test_parse_media_id_kitsu_still_works():
    from comet.utils.parsing import parse_media_id

    assert parse_media_id("series", "kitsu:12345:3") == ("12345", 1, 3)


@pytest.mark.asyncio
async def test_tmdb_get_external_ids_movie_success():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(
        200, {"imdb_id": "tt9999999", "tvdb_id": None}
    )
    api = TMDBApi(mock_session)
    result = await api.get_external_ids("1433456", "movie")

    assert result == {"imdb_id": "tt9999999", "tvdb_id": None}
    called_url = mock_session.get.call_args[0][0]
    assert called_url.endswith("/movie/1433456/external_ids")


@pytest.mark.asyncio
async def test_tmdb_get_external_ids_series_uses_tv_path():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(200, {"imdb_id": "tt7777777"})
    api = TMDBApi(mock_session)
    await api.get_external_ids("99", "series")

    called_url = mock_session.get.call_args[0][0]
    assert called_url.endswith("/tv/99/external_ids")


@pytest.mark.asyncio
async def test_tmdb_get_imdb_from_tmdb_returns_imdb_id():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(200, {"imdb_id": "tt1234567"})
    api = TMDBApi(mock_session)
    assert await api.get_imdb_from_tmdb("111", "movie") == "tt1234567"


@pytest.mark.asyncio
async def test_tmdb_get_imdb_from_tmdb_returns_none_when_missing():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(200, {"imdb_id": None})
    api = TMDBApi(mock_session)
    assert await api.get_imdb_from_tmdb("111", "movie") is None


@pytest.mark.asyncio
async def test_tmdb_get_imdb_from_tmdb_rejects_non_tt_values():
    from comet.metadata.tmdb import TMDBApi

    # TMDB occasionally returns empty strings or weird values; only accept real tt IDs.
    mock_session = _mock_session_returning_json(200, {"imdb_id": "notanid"})
    api = TMDBApi(mock_session)
    assert await api.get_imdb_from_tmdb("111", "movie") is None


@pytest.mark.asyncio
async def test_tmdb_get_imdb_from_tmdb_handles_http_error():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(404, {})
    api = TMDBApi(mock_session)
    assert await api.get_imdb_from_tmdb("111", "movie") is None


@pytest.mark.asyncio
async def test_tmdb_get_movie_metadata_success():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(
        200,
        {
            "title": "WrestleMania 42",
            "release_date": "2026-04-19",
        },
    )
    api = TMDBApi(mock_session)
    assert await api.get_movie_metadata("1433456") == ("WrestleMania 42", 2026, None)


@pytest.mark.asyncio
async def test_tmdb_get_movie_metadata_falls_back_to_original_title():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(
        200,
        {
            "title": None,
            "original_title": "Fallback Title",
            "release_date": "",
        },
    )
    api = TMDBApi(mock_session)
    assert await api.get_movie_metadata("1") == ("Fallback Title", None, None)


@pytest.mark.asyncio
async def test_tmdb_get_tv_metadata_ongoing_show_has_no_year_end():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(
        200,
        {
            "name": "Some Show",
            "first_air_date": "2020-01-01",
            "last_air_date": "2020-02-02",
            "in_production": True,
        },
    )
    api = TMDBApi(mock_session)
    # Ongoing show with last_air_date year == first_air_date year: year_end suppressed.
    assert await api.get_tv_metadata("10") == ("Some Show", 2020, None)


@pytest.mark.asyncio
async def test_tmdb_get_tv_metadata_ended_show_reports_year_end():
    from comet.metadata.tmdb import TMDBApi

    mock_session = _mock_session_returning_json(
        200,
        {
            "name": "Ended Show",
            "first_air_date": "2010-05-05",
            "last_air_date": "2015-08-08",
            "in_production": False,
        },
    )
    api = TMDBApi(mock_session)
    assert await api.get_tv_metadata("11") == ("Ended Show", 2010, 2015)


def test_metadata_manager_extracts_tmdb_provider():
    from comet.metadata.manager import MetadataScraper

    assert MetadataScraper._extract_provider("tmdb:12345") == "tmdb"
    assert MetadataScraper._extract_provider("tmdb:12345:1:1") == "tmdb"
    assert MetadataScraper._extract_provider("tt1234567") == "imdb"
    assert MetadataScraper._extract_provider("kitsu:1") == "kitsu"
