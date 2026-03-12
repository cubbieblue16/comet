import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_get_season_episodes_returns_date_map():
    """get_season_episodes should return a dict mapping air_date -> episode_number."""
    from comet.metadata.tmdb import TMDBApi

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={
        "episodes": [
            {"episode_number": 1, "air_date": "2026-01-06"},
            {"episode_number": 2, "air_date": "2026-01-07"},
            {"episode_number": 9, "air_date": "2026-01-05"},
        ]
    })

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock(return_value=False)))

    api = TMDBApi(mock_session)
    result = await api.get_season_episodes("12345", 31)

    assert result == {
        "2026-01-06": 1,
        "2026-01-07": 2,
        "2026-01-05": 9,
    }


@pytest.mark.asyncio
async def test_get_season_episodes_handles_api_error():
    from comet.metadata.tmdb import TMDBApi

    mock_response = AsyncMock()
    mock_response.status = 404
    mock_response.json = AsyncMock(return_value={})

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock(return_value=False)))

    api = TMDBApi(mock_session)
    result = await api.get_season_episodes("99999", 1)

    assert result == {}


@pytest.mark.asyncio
async def test_get_seasons_for_show_returns_season_list():
    from comet.metadata.tmdb import TMDBApi

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={
        "seasons": [
            {"season_number": 0, "air_date": "2024-01-01"},
            {"season_number": 1, "air_date": "2024-06-01"},
            {"season_number": 2, "air_date": "2025-06-01"},
        ]
    })

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock(return_value=False)))

    api = TMDBApi(mock_session)
    result = await api.get_seasons_for_show("12345")

    assert len(result) == 3
    assert result[0]["season_number"] == 0
    assert result[2]["season_number"] == 2
