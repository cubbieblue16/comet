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


@pytest.mark.asyncio
async def test_resolver_resolves_date_to_episode():
    from comet.services.date_episode_resolver import DateEpisodeResolver

    mock_session = MagicMock()
    resolver = DateEpisodeResolver(mock_session)

    with patch.object(resolver, '_get_tmdb_id', new_callable=AsyncMock, return_value="12345"):
        with patch.object(resolver, '_get_season_date_map', new_callable=AsyncMock, return_value={
            "2026-01-05": 9,
            "2026-01-06": 10,
        }):
            season, episode = await resolver.resolve(
                "tt0115147",
                "The.Daily.Show.2026.01.05.Mark.Kelly.720p.WEB.h264-EDITH",
                search_season=31,
            )
            assert season == 31
            assert episode == 9


@pytest.mark.asyncio
async def test_resolver_returns_none_when_no_date_in_title():
    from comet.services.date_episode_resolver import DateEpisodeResolver

    mock_session = MagicMock()
    resolver = DateEpisodeResolver(mock_session)

    season, episode = await resolver.resolve(
        "tt0115147",
        "Breaking.Bad.S05E16.Felina.1080p.BluRay",
        search_season=5,
    )
    assert season is None
    assert episode is None


@pytest.mark.asyncio
async def test_resolver_returns_none_when_date_not_found_in_schedule():
    from comet.services.date_episode_resolver import DateEpisodeResolver

    mock_session = MagicMock()
    resolver = DateEpisodeResolver(mock_session)

    with patch.object(resolver, '_get_tmdb_id', new_callable=AsyncMock, return_value="12345"):
        with patch.object(resolver, '_get_season_date_map', new_callable=AsyncMock, return_value={
            "2026-01-06": 10,
        }):
            season, episode = await resolver.resolve(
                "tt0115147",
                "The.Daily.Show.2026.01.05.Mark.Kelly.720p.WEB.h264-EDITH",
                search_season=31,
            )
            assert season is None
            assert episode is None


@pytest.mark.asyncio
async def test_resolver_caches_tmdb_lookups():
    """TMDB lookups should be cached so multiple calls for the same show+season don't re-fetch."""
    from comet.services.date_episode_resolver import DateEpisodeResolver, _tmdb_id_cache, _season_date_cache

    # Clear module-level caches to avoid interference from other tests
    _tmdb_id_cache.clear()
    _season_date_cache.clear()

    mock_session = MagicMock()
    resolver = DateEpisodeResolver(mock_session)

    date_map = {"2026-01-05": 9, "2026-01-06": 10}

    # Patch the TMDB API methods (not the resolver's cache methods) to test caching
    with patch.object(resolver._tmdb, 'get_tmdb_id_from_imdb', new_callable=AsyncMock, return_value="12345") as mock_tmdb_api:
        with patch.object(resolver._tmdb, 'get_season_episodes', new_callable=AsyncMock, return_value=date_map) as mock_season_api:
            await resolver.resolve("tt0115147", "The.Daily.Show.2026.01.05.Title.720p", search_season=31)
            await resolver.resolve("tt0115147", "The.Daily.Show.2026.01.06.Title.720p", search_season=31)

            # TMDB API should be called once (cached by resolver)
            assert mock_tmdb_api.call_count == 1
            # Season episodes API should be called once (cached by resolver)
            assert mock_season_api.call_count == 1

    # Clean up
    _tmdb_id_cache.clear()
    _season_date_cache.clear()


@pytest.mark.asyncio
async def test_resolver_season_fallback_includes_season_zero():
    """Season 0 (specials) should be checked in the fallback -- WWE PPVs are often specials."""
    from comet.services.date_episode_resolver import DateEpisodeResolver

    mock_session = MagicMock()
    resolver = DateEpisodeResolver(mock_session)

    seasons_list = [
        {"season_number": 0, "air_date": "2024-01-01"},
        {"season_number": 1, "air_date": "2024-06-01"},
    ]

    with patch.object(resolver, '_get_tmdb_id', new_callable=AsyncMock, return_value="99999"):
        with patch.object(resolver, '_get_seasons_list', new_callable=AsyncMock, return_value=seasons_list):
            with patch.object(resolver, '_get_season_date_map', new_callable=AsyncMock) as mock_date_map:
                # search_season=1 finds nothing, fallback to season 0 which has it
                mock_date_map.side_effect = [
                    {},  # season 1 (searched first via search_season)
                    {"2024-04-06": 5},  # season 0 (fallback)
                ]
                season, episode = await resolver.resolve(
                    "tt9999999",
                    "WWE.WrestleMania.40.2024.04.06.720p",
                    search_season=1,
                )
                assert season == 0
                assert episode == 5
