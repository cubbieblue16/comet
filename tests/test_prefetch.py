import pytest

from comet.services.prefetch import _select_target_hash


def _cache(*hashes):
    """Build a service_cache_status marking each hash cached on 'torbox'."""
    return {h: {"torbox": True} for h in hashes}


def test_select_prefers_played_hash_when_cached_candidate():
    ranked = ["aaa", "bbb", "ccc"]
    torrents = {"aaa": {}, "bbb": {}, "ccc": {}}
    status = _cache("aaa", "bbb", "ccc")
    # Played episode came from the season pack "ccc"; auto-advance reuses it.
    assert _select_target_hash(ranked, torrents, status, "torbox", "ccc") == "ccc"


def test_select_falls_back_when_played_hash_not_a_candidate():
    # Per-episode torrent for N: its hash isn't in N+1's torrent set.
    ranked = ["aaa", "bbb"]
    torrents = {"aaa": {}, "bbb": {}}
    status = _cache("aaa", "bbb")
    assert _select_target_hash(ranked, torrents, status, "torbox", "zzz") == "aaa"


def test_select_falls_back_when_played_hash_uncached():
    ranked = ["aaa", "bbb"]
    torrents = {"aaa": {}, "bbb": {}, "ccc": {}}
    status = _cache("aaa", "bbb")  # ccc present but NOT cached
    assert _select_target_hash(ranked, torrents, status, "torbox", "ccc") == "aaa"


def test_select_returns_top_ranked_when_no_played_hash():
    ranked = ["aaa", "bbb"]
    torrents = {"aaa": {}, "bbb": {}}
    status = _cache("bbb")  # only bbb cached; aaa ranked first but uncached
    assert _select_target_hash(ranked, torrents, status, "torbox", None) == "bbb"


def test_select_returns_none_when_nothing_cached():
    ranked = ["aaa", "bbb"]
    torrents = {"aaa": {}, "bbb": {}}
    status = {}
    assert _select_target_hash(ranked, torrents, status, "torbox", "aaa") is None


@pytest.fixture(autouse=True)
def _clear_recently_warmed():
    from comet.services import prefetch
    prefetch._RECENTLY_WARMED.clear()
    yield
    prefetch._RECENTLY_WARMED.clear()


def test_recently_warmed_false_when_absent():
    from comet.services.prefetch import _recently_warmed
    assert _recently_warmed("tt1:1:2|torbox", now=1000.0, ttl=300) is False


def test_mark_then_recently_warmed_true_within_ttl():
    from comet.services.prefetch import _mark_warmed, _recently_warmed
    _mark_warmed("tt1:1:2|torbox", now=1000.0, ttl=300)
    assert _recently_warmed("tt1:1:2|torbox", now=1200.0, ttl=300) is True


def test_recently_warmed_false_after_ttl():
    from comet.services.prefetch import _mark_warmed, _recently_warmed
    _mark_warmed("tt1:1:2|torbox", now=1000.0, ttl=300)
    assert _recently_warmed("tt1:1:2|torbox", now=1301.0, ttl=300) is False


def test_mark_warmed_prunes_stale_entries():
    from comet.services import prefetch
    prefetch._mark_warmed("old|torbox", now=1000.0, ttl=300)
    # A later mark past the TTL should evict the stale "old" key.
    prefetch._mark_warmed("new|torbox", now=2000.0, ttl=300)
    assert "old|torbox" not in prefetch._RECENTLY_WARMED
    assert "new|torbox" in prefetch._RECENTLY_WARMED


@pytest.mark.asyncio
async def test_warm_next_episode_returns_early_when_no_next_episode(monkeypatch):
    """If metadata says there's no next episode, we must not build a TorrentManager."""
    from unittest.mock import AsyncMock, MagicMock
    import comet.services.prefetch as prefetch

    # Metadata lookup says "no next episode".
    fake_scraper = MagicMock()
    fake_scraper.fetch_metadata_and_aliases = AsyncMock(return_value=(None, {}))
    monkeypatch.setattr(prefetch, "MetadataScraper", lambda session: fake_scraper)

    fake_index = MagicMock()
    fake_index.get_target_air_date = AsyncMock(return_value=None)
    monkeypatch.setattr(prefetch, "EpisodeIndexService", lambda session: fake_index)

    # If the early-return breaks, this would be constructed -> blow up the test.
    def _boom(*a, **k):
        raise AssertionError("TorrentManager must not be built when no next episode")

    monkeypatch.setattr(prefetch, "TorrentManager", _boom)

    await prefetch._warm_next_episode(
        session=MagicMock(),
        config={"removeTrash": False},
        media_only_id="tt0386676",
        next_media_id="tt0386676:3:13",
        season=3,
        next_episode=13,
        debrid_entries=[{"service": "torbox", "apiKey": "k"}],
        debrid_service="torbox",
        debrid_api_key="k",
        ip="",
        played_hash="abc",
    )

    fake_scraper.fetch_metadata_and_aliases.assert_awaited_once()
    fake_index.get_target_air_date.assert_awaited_once()
