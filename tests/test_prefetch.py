from unittest.mock import AsyncMock, MagicMock

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


def _patch_metadata(monkeypatch, prefetch, metadata):
    fake_scraper = MagicMock()
    fake_scraper.fetch_metadata_and_aliases = AsyncMock(return_value=(metadata, {}))
    monkeypatch.setattr(prefetch, "MetadataScraper", lambda session: fake_scraper)

    fake_index = MagicMock()
    fake_index.get_target_air_date = AsyncMock(return_value=None)
    monkeypatch.setattr(prefetch, "EpisodeIndexService", lambda session: fake_index)


def _next_episode_metadata():
    return {
        "title": "Show",
        "year": 2020,
        "year_end": None,
        "season": 1,
        "episode": 2,
    }


class _FakeTorrentManager:
    """Stands in for TorrentManager: cached rows exist but may be stale."""

    def __init__(self, cached_torrents, primary_cached):
        self._cached_torrents = cached_torrents
        self._primary_cached = primary_cached
        self.torrents = {}
        self.primary_cached = False
        self.get_cached_torrents = AsyncMock(side_effect=self._load_cache)
        self.scrape_torrents = AsyncMock()

    async def _load_cache(self):
        self.torrents.update(self._cached_torrents)
        self.primary_cached = self._primary_cached


def _warm_kwargs(played_hash="abc"):
    return dict(
        session=MagicMock(),
        config={"removeTrash": False},
        media_only_id="tt0386676",
        next_media_id="tt0386676:1:2",
        season=1,
        next_episode=2,
        debrid_entries=[{"service": "torbox", "apiKey": "k"}],
        debrid_service="torbox",
        debrid_api_key="k",
        ip="",
        played_hash=played_hash,
    )


@pytest.mark.asyncio
async def test_warm_scrapes_when_cached_rows_exist_but_none_fresh(monkeypatch):
    """primary_cached alone must not suppress scraping: stale rows miss the
    freshly released N+1 torrents (the observed RD failure: 5 stale candidates
    warmed while a scrape would have found the cached season pack)."""
    import comet.services.prefetch as prefetch

    _patch_metadata(monkeypatch, prefetch, _next_episode_metadata())

    fake_tm = _FakeTorrentManager(cached_torrents={}, primary_cached=True)
    monkeypatch.setattr(prefetch, "TorrentManager", lambda *a, **k: fake_tm)

    fake_state = MagicMock()
    fake_state.get_fresh_torrent_count = AsyncMock(return_value=0)
    monkeypatch.setattr(
        prefetch, "CacheStateManager", lambda *a, **k: fake_state, raising=False
    )

    fake_db = MagicMock()
    fake_db.fetch_one = AsyncMock(return_value=None)
    monkeypatch.setattr(prefetch, "database", fake_db)

    await prefetch._warm_next_episode(**_warm_kwargs())

    fake_tm.scrape_torrents.assert_awaited_once()


@pytest.mark.asyncio
async def test_warm_skips_scrape_when_fresh_torrents_exist(monkeypatch):
    import comet.services.prefetch as prefetch
    import comet.api.endpoints.stream as stream

    _patch_metadata(monkeypatch, prefetch, _next_episode_metadata())
    monkeypatch.setattr(
        prefetch.settings, "PREFETCH_NEXT_EPISODE_RESOLVE_LINK", False
    )

    fake_tm = _FakeTorrentManager(
        cached_torrents={"abc": {"title": "Show.S01"}}, primary_cached=True
    )
    monkeypatch.setattr(prefetch, "TorrentManager", lambda *a, **k: fake_tm)

    fake_state = MagicMock()
    fake_state.get_fresh_torrent_count = AsyncMock(return_value=1)
    monkeypatch.setattr(
        prefetch, "CacheStateManager", lambda *a, **k: fake_state, raising=False
    )

    monkeypatch.setattr(
        stream,
        "get_and_cache_multi_service_availability",
        AsyncMock(return_value=({}, {})),
    )

    await prefetch._warm_next_episode(**_warm_kwargs(played_hash="abc"))

    fake_tm.scrape_torrents.assert_not_awaited()


@pytest.mark.asyncio
async def test_played_hash_injected_from_torrents_table_when_absent(monkeypatch):
    """A played season pack missing from N+1's candidate set must be pulled in
    from the torrents table so the availability check can verify it at scope."""
    import comet.services.prefetch as prefetch
    import orjson

    fake_db = MagicMock()
    fake_db.fetch_one = AsyncMock(
        return_value={
            "title": "Show.S01.1080p.Pack",
            "seeders": 50,
            "size": 9999,
            "tracker": "tracker",
            "sources_json": orjson.dumps(["udp://t"]).decode(),
            "parsed_json": orjson.dumps(
                {"raw_title": "Show.S01.1080p.Pack", "parsed_title": "Show"}
            ).decode(),
        }
    )
    monkeypatch.setattr(prefetch, "database", fake_db)

    manager = _FakeTorrentManager(cached_torrents={}, primary_cached=False)
    await prefetch._ensure_played_hash_candidate(manager, "packhash", "tt0386676")

    assert "packhash" in manager.torrents
    injected = manager.torrents["packhash"]
    # fileIndex must NOT be carried over: any cached index belongs to the
    # episode just played. The availability check fills the at-scope index.
    assert injected["fileIndex"] is None
    assert injected["sources"] == ["udp://t"]
    assert injected["parsed"].raw_title == "Show.S01.1080p.Pack"


@pytest.mark.asyncio
async def test_played_hash_injection_skipped_when_already_candidate(monkeypatch):
    import comet.services.prefetch as prefetch

    fake_db = MagicMock()
    fake_db.fetch_one = AsyncMock(return_value=None)
    monkeypatch.setattr(prefetch, "database", fake_db)

    manager = _FakeTorrentManager(
        cached_torrents={"packhash": {"title": "existing"}}, primary_cached=True
    )
    await manager._load_cache()
    await prefetch._ensure_played_hash_candidate(manager, "packhash", "tt0386676")

    fake_db.fetch_one.assert_not_awaited()
    assert manager.torrents["packhash"] == {"title": "existing"}


@pytest.mark.asyncio
async def test_played_hash_injection_skipped_when_not_in_torrents_table(monkeypatch):
    import comet.services.prefetch as prefetch

    fake_db = MagicMock()
    fake_db.fetch_one = AsyncMock(return_value=None)
    monkeypatch.setattr(prefetch, "database", fake_db)

    manager = _FakeTorrentManager(cached_torrents={}, primary_cached=False)
    await prefetch._ensure_played_hash_candidate(manager, "unknown", "tt0386676")

    assert "unknown" not in manager.torrents


@pytest.mark.asyncio
async def test_warm_passes_injected_played_hash_to_availability_check(monkeypatch):
    """The injected played hash must be part of the availability check input,
    otherwise it can never be selected for Part B link pre-resolution."""
    import comet.services.prefetch as prefetch
    import comet.api.endpoints.stream as stream
    import orjson

    _patch_metadata(monkeypatch, prefetch, _next_episode_metadata())
    monkeypatch.setattr(
        prefetch.settings, "PREFETCH_NEXT_EPISODE_RESOLVE_LINK", False
    )

    fake_tm = _FakeTorrentManager(
        cached_torrents={"other": {"title": "Show.S01E02"}}, primary_cached=True
    )
    monkeypatch.setattr(prefetch, "TorrentManager", lambda *a, **k: fake_tm)

    fake_state = MagicMock()
    fake_state.get_fresh_torrent_count = AsyncMock(return_value=1)
    monkeypatch.setattr(
        prefetch, "CacheStateManager", lambda *a, **k: fake_state, raising=False
    )

    fake_db = MagicMock()
    fake_db.fetch_one = AsyncMock(
        return_value={
            "title": "Show.S01.Pack",
            "seeders": 10,
            "size": 123,
            "tracker": "t",
            "sources_json": orjson.dumps([]).decode(),
            "parsed_json": orjson.dumps(
                {"raw_title": "Show.S01.Pack", "parsed_title": "Show"}
            ).decode(),
        }
    )
    monkeypatch.setattr(prefetch, "database", fake_db)

    availability_mock = AsyncMock(return_value=({}, {}))
    monkeypatch.setattr(
        stream, "get_and_cache_multi_service_availability", availability_mock
    )

    await prefetch._warm_next_episode(**_warm_kwargs(played_hash="packhash"))

    availability_mock.assert_awaited_once()
    torrents_arg = availability_mock.await_args.args[2]
    assert "packhash" in torrents_arg


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
