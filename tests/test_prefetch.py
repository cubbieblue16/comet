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
