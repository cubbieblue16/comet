"""Age-aware LIVE_TORRENT_CACHE_TTL: a recent episode (aired within
RECENT_EPISODE_WINDOW_DAYS, or not yet aired) uses the much shorter
RECENT_EPISODE_TORRENT_CACHE_TTL so a thin pre-air/air-minute scrape can't
freeze the live cache over release night. A recent episode whose cache is
stale and too thin escalates to a foreground scrape (THIN_RECENT)."""

from datetime import date, timedelta

from comet.core.models import settings
from comet.services.cache_state import (CacheState, CacheStateManager,
                                        is_recent_episode)

TODAY = date(2026, 9, 3)


def _iso(delta_days: int) -> str:
    return (TODAY + timedelta(days=delta_days)).isoformat()


def _manager(target_air_date=None) -> CacheStateManager:
    return CacheStateManager(
        media_id="tt0000000",
        media_only_id="tt0000000",
        season=1,
        episode=1,
        search_episode=1,
        search_season=1,
        cache_media_ids=["tt0000000"],
        target_air_date=target_air_date,
    )


# --- is_recent_episode --------------------------------------------------


def test_aired_10_days_ago_is_not_recent():
    assert is_recent_episode(_iso(-10), now=TODAY) is False


def test_aired_yesterday_is_recent():
    assert is_recent_episode(_iso(-1), now=TODAY) is True


def test_airs_in_6_days_is_recent():
    assert is_recent_episode(_iso(6), now=TODAY) is True


def test_none_air_date_is_not_recent():
    assert is_recent_episode(None, now=TODAY) is False


def test_garbage_air_date_is_not_recent():
    assert is_recent_episode("not-a-date", now=TODAY) is False


def test_disabled_when_setting_is_none(monkeypatch):
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", None)
    assert is_recent_episode(_iso(-1), now=TODAY) is False


# --- _effective_live_ttl -------------------------------------------------


def test_effective_ttl_recent_uses_short_ttl(monkeypatch):
    monkeypatch.setattr(settings, "LIVE_TORRENT_CACHE_TTL", 172800)
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", 3600)
    monkeypatch.setattr(settings, "RECENT_EPISODE_WINDOW_DAYS", 3)
    manager = _manager(target_air_date=_iso(-1))
    assert manager._effective_live_ttl() == 3600


def test_effective_ttl_not_recent_uses_base(monkeypatch):
    monkeypatch.setattr(settings, "LIVE_TORRENT_CACHE_TTL", 172800)
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", 3600)
    monkeypatch.setattr(settings, "RECENT_EPISODE_WINDOW_DAYS", 3)
    manager = _manager(target_air_date=_iso(-10))
    assert manager._effective_live_ttl() == 172800


def test_effective_ttl_never_expire_base_still_gated_when_recent(monkeypatch):
    monkeypatch.setattr(settings, "LIVE_TORRENT_CACHE_TTL", -1)
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", 3600)
    monkeypatch.setattr(settings, "RECENT_EPISODE_WINDOW_DAYS", 3)
    manager = _manager(target_air_date=_iso(-1))
    assert manager._effective_live_ttl() == 3600


# --- _determine_state / decision -----------------------------------------


def test_recent_stale_thin_is_thin_recent(monkeypatch):
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", 3600)
    monkeypatch.setattr(settings, "RECENT_EPISODE_WINDOW_DAYS", 3)
    monkeypatch.setattr(settings, "RECENT_EPISODE_THIN_CACHE_THRESHOLD", 10)
    manager = _manager(target_air_date=_iso(-1))
    state = manager._determine_state(fresh_count=0, torrent_count=4, is_first=False)
    assert state == CacheState.THIN_RECENT


def test_recent_stale_not_thin_is_stale(monkeypatch):
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", 3600)
    monkeypatch.setattr(settings, "RECENT_EPISODE_WINDOW_DAYS", 3)
    monkeypatch.setattr(settings, "RECENT_EPISODE_THIN_CACHE_THRESHOLD", 10)
    manager = _manager(target_air_date=_iso(-1))
    state = manager._determine_state(fresh_count=0, torrent_count=50, is_first=False)
    assert state == CacheState.STALE


def test_not_recent_stale_thin_is_stale(monkeypatch):
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", 3600)
    monkeypatch.setattr(settings, "RECENT_EPISODE_WINDOW_DAYS", 3)
    monkeypatch.setattr(settings, "RECENT_EPISODE_THIN_CACHE_THRESHOLD", 10)
    manager = _manager(target_air_date=_iso(-10))
    state = manager._determine_state(fresh_count=0, torrent_count=4, is_first=False)
    assert state == CacheState.STALE


def test_recent_fresh_thin_is_fresh(monkeypatch):
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", 3600)
    monkeypatch.setattr(settings, "RECENT_EPISODE_WINDOW_DAYS", 3)
    monkeypatch.setattr(settings, "RECENT_EPISODE_THIN_CACHE_THRESHOLD", 10)
    manager = _manager(target_air_date=_iso(-1))
    state = manager._determine_state(fresh_count=1, torrent_count=4, is_first=False)
    assert state == CacheState.FRESH


def test_threshold_none_disables_thin_recent(monkeypatch):
    monkeypatch.setattr(settings, "RECENT_EPISODE_TORRENT_CACHE_TTL", 3600)
    monkeypatch.setattr(settings, "RECENT_EPISODE_WINDOW_DAYS", 3)
    monkeypatch.setattr(settings, "RECENT_EPISODE_THIN_CACHE_THRESHOLD", None)
    manager = _manager(target_air_date=_iso(-1))
    state = manager._determine_state(fresh_count=0, torrent_count=4, is_first=False)
    assert state == CacheState.STALE


def test_thin_recent_decision_lock_acquired_is_foreground(monkeypatch):
    manager = _manager(target_air_date=_iso(-1))
    decision = manager._determine_decision(CacheState.THIN_RECENT, lock_acquired=True)
    from comet.services.cache_state import ScrapeDecision

    assert decision == ScrapeDecision.SCRAPE_FOREGROUND


def test_thin_recent_decision_no_lock_is_background(monkeypatch):
    manager = _manager(target_air_date=_iso(-1))
    decision = manager._determine_decision(CacheState.THIN_RECENT, lock_acquired=False)
    from comet.services.cache_state import ScrapeDecision

    assert decision == ScrapeDecision.SCRAPE_BACKGROUND
