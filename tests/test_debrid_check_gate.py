"""The DEBRID_CACHE_CHECK_RATIO gate must be evaluated per service.

Taking the max verdict count across services lets a fully-verdicted
RealDebrid suppress the live check for a verdict-less TorBox (and vice
versa), leaving the second service's results stale."""

from comet.api.endpoints.stream import select_entries_needing_debrid_check

RD = {"service": "realdebrid", "apiKey": "k1"}
TB = {"service": "torbox", "apiKey": "k2"}


def test_unverdicted_service_checked_even_when_other_fully_verdicted():
    selected = select_entries_needing_debrid_check(
        [RD, TB],
        {"realdebrid": 10, "torbox": 0},
        total_count=10,
        has_cached_torrents=True,
        use_account_scrape=False,
        check_ratio=0.5,
    )
    assert selected == [TB]


def test_no_services_checked_when_all_above_ratio():
    selected = select_entries_needing_debrid_check(
        [RD, TB],
        {"realdebrid": 10, "torbox": 6},
        total_count=10,
        has_cached_torrents=True,
        use_account_scrape=False,
        check_ratio=0.5,
    )
    assert selected == []


def test_service_below_ratio_checked():
    selected = select_entries_needing_debrid_check(
        [RD, TB],
        {"realdebrid": 10, "torbox": 4},
        total_count=10,
        has_cached_torrents=True,
        use_account_scrape=False,
        check_ratio=0.5,
    )
    assert selected == [TB]


def test_all_services_checked_on_cold_cache():
    selected = select_entries_needing_debrid_check(
        [RD, TB],
        {},
        total_count=10,
        has_cached_torrents=False,
        use_account_scrape=False,
        check_ratio=0.5,
    )
    assert selected == [RD, TB]


def test_service_with_failed_verdict_count_is_checked():
    # A service missing from the counts map (count query errored) fails open.
    selected = select_entries_needing_debrid_check(
        [RD, TB],
        {"realdebrid": 10},
        total_count=10,
        has_cached_torrents=True,
        use_account_scrape=False,
        check_ratio=0.5,
    )
    assert selected == [TB]


def test_empty_when_no_torrents():
    selected = select_entries_needing_debrid_check(
        [RD],
        {"realdebrid": 0},
        total_count=0,
        has_cached_torrents=False,
        use_account_scrape=False,
        check_ratio=0.5,
    )
    assert selected == []
