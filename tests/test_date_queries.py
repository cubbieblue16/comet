"""Unit tests for the shared date-based fallback query helpers, and a
regression test for the BitMagnet scraper's date-fallback path added on
top of the existing imdb-scoped tvsearch."""

from types import SimpleNamespace

import pytest

from comet.scrapers.bitmagnet import BitmagnetScraper
from comet.scrapers.helpers.date_queries import (air_date_variants,
                                                  build_date_queries,
                                                  clean_query_title)
from comet.scrapers.models import ScrapeRequest


def test_clean_query_title_strips_bang():
    assert clean_query_title("WWE Smackdown!") == "WWE Smackdown"


def test_clean_query_title_strips_colon():
    assert clean_query_title("Late Night: Show") == "Late Night Show"


def test_air_date_variants_order():
    assert air_date_variants("2026-08-15") == [
        "2026-08-15",
        "2026-08-14",
        "2026-08-16",
    ]


def test_air_date_variants_year_boundary():
    assert air_date_variants("2026-01-01") == [
        "2026-01-01",
        "2025-12-31",
        "2026-01-02",
    ]


def test_air_date_variants_invalid():
    assert air_date_variants("not-a-date") == []
    assert air_date_variants("") == []
    assert air_date_variants(None) == []


def test_build_date_queries_contains_expected_variants():
    queries = build_date_queries("WWE Smackdown!", "2026-08-15", 28, 33)

    assert "WWE Smackdown 2026.08.14" in queries
    assert "WWE Smackdown 2026.08.15" in queries
    assert "WWE Smackdown 2026.08.16" in queries
    assert "WWE Smackdown S28E33" in queries
    assert len(queries) == len(set(queries))
    assert "!" not in "".join(queries)


def test_build_date_queries_none_air_date():
    assert build_date_queries("X", None, 1, 2) == []


# --- BitMagnet scraper date-fallback integration ---


class _FakeResponse:
    def __init__(self, text: str):
        self._text = text

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


def _torznab_xml(title: str, info_hash: str):
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:torznab="http://torznab.com/schemas/2015/feed">
  <channel>
    <item>
      <title>{title}</title>
      <torznab:attr name="size" value="123456" />
      <torznab:attr name="infohash" value="{info_hash}" />
      <torznab:attr name="seeders" value="5" />
    </item>
  </channel>
</rss>"""


class _FakeSession:
    """Only serves t=search date-fallback calls; scrape_page is monkeypatched
    away so the imdb-scoped tvsearch path never touches this. The first
    three date-variant queries all resolve to the same torrent (dup_hash,
    simulating the same release found via multiple date variants); the
    fourth (S##E##) query resolves to a distinct torrent (new_hash)."""

    def __init__(self, dup_hash: str, new_hash: str):
        self.dup_hash = dup_hash
        self.new_hash = new_hash
        self.calls = []

    def get(self, url, params=None):
        self.calls.append(params)
        index = len(self.calls) - 1
        info_hash = self.dup_hash if index < 3 else self.new_hash
        return _FakeResponse(_torznab_xml(f"date result {index}", info_hash))


@pytest.mark.asyncio
async def test_bitmagnet_date_fallback_adds_and_dedupes(monkeypatch):
    async def fake_scrape_page(self, *args, **kwargs):
        return []

    monkeypatch.setattr(BitmagnetScraper, "scrape_page", fake_scrape_page)

    dup_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    new_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    fake_session = _FakeSession(dup_hash, new_hash)
    scraper = BitmagnetScraper(
        manager=None, session=fake_session, url="https://bitmagnet.kuta.tech"
    )

    request = ScrapeRequest(
        media_type="series",
        media_id="tt0227972:28:33",
        media_only_id="tt0227972",
        title="WWE Smackdown!",
        season=28,
        episode=33,
        air_date="2026-08-15",
    )

    torrents = await scraper.scrape(request)

    hashes = [t["infoHash"] for t in torrents]
    # The date-fallback torrent (found only via the S##E## query) lands
    # in the result.
    assert new_hash in hashes
    # The torrent found via three different date-variant queries appears
    # exactly once (deduped by infoHash).
    assert hashes.count(dup_hash) == 1
    assert len(hashes) == 2
    # scrape_page returned [] so every request went through t=search.
    assert all(params["t"] == "search" for params in fake_session.calls)
    assert len(fake_session.calls) == 4  # 3 date variants + 1 S28E33 query
