"""Negative-verdict caching must be weaker evidence than positive caching.

RealDebrid has no instant-availability API, so a negative verdict means
"StremThru didn't know", not "not cached" — and the user playing an episode
actively MAKES torrents cached on RD. Negatives therefore must:
  1. never overwrite a fresh positive row,
  2. stop counting toward the DEBRID_CACHE_CHECK_RATIO gate after a short
     DEBRID_NEGATIVE_CACHE_TTL (not the multi-day DEBRID_CACHE_TTL),
  3. still refresh their own timestamp on a re-verified negative.
TorBox availability is authoritative, but the same rules only cost it an
occasional extra live check — they regress nothing.

These tests run the real upsert SQL against a throwaway sqlite database
(see conftest.py for the env bootstrap).
"""

import time

import pytest
import pytest_asyncio

from comet.core.models import database, settings
from comet.services.debrid_cache import (cache_availability,
                                         count_verdicted_info_hashes)

CREATE_TABLE = """
CREATE TABLE debrid_availability (
    debrid_service TEXT NOT NULL,
    info_hash TEXT NOT NULL,
    season INTEGER,
    episode INTEGER,
    season_norm INTEGER NOT NULL DEFAULT -1,
    episode_norm INTEGER NOT NULL DEFAULT -1,
    file_index TEXT,
    title TEXT,
    size BIGINT,
    parsed_json TEXT,
    updated_at REAL NOT NULL,
    is_cached BOOLEAN NOT NULL DEFAULT TRUE
)
"""

CREATE_INDEX = """
CREATE UNIQUE INDEX unq_debrid_scope_v3
ON debrid_availability (debrid_service, info_hash, season_norm, episode_norm)
"""


@pytest_asyncio.fixture
async def db():
    await database.connect()
    await database.execute("DROP TABLE IF EXISTS debrid_availability")
    await database.execute(CREATE_TABLE)
    await database.execute(CREATE_INDEX)
    yield database
    await database.execute("DROP TABLE IF EXISTS debrid_availability")
    await database.disconnect()


def _positive_file(info_hash="h1", season=1, episode=2):
    return {
        "info_hash": info_hash,
        "index": "3",
        "title": "Show.S01E02.mkv",
        "season": season,
        "episode": episode,
        "size": 1000,
        "parsed": None,
    }


async def _insert_row(db, *, info_hash, is_cached, updated_at, season=1, episode=2):
    await db.execute(
        """
        INSERT INTO debrid_availability (
            debrid_service, info_hash, season, episode, season_norm,
            episode_norm, file_index, title, size, parsed_json,
            updated_at, is_cached
        ) VALUES (
            'realdebrid', :info_hash, :season, :episode, :season,
            :episode, NULL, NULL, NULL, NULL, :updated_at, :is_cached
        )
        """,
        {
            "info_hash": info_hash,
            "season": season,
            "episode": episode,
            "updated_at": updated_at,
            "is_cached": is_cached,
        },
    )


async def _fetch_row(db, info_hash="h1"):
    return await db.fetch_one(
        "SELECT is_cached, updated_at, title FROM debrid_availability "
        "WHERE info_hash = :h",
        {"h": info_hash},
    )


@pytest.mark.asyncio
async def test_negative_does_not_overwrite_fresh_positive(db):
    await cache_availability(
        "realdebrid", [_positive_file()],
        queried_info_hashes=["h1"], season=1, episode=2,
    )
    row = await _fetch_row(db)
    assert bool(row["is_cached"]) is True

    # Live re-check came back without h1 -> negative verdict. The fresh
    # positive must survive it.
    await cache_availability(
        "realdebrid", [],
        queried_info_hashes=["h1"], season=1, episode=2,
    )
    row = await _fetch_row(db)
    assert bool(row["is_cached"]) is True
    assert row["title"] == "Show.S01E02.mkv"


@pytest.mark.asyncio
async def test_negative_overwrites_expired_positive(db):
    expired = time.time() - settings.DEBRID_CACHE_TTL - 60
    await _insert_row(db, info_hash="h1", is_cached=True, updated_at=expired)

    await cache_availability(
        "realdebrid", [],
        queried_info_hashes=["h1"], season=1, episode=2,
    )
    row = await _fetch_row(db)
    assert bool(row["is_cached"]) is False


@pytest.mark.asyncio
async def test_positive_overwrites_negative(db):
    await _insert_row(
        db, info_hash="h1", is_cached=False, updated_at=time.time() - 10
    )

    await cache_availability(
        "realdebrid", [_positive_file()],
        queried_info_hashes=["h1"], season=1, episode=2,
    )
    row = await _fetch_row(db)
    assert bool(row["is_cached"]) is True


@pytest.mark.asyncio
async def test_negative_refreshes_negative_after_negative_interval(db):
    """A re-verified negative must bump updated_at once the (short) negative
    refresh interval has passed — otherwise the row expires from the verdict
    count and every subsequent stream request re-fires the live check without
    ever being able to record its result."""
    stale = time.time() - settings.DEBRID_NEGATIVE_CACHE_TTL + 60
    await _insert_row(db, info_hash="h1", is_cached=False, updated_at=stale)

    await cache_availability(
        "realdebrid", [],
        queried_info_hashes=["h1"], season=1, episode=2,
    )
    row = await _fetch_row(db)
    assert row["updated_at"] > stale + 1


@pytest.mark.asyncio
async def test_verdict_count_ignores_negatives_past_negative_ttl(db):
    too_old = time.time() - settings.DEBRID_NEGATIVE_CACHE_TTL - 60
    await _insert_row(db, info_hash="h1", is_cached=False, updated_at=too_old)

    count = await count_verdicted_info_hashes("realdebrid", ["h1"], 1, 2)
    assert count == 0


@pytest.mark.asyncio
async def test_verdict_count_includes_fresh_negatives(db):
    await _insert_row(
        db, info_hash="h1", is_cached=False, updated_at=time.time() - 10
    )

    count = await count_verdicted_info_hashes("realdebrid", ["h1"], 1, 2)
    assert count == 1


@pytest.mark.asyncio
async def test_verdict_count_keeps_positives_for_full_ttl(db):
    """Positives older than the negative TTL but within DEBRID_CACHE_TTL must
    still count — the short TTL applies to negatives only."""
    aged = time.time() - settings.DEBRID_NEGATIVE_CACHE_TTL - 60
    assert aged > time.time() - settings.DEBRID_CACHE_TTL, (
        "test precondition: negative TTL must be shorter than positive TTL"
    )
    await _insert_row(db, info_hash="h1", is_cached=True, updated_at=aged)

    count = await count_verdicted_info_hashes("realdebrid", ["h1"], 1, 2)
    assert count == 1
