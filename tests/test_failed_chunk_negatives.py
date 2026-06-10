"""Failed availability-check chunks must not be persisted as negative verdicts.

A StremThru hiccup makes get_instant() return None for a chunk. Those hashes
were never answered, so they must be excluded from `queried_info_hashes` when
caching — otherwise up to 500 hashes/chunk get is_cached=FALSE rows for the
negative TTL and the ratio gate suppresses the corrective live re-check.
"""

import asyncio

import pytest

import comet.services.debrid as debrid_module
from comet.debrid.stremthru import split_answered_chunks
from comet.services.debrid import DebridService


def test_split_answered_chunks_marks_failed_chunk_hashes_unanswered():
    chunks = [["aaa", "bbb"], ["ccc", "ddd"]]
    responses = [
        {"data": {"items": [{"hash": "aaa"}]}},
        None,  # this chunk's request failed
    ]

    availability, unanswered = split_answered_chunks(chunks, responses)

    assert availability == [[{"hash": "aaa"}]]
    assert unanswered == {"ccc", "ddd"}


def test_split_answered_chunks_treats_missing_data_as_unanswered():
    chunks = [["aaa"]]
    responses = [{"error": "store request failed"}]

    availability, unanswered = split_answered_chunks(chunks, responses)

    assert availability == []
    assert unanswered == {"aaa"}


def test_split_answered_chunks_all_answered():
    chunks = [["aaa"], ["bbb"]]
    responses = [
        {"data": {"items": [{"hash": "aaa"}]}},
        {"data": {"items": []}},
    ]

    availability, unanswered = split_answered_chunks(chunks, responses)

    assert availability == [[{"hash": "aaa"}], []]
    assert unanswered == set()


@pytest.mark.asyncio
async def test_failed_chunk_hashes_not_written_as_negatives(monkeypatch):
    answered_file = {
        "info_hash": "aaa",
        "index": 1,
        "title": "Show.S01E02.mkv",
        "size": 123,
        "season": 1,
        "episode": 2,
        "parsed": None,
    }

    async def fake_retrieve(*args, **kwargs):
        # "aaa" answered positively; "bbb"/"ccc" were in a failed chunk.
        return [answered_file], {"bbb", "ccc"}

    captured = {}

    async def fake_cache_availability(
        service, availability, *, queried_info_hashes=None, season=None, episode=None
    ):
        captured["queried"] = queried_info_hashes

    monkeypatch.setattr(debrid_module, "retrieve_debrid_availability", fake_retrieve)
    monkeypatch.setattr(debrid_module, "cache_availability", fake_cache_availability)

    service = DebridService("realdebrid", "key", "1.2.3.4")
    cached = await service.get_and_cache_availability(
        None,
        ["aaa", "bbb", "ccc"],
        {},
        {},
        {},
        None,
        "tt1:1:2",
        "tt1",
        1,
        2,
    )
    await asyncio.sleep(0)

    assert cached == {"aaa"}
    assert captured["queried"] == ["aaa"]


@pytest.mark.asyncio
async def test_all_chunks_failed_writes_no_negatives(monkeypatch):
    async def fake_retrieve(*args, **kwargs):
        return [], {"aaa", "bbb"}

    captured = {}

    async def fake_cache_availability(
        service, availability, *, queried_info_hashes=None, season=None, episode=None
    ):
        captured["queried"] = queried_info_hashes

    monkeypatch.setattr(debrid_module, "retrieve_debrid_availability", fake_retrieve)
    monkeypatch.setattr(debrid_module, "cache_availability", fake_cache_availability)

    service = DebridService("torbox", "key", "1.2.3.4")
    cached = await service.get_and_cache_availability(
        None, ["aaa", "bbb"], {}, {}, {}, None, "tt1:1:2", "tt1", 1, 2
    )
    await asyncio.sleep(0)

    assert cached == set()
    assert captured["queried"] == []
