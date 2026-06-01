import pytest
from unittest.mock import AsyncMock, MagicMock

from comet.debrid import stremthru
from comet.debrid.stremthru import StremThru
from comet.debrid.exceptions import DebridAuthError


def _client(subscription_status):
    response = AsyncMock()
    response.json = AsyncMock(
        return_value={"data": {"subscription_status": subscription_status}}
    )
    session = MagicMock()
    session.get = AsyncMock(return_value=response)
    client = StremThru(
        session=session,
        video_id="tt0386676:3:12",
        media_only_id="tt0386676",
        token="torbox:secret-key",
        ip="",
    )
    return client, session


@pytest.fixture(autouse=True)
def _clear_premium_cache():
    stremthru._PREMIUM_CACHE.clear()
    yield
    stremthru._PREMIUM_CACHE.clear()


@pytest.mark.asyncio
async def test_check_premium_caches_positive_verdict(monkeypatch):
    times = iter([1000.0, 1000.0, 1100.0])  # set-expiry, check#2-now
    monkeypatch.setattr(stremthru.time, "time", lambda: next(times))
    client, session = _client("premium")

    await client.check_premium()
    await client.check_premium()

    assert session.get.call_count == 1  # second call served from cache


@pytest.mark.asyncio
async def test_check_premium_revalidates_after_ttl(monkeypatch):
    # now values: store-expiry(1000), check#2-now(past TTL), store-expiry again
    times = iter([1000.0, 9999.0, 9999.0])
    monkeypatch.setattr(stremthru.time, "time", lambda: next(times))
    client, session = _client("premium")

    await client.check_premium()
    await client.check_premium()

    assert session.get.call_count == 2  # cache expired -> re-checked


@pytest.mark.asyncio
async def test_check_premium_does_not_cache_non_premium(monkeypatch):
    monkeypatch.setattr(stremthru.time, "time", lambda: 1000.0)
    client, session = _client("expired")

    with pytest.raises(DebridAuthError):
        await client.check_premium()

    assert len(stremthru._PREMIUM_CACHE) == 0  # failure never cached
    with pytest.raises(DebridAuthError):
        await client.check_premium()
    assert session.get.call_count == 2  # re-checked every time
