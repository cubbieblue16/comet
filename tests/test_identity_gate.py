"""Identity gate: reject a release only when its parsed show name is positively
identified (catalog lookup, exact scrubbed name, different IMDb id) as a
different series.  Everything unidentifiable stays (fail open)."""

import asyncio
import time
from types import SimpleNamespace

import pytest

from comet.core.models import settings
from comet.services import identity
from comet.services.identity import IdentityGate

TED = "tt14824792"
LASSO = "tt10986410"
TWISTED = "tt14261112"


def T(parsed_title, title=None, h=None):
    return {
        "infoHash": h or parsed_title,
        "title": title or f"{parsed_title}.S02E08.1080p.WEB.h264-GRP",
        "parsed": SimpleNamespace(parsed_title=parsed_title),
    }


def titles(items):
    return [t["parsed"].parsed_title for t in items]


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    identity.reset_state()
    monkeypatch.setattr(settings, "IDENTITY_GATE_ENABLED", True)
    monkeypatch.setattr(identity, "_load_local_index", _no_local)
    yield
    identity.reset_state()


async def _no_local():
    return {}


class Catalog:
    """Stand-in for the Cinemeta search: name -> [(id, name)] exact hits."""

    def __init__(self, table=None, delay=0.0):
        self.table = table or {}
        self.calls = []
        self.delay = delay

    async def __call__(self, title):
        self.calls.append(title)
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.table.get(title, [])


def gate(title="Ted", aliases=None, media_id=TED, **kw):
    return IdentityGate(title, aliases or {}, media_id, "series", **kw)


def run(coro):
    return asyncio.run(coro)


def use(monkeypatch, catalog):
    monkeypatch.setattr(identity, "_fetch_cinemeta", catalog)
    return catalog


def test_ted_lasso_and_twisted_metal_rejected_under_ted(monkeypatch):
    cat = use(
        monkeypatch,
        Catalog(
            {
                "Ted Lasso": [(LASSO, "Ted Lasso")],
                "Twisted Metal": [(TWISTED, "Twisted Metal")],
            }
        ),
    )
    items = [T("Ted"), T("Ted Lasso"), T("Twisted Metal"), T("Ted Lasso", h="x2")]
    out = run(gate().filter(items))
    assert titles(out) == ["Ted"]
    assert sorted(cat.calls) == ["Ted Lasso", "Twisted Metal"]


def test_real_ted_variants_kept(monkeypatch):
    cat = use(monkeypatch, Catalog())
    items = [T("Ted"), T("TED"), T("Ted 2024"), T("Ted (US)")]
    out = run(gate().filter(items))
    assert len(out) == 4
    # "ted" equals the expected title: never looked up.
    assert "Ted" not in cat.calls and "TED" not in cat.calls


@pytest.mark.parametrize(
    "parsed,expected,eid",
    [
        ("The Office Superfan Episodes", "The Office", "tt0386676"),
        ("The Office U S", "The Office", "tt0386676"),
        ("Slow Horses Хромые кони", "Slow Horses", "tt5875444"),
        ("Dark.Winds Тёмные ветра", "Dark Winds", "tt9026888"),
        ("Sofia the First + Special + Shorts", "Sofia the First", "tt2650978"),
        ("kidengee-bluey", "Bluey", "tt7678620"),
        ("Scrubs Die Anfaenger", "Scrubs", "tt0285403"),
        ("Rick and Morty 2x", "Rick and Morty", "tt2861424"),
    ],
)
def test_same_show_examples_kept(monkeypatch, parsed, expected, eid):
    # Catalog has nothing with that exact name (the realistic case).
    use(monkeypatch, Catalog())
    out = run(gate(expected, media_id=eid).filter([T(parsed)]))
    assert titles(out) == [parsed]


def test_catalog_near_miss_is_not_identification(monkeypatch):
    # Cinemeta returns the same series under its own name for a looser query.
    use(monkeypatch, Catalog({"Scrubs Die Anfaenger": [("tt0285403", "Scrubs")]}))
    out = run(gate("Scrubs", media_id="tt0285403").filter([T("Scrubs Die Anfaenger")]))
    assert titles(out) == ["Scrubs Die Anfaenger"]


def test_exact_name_hit_with_same_id_keeps(monkeypatch):
    use(monkeypatch, Catalog({"Ted Show": [(TED, "Ted Show")]}))
    out = run(gate().filter([T("Ted Show")]))
    assert titles(out) == ["Ted Show"]


def test_exact_name_hits_including_requested_id_keep(monkeypatch):
    # Two series share a name and one of them is the requested one: ambiguous.
    use(monkeypatch, Catalog({"Ted Show": [(LASSO, "Ted Show"), (TED, "Ted Show")]}))
    out = run(gate().filter([T("Ted Show")]))
    assert titles(out) == ["Ted Show"]


def test_exact_match_is_scrubbed_not_raw(monkeypatch):
    use(
        monkeypatch,
        Catalog({"RuPauls Drag Race All Stars": [("tt2301351", "RuPaul's Drag Race All Stars")]}),
    )
    out = run(
        gate("RuPaul's Drag Race", media_id="tt1353056").filter(
            [T("RuPauls Drag Race All Stars"), T("RuPauls Drag Race")]
        )
    )
    assert titles(out) == ["RuPauls Drag Race"]


@pytest.mark.parametrize("failure", ["timeout", "error", "garbage"])
def test_lookup_failure_keeps_rows_and_is_not_cached(monkeypatch, failure):
    calls = []

    async def bad(title):
        calls.append(title)
        if failure == "timeout":
            await asyncio.sleep(10)
        if failure == "error":
            raise RuntimeError("boom")
        raise ValueError("bad json")

    monkeypatch.setattr(identity, "_fetch_cinemeta", bad)
    g = gate(lookup_timeout=0.05)
    t0 = time.monotonic()
    out = run(g.filter([T("Ted Lasso")]))
    assert titles(out) == ["Ted Lasso"]
    assert time.monotonic() - t0 < 1.0
    # failures are not cached: a fresh gate retries
    run(gate(lookup_timeout=0.05).filter([T("Ted Lasso")]))
    assert len(calls) == 2


def test_http_layer_failures_keep_rows(monkeypatch):
    class Resp:
        def __init__(self, status=200, payload=None, exc=None):
            self.status, self._p, self._e = status, payload, exc

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self, content_type=None):
            if self._e:
                raise self._e
            return self._p

    class Sess:
        def __init__(self, resp):
            self.resp = resp

        def get(self, url, **kw):
            return self.resp

    async def with_session(resp):
        async def get_session():
            return Sess(resp)

        monkeypatch.setattr(identity.http_client_manager, "get_session", get_session)
        return await identity._fetch_cinemeta("Ted Lasso")

    ok = Resp(200, {"metas": [{"id": LASSO, "name": "Ted Lasso"}, {"id": "bad", "name": "x"}, "junk"]})
    assert run(with_session(ok)) == [(LASSO, "Ted Lasso")]
    for bad in (Resp(500), Resp(200, exc=ValueError("x")), Resp(200, ["not", "a", "dict"]), Resp(200, {"metas": "x"})):
        with pytest.raises(Exception):
            run(with_session(bad))
    # all of them leave rows untouched through the gate
    use(monkeypatch, identity._fetch_cinemeta)
    out = run(gate(lookup_timeout=0.5).filter([T("Ted Lasso")]))
    assert titles(out) == ["Ted Lasso"]


def test_alias_equal_title_not_looked_up(monkeypatch):
    cat = use(monkeypatch, Catalog({"Ted Show": [(LASSO, "Ted Show")]}))
    out = run(gate(aliases={"us": ["Ted Show"]}).filter([T("Ted Show")]))
    assert titles(out) == ["Ted Show"]
    assert cat.calls == []


def test_movies_and_non_imdb_untouched(monkeypatch):
    cat = use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    items = [T("Ted Lasso")]
    assert titles(run(IdentityGate("Ted", {}, TED, "movie").filter(items))) == ["Ted Lasso"]
    assert titles(run(IdentityGate("Ted", {}, "kitsu:1", "series", is_kitsu=True).filter(items))) == ["Ted Lasso"]
    assert titles(run(IdentityGate("Ted", {}, "tmdb:5", "series").filter(items))) == ["Ted Lasso"]
    assert titles(run(IdentityGate("Ted", {}, None, "series").filter(items))) == ["Ted Lasso"]
    assert cat.calls == []


def test_cap_of_30_lookups_most_rows_first(monkeypatch):
    cat = use(monkeypatch, Catalog())
    items = []
    for i in range(45):
        items.append(T(f"Other Show {i}", h=f"a{i}"))
    # the title with the most rows must be looked up even if it comes last
    items += [T("Big Show", h=f"b{i}") for i in range(5)]
    run(gate().filter(items))
    assert len(cat.calls) == 30
    assert "Big Show" in cat.calls


def test_cap_applies_per_request_across_calls(monkeypatch):
    cat = use(monkeypatch, Catalog())
    g = gate()
    run(g.filter([T(f"A {i}") for i in range(20)]))
    run(g.filter([T(f"B {i}") for i in range(20)]))
    assert len(cat.calls) == 30


def test_budget_exhausted_keeps_rows(monkeypatch):
    cat = use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}, delay=0.3))
    g = gate(budget=0.1, lookup_timeout=2.5)
    t0 = time.monotonic()
    out = run(g.filter([T("Ted Lasso")]))
    assert titles(out) == ["Ted Lasso"]
    assert time.monotonic() - t0 < 0.5
    # budget now spent: no new lookups at all
    cat.calls.clear()
    run(g.filter([T("Twisted Metal")]))
    assert cat.calls == []


def test_concurrency_bounded_to_8(monkeypatch):
    live = {"n": 0, "max": 0}

    async def slow(title):
        live["n"] += 1
        live["max"] = max(live["max"], live["n"])
        await asyncio.sleep(0.02)
        live["n"] -= 1
        return []

    monkeypatch.setattr(identity, "_fetch_cinemeta", slow)
    run(gate().filter([T(f"S {i}") for i in range(30)]))
    assert 1 < live["max"] <= 8


def test_cache_hit_makes_no_second_call(monkeypatch):
    cat = use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    run(gate().filter([T("Ted Lasso"), T("Nothing Here")]))
    assert len(cat.calls) == 2
    out = run(gate().filter([T("Ted Lasso", h="n1"), T("Nothing Here", h="n2")]))
    assert titles(out) == ["Nothing Here"]
    assert len(cat.calls) == 2


def test_cached_hits_apply_per_requested_id(monkeypatch):
    # Same cached lookup, different request id: decision is made per request.
    use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    assert titles(run(gate().filter([T("Ted Lasso")]))) == []
    assert titles(run(gate("Ted Lasso", media_id=LASSO).filter([T("Ted Lasso")]))) == ["Ted Lasso"]


def test_cache_is_lru_capped(monkeypatch):
    use(monkeypatch, Catalog())
    monkeypatch.setattr(identity, "_CACHE_MAX", 5)
    for i in range(12):
        run(gate().filter([T(f"Show {i}")]))
    assert len(identity._cache) <= 5


def test_ttl_positive_30d_negative_7d(monkeypatch):
    cat = use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    run(gate().filter([T("Ted Lasso"), T("Nothing Here")]))
    now = time.time()
    monkeypatch.setattr(identity.time, "time", lambda: now + 8 * 86400)
    run(gate().filter([T("Ted Lasso"), T("Nothing Here")]))
    assert cat.calls.count("Ted Lasso") == 1  # positive still cached at 8d
    assert cat.calls.count("Nothing Here") == 2  # negative expired at 7d
    monkeypatch.setattr(identity.time, "time", lambda: now + 31 * 86400)
    run(gate().filter([T("Ted Lasso")]))
    assert cat.calls.count("Ted Lasso") == 2


def test_setting_off_means_no_lookups(monkeypatch):
    monkeypatch.setattr(settings, "IDENTITY_GATE_ENABLED", False)
    cat = use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    out = run(gate().filter([T("Ted Lasso")]))
    assert titles(out) == ["Ted Lasso"]
    assert cat.calls == []


def test_local_index_positive_identification_without_network(monkeypatch):
    cat = use(monkeypatch, Catalog())

    async def local():
        return {"ted lasso": {LASSO}, "ted": {TED}}

    monkeypatch.setattr(identity, "_load_local_index", local)
    out = run(gate().filter([T("Ted"), T("Ted Lasso")]))
    assert titles(out) == ["Ted"]
    assert cat.calls == []


def test_local_index_same_id_or_failure_falls_through(monkeypatch):
    cat = use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))

    async def boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(identity, "_load_local_index", boom)
    assert titles(run(gate().filter([T("Ted Lasso")]))) == []
    assert cat.calls == ["Ted Lasso"]


def test_rejection_is_logged(monkeypatch):
    use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    seen = []
    monkeypatch.setattr(identity, "_log_rejection", seen.append)
    run(gate().filter([T("Ted Lasso", title="Ted.Lasso.S02E08.1080p")]))
    assert len(seen) == 1
    msg = seen[0]
    assert "Rejected (Other Show)" in msg and "Ted.Lasso.S02E08.1080p" in msg
    assert f"Ted Lasso ({LASSO})" in msg and f"Ted ({TED})" in msg


# ---- integration with TorrentManager ------------------------------------

def _manager(**kw):
    from comet.services.orchestration import TorrentManager

    args = dict(
        media_type="series",
        media_full_id=f"{TED}:2:8",
        media_only_id=TED,
        title="Ted",
        year=2024,
        year_end=None,
        season=2,
        episode=8,
        aliases={},
        remove_adult_content=False,
    )
    args.update(kw)
    return TorrentManager(**args)


def test_filter_manager_drops_other_show_before_caching(monkeypatch):
    use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    from comet.services import orchestration

    def fake_worker(torrents, *a):
        return [{"infoHash": t["infoHash"], "title": t["title"], "parsed": SimpleNamespace(parsed_title=t["pt"])} for t in torrents]

    monkeypatch.setattr(orchestration, "filter_worker", fake_worker)
    tm = _manager()
    scraped = [
        {"infoHash": "h1", "title": "Ted.S02E08.mkv", "pt": "Ted"},
        {"infoHash": "h2", "title": "Ted.Lasso.S02E08.mkv", "pt": "Ted Lasso"},
    ]
    run(tm.filter_manager("X", scraped))
    assert [t["infoHash"] for t in tm.ready_to_cache] == ["h1"]


def test_filter_manager_survives_gate_crash(monkeypatch):
    from comet.services import orchestration

    def fake_worker(torrents, *a):
        return [{"infoHash": t["infoHash"], "title": t["title"], "parsed": SimpleNamespace(parsed_title="Ted Lasso")} for t in torrents]

    async def crash(self, items):
        raise RuntimeError("gate bug")

    monkeypatch.setattr(orchestration, "filter_worker", fake_worker)
    monkeypatch.setattr(IdentityGate, "filter", crash)
    tm = _manager()
    run(tm.filter_manager("X", [{"infoHash": "h1", "title": "t", "pt": "x"}]))
    assert [t["infoHash"] for t in tm.ready_to_cache] == ["h1"]


def test_get_cached_torrents_hides_other_show_rows(monkeypatch):
    import orjson
    from RTN import parse

    use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    tm = _manager()

    def row(h, name):
        parsed = parse(name)
        return {
            "info_hash": h,
            "file_index": None,
            "title": name,
            "seeders": 1,
            "size": 1,
            "tracker": "DMM",
            "sources_json": "[]",
            "parsed_json": orjson.dumps(parsed.model_dump()).decode(),
            "episode": 8,
            "updated_at": 1,
        }

    rows = [row("h1", "Ted.S02E08.1080p.WEB.h264-GRP"), row("h2", "Ted.Lasso.S02E08.1080p.WEB.h264-GRP")]

    async def fake_fetch(self, media_id):
        return rows

    monkeypatch.setattr(type(tm), "_fetch_cached_rows", fake_fetch)
    run(tm.get_cached_torrents())
    assert list(tm.torrents) == ["h1"]


def test_get_cached_torrents_setting_off(monkeypatch):
    import orjson
    from RTN import parse

    monkeypatch.setattr(settings, "IDENTITY_GATE_ENABLED", False)
    cat = use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    tm = _manager()
    name = "Ted.Lasso.S02E08.1080p.WEB.h264-GRP"
    rows = [
        {
            "info_hash": "h2", "file_index": None, "title": name, "seeders": 1, "size": 1,
            "tracker": "DMM", "sources_json": "[]",
            "parsed_json": orjson.dumps(parse(name).model_dump()).decode(),
            "episode": 8, "updated_at": 1,
        }
    ]

    async def fake_fetch(self, media_id):
        return rows

    monkeypatch.setattr(type(tm), "_fetch_cached_rows", fake_fetch)
    run(tm.get_cached_torrents())
    assert list(tm.torrents) == ["h2"] and cat.calls == []


def test_prefetch_played_hash_cannot_resurrect_other_show(monkeypatch):
    import orjson
    from RTN import parse

    from comet.services import prefetch

    use(monkeypatch, Catalog({"Ted Lasso": [(LASSO, "Ted Lasso")]}))
    tm = _manager()
    name = "Ted.Lasso.S02E07.1080p.WEB.h264-GRP"

    async def fake_fetch_one(query, params=None, **kw):
        return {
            "title": name, "seeders": 1, "size": 1, "tracker": "DMM",
            "sources_json": "[]",
            "parsed_json": orjson.dumps(parse(name).model_dump()).decode(),
        }

    monkeypatch.setattr(prefetch.database, "fetch_one", fake_fetch_one)
    run(prefetch._ensure_played_hash_candidate(tm, "abc", TED))
    assert "abc" not in tm.torrents
