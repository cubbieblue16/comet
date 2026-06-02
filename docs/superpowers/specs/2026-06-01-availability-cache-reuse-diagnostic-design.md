# Availability-cache reuse — diagnostic phase

**Date:** 2026-06-01
**Branch:** dev-v2.53.0
**Author:** mikekuta (with Claude)

## Problem

Cold stream-list re-opens are slow even when `prefetch_next_episode` has
already warmed the torrents AND the per-account debrid availability minutes
earlier.

### Evidence (Silo S01E09, 2026-06-01)

- `20:48:04` — prefetch ran on /playback/ of S01E08; warmed S01E09 torrents
  and called `get_and_cache_multi_service_availability` for RealDebrid.
- `20:57:29` — user opened stream list for S01E09.
- Stream endpoint logged "Found cached torrents: 271" (prefetch's torrent
  rows reused ✓).
- Stream endpoint then logged "🔄 Checking availability on debrid services:
  realdebrid" and spent **6 seconds** in `stremthru.get_availability` before
  returning a 5.74s response.

The live availability call should NOT have fired:

- `DEBRID_CACHE_TTL = 86400` (1 day) — well within the 9-minute window.
- `DEBRID_CACHE_CHECK_RATIO = 0.0` (default) — live check is gated on
  `total_verified_cached_count == 0` OR first-search; neither should hold
  if prefetch wrote rows for those hashes.

Conclusion: `check_multi_service_availability` (the cache READ in
[stream.py:792](../../../comet/api/endpoints/stream.py#L792)) returned zero
verified rows for the 271 hashes, even though prefetch's WRITE in
[prefetch.py:219](../../../comet/services/prefetch.py#L219) should have
populated them. Some scope/key field differs between the write and the
read — likely candidates:

- `account_key_hash` (apiKey derivation could differ between paths)
- `season_norm` / `episode_norm` (one path passes `season=N` and the other
  `season=str(N)`, or one passes None vs 0)
- `debrid_service` string (case, suffix)
- `info_hash` set (capitalization)

## Approach

Two phases, ship sequentially.

### Phase B1 — Add diagnostic logging (this spec)

Capture both sides of the read/write pair so the next slow re-open writes
both receipts to the same log, with fields aligned for visual diffing.

**B1a — Stream endpoint cache-read receipt** in
[stream.py](../../../comet/api/endpoints/stream.py) right after
`check_multi_service_availability` returns (~line 795).

Log one line per request, gated on `total > 0 AND verified < total` so it
stays quiet when the cache is fully hit:

```python
total = len(torrent_manager.torrents)
verified = sum(
    1 for h in torrent_manager.torrents
    if any(verified_service_cache_status.get(h, {}).values())
)
if total > 0 and verified < total:
    logger.log(
        "SCRAPER",
        f"🔍 Availability-cache read for {media_id}: "
        f"verified={verified}/{total} "
        f"services={[e['service'] for e in debrid_entries]} "
        f"season_norm={search_season} episode_norm={search_episode} "
        f"account_key_hashes={[build_account_key_hash(e['apiKey'])[:8] for e in debrid_entries]}"
    )
```

Add `build_account_key_hash` to the existing
`from comet.debrid.manager import get_debrid_extension` import.

**B1b — Prefetch write receipt** in
[prefetch.py](../../../comet/services/prefetch.py) right after
`get_and_cache_multi_service_availability` returns (~line 229). Log
the same identifiers in the same shape so the two lines visibly align:

```python
logger.log(
    "SCRAPER",
    f"🔍 Availability-cache write for {next_media_id}: "
    f"torrents={len(torrent_manager.torrents)} "
    f"services={[e['service'] for e in debrid_entries]} "
    f"season_norm={season} episode_norm={next_episode} "
    f"account_key_hashes={[build_account_key_hash(e['apiKey'])[:8] for e in debrid_entries]}"
)
```

**Security:** truncate `account_key_hash` to first 8 chars — enough to
diff write-vs-read, not enough to leak the underlying API key.

**Cost:** ~12 LOC across two files. One log line per stream request
(gated on partial-hit). No new dependencies, no behavior changes.

### Phase B2 — Fix the mismatch (deferred, follow-up spec)

After 24-48h of fresh log data, the next slow re-open will leave both
receipts in the log with the differing field obvious. Phase B2 is a
follow-up spec that:

1. Identifies the differing field from real evidence.
2. Aligns whichever side is wrong (almost certainly a one-line fix).
3. Ships and re-measures.

Not designed in advance — the fix depends on which field differs, and the
right place to align (write or read) depends on which is canonical.

## Out of scope (decided in brainstorming)

- Adding `/stream/series/` as a second prefetch trigger. The user's intent
  is "warm N+1 when N starts playing" — a chain, not a browse-trigger.
  Revisit only if Phase B2 leaves measurable gaps.
- Doing anything about the first-click-of-a-new-show 3-second cold start.
  User explicitly accepted that.

## Rollout

1. Commit B1 to dev-v2.53.0.
2. Push → GitHub Actions builds `ghcr.io/cubbieblue16/comet:sha-<commit>`.
3. Deploy via Portainer stack API (NOT direct docker — per
   `feedback_use_portainer.md`).
4. Wait 24-48h.
5. Grep production logs for "Availability-cache write" + "Availability-cache
   read" pairs on the same media_id; diff the fields.
6. Write Phase B2 spec from evidence.

## Success criteria for Phase B1

- Logs emit both receipts for at least one real slow-re-open case in the
  observation window.
- Receipts contain enough identifiers to pinpoint the differing field
  without further instrumentation.
- No regression in stream-list latency (Phase B1 is pure observability).
