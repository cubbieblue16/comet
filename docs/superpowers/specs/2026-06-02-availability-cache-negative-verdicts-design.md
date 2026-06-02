# Availability cache — store negative verdicts (Phase B2)

**Date:** 2026-06-02
**Branch:** dev-v2.53.0
**Author:** mikekuta (with Claude)
**Builds on:** [2026-06-01-availability-cache-reuse-diagnostic-design.md](2026-06-01-availability-cache-reuse-diagnostic-design.md)

## Evidence from Phase B1

Within hours of Phase B1 going live, the new diagnostic logs caught it. Sofia
the First, 2026-06-02 11:04 UTC:

```
11:04:14 prefetch: 🔍 Availability-cache write for tt2136138:2:15:
         torrents=18 services=['torbox'] season_norm=2 episode_norm=15
         account_key_hashes=['033ccfed']

11:04:14 stremthru.get_availability - torbox: Found 5 cached torrents with 5 valid files

11:04:25 stream:   🔍 Availability-cache read for tt2136138:2:14:
         verified=5/18 services=['torbox'] season_norm=2 episode_norm=14
         account_key_hashes=['033ccfed']
         🔄 Checking availability on debrid services: torbox
         stremthru.get_availability - torbox: Found 5 cached torrents with 5 valid files
         Available cached torrents on torbox: 5/18
```

All identifiers match between write and read — there is NO key/scope mismatch.
What's happening:

1. Prefetch queries StremThru for 18 hashes; 5 come back cached.
2. `cache_availability` writes 5 rows. The 13 "no" answers are silently dropped.
3. Subsequent stream-list reads back 5 rows. `verified_count = 5`.
4. With `DEBRID_CACHE_CHECK_RATIO=0.5`, ratio 5/18=0.28 is below threshold.
5. Live re-check fires. Returns the same 5 of 18. Wasted call.

For TorBox this costs ~1s per re-open. For RealDebrid (the original Silo
complaint) it costs ~6s. Same bug, different blast radius.

## Root cause

`debrid_availability` only stores positive verdicts. The schema has no way
to record "we checked this hash on this service at this scope and it's NOT
cached." Absence of a row is ambiguous — it could mean "never checked" OR
"checked and no." The gate at [stream.py:815](../../../comet/api/endpoints/stream.py#L815)
can't tell the difference, so it has to assume "never checked" and re-fire
the live call.

## Fix: add `is_cached` column, write negative rows

### Schema change

Add `is_cached BOOLEAN NOT NULL DEFAULT TRUE` to `debrid_availability` in
[schema_specs.py:320](../../../comet/core/schema_specs.py#L320):

- In `create_sql` for new installs.
- As a `LegacyColumnMigration` so existing rows backfill to TRUE (every
  existing row IS a positive verdict by today's semantics).

Add `is_cached` to `DEBRID_CHANGE_DETECTION_COLUMNS` and
`DEBRID_UPDATE_COLUMNS` in [debrid_cache.py:14-20](../../../comet/services/debrid_cache.py#L14)
so upserts overwrite the verdict (positive → negative or negative → positive)
when state changes.

### Write side

`cache_availability(debrid_service, availability)` → add optional
`queried_info_hashes`, `season`, `episode` kwargs. When provided:

- Write `is_cached=TRUE` rows for each file in `availability` (existing
  behavior).
- Compute `negatives = queried_info_hashes - {hashes positively cached at
  the queried scope}` and write one `is_cached=FALSE` row per hash at the
  queried (season, episode) scope, with file_index/title/size/parsed_json
  all NULL.

Callsite updates:

- [debrid.py:139](../../../comet/services/debrid.py#L139) (`get_and_cache_availability`):
  pass `queried_info_hashes=info_hashes, season=season, episode=episode`.
  Also remove the early-return at [debrid.py:103](../../../comet/services/debrid.py#L103)
  (`if len(availability) == 0: return set()`) — that bypasses the cache write
  entirely, which means if StremThru returns ZERO matches we lose the chance
  to record negatives for all queried hashes.
- [stremthru.py:641](../../../comet/debrid/stremthru.py#L641) (link-gen): no
  change. This site writes file-level rows for a single already-known-cached
  torrent; no negative-row semantics apply.

### Read side

`get_cached_availability` and `get_cached_availability_any_service` in
[debrid_cache.py:106](../../../comet/services/debrid_cache.py#L106): add
`AND is_cached = TRUE` to both queries. Existing callers (`check_existing_availability`,
`apply_cached_availability_any_service`) expect "this row means cached" —
the filter preserves that contract.

New helper `count_verdicted_info_hashes(debrid_service, info_hashes, season,
episode)` returns the count of hashes that have ANY row (positive OR
negative) within TTL. This is the "do we know the answer?" question the
gate actually wants to ask.

### Gate change in stream.py

In [stream.py:807-824](../../../comet/api/endpoints/stream.py#L807):

- Compute `total_verdicted_count` via the new helper for each entry in
  `debrid_entries`, then take the max (any service having a verdict counts).
- Replace `total_verified_cached_count` with `total_verdicted_count` in the
  ratio gate AND the `== 0` clause. The first clause (`not has_cached_torrents
  and not use_account_scrape`) stays unchanged.

The `total_verified_cached_count` variable is still used elsewhere
(rendering, stream sorting) — keep that name for those uses and add the new
`total_verdicted_count` alongside, only for gate decisions.

### Diagnostic logging update

Extend the Phase B1 log line at [stream.py:818](../../../comet/api/endpoints/stream.py#L818)
to include verdicted count too:

```
🔍 Availability-cache read for ...: verified=5/18 verdicted=18/18 ...
```

After this fix lands, expect to see `verified=5/18 verdicted=18/18` for
the same Sofia case — and crucially, the "🔄 Checking availability" line
should NOT appear (gate skipped because verdicted ratio is 1.0).

## What we'd expect to see in logs after deploy

Before (current):
```
verified=5/18 ...
🔄 Checking availability on debrid services: torbox
```

After:
```
verified=5/18 verdicted=18/18 ...
(no live check)
```

Stream-list response time for re-opens should drop from ~6s to <1s on
RealDebrid for cases like Silo. TorBox cases drop from ~1s to ~0.3s.

## Out of scope

- Changing `DEBRID_CACHE_CHECK_RATIO` default. The user explicitly set 0.5
  for the safety-net reason; with negative caching, 0.5 of verdicted is
  the correct semantic.
- Cleanup of stale negative rows. They expire via the same
  `DEBRID_CACHE_TTL` (3 days on this deployment) as positive rows.
- Surfacing "definitely uncached" status to the UI. Future work — out of
  scope for this fix.

## Migration safety

- `ALTER TABLE ... ADD COLUMN is_cached BOOLEAN NOT NULL DEFAULT TRUE` is
  non-blocking on PostgreSQL (constant default, no row rewrite).
- All existing rows correctly default to TRUE (they ARE positive verdicts).
- No downtime needed. Single rolling restart of the container.

## Rollout

1. Commit to dev-v2.53.0.
2. Push → GitHub Actions builds `ghcr.io/cubbieblue16/comet:sha-<commit>`.
3. Deploy via Portainer stack API.
4. Verify migration ran by querying `\d debrid_availability` on the
   Postgres replica.
5. Monitor logs for 24h:
   - Look for `verdicted=N/N` matching torrent counts on re-opens.
   - Look for absence of "🔄 Checking availability" after a prefetch
     warmed the same (media_id, service) within TTL.
   - Watch p90 stream-list latency on RealDebrid-backed accounts.

## Success criteria

- A previously-slow re-open (verified < ratio) now logs `verdicted >= total`
  and skips the live debrid check.
- Stream-list p90 on warm content drops measurably on RealDebrid-backed
  scopes.
- No regression in cold scrapes (the first-time-asking path is unchanged).
- No spike in error rates on debrid_availability writes/reads.
