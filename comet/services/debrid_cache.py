import time

from comet.core.database import (build_distinct_from_predicate,
                                 build_json_list_membership_predicate,
                                 build_scope_lookup_params, build_scope_params,
                                 build_upsert_assignments, encode_json_param)
from comet.core.models import database, settings
from comet.utils.parsing import default_dump

DEBRID_UPDATE_INTERVAL = (
    settings.DEBRID_CACHE_TTL // 2 if settings.DEBRID_CACHE_TTL > 0 else 31536000
)


def _negative_cache_ttl() -> int:
    """Effective TTL for negative verdicts; <= 0 falls back to the full TTL."""
    ttl = settings.DEBRID_NEGATIVE_CACHE_TTL
    if ttl is None or ttl <= 0:
        ttl = settings.DEBRID_CACHE_TTL
    return ttl if ttl and ttl > 0 else 31536000


# A positive row is only considered expired (and overwritable by a negative)
# past the full positive TTL.
DEBRID_POSITIVE_EXPIRY = (
    settings.DEBRID_CACHE_TTL if settings.DEBRID_CACHE_TTL > 0 else 31536000
)
# Negative rows refresh their timestamp on the shorter negative cadence so a
# re-verified negative keeps counting as a verdict instead of expiring and
# re-firing the live check on every stream request.
DEBRID_NEGATIVE_UPDATE_INTERVAL = _negative_cache_ttl() // 2

DEBRID_CHANGE_DETECTION_COLUMNS = (
    "title",
    "file_index",
    "size",
    "parsed_json",
    "is_cached",
)
DEBRID_UPDATE_COLUMNS = (*DEBRID_CHANGE_DETECTION_COLUMNS, "updated_at")
DEBRID_UPDATE_SET_SQL = build_upsert_assignments(DEBRID_UPDATE_COLUMNS)
DEBRID_DISTINCT_UPDATE_WHERE_SQL = build_distinct_from_predicate(
    "debrid_availability",
    "EXCLUDED",
    DEBRID_CHANGE_DETECTION_COLUMNS,
)
INFO_HASH_MEMBERSHIP_SQL = build_json_list_membership_predicate(
    "info_hash", "info_hashes"
)
SCOPE_FILTER_SQL = """
season_norm = :season_norm
AND episode_norm = :episode_norm
"""


def _build_conditional_update() -> str:
    # First conjunct: a negative verdict may never touch a live positive —
    # only refresh negatives or replace positives that outlived their TTL.
    # Second conjunct: only write when something changed or the row is due
    # for a refresh (negatives refresh on the shorter negative cadence).
    return f"""
        DO UPDATE SET
{DEBRID_UPDATE_SET_SQL}
        WHERE
            (
                EXCLUDED.is_cached = TRUE
                OR debrid_availability.is_cached = FALSE
                OR COALESCE(debrid_availability.updated_at, 0) < (EXCLUDED.updated_at - :positive_expiry)
            )
            AND (
                {DEBRID_DISTINCT_UPDATE_WHERE_SQL}
                OR COALESCE(debrid_availability.updated_at, 0) < (EXCLUDED.updated_at - :update_interval)
                OR (
                    EXCLUDED.is_cached = FALSE
                    AND debrid_availability.is_cached = FALSE
                    AND COALESCE(debrid_availability.updated_at, 0) < (EXCLUDED.updated_at - :neg_update_interval)
                )
            )
"""


CONDITIONAL_UPDATE_SQL = _build_conditional_update()
CACHE_AVAILABILITY_QUERY = f"""
    INSERT INTO debrid_availability (
        debrid_service,
        info_hash,
        season,
        episode,
        season_norm,
        episode_norm,
        file_index,
        title,
        size,
        parsed_json,
        updated_at,
        is_cached
    )
    VALUES (
        :debrid_service,
        :info_hash,
        :season,
        :episode,
        :season_norm,
        :episode_norm,
        :file_index,
        :title,
        :size,
        :parsed_json,
        :updated_at,
        :is_cached
    )
    ON CONFLICT (debrid_service, info_hash, season_norm, episode_norm)
    {CONDITIONAL_UPDATE_SQL}
"""


async def cache_availability(
    debrid_service: str,
    availability: list,
    *,
    queried_info_hashes: list[str] | None = None,
    season: int | None = None,
    episode: int | None = None,
):
    """Persist availability verdicts to the cache.

    Positive verdicts (cached files) are written one row per file, as
    before. When `queried_info_hashes` is supplied, this function ALSO
    writes a negative-verdict row (is_cached=FALSE) for every queried hash
    that didn't return a positive match at the queried (season, episode)
    scope. That distinguishes "we never asked" (no row) from "we asked and
    it's not cached" (negative row) in subsequent reads, so the stream
    endpoint's `DEBRID_CACHE_CHECK_RATIO` gate doesn't re-fire the live
    check when negatives are the answer.

    For callers that don't perform a bulk availability check (e.g. caching
    files discovered during link generation for a single known-cached
    torrent), omit `queried_info_hashes` — no negative rows are written.
    """
    current_time = time.time()

    values = [
        {
            "debrid_service": debrid_service,
            "info_hash": file["info_hash"],
            "file_index": str(file["index"]) if file["index"] is not None else None,
            "title": file["title"],
            "season": file["season"],
            "episode": file["episode"],
            **build_scope_params(file["season"], file["episode"]),
            "size": file["size"] if file["index"] is not None else None,
            "parsed_json": (
                encode_json_param(file["parsed"], default=default_dump)
                if file["parsed"] is not None
                else None
            ),
            "updated_at": current_time,
            "update_interval": DEBRID_UPDATE_INTERVAL,
            "neg_update_interval": DEBRID_NEGATIVE_UPDATE_INTERVAL,
            "positive_expiry": DEBRID_POSITIVE_EXPIRY,
            "is_cached": True,
        }
        for file in availability
    ]

    if queried_info_hashes:
        # A queried hash is "positively answered at queried scope" if any
        # returned file matches that scope (file.season/episode None or
        # equal to the queried season/episode). Mirrors the matching done
        # in DebridService.get_and_cache_availability.
        positives_at_scope = {
            file["info_hash"]
            for file in availability
            if (file["season"] is None or file["season"] == season)
            and (file["episode"] is None or file["episode"] == episode)
        }
        negatives = [h for h in queried_info_hashes if h not in positives_at_scope]
        for info_hash in negatives:
            values.append(
                {
                    "debrid_service": debrid_service,
                    "info_hash": info_hash,
                    "file_index": None,
                    "title": None,
                    "season": season,
                    "episode": episode,
                    **build_scope_params(season, episode),
                    "size": None,
                    "parsed_json": None,
                    "updated_at": current_time,
                    "update_interval": DEBRID_UPDATE_INTERVAL,
                    "neg_update_interval": DEBRID_NEGATIVE_UPDATE_INTERVAL,
                    "positive_expiry": DEBRID_POSITIVE_EXPIRY,
                    "is_cached": False,
                }
            )

    if values:
        await database.execute_many(CACHE_AVAILABILITY_QUERY, values)


async def get_cached_availability(
    debrid_service: str,
    info_hashes: list[str],
    season: int | None = None,
    episode: int | None = None,
):
    select_clause = "SELECT info_hash, file_index, title, size, parsed_json AS parsed"

    min_timestamp = time.time() - settings.DEBRID_CACHE_TTL
    base_from_where = f"""
        FROM debrid_availability
        WHERE {INFO_HASH_MEMBERSHIP_SQL}
        AND updated_at >= :min_timestamp
    """

    params = {
        "info_hashes": encode_json_param(info_hashes),
        "min_timestamp": min_timestamp,
        **build_scope_lookup_params(season, episode),
    }

    base_from_where += " AND debrid_service = :debrid_service AND is_cached = TRUE"
    params["debrid_service"] = debrid_service

    if debrid_service == "offcloud":
        query = f"""
            SELECT info_hash, file_index, title, size, parsed
            FROM (
                SELECT
                    info_hash,
                    file_index,
                    title,
                    size,
                    parsed_json AS parsed,
                    ROW_NUMBER() OVER (
                        PARTITION BY info_hash
                        ORDER BY
                            CASE WHEN {SCOPE_FILTER_SQL} THEN 0 ELSE 1 END,
                            updated_at DESC
                    ) AS row_number
                {base_from_where}
                AND (
                    ({SCOPE_FILTER_SQL})
                    OR title IS NULL
                )
            ) ranked_offcloud_availability
            WHERE row_number = 1
        """
        results = await database.fetch_all(query, params)
    else:
        query = f"""
            {select_clause}
            {base_from_where}
            AND {SCOPE_FILTER_SQL}
        """
        results = await database.fetch_all(query, params)

    return results


async def get_cached_availability_any_service(
    info_hashes: list, season: int = None, episode: int = None
):
    min_timestamp = time.time() - settings.DEBRID_CACHE_TTL
    base_from_where = f"""
        FROM debrid_availability
        WHERE {INFO_HASH_MEMBERSHIP_SQL}
        AND updated_at >= :min_timestamp
        AND season_norm = :season_norm
        AND episode_norm = :episode_norm
    """

    params = {
        "info_hashes": encode_json_param(info_hashes),
        "min_timestamp": min_timestamp,
        **build_scope_lookup_params(season, episode),
    }

    base_from_where += " AND is_cached = TRUE"

    query = f"""
        SELECT info_hash, file_index, title, size, parsed
        FROM (
            SELECT
                info_hash,
                file_index,
                title,
                size,
                parsed_json AS parsed,
                ROW_NUMBER() OVER (
                    PARTITION BY info_hash
                    ORDER BY updated_at DESC
                ) AS row_number
            {base_from_where}
        ) latest_debrid_availability
        WHERE row_number = 1
    """

    return await database.fetch_all(query, params)


async def count_verdicted_info_hashes(
    debrid_service: str,
    info_hashes: list[str],
    season: int | None = None,
    episode: int | None = None,
) -> int:
    """Count hashes with ANY verdict (positive OR negative) within TTL at the
    queried (season, episode) scope.

    This answers "do we already know the answer for these hashes?" — the
    question the stream endpoint's DEBRID_CACHE_CHECK_RATIO gate actually
    wants to ask. Unlike `get_cached_availability`, this does NOT filter by
    is_cached, so negative-verdict rows count — but only within the shorter
    DEBRID_NEGATIVE_CACHE_TTL: a negative is weak evidence (especially on
    RealDebrid, where "don't know" != "not cached" and playing an episode
    actively makes torrents cached), so it must not suppress the live
    re-check for the full multi-day positive TTL.
    """
    if not info_hashes:
        return 0

    now = time.time()
    min_timestamp = now - settings.DEBRID_CACHE_TTL
    neg_min_timestamp = now - _negative_cache_ttl()

    query = f"""
        SELECT COUNT(DISTINCT info_hash) AS verdicted
        FROM debrid_availability
        WHERE {INFO_HASH_MEMBERSHIP_SQL}
        AND (
            (is_cached = TRUE AND updated_at >= :min_timestamp)
            OR (is_cached = FALSE AND updated_at >= :neg_min_timestamp)
        )
        AND debrid_service = :debrid_service
        AND {SCOPE_FILTER_SQL}
    """

    params = {
        "info_hashes": encode_json_param(info_hashes),
        "min_timestamp": min_timestamp,
        "neg_min_timestamp": neg_min_timestamp,
        "debrid_service": debrid_service,
        **build_scope_lookup_params(season, episode),
    }

    row = await database.fetch_one(query, params)
    if row is None:
        return 0
    # databases returns Record; handle both mapping and positional access.
    try:
        return int(row["verdicted"] or 0)
    except (KeyError, TypeError):
        return int(row[0] or 0)
