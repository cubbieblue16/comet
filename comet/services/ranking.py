from RTN import Torrent, check_fetch, get_rank, sort_torrents

from comet.core.logger import logger
from comet.core.models import settings


def _is_language_failure(failed_key):
    """True if an RTN check_fetch failed_key represents a LANGUAGE violation.

    These are the user's hard language filter being broken:
      "missing_required_language" -> none of the Required languages present
      "lang_<code>"               -> an Excluded language is present
      "unknown_language"          -> remove_unknown_languages on, none detected
    """
    return (
        failed_key == "missing_required_language"
        or failed_key == "unknown_language"
        or failed_key.startswith("lang_")
    )


def rank_worker(
    torrents,
    rtn_settings,
    rtn_ranking,
    max_results_per_resolution,
    max_size,
    remove_trash,
):
    ranked_torrents = set()
    permissive = settings.PERMISSIVE_RANKING
    for info_hash, torrent in torrents.items():
        if max_size != 0:
            torrent_size = torrent["size"]
            if torrent_size is not None and torrent_size > max_size:
                continue

        parsed = torrent["parsed"]
        raw_title = torrent["title"]

        is_fetchable, failed_keys = check_fetch(parsed, rtn_settings)
        rank = get_rank(parsed, rtn_settings, rtn_ranking)

        if remove_trash:
            if permissive:
                # Permissive relaxes RANK thresholds and ignores RTN's
                # trash/quality verdict, but still honors the user's hard
                # LANGUAGE filter (Required / Excluded / remove-unknown).
                # Reject truly unwatchable content (CAM/SCREENER/TELESYNC) on
                # rank, plus anything that fails the language gate.
                if rank <= -10000 or any(
                    _is_language_failure(key) for key in failed_keys
                ):
                    continue
            else:
                if not is_fetchable or rank < rtn_settings.options["remove_ranks_under"]:
                    continue

        try:
            ranked_torrents.add(
                Torrent(
                    infohash=info_hash,
                    raw_title=raw_title,
                    data=parsed,
                    fetch=is_fetchable,
                    rank=rank,
                    lev_ratio=0.0,
                )
            )
        except Exception as e:
            logger.warning(f"Failed to create Torrent object for '{raw_title}': {e}")

    return sort_torrents(ranked_torrents, max_results_per_resolution)
