"""PERMISSIVE_RANKING must still honor the user's hard LANGUAGE filter.

Bug: with PERMISSIVE_RANKING=True, the permissive branch in rank_worker dropped
only CAM-tier content (rank <= -10000) and discarded check_fetch's is_fetchable
verdict entirely -- so a user's Required / Excluded language selection was
ignored and foreign-language torrents leaked into results.

Permissive should relax RANK thresholds (keep low-ranked releases) and keep
ignoring RTN's trash/quality verdict, but it must still drop torrents that fail
the language gate. The language-related RTN failed_keys are:
  "missing_required_language" -> required language(s) not present
  "lang_<code>"               -> an excluded language is present
  "unknown_language"          -> remove_unknown_languages on, none detected
"""

import pytest
from RTN import parse

import comet.services.ranking as ranking_module
from comet.core.models import rtn_ranking_default, rtn_settings_default

FRENCH = "Some.Movie.2021.1080p.FRENCH.WEBRip.x264-GRP"
ENGLISH = "Some.Movie.2021.1080p.ENGLISH.WEBRip.x264-GRP"
HASH_FR = "a" * 40
HASH_EN = "b" * 40
_HASHES = {FRENCH: HASH_FR, ENGLISH: HASH_EN}


def _settings(**languages):
    base_lang = rtn_settings_default.languages.model_copy(
        update={"required": [], "exclude": [], "allowed": [], "preferred": []}
    )
    return rtn_settings_default.model_copy(
        update={"languages": base_lang.model_copy(update=languages)}
    )


def _torrents(*titles):
    return {
        _HASHES[t]: {"parsed": parse(t), "title": t, "size": None} for t in titles
    }


def _ranked_hashes(torrents, rtn_settings):
    # rank_worker returns RTN's sort_torrents dict keyed by infohash.
    return set(
        ranking_module.rank_worker(
            torrents,
            rtn_settings,
            rtn_ranking_default,
            max_results_per_resolution=100,
            max_size=0,
            remove_trash=True,
        )
    )


def test_permissive_drops_required_language_miss(monkeypatch):
    monkeypatch.setattr(ranking_module.settings, "PERMISSIVE_RANKING", True)
    hashes = _ranked_hashes(_torrents(FRENCH, ENGLISH), _settings(required=["en"]))
    assert HASH_EN in hashes  # satisfies required language -> kept
    assert HASH_FR not in hashes  # missing required language -> dropped


def test_permissive_drops_excluded_language(monkeypatch):
    monkeypatch.setattr(ranking_module.settings, "PERMISSIVE_RANKING", True)
    hashes = _ranked_hashes(_torrents(FRENCH, ENGLISH), _settings(exclude=["fr"]))
    assert HASH_EN in hashes  # no excluded language -> kept
    assert HASH_FR not in hashes  # excluded language present -> dropped


def test_permissive_keeps_results_when_no_language_filter(monkeypatch):
    # Regression guard: with no language constraints, permissive behavior is
    # unchanged -- non-CAM torrents (incl. foreign-language) stay in results.
    monkeypatch.setattr(ranking_module.settings, "PERMISSIVE_RANKING", True)
    hashes = _ranked_hashes(_torrents(FRENCH, ENGLISH), _settings())
    assert HASH_EN in hashes
    assert HASH_FR in hashes
