import datetime
from comet.utils.parsing import extract_date_from_title


def test_dot_separated_date():
    result = extract_date_from_title("The.Daily.Show.2026.01.05.Mark.Kelly.720p.WEB.h264-EDITH")
    assert result == datetime.date(2026, 1, 5)


def test_dash_separated_date():
    result = extract_date_from_title("The.Daily.Show.2026-01-05.Mark.Kelly.720p")
    assert result == datetime.date(2026, 1, 5)


def test_compact_date():
    result = extract_date_from_title("The.Daily.Show.20260105.Mark.Kelly.720p")
    assert result == datetime.date(2026, 1, 5)


def test_no_date_returns_none():
    result = extract_date_from_title("Breaking.Bad.S05E16.Felina.1080p.BluRay")
    assert result is None


def test_invalid_date_returns_none():
    result = extract_date_from_title("Show.2026.99.99.Episode.720p")
    assert result is None


def test_year_only_returns_none():
    """A year alone (e.g., 2026) should not be treated as a date."""
    result = extract_date_from_title("WWE.WrestleMania.40.2024.720p.WEB.h264-GROUP")
    assert result is None


def test_resolution_not_mistaken_for_date():
    """1080 or 720 should not be parsed as part of a date."""
    result = extract_date_from_title("Show.Name.1080p.WEB.h264")
    assert result is None


def test_wwe_weekly_date():
    result = extract_date_from_title("WWE.Raw.2026.03.10.720p.WEB.h264-HEEL")
    assert result == datetime.date(2026, 3, 10)


def test_date_with_hevc_suffix():
    result = extract_date_from_title("The.Daily.Show.2026.03.02.Jafar.Panahi.720p.HEVC.x265-MeGusta")
    assert result == datetime.date(2026, 3, 2)
