from __future__ import annotations
import pytest

from floriano.utils.competence import CompetenceRangeResolver


def test_previous_month_inside_same_year() -> None:
    resolver = CompetenceRangeResolver(as_of_date="2026-04-26")
    assert resolver.previous_month(resolver.as_of_date) == "2026_03"


def test_previous_month_cross_year() -> None:
    resolver = CompetenceRangeResolver(as_of_date="2026-01-05")
    assert resolver.previous_month(resolver.as_of_date) == "2025_12"


def test_default_range_uses_default_start_until_m_minus_one() -> None:
    resolver = CompetenceRangeResolver(
        default_start_month="2025_05",
        as_of_date="2026-04-26",
    )

    result = resolver.resolve()

    assert result.start_month == "2025_05"
    assert result.end_month == "2026_03"
    assert result.months[0] == "2025_05"
    assert result.months[-1] == "2026_03"
    assert "2026_04" not in result.months


def test_single_month_parameter() -> None:
    resolver = CompetenceRangeResolver(as_of_date="2026-04-26")

    result = resolver.resolve(month="2026_04")

    assert result.start_month == "2026_04"
    assert result.end_month == "2026_04"
    assert result.months == ["2026_04"]


def test_start_without_end_processes_only_start_month() -> None:
    resolver = CompetenceRangeResolver(as_of_date="2026-04-26")

    result = resolver.resolve(start_month="2026_03")

    assert result.months == ["2026_03"]


def test_end_without_start_processes_only_end_month() -> None:
    resolver = CompetenceRangeResolver(as_of_date="2026-04-26")

    result = resolver.resolve(end_month="2026_03")

    assert result.months == ["2026_03"]


def test_explicit_range() -> None:
    resolver = CompetenceRangeResolver(as_of_date="2026-04-26")

    result = resolver.resolve(start_month="2025_11", end_month="2026_02")

    assert result.months == ["2025_11", "2025_12", "2026_01", "2026_02"]


def test_invalid_month_format_fails() -> None:
    resolver = CompetenceRangeResolver(as_of_date="2026-04-26")

    with pytest.raises(ValueError):
        resolver.resolve(month="2026-03")


def test_month_cannot_be_combined_with_range() -> None:
    resolver = CompetenceRangeResolver(as_of_date="2026-04-26")

    with pytest.raises(ValueError):
        resolver.resolve(month="2026_03", start_month="2026_01")
