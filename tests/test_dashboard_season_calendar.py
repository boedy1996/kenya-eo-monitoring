from datetime import date

from dashboard.scripts.season_calendar import end_of_february, seasons_between


def test_model_season_boundaries_include_extended_months() -> None:
    seasons = {
        season.code: season
        for season in seasons_between(date(2023, 1, 1), date(2024, 12, 31))
    }

    assert seasons["2023_lr"].start == date(2023, 3, 1)
    assert seasons["2023_lr"].end == date(2023, 8, 31)
    assert seasons["2023_sr"].start == date(2023, 10, 1)
    assert seasons["2023_sr"].end == date(2024, 2, 29)


def test_short_rains_uses_last_day_of_february() -> None:
    assert end_of_february(2024) == date(2024, 2, 29)
    assert end_of_february(2025) == date(2025, 2, 28)


def test_partial_short_rains_overlapping_coverage_is_retained() -> None:
    seasons = seasons_between(date(2016, 1, 4), date(2016, 5, 31))

    assert [season.code for season in seasons] == ["2015_sr", "2016_lr"]
