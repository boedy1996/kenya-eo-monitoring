"""Season boundaries shared by the dashboard builder and validator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


METHOD_VERSION = "seasonal-top3-v2"


@dataclass(frozen=True)
class Season:
    code: str
    year: int
    kind: str
    name: str
    start: date
    end: date


def end_of_february(year: int) -> date:
    """Return the last day of February, including leap years."""
    return date(year, 3, 1) - timedelta(days=1)


def seasons_between(date_from: date, date_to: date) -> list[Season]:
    """Return model seasons that overlap the supplied data coverage."""
    result: list[Season] = []
    for year in range(date_from.year - 1, date_to.year + 1):
        seasons = (
            Season(
                f"{year}_lr",
                year,
                "lr",
                "Long Rains",
                date(year, 3, 1),
                date(year, 8, 31),
            ),
            Season(
                f"{year}_sr",
                year,
                "sr",
                "Short Rains",
                date(year, 10, 1),
                end_of_february(year + 1),
            ),
        )
        result.extend(
            season
            for season in seasons
            if season.end >= date_from and season.start <= date_to
        )
    return result
