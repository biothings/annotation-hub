"""Helpers for validating RENCI Babel release identifiers."""

import re
from datetime import date


class NodeNormReleaseError(ValueError):
    """Raised when Babel release metadata is missing or malformed."""


_RELEASE_PATTERN = re.compile(
    r"(?P<year>\d{4})(?P<month>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
    r"(?P<day>\d{1,2})"
)
_VERSION_LINE_PATTERN = re.compile(r"Babel\s+(?P<release>\S+)")
_MONTH_NUMBERS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def release_date(release: str) -> date:
    """Validate a Babel release identifier and return its calendar date."""

    if not isinstance(release, str):
        raise NodeNormReleaseError(f"Invalid Babel release {release!r}")

    match = _RELEASE_PATTERN.fullmatch(release)
    if not match:
        raise NodeNormReleaseError(f"Invalid Babel release {release!r}")

    try:
        return date(
            int(match.group("year")),
            _MONTH_NUMBERS[match.group("month")],
            int(match.group("day")),
        )
    except ValueError as exc:
        raise NodeNormReleaseError(f"Invalid Babel release {release!r}") from exc


def validate_release(release: str) -> str:
    """Return a validated Babel release identifier."""

    release_date(release)
    return release


def parse_version_marker(marker: str) -> str:
    """Extract the dated release from the first line of VERSION.txt."""

    if not isinstance(marker, str) or not marker.splitlines():
        raise NodeNormReleaseError("Babel VERSION.txt is empty")

    first_line = marker.splitlines()[0].strip()
    match = _VERSION_LINE_PATTERN.fullmatch(first_line)
    if not match:
        raise NodeNormReleaseError(
            f"Invalid Babel VERSION.txt first line {first_line!r}"
        )
    return validate_release(match.group("release"))
