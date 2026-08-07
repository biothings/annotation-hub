"""Release and shard validation helpers for PubMed metadata snapshots."""

from __future__ import annotations

import gzip
import json
import re
from collections.abc import Iterable, Mapping
from datetime import date
from html.parser import HTMLParser
from pathlib import Path

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
_RELEASE_PATTERN = re.compile(
    r"^(?P<year>\d{4})(?P<month>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
    r"(?P<day>0?[1-9]|[12]\d|3[01])$"
)
_SHARD_PATTERN = re.compile(r"^pubmed_metadata_(?P<index>\d{5})\.ndjson\.gz$")
_REQUIRED_STRUCTURE_CHECKS = frozenset(
    {
        "shards-found",
        "records-present",
        "json-parse",
        "record-fields",
        "no-nulls",
        "id-format",
        "pmid-unique",
        "no-extra-fields",
        "month-format",
    }
)


class PubMedReleaseError(ValueError):
    """Raised when an upstream release cannot be trusted for ingestion."""


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a":
            return
        for key, value in attrs:
            if key.casefold() == "href" and value is not None:
                self.hrefs.append(value)
                return


def extract_index_hrefs(index_html: str) -> tuple[str, ...]:
    """Return links from an HTTP directory index without interpreting them."""

    parser = _LinkParser()
    parser.feed(index_html)
    parser.close()
    return tuple(parser.hrefs)


def release_date(release: str) -> date:
    """Parse RENCI's ``YYYYmonD`` or ``YYYYmonDD`` directory convention."""

    if not isinstance(release, str):
        raise PubMedReleaseError(f"invalid PubMed release name: {release!r}")
    match = _RELEASE_PATTERN.fullmatch(release)
    if match is None:
        raise PubMedReleaseError(f"invalid PubMed release name: {release!r}")
    try:
        return date(
            int(match.group("year")),
            _MONTH_NUMBERS[match.group("month")],
            int(match.group("day")),
        )
    except ValueError as exc:
        raise PubMedReleaseError(f"invalid PubMed release date: {release!r}") from exc


def releases_from_index(index_html: str) -> tuple[str, ...]:
    """Return valid release directory names, newest first."""

    releases: dict[str, date] = {}
    for href in extract_index_hrefs(index_html):
        if not href.endswith("/") or href.count("/") != 1:
            continue
        release = href[:-1]
        try:
            releases[release] = release_date(release)
        except PubMedReleaseError:
            continue
    sorted_releases = sorted(releases.items(), key=lambda item: item[1], reverse=True)
    return tuple(release for release, _ in sorted_releases)


def validate_shard_filenames(filenames: Iterable[str]) -> tuple[str, ...]:
    """Return a sorted, contiguous set of PubMed shard filenames."""

    indexed_names: dict[int, str] = {}
    for filename in filenames:
        match = _SHARD_PATTERN.fullmatch(filename)
        if match is None:
            continue
        index = int(match.group("index"))
        if index in indexed_names:
            raise PubMedReleaseError(f"duplicate PubMed shard index: {index:05d}")
        indexed_names[index] = filename

    if not indexed_names:
        raise PubMedReleaseError("PubMed release contains no metadata shards")

    expected_indices = list(range(len(indexed_names)))
    actual_indices = sorted(indexed_names)
    if actual_indices != expected_indices:
        expected = ", ".join(f"{index:05d}" for index in expected_indices)
        actual = ", ".join(f"{index:05d}" for index in actual_indices)
        raise PubMedReleaseError(
            "PubMed shard indices must be contiguous from 00000; "
            f"expected [{expected}], found [{actual}]"
        )

    return tuple(indexed_names[index] for index in actual_indices)


def parse_validation_report(payload: bytes) -> Mapping:
    """Decode one gzip-compressed JSON validation report."""

    try:
        report = json.loads(gzip.decompress(payload).decode("utf-8"))
    except (EOFError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PubMedReleaseError(
            "PubMed validation report is not valid gzip-compressed JSON"
        ) from exc
    if not isinstance(report, Mapping):
        raise PubMedReleaseError("PubMed validation report is not a JSON object")
    return report


def validate_report(report: object, shard_filenames: Iterable[str]) -> None:
    """Validate the upstream report as a completion and structure gate."""

    if not isinstance(report, Mapping):
        raise PubMedReleaseError("PubMed validation report is not a JSON object")
    if report.get("status") not in {"pass", "warn"}:
        raise PubMedReleaseError("PubMed validation report did not complete safely")
    if report.get("errors") != []:
        raise PubMedReleaseError("PubMed validation report contains errors")

    checks_run = report.get("checks_run")
    if not isinstance(checks_run, list):
        raise PubMedReleaseError("PubMed validation report has no checks_run list")
    structure_checks = [
        check
        for check in checks_run
        if isinstance(check, Mapping) and check.get("section") == "structure"
    ]
    if not structure_checks or any(
        check.get("status") != "pass" for check in structure_checks
    ):
        raise PubMedReleaseError(
            "PubMed validation report did not pass all structure checks"
        )
    structure_check_names = {
        check.get("name")
        for check in structure_checks
        if isinstance(check.get("name"), str)
    }
    missing_checks = sorted(_REQUIRED_STRUCTURE_CHECKS - structure_check_names)
    if missing_checks:
        raise PubMedReleaseError(
            "PubMed validation report is missing required structure checks: "
            + ", ".join(missing_checks)
        )

    inputs = report.get("inputs")
    report_shards = inputs.get("shards") if isinstance(inputs, Mapping) else None
    if not isinstance(report_shards, list) or not all(
        isinstance(name, str) for name in report_shards
    ):
        raise PubMedReleaseError(
            "PubMed validation report has no valid shard inventory"
        )

    compressed_report_shards = tuple(
        f"{name}.gz" if name.endswith(".ndjson") else name for name in report_shards
    )
    try:
        validated_report_shards = validate_shard_filenames(compressed_report_shards)
        expected_shards = validate_shard_filenames(shard_filenames)
    except PubMedReleaseError as exc:
        raise PubMedReleaseError(
            f"PubMed validation report has an invalid shard inventory: {exc}"
        ) from exc
    if len(validated_report_shards) != len(report_shards):
        raise PubMedReleaseError(
            "PubMed validation report has an invalid shard inventory"
        )
    if validated_report_shards != expected_shards:
        raise PubMedReleaseError(
            "PubMed validation report shard inventory does not match the release directory"
        )

    checks = report.get("checks")
    structure = checks.get("structure") if isinstance(checks, Mapping) else None
    records_total = (
        structure.get("records_total") if isinstance(structure, Mapping) else None
    )
    if (
        not isinstance(records_total, int)
        or isinstance(records_total, bool)
        or records_total <= 0
    ):
        raise PubMedReleaseError(
            "PubMed validation report has no positive record count"
        )


def local_shard_paths(
    data_folder: str | Path, validation_report_filename: str
) -> tuple[Path, ...]:
    """Validate and return the downloaded shards in one release folder."""

    folder = Path(data_folder)
    try:
        filenames = [path.name for path in folder.iterdir() if path.is_file()]
    except OSError as exc:
        raise PubMedReleaseError(
            f"unable to list PubMed release folder {folder}: {exc}"
        ) from exc

    shard_filenames = validate_shard_filenames(filenames)
    report_path = folder / validation_report_filename
    try:
        report = parse_validation_report(report_path.read_bytes())
    except OSError as exc:
        raise PubMedReleaseError(
            f"unable to read PubMed validation report {report_path}: {exc}"
        ) from exc
    validate_report(report, shard_filenames)
    return tuple(folder / filename for filename in shard_filenames)
