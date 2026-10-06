"""Helpers for validating RENCI Babel releases and their artifact inventory."""

import ast
import csv
import json
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class NodeNormReleaseError(ValueError):
    """Raised when Babel release metadata is missing or malformed."""


_RELEASE_PATTERN = re.compile(
    r"(?P<year>\d{4})(?P<month>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
    r"(?P<day>\d{1,2})"
)
_VERSION_LINE_PATTERN = re.compile(r"Babel\s+(?P<release>\S+)")
_ARTIFACT_FILENAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\.txt")
RELEASE_MANIFEST_FILENAME = "release-manifest.json"
RELEASE_MANIFEST_SCHEMA_VERSION = 2
DUPLICATE_CLIQUE_LEADERS_FILENAME = "duplicate_clique_leaders.tsv"
DUPLICATE_CLIQUE_LEADERS_RELATIVE_PATH = (
    f"reports/duckdb/{DUPLICATE_CLIQUE_LEADERS_FILENAME}"
)
_DUPLICATE_REPORT_COLUMNS = (
    "clique_leader",
    "filenames",
    "clique_leader_count",
)
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


@dataclass(frozen=True)
class NodeNormReleaseManifest:
    """The immutable set of Babel artifacts used to build one NodeNorm release."""

    release: str
    compendia: tuple[str, ...]
    conflations: tuple[str, ...]
    reports: tuple[str, ...] = (DUPLICATE_CLIQUE_LEADERS_RELATIVE_PATH,)


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


def artifact_filenames_from_index(index_html: str) -> tuple[str, ...]:
    """Return canonical ``*.txt`` artifacts from an HTTP directory index.

    Babel also publishes split copies such as ``Protein.txt.00``.  Those are
    deliberately excluded: only direct child links whose decoded basename ends
    exactly in ``.txt`` are complete compendium or conflation artifacts.
    """

    if not isinstance(index_html, str):
        raise NodeNormReleaseError("Babel artifact index is not text")

    parser = _LinkParser()
    parser.feed(index_html)
    parser.close()

    filenames = []
    seen = set()
    for href in parser.hrefs:
        parsed = urlsplit(href)
        if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
            continue
        filename = unquote(parsed.path)
        if not _ARTIFACT_FILENAME_PATTERN.fullmatch(filename):
            continue
        normalized_filename = filename.casefold()
        if normalized_filename in seen:
            raise NodeNormReleaseError(
                f"Babel artifact index repeats filename {filename!r}"
            )
        seen.add(normalized_filename)
        filenames.append(filename)

    if not filenames:
        raise NodeNormReleaseError(
            "Babel artifact index contains no canonical .txt files"
        )
    return tuple(sorted(filenames, key=str.casefold))


def _validate_artifact_filenames(
    filenames: Iterable[str], *, kind: str
) -> tuple[str, ...]:
    if not isinstance(filenames, Iterable) or isinstance(
        filenames, (str, bytes, Mapping)
    ):
        raise NodeNormReleaseError(f"NodeNorm manifest {kind} must be a list")

    validated = []
    seen = set()
    for filename in filenames:
        if not isinstance(filename, str) or not _ARTIFACT_FILENAME_PATTERN.fullmatch(
            filename
        ):
            raise NodeNormReleaseError(f"Invalid {kind} artifact filename {filename!r}")
        normalized_filename = filename.casefold()
        if normalized_filename in seen:
            raise NodeNormReleaseError(
                f"NodeNorm manifest repeats {kind} artifact {filename!r}"
            )
        seen.add(normalized_filename)
        validated.append(filename)

    if not validated:
        raise NodeNormReleaseError(f"NodeNorm manifest contains no {kind} artifacts")
    return tuple(sorted(validated, key=str.casefold))


def make_release_manifest(
    release: str,
    compendia: Iterable[str],
    conflations: Iterable[str],
    reports: Iterable[str] = (DUPLICATE_CLIQUE_LEADERS_RELATIVE_PATH,),
) -> NodeNormReleaseManifest:
    """Validate and construct a release manifest."""

    if not isinstance(reports, Iterable) or isinstance(reports, (str, bytes, Mapping)):
        raise NodeNormReleaseError(
            "NodeNorm manifest reports must be a list; rerun the NodeNorm dump"
        )
    reports = tuple(reports)
    if reports != (DUPLICATE_CLIQUE_LEADERS_RELATIVE_PATH,):
        raise NodeNormReleaseError(
            "NodeNorm manifest must declare the release's duplicate clique leader "
            "report; rerun the NodeNorm dump"
        )
    manifest = NodeNormReleaseManifest(
        release=validate_release(release),
        compendia=_validate_artifact_filenames(compendia, kind="compendium"),
        conflations=_validate_artifact_filenames(conflations, kind="conflation"),
        reports=tuple(reports),
    )
    compendia_by_normalized_name = {
        filename.casefold(): filename for filename in manifest.compendia
    }
    conflation_normalized_names = {
        filename.casefold() for filename in manifest.conflations
    }
    overlap = set(compendia_by_normalized_name) & conflation_normalized_names
    if overlap:
        raise NodeNormReleaseError(
            "NodeNorm manifest assigns artifacts to multiple roles: "
            + ", ".join(sorted(compendia_by_normalized_name[name] for name in overlap))
        )
    return manifest


def write_release_manifest(
    data_folder: str | Path, manifest: NodeNormReleaseManifest
) -> Path:
    """Atomically persist the exact upstream inventory used by the dumper."""

    validated = make_release_manifest(
        manifest.release, manifest.compendia, manifest.conflations, manifest.reports
    )
    folder = Path(data_folder)
    folder.mkdir(parents=True, exist_ok=True)
    manifest_path = folder / RELEASE_MANIFEST_FILENAME
    temporary_path = manifest_path.with_name(f"{manifest_path.name}.tmp")
    payload = {
        "schema_version": RELEASE_MANIFEST_SCHEMA_VERSION,
        "release": validated.release,
        "compendia": list(validated.compendia),
        "conflations": list(validated.conflations),
        "reports": list(validated.reports),
    }
    try:
        with temporary_path.open("w", encoding="utf-8") as manifest_handle:
            json.dump(payload, manifest_handle, indent=2, sort_keys=True)
            manifest_handle.write("\n")
        os.replace(temporary_path, manifest_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return manifest_path


def read_release_manifest(data_folder: str | Path) -> NodeNormReleaseManifest:
    """Read and validate a persisted NodeNorm release manifest."""

    manifest_path = Path(data_folder) / RELEASE_MANIFEST_FILENAME
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise NodeNormReleaseError(
            f"Unable to read NodeNorm release manifest {manifest_path}: {exc}"
        ) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeNormReleaseError(
            f"NodeNorm release manifest {manifest_path} is not valid JSON"
        ) from exc

    if not isinstance(payload, Mapping):
        raise NodeNormReleaseError("NodeNorm release manifest is not a JSON object")
    if payload.get("schema_version") != RELEASE_MANIFEST_SCHEMA_VERSION:
        raise NodeNormReleaseError(
            "Unsupported NodeNorm release manifest schema version "
            f"{payload.get('schema_version')!r}; rerun the NodeNorm dump"
        )
    return make_release_manifest(
        payload.get("release"),
        payload.get("compendia", ()),
        payload.get("conflations", ()),
        payload.get("reports", ()),
    )


def _report_list(value: str, column: str, *, quoted: bool = False) -> list[str]:
    """Read DuckDB lists (Babel quotes Biolink CURIEs, but not filenames)."""

    if (
        not isinstance(value, str)
        or not value.startswith("[")
        or not value.endswith("]")
    ):
        raise NodeNormReleaseError(f"{column} must be a bracketed list")
    if quoted:
        try:
            items = ast.literal_eval(value)
        except (ValueError, SyntaxError) as exc:
            raise NodeNormReleaseError(f"{column} must contain quoted strings") from exc
        if not isinstance(items, list) or any(
            not isinstance(item, str) for item in items
        ):
            raise NodeNormReleaseError(f"{column} must contain quoted strings")
        return items
    items = [item.strip() for item in value[1:-1].split(",")]
    if any(not item or any(char in item for char in "[]\"'") for item in items):
        raise NodeNormReleaseError(f"{column} contains an invalid list item")
    return items


def read_duplicate_clique_leaders(
    data_folder: str | Path,
) -> dict[str, tuple[str, ...]]:
    """Return reported collision sources in manifest order (the first wins).

    Each reported source must identify one distinct compendium. A repeated source
    is ambiguous because the report cannot select between two rows in one file.
    Header-only reports are valid for releases with no duplicate leaders.
    """

    folder = Path(data_folder)
    manifest = read_release_manifest(folder)
    report_path = folder / DUPLICATE_CLIQUE_LEADERS_FILENAME
    by_stem = {Path(filename).stem: filename for filename in manifest.compendia}
    collisions = {}
    try:
        with report_path.open(encoding="utf-8", newline="") as report_handle:
            rows = csv.DictReader(report_handle, delimiter="\t", strict=True)
            if (
                rows.fieldnames is None
                or len(rows.fieldnames) != len(set(rows.fieldnames))
                or not set(_DUPLICATE_REPORT_COLUMNS).issubset(rows.fieldnames)
            ):
                raise NodeNormReleaseError(
                    "missing or repeated required columns: "
                    + ", ".join(_DUPLICATE_REPORT_COLUMNS)
                )
            for row in rows:
                try:
                    if None in row or any(value is None for value in row.values()):
                        raise NodeNormReleaseError("row does not match its header")
                    leader = row["clique_leader"]
                    prefix, _, local_id = leader.partition(":")
                    if not prefix or not local_id or leader != leader.strip():
                        raise NodeNormReleaseError("clique_leader must be a CURIE")
                    if leader in collisions:
                        raise NodeNormReleaseError(f"repeated clique_leader {leader!r}")
                    filenames = _report_list(row["filenames"], "filenames")
                    # Older Babel reports omit this metadata. Validate it when
                    # available, but source selection only requires filenames.
                    types = (
                        _report_list(row["biolink_types"], "biolink_types", quoted=True)
                        if "biolink_types" in row
                        else None
                    )
                    counts = (
                        _report_list(
                            row["clique_identifier_counts"], "clique_identifier_counts"
                        )
                        if "clique_identifier_counts" in row
                        else None
                    )
                    count = row["clique_leader_count"]
                    if not count.isascii() or not count.isdigit() or int(count) < 2:
                        raise NodeNormReleaseError(
                            "clique_leader_count must be at least 2"
                        )
                    if not all(
                        len(values) == int(count)
                        for values in (filenames, types, counts)
                        if values is not None
                    ):
                        raise NodeNormReleaseError(
                            "list lengths do not match clique_leader_count"
                        )
                    if len(set(filenames)) != len(filenames):
                        raise NodeNormReleaseError("repeated compendium is ambiguous")
                    unknown = set(filenames) - set(by_stem)
                    if unknown:
                        raise NodeNormReleaseError(
                            "unknown compendium: " + ", ".join(sorted(unknown))
                        )
                    if types is not None and any(
                        not re.fullmatch(r"biolink:[A-Za-z][A-Za-z0-9_]*", value)
                        for value in types
                    ):
                        raise NodeNormReleaseError("invalid biolink_types entry")
                    if counts is not None and any(
                        not value.isascii() or not value.isdigit() or int(value) < 1
                        for value in counts
                    ):
                        raise NodeNormReleaseError(
                            "clique_identifier_counts must be positive integers"
                        )
                    allowed = {by_stem[filename] for filename in filenames}
                    collisions[leader] = tuple(
                        filename
                        for filename in manifest.compendia
                        if filename in allowed
                    )
                except NodeNormReleaseError as exc:
                    raise NodeNormReleaseError(f"line {rows.line_num}: {exc}") from exc
    except (OSError, UnicodeError, csv.Error, NodeNormReleaseError) as exc:
        raise NodeNormReleaseError(
            f"Invalid NodeNorm duplicate clique leader report {report_path}: {exc}; "
            "rerun the NodeNorm dump"
        ) from exc
    return collisions


def local_compendium_paths(data_folder: str | Path) -> tuple[Path, ...]:
    """Return manifest compendia after verifying the downloaded artifact set."""

    folder = Path(data_folder)
    manifest = read_release_manifest(folder)
    expected = set(manifest.compendia) | set(manifest.conflations)
    try:
        actual = {
            path.name
            for path in folder.iterdir()
            if path.is_file() and _ARTIFACT_FILENAME_PATTERN.fullmatch(path.name)
        }
    except OSError as exc:
        raise NodeNormReleaseError(
            f"Unable to list NodeNorm release folder {folder}: {exc}"
        ) from exc

    if not expected.issubset(actual):
        missing = sorted(expected - actual)
        raise NodeNormReleaseError(
            "Downloaded NodeNorm artifacts do not match the release manifest "
            "(missing: " + ", ".join(missing) + ")"
        )

    empty = [
        filename for filename in expected if (folder / filename).stat().st_size <= 0
    ]
    if empty:
        raise NodeNormReleaseError(
            "Downloaded NodeNorm artifacts are empty: " + ", ".join(sorted(empty))
        )
    read_duplicate_clique_leaders(folder)
    return tuple(folder / filename for filename in manifest.compendia)
