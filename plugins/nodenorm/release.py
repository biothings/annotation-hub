"""Helpers for validating RENCI Babel releases and their artifact inventory."""

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
RELEASE_MANIFEST_SCHEMA_VERSION = 1
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
) -> NodeNormReleaseManifest:
    """Validate and construct a release manifest."""

    manifest = NodeNormReleaseManifest(
        release=validate_release(release),
        compendia=_validate_artifact_filenames(compendia, kind="compendium"),
        conflations=_validate_artifact_filenames(conflations, kind="conflation"),
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
        manifest.release, manifest.compendia, manifest.conflations
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
            f"{payload.get('schema_version')!r}"
        )
    return make_release_manifest(
        payload.get("release"),
        payload.get("compendia", ()),
        payload.get("conflations", ()),
    )


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
    return tuple(folder / filename for filename in manifest.compendia)
