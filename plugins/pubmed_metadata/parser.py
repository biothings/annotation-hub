"""Streaming parser and validation for PubMed metadata NDJSON shards."""

import gzip
import json
import re
from datetime import date
from pathlib import Path
from typing import Iterator


BASE_RECORD_FIELDS = (
    "id",
    "journal_name",
    "journal_abbrev",
    "article_title",
    "volume",
    "issue",
    "pub_year",
    "pub_month",
    "pub_day",
    "abstract",
)
IDENTIFIERS_FIELD = "identifiers"
# pubmed2db PR #17 currently uses ``pub_date`` for the verbatim value, while
# ``pubdate`` is under consideration to match NCBI. Accept either upstream name
# during that transition, but always store the verbatim value as ``pubdate_raw``
# so that neither upstream spelling collides with ``pub_date``, which on our
# side means the normalized Elasticsearch query date and nothing else.
PUBDATE_INPUT_FIELDS = ("pubdate", "pub_date")
BASE_RECORD_FIELD_SET = frozenset(BASE_RECORD_FIELDS)
ALLOWED_RECORD_FIELDS = BASE_RECORD_FIELD_SET | {
    IDENTIFIERS_FIELD,
    *PUBDATE_INPUT_FIELDS,
}
PMID_PATTERN = re.compile(r"^PMID:[1-9][0-9]*$")
YEAR_PATTERN = re.compile(r"^[0-9]{4}$")
NUMERIC_DATE_PART_PATTERN = re.compile(r"^[0-9]{1,2}$")
# PubMed's exported abbreviations are an English data contract. Keep this fixed
# rather than deriving it from the process locale through ``calendar``.
MONTH_NUMBERS = {
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


class PubMedMetadataValidationError(ValueError):
    """Raised when an input shard or record does not match the contract."""


def _location(source: str, line_number: int | None) -> str:
    if line_number is None:
        return source
    return f"{source}:{line_number}"


def _parse_exact_month(value: str) -> int | None:
    if NUMERIC_DATE_PART_PATTERN.fullmatch(value):
        month = int(value)
    else:
        month = MONTH_NUMBERS.get(value.lower(), 0)

    return month if 1 <= month <= 12 else None


def _build_exact_pub_date(record: dict) -> str | None:
    """Return an exact ISO date, or ``None`` when any part is not exact.

    Date derivation is intentionally non-fatal: unusual or malformed-looking
    input components simply do not create a query date. Their fidelity value is
    the source-only ``pubdate_raw`` when the upstream schema provides it.
    """

    year_value = record["pub_year"]
    month_value = record["pub_month"]
    day_value = record["pub_day"]

    if not year_value or not month_value or not day_value:
        return None

    if YEAR_PATTERN.fullmatch(year_value) is None or int(year_value) == 0:
        return None

    month = _parse_exact_month(month_value)
    if month is None:
        return None
    if NUMERIC_DATE_PART_PATTERN.fullmatch(day_value) is None:
        return None

    day = int(day_value)
    try:
        publication_date = date(int(year_value), month, day)
    except ValueError:
        return None
    return publication_date.isoformat()


def _validate_record_fields(record: dict, location: str) -> str | None:
    """Validate a supported upstream schema and return its raw-date field."""

    actual_fields = set(record)
    missing_fields = sorted(BASE_RECORD_FIELD_SET - actual_fields)
    extra_fields = sorted(str(field) for field in actual_fields - ALLOWED_RECORD_FIELDS)
    if missing_fields or extra_fields:
        details = []
        if missing_fields:
            details.append(f"missing fields: {', '.join(missing_fields)}")
        if extra_fields:
            details.append(f"unexpected fields: {', '.join(extra_fields)}")
        raise PubMedMetadataValidationError(f"{location}: {'; '.join(details)}")

    pubdate_fields = [field for field in PUBDATE_INPUT_FIELDS if field in record]
    if len(pubdate_fields) > 1:
        raise PubMedMetadataValidationError(
            f"{location}: multiple verbatim publication-date fields: "
            + ", ".join(pubdate_fields)
        )
    if pubdate_fields and IDENTIFIERS_FIELD not in record:
        raise PubMedMetadataValidationError(
            f"{location}: {pubdate_fields[0]} requires identifiers"
        )
    return pubdate_fields[0] if pubdate_fields else None


def _validated_identifiers(record: dict, pubmed_id: str, location: str) -> list[str]:
    """Return upstream identifiers, or the PMID for legacy ten-field data."""

    if IDENTIFIERS_FIELD not in record:
        return [pubmed_id]

    identifiers = record[IDENTIFIERS_FIELD]
    if not isinstance(identifiers, list) or not all(
        isinstance(identifier, str) and identifier for identifier in identifiers
    ):
        raise PubMedMetadataValidationError(
            f"{location}: identifiers must be a list of nonempty strings"
        )
    if pubmed_id not in identifiers:
        raise PubMedMetadataValidationError(
            f"{location}: identifiers must contain {pubmed_id!r}"
        )
    return list(identifiers)


def transform_pubmed_metadata_record(
    record: object,
    *,
    source: str = "<record>",
    line_number: int | None = None,
) -> dict:
    """Validate and namespace one upstream PubMed record."""

    location = _location(source, line_number)
    if not isinstance(record, dict):
        raise PubMedMetadataValidationError(
            f"{location}: expected a JSON object, got {type(record).__name__}"
        )

    pubdate_field = _validate_record_fields(record, location)

    non_string_fields = sorted(
        field for field in BASE_RECORD_FIELDS if not isinstance(record[field], str)
    )
    if pubdate_field is not None and not isinstance(record[pubdate_field], str):
        non_string_fields.append(pubdate_field)
    if non_string_fields:
        raise PubMedMetadataValidationError(
            f"{location}: fields must contain strings: {', '.join(non_string_fields)}"
        )

    pubmed_id = record["id"]
    if PMID_PATTERN.fullmatch(pubmed_id) is None:
        raise PubMedMetadataValidationError(
            f"{location}: invalid PubMed identifier {pubmed_id!r}"
        )
    identifiers = _validated_identifiers(record, pubmed_id, location)

    pubmed = {
        "identifiers": identifiers,
        "journal": {
            "name": record["journal_name"],
            "abbr": record["journal_abbrev"],
        },
        "title": record["article_title"],
        "vol": record["volume"],
        "iss": record["issue"],
        "abstract": record["abstract"],
    }
    if pubdate_field is not None:
        pubmed["pubdate_raw"] = record[pubdate_field]

    pub_date = _build_exact_pub_date(record)
    if pub_date is not None:
        pubmed["pub_date"] = pub_date

    return {
        "_id": pubmed_id,
        "pubmed": pubmed,
    }


def iter_pubmed_metadata_documents(data_path: str | Path) -> Iterator[dict]:
    """Yield validated documents from a gzip-compressed NDJSON shard."""

    path = Path(data_path)
    try:
        with gzip.open(
            path,
            mode="rt",
            encoding="utf-8",
            errors="strict",
            newline="",
        ) as input_file:
            for line_number, line in enumerate(input_file, start=1):
                if not line.strip():
                    raise PubMedMetadataValidationError(
                        f"{path}:{line_number}: blank lines are not allowed"
                    )
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as error:
                    raise PubMedMetadataValidationError(
                        f"{path}:{line_number}: invalid JSON: {error.msg}"
                    ) from error

                yield transform_pubmed_metadata_record(
                    record,
                    source=str(path),
                    line_number=line_number,
                )
    except PubMedMetadataValidationError:
        raise
    except (EOFError, OSError, UnicodeError) as error:
        raise PubMedMetadataValidationError(
            f"{path}: unable to read gzip stream: {error}"
        ) from error
