"""Streaming parser and validation for PubMed metadata NDJSON shards."""

import gzip
import json
import re
from pathlib import Path
from typing import Iterator


EXPECTED_RECORD_FIELDS = (
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
PUBMED_FIELDS = EXPECTED_RECORD_FIELDS[1:]
PMID_PATTERN = re.compile(r"^PMID:[1-9][0-9]*$")


class PubMedMetadataValidationError(ValueError):
    """Raised when an input shard or record does not match the contract."""


def _location(source: str, line_number: int | None) -> str:
    if line_number is None:
        return source
    return f"{source}:{line_number}"


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

    expected_fields = set(EXPECTED_RECORD_FIELDS)
    actual_fields = set(record)
    missing_fields = sorted(expected_fields - actual_fields)
    extra_fields = sorted(str(field) for field in actual_fields - expected_fields)
    if missing_fields or extra_fields:
        details = []
        if missing_fields:
            details.append(f"missing fields: {', '.join(missing_fields)}")
        if extra_fields:
            details.append(f"unexpected fields: {', '.join(extra_fields)}")
        raise PubMedMetadataValidationError(f"{location}: {'; '.join(details)}")

    non_string_fields = sorted(
        field for field in EXPECTED_RECORD_FIELDS if not isinstance(record[field], str)
    )
    if non_string_fields:
        raise PubMedMetadataValidationError(
            f"{location}: fields must contain strings: {', '.join(non_string_fields)}"
        )

    pubmed_id = record["id"]
    if PMID_PATTERN.fullmatch(pubmed_id) is None:
        raise PubMedMetadataValidationError(
            f"{location}: invalid PubMed identifier {pubmed_id!r}"
        )

    return {
        "_id": pubmed_id,
        "pubmed": {field: record[field] for field in PUBMED_FIELDS},
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
