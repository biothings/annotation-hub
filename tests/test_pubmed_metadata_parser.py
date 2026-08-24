import gzip
import importlib.util
import json
from pathlib import Path

import pytest


PARSER_PATH = (
    Path(__file__).parents[1] / "plugins" / "pubmed_metadata" / "parser.py"
)
PARSER_SPEC = importlib.util.spec_from_file_location(
    "pubmed_metadata_parser", PARSER_PATH
)
parser = importlib.util.module_from_spec(PARSER_SPEC)
assert PARSER_SPEC.loader is not None
PARSER_SPEC.loader.exec_module(parser)


def upstream_record(**overrides):
    record = {
        "id": "PMID:12345678",
        "identifiers": [
            "PMID:12345678",
            "doi:10.1000/Example",
            "PMC:PMC1234567",
        ],
        "journal_name": "Journal of Examples",
        "journal_abbrev": "J Ex",
        "article_title": "A useful example",
        "volume": "12",
        "issue": "3",
        "pub_year": "2026",
        "pub_month": "6",
        "pub_day": "30",
        "abstract": "An abstract with Unicode: β.",
    }
    record.update(overrides)
    if "id" in overrides and "identifiers" not in overrides:
        record["identifiers"] = [record["id"]]
    return record


def legacy_record(**overrides):
    record = upstream_record(**overrides)
    del record["identifiers"]
    return record


def write_shard(path, lines):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as output_file:
        output_file.writelines(lines)


def test_transform_namespaces_pubmed_metadata():
    document = parser.transform_pubmed_metadata_record(upstream_record())

    assert document == {
        "_id": "PMID:12345678",
        "pubmed": {
            "identifiers": [
                "PMID:12345678",
                "doi:10.1000/Example",
                "PMC:PMC1234567",
            ],
            "journal": {
                "name": "Journal of Examples",
                "abbr": "J Ex",
            },
            "title": "A useful example",
            "vol": "12",
            "iss": "3",
            "abstract": "An abstract with Unicode: β.",
            "pub_date": "2026-06-30",
        },
    }


def test_streams_gzip_ndjson(tmp_path):
    shard_path = tmp_path / "pubmed_metadata_00000.ndjson.gz"
    records = [upstream_record(), upstream_record(id="PMID:87654321")]
    write_shard(shard_path, [json.dumps(record) + "\n" for record in records])

    documents = list(parser.iter_pubmed_metadata_documents(shard_path))

    assert [document["_id"] for document in documents] == [
        "PMID:12345678",
        "PMID:87654321",
    ]
    assert documents[0]["pubmed"]["abstract"].endswith("β.")


def test_legacy_schema_defaults_identifiers_to_pmid():
    document = parser.transform_pubmed_metadata_record(legacy_record())

    assert document["pubmed"]["identifiers"] == ["PMID:12345678"]
    assert "pubdate_raw" not in document["pubmed"]


def test_normalizes_well_formed_pmcid_aliases_preserving_order():
    document = parser.transform_pubmed_metadata_record(
        upstream_record(
            identifiers=[
                "PMID:12345678",
                "PMCID:PMC7654321",
                "doi:10.1000/Example",
            ]
        )
    )

    assert document["pubmed"]["identifiers"] == [
        "PMID:12345678",
        "PMC:PMC7654321",
        "doi:10.1000/Example",
    ]


def test_preserves_malformed_pmcid_aliases():
    document = parser.transform_pubmed_metadata_record(
        upstream_record(
            identifiers=["PMID:12345678", "PMCID:wh_2021_113"]
        )
    )

    assert document["pubmed"]["identifiers"] == [
        "PMID:12345678",
        "PMCID:wh_2021_113",
    ]


def test_deduplicates_canonical_pmc_and_pmcid_alias_only():
    document = parser.transform_pubmed_metadata_record(
        upstream_record(
            identifiers=[
                "PMID:12345678",
                "PMC:PMC7654321",
                "PMCID:PMC7654321",
                "doi:10.1000/Example",
                "doi:10.1000/Example",
            ]
        )
    )

    assert document["pubmed"]["identifiers"] == [
        "PMID:12345678",
        "PMC:PMC7654321",
        "doi:10.1000/Example",
        "doi:10.1000/Example",
    ]


def test_alias_collision_keeps_the_first_identifier_position():
    document = parser.transform_pubmed_metadata_record(
        upstream_record(
            identifiers=[
                "PMID:12345678",
                "PMCID:PMC7654321",
                "doi:10.1000/Example",
                "PMC:PMC7654321",
            ]
        )
    )

    assert document["pubmed"]["identifiers"] == [
        "PMID:12345678",
        "PMC:PMC7654321",
        "doi:10.1000/Example",
    ]


@pytest.mark.parametrize("field", ["pubdate", "pub_date"])
def test_accepts_verbatim_pubdate_field_during_upstream_transition(field):
    record = upstream_record(
        pub_year="1998",
        pub_month="Dec-1999 Jan",
        pub_day="",
    )
    record[field] = "1998 Dec-1999 Jan"

    document = parser.transform_pubmed_metadata_record(record)

    assert document["pubmed"]["pubdate_raw"] == "1998 Dec-1999 Jan"
    for component in ("pub_year", "pub_month", "pub_day"):
        assert component not in document["pubmed"]
    assert "pub_date" not in document["pubmed"]


def test_preserves_verbatim_pubdate_alongside_exact_query_date():
    document = parser.transform_pubmed_metadata_record(
        upstream_record(pubdate="2026 Jun 30")
    )

    assert document["pubmed"]["pubdate_raw"] == "2026 Jun 30"
    assert document["pubmed"]["pub_date"] == "2026-06-30"


def test_exact_looking_medline_date_remains_raw_only():
    document = parser.transform_pubmed_metadata_record(
        upstream_record(
            pub_year="2019",
            pub_month="Mar 15",
            pub_day="",
            pubdate="2019 Mar 15",
        )
    )

    assert document["pubmed"]["pubdate_raw"] == "2019 Mar 15"
    assert "pub_date" not in document["pubmed"]


@pytest.mark.parametrize(
    ("record", "message"),
    [
        (
            {key: value for key, value in upstream_record().items() if key != "abstract"},
            "missing fields: abstract",
        ),
        (
            upstream_record(unexpected="value"),
            "unexpected fields: unexpected",
        ),
        (upstream_record(id="12345678"), "invalid PubMed identifier"),
        (upstream_record(pub_year=2026), "fields must contain strings: pub_year"),
        (
            upstream_record(identifiers="PMID:12345678"),
            "identifiers must be a list of nonempty strings",
        ),
        (
            upstream_record(identifiers=["PMID:12345678", ""]),
            "identifiers must be a list of nonempty strings",
        ),
        (
            upstream_record(identifiers=["doi:10.1000/example"]),
            "identifiers must contain 'PMID:12345678'",
        ),
    ],
)
def test_rejects_invalid_records(record, message):
    with pytest.raises(parser.PubMedMetadataValidationError, match=message):
        parser.transform_pubmed_metadata_record(record)


@pytest.mark.parametrize(
    ("date_parts", "expected_date"),
    [
        ({"pub_year": "2026", "pub_month": "6", "pub_day": "3"}, "2026-06-03"),
        ({"pub_year": "2024", "pub_month": "feb", "pub_day": "29"}, "2024-02-29"),
    ],
)
def test_builds_exact_dates(date_parts, expected_date):
    document = parser.transform_pubmed_metadata_record(
        upstream_record(**date_parts)
    )

    assert document["pubmed"]["pub_date"] == expected_date


@pytest.mark.parametrize(
    "date_parts",
    [
        {"pub_year": "2026", "pub_month": "Jun", "pub_day": ""},
        {"pub_year": "2026", "pub_month": "", "pub_day": ""},
        {"pub_year": "", "pub_month": "", "pub_day": ""},
        {"pub_year": "", "pub_month": "Jun", "pub_day": ""},
        {"pub_year": "2026", "pub_month": "", "pub_day": "15"},
        {"pub_year": "26", "pub_month": "Jun", "pub_day": "15"},
        {"pub_year": "2026", "pub_month": "Sep-Dec", "pub_day": ""},
        {"pub_year": "2026", "pub_month": "Spring", "pub_day": ""},
        {"pub_year": "1998", "pub_month": "Dec-1999 Jan", "pub_day": ""},
        {"pub_year": "2026", "pub_month": "Smarch", "pub_day": "15"},
        {"pub_year": "2026", "pub_month": "13", "pub_day": "15"},
        {"pub_year": "2026", "pub_month": "Feb", "pub_day": "30"},
    ],
)
def test_discards_input_components_when_no_exact_date_can_be_derived(date_parts):
    document = parser.transform_pubmed_metadata_record(upstream_record(**date_parts))

    for component in ("pub_year", "pub_month", "pub_day"):
        assert component not in document["pubmed"]
    assert "pub_date" not in document["pubmed"]


def test_rejects_verbatim_pubdate_without_identifiers():
    record = legacy_record()
    record["pubdate"] = "2026 Jun 30"

    with pytest.raises(
        parser.PubMedMetadataValidationError,
        match="pubdate requires identifiers",
    ):
        parser.transform_pubmed_metadata_record(record)


def test_rejects_multiple_verbatim_pubdate_fields():
    record = upstream_record(pubdate="2026 Jun 30", pub_date="2026 Jun 30")

    with pytest.raises(
        parser.PubMedMetadataValidationError,
        match="multiple verbatim publication-date fields",
    ):
        parser.transform_pubmed_metadata_record(record)


def test_rejects_non_string_verbatim_pubdate():
    with pytest.raises(
        parser.PubMedMetadataValidationError,
        match="fields must contain strings: pubdate",
    ):
        parser.transform_pubmed_metadata_record(upstream_record(pubdate=20260630))


def test_reports_shard_and_line_for_invalid_json(tmp_path):
    shard_path = tmp_path / "pubmed_metadata_00000.ndjson.gz"
    write_shard(shard_path, [json.dumps(upstream_record()) + "\n", "{broken}\n"])

    with pytest.raises(
        parser.PubMedMetadataValidationError,
        match=r"pubmed_metadata_00000\.ndjson\.gz:2: invalid JSON",
    ):
        list(parser.iter_pubmed_metadata_documents(shard_path))


def test_rejects_blank_lines(tmp_path):
    shard_path = tmp_path / "pubmed_metadata_00000.ndjson.gz"
    write_shard(shard_path, ["\n"])

    with pytest.raises(
        parser.PubMedMetadataValidationError,
        match=r"pubmed_metadata_00000\.ndjson\.gz:1: blank lines",
    ):
        list(parser.iter_pubmed_metadata_documents(shard_path))


def test_rejects_invalid_gzip(tmp_path):
    shard_path = tmp_path / "pubmed_metadata_00000.ndjson.gz"
    shard_path.write_bytes(b"not a gzip stream")

    with pytest.raises(
        parser.PubMedMetadataValidationError,
        match="unable to read gzip stream",
    ):
        list(parser.iter_pubmed_metadata_documents(shard_path))
