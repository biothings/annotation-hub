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
    return record


def write_shard(path, lines):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as output_file:
        output_file.writelines(lines)


def test_transform_namespaces_pubmed_metadata():
    document = parser.transform_pubmed_metadata_record(upstream_record())

    assert document == {
        "_id": "PMID:12345678",
        "pubmed": {
            "journal_name": "Journal of Examples",
            "journal_abbrev": "J Ex",
            "article_title": "A useful example",
            "volume": "12",
            "issue": "3",
            "pub_year": "2026",
            "pub_month": "6",
            "pub_day": "30",
            "abstract": "An abstract with Unicode: β.",
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
    ],
)
def test_rejects_invalid_records(record, message):
    with pytest.raises(parser.PubMedMetadataValidationError, match=message):
        parser.transform_pubmed_metadata_record(record)


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
