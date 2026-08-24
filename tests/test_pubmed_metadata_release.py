import gzip
import importlib.util
import json
from pathlib import Path

import pytest

RELEASE_PATH = Path(__file__).parents[1] / "plugins" / "pubmed_metadata" / "release.py"
RELEASE_SPEC = importlib.util.spec_from_file_location(
    "pubmed_metadata_release", RELEASE_PATH
)
release = importlib.util.module_from_spec(RELEASE_SPEC)
assert RELEASE_SPEC.loader is not None
RELEASE_SPEC.loader.exec_module(release)


def index_html(*hrefs):
    return (
        "<html><body>"
        + "".join(f'<a href="{href}">{href}</a>' for href in hrefs)
        + "</body></html>"
    )


def validation_report(shard_count=2, **overrides):
    structure_check_names = (
        "shards-found",
        "records-present",
        "json-parse",
        "record-fields",
        "no-nulls",
        "id-format",
        "pmid-unique",
        "no-extra-fields",
        "month-format",
    )
    report = {
        "status": "warn",
        "errors": [],
        "inputs": {
            "shards": [
                f"pubmed_metadata_{index:05d}.ndjson" for index in range(shard_count)
            ]
        },
        "checks_run": [
            *[
                {
                    "name": name,
                    "section": "structure",
                    "status": "pass",
                }
                for name in structure_check_names
            ],
            {
                "name": "core-fields",
                "section": "field accuracy",
                "status": "warn",
            },
        ],
        "checks": {"structure": {"records_total": 10}},
    }
    report.update(overrides)
    return report


def write_validation_report(folder, report):
    payload = gzip.compress(json.dumps(report).encode("utf-8"))
    (folder / "validation_report.json.gz").write_bytes(payload)


def test_release_discovery_sorts_dates_and_ignores_unrelated_links():
    html = index_html(
        "../",
        "2026jun30/",
        "notes.txt",
        "2026aug5/",
        "2026aug04/",
        "2025dec31/",
        "2026feb30/",
        "nested/2027jan1/",
    )

    assert release.releases_from_index(html) == (
        "2026aug5",
        "2026aug04",
        "2026jun30",
        "2025dec31",
    )


def test_shard_validation_accepts_a_contiguous_dynamic_inventory():
    filenames = [
        "validation_report.json.gz",
        "pubmed_metadata_00002.ndjson.gz",
        "pubmed_metadata_00000.ndjson.gz",
        "pubmed_metadata_00001.ndjson.gz",
    ]

    assert release.validate_shard_filenames(filenames) == (
        "pubmed_metadata_00000.ndjson.gz",
        "pubmed_metadata_00001.ndjson.gz",
        "pubmed_metadata_00002.ndjson.gz",
    )


def test_shard_validation_accepts_unpadded_indexes_and_sorts_numerically():
    filenames = [
        "pubmed_metadata_2.ndjson.gz",
        "pubmed_metadata_0.ndjson.gz",
        "pubmed_metadata_1.ndjson.gz",
    ]

    assert release.validate_shard_filenames(filenames) == (
        "pubmed_metadata_0.ndjson.gz",
        "pubmed_metadata_1.ndjson.gz",
        "pubmed_metadata_2.ndjson.gz",
    )


@pytest.mark.parametrize(
    "filenames",
    [
        [],
        ["pubmed_metadata_00001.ndjson.gz"],
        [
            "pubmed_metadata_00000.ndjson.gz",
            "pubmed_metadata_00002.ndjson.gz",
        ],
        [
            "pubmed_metadata_0.ndjson.gz",
            "pubmed_metadata_00000.ndjson.gz",
        ],
    ],
)
def test_shard_validation_rejects_empty_or_noncontiguous_inventories(filenames):
    with pytest.raises(release.PubMedReleaseError):
        release.validate_shard_filenames(filenames)


def test_validation_report_allows_advisory_warnings():
    shards = (
        "pubmed_metadata_00000.ndjson.gz",
        "pubmed_metadata_00001.ndjson.gz",
    )

    release.validate_report(validation_report(), shards)


def test_validation_report_allows_month_format_warning_for_new_shards():
    report = validation_report(
        inputs={
            "shards": [
                "pubmed_metadata_0.ndjson.gz",
                "pubmed_metadata_1.ndjson.gz",
            ]
        }
    )
    next(
        check for check in report["checks_run"] if check["name"] == "month-format"
    )["status"] = "warn"

    release.validate_report(
        report,
        (
            "pubmed_metadata_0.ndjson.gz",
            "pubmed_metadata_1.ndjson.gz",
        ),
    )


def test_validation_report_rejects_other_structure_warnings():
    report = validation_report()
    next(
        check for check in report["checks_run"] if check["name"] == "record-fields"
    )["status"] = "warn"

    with pytest.raises(
        release.PubMedReleaseError, match="did not pass all structure checks"
    ):
        release.validate_report(
            report,
            (
                "pubmed_metadata_00000.ndjson.gz",
                "pubmed_metadata_00001.ndjson.gz",
            ),
        )


def test_validation_report_parses_plain_and_gzip_json():
    report = validation_report()
    plain_payload = json.dumps(report).encode("utf-8")

    assert release.parse_validation_report(plain_payload) == report
    assert release.parse_validation_report(gzip.compress(plain_payload)) == report


@pytest.mark.parametrize(
    "report",
    [
        validation_report(status="fail"),
        validation_report(errors=[{"code": "bad"}]),
        validation_report(
            checks_run=[
                {
                    "name": "record-fields",
                    "section": "structure",
                    "status": "fail",
                }
            ]
        ),
        validation_report(inputs={"shards": ["pubmed_metadata_00000.ndjson"]}),
        validation_report(
            inputs={
                "shards": [
                    "pubmed_metadata_00000.ndjson",
                    "pubmed_metadata_00000.ndjson",
                ]
            }
        ),
        validation_report(
            checks_run=[
                {
                    "name": "shards-found",
                    "section": "structure",
                    "status": "pass",
                }
            ]
        ),
        validation_report(
            checks_run=[
                *validation_report()["checks_run"],
                {
                    "name": "month-format",
                    "section": "structure",
                    "status": "warn",
                },
            ]
        ),
        validation_report(checks={"structure": {"records_total": 0}}),
    ],
)
def test_validation_report_rejects_unsafe_releases(report):
    shards = (
        "pubmed_metadata_00000.ndjson.gz",
        "pubmed_metadata_00001.ndjson.gz",
    )

    with pytest.raises(release.PubMedReleaseError):
        release.validate_report(report, shards)


def test_local_shards_are_discovered_in_index_order(tmp_path):
    for index in (2, 0, 1):
        (tmp_path / f"pubmed_metadata_{index:05d}.ndjson.gz").touch()
    write_validation_report(tmp_path, validation_report(shard_count=3))

    assert [
        path.name
        for path in release.local_shard_paths(tmp_path, "validation_report.json.gz")
    ] == [
        "pubmed_metadata_00000.ndjson.gz",
        "pubmed_metadata_00001.ndjson.gz",
        "pubmed_metadata_00002.ndjson.gz",
    ]


def test_local_unpadded_shards_use_a_plain_json_report(tmp_path):
    shard_names = [
        "pubmed_metadata_0.ndjson.gz",
        "pubmed_metadata_1.ndjson.gz",
    ]
    for shard_name in reversed(shard_names):
        (tmp_path / shard_name).touch()
    report = validation_report(inputs={"shards": shard_names})
    next(
        check for check in report["checks_run"] if check["name"] == "month-format"
    )["status"] = "warn"
    report_filename = "validation_report-20260821.json"
    (tmp_path / report_filename).write_text(json.dumps(report), encoding="utf-8")

    assert [
        path.name for path in release.local_shard_paths(tmp_path, report_filename)
    ] == shard_names


def test_local_shards_reject_a_missing_final_shard(tmp_path):
    for index in (0, 1):
        (tmp_path / f"pubmed_metadata_{index:05d}.ndjson.gz").touch()
    write_validation_report(tmp_path, validation_report(shard_count=3))

    with pytest.raises(release.PubMedReleaseError, match="does not match"):
        release.local_shard_paths(tmp_path, "validation_report.json.gz")


@pytest.mark.parametrize("report_payload", [None, b"not gzip"])
def test_local_shards_require_a_readable_validation_report(tmp_path, report_payload):
    (tmp_path / "pubmed_metadata_00000.ndjson.gz").touch()
    if report_payload is not None:
        (tmp_path / "validation_report.json.gz").write_bytes(report_payload)

    with pytest.raises(release.PubMedReleaseError):
        release.local_shard_paths(tmp_path, "validation_report.json.gz")
