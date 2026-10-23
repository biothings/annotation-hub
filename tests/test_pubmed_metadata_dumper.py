import gzip
import asyncio
import ast
from copy import deepcopy
from datetime import datetime
from functools import partial
import importlib.util
from importlib.metadata import PackageNotFoundError, distribution
import inspect
import json
import logging
import os
import sys
import types
from pathlib import Path

import pytest
import requests

PLUGIN_PATH = Path(__file__).parents[1] / "plugins" / "pubmed_metadata"
ROOT_URL = "https://stars.renci.org/var/babel_outputs/pubmed2db/"
STRUCTURE_CHECK_NAMES = (
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


class FakeResponse:
    def __init__(self, *, text="", content=b"", status_code=200):
        self.text = text
        self.content = content
        self.status_code = status_code
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        response = self.responses[url]
        if isinstance(response, list):
            response = response.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def index_html(*hrefs):
    return (
        "<html><body>"
        + "".join(f'<a href="{href}">{href}</a>' for href in hrefs)
        + "</body></html>"
    )


def validation_report(
    *,
    shard_count=2,
    status="pass",
    padded=True,
    compressed=False,
    month_status="pass",
):
    shard_suffix = ".ndjson.gz" if compressed else ".ndjson"

    def shard_name(index):
        shard_index = f"{index:05d}" if padded else str(index)
        return f"pubmed_metadata_{shard_index}{shard_suffix}"

    return {
        "status": status,
        "errors": [],
        "inputs": {"shards": [shard_name(index) for index in range(shard_count)]},
        "checks_run": [
            {
                "name": name,
                "section": "structure",
                "status": month_status if name == "month-format" else "pass",
            }
            for name in STRUCTURE_CHECK_NAMES
        ],
        "checks": {"structure": {"records_total": 10}},
    }


def packed_report(**kwargs):
    return gzip.compress(json.dumps(validation_report(**kwargs)).encode("utf-8"))


def plain_report(**kwargs):
    return json.dumps(validation_report(**kwargs)).encode("utf-8")


def write_local_release(folder, report_filename=None):
    folder.mkdir(parents=True, exist_ok=True)
    if report_filename is None:
        report_filename = (
            "validation_report.json.gz"
            if folder.name == "2026aug5"
            else f"validation_report-{datetime.strptime(folder.name, '%Y%b%d'):%Y%m%d}.json"
        )
    padded = report_filename == "validation_report.json.gz"
    for index in range(2):
        shard_index = f"{index:05d}" if padded else str(index)
        (folder / f"pubmed_metadata_{shard_index}.ndjson.gz").write_bytes(
            gzip.compress(b"{}\n")
        )
    report = plain_report(padded=padded, compressed=not padded)
    if report_filename.endswith(".gz"):
        report = gzip.compress(report)
    (folder / report_filename).write_bytes(report)


@pytest.fixture
def dumper_module(monkeypatch, tmp_path):
    package_name = "_test_pubmed_metadata_plugin"
    package = types.ModuleType(package_name)
    package.__path__ = [str(PLUGIN_PATH)]
    monkeypatch.setitem(sys.modules, package_name, package)

    class DumperException(Exception):
        pass

    class HTTPDumper:
        SUFFIX_ATTR = "release"

        def __init__(self):
            self.client = None
            self.logger = logging.getLogger("test_pubmed_metadata_dumper")
            self.release = None
            self.to_dump = []
            self.src_doc = {"_id": self.SRC_NAME, "download": {}}
            self._current_release = None
            self.src_root_folder = self.SRC_ROOT_FOLDER
            self.src_name = self.SRC_NAME
            self.archive = True
            self.steps = ["dump", "post"]
            self.logfile = None
            self.t0 = 0
            self.src_dump = types.SimpleNamespace(save=lambda doc: None)
            self.base_post_dump_called = False

        @property
        def current_release(self):
            return self.src_doc.get("download", {}).get("release")

        @property
        def _current_release(self):
            return self.current_release

        @_current_release.setter
        def _current_release(self, value):
            if value is not None:
                self.src_doc["download"] = {
                    "release": value,
                    "data_folder": str(Path(self.src_root_folder) / value),
                    "status": "success",
                }

        @property
        def current_data_folder(self):
            return self.src_doc.get("download", {}).get("data_folder")

        @property
        def new_data_folder(self):
            if self.release is None:
                if self.steps == ["post"]:
                    return self.current_data_folder
                raise DumperException("release is not set")
            return str(Path(self.src_root_folder) / self.release)

        def register_status(self, status, *args, download=None, **kwargs):
            self.src_doc["download"] = {
                "status": status,
                "release": self.release or self.current_release,
                "data_folder": self.current_data_folder,
                **(download or {}),
            }

        def post_dump(self, *args, **kwargs):
            self.base_post_dump_called = True

    biothings = types.ModuleType("biothings")
    biothings.config = types.SimpleNamespace(DATA_ARCHIVE_ROOT=str(tmp_path))
    hub = types.ModuleType("biothings.hub")
    dataload = types.ModuleType("biothings.hub.dataload")
    sdk_dumper = types.ModuleType("biothings.hub.dataload.dumper")
    sdk_dumper.DumperException = DumperException
    sdk_dumper.HTTPDumper = HTTPDumper
    biothings.hub = hub
    hub.dataload = dataload
    dataload.dumper = sdk_dumper

    monkeypatch.setitem(sys.modules, "biothings", biothings)
    monkeypatch.setitem(sys.modules, "biothings.hub", hub)
    monkeypatch.setitem(sys.modules, "biothings.hub.dataload", dataload)
    monkeypatch.setitem(sys.modules, "biothings.hub.dataload.dumper", sdk_dumper)

    module_name = f"{package_name}.dumper"
    spec = importlib.util.spec_from_file_location(
        module_name, PLUGIN_PATH / "dumper.py"
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def release_responses(*, include_incomplete=False, report_status="pass"):
    shard_names = [
        "pubmed_metadata_00000.ndjson.gz",
        "pubmed_metadata_00001.ndjson.gz",
    ]
    release_hrefs = ["2026aug5/"]
    responses = {
        f"{ROOT_URL}2026aug5/": FakeResponse(
            text=index_html(*shard_names, "validation_report.json.gz")
        ),
        f"{ROOT_URL}2026aug5/validation_report.json.gz": FakeResponse(
            content=packed_report(status=report_status)
        ),
    }
    if include_incomplete:
        release_hrefs.insert(0, "2026aug6/")
        responses[f"{ROOT_URL}2026aug6/"] = FakeResponse(text=index_html(*shard_names))
    responses[ROOT_URL] = FakeResponse(text=index_html(*release_hrefs))
    return responses


def new_layout_responses(*, report_status="warn", month_status="warn"):
    responses = release_responses()
    new_shard_names = [
        "pubmed_metadata_0.ndjson.gz",
        "pubmed_metadata_1.ndjson.gz",
    ]
    report_filename = "validation_report-20260821.json"
    responses.update(
        {
            ROOT_URL: FakeResponse(
                text=index_html("manifests/", "2026aug21/", "2026aug5/")
            ),
            f"{ROOT_URL}manifests/": FakeResponse(
                text=index_html("pmids-20260821.txt.gz", report_filename)
            ),
            f"{ROOT_URL}2026aug21/": FakeResponse(text=index_html(*new_shard_names)),
            f"{ROOT_URL}manifests/{report_filename}": FakeResponse(
                content=plain_report(
                    status=report_status,
                    padded=False,
                    compressed=True,
                    month_status=month_status,
                )
            ),
        }
    )
    return responses


def nested_manifest_responses(*, report_status="warn", exact_report=True):
    responses = new_layout_responses()
    new_shard_names = [
        "pubmed_metadata_0.ndjson.gz",
        "pubmed_metadata_1.ndjson.gz",
    ]
    report_filename = "validation_report-20260926.json"
    nested_manifest_hrefs = [
        report_filename if exact_report else "validation_report-20260925.json"
    ]
    responses.update(
        {
            ROOT_URL: FakeResponse(
                text=index_html("manifests/", "2026sep26/", "2026aug21/", "2026aug5/")
            ),
            f"{ROOT_URL}2026sep26/": FakeResponse(
                text=index_html("manifests/", *new_shard_names)
            ),
            f"{ROOT_URL}2026sep26/manifests/": FakeResponse(
                text=index_html(*nested_manifest_hrefs)
            ),
        }
    )
    if exact_report:
        responses[f"{ROOT_URL}2026sep26/manifests/{report_filename}"] = FakeResponse(
            content=plain_report(
                status=report_status,
                padded=False,
                compressed=True,
                month_status="warn",
            )
        )
    return responses


def test_dumper_selects_newest_completed_release_and_queues_its_files(
    dumper_module,
):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses(include_incomplete=True))
    dumper._current_release = "2026jun30"

    dumper.create_todump_list()

    assert dumper.release == "2026aug5"
    assert [Path(item["local"]).name for item in dumper.to_dump] == [
        "pubmed_metadata_00000.ndjson.gz",
        "pubmed_metadata_00001.ndjson.gz",
        "validation_report.json.gz",
    ]
    assert {Path(item["local"]).parent.name for item in dumper.to_dump} == {"2026aug5"}
    assert all(
        item["remote"].startswith(f"{ROOT_URL}2026aug5/") for item in dumper.to_dump
    )
    assert all(timeout == 30 for _, timeout in dumper.client.calls)


def test_legacy_release_does_not_require_the_manifest_index(dumper_module):
    responses = release_responses()
    responses[ROOT_URL] = FakeResponse(text=index_html("manifests/", "2026aug5/"))
    responses[f"{ROOT_URL}2026aug5/"] = FakeResponse(
        text=index_html(
            "manifests/",
            "pubmed_metadata_00000.ndjson.gz",
            "pubmed_metadata_00001.ndjson.gz",
            "validation_report.json.gz",
        )
    )
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(responses)
    dumper._current_release = "2026jun30"

    dumper.create_todump_list()

    assert dumper.release == "2026aug5"
    requested_urls = [url for url, _ in dumper.client.calls]
    assert f"{ROOT_URL}manifests/" not in requested_urls


def test_dumper_selects_new_layout_release_and_queues_sibling_manifest(
    dumper_module,
):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(new_layout_responses())
    dumper._current_release = "2026aug5"

    dumper.create_todump_list()

    assert dumper.release == "2026aug21"
    assert dumper.to_dump == [
        {
            "remote": f"{ROOT_URL}2026aug21/pubmed_metadata_0.ndjson.gz",
            "local": str(Path(dumper.new_data_folder) / "pubmed_metadata_0.ndjson.gz"),
        },
        {
            "remote": f"{ROOT_URL}2026aug21/pubmed_metadata_1.ndjson.gz",
            "local": str(Path(dumper.new_data_folder) / "pubmed_metadata_1.ndjson.gz"),
        },
        {
            "remote": (f"{ROOT_URL}manifests/validation_report-20260821.json"),
            "local": str(
                Path(dumper.new_data_folder) / "validation_report-20260821.json"
            ),
        },
    ]


def test_dumper_selects_release_local_nested_manifest(dumper_module):
    responses = nested_manifest_responses()
    responses[f"{ROOT_URL}manifests/"] = FakeResponse(
        text=index_html(
            "validation_report-20260821.json",
            "validation_report-20260926.json",
        )
    )
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(responses)
    dumper._current_release = "2026aug21"

    dumper.create_todump_list()

    assert dumper.release == "2026sep26"
    assert dumper.to_dump == [
        {
            "remote": f"{ROOT_URL}2026sep26/pubmed_metadata_0.ndjson.gz",
            "local": str(Path(dumper.new_data_folder) / "pubmed_metadata_0.ndjson.gz"),
        },
        {
            "remote": f"{ROOT_URL}2026sep26/pubmed_metadata_1.ndjson.gz",
            "local": str(Path(dumper.new_data_folder) / "pubmed_metadata_1.ndjson.gz"),
        },
        {
            "remote": (
                f"{ROOT_URL}2026sep26/manifests/" "validation_report-20260926.json"
            ),
            "local": str(
                Path(dumper.new_data_folder) / "validation_report-20260926.json"
            ),
        },
    ]
    requested_urls = [url for url, _ in dumper.client.calls]
    assert f"{ROOT_URL}manifests/" not in requested_urls


def test_release_local_manifest_is_authoritative(dumper_module):
    responses = nested_manifest_responses(exact_report=False)
    report_filename = "validation_report-20260926.json"
    responses[f"{ROOT_URL}manifests/"] = FakeResponse(
        text=index_html("validation_report-20260821.json", report_filename)
    )
    responses[f"{ROOT_URL}manifests/{report_filename}"] = FakeResponse(
        content=plain_report(
            status="warn",
            padded=False,
            compressed=True,
            month_status="warn",
        )
    )
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(responses)
    dumper._current_release = "2026jun30"

    dumper.create_todump_list()

    assert dumper.release == "2026aug21"
    requested_urls = [url for url, _ in dumper.client.calls]
    assert f"{ROOT_URL}manifests/" in requested_urls
    assert f"{ROOT_URL}manifests/validation_report-20260926.json" not in requested_urls


def test_completed_invalid_nested_manifest_release_never_falls_back(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(nested_manifest_responses(report_status="fail"))

    with pytest.raises(dumper_module.DumperException, match="did not complete"):
        dumper.create_todump_list()

    requested_urls = [url for url, _ in dumper.client.calls]
    assert f"{ROOT_URL}2026aug21/" not in requested_urls


def test_completed_invalid_new_layout_release_never_falls_back(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(new_layout_responses(report_status="fail"))

    with pytest.raises(dumper_module.DumperException, match="did not complete"):
        dumper.create_todump_list()

    requested_urls = [url for url, _ in dumper.client.calls]
    assert f"{ROOT_URL}2026aug5/" not in requested_urls


def test_release_without_exact_manifest_falls_back_to_completed_release(
    dumper_module,
):
    responses = new_layout_responses()
    responses[f"{ROOT_URL}manifests/"] = FakeResponse(
        text=index_html("validation_report-20260820.json")
    )
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(responses)
    dumper._current_release = "2026jun30"

    dumper.create_todump_list()

    assert dumper.release == "2026aug5"
    requested_urls = [url for url, _ in dumper.client.calls]
    assert f"{ROOT_URL}manifests/validation_report-20260821.json" not in requested_urls


def test_dumper_does_not_queue_the_current_release(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses())
    dumper._current_release = "2026aug5"
    write_local_release(Path(dumper.current_data_folder))

    dumper.create_todump_list()

    assert dumper.release == "2026aug5"
    assert dumper.to_dump == []


def test_force_queues_the_current_release(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses())
    dumper._current_release = "2026aug5"

    dumper.create_todump_list(force=True)

    assert len(dumper.to_dump) == 3


def test_dumper_rejects_a_completed_release_with_a_failed_report(
    dumper_module,
):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses(report_status="fail"))

    with pytest.raises(dumper_module.DumperException, match="did not complete"):
        dumper.create_todump_list()


def test_post_dump_revalidates_the_downloaded_report(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.release = "2026aug5"
    data_folder = Path(dumper.new_data_folder)
    data_folder.mkdir(parents=True)
    for index in range(2):
        (data_folder / f"pubmed_metadata_{index:05d}.ndjson.gz").touch()
    (data_folder / "validation_report.json.gz").write_bytes(packed_report())

    dumper.post_dump()

    assert dumper.base_post_dump_called is True

    (data_folder / "validation_report.json.gz").write_bytes(b"corrupt")
    with pytest.raises(
        dumper_module.DumperException, match="Downloaded PubMed release"
    ):
        dumper.post_dump()


def test_post_dump_revalidates_new_layout_report(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.release = "2026aug21"
    dumper.release_validation_report_filename = "validation_report-20260821.json"
    data_folder = Path(dumper.new_data_folder)
    data_folder.mkdir(parents=True)
    for index in range(2):
        (data_folder / f"pubmed_metadata_{index}.ndjson.gz").touch()
    (data_folder / dumper.release_validation_report_filename).write_bytes(
        plain_report(
            status="warn",
            padded=False,
            compressed=True,
            month_status="warn",
        )
    )

    dumper.post_dump()

    assert dumper.base_post_dump_called is True


@pytest.mark.parametrize(
    "report_filename",
    [
        "validation_report.json.gz",
        "validation_report-20260821.json",
        "validation_report-20260821.json.gz",
    ],
)
def test_post_only_uses_current_folder_without_discovery(
    dumper_module, report_filename
):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper._current_release = "2026aug21"
    dumper.steps = ["post"]
    write_local_release(Path(dumper.current_data_folder), report_filename)

    dumper.post_dump()

    assert dumper.release is None
    assert dumper.base_post_dump_called


@pytest.mark.parametrize("status", ["canceled", "downloading", "failed", "success"])
def test_current_release_with_missing_files_is_downloaded_again(dumper_module, status):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses())
    dumper._current_release = "2026aug5"
    dumper.src_doc["download"]["status"] = status
    folder = Path(dumper.current_data_folder)
    write_local_release(folder)
    (folder / "pubmed_metadata_00001.ndjson.gz").unlink()

    dumper.create_todump_list()

    assert len(dumper.to_dump) == 3


@pytest.mark.parametrize("force", [False, True])
def test_discovery_never_downgrades_a_recorded_release(dumper_module, force):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses())
    dumper._current_release = "2026aug21"

    with pytest.raises(dumper_module.DumperException, match="Refusing to replace"):
        dumper.create_todump_list(force=force)

    assert not dumper.to_dump
    assert dumper.release is None
    assert dumper.current_release == "2026aug21"


def test_discovery_also_protects_the_last_successful_release(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses())
    dumper._current_release = "2026aug5"
    dumper.src_doc["download"]["last_successful_release"] = "2026aug21"
    with pytest.raises(dumper_module.DumperException, match="Refusing to replace"):
        dumper.create_todump_list(force=True)
    assert not dumper.to_dump


@pytest.mark.parametrize(
    "failure",
    [requests.Timeout("timeout"), requests.ConnectionError("reset"), 429, 500, 503],
)
def test_metadata_requests_retry_transient_errors(dumper_module, monkeypatch, failure):
    first = FakeResponse(status_code=failure) if isinstance(failure, int) else failure
    success = FakeResponse(text="ok")
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient({ROOT_URL: [first, success]})
    sleeps = []
    monkeypatch.setattr(dumper_module.time, "sleep", sleeps.append)

    assert dumper._get(ROOT_URL) is success
    assert len(dumper.client.calls) == 2
    assert sleeps == [2]
    assert success.closed
    if isinstance(first, FakeResponse):
        assert first.closed


def test_metadata_retry_is_bounded_and_does_not_retry_404(dumper_module, monkeypatch):
    dumper = dumper_module.PubMedMetadataDumper()
    sleeps = []
    monkeypatch.setattr(dumper_module.time, "sleep", sleeps.append)
    dumper.client = FakeClient({ROOT_URL: [requests.Timeout("timeout")] * 4})
    with pytest.raises(dumper_module.DumperException, match="after 4 attempt"):
        dumper._get(ROOT_URL)
    assert len(dumper.client.calls) == 4
    assert sleeps == [2, 4, 8]

    dumper.client = FakeClient({ROOT_URL: FakeResponse(status_code=404)})
    with pytest.raises(dumper_module.DumperException, match="after 1 attempt"):
        dumper._get(ROOT_URL)
    assert len(dumper.client.calls) == 1


@pytest.mark.parametrize("location", ["release", "nested", "global"])
def test_gzip_dated_reports_are_discovered_at_each_location(dumper_module, location):
    responses = new_layout_responses()
    name = "validation_report-20260821.json.gz"
    shard_names = [f"pubmed_metadata_{i}.ndjson.gz" for i in range(2)]
    base = f"{ROOT_URL}2026aug21/"
    if location == "release":
        responses[base] = FakeResponse(text=index_html(*shard_names, name))
    elif location == "nested":
        responses[base] = FakeResponse(text=index_html(*shard_names, "manifests/"))
        base += "manifests/"
        responses[base] = FakeResponse(text=index_html(name))
    else:
        base = ROOT_URL + "manifests/"
        responses[base] = FakeResponse(text=index_html(name))
    responses[base + name] = FakeResponse(
        content=packed_report(padded=False, compressed=True)
    )
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(responses)

    dumper.create_todump_list()

    assert dumper.release == "2026aug21"
    assert dumper.to_dump[-1]["remote"] == base + name


def test_contiguous_shards_without_report_raise_a_visible_warning(
    dumper_module, caplog
):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses(include_incomplete=True))
    with caplog.at_level(logging.WARNING):
        dumper.create_todump_list()
    warnings = [
        r for r in caplog.records if "no recognized completion report" in r.message
    ]
    assert len(warnings) == 1
    assert warnings[0].notify is True


def test_archive_retention_keeps_rollback_and_ignores_unrelated_paths(
    dumper_module, tmp_path
):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.release = "2026sep26"
    root = Path(dumper.src_root_folder)
    for name in ["2026jun30", "2026aug5", "2026aug21", "2026sep26", "2026oct1"]:
        write_local_release(root / name)
    (root / "2026aug20").mkdir()  # An incomplete download is not a disposable archive.
    (root / "notes").mkdir()
    external = tmp_path / "external"
    external.mkdir()
    (root / "2026jul1").symlink_to(external, target_is_directory=True)

    dumper.post_dump()

    assert not (root / "2026jun30").exists()
    assert not (root / "2026aug5").exists()
    for name in [
        "2026aug21",
        "2026sep26",
        "2026oct1",
        "2026aug20",
        "notes",
        "2026jul1",
    ]:
        assert (root / name).exists()
    assert external.exists()


def test_archive_retention_can_be_disabled_and_failed_validation_never_prunes(
    dumper_module,
):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.release = "2026aug21"
    old = Path(dumper.src_root_folder) / "2026jun30"
    write_local_release(old)
    write_local_release(Path(dumper.new_data_folder))
    dumper.KEEP_ARCHIVED_RELEASES = None
    dumper.post_dump()
    assert old.exists()
    dumper.KEEP_ARCHIVED_RELEASES = 2
    (Path(dumper.new_data_folder) / "validation_report-20260821.json").write_bytes(
        b"bad"
    )
    with pytest.raises(dumper_module.DumperException):
        dumper.post_dump()
    assert old.exists()


def test_archive_retention_prioritizes_last_successful_snapshot(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.release = "2026sep26"
    dumper.src_doc["download"]["last_successful_release"] = "2026aug5"
    root = Path(dumper.src_root_folder)
    for name in ("2026aug5", "2026aug21", "2026sep26"):
        write_local_release(root / name)
    dumper.post_dump()
    assert (root / "2026aug5").exists()
    assert not (root / "2026aug21").exists()


def test_archive_cleanup_failure_does_not_fail_valid_post_dump(
    dumper_module, monkeypatch, caplog
):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.release = "2026sep26"
    root = Path(dumper.src_root_folder)
    for name in ("2026jun30", "2026aug5", "2026sep26"):
        write_local_release(root / name)

    def fail_cleanup(path):
        raise OSError("read-only archive")

    monkeypatch.setattr(dumper_module.shutil, "rmtree", fail_cleanup)
    with caplog.at_level(logging.WARNING):
        dumper.post_dump()
    assert dumper.base_post_dump_called
    assert any("Unable to remove" in r.message and r.notify for r in caplog.records)


@pytest.fixture
def sdk_flow(dumper_module, monkeypatch):
    """Exercise the pinned SDK's actual lifecycle methods without a live Hub DB."""
    try:
        sdk = distribution("biothings")
    except PackageNotFoundError:
        pytest.skip("BioThings is not installed")
    if sdk.version != "1.0.2":
        pytest.skip("Lifecycle regression targets the pinned BioThings 1.0.2")
    path = sdk.locate_file("biothings/hub/dataload/dumper.py")
    tree = ast.parse(path.read_text())
    base = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "BaseDumper"
    )
    names = {
        "dump",
        "register_status",
        "new_data_folder",
        "current_data_folder",
        "current_release",
    }
    methods = [
        n
        for n in base.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names
    ]
    manager_class = next(
        n
        for n in tree.body
        if isinstance(n, ast.ClassDef) and n.name == "DumperManager"
    )
    methods.append(
        next(
            n
            for n in manager_class.body
            if isinstance(n, ast.FunctionDef) and n.name == "clean_stale_status"
        )
    )
    pending = []
    namespace = {
        "inspect": inspect,
        "partial": partial,
        "os": os,
        "datetime": datetime,
        "deepcopy": deepcopy,
        "timesofar": lambda _: 0,
        "DumperException": dumper_module.DumperException,
        "set_pending_to_upload": pending.append,
        "logging": logging,
    }
    exec(
        compile(ast.Module(body=methods, type_ignores=[]), str(path), "exec"), namespace
    )
    fake_base = dumper_module.PubMedMetadataDumper.__mro__[1]
    for name in names:
        monkeypatch.setattr(fake_base, name, namespace[name], raising=False)
    monkeypatch.setattr(fake_base, "get_pinfo", lambda self: {}, raising=False)
    monkeypatch.setattr(fake_base, "release_client", lambda self: None, raising=False)
    monkeypatch.setattr(fake_base, "unprepare", lambda self: {}, raising=False)
    monkeypatch.setattr(fake_base, "prepare", lambda self, state: None, raising=False)

    def clean_stale_status(self):
        store = types.SimpleNamespace(
            find=lambda: [deepcopy(self.src_doc)],
            replace_one=lambda query, doc: setattr(self, "src_doc", deepcopy(doc)),
        )
        namespace["get_src_dump"] = lambda: store
        namespace["clean_stale_status"](None)

    monkeypatch.setattr(
        fake_base, "clean_stale_status", clean_stale_status, raising=False
    )

    async def do_dump(self, **kwargs):
        self.downloaded = list(self.to_dump)
        write_local_release(Path(self.new_data_folder))

    monkeypatch.setattr(fake_base, "do_dump", do_dump, raising=False)

    class JobManager:
        async def defer_to_thread(self, pinfo, callback):
            future = asyncio.get_running_loop().create_future()
            try:
                future.set_result(callback())
            except Exception as exc:
                future.set_exception(exc)
            return future

    return dumper_module.PubMedMetadataDumper, JobManager(), pending


def test_sdk_post_only_marks_modern_snapshot_successful(sdk_flow):
    cls, manager, pending = sdk_flow
    dumper = cls()
    dumper._current_release = "2026sep26"
    dumper.src_doc["download"]["status"] = "downloading"
    write_local_release(Path(dumper.current_data_folder))

    asyncio.run(dumper.dump(steps="post", job_manager=manager))

    assert dumper.src_doc["download"]["status"] == "success"
    assert dumper.current_release == "2026sep26"
    assert pending == ["pubmed_metadata"]


def test_sdk_discovery_failure_recovers_without_redownload_or_upload(sdk_flow):
    cls, manager, pending = sdk_flow
    first = cls()
    first._current_release = "2026aug5"
    write_local_release(Path(first.current_data_folder))
    first.client = FakeClient({ROOT_URL: FakeResponse(status_code=404)})
    with pytest.raises(Exception, match="Unable to read"):
        asyncio.run(first.dump(job_manager=manager))
    assert first.src_doc["download"]["status"] == "failed"

    retry = cls()
    retry.src_doc = deepcopy(first.src_doc)
    retry.client = FakeClient(release_responses())
    result = asyncio.run(retry.dump(job_manager=manager))

    assert result == "Nothing to dump"
    assert retry.src_doc["download"]["status"] == "success"
    assert not hasattr(retry, "downloaded")
    assert not pending


def test_sdk_check_only_does_not_restore_status_or_start_transfer(sdk_flow):
    cls, manager, pending = sdk_flow
    dumper = cls()
    dumper._current_release = "2026aug5"
    write_local_release(Path(dumper.current_data_folder))
    dumper.src_doc["download"].update(
        {
            "status": "failed",
            "last_successful_release": "2026aug5",
            "retry_download": False,
        }
    )
    original = deepcopy(dumper.src_doc)
    dumper.client = FakeClient(release_responses())
    result = asyncio.run(dumper.dump(check_only=True, job_manager=manager))
    assert result == "Nothing to dump"
    assert dumper.src_doc == original
    assert not hasattr(dumper, "downloaded")
    assert not pending


@pytest.mark.parametrize("complete", [False, True])
def test_sdk_canceled_transfer_is_retried_even_at_the_same_release(sdk_flow, complete):
    cls, manager, pending = sdk_flow
    dumper = cls()
    dumper._current_release = "2026aug5"
    write_local_release(Path(dumper.current_data_folder))
    dumper.release = "2026aug5"
    dumper.register_status("downloading", transient=True)
    dumper.clean_stale_status()
    assert dumper.src_doc["download"]["status"] == "canceled"
    if not complete:
        (Path(dumper.current_data_folder) / "validation_report.json.gz").unlink()
    dumper.client = FakeClient(release_responses())

    asyncio.run(dumper.dump(job_manager=manager))

    assert len(dumper.downloaded) == 3
    assert dumper.src_doc["download"]["status"] == "success"
    assert pending == ["pubmed_metadata"]


def test_sdk_automatic_and_explicit_force_cannot_downgrade(sdk_flow):
    cls, manager, pending = sdk_flow
    for force in (False, True):
        dumper = cls()
        dumper._current_release = "2026aug21"
        dumper.src_doc["download"].update(
            {"status": "failed", "last_successful_release": "2026aug21"}
        )
        dumper.client = FakeClient(release_responses())
        with pytest.raises(Exception, match="Refusing to replace"):
            asyncio.run(dumper.dump(force=force, job_manager=manager))
        assert dumper.current_release == "2026aug21"
        assert not hasattr(dumper, "downloaded")
    assert not pending


def test_sdk_split_dump_and_post_works_on_fresh_instances(sdk_flow):
    cls, manager, pending = sdk_flow
    download = cls()
    download.client = FakeClient(nested_manifest_responses())
    asyncio.run(download.dump(steps="dump", job_manager=manager))
    assert download.src_doc["download"]["status"] == "downloading"
    post = cls()
    post.src_doc = deepcopy(download.src_doc)

    asyncio.run(post.dump(steps="post", job_manager=manager))

    assert post.src_doc["download"]["status"] == "success"
    assert post.current_release == "2026sep26"
    assert pending == ["pubmed_metadata"]


def test_sdk_explicit_force_still_redownloads_current_valid_release(sdk_flow):
    cls, manager, pending = sdk_flow
    dumper = cls()
    dumper._current_release = "2026aug5"
    write_local_release(Path(dumper.current_data_folder))
    dumper.client = FakeClient(release_responses())

    asyncio.run(dumper.dump(force=True, job_manager=manager))

    assert len(dumper.downloaded) == 3
    assert pending == ["pubmed_metadata"]
