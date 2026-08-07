import gzip
import importlib.util
import json
import logging
import sys
import types
from pathlib import Path

import pytest

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
    def __init__(self, *, text="", content=b""):
        self.text = text
        self.content = content

    def raise_for_status(self):
        return None


class FakeClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        return self.responses[url]


def index_html(*hrefs):
    return (
        "<html><body>"
        + "".join(f'<a href="{href}">{href}</a>' for href in hrefs)
        + "</body></html>"
    )


def validation_report(*, shard_count=2, status="pass"):
    return {
        "status": status,
        "errors": [],
        "inputs": {
            "shards": [
                f"pubmed_metadata_{index:05d}.ndjson" for index in range(shard_count)
            ]
        },
        "checks_run": [
            {
                "name": name,
                "section": "structure",
                "status": "pass",
            }
            for name in STRUCTURE_CHECK_NAMES
        ],
        "checks": {"structure": {"records_total": 10}},
    }


def packed_report(**kwargs):
    return gzip.compress(json.dumps(validation_report(**kwargs)).encode("utf-8"))


@pytest.fixture
def dumper_module(monkeypatch, tmp_path):
    package_name = "_test_pubmed_metadata_plugin"
    package = types.ModuleType(package_name)
    package.__path__ = [str(PLUGIN_PATH)]
    monkeypatch.setitem(sys.modules, package_name, package)

    class DumperException(Exception):
        pass

    class HTTPDumper:
        def __init__(self):
            self.client = None
            self.logger = logging.getLogger("test_pubmed_metadata_dumper")
            self.release = None
            self.to_dump = []
            self._current_release = None
            self.src_root_folder = self.SRC_ROOT_FOLDER
            self.base_post_dump_called = False

        @property
        def current_release(self):
            return self._current_release

        @property
        def new_data_folder(self):
            return str(Path(self.src_root_folder) / self.release)

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


def test_dumper_does_not_queue_the_current_release(dumper_module):
    dumper = dumper_module.PubMedMetadataDumper()
    dumper.client = FakeClient(release_responses())
    dumper._current_release = "2026aug5"

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
