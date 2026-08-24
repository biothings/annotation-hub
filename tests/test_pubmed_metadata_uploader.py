import gzip
import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


PLUGIN_PATH = Path(__file__).parents[1] / "plugins" / "pubmed_metadata"
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


def validation_report(*, padded: bool) -> dict:
    def shard_name(index: int) -> str:
        shard_index = f"{index:05d}" if padded else str(index)
        suffix = ".ndjson" if padded else ".ndjson.gz"
        return f"pubmed_metadata_{shard_index}{suffix}"

    return {
        "status": "pass",
        "errors": [],
        "inputs": {"shards": [shard_name(index) for index in range(2)]},
        "checks_run": [
            {"name": name, "section": "structure", "status": "pass"}
            for name in STRUCTURE_CHECK_NAMES
        ],
        "checks": {"structure": {"records_total": 2}},
    }


def write_release(folder: Path, *, padded: bool) -> tuple[Path, Path]:
    folder.mkdir()
    shard_paths = []
    for index in range(2):
        shard_index = f"{index:05d}" if padded else str(index)
        shard_path = folder / f"pubmed_metadata_{shard_index}.ndjson.gz"
        shard_path.touch()
        shard_paths.append(shard_path)
    return tuple(shard_paths)


@pytest.fixture
def uploader_module(monkeypatch):
    package_name = "_test_pubmed_metadata_uploader_plugin"
    package = types.ModuleType(package_name)
    package.__path__ = [str(PLUGIN_PATH)]
    monkeypatch.setitem(sys.modules, package_name, package)

    class ParallelizedSourceUploader:
        pass

    biothings = types.ModuleType("biothings")
    hub = types.ModuleType("biothings.hub")
    dataload = types.ModuleType("biothings.hub.dataload")
    sdk_uploader = types.ModuleType("biothings.hub.dataload.uploader")
    sdk_uploader.ParallelizedSourceUploader = ParallelizedSourceUploader
    biothings.hub = hub
    hub.dataload = dataload
    dataload.uploader = sdk_uploader

    monkeypatch.setitem(sys.modules, "biothings", biothings)
    monkeypatch.setitem(sys.modules, "biothings.hub", hub)
    monkeypatch.setitem(sys.modules, "biothings.hub.dataload", dataload)
    monkeypatch.setitem(sys.modules, "biothings.hub.dataload.uploader", sdk_uploader)

    module_name = f"{package_name}.uploader"
    spec = importlib.util.spec_from_file_location(
        module_name, PLUGIN_PATH / "uploader.py"
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_jobs_uses_legacy_gzip_report_before_dated_report(tmp_path, uploader_module):
    data_folder = tmp_path / "2026aug5"
    shard_paths = write_release(data_folder, padded=True)
    legacy_report = json.dumps(validation_report(padded=True)).encode("utf-8")
    (data_folder / "validation_report.json.gz").write_bytes(gzip.compress(legacy_report))
    (data_folder / "validation_report-20260805.json").write_bytes(b"not JSON")

    uploader = uploader_module.PubMedMetadataUploader()
    uploader.data_folder = str(data_folder)

    assert uploader.jobs() == [(str(path),) for path in shard_paths]


def test_jobs_uses_dated_plain_json_report_for_new_layout(tmp_path, uploader_module):
    data_folder = tmp_path / "2026aug21"
    shard_paths = write_release(data_folder, padded=False)
    report = json.dumps(validation_report(padded=False)).encode("utf-8")
    (data_folder / "validation_report-20260821.json").write_bytes(report)

    uploader = uploader_module.PubMedMetadataUploader()
    uploader.data_folder = str(data_folder)

    assert uploader.jobs() == [(str(path),) for path in shard_paths]


@pytest.mark.parametrize(
    "report_filename",
    (None, "validation_report-20260820.json"),
)
def test_jobs_rejects_missing_or_wrong_date_dated_report(
    tmp_path, uploader_module, report_filename
):
    data_folder = tmp_path / "2026aug21"
    write_release(data_folder, padded=False)
    if report_filename is not None:
        report = json.dumps(validation_report(padded=False)).encode("utf-8")
        (data_folder / report_filename).write_bytes(report)

    uploader = uploader_module.PubMedMetadataUploader()
    uploader.data_folder = str(data_folder)

    with pytest.raises(FileNotFoundError, match="complete validated release"):
        uploader.jobs()


def test_jobs_rejects_new_layout_in_an_invalid_release_folder(tmp_path, uploader_module):
    data_folder = tmp_path / "not-a-release"
    write_release(data_folder, padded=False)
    report = json.dumps(validation_report(padded=False)).encode("utf-8")
    (data_folder / "validation_report-20260821.json").write_bytes(report)

    uploader = uploader_module.PubMedMetadataUploader()
    uploader.data_folder = str(data_folder)

    with pytest.raises(FileNotFoundError, match="complete validated release"):
        uploader.jobs()
