import importlib.util
import logging
import sys
import types
from pathlib import Path

import pytest


class FakeResponse:
    def __init__(self, text="", status_code=200, reason="OK"):
        self.text = text
        self.status_code = status_code
        self.reason = reason
        self.closed = False

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.get_calls = []

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture
def nodenorm_modules(monkeypatch, tmp_path):
    config = types.SimpleNamespace(
        DATA_ARCHIVE_ROOT=str(tmp_path),
        logger=logging.getLogger("test_nodenorm_release"),
    )
    biothings_module = types.ModuleType("biothings")
    biothings_module.config = config

    dumper_dependency = types.ModuleType("biothings.hub.dataload.dumper")

    class DummyDumperException(Exception):
        pass

    class DummyLastModifiedHTTPDumper:
        def post_dump(self, *args, **kwargs):
            del args, kwargs
            self.base_post_dump_called = True

    dumper_dependency.DumperException = DummyDumperException
    dumper_dependency.LastModifiedHTTPDumper = DummyLastModifiedHTTPDumper

    manager_dependency = types.ModuleType("biothings.utils.manager")
    manager_dependency.JobManager = type("DummyJobManager", (), {})

    monkeypatch.setitem(sys.modules, "biothings", biothings_module)
    monkeypatch.setitem(sys.modules, "biothings.hub", types.ModuleType("biothings.hub"))
    monkeypatch.setitem(
        sys.modules,
        "biothings.hub.dataload",
        types.ModuleType("biothings.hub.dataload"),
    )
    monkeypatch.setitem(sys.modules, "biothings.hub.dataload.dumper", dumper_dependency)
    monkeypatch.setitem(
        sys.modules, "biothings.utils", types.ModuleType("biothings.utils")
    )
    monkeypatch.setitem(sys.modules, "biothings.utils.manager", manager_dependency)

    package_name = f"_test_nodenorm_release_{id(tmp_path)}"
    module_dir = Path(__file__).parents[1] / "plugins" / "nodenorm"
    package = types.ModuleType(package_name)
    package.__path__ = [str(module_dir)]
    monkeypatch.setitem(sys.modules, package_name, package)

    loaded_modules = {}
    for module_name in ("static", "release", "dumper"):
        spec = importlib.util.spec_from_file_location(
            f"{package_name}.{module_name}", module_dir / f"{module_name}.py"
        )
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, f"{package_name}.{module_name}", module)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        loaded_modules[module_name] = module

    return types.SimpleNamespace(**loaded_modules)


def make_dumper(nodenorm_modules, tmp_path, marker, current_release=None):
    dumper = object.__new__(nodenorm_modules.dumper.NodeNormDumper)
    dumper.to_dump = []
    dumper.to_dump_large = []
    dumper.current_release = current_release
    dumper.new_data_folder = str(tmp_path / "nodenorm" / "latest")
    dumper.logger = logging.getLogger("test_nodenorm_release.instance")
    dumper.client = FakeClient([marker])
    return dumper


def test_parse_real_version_marker(nodenorm_modules):
    marker = (
        "Babel 2025sep1\n"
        "https://github.com/TranslatorSRI/Babel/blob/master/releases/2025sep1.md\n"
    )

    assert nodenorm_modules.release.parse_version_marker(marker) == "2025sep1"


@pytest.mark.parametrize(
    "marker",
    [
        "",
        "2025sep1",
        "Babel 2025feb30",
        "Babel ../../2025sep1",
        "Babel https://example.test/2025sep1",
        "Babel 2025Sep1",
    ],
)
def test_invalid_version_markers_are_rejected(nodenorm_modules, marker):
    with pytest.raises(nodenorm_modules.release.NodeNormReleaseError):
        nodenorm_modules.release.parse_version_marker(marker)


def test_release_marker_request_errors_are_wrapped(nodenorm_modules, tmp_path):
    request_error = nodenorm_modules.dumper.requests_exceptions.Timeout("timed out")
    dumper = make_dumper(nodenorm_modules, tmp_path, request_error)

    with pytest.raises(nodenorm_modules.dumper.DumperException, match="timed out"):
        dumper.get_release()


def test_release_marker_http_errors_are_wrapped_and_closed(nodenorm_modules, tmp_path):
    response = FakeResponse(status_code=503, reason="Service Unavailable")
    dumper = make_dumper(nodenorm_modules, tmp_path, response)

    with pytest.raises(nodenorm_modules.dumper.DumperException, match="status: 503"):
        dumper.get_release()

    assert response.closed is True


def test_new_release_queues_immutable_urls_in_new_folder(nodenorm_modules, tmp_path):
    response = FakeResponse("Babel 2025sep1\n")
    dumper = make_dumper(
        nodenorm_modules, tmp_path, response, current_release="2025mar31"
    )

    dumper.create_todump_list()

    assert dumper.release == "2025sep1"
    assert len(dumper.to_dump) == len(dumper.FILE_COLLECTION) + len(
        dumper.CONFLATION_COLLECTION
    )
    assert len(dumper.to_dump_large) == len(dumper.BIG_FILE_COLLECTION)
    assert all(
        item["remote"].startswith("https://stars.renci.org/var/babel_outputs/2025sep1/")
        for item in dumper.to_dump
    )
    assert all(
        item["remoteurl"].startswith(
            "https://stars.renci.org/var/babel_outputs/2025sep1/"
        )
        for item in dumper.to_dump_large
    )
    expected_folder = tmp_path / "nodenorm" / "latest"
    assert all(Path(item["local"]).parent == expected_folder for item in dumper.to_dump)
    assert all(
        Path(item["localfile"]).parent == expected_folder
        for item in dumper.to_dump_large
    )
    assert dumper.client.get_calls == [
        (dumper.VERSION_URL, {"timeout": dumper.VERSION_REQUEST_TIMEOUT})
    ]
    assert response.closed is True


def test_current_official_release_is_not_queued(nodenorm_modules, tmp_path):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        current_release="2025sep1",
    )

    dumper.create_todump_list()

    assert dumper.to_dump == []
    assert dumper.to_dump_large == []


def test_changed_official_marker_is_followed_even_for_a_rollback(
    nodenorm_modules, tmp_path
):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        current_release="2026jul22",
    )

    dumper.create_todump_list()

    assert dumper.to_dump
    assert dumper.to_dump_large


@pytest.mark.parametrize("current_release", [None, "", "not-a-release"])
def test_missing_or_invalid_current_release_is_repaired(
    nodenorm_modules, tmp_path, current_release
):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        current_release=current_release,
    )

    dumper.create_todump_list()

    assert dumper.to_dump
    assert dumper.to_dump_large


def test_force_queues_current_release_and_resets_queues(nodenorm_modules, tmp_path):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        current_release="2025sep1",
    )
    dumper.to_dump = [{"remote": "stale"}]
    dumper.to_dump_large = [{"remoteurl": "stale"}]

    dumper.create_todump_list(force=True)

    assert len(dumper.to_dump) == len(dumper.FILE_COLLECTION) + len(
        dumper.CONFLATION_COLLECTION
    )
    assert len(dumper.to_dump_large) == len(dumper.BIG_FILE_COLLECTION)
    assert all(item.get("remote") != "stale" for item in dumper.to_dump)
    assert all(item.get("remoteurl") != "stale" for item in dumper.to_dump_large)


def test_post_dump_uses_selected_release_without_refetching(nodenorm_modules, tmp_path):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel unexpected\n"),
        current_release="2025mar31",
    )
    dumper.release = "2025sep1"
    generated_for = []
    dumper._generate_conflation_database = generated_for.append

    dumper.post_dump()

    assert generated_for == [tmp_path / "nodenorm" / "latest"]
    assert dumper.release == "2025sep1"
    assert dumper.client.get_calls == []
    assert dumper.base_post_dump_called is True


def test_production_retains_single_snapshot(nodenorm_modules):
    assert nodenorm_modules.dumper.NodeNormDumper.ARCHIVE is False
