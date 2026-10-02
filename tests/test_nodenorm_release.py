import asyncio
import importlib.util
import logging
import sys
import types
from pathlib import Path

import pytest


def index_html(*hrefs):
    links = "".join(f'<a href="{href}">{href}</a>' for href in hrefs)
    return f"<html><body>{links}</body></html>"


COMPENDIA = ("CellLine.txt", "Food.txt", "FutureType.txt", "Protein.txt")
CONFLATIONS = ("DrugChemical.txt", "GeneProtein.txt")
COMPENDIA_INDEX = index_html(
    "../",
    "CellLine.txt",
    "Food.txt",
    "FutureType.txt",
    "Protein.txt",
    "Protein.txt.00",
    "notes.txt.gz",
)
CONFLATION_INDEX = index_html(*CONFLATIONS)


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
        self.served_responses = []

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        self.served_responses.append(response)
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


def make_dumper(
    nodenorm_modules,
    tmp_path,
    marker,
    current_release=None,
    compendia_index=None,
    conflation_index=None,
):
    dumper = object.__new__(nodenorm_modules.dumper.NodeNormDumper)
    dumper.to_dump = []
    dumper.to_dump_large = []
    dumper.current_release = current_release
    dumper.new_data_folder = str(tmp_path / "nodenorm" / "latest")
    dumper.current_data_folder = dumper.new_data_folder
    dumper.logger = logging.getLogger("test_nodenorm_release.instance")
    dumper.client = FakeClient(
        [
            marker,
            compendia_index or FakeResponse(COMPENDIA_INDEX),
            conflation_index or FakeResponse(CONFLATION_INDEX),
        ]
    )
    return dumper


def write_release_files(nodenorm_modules, data_folder, release="2025sep1"):
    data_folder.mkdir(parents=True, exist_ok=True)
    for filename in (*COMPENDIA, *CONFLATIONS):
        (data_folder / filename).write_text("{}\n", encoding="utf-8")
    manifest = nodenorm_modules.release.make_release_manifest(
        release, COMPENDIA, CONFLATIONS
    )
    nodenorm_modules.release.write_release_manifest(data_folder, manifest)
    return manifest


def test_parse_real_version_marker(nodenorm_modules):
    marker = (
        "Babel 2025sep1\n"
        "https://github.com/TranslatorSRI/Babel/blob/master/releases/2025sep1.md\n"
    )

    assert nodenorm_modules.release.parse_version_marker(marker) == "2025sep1"


def test_artifact_index_keeps_only_canonical_direct_text_files(nodenorm_modules):
    listing = index_html(
        "Food.txt",
        "CellLine.txt",
        "Protein.txt",
        "Protein.txt.00",
        "notes.txt.gz",
        "../escape.txt",
        "/absolute.txt",
        "nested/Other.txt",
        "https://example.test/External.txt",
        "Encoded%2FTraversal.txt",
    )

    assert nodenorm_modules.release.artifact_filenames_from_index(listing) == (
        "CellLine.txt",
        "Food.txt",
        "Protein.txt",
    )


def test_artifact_index_rejects_duplicate_canonical_names(nodenorm_modules):
    with pytest.raises(
        nodenorm_modules.release.NodeNormReleaseError, match="repeats filename"
    ):
        nodenorm_modules.release.artifact_filenames_from_index(
            index_html("Food.txt", "Food.txt")
        )


def test_artifact_index_rejects_empty_inventory(nodenorm_modules):
    with pytest.raises(
        nodenorm_modules.release.NodeNormReleaseError, match="no canonical"
    ):
        nodenorm_modules.release.artifact_filenames_from_index(
            index_html("../", "Protein.txt.00", "notes.json")
        )


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
    normal_remotes = {item["remote"] for item in dumper.to_dump}
    assert normal_remotes == {
        "https://stars.renci.org/var/babel_outputs/2025sep1/compendia/CellLine.txt",
        "https://stars.renci.org/var/babel_outputs/2025sep1/compendia/Food.txt",
        "https://stars.renci.org/var/babel_outputs/2025sep1/compendia/FutureType.txt",
        "https://stars.renci.org/var/babel_outputs/2025sep1/conflation/DrugChemical.txt",
        "https://stars.renci.org/var/babel_outputs/2025sep1/conflation/GeneProtein.txt",
    }
    assert [item["remoteurl"] for item in dumper.to_dump_large] == [
        "https://stars.renci.org/var/babel_outputs/2025sep1/compendia/Protein.txt"
    ]
    assert dumper.release_manifest.compendia == COMPENDIA
    assert dumper.release_manifest.conflations == CONFLATIONS
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
        (dumper.VERSION_URL, {"timeout": dumper.VERSION_REQUEST_TIMEOUT}),
        (
            "https://stars.renci.org/var/babel_outputs/2025sep1/compendia/",
            {"timeout": dumper.ARTIFACT_INDEX_REQUEST_TIMEOUT},
        ),
        (
            "https://stars.renci.org/var/babel_outputs/2025sep1/conflation/",
            {"timeout": dumper.ARTIFACT_INDEX_REQUEST_TIMEOUT},
        ),
    ]
    assert response.closed is True
    assert all(
        remote_response.closed is True
        for remote_response in dumper.client.served_responses
        if isinstance(remote_response, FakeResponse)
    )


def test_current_official_release_is_not_queued(nodenorm_modules, tmp_path):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        current_release="2025sep1",
    )

    write_release_files(
        nodenorm_modules, Path(dumper.current_data_folder), release="2025sep1"
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

    assert len(dumper.to_dump) == 5
    assert len(dumper.to_dump_large) == 1
    assert all(item.get("remote") != "stale" for item in dumper.to_dump)
    assert all(item.get("remoteurl") != "stale" for item in dumper.to_dump_large)


def test_same_release_without_manifest_is_rebuilt(nodenorm_modules, tmp_path):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        current_release="2025sep1",
    )

    dumper.create_todump_list()

    assert dumper.to_dump
    assert dumper.to_dump_large


def test_compendia_inventory_http_error_fails_closed(nodenorm_modules, tmp_path):
    response = FakeResponse(status_code=503, reason="Service Unavailable")
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        compendia_index=response,
    )

    with pytest.raises(
        nodenorm_modules.dumper.DumperException, match="compendia.*status: 503"
    ):
        dumper.create_todump_list()

    assert dumper.to_dump == []
    assert dumper.to_dump_large == []
    assert response.closed is True


def test_empty_compendia_inventory_fails_closed(nodenorm_modules, tmp_path):
    response = FakeResponse(index_html("../", "Protein.txt.00"))
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        compendia_index=response,
    )

    with pytest.raises(nodenorm_modules.dumper.DumperException, match="no canonical"):
        dumper.create_todump_list()

    assert dumper.to_dump == []
    assert dumper.to_dump_large == []
    assert response.closed is True


def test_do_dump_persists_and_validates_release_manifest(nodenorm_modules, tmp_path):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
    )
    data_folder = Path(dumper.new_data_folder)
    data_folder.mkdir(parents=True)
    for filename in (*COMPENDIA, *CONFLATIONS):
        (data_folder / filename).write_text("{}\n", encoding="utf-8")
    dumper.release_manifest = nodenorm_modules.release.make_release_manifest(
        "2025sep1", COMPENDIA, CONFLATIONS
    )

    async def no_downloads(_job_manager):
        return None

    dumper._handle_normal_size_files = no_downloads
    dumper._handle_large_size_files = no_downloads

    asyncio.run(dumper.do_dump())

    assert (
        nodenorm_modules.release.read_release_manifest(data_folder)
        == dumper.release_manifest
    )


def test_post_dump_uses_selected_release_without_refetching(nodenorm_modules, tmp_path):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel unexpected\n"),
        current_release="2025mar31",
    )
    dumper.release = "2025sep1"
    write_release_files(
        nodenorm_modules, Path(dumper.new_data_folder), release=dumper.release
    )
    generated_for = []
    dumper._generate_conflation_database = generated_for.append

    dumper.post_dump()

    assert generated_for == [tmp_path / "nodenorm" / "latest"]
    assert dumper.release == "2025sep1"
    assert dumper.client.get_calls == []
    assert dumper.base_post_dump_called is True


def test_post_dump_rejects_manifest_from_an_incomplete_prior_release(
    nodenorm_modules, tmp_path
):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel unexpected\n"),
        current_release="2025sep1",
    )
    dumper.release = "2026jul22"
    write_release_files(
        nodenorm_modules, Path(dumper.new_data_folder), release="2025sep1"
    )

    with pytest.raises(
        nodenorm_modules.dumper.DumperException,
        match="manifest release '2025sep1'.*selected release '2026jul22'",
    ):
        dumper.post_dump()


def test_post_only_run_uses_current_release_manifest(nodenorm_modules, tmp_path):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel unexpected\n"),
        current_release="2025sep1",
    )
    write_release_files(
        nodenorm_modules, Path(dumper.new_data_folder), release="2025sep1"
    )
    generated_for = []
    dumper._generate_conflation_database = generated_for.append

    dumper.post_dump()

    assert generated_for == [Path(dumper.new_data_folder)]
    assert dumper.client.get_calls == []


def test_production_retains_single_snapshot(nodenorm_modules):
    assert nodenorm_modules.dumper.NodeNormDumper.ARCHIVE is False
