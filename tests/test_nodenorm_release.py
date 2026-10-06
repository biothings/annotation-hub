import asyncio
import importlib.util
import json
import logging
import sys
import types
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
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
DUPLICATE_REPORT_HEADER = "clique_leader\tfilenames\tbiolink_types\tclique_identifier_counts\tclique_leader_count\n"


class FakeResponse:
    def __init__(self, text="", status_code=200, reason="OK", headers=None):
        self.text = text
        self.status_code = status_code
        self.reason = reason
        self.headers = headers or {}
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
    (
        data_folder / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME
    ).write_text(DUPLICATE_REPORT_HEADER, encoding="utf-8")
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


def test_release_marker_nonretryable_request_errors_are_wrapped(
    nodenorm_modules, tmp_path
):
    request_error = nodenorm_modules.dumper.requests_exceptions.InvalidURL(
        "invalid URL"
    )
    dumper = make_dumper(nodenorm_modules, tmp_path, request_error)

    with pytest.raises(nodenorm_modules.dumper.DumperException, match="invalid URL"):
        dumper.get_release()

    assert len(dumper.client.get_calls) == 1


def test_release_marker_certificate_errors_are_not_retried(
    nodenorm_modules, tmp_path, monkeypatch
):
    certificate_error = nodenorm_modules.dumper.requests_exceptions.SSLError(
        "certificate verification failed"
    )
    dumper = make_dumper(nodenorm_modules, tmp_path, FakeResponse())
    dumper.client = FakeClient([certificate_error, FakeResponse("Babel 2025sep1\n")])
    sleeps = []
    monkeypatch.setattr(nodenorm_modules.dumper.time, "sleep", sleeps.append)

    with pytest.raises(
        nodenorm_modules.dumper.DumperException,
        match="certificate verification failed",
    ):
        dumper.get_release()

    assert len(dumper.client.get_calls) == 1
    assert sleeps == []


def test_release_marker_nonretryable_http_errors_fail_immediately(
    nodenorm_modules, tmp_path, monkeypatch
):
    response = FakeResponse(status_code=404, reason="Not Found")
    dumper = make_dumper(nodenorm_modules, tmp_path, response)
    sleeps = []
    monkeypatch.setattr(nodenorm_modules.dumper.time, "sleep", sleeps.append)

    with pytest.raises(nodenorm_modules.dumper.DumperException, match="status: 404"):
        dumper.get_release()

    assert response.closed is True
    assert len(dumper.client.get_calls) == 1
    assert sleeps == []


def test_release_marker_retries_timeout_then_succeeds(
    nodenorm_modules, tmp_path, monkeypatch
):
    timeout = nodenorm_modules.dumper.requests_exceptions.Timeout("timed out")
    response = FakeResponse("Babel 2025sep1\n")
    dumper = make_dumper(nodenorm_modules, tmp_path, response)
    dumper.client = FakeClient([timeout, response])
    sleeps = []
    monkeypatch.setattr(nodenorm_modules.dumper.random, "uniform", lambda *_: 0)
    monkeypatch.setattr(nodenorm_modules.dumper.time, "sleep", sleeps.append)

    assert dumper.get_release() == "2025sep1"

    assert len(dumper.client.get_calls) == 2
    assert sleeps == [2]
    assert response.closed is True


def test_release_marker_retries_interrupted_response_then_succeeds(
    nodenorm_modules, tmp_path, monkeypatch
):
    interrupted = nodenorm_modules.dumper.requests_exceptions.ChunkedEncodingError(
        "incomplete body"
    )
    response = FakeResponse("Babel 2025sep1\n")
    dumper = make_dumper(nodenorm_modules, tmp_path, response)
    dumper.client = FakeClient([interrupted, response])
    sleeps = []
    monkeypatch.setattr(nodenorm_modules.dumper.random, "uniform", lambda *_: 0)
    monkeypatch.setattr(nodenorm_modules.dumper.time, "sleep", sleeps.append)

    assert dumper.get_release() == "2025sep1"

    assert len(dumper.client.get_calls) == 2
    assert sleeps == [2]
    assert response.closed is True


def test_release_marker_honors_retry_after_for_retryable_http_error(
    nodenorm_modules, tmp_path, monkeypatch
):
    unavailable = FakeResponse(
        status_code=503,
        reason="Service Unavailable",
        headers={"Retry-After": "300"},
    )
    response = FakeResponse("Babel 2025sep1\n")
    dumper = make_dumper(nodenorm_modules, tmp_path, response)
    dumper.client = FakeClient([unavailable, response])
    sleeps = []
    monkeypatch.setattr(
        nodenorm_modules.dumper.random, "uniform", lambda _lower, upper: upper
    )
    monkeypatch.setattr(nodenorm_modules.dumper.time, "sleep", sleeps.append)

    assert dumper.get_release() == "2025sep1"

    assert sleeps == [300]
    assert unavailable.closed is True
    assert response.closed is True


def test_release_marker_parses_http_date_retry_after(
    nodenorm_modules, tmp_path, monkeypatch
):
    now = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
    retry_at = now + timedelta(seconds=90)

    class FrozenDateTime:
        @classmethod
        def now(cls, tz=None):
            assert tz is timezone.utc
            return now

    dumper = make_dumper(nodenorm_modules, tmp_path, FakeResponse())
    response = FakeResponse(
        headers={"Retry-After": format_datetime(retry_at, usegmt=True)}
    )
    monkeypatch.setattr(nodenorm_modules.dumper, "datetime", FrozenDateTime)

    assert dumper._retry_after_seconds(response) == 90


def test_release_marker_exhausts_transient_retry_budget(
    nodenorm_modules, tmp_path, monkeypatch
):
    timeouts = [
        nodenorm_modules.dumper.requests_exceptions.Timeout(f"timeout {attempt}")
        for attempt in range(1, 5)
    ]
    dumper = make_dumper(nodenorm_modules, tmp_path, FakeResponse())
    dumper.client = FakeClient(timeouts)
    sleeps = []
    monkeypatch.setattr(nodenorm_modules.dumper.random, "uniform", lambda *_: 0)
    monkeypatch.setattr(nodenorm_modules.dumper.time, "sleep", sleeps.append)

    with pytest.raises(
        nodenorm_modules.dumper.DumperException, match="after 4 attempts.*timeout 4"
    ):
        dumper.get_release()

    assert len(dumper.client.get_calls) == 4
    assert sleeps == [2, 4, 8]


def test_invalid_release_marker_is_not_retried(nodenorm_modules, tmp_path, monkeypatch):
    invalid = FakeResponse("Babel unexpected\n")
    dumper = make_dumper(nodenorm_modules, tmp_path, invalid)
    dumper.client = FakeClient([invalid, FakeResponse("Babel 2025sep1\n")])
    sleeps = []
    monkeypatch.setattr(nodenorm_modules.dumper.time, "sleep", sleeps.append)

    with pytest.raises(nodenorm_modules.dumper.DumperException, match="Invalid"):
        dumper.get_release()

    assert len(dumper.client.get_calls) == 1
    assert sleeps == []
    assert invalid.closed is True


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
        "https://stars.renci.org/var/babel_outputs/2025sep1/reports/duckdb/duplicate_clique_leaders.tsv",
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

    assert len(dumper.to_dump) == 6
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
    (
        data_folder / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME
    ).write_text(DUPLICATE_REPORT_HEADER, encoding="utf-8")
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


def test_duplicate_report_uses_manifest_order_and_preserves_declared_sources(
    nodenorm_modules, tmp_path
):
    write_release_files(nodenorm_modules, tmp_path)
    report = tmp_path / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME
    report.write_text(
        DUPLICATE_REPORT_HEADER
        + "EX:1\t[Protein, CellLine, Food]\t['biolink:Protein', 'biolink:CellLine', "
        "'biolink:Food']\t[2, 1, 4]\t3\n",
        encoding="utf-8",
    )

    assert nodenorm_modules.release.read_duplicate_clique_leaders(tmp_path) == {
        "EX:1": ("CellLine.txt", "Food.txt", "Protein.txt")
    }


def test_header_only_duplicate_report_is_valid(nodenorm_modules, tmp_path):
    write_release_files(nodenorm_modules, tmp_path)

    assert nodenorm_modules.release.read_duplicate_clique_leaders(tmp_path) == {}


@pytest.mark.parametrize(
    "rows, expected",
    [
        ("", {}),
        ("EX:1\t2\t[Protein, Food]\n", {"EX:1": ("Food.txt", "Protein.txt")}),
    ],
)
def test_legacy_duplicate_report_without_metadata_is_valid(
    nodenorm_modules, tmp_path, rows, expected
):
    write_release_files(nodenorm_modules, tmp_path)
    (tmp_path / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME).write_text(
        "clique_leader\tclique_leader_count\tfilenames\n" + rows,
        encoding="utf-8",
    )

    assert nodenorm_modules.release.read_duplicate_clique_leaders(tmp_path) == expected


@pytest.mark.parametrize(
    "column, value, error",
    [
        ("biolink_types", "['biolink:Protein', 'biolink:Food']", None),
        ("clique_identifier_counts", "[1, 2]", None),
        ("biolink_types", "[biolink:Protein, biolink:Food]", "quoted strings"),
        ("biolink_types", "['biolink:Protein']", "list lengths"),
        ("clique_identifier_counts", "[0, 2]", "positive integers"),
        ("clique_identifier_counts", "[1]", "list lengths"),
    ],
)
def test_duplicate_report_optional_metadata_validated_independently(
    nodenorm_modules, tmp_path, column, value, error
):
    write_release_files(nodenorm_modules, tmp_path)
    (tmp_path / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME).write_text(
        f"clique_leader\tclique_leader_count\tfilenames\t{column}\n"
        f"EX:1\t2\t[Protein, Food]\t{value}\n",
        encoding="utf-8",
    )

    if error:
        with pytest.raises(nodenorm_modules.release.NodeNormReleaseError, match=error):
            nodenorm_modules.release.read_duplicate_clique_leaders(tmp_path)
    else:
        assert nodenorm_modules.release.read_duplicate_clique_leaders(tmp_path) == {
            "EX:1": ("Food.txt", "Protein.txt")
        }


@pytest.mark.parametrize(
    "report_text, message",
    [
        ("", "required columns"),
        ("clique_leader\tfilenames\n", "required columns"),
        (
            DUPLICATE_REPORT_HEADER.replace("filenames", "clique_leader"),
            "required columns",
        ),
        (DUPLICATE_REPORT_HEADER + "EX:1\t[CellLine, Food]\n", "match its header"),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine, Food]\t['biolink:CellLine', 'biolink:Food']\t[1, 2]\t2\textra\n",
            "match its header",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "\t[CellLine, Food]\t['biolink:CellLine', 'biolink:Food']\t[1, 2]\t2\n",
            "CURIE",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine, Food]\t['biolink:CellLine', 'biolink:Food']\t[1, 2]\t2\n"
            + "EX:1\t[CellLine, Protein]\t['biolink:CellLine', 'biolink:Protein']\t[1, 2]\t2\n",
            "repeated clique_leader",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[Food, Food]\t['biolink:Food', 'biolink:Food']\t[1, 2]\t2\n",
            "repeated compendium",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[Missing, Food]\t['biolink:CellLine', 'biolink:Food']\t[1, 2]\t2\n",
            "unknown compendium",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine.txt, Food]\t['biolink:CellLine', 'biolink:Food']\t[1, 2]\t2\n",
            "unknown compendium",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\tCellLine, Food\t['biolink:CellLine', 'biolink:Food']\t[1, 2]\t2\n",
            "bracketed list",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine, ]\t['biolink:CellLine', 'biolink:Food']\t[1, 2]\t2\n",
            "invalid list item",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine]\t['biolink:CellLine']\t[1]\t1\n",
            "at least 2",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine, Food]\t['biolink:CellLine', 'biolink:Food']\t[1, 2]\t3\n",
            "list lengths",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine, Food]\t[biolink:CellLine, biolink:Food]\t[1, 2]\t2\n",
            "quoted strings",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine, Food]\t['biolink:CellLine', 'Food']\t[1, 2]\t2\n",
            "invalid biolink_types",
        ),
        (
            DUPLICATE_REPORT_HEADER
            + "EX:1\t[CellLine, Food]\t['biolink:CellLine', 'biolink:Food']\t[0, 2]\t2\n",
            "positive integers",
        ),
    ],
)
def test_duplicate_report_rejects_malformed_or_ambiguous_rows(
    nodenorm_modules, tmp_path, report_text, message
):
    write_release_files(nodenorm_modules, tmp_path)
    (tmp_path / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME).write_text(
        report_text, encoding="utf-8"
    )

    with pytest.raises(
        nodenorm_modules.release.NodeNormReleaseError, match=message
    ) as exc:
        nodenorm_modules.release.read_duplicate_clique_leaders(tmp_path)

    assert "rerun the NodeNorm dump" in str(exc.value)


def test_local_artifact_validation_requires_duplicate_report(
    nodenorm_modules, tmp_path
):
    write_release_files(nodenorm_modules, tmp_path)
    (tmp_path / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME).unlink()

    with pytest.raises(
        nodenorm_modules.release.NodeNormReleaseError,
        match="duplicate clique leader report.*rerun the NodeNorm dump",
    ):
        nodenorm_modules.release.local_compendium_paths(tmp_path)


@pytest.mark.parametrize("manifest_change", [{"schema_version": 1}, {"reports": []}])
def test_old_or_incomplete_manifest_is_rejected(
    nodenorm_modules, tmp_path, manifest_change
):
    write_release_files(nodenorm_modules, tmp_path)
    path = tmp_path / nodenorm_modules.release.RELEASE_MANIFEST_FILENAME
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.update(manifest_change)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(nodenorm_modules.release.NodeNormReleaseError, match="rerun"):
        nodenorm_modules.release.read_release_manifest(tmp_path)


def test_manifest_records_release_bound_report(nodenorm_modules, tmp_path):
    write_release_files(nodenorm_modules, tmp_path, release="2026jul22")
    payload = json.loads(
        (tmp_path / nodenorm_modules.release.RELEASE_MANIFEST_FILENAME).read_text(
            encoding="utf-8"
        )
    )

    assert payload["schema_version"] == 2
    assert payload["release"] == "2026jul22"
    assert payload["reports"] == ["reports/duckdb/duplicate_clique_leaders.tsv"]


@pytest.mark.parametrize("report_text", [None, "not a valid report"])
def test_current_release_with_bad_report_is_redownloaded(
    nodenorm_modules, tmp_path, report_text
):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        current_release="2025sep1",
    )
    folder = Path(dumper.current_data_folder)
    write_release_files(nodenorm_modules, folder)
    report = folder / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME
    if report_text is None:
        report.unlink()
    else:
        report.write_text(report_text, encoding="utf-8")

    dumper.create_todump_list()

    assert any(
        item["remote"].endswith("/2025sep1/reports/duckdb/duplicate_clique_leaders.tsv")
        for item in dumper.to_dump
    )


def test_post_dump_does_not_promote_invalid_duplicate_report(
    nodenorm_modules, tmp_path
):
    dumper = make_dumper(
        nodenorm_modules,
        tmp_path,
        FakeResponse("Babel 2025sep1\n"),
        current_release="2025sep1",
    )
    folder = Path(dumper.new_data_folder)
    write_release_files(nodenorm_modules, folder)
    (folder / nodenorm_modules.release.DUPLICATE_CLIQUE_LEADERS_FILENAME).write_text(
        "bad report", encoding="utf-8"
    )
    generated_for = []
    dumper._generate_conflation_database = generated_for.append

    with pytest.raises(
        nodenorm_modules.dumper.DumperException, match="duplicate clique leader report"
    ):
        dumper.post_dump()

    assert generated_for == []
    assert not getattr(dumper, "base_post_dump_called", False)
