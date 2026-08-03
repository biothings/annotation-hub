import asyncio
import importlib.util
import logging
import sys
import threading
import time
import types
from collections import deque
from pathlib import Path

import pytest


class FakeResponse:
    def __init__(
        self,
        status_code=206,
        *,
        headers=None,
        chunks=(),
        reason="OK",
    ):
        self.status_code = status_code
        self.headers = headers or {}
        self.chunks = chunks
        self.reason = reason
        self.closed = False

    def iter_content(self, chunk_size):
        del chunk_size
        yield from self.chunks

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, responses):
        self.responses = deque(responses)
        self.get_calls = []
        self.head_calls = []
        self.lock = threading.Lock()

    def get(self, url, **kwargs):
        with self.lock:
            self.get_calls.append((url, kwargs))
            response = self.responses.popleft()
        if isinstance(response, Exception):
            raise response
        return response

    def head(self, url, **kwargs):
        with self.lock:
            self.head_calls.append((url, kwargs))
            response = self.responses.popleft()
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture(
    params=[
        pytest.param(("nameres", "NameResDumper"), id="nameres"),
        pytest.param(("nodenorm", "NodeNormDumper"), id="nodenorm"),
    ]
)
def range_dumper_module(request, monkeypatch, tmp_path):
    plugin_name, dumper_class_name = request.param
    config = types.SimpleNamespace(
        DATA_ARCHIVE_ROOT=str(tmp_path),
        logger=logging.getLogger(f"test_{plugin_name}_dumper"),
    )
    biothings_module = types.ModuleType("biothings")
    biothings_module.config = config

    dumper_dependency = types.ModuleType("biothings.hub.dataload.dumper")

    class DummyDumperException(Exception):
        pass

    class DummyLastModifiedHTTPDumper:
        pass

    dumper_dependency.DumperException = DummyDumperException
    dumper_dependency.LastModifiedHTTPDumper = DummyLastModifiedHTTPDumper

    manager_dependency = types.ModuleType("biothings.utils.manager")

    class DummyJobManager:
        pass

    manager_dependency.JobManager = DummyJobManager

    monkeypatch.setitem(sys.modules, "biothings", biothings_module)
    monkeypatch.setitem(sys.modules, "biothings.hub", types.ModuleType("biothings.hub"))
    monkeypatch.setitem(
        sys.modules,
        "biothings.hub.dataload",
        types.ModuleType("biothings.hub.dataload"),
    )
    monkeypatch.setitem(
        sys.modules, "biothings.hub.dataload.dumper", dumper_dependency
    )
    monkeypatch.setitem(
        sys.modules, "biothings.utils", types.ModuleType("biothings.utils")
    )
    monkeypatch.setitem(sys.modules, "biothings.utils.manager", manager_dependency)

    package_name = f"_test_{plugin_name}_{id(tmp_path)}"
    package = types.ModuleType(package_name)
    package.__path__ = [
        str(Path(__file__).parents[1] / "plugins" / plugin_name)
    ]
    monkeypatch.setitem(sys.modules, package_name, package)

    module_dir = Path(__file__).parents[1] / "plugins" / plugin_name
    static_spec = importlib.util.spec_from_file_location(
        f"{package_name}.static", module_dir / "static.py"
    )
    static_module = importlib.util.module_from_spec(static_spec)
    monkeypatch.setitem(sys.modules, f"{package_name}.static", static_module)
    assert static_spec.loader is not None
    static_spec.loader.exec_module(static_module)

    dumper_spec = importlib.util.spec_from_file_location(
        f"{package_name}.dumper", module_dir / "dumper.py"
    )
    dumper_module = importlib.util.module_from_spec(dumper_spec)
    monkeypatch.setitem(sys.modules, f"{package_name}.dumper", dumper_module)
    assert dumper_spec.loader is not None
    dumper_spec.loader.exec_module(dumper_module)
    dumper_module._test_dumper_class = getattr(
        dumper_module, dumper_class_name
    )
    return dumper_module


def make_dumper(dumper_module):
    dumper = object.__new__(dumper_module._test_dumper_class)
    dumper.logger = logging.getLogger("test_range_download_dumper.instance")
    return dumper


@pytest.mark.parametrize(
    ("file_size", "partitions", "expected"),
    [
        (10, 3, [(0, 3), (4, 7), (8, 9)]),
        (100, 50, [(index, index + 1) for index in range(0, 100, 2)]),
    ],
)
def test_range_chunks_cover_file_without_extra_chunk(
    range_dumper_module, file_size, partitions, expected
):
    dumper = make_dumper(range_dumper_module)
    dumper.get_file_size = lambda url: file_size

    assert dumper.get_range_chunks("https://example.test/data", partitions) == expected


@pytest.mark.parametrize(
    "content_length",
    [
        pytest.param(None, id="missing"),
        pytest.param("not-a-number", id="invalid"),
        pytest.param("0", id="nonpositive"),
    ],
)
def test_get_file_size_rejects_invalid_content_length(
    range_dumper_module, content_length
):
    headers = (
        {} if content_length is None else {"Content-Length": content_length}
    )
    response = FakeResponse(status_code=200, headers=headers)
    dumper = make_dumper(range_dumper_module)
    dumper.client = FakeClient([response])

    with pytest.raises(
        range_dumper_module.DumperException, match="invalid Content-Length"
    ):
        dumper.get_file_size("https://example.test/data")

    assert response.closed is True
    assert (
        dumper.client.head_calls[0][1]["timeout"]
        == dumper.RANGE_REQUEST_TIMEOUT
    )


def test_get_file_size_wraps_request_errors(range_dumper_module):
    request_error = range_dumper_module.requests_exceptions.Timeout(
        "request timed out"
    )
    dumper = make_dumper(range_dumper_module)
    dumper.client = FakeClient([request_error])

    with pytest.raises(
        range_dumper_module.DumperException,
        match="Unable to determine size.*request timed out",
    ):
        dumper.get_file_size("https://example.test/data")

    assert (
        dumper.client.head_calls[0][1]["timeout"]
        == dumper.RANGE_REQUEST_TIMEOUT
    )


def test_custom_headers_are_applied_to_size_and_range_requests(
    range_dumper_module, tmp_path
):
    size_response = FakeResponse(
        status_code=200, headers={"Content-Length": "1"}
    )
    range_response = FakeResponse(
        headers={"Content-Range": "bytes 0-0/1"},
        chunks=(b"a",),
    )
    dumper = make_dumper(range_dumper_module)
    dumper.client = FakeClient([size_response, range_response])
    dumper.prepare_local_folders = lambda path: Path(path).parent.mkdir(
        parents=True, exist_ok=True
    )
    output_path = tmp_path / "combined"
    custom_headers = {
        "Authorization": "Bearer example-token",
        "range": "bytes=99-100",
    }

    dumper.download(
        "https://example.test/data",
        output_path,
        headers=custom_headers,
    )

    assert output_path.read_bytes() == b"a"
    assert dumper.client.head_calls[0][1]["headers"] == {
        "Authorization": "Bearer example-token"
    }
    request_headers = dumper.client.get_calls[0][1]["headers"]
    assert request_headers["Authorization"] == "Bearer example-token"
    assert request_headers["Range"] == "bytes=0-0"
    assert custom_headers["range"] == "bytes=99-100"


def test_download_range_retries_retryable_status(range_dumper_module, tmp_path):
    retry_response = FakeResponse(status_code=503, reason="Service Unavailable")
    success_response = FakeResponse(
        headers={"Content-Range": "bytes 0-3/4"},
        chunks=(b"ab", b"cd"),
    )
    dumper = make_dumper(range_dumper_module)
    dumper.client = FakeClient([retry_response, success_response])
    dumper.RANGE_DOWNLOAD_MAX_ATTEMPTS = 2
    dumper.RANGE_DOWNLOAD_BACKOFF_SECONDS = 0
    output_path = tmp_path / "chunk.part0"

    dumper.download_range(
        "https://example.test/data", start=0, end=3, output=str(output_path)
    )

    assert output_path.read_bytes() == b"abcd"
    assert len(dumper.client.get_calls) == 2
    assert dumper.client.get_calls[0][1]["stream"] is True
    assert (
        dumper.client.get_calls[0][1]["timeout"] == dumper.RANGE_REQUEST_TIMEOUT
    )
    assert retry_response.closed is True
    assert success_response.closed is True


def test_download_range_rejects_incomplete_response(
    range_dumper_module, tmp_path
):
    responses = [
        FakeResponse(
            headers={"Content-Range": "bytes 0-3/4"},
            chunks=(b"abc",),
        )
        for _ in range(2)
    ]
    dumper = make_dumper(range_dumper_module)
    dumper.client = FakeClient(responses)
    dumper.RANGE_DOWNLOAD_MAX_ATTEMPTS = 2
    dumper.RANGE_DOWNLOAD_BACKOFF_SECONDS = 0
    output_path = tmp_path / "chunk.part0"

    with pytest.raises(
        range_dumper_module.DumperException,
        match="after 2 attempts.*expected 4 bytes, received 3",
    ):
        dumper.download_range(
            "https://example.test/data", start=0, end=3, output=str(output_path)
        )

    assert not output_path.exists()
    assert not Path(f"{output_path}.tmp").exists()


def test_worker_failure_is_propagated_and_target_is_preserved(
    range_dumper_module, tmp_path
):
    dumper = make_dumper(range_dumper_module)
    dumper.prepare_local_folders = lambda path: Path(path).parent.mkdir(
        parents=True, exist_ok=True
    )
    dumper.get_range_chunks = lambda url, partitions: [(0, 1), (2, 3)]

    def download_range(url, start, end, output):
        del url, end
        if start == 2:
            raise range_dumper_module.DumperException("worker failed")
        Path(output).write_bytes(b"ab")

    dumper.download_range = download_range
    output_path = tmp_path / "combined"
    output_path.write_bytes(b"previous")

    with pytest.raises(
        range_dumper_module.DumperException, match="worker failed"
    ):
        dumper._download_in_ranges(
            "https://example.test/data",
            output_path,
            num_partitions=2,
            max_workers=2,
        )

    assert output_path.read_bytes() == b"previous"
    assert list(tmp_path.glob("combined.part*")) == []


def test_range_worker_concurrency_is_bounded(range_dumper_module, tmp_path):
    dumper = make_dumper(range_dumper_module)
    dumper.prepare_local_folders = lambda path: Path(path).parent.mkdir(
        parents=True, exist_ok=True
    )
    dumper.get_range_chunks = lambda url, partitions: [
        (index, index) for index in range(12)
    ]

    lock = threading.Lock()
    active_workers = 0
    peak_workers = 0

    def download_range(url, start, end, output):
        nonlocal active_workers, peak_workers
        del url, end
        with lock:
            active_workers += 1
            peak_workers = max(peak_workers, active_workers)
        try:
            time.sleep(0.01)
            Path(output).write_bytes(bytes([start]))
        finally:
            with lock:
                active_workers -= 1

    dumper.download_range = download_range
    output_path = tmp_path / "combined"

    dumper._download_in_ranges(
        "https://example.test/data",
        output_path,
        num_partitions=12,
        max_workers=3,
    )

    assert 1 < peak_workers <= 3
    assert output_path.read_bytes() == bytes(range(12))
    assert list(tmp_path.glob("combined.part*")) == []


def test_large_file_concurrency_is_bounded(range_dumper_module):
    dumper = make_dumper(range_dumper_module)
    dumper.MAX_PARALLEL_LARGE_FILES = 2
    dumper.to_dump_large = [
        {
            "remoteurl": f"https://example.test/data-{index}",
            "localfile": f"/tmp/data-{index}",
            "num_partitions": 2,
        }
        for index in range(5)
    ]
    dumper.unprepare = lambda: None
    dumper.get_pinfo = lambda: {}

    class FakeJobManager:
        def __init__(self):
            self.active_jobs = 0
            self.peak_jobs = 0

        async def defer_to_process(self, pinfo, function):
            del pinfo, function

            async def run_job():
                self.active_jobs += 1
                self.peak_jobs = max(self.peak_jobs, self.active_jobs)
                try:
                    await asyncio.sleep(0.01)
                finally:
                    self.active_jobs -= 1

            return asyncio.create_task(run_job())

    job_manager = FakeJobManager()

    asyncio.run(dumper._handle_large_size_files(job_manager))

    assert job_manager.peak_jobs == 2
    assert dumper.to_dump_large == []
