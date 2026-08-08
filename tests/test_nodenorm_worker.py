import importlib.util
import itertools
import json
import logging
import queue
import sqlite3
import sys
import threading
import types
from pathlib import Path

import pytest


@pytest.fixture
def worker_module(monkeypatch, tmp_path):
    config = types.SimpleNamespace(
        DATA_ARCHIVE_ROOT=str(tmp_path),
        logger=logging.getLogger("test_nodenorm_worker"),
    )
    biothings_module = types.ModuleType("biothings")
    biothings_module.config = config

    utils_module = types.ModuleType("biothings.utils")
    dataload_module = types.ModuleType("biothings.utils.dataload")
    dataload_module.merge_struct = lambda incoming, existing: {
        **incoming,
        **existing,
    }
    serializer_module = types.ModuleType("biothings.utils.serializer")
    serializer_module.json_loads = json.loads
    hub_db_module = types.ModuleType("biothings.utils.hub_db")
    hub_db_module.get_src_db = lambda: None
    common_module = types.ModuleType("biothings.utils.common")

    def iter_n(iterable, size):
        iterator = iter(iterable)
        while batch := list(itertools.islice(iterator, size)):
            yield batch

    common_module.iter_n = iter_n

    # worker.py imports pymongo at module scope. None of these tests exercise a
    # pymongo object, so stub it the same way biothings is stubbed above rather
    # than making the suite depend on a real install.
    pymongo_module = types.ModuleType("pymongo")
    errors_module = types.ModuleType("pymongo.errors")
    collection_module = types.ModuleType("pymongo.collection")

    class BulkWriteError(Exception):
        def __init__(self, details=None):
            super().__init__(details)
            self.details = details if details is not None else {}

    class ServerSelectionTimeoutError(Exception):
        pass

    class FakeCollection:
        def __init__(self, database=None, name=None):
            self.database = database
            self.name = name

    class FakeWriteOperation:
        def __init__(self, *arguments):
            self.arguments = arguments

    errors_module.BulkWriteError = BulkWriteError
    errors_module.ServerSelectionTimeoutError = ServerSelectionTimeoutError
    collection_module.Collection = FakeCollection
    pymongo_module.errors = errors_module
    pymongo_module.collection = collection_module
    for operation_name in ("DeleteOne", "ReplaceOne", "UpdateOne"):
        setattr(
            pymongo_module,
            operation_name,
            type(operation_name, (FakeWriteOperation,), {}),
        )

    monkeypatch.setitem(sys.modules, "pymongo", pymongo_module)
    monkeypatch.setitem(sys.modules, "pymongo.errors", errors_module)
    monkeypatch.setitem(sys.modules, "pymongo.collection", collection_module)

    monkeypatch.setitem(sys.modules, "biothings", biothings_module)
    monkeypatch.setitem(sys.modules, "biothings.utils", utils_module)
    monkeypatch.setitem(sys.modules, "biothings.utils.dataload", dataload_module)
    monkeypatch.setitem(sys.modules, "biothings.utils.serializer", serializer_module)
    monkeypatch.setitem(sys.modules, "biothings.utils.hub_db", hub_db_module)
    monkeypatch.setitem(sys.modules, "biothings.utils.common", common_module)

    package_name = f"_test_nodenorm_worker_{id(tmp_path)}"
    module_dir = Path(__file__).parents[1] / "plugins" / "nodenorm"
    package = types.ModuleType(package_name)
    package.__path__ = [str(module_dir)]
    monkeypatch.setitem(sys.modules, package_name, package)

    static_module = types.ModuleType(f"{package_name}.static")
    static_module.CONFLATION_LOOKUP_DATABASE = "conflation.sqlite3"
    static_module.DRUG_CHEMICAL_IDENTIFIER_FILES = set()
    static_module.GENE_PROTEIN_IDENTIFER_FILES = set()
    static_module.IDENTIFIER_LOOKUP_DATABASE = "identifier.sqlite3"
    static_module.NODENORM_UPLOAD_CHUNKS = {}
    monkeypatch.setitem(sys.modules, f"{package_name}.static", static_module)

    spec = importlib.util.spec_from_file_location(
        f"{package_name}.worker", module_dir / "worker.py"
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, f"{package_name}.worker", module)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeIdentifierConnection:
    def __init__(self, commit_error=None, close_error=None):
        self.closed = False
        self.commits = 0
        self.cursor_instance = object()
        self.commit_error = commit_error
        self.close_error = close_error

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1
        if self.commit_error is not None:
            raise self.commit_error

    def close(self):
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def test_identifier_writer_failure_drains_queue_until_stop(worker_module, monkeypatch):
    identifier_queue = queue.Queue()
    identifier_queue.put(["first"])
    identifier_queue.put(["already-buffered"])
    identifier_queue.put(worker_module.IDENTIFIER_WRITER_STOP)
    writer_failed = threading.Event()
    writer_errors = []
    connection = FakeIdentifierConnection()
    write_error = sqlite3.OperationalError("identifier database is full")

    monkeypatch.setattr(
        worker_module,
        "_connect_identifier_database",
        lambda _database: connection,
    )

    def fail_update(_cursor, _identifiers):
        raise write_error

    monkeypatch.setattr(worker_module, "update_identifier_collection", fail_update)

    writer = threading.Thread(
        target=worker_module._write_identifier_batches,
        args=("identifier.sqlite3", identifier_queue, writer_failed, writer_errors),
        daemon=True,
    )
    writer.start()
    writer.join(timeout=2)

    assert not writer.is_alive()
    assert writer_failed.is_set()
    assert writer_errors == [write_error]
    assert identifier_queue.empty()
    assert connection.closed is True


def test_identifier_writer_commit_failure_after_stop_does_not_wait_for_second_stop(
    worker_module, monkeypatch
):
    identifier_queue = queue.Queue()
    identifier_queue.put(["final-batch"])
    identifier_queue.put(worker_module.IDENTIFIER_WRITER_STOP)
    writer_failed = threading.Event()
    writer_errors = []
    commit_error = sqlite3.OperationalError("final commit failed")
    connection = FakeIdentifierConnection(commit_error=commit_error)

    monkeypatch.setattr(
        worker_module,
        "_connect_identifier_database",
        lambda _database: connection,
    )
    monkeypatch.setattr(
        worker_module,
        "update_identifier_collection",
        lambda _cursor, _identifiers: None,
    )

    writer = threading.Thread(
        target=worker_module._write_identifier_batches,
        args=("identifier.sqlite3", identifier_queue, writer_failed, writer_errors),
        daemon=True,
    )
    writer.start()
    writer.join(timeout=2)

    assert not writer.is_alive()
    assert writer_failed.is_set()
    assert writer_errors == [commit_error]
    assert identifier_queue.empty()
    assert connection.closed is True


def test_identifier_writer_close_failure_is_recorded(worker_module, monkeypatch):
    identifier_queue = queue.Queue()
    identifier_queue.put(worker_module.IDENTIFIER_WRITER_STOP)
    writer_failed = threading.Event()
    writer_errors = []
    close_error = sqlite3.OperationalError("close failed")
    connection = FakeIdentifierConnection(close_error=close_error)

    monkeypatch.setattr(
        worker_module,
        "_connect_identifier_database",
        lambda _database: connection,
    )

    worker_module._write_identifier_batches(
        "identifier.sqlite3", identifier_queue, writer_failed, writer_errors
    )

    assert writer_failed.is_set()
    assert writer_errors == [close_error]
    assert identifier_queue.empty()
    assert connection.closed is True


def test_peer_identifier_writer_drains_after_shared_failure(worker_module, monkeypatch):
    identifier_queue = queue.Queue()
    identifier_queue.put(["discard-me"])
    identifier_queue.put(worker_module.IDENTIFIER_WRITER_STOP)
    writer_failed = threading.Event()
    writer_failed.set()
    connection = FakeIdentifierConnection()
    updates = []

    monkeypatch.setattr(
        worker_module,
        "_connect_identifier_database",
        lambda _database: connection,
    )
    monkeypatch.setattr(
        worker_module,
        "update_identifier_collection",
        lambda cursor, identifiers: updates.append((cursor, identifiers)),
    )

    worker_module._write_identifier_batches(
        "identifier.sqlite3", identifier_queue, writer_failed, []
    )

    assert updates == []
    assert identifier_queue.empty()
    assert connection.commits == 0
    assert connection.closed is True


def test_duplicate_cleanup_failure_is_propagated_and_cancels_pending(
    worker_module, monkeypatch
):
    cleanup_error = RuntimeError("duplicate cleanup failed")

    class FakeFuture:
        def __init__(self, error=None, result=None):
            self.error = error
            self.value = result
            self.cancelled = False

        def result(self):
            if self.error is not None:
                raise self.error
            return self.value

        def cancel(self):
            self.cancelled = True
            return True

    failed_future = FakeFuture(error=cleanup_error)
    pending_future = FakeFuture(result=(1, 0))

    class FakeExecutor:
        def __init__(self, **_kwargs):
            self.futures = iter((failed_future, pending_future))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, *_args, **_kwargs):
            return next(self.futures)

    monkeypatch.setattr(
        worker_module.concurrent.futures,
        "ThreadPoolExecutor",
        FakeExecutor,
    )
    monkeypatch.setattr(
        worker_module.concurrent.futures,
        "as_completed",
        lambda futures: iter(futures),
    )
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter(("one", "two")),
    )
    monkeypatch.setattr(
        worker_module,
        "iter_n",
        lambda _identifiers, _batch_size: (("one",), ("two",)),
    )

    with pytest.raises(RuntimeError) as raised:
        worker_module.cleanup_curie_duplication("data", "collection")

    assert raised.value is cleanup_error
    assert failed_future.cancelled is True
    assert pending_future.cancelled is True


def test_upload_raises_original_identifier_writer_error(worker_module, monkeypatch):
    writer_error = sqlite3.OperationalError("identifier database is full")
    worker_error = RuntimeError("Identifier writer failed; aborting upload worker")

    class FakeQueue:
        def __init__(self):
            self.items = queue.Queue()

        def put(self, item):
            self.items.put(item)

        def get(self, timeout=None):
            return self.items.get(timeout=timeout)

        def close(self):
            pass

        def join_thread(self):
            pass

    class FakeContext:
        def Queue(self, **_kwargs):
            return FakeQueue()

        def Event(self):
            return threading.Event()

    class FailedFuture:
        def result(self):
            raise worker_error

        def cancel(self):
            return False

    failed_future = FailedFuture()

    class FakeExecutor:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, *_args, **_kwargs):
            return failed_future

    def fail_identifier_writer(
        _database, identifier_queue, writer_failed, writer_errors
    ):
        writer_errors.append(writer_error)
        writer_failed.set()
        worker_module._drain_identifier_queue(identifier_queue)

    monkeypatch.setattr(worker_module, "_configure_sqlite_tmpdir", lambda: None)
    monkeypatch.setattr(worker_module, "create_identifiers_table", lambda _path: None)
    monkeypatch.setattr(
        worker_module.multiprocessing, "get_context", lambda _kind: FakeContext()
    )
    monkeypatch.setattr(worker_module, "NODENORM_IDENTIFIER_SHARD_COUNT", 1)
    monkeypatch.setattr(
        worker_module,
        "_identifier_database_paths",
        lambda _path: (Path("identifier.sqlite3"),),
    )
    monkeypatch.setattr(
        worker_module, "_write_identifier_batches", fail_identifier_writer
    )
    monkeypatch.setattr(
        worker_module.concurrent.futures,
        "ProcessPoolExecutor",
        FakeExecutor,
    )
    monkeypatch.setattr(
        worker_module.concurrent.futures,
        "as_completed",
        lambda _futures: iter((failed_future,)),
    )
    monkeypatch.setattr(
        worker_module,
        "_build_offset_tasks",
        lambda _data_folder, _collection: iter(({},)),
    )

    post_upload_calls = []
    monkeypatch.setattr(
        worker_module,
        "create_mongo_identifiers_index",
        lambda _collection: post_upload_calls.append("mongo-index"),
    )
    monkeypatch.setattr(
        worker_module,
        "create_identifiers_index",
        lambda _path: post_upload_calls.append("sqlite-index"),
    )
    monkeypatch.setattr(
        worker_module,
        "cleanup_curie_duplication",
        lambda _path, _collection: post_upload_calls.append("cleanup"),
    )

    with pytest.raises(sqlite3.OperationalError) as raised:
        worker_module.upload_process("data", "collection")

    assert raised.value is writer_error
    assert raised.value.__cause__ is worker_error
    assert post_upload_calls == []
