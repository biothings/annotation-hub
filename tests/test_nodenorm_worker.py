import copy
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


class FakeBulkWriteResult:
    def __init__(self, deleted_count, modified_count):
        self.deleted_count = deleted_count
        self.modified_count = modified_count


class FakeMongoCollection:
    """
    A stateful stand-in that applies filters and reports the counts MongoDB
    would, so tests can assert on effects rather than on requests issued.

    It supports exactly the operations the cleanup emits: DeleteOne by `_id`, a
    compare-and-swap `$set` of the identifier array, and `$pull` by identifier
    CURIE. An update whose result equals the stored value reports no
    modification, as MongoDB does -- that is what makes a no-op write visible.
    Anything else raises, so a new kind of operation cannot silently go
    unverified.
    """

    def __init__(self, documents=()):
        self.documents = [copy.deepcopy(document) for document in documents]
        self.bulk_write_calls = []

    def find(self, query):
        curie = query["identifiers.i"]
        return iter(
            [
                copy.deepcopy(document)
                for document in self.documents
                if curie in [identifier["i"] for identifier in document["identifiers"]]
            ]
        )

    def stored(self, document_id):
        for document in self.documents:
            if document["_id"] == document_id:
                return document
        return None

    def curies(self, document_id):
        document = self.stored(document_id)
        return None if document is None else [i["i"] for i in document["identifiers"]]

    def duplicated_curies(self):
        """
        Every CURIE that still resolves to more than one entry, whether repeated
        inside one document or spread across two. This is the 1-1 identifiers.i
        mapping the Elasticsearch terms query depends on.
        """
        counts = {}
        for document in self.documents:
            for identifier in document["identifiers"]:
                counts[identifier["i"]] = counts.get(identifier["i"], 0) + 1
        return {curie for curie, count in counts.items() if count > 1}

    def bulk_write(self, requests):
        self.bulk_write_calls.append(list(requests))
        deleted_count = 0
        modified_count = 0
        for request in requests:
            request_kind = type(request).__name__
            if request_kind == "DeleteOne":
                (query,) = request.arguments
                document = self.stored(query["_id"])
                if document is not None:
                    self.documents.remove(document)
                    deleted_count += 1
                continue

            assert request_kind == "UpdateOne", f"unsupported request {request_kind}"
            query, update = request.arguments
            document = self.stored(query["_id"])
            if document is None:
                continue
            # compare-and-swap guard: a document changed since it was read no
            # longer matches, so the update applies to nothing
            if (
                "identifiers" in query
                and query["identifiers"] != document["identifiers"]
            ):
                continue

            if "$set" in update:
                replacement = update["$set"]["identifiers"]
            elif "$pull" in update:
                removed = set(update["$pull"]["identifiers"]["i"]["$in"])
                replacement = [
                    identifier
                    for identifier in document["identifiers"]
                    if identifier["i"] not in removed
                ]
            else:
                raise AssertionError(f"unsupported update {update}")

            if replacement == document["identifiers"]:
                continue
            document["identifiers"] = copy.deepcopy(replacement)
            modified_count += 1

        return FakeBulkWriteResult(deleted_count, modified_count)


def install_fake_collection(worker_module, monkeypatch, documents=()):
    collection = FakeMongoCollection(documents)
    monkeypatch.setattr(
        worker_module.pymongo.collection,
        "Collection",
        lambda database, name: collection,
    )
    return collection


def identifier_document(document_id, *curies, node_type="biolink:Disease", labels=None):
    labels = labels or {}
    return {
        "_id": document_id,
        "type": node_type,
        "identifiers": [{"i": curie, "l": labels.get(curie, "")} for curie in curies],
    }


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
    pending_future = FakeFuture(result=(1, 0, 0))

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


def run_batch(worker_module, curies, task_id=0):
    return worker_module._curie_duplication_batch_handler(
        task_id=task_id,
        curies=[(curie,) for curie in curies],
        collection_name="collection",
    )


def test_unresolvable_curie_pair_is_counted_not_written(worker_module, monkeypatch):
    """
    Two documents share a CURIE, neither CURIE set is a subset of the other, and
    neither is biolink:Protein -- so no strategy applies. Nothing may be written:
    pymongo rejects None with "None is not a valid request", which would fail the
    whole batch and discard the corrections computed successfully.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("d1", "CURIE:shared", "CURIE:a"),
            identifier_document(
                "d2", "CURIE:shared", "CURIE:b", node_type="biolink:Gene"
            ),
        ],
    )

    assert run_batch(worker_module, ["CURIE:shared"], task_id=7) == (7, 0, 1)
    assert collection.bulk_write_calls == []
    assert collection.curies("d1") == ["CURIE:shared", "CURIE:a"]
    assert collection.curies("d2") == ["CURIE:shared", "CURIE:b"]


@pytest.mark.parametrize(
    "document_count",
    [pytest.param(0, id="missing"), pytest.param(3, id="three-documents")],
)
def test_unexpected_document_count_is_counted_not_ignored(
    worker_module, monkeypatch, document_count
):
    """
    The identifier shards only record CURIEs seen more than once, so a CURIE
    resolving to 0 or 3+ documents is outside what the handler can reason about.
    Neither branch matched, so it used to be dropped with no log at all.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document(f"d{index}", "CURIE:odd", f"CURIE:{index}")
            for index in range(document_count)
        ],
    )

    assert run_batch(worker_module, ["CURIE:odd"], task_id=3) == (3, 0, 1)
    assert collection.bulk_write_calls == []


def test_shared_curie_with_differing_labels_is_resolved(worker_module, monkeypatch):
    """
    The regression that motivated comparing on "i".

    Comparing whole identifier dictionaries made a CURIE carrying two different
    labels look like two unrelated identifiers: the subset check failed, the
    intersection branch produced a ReplaceOne whose filter equalled its
    replacement, and that no-op was reported as a correction while the CURIE
    stayed duplicated. On CURIEs the subset is obvious and the redundant document
    goes away.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document(
                "protein",
                "CURIE:shared",
                "CURIE:only-here",
                node_type="biolink:Protein",
                labels={"CURIE:shared": "label-A"},
            ),
            identifier_document(
                "small-molecule",
                "CURIE:shared",
                labels={"CURIE:shared": "label-B"},
            ),
        ],
    )

    assert run_batch(worker_module, ["CURIE:shared"]) == (0, 1, 0)
    assert collection.stored("small-molecule") is None
    assert collection.duplicated_curies() == set()


def test_colliding_curies_are_pulled_from_the_non_protein_document(
    worker_module, monkeypatch
):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document(
                "protein", "CURIE:shared", "CURIE:p", node_type="biolink:Protein"
            ),
            identifier_document("small-molecule", "CURIE:shared", "CURIE:s", "CURIE:t"),
        ],
    )

    assert run_batch(worker_module, ["CURIE:shared"]) == (0, 1, 0)
    assert collection.curies("protein") == ["CURIE:shared", "CURIE:p"]
    assert collection.curies("small-molecule") == ["CURIE:s", "CURIE:t"]
    assert collection.duplicated_curies() == set()


def test_two_shared_curies_report_one_correction(worker_module, monkeypatch):
    """
    Both CURIEs resolve to the same pair of documents, so the batch queues two
    delete requests that apply once. Counting the buffer reported two
    corrections; the write result reports the one document that changed.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("keep", "CURIE:x", "CURIE:y", "CURIE:extra"),
            identifier_document("drop", "CURIE:x", "CURIE:y"),
        ],
    )

    task_id, corrections, unresolved = run_batch(worker_module, ["CURIE:x", "CURIE:y"])

    assert (task_id, corrections, unresolved) == (0, 1, 0)
    assert len(collection.bulk_write_calls[0]) == 2, "two requests were issued"
    assert collection.stored("drop") is None
    assert collection.duplicated_curies() == set()


def test_curie_repeated_within_a_document_is_trimmed(worker_module, monkeypatch):
    """
    A repeat alongside a distinct identifier. The comparison-matrix logic scored
    every identifier as differing from some other and skipped the document, so
    this was never pruned; its `else` arm was unreachable.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [identifier_document("d1", "CURIE:dupe", "CURIE:dupe", "CURIE:other")],
    )

    assert run_batch(worker_module, ["CURIE:dupe"]) == (0, 1, 0)
    assert collection.curies("d1") == ["CURIE:dupe", "CURIE:other"]
    assert collection.duplicated_curies() == set()


def test_document_without_a_repeated_curie_is_not_counted_unresolved(
    worker_module, monkeypatch
):
    """
    The shard counters record how often a CURIE was seen during upload, so a
    CURIE counted twice can legitimately land in one document once duplicate
    `_id` documents were merged. Nothing to trim is a resolution.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [identifier_document("d1", "CURIE:once", "CURIE:other")],
    )

    assert run_batch(worker_module, ["CURIE:once"]) == (0, 0, 0)
    assert collection.bulk_write_calls == []


def test_trim_does_not_clobber_a_document_changed_underneath_it(
    worker_module, monkeypatch
):
    """
    The trim pins the identifier array it read. Another worker repairing a
    different CURIE on the same document wins, and the trim applies nothing
    rather than reinstating the identifiers that worker removed.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [identifier_document("d1", "CURIE:dupe", "CURIE:dupe", "CURIE:other")],
    )
    original_find = collection.find

    def find_then_change(query):
        documents = list(original_find(query))
        # simulate a concurrent repair landing between the read and the write
        collection.stored("d1")["identifiers"] = [{"i": "CURIE:dupe", "l": ""}]
        return iter(documents)

    monkeypatch.setattr(collection, "find", find_then_change)

    assert run_batch(worker_module, ["CURIE:dupe"]) == (0, 0, 0)
    assert collection.curies("d1") == ["CURIE:dupe"]


def test_pull_that_would_empty_a_document_is_left_unresolved(
    worker_module, monkeypatch
):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document(
                "protein", "CURIE:shared", "CURIE:p", node_type="biolink:Protein"
            ),
            identifier_document(
                "small-molecule", "CURIE:shared", "CURIE:shared", "CURIE:shared"
            ),
        ],
    )

    assert run_batch(worker_module, ["CURIE:shared"]) == (0, 0, 1)
    assert collection.bulk_write_calls == []


def test_duplicate_cleanup_aggregates_and_returns_unresolved_totals(
    worker_module, monkeypatch
):
    batch_results = iter([(0, 2, 1), (1, 0, 3), (2, 5, 0)])
    results_lock = threading.Lock()

    def fake_batch_handler(task_id, curies, collection_name):
        del task_id, curies, collection_name
        with results_lock:
            return next(batch_results)

    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter(("a", "b", "c")),
    )
    monkeypatch.setattr(
        worker_module, "iter_n", lambda _curies, _size: (("a",), ("b",), ("c",))
    )
    monkeypatch.setattr(
        worker_module, "_curie_duplication_batch_handler", fake_batch_handler
    )

    assert worker_module.cleanup_curie_duplication("data", "collection") == (7, 4)


def test_duplicate_cleanup_still_propagates_real_errors(worker_module, monkeypatch):
    """An unresolvable CURIE is tolerated; a genuine failure is not."""
    bulk_write_error = RuntimeError("mongo bulk_write failed")

    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("keep", "CURIE:x", "CURIE:extra"),
            identifier_document("drop", "CURIE:x"),
        ],
    )

    def failing_bulk_write(_requests):
        raise bulk_write_error

    monkeypatch.setattr(collection, "bulk_write", failing_bulk_write)
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter((("CURIE:x",),)),
    )

    with pytest.raises(RuntimeError) as raised:
        worker_module.cleanup_curie_duplication("data", "collection")

    assert raised.value is bulk_write_error
