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

    # worker.py imports pymongo at module scope. The tests exercise the operation
    # values and collection behavior through the stateful fake below, so the
    # module itself can stay independent of a real MongoDB installation.
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
    static_module.DRUG_CHEMICAL_IDENTIFIER_FILES = {"Food.txt"}
    static_module.GENE_PROTEIN_IDENTIFER_FILES = set()
    static_module.IDENTIFIER_LOOKUP_DATABASE = "identifier.sqlite3"
    static_module.NODENORM_UPLOAD_CHUNK_OVERRIDES = {"Protein.txt": 7}
    monkeypatch.setitem(sys.modules, f"{package_name}.static", static_module)

    spec = importlib.util.spec_from_file_location(
        f"{package_name}.worker", module_dir / "worker.py"
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, f"{package_name}.worker", module)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def write_worker_release(data_folder, compendia):
    data_folder.mkdir(parents=True, exist_ok=True)
    conflations = ("DrugChemical.txt", "GeneProtein.txt")
    for filename in (*compendia, *conflations):
        (data_folder / filename).write_text("{}\n", encoding="utf-8")
    (data_folder / "duplicate_clique_leaders.tsv").write_text(
        "clique_leader\tfilenames\tbiolink_types\tclique_identifier_counts"
        "\tclique_leader_count\n",
        encoding="utf-8",
    )
    (data_folder / "release-manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "release": "2026jul22",
                "compendia": list(compendia),
                "conflations": list(conflations),
                "reports": ["reports/duckdb/duplicate_clique_leaders.tsv"],
            }
        ),
        encoding="utf-8",
    )


def test_offset_tasks_follow_manifest_inventory(worker_module, monkeypatch, tmp_path):
    compendia = ("CellLine.txt", "Food.txt", "FutureType.txt", "Protein.txt")
    write_worker_release(tmp_path, compendia)
    (tmp_path / "Protein.txt.00").write_text("split copy\n", encoding="utf-8")
    (tmp_path / "RemovedType.txt").write_text("stale copy\n", encoding="utf-8")

    analyzed = []

    def offsets(input_file, num_partitions):
        analyzed.append((Path(input_file).name, num_partitions))
        return [0, 10]

    monkeypatch.setattr(worker_module, "generate_file_offsets", offsets)

    tasks = list(worker_module._build_offset_tasks(tmp_path, "collection"))

    assert set(analyzed) == {
        ("CellLine.txt", 1),
        ("Food.txt", 1),
        ("FutureType.txt", 1),
        ("Protein.txt", 7),
    }
    assert {Path(task["input_file"]).name for task in tasks} == set(compendia)
    assert all(task["collection_name"] == "collection" for task in tasks)
    assert all(task["offset_start"] == 0 for task in tasks)
    assert all(task["offset_end"] == 10 for task in tasks)

    tasks_by_file = {Path(task["input_file"]).name: task for task in tasks}
    assert (
        tasks_by_file["Food.txt"]["conflation_database"]
        == (tmp_path / "conflation.sqlite3").resolve()
    )
    assert tasks_by_file["CellLine.txt"]["conflation_database"] is None


def test_offset_tasks_fail_before_analysis_without_manifest(
    worker_module, monkeypatch, tmp_path
):
    analyzed = []
    monkeypatch.setattr(
        worker_module,
        "generate_file_offsets",
        lambda *_args, **_kwargs: analyzed.append(True),
    )

    with pytest.raises(ValueError, match="Unable to read NodeNorm release manifest"):
        list(worker_module._build_offset_tasks(tmp_path, "collection"))

    assert analyzed == []


def test_offset_tasks_fail_before_analysis_when_manifest_file_is_missing(
    worker_module, monkeypatch, tmp_path
):
    write_worker_release(tmp_path, ("Food.txt", "CellLine.txt"))
    (tmp_path / "Food.txt").unlink()
    analyzed = []
    monkeypatch.setattr(
        worker_module,
        "generate_file_offsets",
        lambda *_args, **_kwargs: analyzed.append(True),
    )

    with pytest.raises(ValueError, match="missing: Food.txt"):
        list(worker_module._build_offset_tasks(tmp_path, "collection"))

    assert analyzed == []


def test_missing_duplicate_report_fails_before_upload_work(
    worker_module, monkeypatch, tmp_path
):
    write_worker_release(tmp_path, ("Food.txt", "CellLine.txt"))
    (tmp_path / "duplicate_clique_leaders.tsv").unlink()
    calls = []
    monkeypatch.setattr(
        worker_module,
        "_configure_sqlite_tmpdir",
        lambda: calls.append("configure-sqlite"),
    )

    with pytest.raises(ValueError, match="duplicate_clique_leaders.tsv"):
        worker_module.upload_process(tmp_path, "collection")

    assert calls == []


class FakeBulkWriteResult:
    def __init__(self, deleted_count, modified_count, matched_count):
        self.deleted_count = deleted_count
        self.modified_count = modified_count
        self.matched_count = matched_count


class FakeMongoCursor:
    def __init__(self, documents):
        self.documents = documents

    def __iter__(self):
        return iter(self.documents)

    def limit(self, count):
        self.documents = self.documents[:count]
        return self


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

    def find(self, query, projection=None):
        if "_id" in query:
            document_ids = set(query["_id"]["$in"])
            documents = [
                copy.deepcopy(document)
                for document in self.documents
                if document["_id"] in document_ids
            ]
            if query.get("identifiers") == []:
                documents = [
                    document for document in documents if not document["identifiers"]
                ]
        else:
            curie = query["identifiers.i"]
            documents = [
                copy.deepcopy(document)
                for document in self.documents
                if curie in [identifier["i"] for identifier in document["identifiers"]]
            ]
        if projection is not None:
            include_identifiers = (
                "identifiers" in projection or "identifiers.i" in projection
            )
            documents = [
                {
                    "_id": document["_id"],
                    **(
                        {
                            "identifiers": [
                                {"i": identifier["i"]}
                                for identifier in document["identifiers"]
                            ]
                        }
                        if include_identifiers
                        else {}
                    ),
                }
                for document in documents
            ]
        return FakeMongoCursor(documents)

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

    @staticmethod
    def matches(document, query):
        """
        Evaluate the two `identifiers` predicates the cleanup relies on: an exact
        array for the compare-and-swap trim, and `$elemMatch` with `$nin` for the
        pull's "must leave something behind" guard.
        """
        condition = query.get("identifiers")
        if condition is None:
            return True
        if isinstance(condition, list):
            return condition == document["identifiers"]
        excluded = set(condition["$elemMatch"]["i"]["$nin"])
        return any(
            identifier["i"] not in excluded for identifier in document["identifiers"]
        )

    def bulk_write(self, requests):
        self.bulk_write_calls.append(list(requests))
        deleted_count = 0
        modified_count = 0
        matched_count = 0
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
            if document is None or not self.matches(document, query):
                continue
            matched_count += 1

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

        return FakeBulkWriteResult(deleted_count, modified_count, matched_count)


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


class FakeMongoInsertCollection(FakeMongoCollection):
    """Apply unordered inserts, including MongoDB's partial-write behavior."""

    def __init__(self, worker_module):
        super().__init__()
        self.error_type = worker_module.pymongo.errors.BulkWriteError
        self.insert_calls = []
        self.last_error = None

    def insert_many(self, documents, ordered):
        assert ordered is False
        self.insert_calls.append(copy.deepcopy(documents))
        errors = []
        for index, document in enumerate(documents):
            if self.stored(document["_id"]) is None:
                self.documents.append(copy.deepcopy(document))
            else:
                errors.append(
                    {
                        "index": index,
                        "code": 11000,
                        "keyPattern": {"_id": 1},
                        "keyValue": {"_id": document["_id"]},
                        "op": copy.deepcopy(document),
                    }
                )
        if errors:
            self.last_error = self.error_type(
                {"writeErrors": errors, "writeConcernErrors": []}
            )
            raise self.last_error


def install_insert_collection(worker_module, monkeypatch):
    collection = FakeMongoInsertCollection(worker_module)
    monkeypatch.setattr(
        worker_module.pymongo.collection,
        "Collection",
        lambda database, name: collection,
    )
    queued_identifiers = []
    monkeypatch.setattr(
        worker_module,
        "_queue_identifier_batch",
        lambda identifiers: queued_identifiers.extend(identifiers),
    )
    return collection, queued_identifiers


def source_clique(*curies, node_type="biolink:Disease", label="source label"):
    return {
        "type": node_type,
        "ic": "7.25",
        "preferred_name": label,
        "identifiers": [{"i": curie, "l": label} for curie in curies],
        "taxa": ["NCBITaxon:9606"],
    }


def upload_source_cliques(
    worker_module,
    tmp_path,
    filename,
    documents,
    duplicate_clique_leaders=None,
    **kwargs,
):
    input_file = tmp_path / filename
    input_file.write_text(
        "".join(json.dumps(document) + "\n" for document in documents),
        encoding="utf-8",
    )
    return worker_module.subset_upload_worker(
        input_file=input_file,
        buffer_size=2,
        offset_start=0,
        offset_end=input_file.stat().st_size,
        collection_name="collection",
        duplicate_clique_leaders=duplicate_clique_leaders or {},
        **kwargs,
    )


def enriched_source_clique(document):
    expected = copy.deepcopy(document)
    expected["_id"] = expected["identifiers"][0]["i"]
    expected["ic"] = float(expected["ic"])
    for identifier in expected["identifiers"]:
        identifier["c"] = {"gp": None, "dc": None}
    return expected


@pytest.mark.parametrize(
    "load_order",
    [
        *itertools.permutations(("AnatomicalEntity.txt", "ChemicalEntity.txt")),
        *itertools.permutations(
            ("AnatomicalEntity.txt", "ChemicalEntity.txt", "Protein.txt")
        ),
    ],
)
def test_duplicate_leaders_retain_one_whole_source_in_every_load_order(
    worker_module, monkeypatch, tmp_path, load_order
):
    """A smaller winner is intentional: clique size is not biological authority."""
    candidates = {
        "AnatomicalEntity.txt": source_clique(
            "MESH:leader", node_type="biolink:AnatomicalEntity", label="anatomy"
        ),
        "ChemicalEntity.txt": source_clique(
            "MESH:leader",
            "CHEBI:other",
            node_type="biolink:ChemicalEntity",
            label="chemical",
        ),
        "Protein.txt": source_clique(
            "MESH:leader",
            "UniProtKB:other",
            "NCBIGene:other",
            node_type="biolink:Protein",
            label="protein",
        ),
    }
    collection, queued_identifiers = install_insert_collection(
        worker_module, monkeypatch
    )
    declared_sources = tuple(sorted(load_order))
    report = {"MESH:leader": declared_sources}

    results = [
        upload_source_cliques(
            worker_module, tmp_path, filename, [candidates[filename]], report
        )
        for filename in load_order
    ]

    assert collection.documents == [
        enriched_source_clique(candidates[declared_sources[0]])
    ]
    assert sum(result.identifier_count for result in results) == 1
    assert [result.duplicate_leader_counts for result in results] == [
        {("MESH:leader", filename): 1} for filename in load_order
    ]
    assert queued_identifiers == ["MESH:leader"]
    assert len(collection.insert_calls) == 1
    assert collection.bulk_write_calls == []


def test_duplicate_leaders_with_same_type_do_not_merge_aliases_or_metadata(
    worker_module, monkeypatch, tmp_path
):
    collection, queued_identifiers = install_insert_collection(
        worker_module, monkeypatch
    )
    winner = source_clique("EX:leader", "EX:kept", label="winner")
    loser = source_clique("EX:leader", "EX:discarded", label="loser")
    loser["taxa"] = ["NCBITaxon:10090"]
    report = {"EX:leader": ("A.txt", "B.txt")}

    assert (
        upload_source_cliques(
            worker_module, tmp_path, "B.txt", [loser], report
        ).identifier_count
        == 0
    )
    assert (
        upload_source_cliques(
            worker_module, tmp_path, "A.txt", [winner], report
        ).identifier_count
        == 2
    )

    assert collection.documents == [enriched_source_clique(winner)]
    assert queued_identifiers == ["EX:leader", "EX:kept"]


def test_discarded_identifiers_never_reach_accounting_or_conflation(
    worker_module, monkeypatch, tmp_path
):
    collection, queued_identifiers = install_insert_collection(
        worker_module, monkeypatch
    )
    monkeypatch.setattr(worker_module, "NODENORM_IDENTIFIER_BATCH_SIZE", 1)
    conflated_leaders = []

    def record_conflations(buffer, leaders, _connection):
        conflated_leaders.extend(leaders)
        return buffer

    monkeypatch.setattr(
        worker_module, "_update_buffer_with_conflations", record_conflations
    )
    report = {"EX:leader": ("A.txt", "B.txt", "C.txt")}
    retained_count = 0
    for filename, document in (
        ("B.txt", source_clique("EX:leader", "EX:discarded")),
        ("A.txt", source_clique("EX:leader")),
        ("C.txt", source_clique("EX:leader", "EX:discarded")),
    ):
        retained_count += upload_source_cliques(
            worker_module,
            tmp_path,
            filename,
            [document],
            report,
            conflation_database=":memory:",
        ).identifier_count

    assert retained_count == 1
    assert queued_identifiers == ["EX:leader"]
    assert conflated_leaders == ["EX:leader"]
    assert collection.curies("EX:leader") == ["EX:leader"]


@pytest.mark.parametrize(
    "invalid_type",
    [None, "", "   ", [], ["biolink:Protein"], 42, {}],
)
@pytest.mark.parametrize("filename", ["A.txt", "B.txt"])
def test_invalid_source_types_fail_before_accounting_even_for_losing_cliques(
    worker_module, monkeypatch, tmp_path, invalid_type, filename
):
    collection, queued_identifiers = install_insert_collection(
        worker_module, monkeypatch
    )
    monkeypatch.setattr(worker_module, "NODENORM_IDENTIFIER_BATCH_SIZE", 1)

    with pytest.raises(ValueError, match="type"):
        upload_source_cliques(
            worker_module,
            tmp_path,
            filename,
            [source_clique("EX:leader", node_type=invalid_type)],
            {"EX:leader": ("A.txt", "B.txt")},
        )

    assert queued_identifiers == []
    assert collection.documents == []
    assert collection.insert_calls == []


def test_missing_source_type_fails_before_accounting(
    worker_module, monkeypatch, tmp_path
):
    collection, queued_identifiers = install_insert_collection(
        worker_module, monkeypatch
    )
    document = source_clique("EX:leader")
    del document["type"]

    with pytest.raises(ValueError, match="type"):
        upload_source_cliques(worker_module, tmp_path, "A.txt", [document])

    assert queued_identifiers == []
    assert collection.documents == []


def test_reported_leader_in_undeclared_source_fails_before_accounting(
    worker_module, monkeypatch, tmp_path
):
    collection, queued_identifiers = install_insert_collection(
        worker_module, monkeypatch
    )

    with pytest.raises(ValueError, match="EX:leader"):
        upload_source_cliques(
            worker_module,
            tmp_path,
            "Unexpected.txt",
            [source_clique("EX:leader")],
            {"EX:leader": ("A.txt", "B.txt")},
        )

    assert queued_identifiers == []
    assert collection.documents == []


def test_duplicate_report_reconciliation_accepts_exact_source_observations(
    worker_module,
):
    worker_module._validate_duplicate_leader_counts(
        {"EX:leader": ("A.txt", "B.txt")},
        {("EX:leader", "A.txt"): 1, ("EX:leader", "B.txt"): 1},
    )
    worker_module._validate_duplicate_leader_counts({}, {})


@pytest.mark.parametrize(
    "observations",
    [
        pytest.param({("EX:leader", "B.txt"): 1}, id="winner-missing"),
        pytest.param({("EX:leader", "A.txt"): 1}, id="loser-missing"),
        pytest.param(
            {("EX:leader", "A.txt"): 1, ("EX:leader", "B.txt"): 2},
            id="loser-repeated",
        ),
    ],
)
def test_duplicate_report_reconciliation_blocks_missing_or_repeated_sources(
    worker_module, observations
):
    with pytest.raises(
        worker_module.NodeNormCollectionValidationError, match="EX:leader"
    ):
        worker_module._validate_duplicate_leader_counts(
            {"EX:leader": ("A.txt", "B.txt")}, observations
        )


def test_repeated_discarded_clique_is_observed_and_blocks_promotion(
    worker_module, monkeypatch, tmp_path
):
    collection, queued_identifiers = install_insert_collection(
        worker_module, monkeypatch
    )
    report = {"EX:leader": ("A.txt", "B.txt")}
    result = upload_source_cliques(
        worker_module,
        tmp_path,
        "B.txt",
        [source_clique("EX:leader"), source_clique("EX:leader", "EX:other")],
        report,
    )

    assert result.identifier_count == 0
    assert result.duplicate_leader_counts == {("EX:leader", "B.txt"): 2}
    assert queued_identifiers == []
    assert collection.insert_calls == []
    with pytest.raises(worker_module.NodeNormCollectionValidationError):
        worker_module._validate_duplicate_leader_counts(
            report, {("EX:leader", "A.txt"): 1, **result.duplicate_leader_counts}
        )


@pytest.mark.parametrize("complete_report", [False, True])
def test_upload_reconciles_duplicate_report_before_promotion_even_with_audit_off(
    worker_module, monkeypatch, complete_report
):
    report = {"EX:leader": ("A.txt", "B.txt")}
    observed = {("EX:leader", "A.txt"): 1}
    if complete_report:
        observed[("EX:leader", "B.txt")] = 1

    class FakeFuture:
        def result(self):
            return worker_module.UploadPartitionResult(1, observed)

        def cancel(self):
            return False

    class FakeExecutor:
        def __init__(self, **kwargs):
            assert kwargs["initargs"][2] == report

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, *_args, **_kwargs):
            return FakeFuture()

    monkeypatch.setattr(
        worker_module.config, "NODENORM_CURIE_VALIDATION_MODE", "off", raising=False
    )
    monkeypatch.setattr(worker_module, "_configure_sqlite_tmpdir", lambda: None)
    monkeypatch.setattr(worker_module, "create_identifiers_table", lambda _data: None)
    monkeypatch.setattr(worker_module, "NODENORM_IDENTIFIER_SHARD_COUNT", 0)
    monkeypatch.setattr(worker_module, "_identifier_database_paths", lambda _data: ())
    monkeypatch.setattr(
        worker_module.multiprocessing,
        "get_context",
        lambda _kind: types.SimpleNamespace(Event=threading.Event),
    )
    monkeypatch.setattr(
        worker_module.concurrent.futures, "ProcessPoolExecutor", FakeExecutor
    )
    monkeypatch.setattr(
        worker_module.concurrent.futures,
        "as_completed",
        lambda futures: iter(tuple(futures)),
    )
    monkeypatch.setattr(
        worker_module, "local_compendium_paths", lambda _data: (Path("A.txt"),)
    )
    monkeypatch.setattr(
        worker_module, "read_duplicate_clique_leaders", lambda _data: report
    )
    monkeypatch.setattr(
        worker_module,
        "_build_offset_tasks",
        lambda *_args, **_kwargs: iter(({},)),
    )
    promotion_calls = []
    monkeypatch.setattr(
        worker_module,
        "_prepare_collection_for_promotion",
        lambda *args, **kwargs: promotion_calls.append((args, kwargs)),
    )

    if complete_report:
        assert worker_module.upload_process("data", "collection") == 1
        assert promotion_calls == [(("data", "collection"), {"validation_mode": "off"})]
    else:
        with pytest.raises(worker_module.NodeNormCollectionValidationError):
            worker_module.upload_process("data", "collection")
        assert promotion_calls == []


@pytest.mark.parametrize("declared", [False, True])
def test_repeated_leader_inside_one_source_always_fails(
    worker_module, monkeypatch, tmp_path, declared
):
    collection, _ = install_insert_collection(worker_module, monkeypatch)
    report = {"EX:leader": ("A.txt", "B.txt")} if declared else {}
    first = source_clique("EX:leader", label="first")
    second = source_clique("EX:leader", "EX:other", label="second")

    with pytest.raises(worker_module.pymongo.errors.BulkWriteError) as raised:
        upload_source_cliques(worker_module, tmp_path, "A.txt", [first, second], report)

    assert raised.value is collection.last_error
    assert collection.documents == [enriched_source_clique(first)]
    assert collection.bulk_write_calls == []


def test_unlisted_cross_source_leader_collision_fails_without_modifying_winner(
    worker_module, monkeypatch, tmp_path
):
    collection, _ = install_insert_collection(worker_module, monkeypatch)
    first = source_clique("EX:leader", node_type="biolink:Protein")
    upload_source_cliques(worker_module, tmp_path, "A.txt", [first], {})

    with pytest.raises(worker_module.pymongo.errors.BulkWriteError) as raised:
        upload_source_cliques(
            worker_module,
            tmp_path,
            "B.txt",
            [source_clique("EX:leader", "EX:other")],
            {},
        )

    assert raised.value is collection.last_error
    assert collection.documents == [enriched_source_clique(first)]
    assert collection.bulk_write_calls == []


@pytest.mark.parametrize(
    "details",
    [
        pytest.param(
            {"writeErrors": [{"code": 11000, "keyPattern": {"_id": 1}}]},
            id="unexpected-id-duplicate",
        ),
        pytest.param(
            {"writeErrors": [{"code": 11000, "keyPattern": {"other_unique": 1}}]},
            id="other-unique-index",
        ),
        pytest.param(
            {"writeErrors": [{"code": 11000}, {"code": 121}]},
            id="duplicate-and-document-validation",
        ),
        pytest.param(
            {"writeErrors": [], "writeConcernErrors": [{"code": 64}]},
            id="write-concern-only",
        ),
        pytest.param(
            {"writeErrors": [{"code": 11000}], "writeConcernErrors": [{"code": 64}]},
            id="duplicate-and-write-concern",
        ),
    ],
)
def test_upload_propagates_every_bulk_write_error_unchanged(
    worker_module, tmp_path, details
):
    error = worker_module.pymongo.errors.BulkWriteError(copy.deepcopy(details))

    def fail_insert(_documents, ordered):
        assert ordered is False
        raise error

    collection = types.SimpleNamespace(insert_many=fail_insert)
    document = enriched_source_clique(source_clique("EX:leader"))
    original = copy.deepcopy(document)

    with pytest.raises(worker_module.pymongo.errors.BulkWriteError) as raised:
        worker_module._upload_buffer(collection, [document], tmp_path / "A.txt", 1)

    assert raised.value is error
    assert document == original
    assert error.details == details


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


def test_duplicate_cleanup_failure_is_propagated_before_later_batches(
    worker_module, monkeypatch
):
    cleanup_error = RuntimeError("duplicate cleanup failed")
    handled_tasks = []

    def fail_first_batch(task_id, curies, collection_name):
        del curies, collection_name
        handled_tasks.append(task_id)
        raise cleanup_error

    monkeypatch.setattr(
        worker_module,
        "_curie_duplication_batch_handler",
        fail_first_batch,
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
    assert handled_tasks == [0]


@pytest.mark.parametrize("validation_mode", ["report", "strict"])
def test_clean_collection_is_validated_and_promoted(
    worker_module, monkeypatch, validation_mode
):
    calls = []
    monkeypatch.setattr(
        worker_module,
        "create_mongo_identifiers_index",
        lambda collection: calls.append(("mongo-index", collection)),
    )
    monkeypatch.setattr(
        worker_module,
        "create_identifiers_index",
        lambda data: calls.append(("sqlite-index", data)),
    )
    monkeypatch.setattr(
        worker_module,
        "cleanup_curie_duplication",
        lambda data, collection: (
            calls.append(("cleanup", data, collection)) or (2, 0)
        ),
    )
    monkeypatch.setattr(
        worker_module,
        "validate_curie_uniqueness",
        lambda data, collection: (
            calls.append(("validate", data, collection))
            or worker_module.CurieValidationReport(0, 0, 0, 0, ())
        ),
    )

    worker_module._prepare_collection_for_promotion(
        "data", "collection", validation_mode=validation_mode
    )

    assert calls == [
        ("mongo-index", "collection"),
        ("sqlite-index", "data"),
        ("cleanup", "data", "collection"),
        ("validate", "data", "collection"),
    ]


def test_off_mode_cleans_without_running_uniqueness_audit(worker_module, monkeypatch):
    calls = []
    monkeypatch.setattr(
        worker_module,
        "create_mongo_identifiers_index",
        lambda collection: calls.append(("mongo-index", collection)),
    )
    monkeypatch.setattr(
        worker_module,
        "create_identifiers_index",
        lambda data: calls.append(("sqlite-index", data)),
    )
    monkeypatch.setattr(
        worker_module,
        "cleanup_curie_duplication",
        lambda data, collection: (
            calls.append(("cleanup", data, collection)) or (2, 0)
        ),
    )
    monkeypatch.setattr(
        worker_module,
        "validate_curie_uniqueness",
        lambda _data, _collection: pytest.fail("off mode must not run the audit"),
    )

    worker_module._prepare_collection_for_promotion(
        "data", "collection", validation_mode="off"
    )

    assert calls == [
        ("mongo-index", "collection"),
        ("sqlite-index", "data"),
        ("cleanup", "data", "collection"),
    ]


def test_validation_mode_defaults_to_off(worker_module):
    assert worker_module._curie_validation_mode() == "off"


def test_strict_mode_blocks_a_reported_uniqueness_violation(worker_module, monkeypatch):
    calls = []
    monkeypatch.setattr(
        worker_module,
        "create_mongo_identifiers_index",
        lambda collection: calls.append(("mongo-index", collection)),
    )
    monkeypatch.setattr(
        worker_module,
        "create_identifiers_index",
        lambda data: calls.append(("sqlite-index", data)),
    )
    monkeypatch.setattr(
        worker_module,
        "cleanup_curie_duplication",
        lambda data, collection: (
            calls.append(("cleanup", data, collection)) or (0, 1)
        ),
    )
    monkeypatch.setattr(
        worker_module,
        "validate_curie_uniqueness",
        lambda data, collection: (
            calls.append(("validate", data, collection))
            or worker_module.CurieValidationReport(
                candidate_count=1,
                missing_count=0,
                multiple_document_count=1,
                repeated_in_document_count=0,
                samples=("'CURIE:x' (documents=at least 2)",),
            )
        ),
    )

    with pytest.raises(worker_module.NodeNormCollectionValidationError) as raised:
        worker_module._prepare_collection_for_promotion(
            "data", "collection", validation_mode="strict"
        )

    assert "1 violation(s)" in str(raised.value)
    assert calls == [
        ("mongo-index", "collection"),
        ("sqlite-index", "data"),
        ("cleanup", "data", "collection"),
        ("validate", "data", "collection"),
    ]


def test_report_mode_allows_a_reported_uniqueness_violation(
    worker_module, monkeypatch, caplog
):
    monkeypatch.setattr(
        worker_module, "create_mongo_identifiers_index", lambda _collection: None
    )
    monkeypatch.setattr(worker_module, "create_identifiers_index", lambda _data: None)
    monkeypatch.setattr(
        worker_module,
        "cleanup_curie_duplication",
        lambda _data, _collection: (0, 1),
    )
    monkeypatch.setattr(
        worker_module,
        "validate_curie_uniqueness",
        lambda _data, _collection: worker_module.CurieValidationReport(
            candidate_count=1,
            missing_count=0,
            multiple_document_count=1,
            repeated_in_document_count=0,
            samples=("'CURIE:x' (documents=at least 2)",),
        ),
    )

    with caplog.at_level(logging.WARNING):
        worker_module._prepare_collection_for_promotion(
            "data", "collection", validation_mode="report"
        )

    assert "Promotion remains allowed because validation mode is report" in caplog.text


@pytest.mark.parametrize(
    ("validation_mode", "should_raise"),
    [
        pytest.param("report", False, id="report-continues"),
        pytest.param("strict", True, id="strict-blocks"),
    ],
)
def test_exhausted_mongo_audit_is_nonblocking_only_in_report_mode(
    worker_module, monkeypatch, validation_mode, should_raise
):
    monkeypatch.setattr(
        worker_module, "create_mongo_identifiers_index", lambda _collection: None
    )
    monkeypatch.setattr(worker_module, "create_identifiers_index", lambda _data: None)
    monkeypatch.setattr(
        worker_module,
        "cleanup_curie_duplication",
        lambda _data, _collection: (0, 0),
    )

    monkeypatch.setattr(
        worker_module,
        "validate_curie_uniqueness",
        lambda _data, _collection: worker_module.CurieValidationReport(
            candidate_count=9_900_000,
            missing_count=2,
            multiple_document_count=3,
            repeated_in_document_count=4,
            samples=("partial sample",),
            complete=False,
        ),
    )

    if should_raise:
        with pytest.raises(worker_module.NodeNormCollectionValidationError) as raised:
            worker_module._prepare_collection_for_promotion(
                "data", "collection", validation_mode=validation_mode
            )
        assert "Audit incomplete" in str(raised.value)
    else:
        worker_module._prepare_collection_for_promotion(
            "data", "collection", validation_mode=validation_mode
        )


def test_invalid_validation_mode_fails_before_upload_work(worker_module, monkeypatch):
    calls = []
    monkeypatch.setattr(
        worker_module.config, "NODENORM_CURIE_VALIDATION_MODE", "typo", raising=False
    )
    monkeypatch.setattr(
        worker_module,
        "_configure_sqlite_tmpdir",
        lambda: calls.append("configure-sqlite"),
    )

    with pytest.raises(ValueError, match="NODENORM_CURIE_VALIDATION_MODE"):
        worker_module.upload_process("data", "collection")

    assert calls == []


def test_missing_manifest_fails_before_upload_work(
    worker_module, monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(
        worker_module,
        "_configure_sqlite_tmpdir",
        lambda: calls.append("configure-sqlite"),
    )
    monkeypatch.setattr(
        worker_module,
        "create_identifiers_table",
        lambda _path: calls.append("create-identifiers"),
    )

    with pytest.raises(ValueError, match="Unable to read NodeNorm release manifest"):
        worker_module.upload_process(tmp_path, "collection")

    assert calls == []


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
        lambda _data_folder, _collection, input_files=None: iter(({},)),
    )
    monkeypatch.setattr(
        worker_module,
        "local_compendium_paths",
        lambda _data_folder: (Path("input.txt"),),
    )
    monkeypatch.setattr(
        worker_module, "read_duplicate_clique_leaders", lambda _data_folder: {}
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


def test_merged_protein_type_list_is_recognized(worker_module, monkeypatch):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document(
                "protein",
                "CURIE:shared",
                "CURIE:p",
                node_type=["biolink:Protein", "biolink:Gene"],
            ),
            identifier_document("side", "CURIE:shared", "CURIE:s"),
        ],
    )

    assert run_batch(worker_module, ["CURIE:shared"]) == (0, 1, 0)
    assert collection.curies("side") == ["CURIE:s"]
    assert collection.duplicated_curies() == set()


def test_pair_order_uses_unique_curie_count_not_repeated_entries(
    worker_module, monkeypatch
):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("a", "CURIE:x", "CURIE:x"),
            identifier_document("b", "CURIE:x", "CURIE:y"),
        ],
    )

    assert run_batch(worker_module, ["CURIE:x"]) == (0, 1, 0)
    assert collection.stored("a") is None
    assert collection.curies("b") == ["CURIE:x", "CURIE:y"]
    assert collection.duplicated_curies() == set()


def test_equal_curie_sets_prefer_deduplicated_survivor(worker_module, monkeypatch):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("a", "CURIE:x", "CURIE:x"),
            identifier_document("z", "CURIE:x"),
        ],
    )

    assert run_batch(worker_module, ["CURIE:x"]) == (0, 1, 0)
    assert collection.stored("a") is None
    assert collection.curies("z") == ["CURIE:x"]
    assert collection.duplicated_curies() == set()


def test_pair_repair_deduplicates_its_survivor(worker_module, monkeypatch):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("super", "CURIE:x", "CURIE:x", "CURIE:y"),
            identifier_document("sub", "CURIE:x"),
        ],
    )

    assert run_batch(worker_module, ["CURIE:x"]) == (0, 2, 0)
    assert collection.curies("super") == ["CURIE:x", "CURIE:y"]
    assert collection.stored("sub") is None
    assert collection.duplicated_curies() == set()


def test_protein_owns_shared_curie_when_it_is_the_smaller_clique(
    worker_module, monkeypatch
):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("protein", "CURIE:x", node_type="biolink:Protein"),
            identifier_document("non-protein", "CURIE:x", "CURIE:y"),
        ],
    )

    assert run_batch(worker_module, ["CURIE:x"]) == (0, 1, 0)
    assert collection.curies("protein") == ["CURIE:x"]
    assert collection.curies("non-protein") == ["CURIE:y"]
    assert collection.duplicated_curies() == set()


def test_protein_wins_equal_curie_set_tie(worker_module, monkeypatch):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("z-protein", "CURIE:x", node_type="biolink:Protein"),
            identifier_document("a-non-protein", "CURIE:x"),
        ],
    )

    assert run_batch(worker_module, ["CURIE:x"]) == (0, 1, 0)
    assert collection.curies("z-protein") == ["CURIE:x"]
    assert collection.stored("a-non-protein") is None
    assert collection.duplicated_curies() == set()


def test_two_shared_curies_report_one_correction(worker_module, monkeypatch):
    """
    The first CURIE deletes the subset document. Re-reading for the second CURIE
    finds that its duplicate is already resolved, so only one write is needed.
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
    assert [len(requests) for requests in collection.bulk_write_calls] == [1]
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


def test_multiple_repeated_curies_coalesce_to_one_trim(worker_module, monkeypatch):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [identifier_document("d1", "CURIE:x", "CURIE:x", "CURIE:y", "CURIE:y")],
    )

    assert run_batch(worker_module, ["CURIE:x", "CURIE:y"]) == (0, 1, 0)
    assert collection.curies("d1") == ["CURIE:x", "CURIE:y"]
    assert len(collection.bulk_write_calls[0]) == 1
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

    The concurrent change has already removed the repeat. A CAS miss therefore
    cannot by itself be called unresolved; the final collection validator checks
    the resulting CURIE cardinality directly.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [identifier_document("d1", "CURIE:dupe", "CURIE:dupe", "CURIE:other")],
    )
    original_find = collection.find

    def find_then_change(query, projection=None):
        if "_id" in query:
            return original_find(query, projection)
        documents = list(original_find(query, projection))
        # simulate a concurrent repair landing between the read and the write
        collection.stored("d1")["identifiers"] = [{"i": "CURIE:dupe", "l": ""}]
        return iter(documents)

    monkeypatch.setattr(collection, "find", find_then_change)

    assert run_batch(worker_module, ["CURIE:dupe"]) == (0, 0, 0)
    assert collection.curies("d1") == ["CURIE:dupe"]


@pytest.mark.parametrize(
    "curies",
    [
        pytest.param(["CURIE:x", "CURIE:y"], id="x-then-y"),
        pytest.param(["CURIE:y", "CURIE:x"], id="y-then-x"),
    ],
)
def test_dependent_repairs_re_read_between_curies(worker_module, monkeypatch, curies):
    """
    One document sharing a different CURIE with each of two Protein cliques.

    Planning both repairs from one snapshot queues two pulls. MongoDB declines the
    second pull to avoid emptying the shared document, which leaves that CURIE
    duplicated. Applying the first repair before reading the second makes the
    second decision from current state, so it deletes the now-redundant document.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("side", "CURIE:x", "CURIE:y"),
            identifier_document("px", "CURIE:x", node_type="biolink:Protein"),
            identifier_document("py", "CURIE:y", node_type="biolink:Protein"),
        ],
    )

    task_id, corrections, unresolved = run_batch(worker_module, curies)

    assert collection.stored("side") is None
    assert (task_id, corrections, unresolved) == (0, 2, 0)
    assert [len(requests) for requests in collection.bulk_write_calls] == [1, 1]
    assert collection.duplicated_curies() == set()


def test_repair_rejects_an_emptied_surviving_document(worker_module, monkeypatch):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document(
                "protein", "CURIE:x", "CURIE:p", node_type="biolink:Protein"
            ),
            identifier_document("side", "CURIE:x", "CURIE:side"),
        ],
    )
    actual_bulk_write = collection.bulk_write

    def bulk_write_then_corrupt(requests):
        result = actual_bulk_write(requests)
        collection.stored("side")["identifiers"] = []
        return result

    monkeypatch.setattr(collection, "bulk_write", bulk_write_then_corrupt)

    with pytest.raises(worker_module.NodeNormCollectionValidationError) as raised:
        run_batch(worker_module, ["CURIE:x"])

    assert "document(s) with no identifiers" in str(raised.value)


def test_equal_documents_do_not_delete_each_other(worker_module, monkeypatch):
    """
    Two documents with identical CURIE sets, where find() returns them in opposite
    orders for the two CURIEs they share -- which it is free to do, since the query
    is unsorted.

    Choosing the survivor by position made each CURIE nominate the other's keeper
    and deleted both, losing the CURIEs entirely. Ordering on the identifier count
    with the _id as tiebreak makes both CURIEs agree, so the two deletes collapse
    onto one document.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("a", "CURIE:x", "CURIE:y"),
            identifier_document("b", "CURIE:x", "CURIE:y"),
        ],
    )
    unsorted_find = collection.find

    def find_in_opposite_orders(query, projection=None):
        if "_id" in query:
            return unsorted_find(query, projection)
        documents = list(unsorted_find(query, projection))
        if query["identifiers.i"] == "CURIE:y":
            documents.reverse()
        return iter(documents)

    monkeypatch.setattr(collection, "find", find_in_opposite_orders)

    task_id, corrections, unresolved = run_batch(worker_module, ["CURIE:x", "CURIE:y"])

    survivors = [document["_id"] for document in collection.documents]
    assert survivors == ["a"], "the keeper was deleted too"
    assert (task_id, corrections, unresolved) == (0, 1, 0)
    assert collection.duplicated_curies() == set()


def test_stale_subset_delete_from_another_worker_applies_once(
    worker_module, monkeypatch
):
    """
    Two workers resolving the two CURIEs a pair shares, the second reading a
    snapshot taken before the first worker's delete landed. Both nominate the same
    document because the choice no longer depends on query order, so the second
    delete matches nothing and the keeper survives.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("keep", "CURIE:x", "CURIE:y", "CURIE:extra"),
            identifier_document("drop", "CURIE:x", "CURIE:y"),
        ],
    )
    stale_documents = [copy.deepcopy(document) for document in collection.documents]
    current_find = collection.find

    # first worker resolves CURIE:x and its delete lands
    assert run_batch(worker_module, ["CURIE:x"]) == (0, 1, 0)
    assert collection.stored("drop") is None

    # second worker resolves CURIE:y from the snapshot it read beforehand
    def find_stale_pair(query, projection=None):
        if "_id" in query:
            return current_find(query, projection)
        return iter([copy.deepcopy(d) for d in stale_documents])

    monkeypatch.setattr(collection, "find", find_stale_pair)

    task_id, corrections, unresolved = run_batch(worker_module, ["CURIE:y"], task_id=1)

    assert (task_id, corrections, unresolved) == (1, 0, 0)
    assert [document["_id"] for document in collection.documents] == ["keep"]


def test_serial_cleanup_prevents_subset_owner_reversal(worker_module, monkeypatch):
    """
    A delayed subset delete can become unsafe after another repair shrinks its
    keeper and reverses the later ownership decision. Cleanup therefore completes
    each batch before the next one reads the temporary collection.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("z-keeper", "CURIE:x", "CURIE:y", "CURIE:z"),
            identifier_document("a-subset", "CURIE:x", "CURIE:y"),
            identifier_document(
                "protein", "CURIE:z", "CURIE:p", node_type="biolink:Protein"
            ),
        ],
    )
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter((("CURIE:x",), ("CURIE:z",), ("CURIE:y",))),
    )
    monkeypatch.setattr(
        worker_module,
        "iter_n",
        lambda identifiers, _size: ([identifier] for identifier in identifiers),
    )

    assert worker_module.cleanup_curie_duplication("data", "collection") == (2, 0)
    assert collection.curies("z-keeper") == ["CURIE:x", "CURIE:y"]
    assert collection.stored("a-subset") is None
    assert collection.curies("protein") == ["CURIE:z", "CURIE:p"]
    assert collection.duplicated_curies() == set()


def test_serial_cleanup_prevents_pull_owner_reversal(worker_module, monkeypatch):
    """
    Mutable identifier counts can also make two stale Protein-pair decisions pull
    the same CURIEs from opposite documents. Serial batches keep one ownership
    decision visible to every later read.
    """
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document(
                "z-a",
                "CURIE:x",
                "CURIE:y",
                "CURIE:z",
                "CURIE:a-only",
                node_type="biolink:Protein",
            ),
            identifier_document(
                "a-b",
                "CURIE:x",
                "CURIE:y",
                "CURIE:b-only",
                node_type="biolink:Protein",
            ),
            identifier_document(
                "c",
                "CURIE:z",
                "CURIE:c1",
                "CURIE:c2",
                "CURIE:c3",
                "CURIE:c4",
                node_type="biolink:Protein",
            ),
        ],
    )
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter((("CURIE:x",), ("CURIE:z",), ("CURIE:y",))),
    )
    monkeypatch.setattr(
        worker_module,
        "iter_n",
        lambda identifiers, _size: ([identifier] for identifier in identifiers),
    )

    assert worker_module.cleanup_curie_duplication("data", "collection") == (2, 0)
    assert collection.curies("z-a") == ["CURIE:x", "CURIE:y", "CURIE:a-only"]
    assert collection.curies("a-b") == ["CURIE:b-only"]
    assert collection.curies("c") == [
        "CURIE:z",
        "CURIE:c1",
        "CURIE:c2",
        "CURIE:c3",
        "CURIE:c4",
    ]
    assert collection.duplicated_curies() == set()


def test_repeated_subset_is_deleted_instead_of_pulled_empty(worker_module, monkeypatch):
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

    assert run_batch(worker_module, ["CURIE:shared"]) == (0, 1, 0)
    assert collection.stored("small-molecule") is None
    assert collection.duplicated_curies() == set()


def test_curie_validation_accepts_exactly_one_occurrence_per_candidate(
    worker_module, monkeypatch
):
    candidate_curies = [f"CURIE:{index}" for index in range(9)]
    install_fake_collection(
        worker_module,
        monkeypatch,
        [identifier_document("d1", *candidate_curies, "CURIE:other")],
    )
    monkeypatch.setattr(worker_module, "NODENORM_VALIDATION_BATCH_SIZE", 1)
    monkeypatch.setattr(worker_module, "NODENORM_WORKER_COUNT", 2)
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter((curie,) for curie in candidate_curies),
    )

    # Nine one-row batches exceed the four-batch (2 * workers) initial window,
    # exercising both bounded prefill and replenishment.
    report = worker_module.validate_curie_uniqueness("data", "collection")
    assert report == worker_module.CurieValidationReport(9, 0, 0, 0, ())


def test_curie_validation_reports_missing_split_and_repeated_candidates(
    worker_module, monkeypatch
):
    install_fake_collection(
        worker_module,
        monkeypatch,
        [
            identifier_document("y1", "CURIE:split"),
            identifier_document("y2", "CURIE:split"),
            identifier_document("z", "CURIE:repeat", "CURIE:repeat"),
        ],
    )
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter(
            (("CURIE:missing",), ("CURIE:split",), ("CURIE:repeat",))
        ),
    )
    monkeypatch.setattr(worker_module, "NODENORM_VALIDATION_BATCH_SIZE", 1)

    report = worker_module.validate_curie_uniqueness("data", "collection")
    assert report.candidate_count == 3
    assert report.missing_count == 1
    assert report.multiple_document_count == 1
    assert report.repeated_in_document_count == 1

    message = report.failure_message()
    assert "3 violation(s) among 3 processed candidate CURIE(s)" in message
    assert "missing=1" in message
    assert "multiple-documents=1" in message
    assert "repeated-in-document=1" in message
    assert "CURIE:missing" in message
    assert "CURIE:split" in message
    assert "CURIE:repeat" in message


def test_curie_validation_stops_queued_batches_after_query_failure(
    worker_module, monkeypatch
):
    collection = install_fake_collection(worker_module, monkeypatch)
    queried_curies = []
    yielded_curies = []
    mongo_error = RuntimeError("MongoDB query failed")

    def fail_first_query(query, _projection=None):
        duplicate_curie = query["identifiers.i"]
        queried_curies.append(duplicate_curie)
        if duplicate_curie == "CURIE:boom":
            raise mongo_error
        return FakeMongoCursor([])

    monkeypatch.setattr(collection, "find", fail_first_query)
    monkeypatch.setattr(worker_module, "NODENORM_VALIDATION_BATCH_SIZE", 1)
    monkeypatch.setattr(worker_module, "NODENORM_WORKER_COUNT", 1)

    def duplicate_rows(_data_folder):
        for duplicate_curie in (
            "CURIE:boom",
            "CURIE:queued",
            "CURIE:not-submitted",
        ):
            yielded_curies.append(duplicate_curie)
            yield (duplicate_curie,)

    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        duplicate_rows,
    )

    with pytest.raises(RuntimeError) as raised:
        worker_module.validate_curie_uniqueness("data", "collection")

    assert raised.value is mongo_error
    assert queried_curies == ["CURIE:boom"]
    assert yielded_curies[0] == "CURIE:boom"
    assert "CURIE:not-submitted" not in yielded_curies


def test_curie_validation_retries_transient_mongo_timeout(worker_module, monkeypatch):
    collection = install_fake_collection(
        worker_module,
        monkeypatch,
        [identifier_document("d1", "CURIE:x")],
    )
    original_find = collection.find
    attempts = []

    def transient_find(query, projection=None):
        attempts.append(query["identifiers.i"])
        if len(attempts) < 3:
            raise worker_module.pymongo.errors.ServerSelectionTimeoutError(
                "temporary MongoDB contention"
            )
        return original_find(query, projection)

    monkeypatch.setattr(collection, "find", transient_find)
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter((("CURIE:x",),)),
    )
    monkeypatch.setattr(worker_module, "NODENORM_VALIDATION_MONGO_ATTEMPTS", 3)

    report = worker_module.validate_curie_uniqueness("data", "collection")

    assert report.violation_count == 0
    assert attempts == ["CURIE:x", "CURIE:x", "CURIE:x"]


def test_curie_validation_returns_incomplete_report_after_mongo_exhaustion(
    worker_module, monkeypatch
):
    collection = install_fake_collection(worker_module, monkeypatch)
    attempts = []

    def unavailable_find(query, _projection=None):
        attempts.append(query["identifiers.i"])
        raise worker_module.pymongo.errors.ServerSelectionTimeoutError(
            "MongoDB unavailable"
        )

    monkeypatch.setattr(collection, "find", unavailable_find)
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter((("CURIE:x",),)),
    )
    monkeypatch.setattr(worker_module, "NODENORM_VALIDATION_MONGO_ATTEMPTS", 3)

    report = worker_module.validate_curie_uniqueness("data", "collection")

    assert report.complete is False
    assert report.candidate_count == 0
    assert "Audit incomplete" in report.failure_message()
    assert attempts == ["CURIE:x", "CURIE:x", "CURIE:x"]


def test_curie_validation_bounds_failure_examples(worker_module, monkeypatch):
    install_fake_collection(worker_module, monkeypatch)
    monkeypatch.setattr(worker_module, "NODENORM_VALIDATION_FAILURE_SAMPLE_SIZE", 2)
    monkeypatch.setattr(
        worker_module,
        "_iter_duplicate_curies",
        lambda _data_folder: iter((("CURIE:one",), ("CURIE:two",), ("CURIE:three",))),
    )

    report = worker_module.validate_curie_uniqueness("data", "collection")
    message = report.failure_message()
    assert "3 violation(s) among 3 processed candidate CURIE(s)" in message
    assert "CURIE:one" in message
    assert "CURIE:two" in message
    assert "CURIE:three" not in message


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
