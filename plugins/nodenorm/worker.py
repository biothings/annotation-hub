import concurrent.futures
import hashlib
import itertools
import json
import multiprocessing
import os
import queue
import sqlite3
import threading
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import pymongo
from pymongo.errors import BulkWriteError

from biothings import config
from biothings.utils.dataload import merge_struct
from biothings.utils.serializer import json_loads
from biothings.utils.hub_db import get_src_db
from biothings.utils.common import iter_n

from .static import (
    CONFLATION_LOOKUP_DATABASE,
    DRUG_CHEMICAL_IDENTIFIER_FILES,
    GENE_PROTEIN_IDENTIFER_FILES,
    IDENTIFIER_LOOKUP_DATABASE,
    NODENORM_UPLOAD_CHUNKS,
)

logger = config.logger
NODENORM_WORKER_COUNT = 30
NODENORM_CURIE_VALIDATION_MODES = frozenset({"off", "report", "strict"})
NODENORM_VALIDATION_FAILURE_SAMPLE_SIZE = 20
NODENORM_VALIDATION_BATCH_SIZE = 100
NODENORM_VALIDATION_PROGRESS_INTERVAL = 10_000
NODENORM_VALIDATION_MONGO_ATTEMPTS = 3
NODENORM_IDENTIFIER_SHARD_COUNT = 8
NODENORM_IDENTIFIER_BATCH_SIZE = 100_000
NODENORM_IDENTIFIER_SHARD_QUEUE_SIZE = 60
NODENORM_IDENTIFIER_COMMIT_BATCHES = 8
IDENTIFIER_WRITER_STOP = None
IDENTIFIER_QUEUES = None
IDENTIFIER_WRITER_FAILED = None


class NodeNormCollectionValidationError(RuntimeError):
    """The uploaded collection does not satisfy NodeNorm's CURIE contract."""


@dataclass(frozen=True)
class CurieValidationReport:
    """The measured state of duplicate CURIE candidates after cleanup."""

    candidate_count: int
    missing_count: int
    multiple_document_count: int
    repeated_in_document_count: int
    samples: tuple[str, ...]
    complete: bool = True

    @property
    def violation_count(self) -> int:
        return (
            self.missing_count
            + self.multiple_document_count
            + self.repeated_in_document_count
        )

    def failure_message(self) -> str:
        message = (
            "NodeNorm CURIE uniqueness audit found "
            f"{self.violation_count} violation(s) among "
            f"{self.candidate_count} processed candidate CURIE(s); expected exactly one "
            "occurrence in exactly one document. "
            f"missing={self.missing_count}, "
            f"multiple-documents={self.multiple_document_count}, "
            f"repeated-in-document={self.repeated_in_document_count}. "
            f"Examples: {'; '.join(self.samples)}"
        )
        if not self.complete:
            message += " Audit incomplete because MongoDB remained unavailable."
        return message


def _curie_validation_mode() -> str:
    mode = getattr(config, "NODENORM_CURIE_VALIDATION_MODE", "off")
    if not isinstance(mode, str) or mode not in NODENORM_CURIE_VALIDATION_MODES:
        choices = ", ".join(sorted(NODENORM_CURIE_VALIDATION_MODES))
        raise ValueError(
            "NODENORM_CURIE_VALIDATION_MODE must be one of " f"{choices}; got {mode!r}"
        )
    return mode


def _configure_sqlite_tmpdir() -> Path:
    configured_tmpdir = os.environ.get("SQLITE_TMPDIR", "").strip()
    sqlite_tmpdir = (
        Path(configured_tmpdir).expanduser()
        if configured_tmpdir
        else Path(config.DATA_ARCHIVE_ROOT).joinpath("sqlite_tmp")
    ).resolve()
    sqlite_tmpdir.mkdir(parents=True, exist_ok=True)
    if not os.access(sqlite_tmpdir, os.W_OK | os.X_OK):
        raise OSError(f"SQLite temp directory is not writable: {sqlite_tmpdir}")
    os.environ["SQLITE_TMPDIR"] = str(sqlite_tmpdir)
    return sqlite_tmpdir


def upload_process(data_folder: Union[str, Path], collection_name: str) -> int:
    validation_mode = _curie_validation_mode()
    _configure_sqlite_tmpdir()

    create_identifiers_table(data_folder)

    process_context = multiprocessing.get_context("spawn")
    identifier_queues = tuple(
        process_context.Queue(maxsize=NODENORM_IDENTIFIER_SHARD_QUEUE_SIZE)
        for _ in range(NODENORM_IDENTIFIER_SHARD_COUNT)
    )
    identifier_writer_failed = process_context.Event()
    identifier_writer_errors = []
    identifier_writers = [
        threading.Thread(
            target=_write_identifier_batches,
            args=(
                identifier_database,
                identifier_queues[shard_index],
                identifier_writer_failed,
                identifier_writer_errors,
            ),
            name=f"nodenorm-identifier-writer-{shard_index:02d}",
        )
        for shard_index, identifier_database in enumerate(
            _identifier_database_paths(data_folder)
        )
    ]
    started_identifier_writers = []
    upload_error = None

    try:
        for identifier_writer in identifier_writers:
            identifier_writer.start()
            started_identifier_writers.append(identifier_writer)

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=NODENORM_WORKER_COUNT,
            mp_context=process_context,
            initializer=_configure_identifier_writer,
            initargs=(identifier_queues, identifier_writer_failed),
        ) as executor:
            process_futures = set()
            try:
                for index, task in enumerate(
                    _build_offset_tasks(data_folder, collection_name)
                ):
                    future = executor.submit(subset_upload_worker, **task)
                    process_futures.add(future)

                total_document_count = 0
                for index, future in enumerate(
                    concurrent.futures.as_completed(process_futures)
                ):
                    # Completed futures retain their result; drop our reference
                    # before waiting on the next upload task.
                    process_futures.discard(future)
                    identifier_count = future.result()
                    total_document_count += identifier_count
                    logger.debug(
                        "Task %s completed | Update %s identifiers | Total identifiers %s",
                        index,
                        identifier_count,
                        total_document_count,
                    )
                    del identifier_count
                    del future
            except Exception as upload_exception:
                logger.exception(upload_exception)
                for pending_future in process_futures:
                    pending_future.cancel()
                raise
    except Exception as upload_exception:
        upload_error = upload_exception
    finally:
        for identifier_queue in identifier_queues[: len(started_identifier_writers)]:
            identifier_queue.put(IDENTIFIER_WRITER_STOP)
        for identifier_writer in started_identifier_writers:
            identifier_writer.join()
        for identifier_queue in identifier_queues:
            identifier_queue.close()
            identifier_queue.join_thread()

    if identifier_writer_errors:
        if upload_error is not None:
            raise identifier_writer_errors[0] from upload_error
        raise identifier_writer_errors[0]
    if upload_error is not None:
        raise upload_error

    _prepare_collection_for_promotion(
        data_folder, collection_name, validation_mode=validation_mode
    )
    return int(total_document_count)


def _prepare_collection_for_promotion(
    data_folder: Union[str, Path], collection_name: str, validation_mode: str
) -> None:
    """Build repair indexes, clean duplicates, and optionally audit the result."""
    create_mongo_identifiers_index(collection_name)
    create_identifiers_index(data_folder)
    cleanup_curie_duplication(data_folder, collection_name)
    if validation_mode == "off":
        logger.info(
            "Skipping the opt-in NodeNorm CURIE uniqueness audit before promotion"
        )
        return

    try:
        validation_report = validate_curie_uniqueness(data_folder, collection_name)
    except pymongo.errors.ServerSelectionTimeoutError as validation_error:
        message = "NodeNorm CURIE uniqueness audit could not start: MongoDB unavailable"
        if validation_mode == "strict":
            raise NodeNormCollectionValidationError(
                f"{message}; strict mode blocks collection promotion"
            ) from validation_error
        logger.exception("%s; report mode allows collection promotion", message)
        return

    if validation_report.complete and validation_report.violation_count == 0:
        return
    if validation_mode == "strict":
        raise NodeNormCollectionValidationError(validation_report.failure_message())
    logger.warning(
        "%s Promotion remains allowed because validation mode is report.",
        validation_report.failure_message(),
    )


def _configure_identifier_writer(identifier_queues, identifier_writer_failed):
    global IDENTIFIER_QUEUES, IDENTIFIER_WRITER_FAILED

    IDENTIFIER_QUEUES = identifier_queues
    IDENTIFIER_WRITER_FAILED = identifier_writer_failed


def _build_offset_tasks(data_folder: Union[str, Path], collection_name: str):
    """
    Reads every file and builds an index file compiling the offset ranges
    we want to read as a subset of the file processing.

    These are all generated in a queue that we continually feed to multiprocessing queue
    that is continually uploading to the backend database
    """

    def _populate_upload_arguments(
        input_file: Union[Path, str], num_partitions: int
    ) -> list:
        logger.info(
            "Analyzing offsets for %s | number of partitions %s",
            input_file,
            num_partitions,
        )
        conflation_database = None
        if (
            input_file.name in DRUG_CHEMICAL_IDENTIFIER_FILES
            or input_file.name in GENE_PROTEIN_IDENTIFER_FILES
        ):
            data_folder = Path(input_file).absolute().resolve().parent
            conflation_database = data_folder.joinpath(CONFLATION_LOOKUP_DATABASE)

        offsets = generate_file_offsets(input_file, num_partitions)

        argument_collection = []
        for offset_range in window(offsets, 2):
            offset_start = offset_range[0]
            offset_end = offset_range[1]

            arguments = {
                "input_file": input_file,
                "buffer_size": 5000,
                "offset_start": offset_start,
                "offset_end": offset_end,
                "collection_name": collection_name,
                "conflation_database": conflation_database,
            }
            argument_collection.append(arguments)
        return argument_collection

    thread_futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for filename, num_partitions in NODENORM_UPLOAD_CHUNKS.items():
            filepath = Path(data_folder).joinpath(filename).resolve().absolute()
            arguments = {"input_file": filepath, "num_partitions": num_partitions}
            future = executor.submit(_populate_upload_arguments, **arguments)
            thread_futures.append(future)

    concurrent.futures.wait(
        thread_futures, timeout=None, return_when=concurrent.futures.ALL_COMPLETED
    )

    yield from itertools.chain.from_iterable(
        [future.result() for future in thread_futures]
    )


def generate_file_offsets(file: Union[str, Path], num_partitions: int = None):
    """
    Generates an in-memory index for a file. Rather than storing the offset

    We're primarily interested in parallel processsing a file in batches for uploading
    to a database, so we don't need to store every offset as that would be a waste of memory.
    Rather, based off the number of partitions we store the file size as we iterate over the file
    so we can track the approximate percentage we've completed so we can store inteval markers while
    chunking the file
    """
    if num_partitions is None:
        num_partitions = 10

    file = Path(file).absolute().resolve()
    file_size_bytes = file.stat().st_size
    file_index = file.with_suffix(".index")

    logger.debug("Calculating SHA256 hashsum for file: %s", file)
    file_hash = sha256sum(file)

    if file_index.exists():
        with open(file_index, "r", encoding="utf-8") as index_handle:
            previous_index = json.load(index_handle)

        if previous_index["hash"] == file_hash:
            return previous_index["index"]

        logger.debug(
            "Different hash found for file %s [%s, %s] [previous, current]. "
            "Updating file offsets with the new file",
            file,
            previous_index["hash"],
            file_hash,
        )

    partitions = [0]
    marker = file_size_bytes / num_partitions
    progress_bytes = 0
    with open(file, "rb") as handle:
        while line := handle.readline():
            if progress_bytes >= marker:
                partitions.append(handle.tell())
                progress_bytes = 0
            else:
                progress_bytes += len(line)
        partitions.append(handle.tell())
    with open(file_index, "w", encoding="utf-8") as index_handle:
        index_handle.write(json.dumps({"index": partitions, "hash": file_hash}))
    return partitions


def sha256sum(file: Union[str, Path], buffer_size: int = None):
    """
    Generates a sha256 hashsum for a file to determine if the previously generated
    index requires updating
    """
    if buffer_size is None:
        buffer_size = 1024 * 128

    filehash = hashlib.sha256()
    buffer = bytearray(buffer_size)
    file_view = memoryview(buffer)

    with open(file, "rb", buffering=0) as file_handle:
        while n := file_handle.readinto(file_view):
            filehash.update(file_view[:n])
    return filehash.hexdigest()


def window(seq: tuple, n: int = 2):
    """
    Returns a sliding window (of width n) over data from the iterable.
    s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...
    """
    it = iter(seq)
    result = tuple(itertools.islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


def subset_upload_worker(
    input_file: Union[str, Path],
    buffer_size: int,
    offset_start: int,
    offset_end: int,
    collection_name: str,
    conflation_database: str = None,
) -> int:
    """
    Internal function for handling the multipart uploading of the file in partitions

    Accepts the file path, along with the chunk start and chunk end in bytes to determine
    what offset to start and stop at in the file

    The conflation_connection is a static singular sqlite3 file located in the same directory as the
    data files as it's generated post-dump. We can just derived it at run-time from the provided
    data filepath

    Afterwards the data processing is straight forward, we effectively don't transform the state of
    the nodenorm files
    """
    logger.info(
        "Starting bulk upload to backend %s [%s|%s]",
        input_file,
        offset_start,
        offset_end,
    )
    conflation_connection = None
    if conflation_database is not None:
        conflation_connection = sqlite3.connect(str(conflation_database))

    upload_database = get_src_db()
    collection = pymongo.collection.Collection(
        database=upload_database, name=collection_name
    )

    with open(input_file, encoding="utf-8") as file_handle:
        buffer = []
        identifier_batch = []
        identifier_count = 0
        canonical_identifiers = []
        file_handle.seek(offset_start)
        while file_handle.tell() < offset_end:
            line = file_handle.readline()
            doc = json_loads(line)

            canonical_identifier = doc["identifiers"][0]["i"]
            canonical_identifiers.append(canonical_identifier)
            doc["_id"] = canonical_identifier
            try:
                doc["ic"] = float(doc["ic"])
            except (TypeError, ValueError):
                doc["ic"] = 0.0

            buffer.append(doc)

            for identifier in doc["identifiers"]:
                identifier_batch.append(identifier["i"])
                identifier_count += 1
                if len(identifier_batch) >= NODENORM_IDENTIFIER_BATCH_SIZE:
                    _queue_identifier_batch(identifier_batch)
                    identifier_batch = []
                identifier["c"] = {"gp": None, "dc": None}

            if len(buffer) >= buffer_size:
                if conflation_connection is not None:
                    buffer = _update_buffer_with_conflations(
                        buffer, canonical_identifiers, conflation_connection
                    )
                _upload_buffer(
                    collection, buffer, input_file, file_handle.tell() / offset_end
                )
                buffer = []
                canonical_identifiers = []

        if len(buffer) > 0:
            if conflation_connection is not None:
                buffer = _update_buffer_with_conflations(
                    buffer, canonical_identifiers, conflation_connection
                )
            _upload_buffer(
                collection, buffer, input_file, file_handle.tell() / offset_end
            )
        if identifier_batch:
            _queue_identifier_batch(identifier_batch)
    return identifier_count


def _queue_identifier_batch(identifier_batch):
    """
    Send a bounded identifier batch to the SQLite writer.

    A timeout lets workers notice a writer failure instead of blocking forever on
    a full queue.
    """
    if IDENTIFIER_QUEUES is None or IDENTIFIER_WRITER_FAILED is None:
        raise RuntimeError("Identifier writer queues were not configured")

    shard_batches = [[] for _ in range(len(IDENTIFIER_QUEUES))]
    for identifier in identifier_batch:
        shard_batches[_identifier_shard(identifier)].append(identifier)

    for shard_index, shard_batch in enumerate(shard_batches):
        if not shard_batch:
            continue

        _put_identifier_batch(IDENTIFIER_QUEUES[shard_index], shard_batch)


def _identifier_shard(identifier: str) -> int:
    # Use a stable hash because Python's built-in hash is randomized per process.
    return zlib.crc32(identifier.encode("utf-8")) % NODENORM_IDENTIFIER_SHARD_COUNT


def _put_identifier_batch(identifier_queue, identifier_batch):
    while True:
        if IDENTIFIER_WRITER_FAILED.is_set():
            raise RuntimeError("Identifier writer failed; aborting upload worker")
        try:
            identifier_queue.put(identifier_batch, timeout=5)
            return
        except queue.Full:
            continue


def _update_buffer_with_conflations(
    buffer: list[dict],
    canonical_identifiers: list[str],
    conflation_database: sqlite3.Connection,
) -> list[str]:
    """
    Batch updates the buffer documents with the conflation identifiers found

    Performs a lookup against the conflation database to find all conflation identifiers
    We then create an index table so we can quickly lookup up the buffer index based off the
    canonical identifier

    We iterate over the discovered conflation identifier results and update the buffer with the
    conflation information before returning the newly updated buffer
    """
    identifiers_repr = ", ".join("?" for _ in canonical_identifiers)
    search_statement = f"SELECT identifiers, type FROM conflations WHERE conflation in ({identifiers_repr})"
    identifier_results = conflation_database.execute(
        search_statement, canonical_identifiers
    )

    lookup_buffer_index = {
        document["identifiers"][0]["i"]: index for index, document in enumerate(buffer)
    }

    for conflation_result in identifier_results.fetchall():
        if conflation_result is not None:
            identifiers = conflation_result[0].strip().split(",")
            conflation_type = conflation_result[1]

            for identifier in identifiers:
                buffer_index = lookup_buffer_index.get(identifier, None)
                if buffer_index is not None:
                    for identifier in buffer[buffer_index]["identifiers"]:
                        if conflation_type == "GeneProtein":
                            identifier["c"]["gp"] = identifiers
                        elif conflation_type == "DrugChemical":
                            identifier["c"]["dc"] = identifiers
    return buffer


def _upload_buffer(
    collection: pymongo.collection.Collection,
    buffer: list[dict],
    input_file: Union[str, Path],
    progress: float,
):
    try:
        t0 = time.perf_counter()
        collection.insert_many(buffer, ordered=False)
        logger.debug(
            "insert many #[%d] in [%3.4f]s | file %s subset progress: %1.3f%%",
            len(buffer),
            time.perf_counter() - t0,
            input_file.name,
            progress * 100,
        )
    except BulkWriteError as bulk_write_error:
        _handle_bulk_write_error(bulk_write_error, collection, input_file)


def _handle_bulk_write_error(
    bulk_write_error: BulkWriteError,
    collection: pymongo.collection.Collection,
    input_file: Union[str, Path],
):
    logger.debug("Fixing %d records ", len(bulk_write_error.details["writeErrors"]))
    ids = [d["op"]["_id"] for d in bulk_write_error.details["writeErrors"]]

    # build hash of existing docs
    docs = collection.find({"_id": {"$in": ids}})

    hdocs = {}
    for doc in docs:
        hdocs[doc["_id"]] = doc

    bulk = []
    for err in bulk_write_error.details["writeErrors"]:
        errdoc = err["op"]
        existing = hdocs[errdoc["_id"]]
        if errdoc is existing:
            continue
        assert "_id" in existing
        _id = errdoc.pop("_id")
        merged = merge_struct(errdoc, existing)

        # update previously fetched doc. if several errors are about the same doc id,
        # we would't merged things properly without an updated document
        assert "_id" in merged
        bulk.append(pymongo.UpdateOne({"_id": _id}, {"$set": merged}))
        hdocs[_id] = merged

    collection.bulk_write(bulk, ordered=False)


def create_identifiers_table(data_folder: Union[str, Path]) -> None:
    logger.debug("Creating sqlite3 identifiers databases")
    for identifier_database in _identifier_database_paths(data_folder):
        identifier_connection = _connect_identifier_database(identifier_database)
        cursor = identifier_connection.cursor()
        identifier_existence_check = "DROP TABLE IF EXISTS identifiers"
        cursor.execute(identifier_existence_check)

        identifier_table = (
            "CREATE TABLE IF NOT EXISTS identifiers("
            "identifier text PRIMARY KEY NOT NULL, "
            "count INT DEFAULT 1"
            ") WITHOUT ROWID;"
        )
        cursor.execute(identifier_table)
        identifier_connection.commit()
        identifier_connection.close()


def create_identifiers_index(data_folder: Union[str, Path]) -> None:
    logger.debug("Creating sqlite3 duplicate identifiers database indexes")
    for identifier_database in _identifier_database_paths(data_folder):
        identifier_connection = _connect_identifier_database(identifier_database)
        cursor = identifier_connection.cursor()

        identifier_index = (
            "CREATE INDEX IF NOT EXISTS idx_identifiers_duplicates "
            "ON identifiers (count, identifier) WHERE count > 1;"
        )
        cursor.execute(identifier_index)
        identifier_connection.commit()
        identifier_connection.close()


def create_mongo_identifiers_index(collection_name: str) -> None:
    logger.debug("Creating mongodb identifiers.i database index")
    upload_database = get_src_db()
    collection = pymongo.collection.Collection(
        database=upload_database, name=collection_name
    )
    collection.create_index("identifiers.i")


def _identifier_database_paths(data_folder: Union[str, Path]) -> tuple[Path, ...]:
    identifier_database = (
        Path(data_folder).resolve().absolute().joinpath(IDENTIFIER_LOOKUP_DATABASE)
    )
    if NODENORM_IDENTIFIER_SHARD_COUNT == 1:
        return (identifier_database,)

    return tuple(
        identifier_database.with_name(
            f"{identifier_database.stem}.{shard_index:02d}{identifier_database.suffix}"
        )
        for shard_index in range(NODENORM_IDENTIFIER_SHARD_COUNT)
    )


def _connect_identifier_database(identifier_database: Union[str, Path]):
    identifier_connection = sqlite3.connect(str(identifier_database))
    identifier_connection.execute("PRAGMA journal_mode=WAL")
    identifier_connection.execute("PRAGMA synchronous=NORMAL")
    return identifier_connection


def _write_identifier_batches(
    identifier_database: Union[str, Path],
    identifier_queue,
    identifier_writer_failed,
    identifier_writer_errors: list[Exception],
):
    """
    Own one SQLite identifier shard connection and persist streamed worker batches.
    """
    identifier_connection = None
    stop_received = False

    try:
        identifier_connection = _connect_identifier_database(identifier_database)
        cursor = identifier_connection.cursor()

        while True:
            try:
                identifier_batch = identifier_queue.get(timeout=5)
            except queue.Empty:
                continue

            if identifier_batch is IDENTIFIER_WRITER_STOP:
                stop_received = True
                break
            if identifier_writer_failed.is_set():
                continue

            update_identifier_collection(cursor, identifier_batch)
            batch_count = 1
            while batch_count < NODENORM_IDENTIFIER_COMMIT_BATCHES:
                if identifier_writer_failed.is_set():
                    break

                try:
                    identifier_batch = identifier_queue.get_nowait()
                except queue.Empty:
                    break

                if identifier_batch is IDENTIFIER_WRITER_STOP:
                    stop_received = True
                    break

                update_identifier_collection(cursor, identifier_batch)
                batch_count += 1

            identifier_connection.commit()

            if stop_received:
                break
    except Exception as write_exception:
        identifier_writer_errors.append(write_exception)
        identifier_writer_failed.set()
        logger.exception(write_exception)
        if not stop_received:
            _drain_identifier_queue(identifier_queue)
    finally:
        if identifier_connection is not None:
            try:
                identifier_connection.close()
            except Exception as close_exception:
                identifier_writer_errors.append(close_exception)
                identifier_writer_failed.set()
                logger.exception(close_exception)


def _drain_identifier_queue(identifier_queue):
    """Discard queued batches until shutdown so producer feeder threads can exit."""
    while True:
        try:
            identifier_batch = identifier_queue.get(timeout=5)
        except queue.Empty:
            continue
        if identifier_batch is IDENTIFIER_WRITER_STOP:
            return


def update_identifier_collection(cursor: sqlite3.Cursor, identifiers: list[str]):
    """
    Stores an identifier batch in sqlite3 for post-update duplicate cleanup.
    """
    upsert_statement = (
        "INSERT INTO identifiers(identifier) "
        "VALUES(?) "
        "ON CONFLICT(identifier) "
        "DO UPDATE SET count=count+1;"
    )
    identifier_information = ((identifier,) for identifier in identifiers)

    cursor.executemany(upsert_statement, identifier_information)


def cleanup_curie_duplication(
    data_folder: Union[str, Path], collection_name: str
) -> tuple[int, int]:
    """
    Handle the CURIE duplication directly in the mongodb database

    Returns the number of operations MongoDB applied and the number of duplicate
    CURIEs left unresolved.

    A CURIE we cannot reason about is counted rather than raising immediately so
    the cleanup can evaluate and summarize the full candidate set. When enabled,
    the direct audit measures what remains and the configured validation mode
    decides whether it blocks promotion. Actual processing errors propagate.

    Neither count establishes that `identifiers.i` is unique afterwards, which is
    the 1-1 mapping the Elasticsearch terms query depends on (see README,
    "Post Upload Processing"):

    - corrections are documents deleted plus documents modified, so they are not
      a count of distinct documents, and repairing several CURIEs on one document
      can report fewer corrections than CURIEs resolved;
    - a pull the server declines in order to keep a document non-empty applies
      nothing and is indistinguishable here from one that had already been
      applied, so it is not counted unresolved even though the CURIE still is.

    Validating the collection against the shard CURIE set before the uploader
    promotes it is what settles uniqueness. These counters are for reporting.
    """
    logger.info("Handling CURIE duplication issue")

    total_correction_count = 0
    total_unresolved_count = 0

    # Pair repairs make decisions from both documents but MongoDB applies each
    # write to only one of them. If batches overlap, one can therefore act on a
    # keeper snapshot that another batch has since changed or deleted. Stable
    # sorting does not solve that write skew. Upload into the temporary collection
    # is complete at this point, so stream and finish one batch before the next one
    # reads. Do not parallelize this loop without re-reading and writing while all
    # documents involved in a repair are locked together.
    try:
        duplicate_curies = _iter_duplicate_curies(data_folder)
        for index, curie_batch in enumerate(iter_n(duplicate_curies, 1000)):
            task_id, num_corrections, num_unresolved = _curie_duplication_batch_handler(
                task_id=index,
                curies=curie_batch,
                collection_name=collection_name,
            )
            total_correction_count += num_corrections
            total_unresolved_count += num_unresolved
            logger.debug(
                "Task %s completed | Applied %s operation(s) | Total corrections %s",
                task_id,
                num_corrections,
                total_correction_count,
            )
    except Exception:
        logger.exception("CURIE duplicate cleanup failed")
        raise

    logger.info(
        "CURIE duplicate cleanup applied %s operation(s) and left %s duplicate "
        "CURIE(s) unresolved; this is not a uniqueness check",
        total_correction_count,
        total_unresolved_count,
    )
    if total_unresolved_count > 0:
        logger.warning(
            "%s duplicate CURIE(s) were left as-is; grep the log for "
            "'Unable to resolve duplicate CURIE' for the individual CURIEs",
            total_unresolved_count,
        )
    return total_correction_count, total_unresolved_count


def _iter_duplicate_curies(data_folder: Union[str, Path]):
    identifier_table = "SELECT identifier FROM identifiers WHERE count > 1;"

    for identifier_database in _identifier_database_paths(data_folder):
        identifier_connection = sqlite3.connect(str(identifier_database))
        try:
            cursor = identifier_connection.cursor()
            results = cursor.execute(identifier_table)
            while duplicate_curies := results.fetchmany(10_000):
                yield from duplicate_curies
        finally:
            identifier_connection.close()


def validate_curie_uniqueness(
    data_folder: Union[str, Path],
    collection_name: str,
) -> CurieValidationReport:
    """
    Measure whether every CURIE counted more than once now occurs exactly once.

    The SQLite shards are the complete candidate set: a CURIE cannot occur more
    than once in MongoDB unless the upload encountered it more than once. Each
    lookup uses the existing ``identifiers.i`` index and is capped at two
    documents. Inspecting the projected identifier arrays also catches a CURIE
    repeated within one document, which a document-count check alone would miss.

    CURIE equality here is the same raw, case-sensitive equality used by the
    uploader and MongoDB cleanup. This audit does not attempt to reproduce the
    external Elasticsearch normalizer.

    This indexed candidate check intentionally avoids a collection-wide empty-
    array scan. Source ingestion already requires ``identifiers[0]``, and every
    cleanup mutation either deletes the document, deduplicates to at least one
    identifier, or guards its pull on leaving an identifier behind.

    This function only measures and returns the result. The caller applies the
    configured report-or-strict promotion policy.
    """
    logger.info("Validating duplicate CURIE repairs before collection promotion")
    upload_database = get_src_db()
    collection = pymongo.collection.Collection(
        database=upload_database, name=collection_name
    )

    candidate_count = 0
    failure_counts = {"missing": 0, "multiple": 0, "repeated": 0}
    failure_samples = []
    next_progress = NODENORM_VALIDATION_PROGRESS_INTERVAL
    curie_batches = iter_n(
        _iter_duplicate_curies(data_folder), NODENORM_VALIDATION_BATCH_SIZE
    )
    validation_stop = threading.Event()
    audit_complete = True

    # Validation is read-only and can safely recover most of the indexed lookup
    # throughput that state-dependent cleanup deliberately gives up. Keep only a
    # bounded number of batches in flight so a very large candidate set does not
    # become another unbounded Future list.
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=NODENORM_WORKER_COUNT
    ) as executor:
        pending_futures = set()
        try:
            for _index in range(NODENORM_WORKER_COUNT * 2):
                if validation_stop.is_set():
                    break
                try:
                    curie_batch = next(curie_batches)
                except StopIteration:
                    break
                pending_futures.add(
                    executor.submit(
                        _validate_curie_batch,
                        collection,
                        curie_batch,
                        validation_stop,
                    )
                )

            while pending_futures:
                completed_futures, pending_futures = concurrent.futures.wait(
                    pending_futures,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in completed_futures:
                    batch_count, batch_failures, batch_samples = future.result()
                    candidate_count += batch_count
                    for failure_kind, count in batch_failures.items():
                        failure_counts[failure_kind] += count
                    remaining_sample_slots = (
                        NODENORM_VALIDATION_FAILURE_SAMPLE_SIZE - len(failure_samples)
                    )
                    if remaining_sample_slots > 0:
                        failure_samples.extend(batch_samples[:remaining_sample_slots])

                    if not validation_stop.is_set():
                        try:
                            curie_batch = next(curie_batches)
                        except StopIteration:
                            pass
                        else:
                            pending_futures.add(
                                executor.submit(
                                    _validate_curie_batch,
                                    collection,
                                    curie_batch,
                                    validation_stop,
                                )
                            )

                if candidate_count >= next_progress:
                    logger.info(
                        "Validated %s duplicate CURIE candidate(s) so far",
                        candidate_count,
                    )
                    while candidate_count >= next_progress:
                        next_progress += NODENORM_VALIDATION_PROGRESS_INTERVAL
        except pymongo.errors.ServerSelectionTimeoutError:
            validation_stop.set()
            for pending_future in pending_futures:
                pending_future.cancel()
            audit_complete = False
            logger.warning(
                "MongoDB remained unavailable; returning a partial CURIE audit "
                "after %s processed candidate(s): missing=%s, "
                "multiple-documents=%s, repeated-in-document=%s",
                candidate_count,
                failure_counts["missing"],
                failure_counts["multiple"],
                failure_counts["repeated"],
            )
        except Exception:
            # Setting the event lets already-running batches stop after their
            # current Mongo query; cancel() keeps queued batches from starting.
            # Without both, ThreadPoolExecutor's context manager waits for up to
            # twice the worker count of unnecessary batches before propagating a
            # server failure.
            validation_stop.set()
            for pending_future in pending_futures:
                pending_future.cancel()
            raise

    validation_report = CurieValidationReport(
        candidate_count=candidate_count,
        missing_count=failure_counts["missing"],
        multiple_document_count=failure_counts["multiple"],
        repeated_in_document_count=failure_counts["repeated"],
        samples=tuple(failure_samples),
        complete=audit_complete,
    )
    logger.info(
        "NodeNorm CURIE uniqueness audit %s after %s processed candidate CURIE(s) "
        "with %s violation(s)",
        "completed" if audit_complete else "stopped early",
        candidate_count,
        validation_report.violation_count,
    )
    return validation_report


def _validate_curie_batch(
    collection, curie_rows, validation_stop=None
) -> tuple[int, dict, list[str]]:
    """Validate one bounded, read-only batch and return aggregate diagnostics."""
    failure_counts = {"missing": 0, "multiple": 0, "repeated": 0}
    failure_samples = []
    projection = {"_id": 1, "identifiers.i": 1}
    processed_count = 0

    for result in curie_rows:
        if validation_stop is not None and validation_stop.is_set():
            break
        duplicate_curie = result[0]
        try:
            documents = _find_curie_documents_with_retry(
                collection, duplicate_curie, projection
            )
        except Exception:
            if validation_stop is not None:
                validation_stop.set()
            raise
        processed_count += 1
        occurrence_count = sum(
            identifier.get("i") == duplicate_curie
            for document in documents
            for identifier in document.get("identifiers", [])
        )

        if len(documents) == 0:
            failure_kind = "missing"
        elif len(documents) > 1:
            failure_kind = "multiple"
        elif occurrence_count != 1:
            failure_kind = "repeated"
        else:
            continue

        failure_counts[failure_kind] += 1
        if len(failure_samples) < NODENORM_VALIDATION_FAILURE_SAMPLE_SIZE:
            document_count = (
                "at least 2" if len(documents) == 2 else str(len(documents))
            )
            failure_samples.append(
                f"{duplicate_curie!r} (documents={document_count}, "
                f"occurrences in returned documents={occurrence_count})"
            )

    return processed_count, failure_counts, failure_samples


def _find_curie_documents_with_retry(collection, duplicate_curie, projection):
    """Retry only the transient MongoDB failure expected during Hub contention."""
    for attempt in range(1, NODENORM_VALIDATION_MONGO_ATTEMPTS + 1):
        try:
            return list(
                collection.find({"identifiers.i": duplicate_curie}, projection).limit(2)
            )
        except pymongo.errors.ServerSelectionTimeoutError:
            if attempt >= NODENORM_VALIDATION_MONGO_ATTEMPTS:
                raise
            logger.warning(
                "MongoDB unavailable while auditing CURIE %s; retrying (%s/%s)",
                duplicate_curie,
                attempt,
                NODENORM_VALIDATION_MONGO_ATTEMPTS,
            )

    raise AssertionError("MongoDB validation retry loop ended unexpectedly")


def _identifier_curies(document: dict) -> list[str]:
    """
    The CURIEs of a document's identifiers, in order, repeats included.

    Comparisons must be made on the CURIE. Comparing whole identifier
    dictionaries makes a CURIE shared with a different label or description look
    like two unrelated identifiers, which is precisely the duplication this
    cleanup exists to remove.
    """
    return [identifier["i"] for identifier in document["identifiers"]]


def _document_has_type(document: dict, node_type: str) -> bool:
    """Support both source strings and type lists produced by duplicate merges."""
    document_types = document["type"]
    if isinstance(document_types, (list, tuple, set)):
        return node_type in document_types
    return document_types == node_type


def _deduplicate_document_identifiers(
    task_id: int, document: dict
) -> tuple[Union[object, None], bool]:
    """
    Trim repeated CURIEs from a single document, keeping the first occurrence.

    Returns the operation to apply, or None, plus whether the CURIE was left
    unresolved. Finding nothing to trim is a resolution, not a failure: the
    shard counters record how often a CURIE was seen during upload, so a CURIE
    counted twice can legitimately end up in one document once the duplicate
    `_id` documents were merged.

    The filter pins the identifier array this decision was made from, so a
    document another worker has since changed is left alone instead of being
    clobbered. A miss applies nothing and is visible in the batch's applied
    count.
    """
    identifiers = document["identifiers"]
    seen_curies = set()
    deduplicated = []
    for identifier in identifiers:
        if identifier["i"] in seen_curies:
            continue
        seen_curies.add(identifier["i"])
        deduplicated.append(identifier)

    if len(deduplicated) == len(identifiers):
        logger.debug(
            "[Task %d] Document %s holds no repeated CURIE; nothing to trim",
            task_id,
            document["_id"],
        )
        return None, False

    logger.debug(
        "[Task %d] Trim %d repeated identifier(s) from document %s",
        task_id,
        len(identifiers) - len(deduplicated),
        document["_id"],
    )
    return (
        pymongo.UpdateOne(
            {"_id": document["_id"], "identifiers": identifiers},
            {"$set": {"identifiers": deduplicated}},
        ),
        False,
    )


def _evaluate_document_subset(
    task_id: int, more_identifiers_doc: dict, less_identifiers_doc: dict
):
    """
    If every CURIE of one document is also in the other we can drop it and keep
    the other, which covers the whole document. Anything short of that needs the
    intersection analysis below.
    """
    if not set(_identifier_curies(less_identifiers_doc)) <= set(
        _identifier_curies(more_identifiers_doc)
    ):
        return None

    logger.debug(
        "[Task %d] Delete document %s, a CURIE subset of %s",
        task_id,
        less_identifiers_doc["_id"],
        more_identifiers_doc["_id"],
    )
    return pymongo.DeleteOne({"_id": less_identifiers_doc["_id"]})


def _evaluate_document_intersection(
    task_id: int, more_identifiers_doc: dict, less_identifiers_doc: dict
):
    """
    One crucial assumption here is that at least one of these documents has a
    type of biolink:Protein.

    If the type biolink:Protein isn't found then we cannot make any assumptions
    about how to handle the intersection between the two documents.

    The colliding CURIEs are pulled from the non-Protein document by CURIE rather
    than by rewriting its identifier array, so concurrent repairs of other CURIEs
    on the same document compose instead of overwriting one another, and pulling
    the same CURIE twice is harmless.
    """
    more_is_protein = _document_has_type(more_identifiers_doc, "biolink:Protein")
    less_is_protein = _document_has_type(less_identifiers_doc, "biolink:Protein")
    if not more_is_protein and not less_is_protein:
        return None

    if more_is_protein:
        main_document = more_identifiers_doc
        side_document = less_identifiers_doc
    else:
        main_document = less_identifiers_doc
        side_document = more_identifiers_doc

    main_curies = set(_identifier_curies(main_document))
    side_curies = _identifier_curies(side_document)
    colliding_curies = sorted({curie for curie in side_curies if curie in main_curies})
    remaining_curies = [curie for curie in side_curies if curie not in main_curies]

    # Removing every identifier would leave a document that identifies nothing,
    # so leave the pair for a strategy that can account for it
    if not colliding_curies or not remaining_curies:
        return None

    logger.debug(
        "[Task %d] Pull %d colliding CURIE(s) from document %s, keeping them on "
        "the biolink:Protein document %s",
        task_id,
        len(colliding_curies),
        side_document["_id"],
        main_document["_id"],
    )
    return pymongo.UpdateOne(
        {
            "_id": side_document["_id"],
            # The check above only proves this pull spares an identifier in the
            # snapshot it was planned from. A document sharing one CURIE with a
            # Protein clique can share another with a different one, and those
            # pulls compose: each spares something on its own and together they
            # empty the document. Restate the requirement as part of the filter so
            # the server evaluates it against the document as it stands at write
            # time, whichever batch or worker gets there first.
            "identifiers": {"$elemMatch": {"i": {"$nin": colliding_curies}}},
        },
        {"$pull": {"identifiers": {"i": {"$in": colliding_curies}}}},
    )


def _resolve_document_pair(
    task_id: int, documents: list[dict], duplicate_curie: str
) -> tuple[Union[object, None], bool, list[dict]]:
    # Compare semantic CURIE-set size rather than raw array length: repeats do not
    # make a document a superset. For equal sets, prefer an already-deduplicated
    # survivor, then use _id only as the deterministic final tiebreak.
    more_identifiers_doc, less_identifiers_doc = sorted(
        documents,
        key=lambda document: (
            -len(set(_identifier_curies(document))),
            len(document["identifiers"]),
            str(document["_id"]),
        ),
    )

    more_is_protein = _document_has_type(more_identifiers_doc, "biolink:Protein")
    less_is_protein = _document_has_type(less_identifiers_doc, "biolink:Protein")

    if more_is_protein != less_is_protein:
        protein_document = (
            more_identifiers_doc if more_is_protein else less_identifiers_doc
        )
        non_protein_document = (
            less_identifiers_doc if more_is_protein else more_identifiers_doc
        )
        # Protein owns every shared CURIE. Delete the non-Protein document only
        # when Protein covers its complete CURIE set; otherwise keep both and pull
        # the intersection from the non-Protein side. Running the generic subset
        # rule first could instead delete a smaller Protein clique.
        operation = _evaluate_document_subset(
            task_id, protein_document, non_protein_document
        )
        if operation is not None:
            return operation, False, [protein_document]

        operation = _evaluate_document_intersection(
            task_id, more_identifiers_doc, less_identifiers_doc
        )
        if operation is not None:
            return operation, False, [more_identifiers_doc, less_identifiers_doc]
    else:
        operation = _evaluate_document_subset(
            task_id, more_identifiers_doc, less_identifiers_doc
        )
        if operation is not None:
            return operation, False, [more_identifiers_doc]

        operation = _evaluate_document_intersection(
            task_id, more_identifiers_doc, less_identifiers_doc
        )
        if operation is not None:
            return operation, False, [more_identifiers_doc, less_identifiers_doc]

    logger.critical(
        "[Task %d] Unable to resolve duplicate CURIE %s: neither the subset "
        "nor the intersection of the 2 documents sharing it can be evaluated",
        task_id,
        duplicate_curie,
    )
    return None, True, [more_identifiers_doc, less_identifiers_doc]


def _curie_duplication_batch_handler(
    task_id: int, curies: list[tuple[str, ...]], collection_name: str
) -> tuple[int, int, int]:
    """
    `curies` holds the sqlite rows streamed by `_iter_duplicate_curies`, so each
    entry is a row tuple whose first column is the CURIE.

    Returns the number of operations MongoDB reports as applied and the number of
    CURIEs no strategy could resolve. The corrections figure comes from the write
    result rather than the size of the request buffer, because two CURIEs shared
    by the same pair of documents queue two requests that apply once.
    """

    num_retry = 10
    counter = 0
    collection = None

    # Due to potential thread starvation from the job-manager heartbeat (which blocks for reasons
    # I'm not quite sure of) we need a potential retry mechanism in case we happen to get starved
    # of resources before we can legitmately connect
    upload_database = get_src_db()
    while collection is None:
        try:
            collection = pymongo.collection.Collection(
                database=upload_database, name=collection_name
            )
        except pymongo.errors.ServerSelectionTimeoutError as mongo_timeout_error:
            counter += 1
            logger.exception(mongo_timeout_error)

            if counter >= num_retry:
                raise mongo_timeout_error

            logger.info(
                "Unable to connect to [%s]@<%s>. Re-attempting connection. %d attempts left",
                upload_database,
                collection_name,
                num_retry - counter,
            )

    # Trims are kept apart from pair repairs so their matched count remains useful.
    # They are also coalesced by document below because one trim removes every
    # repeated CURIE in that identifier array. Final validation, rather than a CAS
    # miss alone, determines whether any repeat survived.
    trim_operations = {}
    pair_operations = []
    inspected_document_ids = set()
    unresolved_count = 0
    for result in curies:
        # Named distinctly from the identifier documents inspected below, which
        # otherwise shadow it
        duplicate_curie = result[0]

        documents = list(collection.find({"identifiers.i": duplicate_curie}))
        inspected_document_ids.update(document["_id"] for document in documents)

        # Handle case where the identifier.i is duplicated within the same document
        if len(documents) == 1:
            operation, unresolved = _deduplicate_document_identifiers(
                task_id, documents[0]
            )
            if not unresolved and operation is not None:
                # One trim deduplicates every CURIE in the document. Coalescing by
                # _id prevents later candidates from queueing the same CAS again
                # and turning an already-resolved no-op into a false unresolved.
                trim_operations.setdefault(documents[0]["_id"], operation)
            operation = None
            operations = pair_operations
        # Handle case where the identifier.i is spread across 2 documents
        elif len(documents) == 2:
            operation, unresolved, surviving_documents = _resolve_document_pair(
                task_id, documents, duplicate_curie
            )
            for surviving_document in surviving_documents:
                trim_operation, _trim_unresolved = _deduplicate_document_identifiers(
                    task_id, surviving_document
                )
                if trim_operation is not None:
                    trim_operations.setdefault(
                        surviving_document["_id"], trim_operation
                    )
            operations = pair_operations
        # A CURIE the identifier shards counted more than once should be found in
        # 1 or 2 documents. Anything else is outside what the branches above can
        # reason about, so leave the documents as they are and report it.
        else:
            operation, unresolved, operations = None, True, pair_operations
            logger.critical(
                "[Task %d] Unable to resolve duplicate CURIE %s: found in %d "
                "document(s), expected 1 or 2",
                task_id,
                duplicate_curie,
                len(documents),
            )

        if unresolved:
            unresolved_count += 1
        elif operation is not None:
            operations.append(operation)

    trim_requests = list(trim_operations.values())
    request_count = len(trim_requests) + len(pair_operations)
    if request_count == 0:
        logger.debug(
            "[Task %d] Bulk writing found no changes to collection to apply", task_id
        )
        return task_id, 0, unresolved_count

    logger.debug(
        "[Task %d] Bulk writing %s changes to collection", task_id, request_count
    )
    correction_count = 0

    if trim_requests:
        trim_result = collection.bulk_write(trim_requests)
        correction_count += trim_result.modified_count
        # A miss is not automatically unresolved: another repair may already have
        # deduplicated the same document. The direct validation pass below is the
        # authoritative check, so report misses here without guessing their state.
        missed_trim_count = len(trim_requests) - trim_result.matched_count
        if missed_trim_count > 0:
            logger.warning(
                "[Task %d] %d repeated-CURIE trim(s) did not apply because their "
                "document changed underneath them; final validation will recheck",
                task_id,
                missed_trim_count,
            )

    if pair_operations:
        pair_result = collection.bulk_write(pair_operations)
        correction_count += pair_result.deleted_count + pair_result.modified_count

    _validate_nonempty_repair_documents(
        collection, inspected_document_ids, task_id=task_id
    )

    # Correction counts are operations MongoDB applied, so they do not establish
    # that identifiers.i is unique afterwards: a pull the server declined in order
    # to keep a document non-empty also applies nothing. Validating the collection
    # is what settles that.
    if correction_count < request_count:
        logger.debug(
            "[Task %d] %d of %d write request(s) applied; the rest were already "
            "satisfied or were declined by their filter",
            task_id,
            correction_count,
            request_count,
        )
    return task_id, correction_count, unresolved_count


def _validate_nonempty_repair_documents(
    collection, document_ids: set, task_id: int
) -> None:
    """Verify every surviving document inspected by a repair still identifies something."""
    if not document_ids:
        return

    projection = {"_id": 1}
    empty_documents = collection.find(
        {"_id": {"$in": list(document_ids)}, "identifiers": []}, projection
    )
    empty_document_ids = [document["_id"] for document in empty_documents]
    if empty_document_ids:
        raise NodeNormCollectionValidationError(
            f"[Task {task_id}] Duplicate repair left {len(empty_document_ids)} "
            "document(s) with no identifiers; examples: "
            f"{empty_document_ids[:NODENORM_VALIDATION_FAILURE_SAMPLE_SIZE]!r}"
        )
