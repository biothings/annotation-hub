import asyncio
import concurrent.futures
import json
import math
import os
import shutil
import sqlite3
import time
from functools import partial
from pathlib import Path
from typing import override, Union

from biothings import config
from biothings.hub.dataload.dumper import DumperException, LastModifiedHTTPDumper
from biothings.utils.manager import JobManager
from requests import exceptions as requests_exceptions

from .release import NodeNormReleaseError, parse_version_marker, validate_release
from .static import (
    BABEL_OUTPUT_ROOT,
    CONFLATION_LOOKUP_DATABASE,
    NODENORM_BIG_FILE_COLLECTION,
    NODENORM_CONFLATION_COLLECTION,
    NODENORM_FILE_COLLECTION,
    VERSION_URL,
)

logger = config.logger


class _RetryableRangeDownloadError(Exception):
    pass


class NodeNormDumper(LastModifiedHTTPDumper):
    SRC_NAME = "nodenorm"
    SRC_ROOT_FOLDER = Path(config.DATA_ARCHIVE_ROOT) / SRC_NAME
    AUTO_UPLOAD = True
    SUFFIX_ATTR = "release"

    ARCHIVE = False
    SCHEDULE = None

    VERSION_URL = VERSION_URL
    SOURCE_ROOT_URL = BABEL_OUTPUT_ROOT
    VERSION_REQUEST_TIMEOUT = 30
    PINNED_RELEASE = "2026jul22"

    FILE_COLLECTION = NODENORM_FILE_COLLECTION
    BIG_FILE_COLLECTION = NODENORM_BIG_FILE_COLLECTION
    CONFLATION_COLLECTION = NODENORM_CONFLATION_COLLECTION

    MAX_PARALLEL_LARGE_FILES = 2
    LARGE_FILE_RANGE_WORKERS = 8
    NORMAL_FILE_RANGE_WORKERS = 2
    RANGE_DOWNLOAD_MAX_ATTEMPTS = 4
    RANGE_DOWNLOAD_BACKOFF_SECONDS = 2
    RANGE_REQUEST_TIMEOUT = (15, 300)
    RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        src_name: str = None,
        src_root_folder: str = None,
        log_folder: str = None,
        archive: bool = None,
    ):
        super().__init__(src_name, src_root_folder, log_folder, archive)
        self.to_dump_large = []

    def create_todump_list(self, force: bool = False) -> None:
        self.to_dump = []
        self.to_dump_large = []
        self.set_release()

        if not force and self.current_release:
            try:
                validate_release(self.current_release)
                if self.release == self.current_release:
                    self.logger.info(
                        "NodeNorm release %s is already current",
                        self.release,
                    )
                    return
            except NodeNormReleaseError:
                self.logger.warning(
                    "Current NodeNorm release %r is invalid; downloading %s",
                    self.current_release,
                    self.release,
                )

        release_url = f"{self.SOURCE_ROOT_URL}/{self.release}"
        local_datafolder = Path(self.new_data_folder)

        for nodenorm_file in self.FILE_COLLECTION:
            self.to_dump.append(
                {
                    "remote": f"{release_url}/compendia/{nodenorm_file}",
                    "local": str(local_datafolder.joinpath(nodenorm_file)),
                }
            )

        for nodenorm_file in self.CONFLATION_COLLECTION:
            self.to_dump.append(
                {
                    "remote": f"{release_url}/conflation/{nodenorm_file}",
                    "local": str(local_datafolder.joinpath(nodenorm_file)),
                }
            )

        for nodenorm_file, file_partitions in self.BIG_FILE_COLLECTION.items():
            self.to_dump_large.append(
                {
                    "remoteurl": f"{release_url}/compendia/{nodenorm_file}",
                    "localfile": str(local_datafolder.joinpath(nodenorm_file)),
                    "num_partitions": file_partitions,
                }
            )

    @override
    async def do_dump(self, job_manager: JobManager = None):
        await self._handle_normal_size_files(job_manager)
        await self._handle_large_size_files(job_manager)
        self.logger.info("%s successfully downloaded", self.SRC_NAME)

    async def _handle_normal_size_files(self, job_manager: JobManager):
        self.logger.info("%d file(s) to download (normal size)", len(self.to_dump))
        jobs = []
        self.unprepare()
        for file_mapping in self.to_dump:
            remote = file_mapping["remote"]
            local = file_mapping["local"]

            pinfo = self.get_pinfo()
            pinfo["step"] = "dump"
            pinfo["description"] = remote

            job = await job_manager.defer_to_process(
                pinfo, partial(self.download, remote, local)
            )
            jobs.append(job)

        await asyncio.gather(*jobs)
        self.to_dump = []

    async def _handle_large_size_files(self, job_manager: JobManager):
        self.logger.info("%d file(s) to download (large size)", len(self.to_dump_large))
        self.unprepare()

        for batch_start in range(
            0, len(self.to_dump_large), self.MAX_PARALLEL_LARGE_FILES
        ):
            jobs = []
            batch = self.to_dump_large[
                batch_start : batch_start + self.MAX_PARALLEL_LARGE_FILES
            ]
            for file_mapping in batch:
                pinfo = self.get_pinfo()
                pinfo["step"] = "dump"
                pinfo["description"] = file_mapping["remoteurl"]

                job = await job_manager.defer_to_process(
                    pinfo, partial(self.large_download, **file_mapping)
                )
                jobs.append(job)
            await asyncio.gather(*jobs)

        self.to_dump_large = []

    def large_download(
        self, remoteurl: str, localfile: Union[str, Path], num_partitions: int = 100
    ) -> None:
        """
        Handles downloading of particularly large files.
        It breaks them into smaller chunks.
        """
        logger.info(
            "Downloading (large) file %s -> %s | Partitions %s",
            remoteurl,
            localfile,
            num_partitions,
        )
        self._download_in_ranges(
            remoteurl,
            localfile,
            num_partitions,
            max_workers=self.LARGE_FILE_RANGE_WORKERS,
        )

    def download(
        self, remoteurl: str, localfile: Union[str, Path], headers: dict = None
    ) -> None:
        """
        Handles downloading of remote files over HTTP to the local file system

        Leverages multiple threads to download the remote file in multiple chunks
        concurrently and then combines them at the end
        """
        logger.info(
            "Downloading (normal) file %s -> %s | Partitions %s",
            remoteurl,
            localfile,
            10,
        )
        self._download_in_ranges(
            remoteurl,
            localfile,
            10,
            max_workers=self.NORMAL_FILE_RANGE_WORKERS,
            headers=headers,
        )

    def _download_in_ranges(
        self,
        remoteurl: str,
        localfile: Union[str, Path],
        num_partitions: int,
        max_workers: int,
        headers: dict = None,
    ) -> None:
        self.prepare_local_folders(localfile)
        local_path = Path(localfile)
        if headers is None:
            chunks = self.get_range_chunks(remoteurl, num_partitions)
        else:
            chunks = self.get_range_chunks(remoteurl, num_partitions, headers=headers)
        chunk_paths = [
            Path(f"{local_path}.part{index}") for index in range(len(chunks))
        ]

        workers = min(max_workers, len(chunks))
        future_to_chunk = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                for chunk_path, (chunk_start, chunk_end) in zip(chunk_paths, chunks):
                    download_arguments = {
                        "url": remoteurl,
                        "start": chunk_start,
                        "end": chunk_end,
                        "output": str(chunk_path),
                    }
                    if headers is not None:
                        download_arguments["headers"] = headers
                    future = executor.submit(
                        self.download_range,
                        **download_arguments,
                    )
                    future_to_chunk[future] = chunk_path

                for future in concurrent.futures.as_completed(future_to_chunk):
                    future.result()
        except Exception:
            for future in future_to_chunk:
                future.cancel()
            self._cleanup_chunk_files(chunk_paths)
            raise

        combined_path = Path(f"{local_path}.tmp")
        combined_path.unlink(missing_ok=True)
        try:
            self._validate_chunk_files(chunk_paths, chunks)
            with combined_path.open("wb") as combined_output:
                for chunk_path in chunk_paths:
                    with chunk_path.open("rb") as partial_input:
                        shutil.copyfileobj(
                            partial_input, combined_output, length=1024 * 1024
                        )
            os.replace(combined_path, local_path)
        except Exception:
            combined_path.unlink(missing_ok=True)
            self._cleanup_chunk_files(chunk_paths)
            raise

        self._cleanup_chunk_files(chunk_paths)
        logger.info("Combined all chunks -> %s", local_path)

    def get_file_size(self, url: str, headers: dict = None) -> int:
        """
        Sends a HEAD request to the specified URL and extracts
        the `Content-Length` header to determine the file size

        Used for determining how to chunk the file download
        """
        request_arguments = {"timeout": self.RANGE_REQUEST_TIMEOUT}
        if headers is not None:
            request_arguments["headers"] = {
                key: value for key, value in headers.items() if key.lower() != "range"
            }
        try:
            response = self.client.head(url, **request_arguments)
        except requests_exceptions.RequestException as exc:
            raise DumperException(
                f"Unable to determine size of '{url}': {exc}"
            ) from exc

        try:
            if response.status_code >= 400:
                raise DumperException(
                    f"Unable to determine size of '{url}' "
                    f"(status: {response.status_code}, reason: {response.reason})"
                )
            content_length = response.headers.get("Content-Length")
            try:
                size = int(content_length)
            except (TypeError, ValueError) as exc:
                raise DumperException(
                    f"Unable to determine size of '{url}': "
                    f"invalid Content-Length {content_length!r}"
                ) from exc
            if size <= 0:
                raise DumperException(
                    f"Unable to determine size of '{url}': invalid Content-Length"
                )
            return size
        finally:
            response.close()

    def get_range_chunks(
        self, url: str, num_partitions: int = 10, headers: dict = None
    ) -> list[tuple[int, int]]:
        """
        Partitions a file into distinct chunks for consuming each chunk within
        a thread concurrently
        """
        if num_partitions <= 0:
            raise ValueError("num_partitions must be greater than zero")

        if headers is None:
            file_size = self.get_file_size(url)
        else:
            file_size = self.get_file_size(url, headers=headers)
        chunk_size = math.ceil(file_size / num_partitions)
        return [
            (chunk_start, min(chunk_start + chunk_size - 1, file_size - 1))
            for chunk_start in range(0, file_size, chunk_size)
        ]

    def download_range(
        self,
        url: str,
        start: int,
        end: int,
        output: str,
        headers: dict = None,
    ) -> None:
        self.logger.debug("Downloading Filepart '%s' as '%s'", url, output)
        request_headers = {
            key: value
            for key, value in (headers or {}).items()
            if key.lower() != "range"
        }
        request_headers["Range"] = f"bytes={start}-{end}"
        expected_content_range = f"bytes {start}-{end}/"
        expected_size = end - start + 1
        output_path = Path(output)
        temporary_path = Path(f"{output}.tmp")
        output_path.unlink(missing_ok=True)
        temporary_path.unlink(missing_ok=True)

        for attempt in range(1, self.RANGE_DOWNLOAD_MAX_ATTEMPTS + 1):
            response = None
            temporary_path.unlink(missing_ok=True)
            try:
                response = self.client.get(
                    url,
                    headers=request_headers,
                    stream=True,
                    timeout=self.RANGE_REQUEST_TIMEOUT,
                )

                if response.status_code != 206:
                    message = (
                        f"Error while downloading '{url}' range {start}-{end} "
                        f"(status: {response.status_code}, reason: {response.reason})"
                    )
                    if response.status_code not in self.RETRYABLE_HTTP_STATUS_CODES:
                        raise DumperException(message)
                    raise _RetryableRangeDownloadError(message)

                content_range = response.headers.get("Content-Range", "")
                if not content_range.startswith(expected_content_range):
                    raise _RetryableRangeDownloadError(
                        f"Invalid Content-Range for '{url}' range {start}-{end}: "
                        f"{content_range!r}"
                    )

                bytes_written = 0
                with temporary_path.open("wb") as file_handle:
                    for response_part in response.iter_content(512 * 1024):
                        if response_part:
                            file_handle.write(response_part)
                            bytes_written += len(response_part)

                if bytes_written != expected_size:
                    raise _RetryableRangeDownloadError(
                        f"Incomplete response for '{url}' range {start}-{end}: "
                        f"expected {expected_size} bytes, received {bytes_written}"
                    )

                os.replace(temporary_path, output_path)
                logger.info(
                    "Chunk Completed | %s | Byte Range [%d, %d]",
                    output,
                    start,
                    end,
                )
                return
            except (
                requests_exceptions.RequestException,
                _RetryableRangeDownloadError,
            ) as exc:
                temporary_path.unlink(missing_ok=True)
                if attempt == self.RANGE_DOWNLOAD_MAX_ATTEMPTS:
                    raise DumperException(
                        f"Failed downloading '{url}' range {start}-{end} "
                        f"after {attempt} attempts: {exc}"
                    ) from exc

                delay = self.RANGE_DOWNLOAD_BACKOFF_SECONDS * (2 ** (attempt - 1))
                self.logger.warning(
                    "Retrying '%s' range %d-%d in %d seconds "
                    "after attempt %d failed: %s",
                    url,
                    start,
                    end,
                    delay,
                    attempt,
                    exc,
                )
                time.sleep(delay)
            except Exception:
                temporary_path.unlink(missing_ok=True)
                raise
            finally:
                if response is not None:
                    response.close()

    @staticmethod
    def _validate_chunk_files(
        chunk_paths: list[Path], chunks: list[tuple[int, int]]
    ) -> None:
        for chunk_path, (chunk_start, chunk_end) in zip(chunk_paths, chunks):
            expected_size = chunk_end - chunk_start + 1
            if not chunk_path.is_file():
                raise DumperException(f"Missing downloaded chunk '{chunk_path}'")
            actual_size = chunk_path.stat().st_size
            if actual_size != expected_size:
                raise DumperException(
                    f"Invalid size for downloaded chunk '{chunk_path}': "
                    f"expected {expected_size} bytes, found {actual_size}"
                )

    @staticmethod
    def _cleanup_chunk_files(chunk_paths: list[Path]) -> None:
        for chunk_path in chunk_paths:
            chunk_path.unlink(missing_ok=True)
            Path(f"{chunk_path}.tmp").unlink(missing_ok=True)

    def get_release(self) -> str:
        """Return the branch override or RENCI's official release marker."""

        if self.PINNED_RELEASE is not None:
            return validate_release(self.PINNED_RELEASE)

        response = None
        try:
            response = self.client.get(
                self.VERSION_URL,
                timeout=self.VERSION_REQUEST_TIMEOUT,
            )
            if response.status_code >= 400:
                raise DumperException(
                    f"Unable to read NodeNorm release marker '{self.VERSION_URL}' "
                    f"(status: {response.status_code}, reason: {response.reason})"
                )
            try:
                return parse_version_marker(response.text)
            except NodeNormReleaseError as exc:
                raise DumperException(
                    f"Invalid NodeNorm release marker '{self.VERSION_URL}': {exc}"
                ) from exc
        except requests_exceptions.RequestException as exc:
            raise DumperException(
                f"Unable to read NodeNorm release marker '{self.VERSION_URL}': {exc}"
            ) from exc
        finally:
            if response is not None:
                response.close()

    def set_release(self) -> None:
        """Set the SDK release value from RENCI's authoritative marker."""

        self.release = self.get_release()

    def post_dump(self, *args, **kwargs):
        data_directory = Path(self.new_data_folder)
        self._generate_conflation_database(data_directory)
        super().post_dump(*args, **kwargs)

    def _generate_conflation_database(
        self, data_directory: Union[str, Path]
    ) -> Union[str, Path]:
        """
        Takes the generated conflation files and creates a sqlite3 database used for looking
        up the conflation identifiers for the supported types of nodes

        Finds the identifiers and then flattens so each identifier found within the conflation list
        points back to the same list of identifiers, so every CURIE within the list points to the
        same list

        Example:
        conflation  | identifiers
        identifier0 | identifer0,identifer1,identifer2,identifer3,identifer4
        identifier1 | identifer0,identifer1,identifer2,identifer3,identifer4
        identifier2 | identifer0,identifer1,identifer2,identifer3,identifer4
        identifier3 | identifer0,identifer1,identifer2,identifer3,identifer4
        identifier4 | identifer0,identifer1,identifer2,identifer3,identifer4
        """
        conflation_database_path = (
            data_directory.joinpath(CONFLATION_LOOKUP_DATABASE).resolve().absolute()
        )
        conflation_database = sqlite3.connect(conflation_database_path)
        cursor = conflation_database.cursor()

        enable_foreign_keys = "PRAGMA foreign_keys = ON;"
        cursor.execute(enable_foreign_keys)
        conflation_existance_check = "DROP TABLE IF EXISTS conflations"
        cursor.execute(conflation_existance_check)

        conflations_table = (
            "CREATE TABLE conflations "
            "("
            "conflation text PRIMARY KEY NOT NULL, "
            "identifiers text NOT NULL, "
            "type text NOT NULL"
            ");"
        )
        cursor.execute(conflations_table)

        conflation_files = [
            data_directory.joinpath("GeneProtein.txt").resolve().absolute(),
            data_directory.joinpath("DrugChemical.txt").resolve().absolute(),
        ]
        for conflation_file in conflation_files:
            if not conflation_file.exists():
                raise OSError(f"Unable to locate conflation file {conflation_file}")

            with open(conflation_file, "r", encoding="utf-8") as handle:
                batch = []
                for line in handle.readlines():
                    identifiers = json.loads(line)

                    # There have been bugs in the past with Babel where duplicate identifiers appear
                    # on the line. This ensures we have unique identifiers in the original order
                    cleaned_identifiers = list(dict.fromkeys(identifiers))

                    identifiers_repr = ",".join(cleaned_identifiers)
                    for identifier in cleaned_identifiers:
                        batch.append(
                            {
                                "conflation": identifier,
                                "identifiers": identifiers_repr,
                                "type": conflation_file.stem,
                            }
                        )

                    if len(batch) >= 10000:
                        cursor.executemany(
                            "INSERT INTO conflations VALUES (:conflation, :identifiers, :type)",
                            batch,
                        )
                        batch = []

        if len(batch) > 0:
            cursor.executemany(
                "INSERT INTO conflations VALUES (:conflation, :identifiers, :type)",
                batch,
            )
            batch = []
        conflation_database.commit()
        conflation_database.close()
