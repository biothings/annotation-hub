import asyncio
import concurrent.futures
import gzip
import math
import os
import shutil
import time
from functools import partial
from pathlib import Path
from typing import override, Union
from urllib.parse import urlparse

from biothings import config
from biothings.hub.dataload.dumper import DumperException, LastModifiedHTTPDumper
from biothings.utils.manager import JobManager
from requests import exceptions as requests_exceptions


from .static import (
    BASE_URL,
    SYNONYM_BIG_FILE_COLLECTION,
    SYNONYM_FILE_COLLECTION,
)

logger = config.logger


class _RetryableRangeDownloadError(Exception):
    pass


file_collections = {
    "synonym": SYNONYM_FILE_COLLECTION,
    "synonym-large": SYNONYM_BIG_FILE_COLLECTION,
}


class NameResDumper(LastModifiedHTTPDumper):
    SRC_NAME = "nameres"
    SRC_ROOT_FOLDER = Path(config.DATA_ARCHIVE_ROOT) / SRC_NAME
    SCHEDULE = "0 2 1 * *"  # Monthly updates on the 1st of every month
    AUTO_UPLOAD = True
    SUFFIX_ATTR = "release"

    ARCHIVE = False
    SCHEDULE = None

    MAX_PARALLEL_NORMAL_FILES = 4
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
        self.to_dump = []
        self.to_dump_large = []

    def create_todump_list(self, force: bool = False) -> None:
        self.set_release()
        local_datafolder = Path(self.current_data_folder)

        for synonym_file in file_collections["synonym"]:
            self.to_dump.append(
                {
                    "remote": f"{BASE_URL}/synonyms/{synonym_file}",
                    "local": str(local_datafolder.joinpath(synonym_file)),
                }
            )

        for synonym_file, file_partitions in file_collections["synonym-large"].items():
            self.to_dump_large.append(
                {
                    "remoteurl": f"{BASE_URL}/synonyms/{synonym_file}",
                    "localfile": str(local_datafolder.joinpath(synonym_file)),
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
        self.unprepare()

        for batch_start in range(
            0, len(self.to_dump), self.MAX_PARALLEL_NORMAL_FILES
        ):
            jobs = []
            batch = self.to_dump[
                batch_start : batch_start + self.MAX_PARALLEL_NORMAL_FILES
            ]
            for file_mapping in batch:
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
        num_partitions = 2
        self._download_in_ranges(
            remoteurl,
            localfile,
            num_partitions,
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
            chunks = self.get_range_chunks(
                remoteurl, num_partitions, headers=headers
            )
        chunk_paths = [
            Path(f"{local_path}.part{index}") for index in range(len(chunks))
        ]

        workers = min(max_workers, len(chunks))
        future_to_chunk = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=workers
            ) as executor:
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
                key: value
                for key, value in headers.items()
                if key.lower() != "range"
            }

        for attempt in range(1, self.RANGE_DOWNLOAD_MAX_ATTEMPTS + 1):
            response = None
            retry_error = None
            try:
                response = self.client.head(url, **request_arguments)
                if response.status_code >= 400:
                    message = (
                        f"Unable to determine size of '{url}' "
                        f"(status: {response.status_code}, "
                        f"reason: {response.reason})"
                    )
                    if response.status_code not in self.RETRYABLE_HTTP_STATUS_CODES:
                        raise DumperException(message)
                    retry_error = DumperException(message)
                else:
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
                            f"Unable to determine size of '{url}': "
                            "invalid Content-Length"
                        )
                    return size
            except (
                requests_exceptions.ConnectionError,
                requests_exceptions.Timeout,
            ) as exc:
                retry_error = exc
            except requests_exceptions.RequestException as exc:
                raise DumperException(
                    f"Unable to determine size of '{url}': {exc}"
                ) from exc
            finally:
                if response is not None:
                    response.close()

            if attempt == self.RANGE_DOWNLOAD_MAX_ATTEMPTS:
                raise DumperException(
                    f"Unable to determine size of '{url}' after {attempt} "
                    f"attempts: {retry_error}"
                ) from retry_error

            delay = self.RANGE_DOWNLOAD_BACKOFF_SECONDS * (2 ** (attempt - 1))
            self.logger.warning(
                "Retrying size request for '%s' in %d seconds after "
                "attempt %d failed: %s",
                url,
                delay,
                attempt,
                retry_error,
            )
            time.sleep(delay)

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
                    if (
                        response.status_code
                        not in self.RETRYABLE_HTTP_STATUS_CODES
                    ):
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

    def set_release(self) -> None:
        """
        Parses the BASE_URL to extract the data from the url pathing
        """
        parse_result = urlparse(BASE_URL)
        self.release = parse_result.path.split("/")[-1]

    def post_dump(self, *args, **kwargs):
        """
        Post dump processing:
        - unzip all gzipped file
        """
        # Force creation of the to_dump collection
        self.create_todump_list(force=True)

        def decompress_file(archive_file: Union[str, Path]) -> None:
            decompressed_file = archive_file.with_name(archive_file.stem)
            with gzip.open(archive_file, "rb") as input_handle, open(
                decompressed_file, "wb"
            ) as output_handle:
                logger.info(
                    "Decompressing %s -> %s", archive_file.name, decompressed_file.name
                )
                shutil.copyfileobj(input_handle, output_handle)
            logger.debug("Deleting archive file %s", archive_file.name)
            archive_file.unlink()

        thread_futures = []
        data_directory = Path(self.current_data_folder).resolve().absolute()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=os.cpu_count()
        ) as executor:
            for archive_file in data_directory.glob("**/*.gz"):
                arguments = {"archive_file": archive_file}
                future = executor.submit(decompress_file, **arguments)
                thread_futures.append(future)
            concurrent.futures.wait(
                thread_futures,
                timeout=None,
                return_when=concurrent.futures.ALL_COMPLETED,
            )
