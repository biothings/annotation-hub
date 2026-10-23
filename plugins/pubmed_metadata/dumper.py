"""Dumper for the RENCI PubMed metadata export."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import shutil
import time
from urllib.parse import urljoin

import requests
from biothings import config
from biothings.hub.dataload.dumper import DumperException, HTTPDumper

from .release import (
    PubMedReleaseError,
    extract_index_hrefs,
    find_validation_report_filename,
    local_shard_paths,
    parse_validation_report,
    release_date,
    releases_from_index,
    validate_report,
    validate_shard_filenames,
)
from .static import (
    MANIFESTS_DIRECTORY,
    PUBMED_METADATA_ROOT_URL,
)


class PubMedMetadataDumper(HTTPDumper):
    """Discover and download the newest completed PubMed metadata snapshot."""

    SRC_NAME = "pubmed_metadata"
    SRC_ROOT_FOLDER = Path(config.DATA_ARCHIVE_ROOT) / SRC_NAME
    SOURCE_ROOT_URL = PUBMED_METADATA_ROOT_URL

    ARCHIVE = True
    AUTO_UPLOAD = True
    MAX_PARALLEL_DUMP = 4
    SCHEDULE = None

    REQUEST_TIMEOUT = 30
    REQUEST_MAX_ATTEMPTS = 4
    REQUEST_BACKOFF_SECONDS = 2
    RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}
    KEEP_ARCHIVED_RELEASES = getattr(
        config, "PUBMED_METADATA_KEEP_ARCHIVED_RELEASES", 2
    )

    def _get(self, url: str) -> requests.Response:
        for attempt in range(1, self.REQUEST_MAX_ATTEMPTS + 1):
            response = None
            try:
                response = self.client.get(url, timeout=self.REQUEST_TIMEOUT)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                retryable = isinstance(
                    exc, (requests.ConnectionError, requests.Timeout)
                ) or (
                    response is not None
                    and response.status_code in self.RETRYABLE_HTTP_STATUS_CODES
                )
                if not retryable or attempt == self.REQUEST_MAX_ATTEMPTS:
                    raise DumperException(
                        f"Unable to read PubMed release metadata from {url} "
                        f"after {attempt} attempt(s): {exc}"
                    ) from exc
                delay = self.REQUEST_BACKOFF_SECONDS * 2 ** (attempt - 1)
                self.logger.warning(
                    "Retrying PubMed metadata request %s in %s seconds: %s",
                    url,
                    delay,
                    exc,
                )
            finally:
                if response is not None:
                    response.close()
            time.sleep(delay)

    def _release_inventory(
        self,
        release: str,
        get_global_manifest_filenames: Callable[[], tuple[str, ...]],
    ) -> tuple[str, tuple[str, ...], str, str] | None:
        release_url = urljoin(self.SOURCE_ROOT_URL, f"{release}/")
        index_response = self._get(release_url)
        filenames = extract_index_hrefs(index_response.text)

        report_filename = find_validation_report_filename(filenames, release)
        if report_filename is not None:
            report_url = urljoin(release_url, report_filename)
        else:
            if f"{MANIFESTS_DIRECTORY}/" in filenames:
                release_manifests_url = urljoin(release_url, f"{MANIFESTS_DIRECTORY}/")
                release_manifests_response = self._get(release_manifests_url)
                release_manifest_filenames = extract_index_hrefs(
                    release_manifests_response.text
                )
                report_filename = find_validation_report_filename(
                    release_manifest_filenames, release
                )
                report_url = (
                    urljoin(release_manifests_url, report_filename)
                    if report_filename is not None
                    else None
                )
            else:
                report_filename = find_validation_report_filename(
                    get_global_manifest_filenames(), release
                )
                report_url = (
                    urljoin(
                        self.SOURCE_ROOT_URL, f"{MANIFESTS_DIRECTORY}/{report_filename}"
                    )
                    if report_filename is not None
                    else None
                )

        if report_filename is None:
            try:
                validate_shard_filenames(filenames)
            except PubMedReleaseError:
                self.logger.info("Ignoring incomplete PubMed release %s", release)
            else:
                self.logger.warning(
                    "PubMed release %s lists contiguous shards but no recognized "
                    "completion report; check the upstream report layout",
                    release,
                    extra={"notify": True},
                )
            return None

        try:
            shard_filenames = validate_shard_filenames(filenames)
        except PubMedReleaseError as exc:
            raise DumperException(f"Invalid PubMed release {release}: {exc}") from exc

        report_response = self._get(report_url)
        try:
            report = parse_validation_report(report_response.content)
            validate_report(report, shard_filenames)
        except PubMedReleaseError as exc:
            raise DumperException(f"Invalid PubMed release {release}: {exc}") from exc

        return release_url, shard_filenames, report_url, report_filename

    def get_release(self) -> str:
        """Return the newest release with a valid published completion report."""

        root_response = self._get(self.SOURCE_ROOT_URL)
        releases = releases_from_index(root_response.text, logger=self.logger)
        if not releases:
            raise DumperException(
                f"No dated PubMed releases found at {self.SOURCE_ROOT_URL}"
            )

        root_filenames = extract_index_hrefs(root_response.text)
        manifest_filenames: tuple[str, ...] | None = None

        def get_global_manifest_filenames() -> tuple[str, ...]:
            nonlocal manifest_filenames
            if manifest_filenames is None:
                if f"{MANIFESTS_DIRECTORY}/" not in root_filenames:
                    manifest_filenames = ()
                else:
                    manifests_url = urljoin(
                        self.SOURCE_ROOT_URL, f"{MANIFESTS_DIRECTORY}/"
                    )
                    manifests_response = self._get(manifests_url)
                    manifest_filenames = extract_index_hrefs(manifests_response.text)
            return manifest_filenames

        for release in releases:
            inventory = self._release_inventory(release, get_global_manifest_filenames)
            if inventory is None:
                continue
            (
                self.release_url,
                self.release_shard_filenames,
                self.release_validation_report_url,
                self.release_validation_report_filename,
            ) = inventory
            return release

        raise DumperException(
            f"No completed PubMed release found at {self.SOURCE_ROOT_URL}"
        )

    def set_release(self) -> None:
        """Set the SDK release value using upstream release discovery."""

        candidate = self.get_release()
        download = self.src_doc.get("download", {})
        for previous in (self.current_release, download.get("last_successful_release")):
            if not previous:
                continue
            try:
                previous_date = release_date(previous)
            except PubMedReleaseError:
                self.logger.warning("Current PubMed release %r is not dated", previous)
                continue
            if release_date(candidate) < previous_date:
                raise DumperException(
                    f"Refusing to replace PubMed release {previous} with older release {candidate}"
                )
        self.release = candidate

    async def dump(self, steps=None, force=False, job_manager=None, **kwargs):
        # The SDK also forces retries internally. Preserve the operator's intent.
        self._explicit_force = force
        self._check_only = kwargs.get("check_only", False)
        try:
            return await super().dump(
                steps=steps, force=force, job_manager=job_manager, **kwargs
            )
        finally:
            del self._explicit_force
            del self._check_only

    def register_status(self, status, *args, **kwargs):
        """Keep successful-release identity when SDK download state is replaced."""

        previous = self.src_doc.get("download", {})
        successful_release = previous.get("last_successful_release")
        if previous.get("status") == "success":
            successful_release = previous.get("release")
        if status == "success":
            successful_release = self.release or self.current_release
        retry_download = previous.get(
            "retry_download",
            previous.get("status") in {"failed", "downloading", "canceled"},
        )
        if status in {"success", "downloading"}:
            retry_download = status == "downloading"
        download = dict(kwargs.pop("download", {}))
        download["retry_download"] = retry_download
        if successful_release:
            download["last_successful_release"] = successful_release
        return super().register_status(status, *args, download=download, **kwargs)

    def create_todump_list(self, force: bool = False) -> None:
        """Queue every file in a newer, fully validated release."""

        self.to_dump = []
        self.release = None
        self.set_release()

        download = self.src_doc.get("download", {})
        explicit_force = getattr(self, "_explicit_force", force)
        reusable = download.get("status") == "success" or (
            download.get("last_successful_release") == self.current_release
            and not download.get("retry_download", True)
        )
        if not explicit_force and self.release == self.current_release and reusable:
            try:
                paths = local_shard_paths(self.current_data_folder)
                if tuple(path.name for path in paths) == self.release_shard_filenames:
                    self.logger.info(
                        "PubMed release %s is already complete locally",
                        self.release,
                    )
                    if download.get("status") != "success" and not getattr(
                        self, "_check_only", False
                    ):
                        self.register_status("success")
                    return
            except PubMedReleaseError as exc:
                self.logger.warning(
                    "Current PubMed files need to be downloaded again: %s",
                    exc,
                )

        data_folder = Path(self.new_data_folder)
        filenames = self.release_shard_filenames
        self.to_dump.extend(
            {
                "remote": urljoin(self.release_url, filename),
                "local": str(data_folder / filename),
            }
            for filename in filenames
        )
        self.to_dump.append(
            {
                "remote": self.release_validation_report_url,
                "local": str(data_folder / self.release_validation_report_filename),
            }
        )

    def post_dump(self, *args, **kwargs) -> None:
        """Revalidate the downloaded release before marking it successful."""

        try:
            local_shard_paths(self.new_data_folder)
        except PubMedReleaseError as exc:
            release = self.release or self.current_release
            raise DumperException(
                f"Downloaded PubMed release {release} is invalid: {exc}"
            ) from exc
        super().post_dump(*args, **kwargs)
        self._prune_archived_releases()

    def _prune_archived_releases(self) -> None:
        """Keep this validated snapshot and the configured rollback snapshots."""

        keep = self.KEEP_ARCHIVED_RELEASES
        if keep is None:
            return
        if isinstance(keep, bool) or not isinstance(keep, int) or keep < 2:
            raise DumperException(
                "PUBMED_METADATA_KEEP_ARCHIVED_RELEASES must be at least 2 or None"
            )
        current = Path(self.new_data_folder)
        if (
            current.is_symlink()
            or current.parent.resolve() != Path(self.src_root_folder).resolve()
        ):
            self.logger.warning(
                "Skipping archive cleanup outside the PubMed source root: %s", current
            )
            return
        try:
            current_date = release_date(current.name)
        except PubMedReleaseError:
            self.logger.warning(
                "Skipping archive cleanup for undated folder %s", current
            )
            return
        older = []
        try:
            folders = list(current.parent.iterdir())
        except OSError as exc:
            self.logger.warning(
                "Unable to list PubMed archives: %s", exc, extra={"notify": True}
            )
            return
        for folder in folders:
            if folder.is_symlink() or not folder.is_dir() or folder == current:
                continue
            try:
                date = release_date(folder.name)
                if date >= current_date:
                    continue
                local_shard_paths(folder)
            except PubMedReleaseError:
                continue
            older.append((date, folder))
        last_successful = self.src_doc.get("download", {}).get(
            "last_successful_release"
        )
        older.sort(
            key=lambda item: (item[1].name != last_successful, -item[0].toordinal())
        )
        for _, folder in older[keep - 1 :]:
            try:
                shutil.rmtree(folder)
                self.logger.info("Removed old PubMed snapshot %s", folder)
            except OSError as exc:
                self.logger.warning(
                    "Unable to remove old PubMed snapshot %s: %s",
                    folder,
                    exc,
                    extra={"notify": True},
                )
