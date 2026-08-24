"""Dumper for the RENCI PubMed metadata export."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from urllib.parse import urljoin

import requests
from biothings import config
from biothings.hub.dataload.dumper import DumperException, HTTPDumper

from .release import (
    PubMedReleaseError,
    extract_index_hrefs,
    local_shard_paths,
    parse_validation_report,
    release_date,
    releases_from_index,
    validate_report,
    validate_shard_filenames,
)
from .static import (
    MANIFESTS_DIRECTORY,
    MANIFEST_VALIDATION_REPORT_FILENAME_FORMAT,
    PUBMED_METADATA_ROOT_URL,
    VALIDATION_REPORT_FILENAME,
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

    def _get(self, url: str) -> requests.Response:
        try:
            response = self.client.get(url, timeout=self.REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            raise DumperException(
                f"Unable to read PubMed release metadata from {url}: {exc}"
            ) from exc

    def _release_inventory(
        self,
        release: str,
        get_manifest_filenames: Callable[[], tuple[str, ...]],
    ) -> tuple[str, tuple[str, ...], str, str] | None:
        release_url = urljoin(self.SOURCE_ROOT_URL, f"{release}/")
        index_response = self._get(release_url)
        filenames = extract_index_hrefs(index_response.text)

        if VALIDATION_REPORT_FILENAME in filenames:
            report_filename = VALIDATION_REPORT_FILENAME
            report_url = urljoin(release_url, report_filename)
        else:
            report_filename = MANIFEST_VALIDATION_REPORT_FILENAME_FORMAT.format(
                release_date(release)
            )
            if report_filename not in get_manifest_filenames():
                self.logger.info(
                    "Ignoring incomplete PubMed release %s: "
                    "no matching completion report",
                    release,
                )
                return None
            report_url = urljoin(
                self.SOURCE_ROOT_URL,
                f"{MANIFESTS_DIRECTORY}/{report_filename}",
            )

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
        releases = releases_from_index(root_response.text)
        if not releases:
            raise DumperException(
                f"No dated PubMed releases found at {self.SOURCE_ROOT_URL}"
            )

        root_filenames = extract_index_hrefs(root_response.text)
        manifest_filenames: tuple[str, ...] | None = None

        def get_manifest_filenames() -> tuple[str, ...]:
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
            inventory = self._release_inventory(release, get_manifest_filenames)
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

        self.release = self.get_release()

    def create_todump_list(self, force: bool = False) -> None:
        """Queue every file in a newer, fully validated release."""

        self.to_dump = []
        self.set_release()

        if not force and self.current_release:
            try:
                if release_date(self.release) <= release_date(self.current_release):
                    self.logger.info(
                        "PubMed release %s is not newer than current release %s",
                        self.release,
                        self.current_release,
                    )
                    return
            except PubMedReleaseError:
                self.logger.warning(
                    "Current PubMed release %r is not a dated release; downloading %s",
                    self.current_release,
                    self.release,
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
            local_shard_paths(
                self.new_data_folder,
                getattr(
                    self,
                    "release_validation_report_filename",
                    VALIDATION_REPORT_FILENAME,
                ),
            )
        except PubMedReleaseError as exc:
            raise DumperException(
                f"Downloaded PubMed release {self.release} is invalid: {exc}"
            ) from exc
        super().post_dump(*args, **kwargs)
