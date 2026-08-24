"""Uploader for the RENCI PubMed metadata export."""

from pathlib import Path

from biothings.hub.dataload.uploader import ParallelizedSourceUploader

from .mapping import get_pubmed_metadata_mapping
from .parser import iter_pubmed_metadata_documents
from .release import PubMedReleaseError, local_shard_paths, release_date
from .static import (
    MANIFEST_VALIDATION_REPORT_FILENAME_FORMAT,
    NLM_TERMS_URL,
    PUBMED2DB_URL,
    PUBMED_METADATA_ROOT_URL,
    VALIDATION_REPORT_FILENAME,
)


class PubMedMetadataUploader(ParallelizedSourceUploader):
    """Stream each upstream shard into one source collection."""

    name = "pubmed_metadata"
    MAX_PARALLEL_UPLOAD = 4
    keep_archive = 1
    __metadata__ = {
        "src_meta": {
            "url": PUBMED_METADATA_ROOT_URL,
            "license": "NLM PubMed Terms and Conditions",
            "license_url": NLM_TERMS_URL,
            "description": (
                "PubMed citation metadata exported for Translator by "
                f"{PUBMED2DB_URL}"
            ),
        }
    }

    def jobs(self) -> list[tuple[str]]:
        data_folder = Path(self.data_folder)
        try:
            legacy_report_path = data_folder / VALIDATION_REPORT_FILENAME
            if legacy_report_path.is_file():
                report_filename = VALIDATION_REPORT_FILENAME
            else:
                report_filename = MANIFEST_VALIDATION_REPORT_FILENAME_FORMAT.format(
                    release_date(data_folder.name)
                )
            shard_paths = local_shard_paths(data_folder, report_filename)
        except PubMedReleaseError as exc:
            raise FileNotFoundError(
                "PubMed metadata upload requires the complete validated "
                f"release: {exc}"
            ) from exc
        return [(str(path),) for path in shard_paths]

    def load_data(self, data_path: str):
        return iter_pubmed_metadata_documents(data_path)

    @classmethod
    def get_mapping(cls) -> dict:
        return get_pubmed_metadata_mapping()
