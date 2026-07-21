"""Uploader for the RENCI PubMed metadata export."""

from pathlib import Path

from biothings.hub.dataload.uploader import ParallelizedSourceUploader

from .parser import iter_pubmed_metadata_documents
from .static import (
    BASE_URL,
    NLM_TERMS_URL,
    PUBMED2DB_URL,
    PUBMED_METADATA_FILES,
)


def _stored_keyword() -> dict:
    """Map a value into ``_source`` without building a search index for it."""

    return {"type": "keyword", "index": False, "doc_values": False}


class PubMedMetadataUploader(ParallelizedSourceUploader):
    """Stream each upstream shard into one source collection."""

    name = "pubmed_metadata"
    MAX_PARALLEL_UPLOAD = 4
    keep_archive = 1
    __metadata__ = {
        "src_meta": {
            "url": BASE_URL,
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
        shard_paths = [data_folder / filename for filename in PUBMED_METADATA_FILES]
        missing_paths = [path.name for path in shard_paths if not path.is_file()]
        if missing_paths:
            raise FileNotFoundError(
                "PubMed metadata upload requires all 16 shards; missing: "
                + ", ".join(missing_paths)
            )
        return [(str(path),) for path in shard_paths]

    def load_data(self, data_path: str):
        return iter_pubmed_metadata_documents(data_path)

    @classmethod
    def get_mapping(cls) -> dict:
        return {
            "pubmed": {
                "properties": {
                    "journal_name": _stored_keyword(),
                    "journal_abbrev": _stored_keyword(),
                    "article_title": {"type": "text", "index": False},
                    "volume": _stored_keyword(),
                    "issue": _stored_keyword(),
                    "pub_year": _stored_keyword(),
                    "pub_month": _stored_keyword(),
                    "pub_day": _stored_keyword(),
                    "abstract": {"type": "text", "index": False},
                }
            }
        }
