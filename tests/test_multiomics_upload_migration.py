"""Upgrade populated hubs after retiring the Multiomics TSV uploaders."""

from copy import deepcopy
import importlib.util
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "prepare_multiomics_upload_migration", ROOT / "bin" / "prepare_multiomics.py"
)
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)
LOGGER = logging.getLogger("multiomics-upload-migration-test")

SOURCES = (
    (
        "multiomics_clinicaltrials_kp",
        "multiomics_clinical_trials",
        "multiomics_clinical_trials_annotator",
        "4.1.3",
        "4.2.4",
    ),
    (
        "multiomics_drug_approvals_kp",
        "multiomics_drug_approvals",
        "multiomics_drug_approvals_annotator",
        "0.5.6",
        "0.5.7",
    ),
)


class DumpStore:
    """Hub reads return detached documents, just as its DB backend does."""

    def __init__(self, documents):
        self.documents = deepcopy(documents)

    def find_one(self, query):
        return deepcopy(self.documents.get(query["_id"]))

    def save(self, document):
        self.documents[document["_id"]] = deepcopy(document)


class SourceManager:
    def __init__(self, documents, registered):
        self.src_dump = DumpStore(documents)
        self.reset_calls = []
        self.upload_calls = []
        self.upload_manager = SimpleNamespace(
            register={
                main: [type(name, (), {"name": name}) for name in names]
                for main, names in registered.items()
            },
            upload_src=self.upload_src,
        )
        # Source data and master records are separate from src_dump history.
        self.source_data = {
            name: [{"_id": name + ":1", "value": "keep this source data"}]
            for _, legacy, annotator, _, _ in SOURCES
            for name in (legacy, annotator)
        }
        self.src_master = {
            name: {"_id": name, "mapping": {"value": {"type": "keyword"}}}
            for name in self.source_data
        }

    def reset(self, name, key="upload", subkey=None):
        self.reset_calls.append((name, key, subkey))
        document = self.src_dump.find_one({"_id": name})
        del document[key]["jobs"][subkey]
        self.src_dump.save(document)

    def upload_src(self, *args, **kwargs):
        self.upload_calls.append((args, kwargs))
        raise AssertionError("Metadata migration must not schedule uploads")


def _documents():
    documents = {}
    for main, legacy, annotator, old_release, release in SOURCES:
        documents[main] = {
            "_id": main,
            "download": {
                "status": "success",
                "release": release,
                "data_folder": "/archive/" + main + "/" + release,
            },
            "upload": {
                "jobs": {
                    legacy: {
                        "step": legacy,
                        "release": old_release,
                        "status": "success",
                        "started_at": "2026-08-13T10:00:00-07:00",
                    },
                    annotator: {
                        "step": annotator,
                        "release": release,
                        "status": "success",
                        "started_at": "2026-11-13T10:00:00-08:00",
                        "count": 100,
                    },
                },
                "custom": "preserve upload metadata outside jobs",
            },
            "inspect": {
                "jobs": {
                    legacy: {"status": "success", "results": {"count": 12}},
                    annotator: {"status": "success", "results": {"count": 100}},
                }
            },
            "pending": ["inspect"],
            "custom": {"preserve": True},
        }
    documents["unrelated_source"] = {
        "_id": "unrelated_source",
        "upload": {"jobs": {"unrelated": {"release": "1.0"}}},
    }
    return documents


def _registered():
    return {main: [annotator] for main, _, annotator, _, _ in SOURCES}


def _upload_releases(document):
    return {job["release"] for job in document["upload"]["jobs"].values()}


def test_retired_upload_history_no_longer_breaks_build_version_consistency():
    manager = SourceManager(_documents(), _registered())
    before = deepcopy(manager.src_dump.documents)
    collections = deepcopy(manager.source_data)
    masters = deepcopy(manager.src_master)
    for main, _, _, old_release, release in SOURCES:
        # These are the two deployed version mismatches that abort metadata.
        assert _upload_releases(before[main]) == {old_release, release}

    changed = PREPARE.migrate_multiomics_upload_metadata(manager, LOGGER)

    expected = deepcopy(before)
    for main, legacy, _, _, release in SOURCES:
        del expected[main]["upload"]["jobs"][legacy]
        assert _upload_releases(manager.src_dump.documents[main]) == {release}
    assert manager.src_dump.documents == expected
    assert changed == [f"{main}.{legacy}" for main, legacy, _, _, _ in SOURCES]
    assert manager.reset_calls == [
        (main, "upload", legacy) for main, legacy, _, _, _ in SOURCES
    ]
    assert manager.source_data == collections
    assert manager.src_master == masters
    assert manager.upload_calls == []

    assert PREPARE.migrate_multiomics_upload_metadata(manager, LOGGER) == []
    assert manager.src_dump.documents == expected
    assert len(manager.reset_calls) == 2


@pytest.mark.parametrize("state", ["fresh", "no_upload", "no_jobs", "no_legacy"])
def test_migration_is_noop_without_retired_upload_history(state):
    documents = _documents()
    for main, legacy, _, _, _ in SOURCES:
        if state == "fresh":
            del documents[main]
        elif state == "no_upload":
            del documents[main]["upload"]
        elif state == "no_jobs":
            documents[main]["upload"] = {}
        else:
            del documents[main]["upload"]["jobs"][legacy]
    manager = SourceManager(documents, _registered())

    assert PREPARE.migrate_multiomics_upload_metadata(manager, LOGGER) == []

    assert manager.src_dump.documents == documents
    assert manager.reset_calls == []


@pytest.mark.parametrize(
    "registration", ["absent", "legacy_only", "both", "unrelated_only"]
)
def test_migration_requires_annotator_registered_and_legacy_retired(registration):
    registered = {}
    for main, legacy, annotator, _, _ in SOURCES:
        if registration == "legacy_only":
            registered[main] = [legacy]
        elif registration == "both":
            registered[main] = [legacy, annotator]
        elif registration == "unrelated_only":
            registered[main] = ["unrelated_uploader"]
    documents = _documents()
    manager = SourceManager(documents, registered)

    assert PREPARE.migrate_multiomics_upload_metadata(manager, LOGGER) == []

    assert manager.src_dump.documents == documents
    assert manager.reset_calls == []


def test_retired_upload_history_is_removed_even_when_versions_still_match():
    documents = _documents()
    for main, legacy, _, _, release in SOURCES:
        documents[main]["upload"]["jobs"][legacy]["release"] = release
    manager = SourceManager(documents, _registered())

    changed = PREPARE.migrate_multiomics_upload_metadata(manager, LOGGER)

    assert len(changed) == 2
    for main, legacy, annotator, _, release in SOURCES:
        assert set(manager.src_dump.documents[main]["upload"]["jobs"]) == {annotator}
        assert _upload_releases(manager.src_dump.documents[main]) == {release}
