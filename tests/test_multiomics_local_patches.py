"""Exercise the pinned upstream plugins with annotation-hub's local patches."""

import gzip
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
PATCHES = ROOT / "patches" / "multiomics"
ENTRIES = json.loads((PATCHES / "patches.json").read_text())


def _module(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PREPARE = _module(ROOT / "bin" / "prepare_multiomics.py")


@pytest.fixture(scope="session")
def pristine_plugins(tmp_path_factory):
    root = tmp_path_factory.mktemp("multiomics-pristine")
    shutil.copytree(PATCHES, root / "patches" / "multiomics")
    for entry in ENTRIES:
        source = ROOT / entry["path"]
        if not (source / ".git").exists():
            pytest.fail(
                "Initialize Multiomics test inputs with git submodule update --init"
            )
        target = root / entry["path"]
        subprocess.run(
            ["git", "clone", "--shared", "--no-checkout", str(source), str(target)],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "-C", str(target), "checkout", "--detach", entry["commit"]],
            check=True,
            capture_output=True,
        )
    return root


@pytest.fixture
def checkout(tmp_path, pristine_plugins):
    root = tmp_path / "hub"
    shutil.copytree(pristine_plugins, root)
    return root


@pytest.fixture
def prepared(checkout):
    PREPARE.prepare_multiomics(checkout)
    return checkout


def _parser(root, index=1):
    return _module(root / ENTRIES[index]["path"] / "annotator_parser.py")


def _write_rows(folder, stem, version, kind, rows, compressed=True):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{stem}_kg_{kind}_v{version}.jsonl"
    content = "".join(json.dumps(row) + "\n" for row in rows)
    if compressed:
        path = path.with_suffix(path.suffix + ".gz")
        with gzip.open(path, "wt") as output:
            output.write(content)
    else:
        path.write_text(content)
    return path


def _edge(predicate="biolink:contraindicated_in", **attributes):
    return {
        "id": "edge-1",
        "subject": "CHEBI:1",
        "object": "MONDO:1",
        "predicate": predicate,
        "object_name": "condition",
        **attributes,
    }


def _drug_input(folder, edges, compressed=True):
    _write_rows(
        folder,
        "drug_approvals",
        "0.5.7",
        "nodes",
        [
            {"id": "CHEBI:1", "name": "drug"},
            {"id": "MONDO:1", "name": "condition"},
        ],
        compressed,
    )
    return _write_rows(folder, "drug_approvals", "0.5.7", "edges", edges, compressed)


def test_preparation_is_idempotent_and_preserves_upstream_heads(checkout):
    needed = PREPARE.prepare_multiomics(checkout, check=True)
    assert len(needed) == 4
    for entry in ENTRIES:
        manifest = json.loads((checkout / entry["path"] / "manifest.json").read_text())
        assert len(manifest["uploaders"]) == 2  # --check did not write
    assert PREPARE.prepare_multiomics(checkout) == needed
    assert PREPARE.prepare_multiomics(checkout) == []
    for entry in ENTRIES:
        head = subprocess.check_output(
            ["git", "-C", str(checkout / entry["path"]), "rev-parse", "HEAD"], text=True
        ).strip()
        assert head == entry["commit"]


def test_known_annotation_only_manifest_override_is_upgraded(checkout):
    for entry in ENTRIES:
        path = checkout / entry["path"] / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["uploaders"] = [manifest["uploaders"][1]]
        path.write_text(json.dumps(manifest))
    assert len(PREPARE.prepare_multiomics(checkout)) == 4
    assert PREPARE.prepare_multiomics(checkout, check=True) == []


@pytest.mark.parametrize("filename", ["manifest.json", "annotator_parser.py"])
def test_unknown_local_edits_stop_both_plugins_before_any_write(checkout, filename):
    path = checkout / ENTRIES[1]["path"] / filename
    if filename == "manifest.json":
        content = json.loads(path.read_text())
        content["dumper"]["uncompress"] = True
        path.write_text(json.dumps(content))
    else:
        path.write_text(path.read_text() + "\n# custom local change\n")
    before = {
        file: file.read_bytes()
        for entry in ENTRIES
        for file in (checkout / entry["path"]).glob("*.py")
    }
    before.update(
        {
            checkout
            / e["path"]
            / "manifest.json": (checkout / e["path"] / "manifest.json").read_bytes()
            for e in ENTRIES
        }
    )
    with pytest.raises(PREPARE.PatchError, match="Unrecognized local edits"):
        PREPARE.prepare_multiomics(checkout)
    assert {path: path.read_bytes() for path in before} == before


def test_different_submodule_revision_stops_before_any_write(checkout):
    plugin = checkout / ENTRIES[1]["path"]
    subprocess.run(
        ["git", "-C", str(plugin), "checkout", "--detach", "HEAD^"],
        check=True,
        capture_output=True,
    )
    with pytest.raises(PREPARE.PatchError, match="expected"):
        PREPARE.prepare_multiomics(checkout)
    manifest = json.loads((checkout / ENTRIES[0]["path"] / "manifest.json").read_text())
    assert len(manifest["uploaders"]) == 2


def test_missing_submodules_are_optional_only_for_hub_startup(checkout):
    shutil.rmtree(checkout / "plugins")
    with pytest.raises(PREPARE.PatchError, match="Initialize"):
        PREPARE.prepare_multiomics(checkout)
    assert PREPARE.prepare_multiomics(checkout, allow_missing=True) == []


def test_symlinked_plugin_is_not_patched(checkout, tmp_path):
    plugin = checkout / ENTRIES[1]["path"]
    moved = tmp_path / "external-plugin"
    plugin.rename(moved)
    plugin.symlink_to(moved, target_is_directory=True)
    with pytest.raises(PREPARE.PatchError, match="symlinked plugin"):
        PREPARE.prepare_multiomics(checkout)
    assert len(json.loads((moved / "manifest.json").read_text())["uploaders"]) == 2


@pytest.mark.parametrize("index", [0, 1])
def test_only_annotation_uploaders_and_versioned_inputs_are_registered(prepared, index):
    plugin = prepared / ENTRIES[index]["path"]
    manifest = json.loads((plugin / "manifest.json").read_text())
    version = _module(plugin / "version.py").get_release(None)
    assert version == manifest["version"]
    assert [uploader["parser"] for uploader in manifest["uploaders"]] == [
        "annotator_parser:load_data"
    ]
    assert manifest["uploaders"][0]["name"].endswith("_annotator")
    assert len(manifest["dumper"]["data_url"]) == 2
    for url in manifest["dumper"]["data_url"]:
        assert url.endswith(f"_v{version}.jsonl.gz")
        assert "_current" not in url


@pytest.mark.parametrize("compressed", [False, True])
def test_clinical_trials_reads_pinned_jsonl_and_combines_boxed_warnings(
    prepared, tmp_path, compressed
):
    folder = tmp_path / "data"
    _write_rows(
        folder,
        "clinical_trials",
        "4.2.4",
        "nodes",
        [
            {"id": "MONDO:1", "name": "condition"},
            {
                "id": "CLINICALTRIALS:NCT1",
                "clinical_trial_tested_intervention": True,
                "clinical_trial_phase": "clinical_trial_phase_3",
                "clinical_trial_overall_status": "COMPLETED",
                "clinical_trial_start_date": "2020-01-01",
                "clinical_trial_enrollment": 12,
            },
        ],
        compressed,
    )
    _write_rows(
        folder,
        "clinical_trials",
        "4.2.4",
        "edges",
        [
            _edge(
                "biolink:in_clinical_trials_for",
                has_supporting_studies=["CLINICALTRIALS:NCT1"],
                intervention_boxed_warning="1/1",
            ),
            _edge(
                "biolink:in_clinical_trials_for",
                id="edge-2",
                has_supporting_studies=["CLINICALTRIALS:NCT1"],
                intervention_boxed_warning=None,
            ),
        ],
        compressed,
    )
    documents = list(_parser(prepared, 0).load_data(str(folder)))
    assert len(documents) == 1
    assert documents[0]["boxed_warning"] is True
    assert len(documents[0]["clinical_trials"]) == 2
    trial = documents[0]["clinical_trials"][0]
    assert (trial["id"], trial["start_date"], trial["study_size"]) == (
        "NCT1",
        "2020-01-01",
        12,
    )


@pytest.mark.parametrize("compressed", [False, True])
def test_drug_approvals_keeps_predicate_and_optional_kg_status(
    prepared, tmp_path, compressed
):
    folder = tmp_path / "data"
    _drug_input(
        folder,
        [
            _edge("biolink:applied_to_treat", clinical_approval_status="off_label_use"),
            _edge("biolink:contraindicated_in", id="contra-no-status"),
            _edge(
                "biolink:contraindicated_in",
                id="contra-with-status",
                clinical_approval_status="approved_for_condition",
            ),
        ],
        compressed,
    )
    documents = list(_parser(prepared).load_data(str(folder)))
    entries = {entry["edge_id"]: entry for entry in documents[0]["clinical_approval"]}
    assert entries["edge-1"]["status"] == "off_label_use"
    assert entries["edge-1"]["predicate"] == "biolink:applied_to_treat"
    assert entries["contra-no-status"]["predicate"] == "biolink:contraindicated_in"
    assert "status" not in entries["contra-no-status"]
    assert entries["contra-with-status"]["predicate"] == "biolink:contraindicated_in"
    assert entries["contra-with-status"]["status"] == "approved_for_condition"


def test_treats_supersedes_only_duplicate_applied_to_treat(prepared, tmp_path):
    folder = tmp_path / "data"
    _drug_input(
        folder,
        [
            _edge("biolink:applied_to_treat"),
            _edge(
                "biolink:treats",
                id="treats",
                clinical_approval_status="approved_for_condition",
            ),
            _edge("biolink:contraindicated_in", id="contra"),
        ],
    )
    entries = list(_parser(prepared).load_data(str(folder)))[0]["clinical_approval"]
    assert [entry["predicate"] for entry in entries] == [
        "biolink:treats",
        "biolink:contraindicated_in",
    ]


@pytest.mark.parametrize("missing", ["edges", "nodes"])
def test_missing_input_fails_instead_of_succeeding_with_zero_documents(
    prepared, tmp_path, missing
):
    folder = tmp_path / "data"
    _drug_input(folder, [_edge()])
    (folder / f"drug_approvals_kg_{missing}_v0.5.7.jsonl.gz").unlink()
    with pytest.raises(FileNotFoundError):
        list(_parser(prepared).load_data(str(folder)))


@pytest.mark.parametrize("malformed", ["json", "edge", "node"])
def test_corrupt_input_propagates_without_partial_success(
    prepared, tmp_path, malformed
):
    folder = tmp_path / "data"
    edge_path = _drug_input(folder, [_edge()], compressed=False)
    if malformed == "json":
        with edge_path.open("a") as output:
            output.write("{broken JSON\n")
        error = json.JSONDecodeError
    elif malformed == "edge":
        with edge_path.open("a") as output:
            output.write(
                json.dumps(
                    {"subject": "CHEBI:1", "predicate": "biolink:contraindicated_in"}
                )
                + "\n"
            )
        error = KeyError
    else:
        (folder / "drug_approvals_kg_nodes_v0.5.7.jsonl").write_text(
            '{"name": "missing id"}\n'
        )
        error = KeyError
    with pytest.raises(error):
        next(_parser(prepared).load_data(str(folder)))
