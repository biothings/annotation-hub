"""Validate imported HPO data against a small, fully local ontology release."""

import importlib.util
from pathlib import Path
import textwrap
import urllib.request

import pytest


PLUGIN_ROOT = Path(__file__).resolve().parents[1] / "plugins" / "hpo"


@pytest.fixture
def parser():
    spec = importlib.util.spec_from_file_location("hpo_parser", PLUGIN_ROOT / "parser.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def release(tmp_path):
    (tmp_path / "hp.obo").write_text(
        textwrap.dedent('''\
            format-version: 1.2
            ontology: hp

            [Term]
            id: HP:0000001
            name: All

            [Term]
            id: HP:0000002
            name: Example phenotype
            def: "A phenotype." [PMID:123]
            is_a: HP:0000001 ! All
            synonym: "Exact name" EXACT []
            synonym: "Related name" RELATED []
            synonym: "Broad name" BROAD []
            xref: UMLS:C123
            xref: NCIT:C456
            xref: MSH:D789
            xref: ORPHA:123
            xref: https://example.org/term
            created_by: test-author

            [Term]
            id: HP:0000003
            name: Child phenotype
            is_a: HP:0000002 ! Example phenotype

            [Term]
            id: CHEBI:1
            name: Other namespace
        '''),
        encoding="utf-8",
    )
    (tmp_path / "phenotype_to_genes.txt").write_text(
        "hpo_id\thpo_name\tncbi_gene_id\tgene_symbol\tdisease_id\n"
        "HP:0000002\tExample phenotype\t123\tGENE1\tOMIM:1\n"
        "HP:0000002\tExample phenotype\t456\tGENE2\tORPHA:2\n",
        encoding="utf-8",
    )
    return tmp_path


def test_loads_archived_ontology_with_annotations_and_hierarchy(parser, release, monkeypatch):
    def unexpected_request(*args, **kwargs):
        raise AssertionError("HPO upload must not download an ontology")

    monkeypatch.setattr(urllib.request, "urlopen", unexpected_request)
    documents = {record["_id"]: record for record in parser.load_data(release)}

    assert set(documents) == {"HP:0000001", "HP:0000002", "HP:0000003"}
    phenotype = documents["HP:0000002"]
    assert phenotype["hp"] == "HP:0000002"
    assert phenotype["name"] == "Example phenotype"
    assert phenotype["def"] == "A phenotype. [PMID:123]"
    assert phenotype["parents"] == ["HP:0000001"]
    assert phenotype["children"] == ["HP:0000003"]
    assert phenotype["ancestors"] == ["HP:0000001"]
    assert phenotype["descendants"] == ["HP:0000003"]
    assert phenotype["synonym"] == {
        "exact": ["Exact name"],
        "related": ["Related name"],
        "broad": ["Broad name"],
    }
    assert phenotype["xrefs"] == {
        "umls": ["C123"],
        "ncit": ["C456"],
        "mesh": ["D789"],
        "orpha": ["ORPHA:123"],
    }
    assert phenotype["annotations"] == [
        {"gene": {"id": "123", "symbol": "GENE1"}, "disease_id": "OMIM:1"},
        {"gene": {"id": "456", "symbol": "GENE2"}, "disease_id": "ORPHA:2"},
    ]
    assert "created_by" not in phenotype
    assert "is_a" not in phenotype
    assert "xref" not in phenotype
    assert documents["HP:0000001"]["annotations"] == []
    assert set(documents["HP:0000001"]["descendants"]) == {"HP:0000002", "HP:0000003"}
    assert set(documents["HP:0000003"]["ancestors"]) == {"HP:0000001", "HP:0000002"}


def test_requires_downloaded_ontology(parser, release):
    (release / "hp.obo").unlink()

    with pytest.raises(FileNotFoundError):
        list(parser.load_data(release))


def test_requires_gene_annotations(parser, release):
    (release / "phenotype_to_genes.txt").unlink()

    with pytest.raises(FileNotFoundError, match="phenotype_to_genes"):
        list(parser.load_data(release))
