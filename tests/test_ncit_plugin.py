"""Exercise the migrated NCIT parser against an archived OBO release."""

import importlib.util
from pathlib import Path

import pytest

PLUGIN = Path(__file__).resolve().parents[1] / "plugins" / "ncit"
SPEC = importlib.util.spec_from_file_location("ncit_parser", PLUGIN / "parser.py")
PARSER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PARSER)


@pytest.fixture
def ontology(tmp_path):
    (tmp_path / "ncit.obo").write_text(
        '''format-version: 1.2
ontology: ncit

[Term]
id: NCIT:C1
name: Root

[Term]
id: NCIT:C2
name: Child
def: "A child term." [NCIT:C1]
synonym: "Child alias" EXACT []
synonym: "Related child" RELATED []
xref: SNOMEDCT_US:123
xref: HP:0000001
xref: NCIT.Codes:C2
xref: https://example.org/ignored
is_a: NCIT:C1
relationship: part_of NCIT:C9

[Term]
id: NCIT:C3
name: Retired child
is_obsolete: true
replaced_by: NCIT:C2
consider: NCIT:C1

[Term]
id: NCIT:C9
name: Related term

[Term]
id: HP:0000001
name: Foreign ontology term
''',
        encoding="utf-8",
    )
    return tmp_path


def test_archived_ncit_preserves_documents_and_is_a_hierarchy(ontology, monkeypatch):
    def reject_network(*args, **kwargs):
        pytest.fail("An upload must only read its downloaded ontology")

    monkeypatch.setattr("urllib.request.urlopen", reject_network)
    documents = {doc["_id"]: doc for doc in PARSER.load_data(ontology)}

    assert set(documents) == {"NCIT:C1", "NCIT:C2", "NCIT:C3", "NCIT:C9"}
    assert documents["NCIT:C1"]["children"] == ["NCIT:C2"]
    assert documents["NCIT:C1"]["descendants"] == ["NCIT:C2"]
    assert documents["NCIT:C2"] == {
        "_id": "NCIT:C2",
        "label": "Child",
        "definition": "A child term.",
        "parents": ["NCIT:C1"],
        "ancestors": ["NCIT:C1"],
        "synonym": {"exact": ["Child alias"], "related": ["Related child"]},
        "xrefs": {
            "snomedct_us": ["123"],
            "hp": ["HP:0000001"],
            "ncit_codes": ["C2"],
        },
        "relationships": {"ncit": ["NCIT:C9"]},
    }
    assert "parents" not in documents["NCIT:C9"]
    assert "children" not in documents["NCIT:C9"]


def test_archived_ncit_keeps_obsolete_term_replacement(ontology):
    documents = {doc["_id"]: doc for doc in PARSER.load_data(ontology)}
    assert documents["NCIT:C3"] == {
        "_id": "NCIT:C3",
        "label": "Retired child",
        "is_obsolete": True,
        "replaced_by": "NCIT:C2",
        "consider": ["NCIT:C1"],
        "synonym": {},
        "xrefs": {},
        "relationships": {},
    }


def test_missing_archived_ontology_fails_without_fetching(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(PARSER.load_data(tmp_path))
