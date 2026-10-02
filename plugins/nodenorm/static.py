"""Static URLs and performance/semantic overrides for the NodeNorm loader."""

CONFLATION_LOOKUP_DATABASE = "conflation.sqlite3"
IDENTIFIER_LOOKUP_DATABASE = "identifier.sqlite3"
BABEL_OUTPUT_ROOT = "https://stars.renci.org/var/babel_outputs"
VERSION_URL = f"{BABEL_OUTPUT_ROOT}/latest/VERSION.txt"


NODENORM_CONFLATION_COLLECTION = ["DrugChemical.txt", "GeneProtein.txt"]


# These maps tune concurrency only. Artifact inclusion comes from the selected
# Babel release manifest, so a new compendium is never excluded by this file.
NODENORM_LARGE_DOWNLOAD_CHUNK_OVERRIDES = {
    "MolecularMixture.txt": 50,
    "Gene.txt": 75,
    "Publication.txt": 100,
    "SmallMolecule.txt": 150,
    "Protein.txt": 200,
}

DRUG_CHEMICAL_IDENTIFIER_FILES = [
    "Drug.txt",
    "ChemicalEntity.txt",
    "SmallMolecule.txt",
    "ComplexMolecularMixture.txt",
    "MolecularMixture.txt",
    "Protein.txt",
    "Food.txt",
]


GENE_PROTEIN_IDENTIFER_FILES = ["Protein.txt", "Gene.txt"]


NODENORM_UPLOAD_CHUNK_OVERRIDES = {
    "ChemicalEntity.txt": 10,
    "Disease.txt": 10,
    "Drug.txt": 10,
    "Gene.txt": 30,
    "MolecularActivity.txt": 10,
    "MolecularMixture.txt": 30,
    "OrganismTaxon.txt": 10,
    "PhenotypicFeature.txt": 10,
    "Protein.txt": 50,
    "Publication.txt": 30,
    "SmallMolecule.txt": 40,
    "umls.txt": 10,
}
