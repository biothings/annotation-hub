import importlib.util
from pathlib import Path

STATIC_PATH = Path(__file__).parents[1] / "plugins" / "pubmed_metadata" / "static.py"
STATIC_SPEC = importlib.util.spec_from_file_location(
    "pubmed_metadata_static", STATIC_PATH
)
static = importlib.util.module_from_spec(STATIC_SPEC)
assert STATIC_SPEC.loader is not None
STATIC_SPEC.loader.exec_module(static)


def test_pubmed_source_uses_a_stable_release_root():
    assert static.PUBMED_METADATA_ROOT_URL == (
        "https://stars.renci.org/var/babel_outputs/pubmed2db/"
    )
    assert static.VALIDATION_REPORT_FILENAME == "validation_report.json.gz"
