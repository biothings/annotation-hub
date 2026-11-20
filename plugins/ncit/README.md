# NCI Thesaurus
API for [NCIT](https://www.ebi.ac.uk/ols/ontologies/ncit)

## Annotation Hub import

The manifest and release helper were copied from
[`biothings/pending.api`](https://github.com/biothings/pending.api/tree/6a31824dd390376760eda2759ae1c71823572ee1/plugins/ncit)
at commit `6a31824dd390376760eda2759ae1c71823572ee1`.
`parser.py` vendors that revision's
[`hub/dataload/data_parsers.py`](https://github.com/biothings/pending.api/blob/6a31824dd390376760eda2759ae1c71823572ee1/hub/dataload/data_parsers.py)
and adds a `load_data` entry point for `ncit.obo` with prefix `NCIT`. This keeps
the manifest independent of pending.api's `hub` package and avoids bare module
imports when the SDK generates the uploader. The manifest declares `networkx`
and `obonet`; download and release URLs use HTTPS. The parser's output is
unchanged, including CURIE IDs, `label`, `definition`, hierarchy, synonyms,
cross-references, relationships, and obsolete terms.

The release helper reads the ontology's `data-version` header with a bounded
HTTP request. It reports the actual ontology release (for example, `2026-03-19`)
instead of pending.api's previous download-date label. The Hub build version
continues to identify the build date independently.

With `DATA_PLUGIN_FOLDER` set to this repository's `plugins` directory, the Hub
discovers this manifest at startup. Alternatively, register it in the Hub console
with `register_url("local://ncit")`. It keeps the source name `ncit` and the
upstream weekly download schedule. Build this source separately and publish it
under the existing `ncit` Elasticsearch alias, rather than merging it into the
annotations build. Build configuration and alias publication remain Hub
deployment settings.

## Original version can be found here

- <https://github.com/DylanWelzel/ncit>

### The imported version was forked from this commit

- <https://github.com/DylanWelzel/ncit/commit/4dc617114398ec8f9b234d6fb760b2bc7e95add8>
