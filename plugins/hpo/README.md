# hpo
BioThings API for HPO

## Annotation Hub import

Copied from
[`biothings/pending.api` at `6a31824dd390376760eda2759ae1c71823572ee1`](https://github.com/biothings/pending.api/tree/6a31824dd390376760eda2759ae1c71823572ee1/plugins/hpo).
The runtime files and license are retained; the exploratory notebook is omitted.

This remains a manifest-based plugin with `networkx` and `obonet` declared in
`manifest.json`. The manifest uses HTTPS download URLs. The parser reads
`hp.obo` and `phenotype_to_genes.txt` from the downloaded data folder, so upload
uses the archived inputs without fetching another ontology version. The existing
HPO document fields and the weekly download schedule are preserved.

With `DATA_PLUGIN_FOLDER` configured to this repository's `plugins` directory,
Hub discovers the manifest and registers the source as `local://hpo`.

## Original version can be found here

- <https://github.com/biothings/hpo>

### The imported version was forked from this commit

- <https://github.com/biothings/hpo/commit/79fc106ce83a03bc7062b80c5d1c977f56aed793>
