# annotation-hub
A BioThings-Hub to aggregate knowledge graph node-level data for our core-components services


## Running
In order to run you need to setup both a `config.py` and `config_hub.py` file in the root directory
of the hub. Examples of these can be found from the biothings.api

Once they're setup you start the hub by running the following command at the root of the directory

```shell
PYTHONPATH=. python3 ./bin/hub.py
```

Initialize the Multiomics submodules with `git submodule update --init`. Hub
startup applies annotation-hub's local integration patches before loading them.
The pinned plugins read data releases from upstream `version.txt` files,
independently of their manifest versions.
See [the patch instructions](patches/multiomics/README.md) for manual preparation,
versioned downloads, and local-edit safeguards.

## Plugins

We have 3 groups of plugins based off which web api they feed

### NodeNormalization
The corresponding web api can be found [here](https://github.com/biothings/NodeNormalizationAPI)

plugins
* `~/plugins/nodenorm`

### NameResolution
The corresponding web api can be found [here](https://github.com/biothings/NameResolutionAPI)

plugins
* `~/plugins/nameres`

### NodeAnnotator
The corresponding web api can be found [here](https://github.com/biothings/biothings_annotator)

plugins
* `~/plugins/atc`
* `~/plugins/hpo` (standalone HPO index)
* `~/plugins/multiomics_clinicaltrials_kp`
* `~/plugins/multiomics_drug_approvals_kp`
* `~/plugins/ncit` (standalone NCI Thesaurus index)
* `~/plugins/pubmed_metadata` (standalone PubMed index)

HPO and NCIT are local manifest plugins imported from `biothings/pending.api`.
With `DATA_PLUGIN_FOLDER` pointing to this repository's `plugins` directory, the
Hub discovers them at startup. They can also be registered in the Hub console
with `register_url("local://hpo")` and `register_url("local://ncit")`. Both retain
their weekly download schedules and source names. Build each source separately
and publish it under the existing `hpo` or `ncit` alias; they are not part of the
combined annotations build. See their plugin READMEs for provenance and details.
