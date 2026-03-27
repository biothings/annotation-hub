# annotation-hub
A BioThings-Hub to aggregate knowledge graph node-level data for our core-components services


## Running
In order to run you need to setup both a `config.py` and `config_hub.py` file in the root directory
of the hub. Examples of these can be found from the biothings.api

Once they're setup you start the hub by running the following command at the root of the directory

```shell
PYTHONPATH=. python3 ./bin/hub.py
```

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
* `~/plugins/multiomics_clinicaltrials_kp`
* `~/plugins/multiomics_drug_approvals_kp`
