# Local Multiomics integration patches

These patches belong to annotation-hub. They change only local files in the
checked-out submodules; they do not change upstream repositories, submodule
commit IDs, or remote URLs.

The pinned upstream manifests register obsolete TSV KG uploaders alongside the
JSONL annotators. The annotation hub uses only the annotator collections, so the
local manifests register only `multiomics_clinical_trials_annotator` and
`multiomics_drug_approvals_annotator`. The TSV parsers remain untouched.

Both local manifests download versioned JSONL files (`v4.2.4` and `v0.5.7`)
instead of floating `_current` aliases. The annotator input filenames match these
URLs, and release functions retain the matching upstream version labels. This
prevents a new upstream release from silently entering the old release folder;
versioned URLs are still controlled by the data provider, not content-addressed.

The Drug Approvals patch propagates input and parse errors instead of silently
ending the upload. Each approval entry retains its Biolink `predicate` alongside
the KG's optional `status`. For example, `biolink:contraindicated_in` remains
visible even when `clinical_approval_status` is absent or is
`approved_for_condition`. Status alone does not describe that relationship.
The upstream suppression of duplicate `applied_to_treat` entries is preserved.

After checkout, initialize the submodules and prepare them locally:

```shell
git submodule update --init
python3 bin/prepare_multiomics.py
python3 bin/prepare_multiomics.py --check
```

The hub entry point also runs preparation before it loads data plugins.
Uninitialized plugins are skipped at hub startup so deployments that do not use
Multiomics can still start; the standalone command requires both plugins.
`--check` writes nothing and exits with status 1 if patches are needed, 0 when
already prepared, or 2 for an error.

Preparation is idempotent. It verifies the exact upstream revisions in
`patches.json` and preflights all target files before writing. It accepts pristine
upstream files, files already patched by this tool, and the known annotation-only
manifest override used on su06. Other local edits stop preparation and are left
untouched. Existing data folders and running services are not modified.

If preparation fails at hub startup, an error with `notify` reports the cause.
The hub continues starting its other sources while blocking Multiomics dumper,
uploader, and data-plugin registration for that process, including later reloads
or registration attempts. No unprepared Multiomics sources can be scheduled.
Fix the revision/local-edit problem and restart the hub to enable them again.
The standalone preparation command still fails with a nonzero exit status.

When advancing a submodule, review and regenerate its patch and update
`patches.json` together. Do not just change the expected revision to bypass the
guard. To discard these local changes deliberately, restore the two patched
files (`manifest.json` and `annotator_parser.py`) within each submodule; restarting
the hub will apply its integration patches again.
