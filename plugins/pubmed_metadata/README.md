# PubMed metadata

This NodeAnnotator source ingests dated RENCI `pubmed2db` snapshots published
under <https://stars.renci.org/var/babel_outputs/pubmed2db/>. On each manually
triggered dump, it selects the newest completed release, records the dated
directory name as the BioThings source release, and archives the release in its
own data folder.

Each upstream record is stored under a `pubmed` source key in a standalone
PubMed index:

```json
{
  "_id": "PMID:12345678",
  "pubmed": {
    "identifiers": [
      "PMID:12345678",
      "doi:10.1000/example",
      "PMC:PMC1234567"
    ],
    "journal": {
      "name": "Example Journal",
      "abbr": "Example J"
    },
    "title": "Example title",
    "vol": "1",
    "iss": "2",
    "pub_date": "2026-06-30",
    "abstract": "Example abstract"
  }
}
```

The parser streams each compressed shard without materializing it in memory. It
supports both the legacy ten-field schema and the current schema with
`identifiers`. It requires string metadata, a list of nonempty identifier
strings containing the record's `PMID:<digits>`, valid UTF-8, and valid
gzip/NDJSON input. Legacy records without `identifiers` default to their PMID.
A structurally malformed record fails the upload with the shard and line number
rather than producing a partial or silently altered document. The default
storage also treats duplicate IDs as an error.

The parser converts NLM month abbreviations to numbers and preserves the
available publication-date precision: `YYYY-MM-DD` when all parts exist,
`YYYY-MM` when the day is missing, and `YYYY` when only the year exists. It
omits `pub_date` when the year is absent. Elasticsearch maps all three forms as
a date; partial dates sort at the beginning of their represented period.

Before queueing a large download, the dumper requires the release's
`validation_report.json.gz`, verifies that it reports no errors and passes all
structural checks, and confirms that its shard inventory matches a nonempty set
of contiguous files beginning at shard `00000`. A release directory without a
validation report is treated as incomplete. The report is retained alongside
the downloaded shards for auditability and is checked again after download.
Overall report warnings are allowed when the error list is empty and every
required structural check passes.

Downloads and uploads are each capped at four concurrent shards. Dumps are not
scheduled automatically because each full snapshot is very large; an operator
must trigger the release check. Abstracts are retained in Elasticsearch
`_source` but are not indexed or sortable. The other metadata fields are
indexed. `title` supports full-text matching and relevance scoring but is not
sortable. `journal.name` uses its `.raw` keyword subfield when sorting; the
keyword and date fields are directly sortable. `identifiers` uses the Hub's
lowercase keyword normalizer so PMID, DOI, and PMC CURIE lookups are
case-insensitive.
Because this is a very large source, the uploader retains only one previous
MongoDB source collection instead of the BioThings default of ten.

Build `pubmed_metadata` by itself into a versioned `pubmed_*` Elasticsearch
index. After validating the index, point the stable `annotator-pubmed` alias to
it. NodeAnnotator routes `PMID:` identifiers to that alias.

For subsequent releases, move the alias from the previous index to the newly
validated index in one atomic Elasticsearch alias update. Keep the previous
index temporarily for rollback and remove it separately after validation.
Build configuration and alias state are deployment state and are not stored in
this repository.

This is a full snapshot rather than an incremental feed. Current exports include
the PMID plus available DOI and PMC identifiers. Release discovery is based on
the upstream `YYYYmonD`/`YYYYmonDD` directory names, while release completeness
and schema compatibility are gated by the published validation report and the
parser's strict record validation.

The export is produced by [TranslatorSRI/pubmed2db](https://github.com/TranslatorSRI/pubmed2db)
from NLM PubMed data. Downstream use must follow the
[NLM PubMed terms and conditions](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/README.txt).
