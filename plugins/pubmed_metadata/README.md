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
    "pubdate_raw": "2026 Jun 30",
    "pub_date": "2026-06-30",
    "abstract": "Example abstract"
  }
}
```

The parser streams each compressed shard without materializing it in memory. It
supports the legacy ten-field schema, the current schema with `identifiers`, and
the forthcoming schema carrying PubMed's verbatim publication date. Upstream has
not settled that field's name, so the parser accepts either `pubdate` or
`pub_date` as the input spelling and always stores it as `pubdate_raw`. That
rename is deliberate: on our side `pub_date` means the normalized query date
only, so an input record and an output document never use one name for two
different values. It requires string metadata, a list of nonempty identifier
strings containing the record's `PMID:<digits>`, valid UTF-8, and valid
gzip/NDJSON input. The canonical PMID is always the Elasticsearch `_id`; it is
validated as an exact member of current upstream `identifiers` arrays, then
removed from that array before storage so `pubmed.identifiers` contains only
alternates. Legacy records without `identifiers` store an empty identifier list.
A well-formed upstream `PMCID:PMC<digits>` identifier is normalized during
upload to the established `PMC:PMC<digits>` contract. If both spellings are
present, the first position is retained and the canonical identifier is stored
once; malformed `PMCID:` values and unrelated identifiers remain unchanged.
A structurally malformed record fails the upload with the shard and line number
rather than producing a partial or silently altered document. The default
storage also treats duplicate IDs as an error.

Publication dates deliberately have two representations. `pubdate_raw` retains
PubMed's display value verbatim, including seasons and ranges such as `1998
Dec-1999 Jan`. It remains in Elasticsearch `_source` but has neither an
inverted index nor doc values. The upstream `pub_year`, `pub_month`, and
`pub_day` components are used transiently during ingestion and are not stored.
Legacy ten-field and current eleven-field snapshots do not contain the verbatim
field, so the parser leaves `pubdate_raw` absent rather than synthesizing a
value that could conceal a previously truncated `MedlineDate`. Full DocMeta date
projection therefore depends on a new upstream export containing the twelfth
field.

`pub_date` is a separate query field. The parser emits it only when all three
components form a valid calendar day, normalized as `YYYY-MM-DD`, and
Elasticsearch maps it with `strict_date`. Year-only, month-only, seasonal,
ranged, and otherwise nonexact dates retain their source values but do not get
a `pub_date`; this avoids turning incomplete dates into artificial points in
time. A DocumentMetadataAPI adapter can reconstruct exact year/month/day values
from `pub_date`; when it is absent, it can apply the upstream leading-year and
remainder convention to `pubdate_raw` for partial, seasonal, and ranged dates.
`pubdate_raw` remains the fidelity value. Excluding `pub_date` from API
responses is the adapter's responsibility rather than an Elasticsearch mapping
concern.

Before queueing a large download, the dumper requires either the legacy
release-local `validation_report.json.gz` or the exact date-matched
`manifests/validation_report-YYYYMMDD.json`. It accepts gzip-compressed and
plain JSON reports, verifies that the report has no errors, and confirms that
its shard inventory matches a nonempty set of numerically contiguous files
beginning at shard `0`. Both legacy zero-padded and current unpadded
`pubmed_metadata_<index>.ndjson.gz` names are supported. A release without
either matching completion report is treated as incomplete; a release with a
report that fails validation stops discovery instead of falling back to an
older snapshot. The selected report is retained alongside the downloaded
shards for auditability and checked again after download. `month-format` is the
only structural check allowed to warn; every other reported structural check
must pass. Advisory warnings outside the structure section remain allowed when
the error list is empty.

Downloads and uploads are each capped at four concurrent shards. Dumps are not
scheduled automatically because each full snapshot is very large; an operator
must trigger the release check. Abstracts are retained in Elasticsearch
`_source` but are not indexed or sortable. `title` supports full-text matching
and relevance scoring but is not sortable. `journal.name` uses its `.raw`
keyword subfield when sorting; the searchable keyword and exact-date fields are
directly sortable. `identifiers` uses the Hub's lowercase keyword normalizer so
DOI and PMC CURIE lookups are case-insensitive. PMIDs resolve through the
document `_id` instead of this alternate-identifier field.
Because this is a very large source, the uploader retains only one previous
MongoDB source collection instead of the BioThings default of ten.

Build `pubmed_metadata` by itself into a versioned `pubmed_*` Elasticsearch
index. After validating the index, point the stable `annotator-pubmed` alias to
it. NodeAnnotator routes `PMID:` identifiers to exact `_id` lookups on that
alias.

For subsequent releases, move the alias from the previous index to the newly
validated index in one atomic Elasticsearch alias update. Keep the previous
index temporarily for rollback and remove it separately after validation.
Build configuration and alias state are deployment state and are not stored in
this repository.

This is a full snapshot rather than an incremental feed. Current upstream
exports include the PMID plus available DOI and PMC identifiers; the uploader
retains the latter identifiers as alternates after validating and removing the
primary PMID. Release discovery is based on the upstream
`YYYYmonD`/`YYYYmonDD` directory names, while release completeness and schema
compatibility are gated by the published validation report and the parser's
strict record validation.

The export is produced by [TranslatorSRI/pubmed2db](https://github.com/TranslatorSRI/pubmed2db)
from NLM PubMed data. Downstream use must follow the
[NLM PubMed terms and conditions](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/README.txt).
