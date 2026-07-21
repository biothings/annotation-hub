# PubMed metadata

This NodeAnnotator source ingests the RENCI `pubmed2db` snapshot published at
<https://stars.renci.org/var/babel_outputs/pubmed2db/2026jun30/>. The pinned
release contains 16 gzip-compressed NDJSON shards (about 16.85 GB compressed).

Each upstream record is stored under a source-specific key so it can be merged
cleanly into `annotator_extra`:

```json
{
  "_id": "PMID:12345678",
  "pubmed": {
    "journal_name": "Example Journal",
    "journal_abbrev": "Example J",
    "article_title": "Example title",
    "volume": "1",
    "issue": "2",
    "pub_year": "2026",
    "pub_month": "6",
    "pub_day": "30",
    "abstract": "Example abstract"
  }
}
```

The parser streams each compressed shard without materializing it in memory. It
requires the exact ten-field upstream schema, string values, valid `PMID:<digits>`
identifiers, valid UTF-8, and valid gzip/NDJSON input. A malformed record fails
the upload with the shard and line number rather than producing a partial or
silently altered document. The default storage also treats duplicate IDs as an
error.

Downloads and uploads are each capped at four concurrent shards. All content
fields are retained in Elasticsearch `_source`, but they are intentionally not
indexed: NodeAnnotator retrieves these documents by identifier, and indexing
millions of titles and abstracts would add substantial storage overhead.
Because this is a very large source, the uploader retains only one previous
MongoDB source collection instead of the BioThings default of ten.

After the first successful upload, add `pubmed_metadata` to the source list of
the `annotator_extra` build configuration in the Hub database. Build
configuration is deployment state and is not stored in this repository. Serving
the merged records also requires the NodeAnnotator runtime to route `PMID:`
identifiers to `annotator_extra`.

This is a full snapshot rather than an incremental feed. DOI and PMC identifiers
are not present in this export. To adopt a newer snapshot, update `RELEASE` in
`static.py` and verify that its shard count and schema are unchanged.

The export is produced by [TranslatorSRI/pubmed2db](https://github.com/TranslatorSRI/pubmed2db)
from NLM PubMed data. Downstream use must follow the
[NLM PubMed terms and conditions](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/README.txt).
