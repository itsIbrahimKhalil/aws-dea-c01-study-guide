# 03 · Data Formats & Compression — how the bytes are packed decides what you pay

> **Exam map:** D1 · Task 1.2, 1.4 — D2 · Task 2.4 · **Skills:** 1.2.6, 2.4.5, 1.4.1 (also supports 1.2.4, 1.2.7, 3.1.7) · **Weight:** 🔥🔥🔥 High · **Read time:** ~24 min

## The idea

Athena bills by **bytes scanned**, Redshift Spectrum bills by bytes scanned, Glue and EMR bill by how long workers run — and all of those numbers depend less on the engine than on **how the data is laid out in files**. The same 1 TB of orders can cost dollars or cents to query depending on its format, compression and file sizes. That's why "convert CSV to Parquet" is one of the most frequent correct answers on the exam.

The analogy: a **filing cabinet of customer records**. A **row format** (CSV, JSON, Avro) is a cabinet where each folder holds one customer's complete record — perfect when you need *everything about one customer*, painful when you want *the total revenue of all customers*, because you must open every folder. A **columnar format** (Parquet, ORC) reorganizes the cabinet so each drawer holds a single field for all customers — one drawer of prices, one of countries. To sum revenue you open one drawer. Each drawer also has a **label on the front** ("prices 3.10 to 99.50 inside") so you can skip drawers that can't contain what you're looking for. **Compression** is vacuum-packing the drawer contents; **splittability** is whether several clerks can each grab part of a drawer at once, or whether one clerk must unpack it from the start.

After this guide you'll crack questions on choosing a format, which compression codec to pick, why one huge `.gz` file makes a job crawl, how to fix the small-files problem, which AWS service converts formats with least effort, which SerDe reads a messy CSV, and why a renamed column suddenly returns NULLs.

## Row vs columnar

| | Row-oriented | Columnar |
|---|---|---|
| Formats | CSV/TSV, JSON / JSON Lines, Avro, XML | **Parquet**, **ORC** |
| Best for | Writing whole records, streaming, reading full rows, OLTP-style exchange | Analytics: few columns over many rows, aggregations, filters |
| Compression | Moderate (mixed types next to each other) | High (similar values stored together → dictionary, run-length encoding) |
| Query cost on Athena/Spectrum | Scans every byte of every row | Scans only referenced columns and matching blocks |

Why columnar wins for analytics — three mechanisms:

1. **Column pruning (projection)**: `SELECT country, SUM(price)` reads two column chunks and ignores the other 48 columns.
2. **Predicate pushdown via statistics**: each Parquet row group and ORC stripe stores **min/max** (and null counts) per column; `WHERE order_date = '2026-09-01'` skips blocks whose range can't match. Sorting data by a commonly filtered column makes these ranges tight and skipping dramatic.
3. **Better compression**: similar values side by side compress far better, so even the columns you do read are fewer bytes.

**THE trap:** columnar is not "always better." A workload that reads **entire records one at a time** (streaming consumers, record-by-record exchange with schema evolution) prefers **Avro**. And a pile of tiny Parquet files can be slower than fewer larger CSVs — the per-file overhead eats the benefit.

## The formats, one by one

| Format | Type | Schema | Key traits | Typical AWS use |
|---|---|---|---|---|
| **CSV / TSV** | Row, text | None (header optional) | Universal; no types, no nesting; quoting and embedded delimiters cause pain | Source exports, Redshift `COPY`, S3 exchanges |
| **JSON** | Row, text | Self-describing per record | Nested, flexible; verbose | APIs, logs, events |
| **JSON Lines** | Row, text | Per record | **One JSON object per line** — what Athena, Glue, Firehose and Redshift `COPY` want | Lake raw zones, Firehose output |
| **XML** | Row, text, hierarchical | XSD optional | Verbose, nested; Glue XML classifier with a `rowTag`; Athena has no native XML SerDe | Legacy/B2B feeds — convert early |
| **Avro** | **Row, binary** | **Schema (JSON) embedded in the file header** | Compact, splittable, strongest schema-evolution rules (defaults for new fields); streaming favourite | Kafka/MSK with Glue Schema Registry, DMS/Kinesis records, raw zone |
| **Parquet** | **Columnar, binary** | Embedded in footer | Nested types, row groups (default **128 MB**), column chunks, pages; min/max stats; dictionary + RLE encoding; optional bloom filters | **The AWS analytics default** — Athena, Spectrum, Glue, EMR, Iceberg, Redshift `UNLOAD` |
| **ORC** | **Columnar, binary** | Embedded | Hive heritage; stripes (default **64 MB**), built-in lightweight indexes, optional **bloom filters**; default codec ZLIB | Hive/EMR workloads, Athena |
| **Amazon Ion** | Row, text or binary | Self-describing, richly typed superset of JSON | Used by **DynamoDB export to S3** (DynamoDB JSON or Ion) and QLDB heritage; Athena has an Ion SerDe | DynamoDB exports |

Parquet vs ORC on the exam: both are correct "columnar" answers; Parquet is the default in most AWS scenarios (Spark, Glue, Firehose, Iceberg). ORC appears when the scenario is Hive-centric or mentions ORC explicitly. Avro vs Parquet: *"streaming, schema evolution, write-heavy, whole records"* → Avro; *"analytics, Athena cost, read a few columns"* → Parquet.

**THE trap:** a "JSON" file containing a **pretty-printed** document or a single top-level array (`[{...},{...}]`) returns errors or one giant row in Athena/Glue. Readers expect **JSON Lines** — one complete object per line. Fix upstream or reformat with a Glue job/Lambda. (Firehose format conversion also rejects arrays of documents.)

## Compression codecs and splittability

A file is **splittable** when an engine can start reading in the middle, so many workers process one file in parallel. Whole-file text compression often breaks that.

| Codec | Ratio | Speed | Splittable when compressing a whole text file? | Notes |
|---|---|---|---|---|
| **GZIP** (.gz) | High | Medium | **No** | Widely supported; Athena's default write codec for Parquet/text in Hive tables; Redshift `COPY` reads it |
| **BZIP2** (.bz2) | Highest | **Slow** | **Yes** | Splittable but CPU-expensive |
| **Snappy** | Moderate | **Very fast** | No (as a raw text codec) | **Default in Spark/Glue Parquet**; great inside columnar files |
| **LZO** | Moderate | Fast | **Only with an index** | Hadoop-era |
| **ZSTD** (Zstandard) | High | Fast | No (plain .zst) | Best ratio/speed balance; Athena's default for **Iceberg** tables; Redshift Iceberg writes default to ZSTD |
| **LZ4** | Lower | Fastest | No | Speed over size; Athena reads but doesn't write LZ4 Parquet |
| **ZLIB / DEFLATE** | High | Medium | Inside ORC/Avro blocks | ORC default (ZLIB); DEFLATE for Avro |

**The key insight:** inside **Parquet and ORC**, compression is applied **per page / per stripe**, not to the whole file — so a Parquet file compressed with Snappy, GZIP or ZSTD **stays splittable**. Same for Avro (compressed per block). Splittability only bites for whole-file-compressed **text** (CSV, JSON, logs).

**THE trap:** *"A Glue/EMR job processing one 20 GB `.csv.gz` file uses only one task while the other workers sit idle."* GZIP isn't splittable, so one task must decompress the entire file. Fix: have the producer write **many smaller files** (e.g., 100–250 MB each), use a splittable codec (BZIP2) if you must keep text, or — best — convert once to **Parquet**. Adding workers does nothing.

More compression facts the exam uses:

- Athena bills bytes scanned **as stored** (compressed), so compression cuts cost directly.
- Athena detects text compression from the **file extension** (`.gz`, `.bz2`...); lowercase extensions only, and **ZIP is not supported**.
- Redshift `COPY` accepts GZIP, LZOP, BZIP2 and ZSTD files; split the load into multiple files **roughly equal in size, 1 MB–1 GB after compression**, ideally a multiple of the number of slices — see [Guide 24](24-Redshift-Loading-Integration-Sharing.md).

## File sizing and the small-files problem

Target files large enough that per-file overhead is negligible but numerous enough to parallelize. A widely used rule of thumb for S3 analytics is **~128 MB to ~1 GB per file** (Athena tuning guidance aims for splits around **128 MB**; Parquet row groups default to 128 MB; S3 Tables compaction targets **512 MB** by default). Redshift `COPY` wants **1 MB–1 GB compressed** per file.

Why thousands of tiny files hurt:

- Every file costs an S3 **LIST/GET request**, an open, a footer read and task-scheduling overhead — engines spend more time opening than reading.
- Many requests against one prefix can trigger **503 Slow Down** throttling (S3 supports **5,500 GET/HEAD per second per prefix**).
- Columnar metadata overhead outweighs its benefit on tiny files.
- Spark drivers/Glue jobs can run out of memory tracking millions of file splits.

Causes: streaming writes with short buffers, over-partitioning (e.g., by hour *and* customer), Spark jobs with too many output partitions, per-event Lambda writes.

| Fix | Where |
|---|---|
| **Compaction job** — periodically rewrite small files into large ones (Glue/EMR Spark `coalesce`/`repartition` before write) | Any lake |
| **Glue file grouping**: `groupFiles: 'inPartition'` + `groupSize` (bytes) — reads many small files into one task; **auto-enabled above 50,000 files**; works for CSV, JSON, XML, Ion, grokLog (not Parquet/ORC/Avro) | Glue DynamicFrames ([Guide 12](12-AWS-Glue-ETL.md)) |
| **S3DistCp `--groupBy` (regex) + `--targetSize`** — concatenates small files while copying | EMR ([Guide 15](15-Amazon-EMR.md)) |
| **Bigger Firehose buffers** (size up to 128 MiB, interval up to 900 s) | Ingestion ([Guide 07](07-Amazon-Data-Firehose.md)) |
| **Iceberg compaction** — Athena `OPTIMIZE ... REWRITE DATA USING BIN_PACK`, Glue Data Catalog compaction optimizer, S3 Tables automatic compaction | Table formats ([Guide 04](04-Open-Table-Formats-S3-Tables.md)) |
| **Coarser partitions** — partition by day, not minute; avoid high-cardinality keys | Design ([Guide 26](26-Amazon-Athena.md)) |

**THE trap:** *"queries slowed as a Firehose/streaming pipeline wrote millions of 50 KB objects"* — adding Athena capacity or more partitions makes it worse. The fix is **compaction plus larger buffers** (or an Iceberg table with automatic compaction).

## Converting formats (skill 1.2.6)

| Path | How | Pick when |
|---|---|---|
| **Firehose record format conversion** | Converts **JSON → Parquet or ORC** on the fly using a schema from a **Glue Data Catalog table**; deserializer = OpenX JSON SerDe or Hive JSON SerDe; **CSV/other input needs a Lambda transform to JSON first** | Streaming data should land as Parquet with **no code/least overhead** |
| **Athena CTAS** | `CREATE TABLE ... WITH (format='PARQUET', write_compression='SNAPPY', partitioned_by=ARRAY['dt'], bucketed_by=ARRAY['user_id'], bucket_count=16) AS SELECT ...`; default format **Parquet**; one query can write at most **100 partitions** (chain `INSERT INTO` for more) | One-off or scheduled SQL conversion, no infrastructure |
| **Athena `INSERT INTO` / `UNLOAD`** | Append converted data; `UNLOAD` writes query results as Parquet/ORC/Avro/JSON/text | Incremental SQL conversion, exports |
| **AWS Glue ETL** | Read CSV/JSON → write `format="parquet"` (Glue's optimized writer: `useGlueParquetWriter` / legacy `glueparquet`, which computes schema on the fly and handles evolving schemas), partitioned, with job bookmarks | Recurring pipelines, complex transforms, large volumes |
| **EMR Spark** | `df.write.parquet(...)` / ORC | Existing Spark code, huge scale, custom libs |
| **Glue DataBrew** | Recipe job outputs CSV, JSON, Parquet, Avro, ORC and others | Visual, no-code prep ([Guide 14](14-Glue-DataBrew-Data-Preparation.md)) |
| **Redshift `UNLOAD`** | `UNLOAD ('select ...') TO 's3://...' IAM_ROLE ... FORMAT AS PARQUET PARTITION BY (dt)` (Parquet output Snappy-compressed by default) | Archive/share warehouse data to the lake |
| **AWS DMS S3 target** | Endpoint setting `DataFormat=parquet` (default is CSV) | Database CDC landing directly as Parquet |

```sql
-- Athena: convert a raw CSV table to partitioned, Snappy-compressed Parquet
CREATE TABLE curated.orders_parquet
WITH (format = 'PARQUET',
      write_compression = 'SNAPPY',
      external_location = 's3://amzn-s3-demo-bucket/curated/orders/',
      partitioned_by = ARRAY['order_date'])
AS SELECT order_id, customer_id, amount, order_date   -- partition column last
FROM raw.orders_csv;
```

**THE trap:** Firehose format conversion is **JSON-only input**. *"Firehose receives CSV and must deliver Parquet"* → add a **Lambda transformation** (CSV → JSON), then enable record format conversion with a Glue table schema.

## SerDes in Athena and Glue

A **SerDe** (serializer/deserializer) tells Athena/Hive how to turn bytes into rows. Picking the wrong one is a classic troubleshooting question.

| SerDe | Use for | Watch out |
|---|---|---|
| **LazySimpleSerDe** (default for `ROW FORMAT DELIMITED`) | Simple CSV/TSV/custom delimiters, fastest | **Doesn't understand quotes** — `"Smith, John"` splits into two columns and quotes stay in values |
| **OpenCSVSerDe** | CSV with **quoted fields containing commas** (`quoteChar`, `separatorChar`, `escapeChar`) | Reads values as strings and converts; embedded line breaks inside fields are not supported |
| **Hive JSON SerDe** (`org.apache.hive.hcatalog.data.JsonSerDe`) | Well-formed JSON Lines; timestamp format control | Strict: errors on malformed records or type mismatches |
| **OpenX JSON SerDe** (`org.openx.data.jsonserde.JsonSerDe`) | Messier JSON: `ignore.malformed.json`, **case-insensitive keys by default**, `dots.in.keys`, key-name mappings for duplicate/illegal names | Pick it when records are occasionally malformed or keys differ only by case |
| **Parquet / ORC SerDes** (`STORED AS PARQUET` / `ORC`) | Columnar files | Column access by name vs index (below) |
| **Avro SerDe** | Avro files; schema via `avro.schema.literal`/URL | — |
| **Ion SerDe** | DynamoDB exports in Ion | — |
| **Grok SerDe** / **RegexSerDe** | Unstructured **log lines** (Apache, syslog, custom app logs) parsed by named patterns or regex groups | Regex must match whole line; unmatched lines → NULLs |
| **CloudTrail SerDe** | CloudTrail JSON logs (or use CloudTrail Lake / Athena's built-in table creation) | — |

- `TBLPROPERTIES ('skip.header.line.count'='1')` skips CSV header rows (LazySimple and OpenCSV).
- Glue crawlers use **classifiers** (built-in CSV/JSON/XML/Parquet/ORC/Avro, custom Grok/XML/JSON/CSV) to choose schemas — see [Guide 13](13-Glue-Data-Catalog-Crawlers.md).

**THE trap:** *"Athena query on a CSV returns shifted columns and values with quote characters"* → the table uses LazySimpleSerDe; recreate it with **OpenCSVSerDe**. *"Some JSON records are malformed and the whole query fails"* → **OpenX JSON SerDe with `ignore.malformed.json = true`**.

## Schema evolution by format

How a reader maps file columns to table columns decides which schema changes are safe:

- **CSV/TSV are positional** — column 3 is column 3. You can **add columns at the end** and rename freely, but adding in the middle, removing or reordering corrupts everything after it.
- **JSON and Avro map by field name** — adding, removing and reordering are safe; renames look like drop + add. Avro adds formal compatibility rules (new fields need defaults).
- **Athena reads Parquet by name by default** (`parquet.column.index.access = false`) → add/remove/reorder safe, **renames break** (old files return NULL for the new name).
- **Athena reads ORC by index by default** (`orc.column.index.access = true`) → renames safe, but adding in the middle or removing columns misaligns data. Flip the property to read by name.
- Safest universal move: **add new columns at the end**.
- Table formats fix this properly: **Iceberg tracks columns by field ID**, so add/drop/rename/reorder are all safe ([Guide 04](04-Open-Table-Formats-S3-Tables.md)). Schema registry compatibility modes and SCDs live in [Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md).

**THE trap:** *"after a column was renamed in new Parquet files, Athena returns NULLs for that column in older partitions"* — Parquet is read by name, so old files don't have the new name. Options: keep the old name, use a view to coalesce, or move to Iceberg for safe renames.

## Partitioning and bucketing (preview)

- **Partitioning** splits data into folders by a low-to-moderate cardinality key the queries filter on (`dt=2026-09-01/`), so engines skip whole prefixes.
- **Bucketing** hashes a **high-cardinality** key (e.g., `user_id`) into a fixed number of files inside each partition, so lookups on one value read one file.
- Combine them with sorting inside files to maximize min/max skipping. Depth in [Guide 26](26-Amazon-Athena.md) (partition projection, `bucketed_by`) and [Guide 05](05-S3-Data-Lake-Storage.md) (S3 prefix layout).
- Plain files + partitions still lack ACID, row-level updates and safe concurrent writes — that's what table formats add on top ([Guide 04](04-Open-Table-Formats-S3-Tables.md)).

## Code-level optimization reminders (skill 1.4.1)

Runtime drops when you **read less and move less**: select only needed columns early, push filters down to the source (Glue `push_down_predicate` on partitions, JDBC pushdown), write columnar and compressed, right-size output partitions (`coalesce`/`repartition`) to avoid small files, and avoid shuffles with broadcast joins — see [Guide 16](16-Apache-Spark-Essentials.md).

## Question patterns

> *"Analysts query 5 TB of CSV logs in S3 with Athena, usually selecting 4 of 60 columns filtered by date. Reduce cost and improve performance with the LEAST effort."* → **Convert to partitioned Parquet (e.g., Athena CTAS or a Glue job) partitioned by date** (column pruning + partition pruning + compression; compressing the CSV with GZIP helps less and still scans every column)

> *"A Firehose stream receives JSON clickstream events. The data must land in S3 in a columnar format for Athena with no custom code."* → **Enable Firehose record format conversion to Parquet using a Glue Data Catalog table schema** (built-in; a downstream Glue job adds components)

> *"Firehose receives CSV records from devices; the lake requires Parquet."* → **Lambda transform CSV → JSON in Firehose, then record format conversion to Parquet** (conversion only accepts JSON input)

> *"A nightly Glue Spark job reads a single 40 GB gzip-compressed CSV and runs for hours even after doubling the workers."* → **Have the source produce many smaller files or convert once to Parquet; GZIP isn't splittable** (one task decompresses the whole file — more DPUs don't help)

> *"A data lake receives millions of 20 KB JSON files per day; Athena queries are slow and occasionally fail with S3 Slow Down errors."* → **Compact into larger files (~128 MB+ Parquet) with a scheduled Glue/EMR job and increase producer buffering** (small-files overhead and per-prefix GET limits)

> *"A Glue ETL job reading hundreds of thousands of small CSV files runs out of driver memory and spends most time scheduling tasks."* → **Enable `groupFiles: 'inPartition'` with a suitable `groupSize`** (groups small files per task; auto-on above 50,000 files)

> *"An EMR cluster must copy log files from one S3 prefix to another, combining many small hourly files into ~1 GB files."* → **S3DistCp with `--groupBy` and `--targetSize`** (built for concatenating while copying)

> *"Kafka producers need a compact binary format with an embedded schema and strong backward-compatible evolution; consumers read whole records."* → **Avro with the AWS Glue Schema Registry** (row-based streaming + evolution; Parquet is for analytics files)

> *"An Athena table over CSV exports shows address values split across columns and stray double quotes."* → **Recreate the table with OpenCSVSerDe (quoteChar `"`)** (LazySimpleSerDe ignores quotes)

> *"Some JSON event records are malformed, causing Athena queries to fail. Analysts want to query the valid records."* → **OpenX JSON SerDe with `ignore.malformed.json` set to true** (Hive JSON SerDe is strict)

> *"Engineers must query Apache web server logs in S3 with SQL without preprocessing."* → **Athena table using Grok (or Regex) SerDe** (parses unstructured log lines at read time)

> *"A Redshift team must archive 3-year-old fact data to S3 in a format Athena and Spectrum can query cheaply."* → **Redshift `UNLOAD ... FORMAT AS PARQUET PARTITION BY (year, month)`** (columnar, partitioned, queryable in place)

> *"DMS replicates an Aurora database to S3 for Athena; analysts complain CSV output is slow to query."* → **Set the DMS S3 target endpoint `DataFormat` to parquet** (native, no extra job; default is CSV)

> *"After a column was renamed in newly written ORC files, Athena shows correct data; after a column was added in the middle, values in later columns look wrong."* → **ORC is read by index by default — set `orc.column.index.access=false` (read by name) or add columns only at the end** (index access tolerates renames but not inserts)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Analytics, few columns, Athena cost | Parquet (or ORC) |
| Whole records, streaming, schema evolution | Avro (+ Glue Schema Registry) |
| DynamoDB export format | DynamoDB JSON or Amazon Ion |
| Hive-centric columnar, bloom filters | ORC |
| Athena/Glue JSON must be | JSON Lines — one object per line |
| XML in the lake | Convert via Glue (XML classifier / rowTag) — Athena has no XML SerDe |
| Column pruning + min/max skipping | Columnar formats; sort by filter column |
| Parquet row group default | 128 MB (ORC stripe 64 MB) |
| GZIP | Good ratio, NOT splittable for text |
| BZIP2 | Splittable, slow |
| Snappy | Fast; Spark/Glue Parquet default |
| ZSTD | Best ratio/speed; Athena Iceberg default |
| LZO | Splittable only with index |
| Compressed Parquet/ORC splittable? | Yes — compression is per page/stripe |
| One huge .gz = one task | Split files / convert to Parquet |
| Target file size | ~128 MB–1 GB (S3 Tables compaction 512 MB) |
| Redshift COPY file size | 1 MB–1 GB compressed, multiple of slices |
| Millions of tiny files | Compaction, bigger buffers, coarser partitions |
| Glue many small input files | `groupFiles: inPartition` + `groupSize` (auto > 50,000 files) |
| EMR combine small files | S3DistCp `--groupBy` / `--targetSize` |
| Iceberg small files | `OPTIMIZE ... BIN_PACK` / table optimizers / S3 Tables |
| Stream JSON → Parquet, no code | Firehose record format conversion + Glue table |
| Firehose CSV → Parquet | Lambda CSV→JSON first |
| SQL conversion, no infra | Athena CTAS (default Parquet, ≤ 100 partitions/query) |
| Warehouse → lake Parquet | Redshift UNLOAD FORMAT AS PARQUET |
| DMS → Parquet | S3 endpoint `DataFormat=parquet` |
| No-code visual conversion | Glue DataBrew recipe job |
| Quoted CSV fields with commas | OpenCSVSerDe |
| Simple delimited, fastest | LazySimpleSerDe |
| Malformed / case-varying JSON | OpenX JSON SerDe |
| Log lines | Grok SerDe / RegexSerDe |
| Skip CSV header | `skip.header.line.count` |
| Parquet in Athena maps columns by | Name (renames break) |
| ORC in Athena maps columns by | Index (inserts/removals break) |
| CSV maps columns by | Position — add only at end |
| Safe add/drop/rename/reorder | Iceberg (field IDs) |
| High-cardinality lookup key | Bucketing |
| Low-cardinality filter key | Partitioning |
| Compression and Athena cost | Billed on compressed bytes scanned |

Formats and partitions get you fast reads, but not transactions, row-level deletes or time travel — the next layer up is the table format: [Guide 04 — Open Table Formats & S3 Tables](04-Open-Table-Formats-S3-Tables.md).
