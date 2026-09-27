# 13 · Glue Data Catalog & Crawlers — the library card catalog of your data lake

> **Exam map:** D2 · Task 2.2 — D1 · Task 1.1 · **Skills:** 2.2.1, 2.2.2, 2.2.3, 2.2.4, 2.2.5, 1.1.5 · **Weight:** 🔥🔥🔥 High · **Read time:** ~22 min

## The idea

Think of a huge library with no card catalog. The books (your files in Amazon S3) are all there, but to find "sales from March 2026" you'd walk every aisle opening covers. Query engines have the same problem: Athena or Redshift Spectrum can't read a folder of Parquet files until someone says *what's in it*. They need the column names and types, the file format, where the files are, and how they're split into folders.

The **AWS Glue Data Catalog** is that card catalog. It holds **metadata only**, never the data. Each card (a **table**) lists the columns, the format and SerDe (serializer/deserializer, the code that knows how to read the bytes), the S3 location, and the **partitions**, which work like shelf labels (`dt=2026-03-14`) so a reader can go straight to one shelf. A **crawler** is the librarian: it walks new arrivals, works out what kind of book each is (classifiers), and files or updates the cards. Partition *projection* is a library that posts its shelving rule on the wall ("one shelf per day, labelled yyyy/MM/dd"), so you can find a shelf without checking the card index at all.

The exam cares about three things here: **how metadata gets into the catalog** (crawler vs. write-time updates vs. DDL), **how partitions stay in sync** as data lands (and which method has the *"least operational overhead"*), and **the crawler settings that decide whether you get one clean table or 400 broken ones**. Schema Registry, federation and catalog security round it out.

## What the Data Catalog is

```
Data Catalog (one default catalog per account per Region)
 └─ Database  (a namespace, e.g. sales_raw)
     └─ Table  (columns + types, classification e.g. parquet/csv/json,
     │          SerDe + input/output format, S3 location, table properties)
     └─ Partitions (one entry per partition value set: values + its own location/schema)
```

- **Hive-metastore compatible.** Anything that speaks the Hive metastore API can use it. This is the "technical data catalog" of skill 2.2.2.
- **Consumers:** **Athena**, **Redshift Spectrum** (external schema → Glue database), **EMR** (Hive, Spark, Trino/Presto configured to use Glue as the metastore through the `hive-site` / `spark-hive-site` classification, setting `hive.metastore.client.factory.class` to the Glue client factory), **Glue ETL** (`from_catalog`, and `--enable-glue-datacatalog` for Spark SQL), **Lake Formation** (permissions layered on top), SageMaker Unified Studio and Amazon Quick through Athena.
- **Replaces a self-managed Hive metastore on RDS/EC2.** *"Share one metastore across transient EMR clusters, Athena and Glue with the least overhead"* → **Glue Data Catalog**, not a Hive metastore on an RDS instance.
- **Beyond S3 tables:** entries can point to JDBC tables (RDS, Redshift), DynamoDB tables, Kafka/Kinesis streams (used by Glue streaming), and **Iceberg/Hudi/Delta** tables ([Guide 04](04-Open-Table-Formats-S3-Tables.md)).
- **Pricing:** the **first million objects stored and first million requests per month are free**, then a small per-100,000 charge. Crawlers and statistics jobs bill in DPU-hours like Glue jobs.

**Cross-account:** share catalog resources with a **Glue resource policy** (catalog-level JSON policy) or, more commonly for data lakes, with **Lake Formation grants / LF-Tags via AWS RAM** ([Guide 40](40-Lake-Formation.md)). Lake Formation is also the answer for **column-, row- and cell-level** permissions. IAM plus resource policies only reach database/table level.

## Getting metadata in — four doors

| Door | How | Best when |
|---|---|---|
| **Crawler** | Scans a store, infers schema and partitions | Unknown or evolving schemas, onboarding new sources, many tables |
| **Write-time update** | Glue ETL `enableUpdateCatalog`, Firehose format conversion + dynamic partitioning, EMR/Spark `saveAsTable`, Iceberg commits | You control the writer, so the catalog is updated the moment the data lands |
| **DDL** | Athena/Spark `CREATE EXTERNAL TABLE`, `ALTER TABLE ADD PARTITION` | Schema is known and stable; infrastructure as code |
| **API/IaC** | `CreateTable`, `BatchCreatePartition`, CloudFormation `AWS::Glue::Table` | Automated pipelines, Lambda on S3 events |

## Crawlers — the librarian

**Supported stores:** **S3**, **DynamoDB**, **Delta Lake, Apache Iceberg, Apache Hudi** (point at the table's metadata path) through native clients. Through **JDBC**: Redshift, Snowflake, Aurora, MariaDB, SQL Server, MySQL, Oracle, PostgreSQL (in RDS or elsewhere). Through the **MongoDB client**: MongoDB, MongoDB Atlas, **Amazon DocumentDB**. JDBC and MongoDB-type sources need a **Glue connection** (2.2.5). S3 can optionally use a *Network* connection to crawl through a VPC. **Crawlers cannot crawl data streams.** For Kafka/Kinesis you create the table manually or use the Schema Registry.

### Classifiers — how the librarian recognizes a book

- **Built-in classifiers** recognize Avro, Parquet, ORC, JSON, CSV, XML, Ion, common log formats (Apache, syslog and similar) and compressed variants.
- **Custom classifiers** run **first**, in the order you list them. Four kinds exist: **Grok** (regex-like patterns for logs), **XML** (row tag), **JSON** (a **JSONPath** expression such as `$.records[*]` to pick the record array), and **CSV** (delimiter, quote character, header present/absent).
- A classifier returns a certainty. The first one with **1.0** wins. If none reaches 1.0, the highest certainty wins. If all return 0.0, the table is classified `UNKNOWN`.

**THE trap:** *"A crawled CSV table has columns `col0, col1, col2…` and the header row appears as data."* When every column is a string, the built-in CSV classifier can't tell the header from data. Fix it with a **custom CSV classifier set to "has heading"**. Renaming columns by hand gets undone at the next crawl.

**THE trap:** *"JSON files wrap records inside a top-level array, and the crawler creates one column called `array`."* Use a **custom JSON classifier with a JSONPath** (`$[*]`) so each element becomes a row.

### Crawler configuration that the exam tests

| Setting | Options | Why it matters |
|---|---|---|
| **Include path + exclude patterns** | Glob patterns (`**/_temporary/**`, `**.metadata`, `**/*.crc`) | Keep manifests, temp folders and non-data files out |
| **Schedule** (skill 1.1.5) | On demand, or **cron** schedules (e.g., `cron(0 2 * * ? *)`); can also be started by a Glue trigger/workflow, EventBridge, Step Functions or MWAA | Run it after data lands, not blindly every hour |
| **Recrawl policy** | **Crawl all folders** · **Crawl new folders only** (incremental; existing partitions aren't re-checked) · **Crawl based on S3 events** (reads S3 event notifications from an **SQS queue**, directly or via EventBridge, and visits only changed objects) | Event mode is the fastest and cheapest for big buckets: no full listing |
| **Schema change policy — updates** | **Update the table definition** · **Add new columns only** · **Ignore the change / log only** (`UPDATE_IN_DATABASE` / `LOG`) | Protect hand-tuned schemas with "add new columns only" or "log" |
| **Schema change policy — deletions** | **Delete** · **Mark as deprecated** · **Log/ignore** (`DELETE_FROM_DATABASE` / `DEPRECATE_IN_DATABASE` / `LOG`) | What happens to a table when its S3 data disappears |
| **Update all new and existing partitions with metadata from the table** | Partitions inherit the table's schema | Fixes **`HIVE_PARTITION_SCHEMA_MISMATCH`** in Athena |
| **Create a single schema for each S3 path** (table grouping) | Combine compatible schemas under one table | Stops one table per folder when files differ slightly |
| **Table level** | Which folder depth becomes the table root | Forces the table boundary where you want it |
| **Sampling** | S3: crawl only the first N files per leaf folder; DynamoDB/MongoDB: scan a sample | Faster crawls of homogeneous data |
| **Max tables threshold** | Fail the crawl if it would create more than N tables | Guard rail against table explosions |
| **IAM role** | Needs S3 read on the path, **`kms:Decrypt` for SSE-KMS objects**, plus Glue catalog permissions (`AWSGlueServiceRole`) | *"Crawler runs but creates no tables / AccessDenied"* is usually S3 or KMS permissions |
| **Lake Formation credentials** | The crawler can use Lake Formation-vended credentials for S3 locations registered with LF (also cross-account) | Works with LF-governed lakes without broad IAM S3 grants |

### Partitions: what the crawler sees

A crawler turns folder levels under the table root into **partition columns**:

```
s3://amzn-s3-demo-bucket/sales/year=2026/month=03/day=14/part-0001.parquet   → year, month, day   (Hive-style)
s3://amzn-s3-demo-bucket/sales/2026/03/14/part-0001.parquet                  → partition_0, partition_1, partition_2
```

**THE trap:** paths **without `key=value`** produce **`partition_0`, `partition_1`…** columns. Either write Hive-style paths upstream (Firehose dynamic partitioning, Glue `partitionKeys`, or Spark `partitionBy`), rename the columns in the table, or define the table with DDL and partition projection.

**THE trap:** **mixed formats or incompatible schemas under one prefix** (CSV and JSON together, or two unrelated feeds in sibling folders) make the crawler create **many small tables, one per folder**, instead of one partitioned table. Fix it at the source: **one dataset per prefix, one format per dataset**. Or use include/exclude paths, "single schema per S3 path", and table level.

**THE trap:** treating crawlers as free and instant. A crawler is a DPU-billed job that lists S3 and reads samples. On a bucket with millions of objects, a full crawl every 15 minutes is slow and expensive, and there's always a gap between data landing and the partition appearing. Prefer **S3-event-based crawls**, **write-time catalog updates**, or **partition projection**.

## Partition synchronization (skill 2.2.4) — ranked by operational overhead

New data lands every hour. How does the catalog learn about the new shelf? From least to most effort:

| Method | How it works | Engines | Notes |
|---|---|---|---|
| **1. Partition projection** | Table properties describe the pattern; **Athena computes partitions at query time**; nothing is registered | **Athena only** | Zero maintenance, great for very many partitions. Athena **ignores catalog partitions** on projected tables; **Redshift Spectrum, EMR and Athena for Spark use normal catalog partitions** |
| **2. Write-time registration** | The writer adds the partition: Glue `getSink(enableUpdateCatalog=True, partitionKeys=…)`, Firehose (with format conversion + dynamic partitioning to Glue tables), EMR/Spark writing to catalog tables, **Iceberg** (no Hive partitions to sync at all) | All engines | Partition exists the moment data does ([Guide 12](12-AWS-Glue-ETL.md)) |
| **3. Event-driven DDL/API** | S3 event → **Lambda** runs `ALTER TABLE ADD PARTITION` (Athena) or **`BatchCreatePartition`** (Glue API) | All engines | Works for **non-Hive paths** (you supply the `LOCATION`) |
| **4. `MSCK REPAIR TABLE`** | Athena/Hive scans the table location and adds missing partitions | All (once registered) | **Hive-style `key=value` paths only**; slow on large tables; only adds, doesn't remove missing partitions |
| **5. Scheduled / event crawler** | Crawler discovers new folders | All engines | Most moving parts; latency between landing and visibility |

Decision rules:
- *"Athena queries on a table with a new partition every hour, least operational overhead, no crawler"* → **partition projection**.
- *"Same table must also be queried from Redshift Spectrum"* → projection alone won't do it. **Register partitions**: write-time updates, or Lambda + `ALTER TABLE ADD PARTITION` / `BatchCreatePartition`.
- *"Folders are `2026/03/14` (not Hive-style) and MSCK REPAIR finds nothing"* → `ALTER TABLE ADD PARTITION ... LOCATION` or projection with `storage.location.template`.

A correct partition projection table for non-Hive date paths:

```sql
CREATE EXTERNAL TABLE app_logs (
  request_id  string,
  status      int,
  latency_ms  bigint
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://amzn-s3-demo-bucket/app-logs/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.format'        = 'yyyy/MM/dd',
  'projection.dt.range'         = '2024/01/01,NOW',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'storage.location.template'   = 's3://amzn-s3-demo-bucket/app-logs/${dt}/'
);
```

Projection types: **`date`**, **`integer`**, **`enum`** (finite list such as Regions), **`injected`** (the value must appear in the query's WHERE clause, e.g., a device ID). Queries outside the range return zero rows, not an error, and `SHOW PARTITIONS` doesn't list projected partitions. If most projected partitions would be empty, ordinary registered partitions perform better. Athena-side detail is in [Guide 26](26-Amazon-Athena.md).

## Partition indexes and column statistics — making lookups fast

**Partition indexes** fix a specific problem: a table with hundreds of thousands to millions of partitions, where every `GetPartitions` call loads them all and then filters.
- Up to **3 partition indexes per table**, each on a subset or ordering of the partition keys (string, numeric and date types).
- The query must filter on the index's **first key**. Supported operators are `=, >, >=, <, <=, BETWEEN` joined with AND. `OR`, `IN`, `LIKE` and `NOT` parts are filtered afterwards.
- **Redshift Spectrum, EMR and Glue Spark DataFrames** use active indexes automatically. **Athena** needs the table property **`partition_filtering.enabled = true`**. **Glue DynamicFrames** use them through **`catalogPartitionPredicate`** ([Guide 12](12-AWS-Glue-ETL.md)).
- Once an index exists, new partitions are type-checked against it, and you can't rename or retype the indexed keys.

**Column statistics** (min/max, distinct count, null count, average/max length) can be computed by Glue for catalog tables, on demand or on a schedule (for Iceberg through table optimizers). **Athena and Redshift Spectrum's cost-based optimizers** use them to choose join order and aggregation strategy. Signal: *"improve join performance in Athena without changing the queries"* → generate column statistics.

## Glue Schema Registry — the publisher's style guide for streams

Streams have no folders to crawl, so producers and consumers need a contract. The **Glue Schema Registry** stores versioned schemas in **Avro, JSON Schema or Protobuf**, organized in registries. Producer and consumer **serializer/deserializer (SerDe) libraries** for **Apache Kafka / Amazon MSK, Kinesis Data Streams (KPL/KCL), Managed Service for Apache Flink, and Lambda** check each record against the registered schema. A producer sending an incompatible record fails **at the producer**, before bad data enters the stream. Glue streaming jobs can also read tables backed by a registry schema (Avro).

**Compatibility modes** decide which new versions are accepted:

| Mode | New version must... |
|---|---|
| **BACKWARD** (default) | let consumers on the new schema read data written with the **previous** version (e.g., delete a field, add an optional field) |
| **BACKWARD_ALL** | ...read data written with **all** previous versions |
| **FORWARD** | let consumers on the **previous** version read data written with the new one (e.g., add a field, delete an optional field) |
| **FORWARD_ALL** | ...let consumers on **all** previous versions read new data |
| **FULL** | be both backward and forward compatible with the previous version |
| **FULL_ALL** | be both backward and forward compatible with all previous versions |
| **NONE** | anything accepted, no checks |
| **DISABLED** | no new versions accepted at all (schema frozen) |

Pick BACKWARD when **consumers upgrade first**, FORWARD when **producers upgrade first**, FULL when either side may upgrade at any time. Schema evolution strategy in depth: [Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md).

## Connections for cataloging (skill 2.2.5)

Crawlers and jobs share the same **Glue connection** objects: **JDBC** (URL + credentials, ideally a **Secrets Manager** secret, plus VPC/subnet/security group), **MongoDB/DocumentDB**, **Network** (VPC path only), **Kafka**, and native connectors (Snowflake, BigQuery and others). Always run **Test connection**. It fails for the same reasons jobs do: a missing **self-referencing all-TCP rule** on the security group, a database security group that doesn't admit Glue's group, no route to the database, or no S3 gateway endpoint/NAT for the crawler to reach S3 and Glue from a private subnet. A JDBC crawler's include path looks like `database/schema/%`, where `%` is a wildcard. Network depth: [Guide 38](38-Networking-for-Data-Pipelines.md). Job-side use: [Guide 12](12-AWS-Glue-ETL.md).

## One catalog, many catalogs — federation and views

The Glue Data Catalog has grown from "one Hive metastore per Region" into a **multi-catalog** layer under the SageMaker lakehouse architecture:
- **Amazon S3 Tables** show up as a federated catalog (`s3tablescatalog`), with each table bucket as a sub-catalog, so Athena, EMR, Glue and Redshift can query managed Iceberg tables ([Guide 04](04-Open-Table-Formats-S3-Tables.md)).
- **Redshift managed storage** (and Redshift databases) can be registered as catalogs, so Spark and Athena reach them through the same catalog.
- **Catalog federation to remote Iceberg REST catalogs** lets you query tables held in external Iceberg catalogs in place, with Lake Formation permissions applied.
- **Data Catalog views** are multi-dialect SQL views defined once and queryable from several engines (Athena, Redshift Spectrum, Spark), with **definer-style** permissions: grant access to the view without granting the underlying tables.

On the exam these show up mainly as *"single place to govern and query Iceberg tables across engines"* → Glue Data Catalog (+ Lake Formation). The old answer of "run your own Hive metastore" isn't it.

## Catalog security

- **Metadata encryption at rest:** a catalog-level setting that encrypts databases, tables, partitions and more with an **AWS KMS key**. Every principal that reads the catalog (Athena users, crawler roles, EMR roles) then needs **`kms:Decrypt`** on that key, a common cause of *"Athena can't see tables after encryption was enabled"*.
- **Connection password encryption:** a separate setting that KMS-encrypts passwords stored in connection objects. Better still, keep credentials in Secrets Manager.
- **Access control:** IAM identity policies (`glue:GetTable`, `glue:GetPartitions`…) + the **catalog resource policy** for cross-account + **Lake Formation** for fine-grained and tag-based access. When Lake Formation governs a table, IAM alone isn't enough; LF grants must allow the access too ([Guide 40](40-Lake-Formation.md)).
- **Audit:** catalog API calls are recorded in **CloudTrail** ([Guide 43](43-Audit-Logging-CloudTrail-Config.md)).

## Technical catalog vs business catalog

The Glue Data Catalog answers *"what columns, what format, where"*. That's **technical metadata** for engines. **Amazon SageMaker Catalog** (built on Amazon DataZone) answers *"what does this dataset mean, who owns it, may I use it"*: a **business catalog** with glossaries, metadata forms, AI-generated descriptions, publish/subscribe access requests and lineage. It *harvests* technical metadata from the Glue catalog and Redshift. 🆕 **New in exam guide v1.1:** skill 2.2.6 covers business catalogs. Deep dive in [Guide 41](41-SageMaker-Unified-Studio-Catalog-Governance.md). Signal words: *"business users discover and request access"*, *"glossary"*, *"data products"* → SageMaker Catalog. *"Schema for Athena/EMR/Spectrum"* → Glue Data Catalog.

## Question patterns

> *"Transient EMR clusters, Athena and Glue ETL jobs must share table definitions. The team currently runs a Hive metastore on an RDS instance. LEAST operational overhead?"* → **Use the Glue Data Catalog as the metastore for EMR (hive-site / spark-hive-site configuration)** (serverless and shared; self-managed metastores are the distractor)

> *"A crawler over `s3://…/clicks/2026/09/26/` creates columns named partition_0, partition_1, partition_2, and analysts can't filter by date intuitively."* → **Write Hive-style `year=/month=/day=` paths upstream (or rename the partition columns / use partition projection with a storage template)** (non-Hive paths produce generic partition names)

> *"A crawler pointed at one bucket prefix created 350 tables instead of one."* → **Different schemas/formats share the prefix: separate datasets into their own prefixes, or enable 'create a single schema for each S3 path' and set the table level** (the librarian files a separate card for every incompatible folder)

> *"Athena returns HIVE_PARTITION_SCHEMA_MISMATCH after a column type changed in newer files."* → **Crawler option 'Update all new and existing partitions with metadata from the table'** (partitions inherit the table schema; re-crawling without it keeps the mismatch)

> *"A bucket receives millions of new objects a day; the nightly full crawl takes hours. Reduce crawl time and cost."* → **S3 event-based recrawl (S3 event notifications → SQS → crawler in event mode)** (visits only changed objects; 'new folders only' still lists everything and misses new files in existing folders)

> *"IoT data lands hourly under `dt=` prefixes and is queried only from Athena. New hours must be queryable immediately with the LEAST operational overhead."* → **Partition projection on `dt` with `range = …,NOW`** (no crawler, no Lambda, no MSCK; nothing to keep in sync)

> *"The same hourly table must now also be queried from Redshift Spectrum, and new partitions must appear within minutes."* → **Register partitions at write time (Glue `enableUpdateCatalog` / Firehose) or via S3 event → Lambda → `BatchCreatePartition`** (Spectrum ignores Athena partition projection)

> *"Files land in `s3://…/logs/2026/09/26/`. The team runs `MSCK REPAIR TABLE`, but no partitions are added."* → **`ALTER TABLE ADD PARTITION … LOCATION` (or partition projection)** (MSCK only understands Hive `key=value` folders)

> *"A table has 4 million partitions; Glue jobs and Spectrum queries spend minutes fetching partition metadata."* → **Create a partition index on the most-filtered leading key (Athena also needs `partition_filtering.enabled`)** (server-side pruning; max 3 indexes per table)

> *"A crawler runs successfully but creates no tables for an SSE-KMS encrypted bucket."* → **Grant the crawler role `kms:Decrypt` on the key (plus S3 read)** (encryption denies the sample reads silently)

> *"Crawled CSV tables show col0, col1… and the header row as data."* → **Custom CSV classifier with 'has heading'** (all-string columns defeat header detection)

> *"Kafka producers on MSK must be prevented from publishing records that would break existing consumers; consumers upgrade before producers."* → **Glue Schema Registry with Avro/Protobuf schema in BACKWARD compatibility, SerDe in producers** (enforced at the producer; FORWARD fits 'producers upgrade first')

> *"A schema for a regulatory feed must never change again."* → **Set compatibility to DISABLED** (NONE allows anything, DISABLED allows nothing)

> *"Crawl an Amazon RDS for MySQL database that's only reachable inside a private subnet."* → **JDBC Glue connection with VPC/subnet/SG (self-referencing all-TCP rule), credentials from Secrets Manager, include path `db/%`** (the crawler uses the connection's network path)

> *"Business analysts need to search datasets by business terms, see owners, and request access; engineers need schemas for Athena."* → **SageMaker Catalog for business metadata and access workflow, Glue Data Catalog as the technical catalog underneath** (two catalogs, two jobs)

> *"Join-heavy Athena queries on Parquet tables are slow; the team can't rewrite them."* → **Generate Glue Data Catalog column statistics so the cost-based optimizer can pick better plans** (no query changes needed)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Technical metadata store, Hive-compatible, serverless | Glue Data Catalog |
| Catalog stores data? | No, metadata only (location, schema, SerDe, partitions) |
| Share metastore across EMR/Athena/Glue | Glue Data Catalog as Hive metastore |
| Cross-account catalog access | Glue resource policy, or Lake Formation + RAM |
| Row/column/cell permissions | Lake Formation (not IAM) |
| Discover schema of new data | Crawler |
| Crawler sources | S3, DynamoDB, JDBC (RDS/Aurora/Redshift/Snowflake…), MongoDB/DocumentDB, Iceberg/Hudi/Delta; not streams |
| Custom log format | Grok classifier |
| JSON records inside an array | JSON classifier with JSONPath |
| CSV header not detected | Custom CSV classifier, "has heading" |
| Schedule a crawler | Cron schedule, trigger, EventBridge, Step Functions, MWAA |
| Fastest incremental crawl | S3 event mode (events → SQS) |
| Don't overwrite curated schema | Schema change policy: add new columns only / log |
| Source folder deleted | Deletion behavior: delete / deprecate / log |
| HIVE_PARTITION_SCHEMA_MISMATCH | Partitions inherit metadata from table |
| Too many tables from one prefix | Single schema per S3 path / table level / separate prefixes |
| partition_0, partition_1 | Non-Hive paths |
| Crawler AccessDenied on SSE-KMS | Role needs kms:Decrypt |
| Partitions with least overhead (Athena only) | Partition projection |
| Projection honored by Spectrum/EMR? | No, Athena only |
| Partitions visible to every engine instantly | Write-time registration (enableUpdateCatalog, Firehose, Iceberg) |
| Non-Hive paths, add partition | ALTER TABLE ADD PARTITION … LOCATION / BatchCreatePartition |
| MSCK REPAIR TABLE | Hive-style paths only, slow at scale |
| Millions of partitions, slow metadata | Partition index (max 3/table) |
| Athena use of partition index | partition_filtering.enabled = true |
| Glue DynamicFrame use of index | catalogPartitionPredicate |
| Better join plans, no query change | Column statistics (Athena/Spectrum CBO) |
| Stream schema contract | Glue Schema Registry (Avro/JSON Schema/Protobuf) |
| Default compatibility | BACKWARD |
| Consumers upgrade first | BACKWARD |
| Producers upgrade first | FORWARD |
| Either side any time | FULL (or FULL_ALL) |
| Freeze schema | DISABLED |
| Encrypt catalog metadata | Data Catalog encryption settings (KMS) |
| Encrypt connection passwords | Connection password encryption (KMS) / Secrets Manager |
| S3 Tables in the catalog | s3tablescatalog federated catalog |
| One view for many engines | Glue Data Catalog (multi-dialect) views |
| Business glossary, request access | SageMaker Catalog |

With tables and partitions in place, the next question is how non-coders clean and profile that data before it moves on. That's [Guide 14 — Glue DataBrew & Data Preparation](14-Glue-DataBrew-Data-Preparation.md).
