# 30 · Data Modeling, Schema Evolution & Lineage — change the menu without confusing the cooks

> **Exam map:** D2 · Task 2.4 (with touches of 2.1, 2.2) · **Skills:** 2.4.1, 2.4.2, 2.4.3, 2.4.4, 2.4.5 · **Weight:** 🔥🔥🔥 High · **Read time:** ~22 min

## The idea

Picture a busy **restaurant kitchen**. Every order that comes in is written on a **ticket**: table 12, two burgers, one soda, 19:42. The tickets are the *events* — thousands per night, each one small. But a ticket on its own is almost meaningless; it only makes sense next to the kitchen's reference books: the **menu** (what "burger #3" is and what it costs), the **customer book** (who sits at table 12), the **calendar** (was 19:42 a Friday in a holiday week?). In data modeling, the tickets are **facts** and the reference books are **dimensions**. Arrange the tickets in the middle with the books around them and you have drawn a **star schema** — the most-tested model on this exam.

Kitchens change. The menu gets a new column ("vegan: yes/no"), a supplier starts sending crates labeled differently, a dish is renamed. That's **schema evolution**: changing the shape of data *without* breaking the cooks still holding yesterday's tickets. Some changes are harmless (add a new optional column), some are poison (rename a column that every recipe refers to). Finally, when a guest gets sick, the health inspector asks "which farm did that lettuce come from, and which dishes used it?" — that's **data lineage**: the farm-to-table trail from source to report.

This guide covers the whole Task 2.4 kitchen: how to model data for Amazon Redshift, Amazon DynamoDB, and an AWS Lake Formation data lake; slowly changing dimensions (SCDs); modeling nested and unstructured data; which schema changes are safe per file format and per AWS Glue Schema Registry compatibility mode; schema conversion with AWS SCT and AWS DMS Schema Conversion; lineage with Amazon SageMaker ML Lineage Tracking and Amazon SageMaker Catalog; and the partitioning/indexing/compression rules of thumb the exam loves.

## Three levels of model, and the normalize/denormalize fork

**Conceptual** models name business entities and relationships ("a customer places orders"); **logical** models add attributes, keys, and normal form, still engine-agnostic; **physical** models are real tables with data types, partitions, dist/sort keys, indexes, and file formats — the level the exam tests.

**Normalization** removes redundancy: **1NF** = atomic values, no repeating groups; **2NF** = no attribute depends on only *part* of a composite key; **3NF** = no attribute depends on another non-key attribute (no transitive dependencies). Great for **OLTP** (online transaction processing — many small writes that must stay consistent): each fact lives in one place, so updates are cheap and anomaly-free.

**Denormalization** deliberately duplicates data to avoid joins. Great for **OLAP** (online analytical processing — big scans and aggregations): fewer joins, simpler SQL, faster columnar scans.

*"Transactional app with frequent updates"* → normalized (RDS/Aurora, 3NF). *"Analysts run aggregations over billions of rows"* → dimensional/denormalized (Redshift star schema, wide Parquet tables in S3).

## Star schema anatomy (and its cousins)

```
                 dim_date
                    |
 dim_customer -- fact_sales -- dim_product
                    |
                 dim_store
```

- **Fact table** — one row per business event at a declared **grain**. Holds foreign keys to dimensions plus numeric **measures**. Long and narrow; grows forever.
- **Grain** — the sentence "one row = one ___" (one order line? one order? one daily store total?). **Declare it first**; mixing grains in one fact table is the classic double-counting bug.
- **Measures:** **additive** (sum across every dimension — quantity, revenue), **semi-additive** (sum across some dimensions but not time — account balance, inventory level: average or take the last value over time), **non-additive** (ratios, percentages, unit prices — recompute from additive parts, never sum).
- **Dimension tables** — descriptive context (names, categories, regions). Short and wide; change slowly.
- **Surrogate key** — a meaningless integer key (`customer_sk`) generated in the warehouse, instead of the source system's natural/business key. Needed for SCD Type 2 (one customer, many versions), for merging sources whose natural keys collide, and for insulating the warehouse from source key changes.

| Model | Shape | Pick when |
|---|---|---|
| **Star** | Fact + denormalized dimensions (one join hop) | Default for BI on Redshift; simplest, fastest queries |
| **Snowflake** | Dimensions normalized into sub-dimensions (product → category → department) | Huge dimensions with repeated hierarchies, storage/maintenance matters more than join count |
| **One big table (OBT)** | Fact pre-joined with all dimension attributes | Columnar engines (Athena/Parquet, Redshift) where scans are cheap and joins aren't; dashboards with fixed questions |
| **Data vault** | Hubs (business keys), links (relationships), satellites (history) | Auditable, insert-only raw integration layer from many changing sources; *brief* on this exam |

**THE trap:** normalizing a Redshift warehouse to 3NF "to save space". Redshift is columnar and compresses repeated values well; the expensive thing is joins and data redistribution. Star (or moderately denormalized) wins.

## Slowly changing dimensions (SCD)

A customer moves from Lahore to Karachi. What does the dimension do?

| Type | Behavior | History? | Use when |
|---|---|---|---|
| **0** | Keep the original value forever | Frozen | "Original signup channel" must never change |
| **1** | **Overwrite** in place | **None** | Corrections (typo fixes); history irrelevant |
| **2** | **Add a new row** with a new surrogate key; mark old row expired | **Full** | *"Report sales by the region the customer lived in AT THE TIME of the sale"* |
| **3** | Add a column (`previous_city`) | **One prior value** | Only "current vs previous" comparison needed |
| **4** | Current row in the dimension; history in a separate history table | Full (separate table) | Rapidly changing attributes bloating the main dimension |
| **6** | 1 + 2 + 3 hybrid: Type 2 rows plus a "current value" column overwritten on all versions | Full + current | Report by historical AND current attribute in one query |

A Type 2 dimension row carries: `customer_sk` (surrogate), `customer_id` (natural key), the tracked attributes, `effective_from`, `effective_to` (e.g., `9999-12-31` for the open row), and `is_current`. Facts store the surrogate key that was current when the event happened — that's what makes "as-was" reporting automatic.

**Exam focus is Types 1, 2, 3.** Signal words: *"overwrite / history not required"* → 1; *"preserve full history / point-in-time reporting"* → 2; *"only the previous value"* → 3. The SQL to implement Type 2 (expire the current row, insert the new version — via `MERGE` or staging DELETE/INSERT) is worked through in [Guide 34 — SQL for Data Engineers](34-SQL-for-Data-Engineers.md).

## Modeling semi-structured and unstructured data

| Data | Store it as | Query/flatten with |
|---|---|---|
| Nested JSON in S3 | Raw JSON in the raw zone → Parquet/Iceberg with nested `struct`/`array`/`map` columns | **AWS Glue Relationalize**: flattens structs and **pivots arrays into child tables** joined back by generated keys |
| JSON in Redshift | **`SUPER`** column, loaded with `JSON_PARSE` or COPY | **PartiQL** navigation and unnesting (`FROM t, t.items AS i`) |
| JSON/Parquet in Athena | `struct`, `array`, `map` column types | `CROSS JOIN UNNEST`, `json_extract_scalar` ([Guide 34](34-SQL-for-Data-Engineers.md)) |
| Avro/Parquet/ORC | Native nested types; Avro embeds its schema | Spark, Athena, Redshift Spectrum |
| DynamoDB items | Documents: maps and lists in an item (max **400 KB**) | Key-based access; export to S3 for analytics |
| Images, PDFs, audio | Objects in S3 + a **metadata catalog** (key, owner, tags, extracted text) | Search metadata; for semantic search, **embeddings** in a vector index ([Guide 19](19-GenAI-LLMs-Vectors.md)) |

Decision rule: *"schema changes frequently / varies per record, must query in Redshift"* → **SUPER**. *"Nested JSON to relational tables with least code"* → **Relationalize** (or a DataBrew/Glue Studio flatten). *"Unstructured files must be findable by attributes"* → S3 + metadata catalog, not stuffing blobs into a database.

## Schema design per store (skill 2.4.1)

**Amazon Redshift** — star schema. Put the big fact table and its biggest joined dimension on the **same `DISTKEY`** (co-located joins); small dimensions `DISTSTYLE ALL`; sort key on the main range filter (usually date); semi-structured → `SUPER`. Constraints are **informational only — not enforced** ([Guide 33](33-Data-Quality.md)). Depth: [Guide 23](23-Redshift-Architecture-Table-Design.md).

**Amazon DynamoDB** — **access-pattern-first**: list every query, then design a high-cardinality **partition key**, a **sort key** for ranges/hierarchies (`ORDER#2026-09-01#1234`), **GSIs** for alternate patterns (addable anytime), **LSIs** only at table creation. Often **single-table design**. No joins or ad hoc analytics — export to S3 for that. Depth: [Guide 27](27-DynamoDB.md).

**Lake Formation / S3 data lake** —
- **Databases per zone and/or domain** (`raw_sales`, `curated_sales`, `analytics_finance`) so permissions map cleanly to Lake Formation grants.
- **Partition by the columns queries filter on** — almost always date (`dt=2026-09-27/`), sometimes a low-cardinality dimension (`region=`).
- **Columnar formats** (Parquet/ORC) with compression; **Apache Iceberg** tables when you need updates/deletes, time travel, or schema/partition evolution.
- **Naming conventions**: lowercase snake_case (the Glue Data Catalog stores names in lowercase), no spaces, zone prefixes.
- **Tag sensitive columns** with **LF-Tags** (e.g., `classification=pii`) and grant by tag instead of by table — scales to thousands of tables. Depth: [Guide 40 — Lake Formation](40-Lake-Formation.md).

## When the data itself changes (skill 2.4.2)

| Change in characteristics | Response |
|---|---|
| **Volume grows 10x**, or queries filter on a new column | Finer partitions (hour) or clustering; Hive-style tables need a CTAS rewrite, **Iceberg partition evolution** doesn't |
| **New optional column** | Additive, nullable — safe almost everywhere; crawler "add new columns only" |
| **Column removed / renamed** | Breaking for positional formats and downstream SQL — coordinate, or keep the old column as null |
| **Type change** | **Widening** (int → bigint, float → double, longer VARCHAR) usually safe; narrowing or string → number breaks |
| **New source / format** | New raw table, transform into the same curated schema; Schema Registry for streams |
| **Late-arriving facts** | Event-time partitions + reprocess a sliding window, or Iceberg `MERGE`; late *dimension* → placeholder ("inferred member") row filled later |
| **Data drift** (null rate spikes, new category values) | Profiling + anomaly detection — Glue Data Quality / DataBrew ([Guide 33](33-Data-Quality.md)) |

## Schema evolution: what is safe, per format and per engine

The universal rule: **adding a nullable (optional) column at the end is safe; renames, drops, reorders, and type changes are risky.** How risky depends on how the reader matches columns — **by name** or **by position**. Athena's documented behavior:

| Change | CSV/TSV | JSON | Avro | Parquet (default: by **name**) | ORC (default: by **index**) |
|---|---|---|---|---|---|
| Add column at end | Yes | Yes | Yes | Yes | Yes |
| Add column in middle | **No** | Yes | Yes | Yes | **No** |
| Remove column | **No** | Yes | Yes | Yes | **No** |
| Reorder columns | **No** | Yes | Yes | Yes | **No** |
| Rename column | Yes | **No** | **No** | **No** | Yes |

Toggle with SerDe properties `parquet.column.index.access` / `orc.column.index.access`. **THE trap:** *"We renamed a column in our Parquet files and Athena now returns NULLs"* — Parquet is read **by name**, so the old name no longer matches. Positional formats (CSV, ORC-by-index) survive renames but break on inserts/drops in the middle.

**Avro** is the evolution-friendly row format: each file embeds its **writer schema**; readers supply a **reader schema**, and fields added with **default values** resolve cleanly — which is why Avro + a schema registry dominates Kafka/Kinesis pipelines.

**AWS Glue crawler schema-change policy** (details in [Guide 13](13-Glue-Data-Catalog-Crawlers.md)):
- `UpdateBehavior`: `UPDATE_IN_DATABASE` (default — update the table definition) or `LOG` (ignore the change, just log it). Console adds **"Add new columns only"** (config `"AddOrUpdateBehavior": "MergeNewColumns"`) — keeps your curated types/names but picks up new columns.
- **"Update all new and existing partitions with metadata from the table"** (`"InheritFromTable"`) — partitions inherit the table schema, the fix for *HIVE_PARTITION_SCHEMA_MISMATCH* errors.
- `DeleteBehavior`: `LOG`, `DELETE_FROM_DATABASE`, or `DEPRECATE_IN_DATABASE` (mark the table deprecated).

**THE trap:** *"A crawler keeps overwriting column types we fixed manually"* → set the policy to **add new columns only** or **ignore/LOG**, not "delete and recreate the table".

### AWS Glue Schema Registry compatibility modes

The registry (serverless, free) stores versioned **Avro, JSON Schema, and Protobuf** schemas for Kafka/Amazon MSK, Kinesis Data Streams, Managed Service for Apache Flink, and Lambda. Producers' serializers validate each record and stamp it with the schema version ID; consumers' deserializers fetch that version. A new version is **accepted only if it passes the schema's compatibility mode** — this is how you stop a producer from shipping a breaking change.

| Mode | Checks the new version against | Meaning |
|---|---|---|
| **BACKWARD** (recommended; SerDe default) | The **previous** version | Consumers on the **new** schema can read data written with the old one → **upgrade consumers first** |
| **BACKWARD_ALL** | **All** previous versions | Same, against the whole history |
| **FORWARD** | The previous version | Consumers on the **old** schema can read data written with the new one → **upgrade producers first** |
| **FORWARD_ALL** | All previous versions | Same, against the whole history |
| **FULL** | The previous version | Both backward and forward → upgrade in any order |
| **FULL_ALL** | All previous versions | Both, against the whole history |
| **NONE** | Nothing | Any change accepted (development only) |
| **DISABLED** | — | **No new versions** can be registered at all |

Which change registers? ("Optional" = nullable/has a default; "required" = no default.)

| Proposed change | BACKWARD | FORWARD | FULL | NONE |
|---|---|---|---|---|
| Add an **optional** field | Yes | Yes | Yes | Yes |
| Delete an **optional** field | Yes | Yes | Yes | Yes |
| Add a **required** field | **No** (new readers can't fill it for old records) | Yes | **No** | Yes |
| Delete a **required** field | Yes (new readers ignore it) | **No** (old readers demand it) | **No** | Yes |

Memory hook: **BACKWARD = delete fields or add optional fields; FORWARD = add fields or delete optional fields; FULL = only add/remove optional fields.** The `_ALL` variants apply the same rule against every prior version, not just the last checkpoint.

**THE trap:** confusing DISABLED with NONE. NONE = "anything goes"; DISABLED = "frozen, no versioning".

### Engine-specific evolution

- **Apache Iceberg** tracks columns by **unique field IDs**, not names or positions — so add, drop, **rename**, reorder, and type promotion (int → long, float → double, widening decimal precision) are all metadata-only and safe. **Partition evolution** (e.g., Spark SQL `ALTER TABLE db.t ADD PARTITION FIELD hour(event_ts)`) changes layout for new data only. Deep dive: [Guide 04 — Open Table Formats](04-Open-Table-Formats-S3-Tables.md).
- **Amazon Redshift**: `ALTER TABLE ... ADD COLUMN` adds **one column per statement**, and you can't add (or drop) a column that is the DISTKEY or SORTKEY. `ALTER COLUMN ... TYPE` only **increases a VARCHAR's size** — any other type change means add-new-column, backfill, drop-old (or deep copy). For fast-changing shapes, land the payload in **`SUPER`**.
- **DynamoDB**: schemaless beyond the key attributes — new attributes simply appear on new items. Changing a *key* means a new table (or a new GSI) and a backfill.

## Schema conversion (skill 2.4.3)

Heterogeneous migrations (Oracle → Aurora PostgreSQL, SQL Server → Aurora MySQL, Teradata → Redshift) need the **schema and code** (tables, views, stored procedures, functions) translated before AWS DMS (AWS Database Migration Service) moves the data.

- **AWS DMS Schema Conversion** — **fully managed** in the DMS console; produces an **assessment report** (what converts automatically vs. manually) and applies converted DDL/code to the target. Since Dec 2024 it adds **generative AI** conversion of complex code (stored procedures, functions, triggers) using LLMs on Amazon Bedrock. Pick for *"least operational overhead / nothing to install"*.
- **AWS SCT** — the **downloadable desktop** tool with the same assessment/conversion engine plus **data extraction agents** for big **data-warehouse** migrations (Teradata, Netezza, Oracle DW → Redshift).

> ⚠️ **2026 status:** AWS SCT was **removed from the v1.1 in-scope services list**, but skill 2.4.3 still names it alongside DMS Schema Conversion. Expect DMS Schema Conversion to be the "managed / least overhead" answer and SCT to appear for data-warehouse conversions or as the older-question answer. Homogeneous migrations (MySQL → Aurora MySQL) need **no** schema conversion. Migration mechanics: [Guide 10 — DMS & Database Ingestion](10-DMS-Database-Ingestion.md).

## Data lineage (skill 2.4.4)

Lineage answers: *where did this number come from, what did we do to it, and who breaks if I change it?* It powers **trust** (provenance), **impact analysis** (downstream consumers before a change), **root-cause analysis** (trace a bad report value back, column by column), and **compliance** (show where PII flows).

**Amazon SageMaker ML Lineage Tracking** — lineage for **ML workflows**. Entities: **trial components** (processing, training, batch transform jobs), **artifacts** (URI-addressable data — an S3 dataset, an ECR image, a model), **actions** (e.g., model deployment, a pipeline step), **contexts** (logical groupings — an endpoint, a model package), and **associations** linking them (types: `ContributedTo`, `AssociatedWith`, `DerivedFrom`, `Produced`, `SameAs`). SageMaker creates entities automatically for jobs and pipelines; you can add custom ones and traverse with `QueryLineage`, including **cross-account**. Signal: *"which dataset and code version produced this model?"*

**Amazon SageMaker Catalog lineage** 🆕 (built on Amazon DataZone) — lineage for **data assets** across the organization:
- **OpenLineage-compatible**: events from OpenLineage-enabled tools (Airflow/Amazon MWAA via the OpenLineage provider, Spark, dbt) or sent directly with the **`PostLineageEvent`** API. AWS contributed an OpenLineage transport for SageMaker (merged in OpenLineage **1.33.0**).
- **Automatically captured** from **AWS Glue** (Data Catalog data source runs; Glue 5.0+ Spark jobs and notebooks can be configured to emit events) and **Amazon Redshift** (query-based) when enabled in the project/blueprint, plus Visual ETL and notebooks in Unified Studio, and catalog activity (publish, subscribe).
- Graph of **dataset nodes** and **job/job-run nodes**, **column-level lineage**, and **versioned history** so you can view lineage at a point in time.
- Read with `GetLineageNode` / `ListLineageNodeHistory`. Governance context: [Guide 41 — SageMaker Unified Studio & Catalog](41-SageMaker-Unified-Studio-Catalog-Governance.md).

> 🆕 **New in exam guide v1.1:** skill 2.4.4 now names SageMaker Catalog lineage alongside ML Lineage Tracking.

Other lineage tools: **AWS Glue DataBrew** shows a visual **data lineage** view per dataset/recipe (source → project → recipe → job → output); for fully custom lineage graphs, model nodes and edges in **Amazon Neptune** (graph database — lineage *is* a graph). Rule: **ML provenance → ML Lineage Tracking; data-asset provenance in the business catalog → SageMaker Catalog.**

## Optimization best practices (skill 2.4.5)

- **Partition** on the columns in most `WHERE` clauses, with **low-to-medium cardinality** — dates first. Target partitions holding reasonably large files (hundreds of MB), not thousands of tiny ones.
- **THE trap: over-partitioning.** Partitioning by `customer_id` or by minute creates millions of partitions with tiny files — slower planning, more S3 requests, higher cost. For **high-cardinality** filter columns use **bucketing** (hash into a fixed number of files), **sorting within files** (so Parquet min/max statistics skip row groups), or Z-order-style clustering / Iceberg sort orders.
- **Columnar + compression**: Parquet/ORC with Snappy/ZSTD cuts bytes scanned ([Guide 03](03-Data-Formats-Compression.md)).
- **RDS/Aurora indexes**: B-tree on selective filter/join columns; in a **composite index, order matters** — equality columns first, then range (`(customer_id, order_date)` helps `WHERE customer_id = ? AND order_date > ?`, not `WHERE order_date > ?` alone); **covering indexes** hold every selected column. Every index slows writes.
- **DynamoDB**: GSIs for alternate access patterns; **sparse GSIs** index only items carrying the attribute.
- **Redshift**: sort keys (zone maps skip blocks), dist keys, `ANALYZE`, `ENCODE AUTO`.
- **Amazon OpenSearch Service**: shard size **10–30 GiB** for search-latency workloads, **30–50 GiB** for write-heavy log analytics; no more than **25 shards per GiB of JVM heap** ([Guide 29](29-OpenSearch-Service.md)).
- **Statistics**: AWS Glue Data Catalog **column statistics** (distinct values, nulls, min/max — generated on demand, on a schedule, or automatically) feed the cost-based optimizers of **Athena and Redshift Spectrum** for better join ordering.

## Question patterns

> *"Analysts must report revenue by the sales region a customer belonged to at the time of each order, even after customers move."* → **SCD Type 2 dimension with surrogate keys, effective dates, and a current flag** (history + point-in-time; Type 1 overwrites history, Type 3 keeps only one prior value).

> *"A retail company's Redshift warehouse joins a 5-billion-row sales table to a 50-million-row customer table and a 2,000-row store table. Minimize data movement."* → **DISTKEY on customer_id in both big tables; DISTSTYLE ALL on the small store table** (co-located join for the big pair; small dimension copied to every node).

> *"IoT JSON payloads change shape frequently; the team must query them in Redshift without constant DDL changes."* → **Load into a SUPER column and query with PartiQL** (schemaless; ALTER TABLE per change is operational pain).

> *"Deeply nested JSON with arrays must be loaded into relational tables with the least custom code."* → **AWS Glue Relationalize** (flattens structs, pivots arrays into child tables with join keys).

> *"Kafka producers on Amazon MSK must be prevented from publishing schema changes that break existing consumers. Consumers are always upgraded first."* → **Glue Schema Registry with BACKWARD compatibility** (new readers must read old data; FORWARD fits producer-first upgrades; NONE blocks nothing).

> *"Under BACKWARD compatibility, a producer tries to add a new field with no default value. What happens?"* → **Registration fails** (consumers on the new schema can't fill the field for old records; make it optional with a default).

> *"After an upstream team renamed a column, Athena queries on the Parquet table return NULL for it."* → **Parquet is read by name** — rename the column in the table definition back / add an alias column, or use index access (`parquet.column.index.access=true`) if renames are common; Iceberg's ID-based evolution avoids this entirely.

> *"A Glue crawler keeps overwriting column names and types that the team curated manually, but new columns must still appear."* → **Crawler schema-change option 'Add new columns only'** (MergeNewColumns) — not LOG (would miss new columns), not a recrawl-from-scratch.

> *"A daily-partitioned Iceberg table now receives 50x more data and queries filter by hour. Change the layout without rewriting existing data."* → **Iceberg partition evolution** (add an hour transform; old files keep the day layout).

> *"Migrate an on-premises SQL Server database with hundreds of stored procedures to Aurora PostgreSQL with the least operational overhead."* → **AWS DMS Schema Conversion (with generative AI assistance) then DMS for data** (managed, no desktop install; SCT is the downloadable alternative).

> *"Migrate a Teradata data warehouse to Amazon Redshift, including bulk historical data extraction."* → **AWS SCT with data extraction agents** (DW-focused conversion + extraction), then load to Redshift.

> *"Data stewards need to see which upstream Glue tables and Redshift queries feed a published sales asset, including column-level lineage, in the business catalog."* → **Amazon SageMaker Catalog data lineage** (auto-captured from Glue and Redshift; OpenLineage-compatible).

> *"An ML team must prove which dataset version and processing job produced the model behind a production endpoint."* → **SageMaker ML Lineage Tracking** (artifacts, trial components, contexts, associations; QueryLineage).

> *"Athena queries on a table partitioned by user_id are slow; the bucket holds millions of tiny files."* → **Re-partition by date and bucket/sort by user_id** (high-cardinality partitioning is the anti-pattern).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| OLTP, frequent updates, consistency | Normalized (3NF) |
| Analytics, aggregations, fewer joins | Star schema / denormalized |
| Account balance, inventory level | Semi-additive measure (don't sum over time) |
| Warehouse-generated integer key | Surrogate key (needed for SCD2) |
| Overwrite, no history | SCD Type 1 |
| Full history, "at the time of" reporting | SCD Type 2 (effective dates + current flag) |
| Current + previous value only | SCD Type 3 |
| Nested JSON → relational tables | Glue Relationalize |
| Frequently changing JSON in Redshift | SUPER + PartiQL |
| DynamoDB design starting point | Access patterns first; single-table design |
| Data lake layout | DB per zone/domain, date partitions, Parquet/Iceberg, LF-Tags on PII columns |
| Safe change everywhere | Add nullable column at the end |
| Parquet default in Athena | Read by name (renames break) |
| ORC default in Athena / CSV | By position (drops/inserts in the middle break) |
| Crawler must keep curated schema but add columns | "Add new columns only" (MergeNewColumns) |
| Partition schema mismatch | InheritFromTable (update partitions from table) |
| Consumers upgrade first | BACKWARD (recommended default) |
| Producers upgrade first | FORWARD |
| No new schema versions allowed | DISABLED (not NONE) |
| Rename/reorder safely, no rewrite | Iceberg (field IDs) |
| Change partition layout without rewrite | Iceberg partition evolution |
| Redshift change column type | Only VARCHAR size increase; else add-backfill-drop |
| Managed schema conversion, least overhead | AWS DMS Schema Conversion (gen AI for complex code) |
| DW migration to Redshift + extraction | AWS SCT + data extraction agents |
| ML model provenance | SageMaker ML Lineage Tracking |
| Business-catalog lineage, column-level | SageMaker Catalog lineage (OpenLineage, PostLineageEvent) |
| High-cardinality filter column | Bucketing/sorting/clustering, not partitioning |
| OpenSearch shard size | 10–30 GiB search / 30–50 GiB logs |

Models and schemas decide how data is shaped; the next question is how long each shape should live and how to make it survive disasters — continue with [Guide 31 — Data Lifecycle, Retention & Resiliency](31-Data-Lifecycle-Retention-Resiliency.md).
