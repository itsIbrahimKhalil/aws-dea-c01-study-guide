# 04 · Open Table Formats & Amazon S3 Tables — giving a pile of files a ledger

> **Exam map:** D2 · Task 2.1, 2.2, 2.3, 2.4 — D1 · Task 1.2 · **Skills:** 2.1.7 🆕, 2.4.2, 2.4.5, 2.3.5, 1.2.6 · **Weight:** 🔥🔥🔥 High · **Read time:** ~26 min

> 🆕 **New in exam guide v1.1:** skill 2.1.7 *"Manage open table formats (e.g., Apache Iceberg)"* was added, and **Amazon S3 Tables** joined the in-scope service list. Most prep material written before 2026 covers Iceberg lightly and S3 Tables not at all.

## The idea

Picture a **warehouse full of boxes with no inventory ledger**. Anyone can drop new boxes on a shelf or pull old ones off. A buyer counting stock while a delivery is half-unloaded sees a wrong total. Two workers restocking the same shelf trample each other. To change one item in a box you must repack the whole box. And nobody can tell you what the shelves looked like last Tuesday. That's a data lake of **plain files on S3**: fine for append-and-read, terrible for updates, deletes, concurrent writers and audit.

An **open table format** (Apache Iceberg, Apache Hudi, Delta Lake) adds the missing **ledger**. The data files stay ordinary Parquet/ORC/Avro in S3, but a **metadata layer** records exactly which files make up the table at each moment. A writer prepares new files, then commits by writing a new ledger page and atomically swapping the "current page" pointer. Readers always see a complete page — never a half-unloaded truck. Older pages remain, so you can **time travel**. Every major AWS engine can read the same ledger, so Athena, Redshift, EMR, Glue and Firehose share one copy of the data.

**Amazon S3 Tables** is that same warehouse with a **full-time ledger clerk and cleaning crew included**: a new bucket type that stores Iceberg tables and runs the housekeeping (compaction, snapshot expiry, garbage removal) for you.

This guide lets you crack questions about ACID on S3, upserts and CDC into the lake, GDPR deletes, time travel and rollback, schema and partition evolution, compaction and snapshot expiry, which AWS engine supports what, Iceberg vs Hudi vs Delta, and S3 Tables vs self-managed Iceberg.

## What plain files can't do

| Need | Plain Parquet on S3 (Hive-style table) | With a table format |
|---|---|---|
| Atomic multi-file writes (ACID) | Readers can see partial writes | Commit is one atomic metadata swap |
| Update / delete one row | Rewrite whole files or partitions | Row-level `UPDATE`, `DELETE`, `MERGE INTO` |
| Concurrent writers | Last writer silently wins / corrupts | Optimistic concurrency: conflicting commit retries or fails |
| Consistent reads during writes | No | Snapshot isolation |
| See past versions | Only via S3 versioning, file by file | **Time travel** by timestamp or snapshot ID; rollback |
| Rename/drop/reorder columns | Breaks positional/name readers | Safe schema evolution |
| Change partitioning | Rewrite everything, rewrite queries | **Partition evolution**, hidden partitioning |
| Plan queries on millions of files | Slow S3 listing | File-level stats in manifests; no listing |

## How Apache Iceberg works

```
Glue Data Catalog  ──pointer──>  metadata.json  (schema, partition spec, snapshot list, properties)
                                      │
                                      ▼  current snapshot
                                 manifest list  (one per snapshot)
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                   manifest file              manifest file    (lists data/delete files +
                         │                         │            per-column min/max, counts)
                  ┌──────┴──────┐                  ▼
                  ▼             ▼              data files (Parquet/ORC/Avro)
            data files     delete files
```

- **Catalog**: holds the pointer to the current `metadata.json`. On AWS that's usually the **AWS Glue Data Catalog** (also reachable through its **Iceberg REST endpoint** for third-party engines), or S3 Tables' own catalog.
- **Metadata file**: schema (with **field IDs**), partition specs, properties, list of snapshots.
- **Snapshot**: the table's state after one commit; each has a **manifest list**.
- **Manifests**: list data files and delete files with per-column statistics, so engines prune files **without listing S3**.
- **Commit** = write new files → write new metadata → atomically swap the catalog pointer. If another writer committed first, the loser re-checks for conflicts and retries (**optimistic concurrency**). Athena relies on Glue's optimistic locking.

### Time travel and rollback

```sql
-- Athena (engine v3)
SELECT * FROM sales FOR TIMESTAMP AS OF (current_timestamp - interval '1' day);
SELECT * FROM sales FOR VERSION AS OF 949530903748831860;   -- snapshot ID
```

- Use cases: reproduce last month's report, audit, compare versions, debug a bad load.
- **Rollback** a bad commit by pointing the table back to an earlier snapshot (Spark procedures on EMR/Glue such as `rollback_to_snapshot`) — no data copying.
- Time travel only reaches snapshots that **have not been expired**.

**THE trap:** *"auditors need to query the table as of 30 days ago"* while snapshot expiry (Athena `VACUUM` default **5 days**, S3 Tables default **120 hours**) keeps less history → time travel fails. Raise the retention settings (or tag the snapshot in self-managed Iceberg) *before* you need it; a nightly backup isn't required.

### Schema evolution by field ID

Iceberg identifies columns by an internal **ID**, not by name or position, so **add, drop, rename, reorder and widen types** (e.g., int → bigint) are metadata-only and safe — old files are read correctly. Contrast: Athena reads plain Parquet by name (renames break) and ORC by index (inserts break) — see [Guide 03](03-Data-Formats-Compression.md). Broader schema-evolution strategy lives in [Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md).

### Hidden partitioning and partition evolution

- You partition by a **transform of a column**: `day(event_ts)`, `month(...)`, `year(...)`, `hour(...)`, `bucket(16, customer_id)`, `truncate(10, sku)`. Queries filter on `event_ts` itself; Iceberg maps the filter to partitions automatically. No extra `dt` string column, no user mistakes that scan everything.
- **Partition evolution**: change the spec (e.g., month → day as volume grows) without rewriting old data; old files keep the old layout, new files use the new one, queries span both.

```sql
CREATE TABLE lake.events (event_id bigint, customer_id bigint, event_ts timestamp, payload string)
PARTITIONED BY (day(event_ts), bucket(16, customer_id))
LOCATION 's3://amzn-s3-demo-bucket/curated/events/'
TBLPROPERTIES ('table_type' = 'ICEBERG');   -- Athena: CREATE TABLE, not CREATE EXTERNAL TABLE
```

### Row-level changes: copy-on-write vs merge-on-read

```sql
MERGE INTO lake.customers t
USING staging.customer_changes s ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET email = s.email, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, email, updated_at) VALUES (s.customer_id, s.email, s.updated_at);
```

| | Copy-on-write (CoW) | Merge-on-read (MoR) |
|---|---|---|
| On update/delete | Rewrite each affected data file | Write small **delete files** (position deletes or equality deletes; in Iceberg **v3**, compact **deletion vectors**) |
| Write cost | High (write amplification) | Low |
| Read cost | Lowest | Higher until compaction merges deletes |
| Fits | Read-heavy, infrequent batch updates | Frequent updates, CDC, streaming upserts |

- **Athena always uses merge-on-read with position deletes** for `UPDATE`, `DELETE` and `MERGE INTO` (it ignores copy-on-write table properties). Spark on EMR/Glue can do either.
- MoR makes **compaction mandatory** — delete files pile up and slow reads.

**THE trap:** assuming the writer cleans up after itself. **Firehose, Redshift and Athena DML do not compact or expire snapshots** on self-managed Iceberg tables — every streaming commit adds files and snapshots. Pair them with Glue Data Catalog table optimizers, scheduled `OPTIMIZE`/`VACUUM`, or use S3 Tables. Also avoid several Firehose streams writing the *same* table: optimistic concurrency makes them fight over commits and failed batches land in the S3 error prefix.

## Table maintenance (the chores that keep the ledger fast)

| Chore | Why | Athena | Spark (EMR/Glue) | Managed |
|---|---|---|---|---|
| **Compaction** | Merge small files and apply delete files | `OPTIMIZE t REWRITE DATA USING BIN_PACK [WHERE <partition predicate>]` | `rewrite_data_files` (binpack, sort, z-order) | Glue Data Catalog **compaction optimizer**; S3 Tables automatic |
| **Expire snapshots** | Shrink metadata; free files only old snapshots use | `VACUUM t` (uses `vacuum_max_snapshot_age_seconds`, default **432000 s = 5 days**; `vacuum_min_snapshots_to_keep`, default **1**) | `expire_snapshots` | Glue **snapshot retention** optimizer; S3 Tables snapshot management |
| **Remove orphan files** | Delete files no snapshot references (failed jobs, aborted writes) | `VACUUM t` (orphans older than the snapshot-age setting) | `remove_orphan_files` | Glue **orphan file deletion** optimizer; S3 Tables unreferenced file removal |
| **Rewrite manifests** | Faster planning | — | `rewrite_manifests` | — |

- Athena `OPTIMIZE` only accepts **partition columns** in its `WHERE`, and is billed by data scanned. `VACUUM` needs `s3:DeleteObject` — without it the query "succeeds" but deletes nothing.
- **Glue Data Catalog table optimizers** run compaction (binpack, sort, Z-order), snapshot retention and orphan file deletion automatically for Iceberg tables in general-purpose buckets, configurable per table or catalog-wide — the *"least operational overhead"* answer for self-managed Iceberg.

**THE trap (GDPR / right to erasure):** `DELETE FROM customers WHERE customer_id = 42` on an Iceberg table **does not physically remove the old bytes**. Older snapshots still reference the original data files (and with MoR the row is only masked by a delete file). To truly purge: **delete → compact (rewrite files without the row) → expire snapshots older than the delete → remove orphan files** (Athena: `DELETE`, `OPTIMIZE`, then `VACUUM` with a short snapshot age). Also mind S3 versioning on the bucket, which keeps noncurrent object versions until lifecycle expires them. Legal-deletion patterns: [Guide 31](31-Data-Lifecycle-Retention-Resiliency.md).

## Iceberg on AWS — who supports what

| Service | Iceberg support (as of 2026) |
|---|---|
| **Amazon Athena** (engine v3) | `CREATE TABLE`/CTAS (`table_type='ICEBERG'`), `INSERT`, `UPDATE`, `DELETE`, `MERGE INTO`, time/version travel, schema & partition evolution, `OPTIMIZE`, `VACUUM`; creates **v2** tables; Parquet (default), ORC, Avro; ZSTD default compression; Glue catalog only |
| **AWS Glue ETL** | `--datalake-formats iceberg` job parameter (Glue 4.0/5.x); Spark SQL DDL/DML, procedures, streaming writes |
| **Amazon EMR** | Spark, Trino, Flink, Hive with Iceberg (`iceberg-defaults` classification); EMR 7.12+ supports **Iceberg v3** deletion vectors and row lineage |
| **Amazon Redshift** | Reads Iceberg via Glue Data Catalog (auto-mounted `awsdatacatalog`, S3 Tables via `s3tablescatalog`); **writes**: `CREATE TABLE ... USING ICEBERG`, CTAS, `INSERT` (GA Nov 2025), **`UPDATE`/`DELETE`/`MERGE`** (Apr 2026), Iceberg **v3** tables (Aug 2026). Redshift doesn't compact — use Glue optimizers or S3 Tables |
| **Amazon Data Firehose** | Iceberg destination (self-managed in S3 or **S3 Tables**): route records from one stream to **multiple tables**, apply **insert/update/delete** per record; no compaction — pair with optimizers |
| **AWS Glue Data Catalog** | The Iceberg catalog for AWS engines; **Iceberg REST endpoint**; crawlers can register existing Iceberg tables; table optimizers |
| **AWS Lake Formation** | Table/column/row/cell permissions on Iceberg tables for Athena, EMR, Glue, Redshift (see [Guide 40](40-Lake-Formation.md)). Note: LF permissions govern reads/writes, not maintenance commands like `OPTIMIZE`/`VACUUM` |
| **Managed Service for Apache Flink** | Iceberg sink via the Flink connector (streaming upserts) |

> 🆕 **Iceberg v3 on AWS (Nov 2025 onward):** **deletion vectors** (compact per-file bitmaps replacing v2 position-delete files — cheaper frequent updates and GDPR deletes) and **row lineage** (row-level change tracking for incremental/CDC consumers) are supported in EMR 7.12+, Glue, SageMaker notebooks, S3 Tables and the Glue Data Catalog, and Redshift reads/writes v3. Athena documentation still describes creating v2 tables — check engine support before upgrading a shared table's `format-version` to 3.

## Apache Hudi

Hudi (Hadoop Upserts Deletes and Incrementals) came from **streaming upserts and CDC** at scale.

- **Record key** (like a primary key) + **precombine field** (e.g., `updated_at` — when two versions of a key arrive, the larger wins) → deduplicated **upserts**.
- Table types: **Copy-on-Write** (Parquet only; rewrite on update; read-optimized) and **Merge-on-Read** (Parquet base + Avro delta logs, compacted later; write-optimized).
- Query types: **snapshot**, **read-optimized** (MoR: only compacted base files — faster, slightly stale), and **incremental** (only records changed since a commit — a built-in change feed for downstream pipelines).
- AWS: write with Spark on **EMR** or **Glue** (`--datalake-formats hudi`); **Athena** and **Redshift Spectrum** read Hudi tables; DMS CDC → S3 → Hudi upsert is a classic pattern.

## Delta Lake

- A **transaction log** (`_delta_log/` of JSON commits plus Parquet checkpoints) next to Parquet data; ACID, time travel, `MERGE`, schema enforcement.
- AWS: Spark on **EMR** and **Glue** (`--datalake-formats delta`); **Athena** reads Delta tables natively; Redshift Spectrum reads Delta through generated manifest files.
- Typical exam context: a company already standardized on **Databricks**/Delta.

## Choosing a table format

| Signal | Pick |
|---|---|
| Multi-engine AWS lakehouse, Athena + Redshift + EMR + Firehose, S3 Tables, "open standard" | **Apache Iceberg** (AWS's strategic default) |
| Heavy record-level upserts from CDC/streams, need incremental pulls of changed records | **Apache Hudi** (Iceberg MERGE/MoR also works; Hudi is the heritage answer) |
| Existing Databricks / Delta ecosystem | **Delta Lake** |
| Fully managed tables, no maintenance jobs | **Iceberg in S3 Tables** |

> ⚠️ **2026 status:** **AWS Lake Formation Governed Tables** reached end of support on **December 31, 2024** (APIs stopped working after Feb 17, 2025); AWS directed customers to Iceberg, Hudi and Delta. If "governed tables" appears as an option for ACID on S3, treat it as a **distractor**.

**THE trap:** picking Hudi or Delta just because the question says *"upsert"*. On AWS, Iceberg `MERGE INTO` (Athena, Glue, EMR, Redshift) and Firehose's Iceberg upserts cover most upsert scenarios; choose Hudi only when record key + precombine semantics or **incremental queries** are the explicit signal, and Delta only for an existing Delta/Databricks estate.

## Amazon S3 Tables

> 🆕 **New in exam guide v1.1:** Amazon S3 Tables (launched Dec 2024) is newly in-scope.

**What it is:** a **table bucket** — a distinct S3 bucket type (alongside general-purpose, directory and vector buckets) whose subresources are **tables** stored in **Apache Iceberg** format, grouped into **namespaces** (think database schemas).

```
table bucket  (amzn-s3-demo-table-bucket)
 └── namespace (sales)            ─┐ appears in Glue Data Catalog as:
      └── table (orders, Iceberg)  │ s3tablescatalog / <table bucket> / <namespace> / <table>
```

**Why pick it:** AWS positions table buckets as delivering up to **3x faster query throughput** and up to **10x higher transactions per second** than self-managed Iceberg tables in general-purpose buckets, and — the exam's real signal — **maintenance is automatic**:

| Maintenance | Level | Default |
|---|---|---|
| **Compaction** (applies row-level deletes too) | Table | On; target file size **512 MB** (configurable **64–512 MB**); strategies **auto** (default), binpack, sort, z-order |
| **Snapshot management** | Table | On; keep at least **1** snapshot, max snapshot age **120 hours** (5 days) |
| **Unreferenced file removal** | Table bucket | On; unreferenced files marked noncurrent after **3 days**, deleted after **10 more days** |

- Iceberg table properties like `history.expire.max-snapshot-age-ms` are **not** used — configure maintenance through the S3 Tables API. Note that maintenance deletions are permanent.
- **Access paths:**
  - **Glue Data Catalog integration (recommended)**: one-time per-Region integration creates the federated **`s3tablescatalog`**; table buckets become sub-catalogs, namespaces become databases. Then **Athena, Redshift, EMR, Glue ETL, Amazon Quick, Firehose and SageMaker Unified Studio** query them. Access is governed with **IAM** permissions, or with **Lake Formation** grants for fine-grained (column/row) control (some Regions still require Lake Formation for the integration).
  - **S3 Tables Iceberg REST endpoint** — direct access from any Iceberg REST client (Spark, PyIceberg); or the Glue Iceberg REST endpoint once integrated.
  - **S3 Tables Catalog for Apache Iceberg** client library (older direct option).
- **Security:** its own IAM namespace (`s3tables:` actions), **table bucket policies and table policies** (resource-based), SCPs; **Block Public Access is always on and can't be disabled**. Encryption: **SSE-S3 by default**; **SSE-KMS with customer managed keys** (per bucket default or per table; AWS managed keys not supported) — grant the S3 Tables **maintenance service principal** use of the key or compaction fails.
- **Ingestion:** Firehose delivers streams straight into S3 Tables (route to multiple tables, upserts/deletes); Glue, EMR, Athena CTAS/INSERT, Redshift INSERT/MERGE all write to them.
- **Newer features (Dec 2025):** **Intelligent-Tiering** storage class for tables (Frequent → Infrequent after **30** days without access → Archive Instant Access after **90** days; opt-in per table or bucket default; compaction only processes Frequent-tier data) and **replication** — read-only replica tables across Regions and/or accounts, updated within minutes, preserving snapshot history.
- **Pricing components (high level):** storage per GB-month (slightly above S3 Standard), PUT/GET requests, an **object monitoring** fee per 1,000 objects, and **compaction** charges per object and per GB processed (sort/z-order cost more than binpack); replication adds its own request/update charges.
- **Quotas (default, adjustable):** **10 table buckets per Region per account**, **10,000 tables** and **10,000 namespaces** per table bucket.

### S3 Tables vs self-managed Iceberg in general-purpose buckets

| Choose **S3 Tables** when | Choose **self-managed Iceberg** when |
|---|---|
| *"Least operational overhead"*, no compaction/expiry jobs to build | You need full control of maintenance timing, custom procedures, branches/tags (user-defined tags/branches break S3 Tables snapshot management) |
| New tables, high write/commit rates (streaming, frequent small commits) | Data already lives in existing buckets with established tooling, lifecycle and replication rules |
| Table-level permissions without managing prefixes | You need general-purpose bucket features (e.g., existing access point designs, S3 event workflows on data files) |
| Managed Iceberg for Firehose or Redshift writes | Cost sensitivity to managed maintenance fees for very cold, rarely changing tables |

Self-managed Iceberg is still low-overhead if you turn on **Glue Data Catalog table optimizers** — that's the middle answer when data must stay in general-purpose buckets.

## Question patterns

> *"A company ingests CDC records from AWS DMS into S3 and must apply inserts, updates and deletes so that Athena always shows current rows, with ACID guarantees and minimal custom code."* → **Iceberg table in the Glue Data Catalog, applied with `MERGE INTO` (Athena or Glue Spark)** (plain Parquet can't update rows; rewriting partitions is heavy custom code)

> *"Under GDPR, a customer's records must be permanently erased from an Iceberg table, including historical versions."* → **`DELETE`, then compaction, then expire snapshots older than the delete and remove orphan files (Athena `OPTIMIZE` + `VACUUM`)** (a DELETE alone leaves old snapshots pointing at the original files)

> *"Analysts must reproduce a report exactly as the data looked at 09:00 yesterday, before a faulty load."* → **Iceberg time travel: `SELECT ... FOR TIMESTAMP AS OF ...`** (no backup restore needed; works while that snapshot is retained)

> *"A bad ETL job wrote corrupted rows into an Iceberg table an hour ago. Restore the previous state quickly without copying data."* → **Roll back to the previous snapshot (Spark `rollback_to_snapshot` on EMR/Glue)** (metadata pointer change, not a data restore)

> *"An Iceberg table partitioned by month has grown so large that daily queries scan too much. Change to daily partitions without rewriting historical data or changing queries."* → **Iceberg partition evolution to `day(event_ts)`** (hidden partitioning keeps queries unchanged; Hive tables would need a full rewrite)

> *"A Firehose stream writes to an Iceberg table every minute; after weeks, Athena queries are slow because of thousands of small files and delete files. Least operational overhead fix?"* → **Enable Glue Data Catalog automatic compaction (and snapshot retention/orphan deletion) on the table** (managed; a scheduled Athena OPTIMIZE works but is more to operate — or use S3 Tables, which compacts automatically)

> *"A team wants Apache Iceberg tables for a new streaming analytics workload with no maintenance jobs and table-level access control."* → **Amazon S3 Tables (table bucket) integrated with the Glue Data Catalog** (managed compaction, snapshot management and unreferenced file removal)

> *"Tables in an S3 table bucket must be queried from Athena and Redshift using the existing Glue Data Catalog."* → **Enable the S3 Tables integration with AWS analytics services (creates `s3tablescatalog`), then grant IAM or Lake Formation permissions** (engines discover table buckets as sub-catalogs)

> *"An Apache Spark application running outside AWS must read and write S3 Tables using the standard Iceberg REST catalog protocol."* → **S3 Tables Iceberg REST endpoint (or the Glue Iceberg REST endpoint)** (open protocol, no engine-specific client code)

> *"One Firehose stream carries events for 30 entity types that must land in 30 separate Iceberg tables with updates and deletes applied."* → **Firehose Iceberg destination with routing to multiple tables and record-level operations** (one stream fans into many tables; dynamic partitioning to plain S3 can't upsert)

> *"A SQL-first team wants to upsert daily changes from Redshift into an Iceberg table that Athena and EMR also read, without moving data to another engine."* → **Redshift `MERGE` into the Iceberg table (supported since April 2026)** (Redshift writes Iceberg natively; Glue/EMR hop is no longer required)

> *"A data lake needs record-level upserts with a precombine field to keep the latest version of each key, plus incremental queries that return only changed records since the last commit."* → **Apache Hudi on EMR/Glue** (record key + precombine + incremental query type are Hudi's signature)

> *"An architect proposes Lake Formation governed tables for ACID transactions on S3."* → **Use Apache Iceberg instead** (governed tables reached end of support Dec 31, 2024)

> *"A table receives thousands of small row-level updates per hour and reads are infrequent. Which Iceberg write mode minimizes write cost?"* → **Merge-on-read (delete files / deletion vectors) with regular compaction** (copy-on-write rewrites whole files per update)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| ACID, updates, deletes on S3 | Open table format — Iceberg |
| Table format = | Metadata layer tracking which files form each snapshot |
| Iceberg hierarchy | Catalog → metadata.json → manifest list → manifests → data/delete files |
| Iceberg catalog on AWS | Glue Data Catalog (+ Iceberg REST endpoint) |
| Concurrent writers | Optimistic concurrency, atomic pointer swap |
| Query past state | `FOR TIMESTAMP AS OF` / `FOR VERSION AS OF` |
| Undo bad write | Roll back to earlier snapshot |
| Safe rename/drop/reorder columns | Iceberg schema evolution (field IDs) |
| Partition by transform, filter on raw column | Hidden partitioning (`day(ts)`, `bucket(n,col)`) |
| Change partitioning without rewrite | Partition evolution |
| Upsert / CDC apply | `MERGE INTO` |
| Frequent updates, cheap writes | Merge-on-read (+ compaction) |
| Read-heavy, rare updates | Copy-on-write |
| Athena UPDATE/DELETE/MERGE mode | Always merge-on-read, position deletes |
| Iceberg v3 | Deletion vectors + row lineage (EMR 7.12+, Glue, S3 Tables, Redshift) |
| Small files in Iceberg | `OPTIMIZE ... REWRITE DATA USING BIN_PACK` / compaction optimizer |
| Expire snapshots + orphans in Athena | `VACUUM` (default 5-day snapshot age, keep 1) |
| Automatic maintenance, general-purpose bucket | Glue Data Catalog table optimizers |
| GDPR purge | DELETE → compact → expire snapshots → remove orphans |
| Redshift + Iceberg | Read, CREATE/CTAS/INSERT, UPDATE/DELETE/MERGE (2026) |
| Firehose + Iceberg | Multi-table routing, insert/update/delete, no compaction |
| Record key + precombine, incremental queries | Apache Hudi |
| Hudi MoR read-optimized query | Compacted base files only (faster, staler) |
| `_delta_log`, Databricks shop | Delta Lake |
| Lake Formation governed tables | Retired Dec 31, 2024 — distractor |
| Managed Iceberg, least overhead | Amazon S3 Tables |
| S3 Tables hierarchy | Table bucket → namespace → table |
| S3 Tables performance claim | Up to 3x query throughput, 10x TPS vs self-managed |
| S3 Tables compaction default | 512 MB target (64–512 MB), strategy auto |
| S3 Tables snapshot default | Min 1 snapshot, max age 120 h |
| S3 Tables unreferenced files | Noncurrent after 3 days, deleted 10 days later |
| S3 Tables in Athena/Redshift/EMR | Glue Data Catalog integration → `s3tablescatalog` |
| S3 Tables from external Spark/PyIceberg | Iceberg REST endpoint |
| S3 Tables encryption | SSE-S3 default; SSE-KMS customer managed keys only |
| S3 Tables cold data | Intelligent-Tiering (30 d → IA, 90 d → Archive Instant) |
| S3 Tables DR / cross-account copy | S3 Tables replication (read-only replicas) |
| S3 Tables default quota | 10 table buckets/Region; 10,000 tables per bucket |

Table formats sit on top of S3 itself — next, master the storage layer underneath: [Guide 05 — S3 Data Lake Storage](05-S3-Data-Lake-Storage.md).
