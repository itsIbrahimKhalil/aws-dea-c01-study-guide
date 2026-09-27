# 26 · Amazon Athena — SQL on the lake, billed by the byte

> **Exam map:** D1 · Task 1.2 — D2 · Task 2.2 — D3 · Task 3.1, 3.2, 3.3 — D4 · Task 4.4 · **Skills:** 3.1.7, 3.2.3, 3.2.4, 3.2.5, 2.2.1, 2.2.4, 3.3.8, 4.4.4, 1.2.6 · **Weight:** 🔥🔥🔥 High · **Read time:** ~22 min

## The idea

Picture a huge warehouse of sealed boxes (your S3 bucket) and a **reading-room clerk** you can hire by the page. You hand the clerk a question written in SQL; they walk the aisles, open only the boxes they need, read what's inside, and hand you an answer. You never rent the building, never hire staff, never keep lights on at night. The bill is simple: **you pay for how many pages the clerk had to read**, not for how long they took or how smart the question was.

That clerk is **Amazon Athena**: a **serverless**, interactive SQL service that queries data where it already lives, mostly in Amazon S3. It has no storage of its own. It reads table definitions (column names, types, S3 locations, partitions) from the **AWS Glue Data Catalog**, the building's card index ([Guide 13](13-Glue-Data-Catalog-Crawlers.md)). The SQL engine is **Athena engine version 3**, built on the open-source **Trino** engine (the successor to PrestoSQL). There's a second personality too, **Athena for Apache Spark**, for notebook-style PySpark exploration.

Every Athena exam question comes down to one thing: **make the clerk read fewer pages.** You do that by indexing the aisles (partitions), shrinking the pages (compression), printing each column in its own booklet so the clerk can skip columns (Parquet/ORC), and not handing over a million sticky notes when one binder would do (small files). Once that clicks, the pricing questions, performance questions, workgroup cost-control questions and troubleshooting questions all get easy. This guide also covers CTAS/UNLOAD, views, federated queries, Iceberg DML, Spark notebooks, capacity reservations and the error messages the exam likes to quote.

## Pricing: the "pages read" meter

| Mode | What you pay | Pick when |
|---|---|---|
| **Per query (default)** | **$5 per TB scanned**, rounded up to the nearest MB, **10 MB minimum per query** | Ad hoc, spiky, unpredictable use; *"pay only when queries run"* |
| **Capacity reservations** | **$0.30 per DPU-hour** for dedicated capacity; **no per-TB charge** for queries on it | Predictable heavy workloads, guaranteed concurrency, cost you can forecast |
| **Athena for Apache Spark** | **$0.35 per DPU-hour** of compute | Notebook or PySpark exploration |
| **Federated queries** | Per TB scanned across sources (same 10 MB minimum) **plus Lambda charges** for Lambda-based connectors | SQL over non-S3 sources |

Billing details that turn up in questions:
- **Failed queries aren't charged** on per-query pricing. **Canceled queries are charged** for the data scanned up to the point of cancellation.
- You pay for **bytes scanned as stored**, so compressed data is cheaper to query. That's why the FAQ says compression + partitioning + columnar formats can save **30%–90%** per query.
- S3 requests, S3 storage of results and Glue Data Catalog requests are billed separately at their own rates.

**Capacity reservations** (skill 3.2.5, provisioned vs serverless): you reserve **DPUs** (Data Processing Units, roughly **4 vCPU + 16 GB** each) and assign **workgroups** to the reservation, with up to **20 workgroups per reservation**. Since **Feb 2026** the minimum is **4 DPUs** and a reservation can last as little as **1 minute** (previously **24 DPUs** and 1 hour). Queries on a reservation **don't count toward the account's active-DML quota**. When the reservation is busy they queue, for up to 10 hours. Workgroups that run Spark can't use reservations. Newer controls (Nov 2025) let you cap DPUs per query or workgroup and auto-scale a reservation with a Step Functions solution.

**THE trap:** *"thousands of scheduled dashboard queries a day, predictable, cost must be forecastable, queries must not queue behind ad hoc users"* → **capacity reservation** for that workgroup. The per-TB model isn't wrong in general, but it can't guarantee concurrency. And for *"occasional ad hoc queries"*, keep per-query pricing, because a reservation bills whether or not queries run.

## Performance and cost levers (the heart of the exam)

### 1. Partitions: index the aisles

Hive-style partitioning lays data out as `s3://amzn-s3-demo-bucket/sales/year=2026/month=09/day=26/`. Each `key=value` folder becomes a **partition column**. A query that filters on it (`WHERE year='2026' AND month='09'`) reads **only those folders**. This is called **partition pruning**.

- Pick partition keys that match the **most common filters**. Date is the classic.
- **Don't over-partition.** Hourly partitions for day-level queries create tiny files and planning overhead. Keeping data sorted by timestamp inside day partitions is almost as good as hourly partitions.
- Athena wants partition keys to be **STRING** so filters can be pushed down into Glue. Cast them in the query if you need dates or numbers.
- For tables with huge partition counts, **Glue partition indexes** speed up partition lookups. Athena can query tables with up to **10 million partitions**, but it reads at most **1 million partitions in a single scan**.

**Registering partitions** (skill 2.2.4). A new S3 folder isn't queryable until the catalog knows about it:

| Method | Notes |
|---|---|
| `MSCK REPAIR TABLE t` | Scans for Hive-style folders and **only adds** partitions (never removes stale ones). Slow and can fail on huge tables. Needs lower-case `key=value` paths |
| `ALTER TABLE t ADD IF NOT EXISTS PARTITION (...) LOCATION '...'` | Precise and fast. Good right after a pipeline writes a partition |
| Glue crawler | Schedule- or event-driven discovery ([Guide 13](13-Glue-Data-Catalog-Crawlers.md)) |
| **Partition projection** | No registration at all (below) |

**THE trap:** *"new data landed in S3 but Athena returns zero rows (or yesterday's totals)"* → the **partitions aren't registered** (run `MSCK REPAIR TABLE` / `ADD PARTITION` / a crawler), or use **partition projection** so there's nothing to register. It isn't a permissions problem.

### 2. Partition projection: compute the partitions, don't look them up

With **partition projection**, Athena *calculates* partition values and S3 locations from rules in the table's properties. It never calls Glue `GetPartitions`. That makes queries on highly partitioned tables faster and removes partition maintenance completely.

```sql
CREATE EXTERNAL TABLE app_logs (
  request_id string, status int, latency_ms int, user_id string)
PARTITIONED BY (dt string, region string, tenant string)
STORED AS PARQUET
LOCATION 's3://amzn-s3-demo-bucket/app-logs/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.format'        = 'yyyy/MM/dd',
  'projection.dt.range'         = '2024/01/01,NOW',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'projection.region.type'      = 'enum',
  'projection.region.values'    = 'us-east-1,eu-west-1',
  'projection.tenant.type'      = 'injected',
  'storage.location.template'   =
    's3://amzn-s3-demo-bucket/app-logs/${region}/${tenant}/${dt}/'
);
```

- Types: **`enum`** (small fixed list), **`integer`** (range), **`date`** (range, supports `NOW±n UNIT`), **`injected`** (unbounded values such as tenant or device IDs, which **the query must supply**).
- **`storage.location.template`** maps non-Hive layouts like `.../us-east-1/tenant42/2026/09/26/`. It needs a placeholder for every partition column.
- **Injected** columns are string only. A query with **no filter on an injected column fails**, and an `IN` list can hold at most **1,000** values.
- Once projection is enabled, Athena **ignores partitions registered in the catalog**. `SHOW PARTITIONS` won't list projected partitions. Values outside the range return **zero rows, not an error**.

**THE trap:** projection is **Athena-only**. Redshift Spectrum, EMR and Athena for Spark reading the same table use the normal catalog partitions. If another engine needs the partitions too, you still have to register them.

### 3. Formats, compression, file sizes

- **Columnar (Parquet/ORC)**: Athena reads only the referenced columns and uses min/max statistics to skip row groups. This is the biggest single saving over CSV/JSON. Convert with CTAS, Glue ETL or Firehose format conversion ([Guide 03](03-Data-Formats-Compression.md)).
- **Compression**: fewer bytes stored means fewer bytes billed. Snappy/ZSTD/GZIP inside Parquet are fine. **Big GZIP'd CSV files can't be split**, so one worker reads each whole file.
- **Small files hurt.** Every file costs a list/open request and planning time. Thousands of KB-sized files are slow and can trigger **S3 `SlowDown`** (S3 supports about **5,500 GET/s per prefix**). Fix it by compacting (CTAS, Glue with file grouping, Iceberg `OPTIMIZE`), buffering longer in Firehose, or partitioning less. A common target is files of roughly **128 MB or more**; Parquet's default row group is 128 MB.
- **Bucketing** (`bucketed_by`, `bucket_count` in CTAS) hashes a **high-cardinality column** (for example `user_id`) into a fixed number of files. Lookups for a single value read one bucket. It helps little when queries filter on many values.

### 4. Write cheaper SQL

| Do | Why |
|---|---|
| **Select only needed columns** (never `SELECT *` on wide Parquet) | Columnar pruning reduces bytes scanned |
| Filter on **partition columns directly** | Enables pruning |
| **`ORDER BY` with `LIMIT`** for top-N | Athena runs a dedicated top-N operator instead of a full distributed sort |
| **`approx_distinct`, `approx_percentile`** | Much less memory than `COUNT(DISTINCT)` when a small, bounded error is acceptable |
| Put the **larger table on the left** of an equi-join | Right side is the in-memory build side |
| `UNION ALL` instead of `UNION`; `regexp_like` / anchored `LIKE 'abc%'` | Less work |
| **CTAS** to materialize shared joins/aggregations; **`UNLOAD`** for large result exports | Pre-compute once; results come back in parallel instead of as one CSV |
| **Query result reuse** | Serves identical repeat queries without rescanning |

**THE trap (the big one):** **`LIMIT` does not reduce bytes scanned.** `SELECT * FROM big_csv LIMIT 10` may stop early, but on a full scan of unpartitioned, row-based data you should assume the whole table is billed. Partitions, columnar formats and column selection are what cut cost.

**THE trap:** **wrapping a partition column in a function or cast** can defeat pruning. `WHERE date_format(from_iso8601_date(dt), '%Y') = '2026'` or `WHERE substr(dt,1,4)='2026'` may scan every partition. Write `WHERE dt BETWEEN '2026/01/01' AND '2026/12/31'` against the raw string instead. Use `EXPLAIN` to see which partition values will be read.

**Query result reuse**: enable it per query and set a **maximum age** (default **60 minutes**, maximum **7 days**). It only works **within the same workgroup** for SELECT/EXECUTE with matching query text and settings. It is **not** supported for federated catalogs, tables with Lake Formation fine-grained (row/column) permissions, or workgroups using managed query results. The catch: results can be **stale** until the maximum age passes.

## CTAS, INSERT INTO, UNLOAD

**CTAS** (`CREATE TABLE AS SELECT`) runs a query and writes the result as a **new table plus files**. It's the serverless, one-statement CSV-to-Parquet converter (skill 1.2.6).

```sql
CREATE TABLE curated.sales_parquet
WITH (
  format            = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://amzn-s3-demo-bucket/curated/sales/',
  partitioned_by    = ARRAY['sale_date']        -- must be LAST in the SELECT
) AS
SELECT order_id, customer_id, try_cast(amount AS decimal(12,2)) AS amount,
       sale_date
FROM raw.sales_csv
WHERE sale_date >= '2026-09-01';
```

- If you don't specify a format, CTAS writes **Parquet**. `external_location` must be **empty**. Partition columns go **last** in the SELECT list. `bucketed_by` + `bucket_count` add bucketing.
- `WITH NO DATA` creates only the schema.
- **CTAS and INSERT INTO can write at most 100 partitions per statement.** Past that you get `HIVE_TOO_MANY_OPEN_PARTITIONS`. The fix is one CTAS followed by a series of **`INSERT INTO ... SELECT`** statements, each covering ≤100 partitions with non-overlapping `WHERE` ranges. For big backfills, Glue/EMR Spark is the other answer.
- **INSERT INTO** appends query results to an existing Hive or Iceberg table. It's how you add daily increments.
- A failed CTAS/INSERT **can leave orphaned files** behind. Athena never deletes them.

**`UNLOAD`** writes a SELECT's results to S3 as **Parquet, ORC, Avro, JSON or text**, **without creating a table**. Pick it for *"export results in Parquet for another team/tool"*. It's also faster than a normal SELECT for large result sets because workers write in parallel. It supports `partitioned_by` (max **100** partitions) and requires an empty target unless you're writing partitions.

**Managed query results** (June 2025): a workgroup can let Athena **store results itself**, with no results bucket, **no extra cost**, auto-deleted after **24 hours**, and encrypted with an AWS owned key or your KMS key. It isn't compatible with query result reuse.

## Views, nested data, dirty data

- **`CREATE VIEW`** stores a saved SELECT in the catalog. It holds no data, and every query re-runs it (and re-scans). Use views to hide complexity or expose a column subset.
- **Glue Data Catalog views** (multi-dialect views), the governed sharing option:
  ```sql
  CREATE PROTECTED MULTI DIALECT VIEW sales_eu SECURITY DEFINER AS
  SELECT order_id, amount FROM sales WHERE region = 'EU';
  ```
  They're defined once in the catalog, can be queried from more than one engine (Athena and Redshift, for example), and **Lake Formation grants SELECT on the view** so users never get access to the base tables. The definer must be an IAM role with grantable SELECT on the tables. Details are in [Guide 40](40-Lake-Formation.md).
- **Nested JSON / arrays**: flatten with `CROSS JOIN UNNEST(items) AS t(item)`. Pull fields with dot notation on structs or `json_extract_scalar(payload, '$.customer.id')` on JSON strings.
- **Dirty data** (skill 3.2.2): `try_cast(x AS integer)` returns NULL instead of failing. `coalesce`, `NULLIF` and `regexp_like` help too. A CTAS can write a cleaned copy.

```sql
SELECT o.order_id, item.sku, try_cast(item.qty AS integer) AS qty
FROM raw.orders o
CROSS JOIN UNNEST(o.items) AS t(item)
WHERE json_extract_scalar(o.meta, '$.channel') = 'web';
```

## Workgroups: separate meters for separate teams

A **workgroup** is a named bucket of settings, query history and cost tracking. Create one per team, app or environment (an account can have up to **1,000**).

| Workgroup feature | Exam use |
|---|---|
| **Override client-side settings** | **Enforces** the workgroup's result location, encryption and expected bucket owner for every query, whatever the user's client sends |
| **Per-query data usage control** | Any query scanning more than the limit (**10 MB** to 7 EB) is **canceled** automatically |
| **Workgroup-wide data usage alerts** | Aggregate bytes per period → **CloudWatch alarm → SNS**. These **don't cancel** queries; hook a Lambda to disable the workgroup if you need a hard stop |
| Engine version, Spark vs SQL, capacity reservation assignment | Per workgroup |
| **Tags** + CloudWatch per-workgroup metrics | Cost allocation / chargeback ([Guide 44](44-Cost-Optimization.md)) |
| IAM on `arn:aws:athena:...:workgroup/name` | Control who can run queries where |

**THE trap:** *"stop any single query from scanning more than 1 TB"* → **per-query data usage control** (cancels). *"Notify the team lead when the marketing team's daily scans exceed 10 TB"* → **workgroup-wide data usage alert** (SNS). Mix them up and you pick an answer that can't do the job. AWS Budgets can't cancel a query either.

## Federated queries: one SQL, many sources

**Athena Federated Query** runs SQL across **non-S3 sources** through **data source connectors**. You can **join S3 data with DynamoDB, RDS/Aurora (MySQL/PostgreSQL), Redshift, DocumentDB, OpenSearch, CloudWatch Logs/Metrics, Timestream, JDBC sources, Snowflake, BigQuery…** in a single statement, **without first moving the data**.

| Connector type | How it works |
|---|---|
| **Glue Data Catalog federated connector** (recommended) | Built on an **AWS Glue connection**. Registered as a **federated catalog** in the Glue Data Catalog, so **Lake Formation fine-grained access** can apply |
| ↳ **Managed connectors** (from **Apr 21, 2026**) | For **12 sources** (DynamoDB, MySQL, PostgreSQL, Oracle, SQL Server, Redshift, DocumentDB, OpenSearch, SAP HANA, Snowflake, Teradata, BigQuery), Athena runs the connector for you, with **no Lambda in your account** |
| **Athena data catalog connector** (classic) | A **Lambda function** in your account (deployed from the Serverless Application Repository or built with the Query Federation SDK). Lambda-based connectors use an **S3 spill bucket** for data that doesn't fit in Lambda memory |

- Connectors push down filters and projections where the source can handle them.
- **Read-only**: `INSERT INTO` a federated source isn't supported. Views over federated data are stored in Glue.
- Lambda-based connectors that reach private databases need **VPC config** and a route to Secrets Manager (a VPC endpoint).
- **Athena UDFs** use `USING EXTERNAL FUNCTION f(x varchar) RETURNS varchar LAMBDA 'my-fn' SELECT f(col) ...` to call custom Lambda logic (tokenize, decrypt, geocode) from SQL.

**THE trap:** *"analysts need a one-off report joining S3 clickstream with current DynamoDB customer profiles with least effort"* → **Athena federated query**. A nightly Glue job copying DynamoDB to S3 works, but it's more overhead and the data is stale. If the same join runs **constantly at scale**, the better answer shifts to **zero-ETL / export into the lake or Redshift** ([Guide 27](27-DynamoDB.md), [Guide 10](10-DMS-Database-Ingestion.md)), because federated queries hit the operational database every time.

## Athena for Apache Spark (skill 3.2.4)

Create a **Spark-enabled workgroup** and you get serverless PySpark **sessions**. Each unit of code you run is a **calculation**, billed per **DPU-hour ($0.35)**. It starts in seconds, with no cluster to size.

| Release version | Where you use it |
|---|---|
| **PySpark engine version 3** (Spark 3.2.1) | The **Athena console notebook editor** (Jupyter-compatible notebooks, cells run as calculations) |
| **Apache Spark version 3.5** (Spark 3.5.6, Nov 2025) | **SageMaker Unified Studio notebooks** or any **Spark Connect** client. Adds live Spark UI, per-session cost attribution, Lake Formation table-level access |

Signal words: *"interactive exploration with Python/Spark, notebook, no clusters to manage, pay per use"* → Athena for Apache Spark. Scheduled heavy ETL still belongs to Glue/EMR ([Guide 16](16-Apache-Spark-Essentials.md), [Guide 35](35-Analytics-Visualization-Quick-Notebooks.md)).

## Iceberg on Athena (open table formats, skill 2.1.7)

> 🆕 **New in exam guide v1.1:** managing open table formats (Apache Iceberg) is a new skill. Athena is the easiest serverless way to run Iceberg DML. The deep theory lives in [Guide 04](04-Open-Table-Formats-S3-Tables.md).

```sql
CREATE TABLE lake.orders (order_id bigint, status string, amount decimal(12,2), ts timestamp)
PARTITIONED BY (day(ts), bucket(16, order_id))        -- hidden partitioning
LOCATION 's3://amzn-s3-demo-bucket/lake/orders/'
TBLPROPERTIES ('table_type' = 'ICEBERG');              -- no EXTERNAL keyword

MERGE INTO lake.orders t
USING staging.order_updates s
  ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET status = s.status, amount = s.amount
WHEN NOT MATCHED THEN INSERT (order_id, status, amount, ts)
  VALUES (s.order_id, s.status, s.amount, s.ts);
```

- Row-level **`UPDATE`, `DELETE`, `MERGE INTO`** are **Iceberg-only**. On plain Hive tables you rewrite partitions instead. This is the answer for GDPR deletes and CDC upserts in the lake.
- **Time travel**: `SELECT ... FROM t FOR TIMESTAMP AS OF (current_timestamp - interval '1' day)` or `FOR VERSION AS OF <snapshot_id>`.
- **Compaction**: `OPTIMIZE t REWRITE DATA USING BIN_PACK [WHERE ...]` merges small files and applies accumulated delete files.
- **Cleanup**: `VACUUM t` **expires old snapshots and removes orphan files**, keeping snapshots newer than `vacuum_max_snapshot_age_seconds` (default **432,000 s = 5 days**). After a VACUUM you can't time-travel past the retention window.
- **Schema evolution** is metadata-only: `ALTER TABLE t ADD COLUMNS (channel string)`, plus rename, drop and reorder columns. Partition evolution is supported too.
- An Iceberg CTAS uses `WITH (table_type='ICEBERG', is_external=false, location='s3://...', partitioning=ARRAY['day(ts)'])`. Athena also queries **S3 Tables** (managed Iceberg table buckets).

## Querying AWS logs (skills 3.3.8, 4.4.4)

Athena is the cheap, serverless answer to *"analyze logs already sitting in S3 with SQL."* AWS publishes ready-made DDL for **CloudTrail, ALB/NLB/CLB, CloudFront, VPC Flow Logs, S3 server access logs, WAF, Route 53 Resolver** and more. Most of them use **partition projection** on date (and Region/account), so new days become queryable automatically.

```sql
SELECT useridentity.arn, eventname, count(*) AS calls
FROM cloudtrail_logs
WHERE timestamp >= '2026/09/01' AND eventsource = 's3.amazonaws.com'
  AND eventname IN ('DeleteObject','DeleteBucket')
GROUP BY 1, 2 ORDER BY calls DESC LIMIT 20;
```

Choosing between log tools is covered below and in [Guide 43](43-Audit-Logging-CloudTrail-Config.md).

## Security

- **Three permission layers:** IAM (Athena actions and the workgroup) + **S3** (read the data, write the results location) + **Glue Data Catalog** permissions. With **Lake Formation** you get **database/table/column/row/cell** grants and LF-Tags; Athena enforces them through credential vending ([Guide 40](40-Lake-Formation.md)).
- **Encryption:** data and query results support **SSE-S3, SSE-KMS (recommended), CSE-KMS**. **SSE-C and asymmetric KMS keys are not supported.** Reading KMS-encrypted data needs `kms:Decrypt`. Writing encrypted results needs `kms:GenerateDataKey` too. Enforce result encryption with the workgroup's **override client-side settings**. Many small SSE-KMS objects can hit KMS throttling; **S3 Bucket Keys** reduce KMS calls.
- **Network:** an **interface VPC endpoint (PrivateLink)** for Athena keeps API calls off the internet. S3 access goes through a gateway endpoint ([Guide 38](38-Networking-for-Data-Pipelines.md)).
- Glacier: objects in **S3 Glacier Flexible Retrieval / Deep Archive** are **skipped** unless restored *and* the table sets `read_restored_glacier_objects`. Glacier Instant Retrieval is queryable directly.

> ⚠️ **2026 status:** **S3 Select** is closed to new customers (since July 2024). *"Query a subset of an S3 object with SQL"* → **Athena** is the modern answer. Older questions may still offer S3 Select.

## Common errors (troubleshooting table)

| Symptom / message | Usual cause | Fix |
|---|---|---|
| **Zero rows**, no error | Partitions not registered; wrong `LOCATION`; projection range excludes the value; files start with `_` or `.` (treated as hidden); Glacier objects skipped | `MSCK REPAIR` / `ADD PARTITION` / crawler / projection; fix location; rename files |
| **`HIVE_PARTITION_SCHEMA_MISMATCH`** | A partition's schema in the catalog differs from the table's (the crawler saw different types in one folder) | Fix the data, drop and re-add the partition, align the schema; crawler option "update all partitions from table" |
| **`HIVE_BAD_DATA: Error parsing field value`** | A value doesn't fit the declared type (text in an int column, empty CSV fields) | Declare the column as `string` and `try_cast` it, or clean upstream |
| **`HIVE_CURSOR_ERROR`** / row not valid JSON | Pretty-printed or multi-line JSON; corrupt file; file deleted mid-query | One JSON object per line; `ignore.malformed.json`; don't overwrite files during queries |
| **"Query exhausted resources at this scale factor"** | Huge `ORDER BY` without `LIMIT`, big window function, oversized build side, `COUNT(DISTINCT)` on high cardinality, skew | Add `LIMIT`/`PARTITION BY`, smaller table on the right, `approx_distinct`, pre-aggregate with CTAS, or move to Spark |
| **`HIVE_TOO_MANY_OPEN_PARTITIONS`** | CTAS/INSERT writing >100 partitions | CTAS + batched `INSERT INTO` |
| **S3 `SlowDown: Please reduce your request rate`** | Thousands of small files / hot prefix / concurrent queries on the same files | Compact files, spread prefixes, stagger queries |
| **Query timeout** | DML exceeds the **30-minute default** (raise it via Service Quotas up to **240 min**); Glue/LF API throttling | Optimize, partition, request a quota increase, or move heavy ETL to Glue/EMR |
| **`TooManyRequestsException`** | Over the active DML query quota (for example 200 in us-east-1) | Retry with backoff, queue, or use a capacity reservation |
| **Access Denied (S3/KMS)** | Missing `s3:GetObject`/`ListBucket` on data, `PutObject` on results, or `kms:Decrypt` in the key policy | Fix the IAM, bucket and key policy |
| **Insufficient Lake Formation permissions** | Location registered with LF but no LF grant (or IAMAllowedPrincipals removed) | Grant SELECT/DESCRIBE in Lake Formation |

## Athena vs its neighbours

| Need | Best fit | Why |
|---|---|---|
| Ad hoc SQL on S3, no infrastructure, pay per query | **Athena** | Serverless, per-TB pricing |
| Hot BI dashboards, complex joins, many concurrent users, sub-second repeat queries | **Amazon Redshift** (provisioned or Serverless) | Local columnar storage, result cache, WLM ([Guide 23](23-Redshift-Architecture-Table-Design.md)) |
| Join S3 lake data with tables **already in Redshift** | **Redshift Spectrum** | Runs inside the warehouse's SQL ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)) |
| Long-running Trino/Presto with custom plugins/config, or the team already runs EMR | **EMR with Trino** | Full control of the cluster ([Guide 15](15-Amazon-EMR.md)) |
| Interactive PySpark notebooks, serverless | **Athena for Apache Spark** | Spark without clusters |
| Scheduled, heavy transformation jobs | **Glue ETL / EMR** | Built for ETL, bookmarks, retries ([Guide 12](12-AWS-Glue-ETL.md)) |

## Question patterns

> *"A company stores 4 TB/day of CSV clickstream in S3. Analysts run Athena queries filtering on event date and selecting 5 of 60 columns. Costs are high. What reduces cost the MOST?"* → **Convert to compressed Parquet partitioned by date** (via CTAS or Glue) (the date filter prunes partitions and columnar storage skips 55 columns; adding `LIMIT` or switching to Redshift doesn't fix the bytes scanned).

> *"An IoT table is partitioned by year/month/day/hour/device_id with millions of partitions. Queries spend most of their time in planning, and MSCK REPAIR takes hours. LEAST operational overhead?"* → **Partition projection** (`date` type for time, `injected` for device_id) (removes both catalog lookups and partition maintenance; a crawler schedule still leaves millions of catalog partitions).

> *"New hourly folders written by a Firehose stream aren't showing up in Athena results until someone runs a manual command."* → **Partition projection with a `NOW`-based date range and `storage.location.template`** (Firehose's `yyyy/MM/dd/HH` layout isn't Hive-style, so MSCK REPAIR can't find it anyway).

> *"The data science team must never run a query that scans more than 500 GB, and finance wants an email when a workgroup exceeds 20 TB in a day."* → **Per-query data usage control of 500 GB (cancels) + workgroup-wide data usage alert with SNS** (per-query cancels; workgroup-wide alerts only notify).

> *"Several teams share one account. Each team must be billed for its own Athena usage, and query results must always be encrypted with SSE-KMS in a specific bucket regardless of client settings."* → **One workgroup per team with tags, and 'Override client-side settings' enabled with SSE-KMS results** (workgroups are the unit of isolation and chargeback).

> *"A CTAS converting three years of daily-partitioned data to Parquet fails with HIVE_TOO_MANY_OPEN_PARTITIONS."* → **Split into a CTAS plus several INSERT INTO statements of ≤100 partitions each** (or run the backfill in Glue Spark). Raising a quota isn't an option.

> *"Downstream ML engineers need the results of a daily Athena aggregation delivered to S3 as Snappy Parquet files. No new catalog table is wanted."* → **UNLOAD ... WITH (format='PARQUET', compression='SNAPPY')** (CTAS would register a table; a plain SELECT only produces CSV).

> *"A dashboard fires the same Athena query every few minutes all day. The data updates nightly. Reduce cost with minimal change."* → **Enable query result reuse with a max age of several hours** (served from the previous result in the same workgroup; stale reads are acceptable because data changes nightly).

> *"Analysts need to join customer data in Aurora PostgreSQL and order history in DynamoDB with S3 sales data for a monthly report, without building pipelines."* → **Athena federated query** (Glue-connection or managed connectors) (one SQL across sources; DMS/Glue pipelines are more overhead for a monthly report).

> *"A data lake table must support GDPR 'right to be forgotten' deletes and daily CDC upserts, queried with serverless SQL."* → **Apache Iceberg table in Athena using MERGE INTO / DELETE** (Hive tables can't do row-level changes; follow with `OPTIMIZE` and `VACUUM`).

> *"After months of small MERGE operations, Iceberg queries in Athena are slow and the table has thousands of small files and delete files."* → **`OPTIMIZE ... REWRITE DATA USING BIN_PACK`, then `VACUUM`** (compaction merges files and applies deletes; VACUUM expires snapshots and orphan files). Glue Data Catalog table optimizers can automate this.

> *"A query using `WHERE substr(dt, 1, 7) = '2026-09'` on a table partitioned by dt scans the whole table."* → **Filter the partition column directly, e.g. `dt BETWEEN '2026-09-01' AND '2026-09-30'`** (a function on the partition column prevents pruning).

> *"A query fails with 'Query exhausted resources at this scale factor'. It uses ROW_NUMBER() over the entire 2-billion-row table to find the 10 most recent events."* → **Rewrite as ORDER BY ts DESC LIMIT 10** (top-N operator; a window over the whole table holds everything in memory).

> *"Security auditors want to find who deleted objects from a bucket last month using existing CloudTrail logs in S3, cheaply and without loading data anywhere."* → **Athena table over the CloudTrail logs (AWS-provided DDL with partition projection)** (OpenSearch or Redshift would need ingestion first).

> *"A data scientist wants to explore S3 data interactively with PySpark in a notebook, without provisioning clusters, and pay only while code runs."* → **Athena for Apache Spark** (Spark-enabled workgroup / SageMaker Unified Studio notebook); EMR on EC2 means managing a cluster.

> *"A BI team runs predictable, high-concurrency Athena workloads during business hours and complains that queries queue behind ad hoc users. They want predictable cost."* → **Athena capacity reservation assigned to the BI workgroup** (dedicated DPUs, no per-TB billing, isolated from other workgroups).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Serverless SQL on S3, pay per query | Athena ($5/TB scanned, 10 MB minimum) |
| Failed vs canceled query billing | Failed: not charged · Canceled: charged up to the cancel point |
| Cut cost/latency #1 | Partition + Parquet/ORC + compression + select fewer columns |
| `LIMIT` to save money | Doesn't reduce scanned bytes on a full scan |
| Function on partition column | Breaks pruning; filter the raw column |
| New partitions invisible / zero rows | MSCK REPAIR / ADD PARTITION / crawler, or partition projection |
| Millions of partitions, slow planning | Partition projection (enum/integer/date/injected) |
| Non-Hive S3 layout | `storage.location.template` |
| Unbounded IDs as partitions | `injected` type (query must filter on it) |
| Projection honored by Spectrum/EMR? | No, Athena only |
| Many tiny files, SlowDown | Compact (CTAS / Glue / Iceberg OPTIMIZE), bigger files |
| High-cardinality point lookups | Bucketing (`bucketed_by`) |
| CSV → Parquet serverless, one statement | CTAS (default format Parquet) |
| Export results as Parquet, no table | UNLOAD |
| >100 partitions in CTAS | CTAS + batched INSERT INTO |
| Top-N | ORDER BY + LIMIT |
| Approximate distinct count | approx_distinct |
| Same query repeated, data rarely changes | Query result reuse (default 60 min, max 7 days, same workgroup) |
| No results bucket to manage | Managed query results (24 h, free) |
| Cancel queries over X bytes | Workgroup per-query data usage control |
| Alert on team's daily scan volume | Workgroup-wide data usage alert → CloudWatch/SNS |
| Enforce result location/encryption | Workgroup "override client-side settings" |
| Chargeback per team | Workgroups + tags |
| Predictable concurrency/cost | Capacity reservation (DPUs, min 4, $0.30/DPU-hr) |
| DML timeout | 30 min default, up to 240 via quota |
| SQL across DynamoDB/RDS/Redshift + S3 | Federated query (Glue connection / managed connectors; Lambda for classic) |
| Lambda connector overflow | Spill bucket in S3 |
| Custom logic in SQL | Lambda UDF (`USING EXTERNAL FUNCTION`) |
| PySpark notebooks, serverless | Athena for Apache Spark ($0.35/DPU-hr) |
| Row-level UPDATE/DELETE/MERGE | Iceberg table (`'table_type'='ICEBERG'`) |
| Query as of yesterday | `FOR TIMESTAMP AS OF` / `FOR VERSION AS OF` |
| Compact Iceberg / clean snapshots | `OPTIMIZE ... BIN_PACK` / `VACUUM` |
| Share a filtered view, hide base tables | Glue Data Catalog (multi-dialect) view + Lake Formation |
| Dirty values break casts | `try_cast` |
| Flatten arrays / JSON | `CROSS JOIN UNNEST`, `json_extract_scalar` |
| Query CloudTrail/ALB/VPC Flow logs in S3 | Athena + AWS DDL + partition projection |
| Encryption options | SSE-S3, SSE-KMS, CSE-KMS (no SSE-C) |
| Private API access | Interface VPC endpoint (PrivateLink) |
| Replaces S3 Select | Athena |

Athena is the clerk you hire to read the whole archive; when an application instead needs one record back in milliseconds, millions of times a second, you want a different building entirely: [Guide 27 — DynamoDB](27-DynamoDB.md).
