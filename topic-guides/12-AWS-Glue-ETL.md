# 12 · AWS Glue ETL — a rented industrial kitchen for your data

> **Exam map:** D1 · Task 1.1, 1.2 — D3 · Task 3.1, 3.3 · **Skills:** 1.1.2, 1.1.3, 1.2.2, 1.2.3, 1.2.4, 1.2.5, 1.2.6, 1.2.7, 3.1.4, 3.3.6 · **Weight:** 🔥🔥🔥 High · **Read time:** ~25 min

## The idea

Say you want to run a restaurant but don't want to own a building. You rent an **industrial kitchen by the minute**. You bring the recipe (your script), say how many **cooking stations** you need (workers), and the landlord supplies the stoves, staff and cleanup. When the last plate goes out, the meter stops. That's **AWS Glue ETL**: serverless **Apache Spark** (or plain Python) that runs your extract-transform-load (ETL) code with no cluster to create, patch or tear down.

The kitchen comparison carries through the whole guide. A **DPU** (Data Processing Unit) is the standard unit of stove capacity. A **job bookmark** is the ticket spike by the pass that records which orders were already served, so tomorrow's shift doesn't cook them again. **Pushdown predicates** mean you only bring the shelves you need out of the pantry instead of emptying the whole storeroom. **Flex** is renting the kitchen off-peak at a discount, on the understanding that you might wait for a free slot. A **Glue connection** is the service door into your private pantry (your VPC).

Glue is one of the two services DEA-C01 tests most (Redshift is the other). Questions rarely ask "what is Glue". They ask *which job type*, *which setting*, and *why the job is slow, failing, or reprocessing data*. After this guide you should be able to answer: bookmark says "enabled" but data is reprocessed; Glue can't reach RDS; a job of 400,000 small JSON files runs out of driver memory; *"most cost-effective"* for a nightly non-urgent job; how to get new partitions into the catalog *"without running a crawler"*.

## The Glue family — what lives where

Glue is a set of cooperating tools that share the **Glue Data Catalog**:

| Component | What it does | Deep dive |
|---|---|---|
| **Data Catalog** | Hive-compatible metadata store: databases, tables, partitions | [Guide 13](13-Glue-Data-Catalog-Crawlers.md) |
| **Crawlers & classifiers** | Infer schemas and partitions, write tables into the Catalog | [Guide 13](13-Glue-Data-Catalog-Crawlers.md) |
| **ETL jobs** | Spark batch, Spark streaming, Python shell, Ray | **This guide** |
| **Glue Studio** | Visual ETL canvas, script editor, notebooks, job monitoring dashboard | This guide |
| **Interactive sessions** | On-demand serverless Spark behind Jupyter or your IDE | This guide |
| **DataBrew** | No-code data prep with 250+ transforms and profiling | [Guide 14](14-Glue-DataBrew-Data-Preparation.md) |
| **Data Quality** | DQDL rules, recommendations, anomaly detection | [Guide 33](33-Data-Quality.md) |
| **Workflows & triggers** | Native orchestration of crawlers and jobs | [Guide 21](21-MWAA-Glue-Workflows.md) |
| **Schema Registry** | Avro/JSON Schema/Protobuf schemas for streams | [Guide 13](13-Glue-Data-Catalog-Crawlers.md) |
| **Zero-ETL & SaaS connectors** | Managed replication from SaaS apps / DynamoDB into Redshift or the lakehouse | This guide + [Guide 10](10-DMS-Database-Ingestion.md) |

## Job types — pick the right stove

| Job type (command) | Engine | Pick when | Watch out |
|---|---|---|---|
| **Spark** (`glueetl`) | Distributed Spark (PySpark/Scala) | Large batch transforms, joins, format conversion (CSV → Parquet), big JDBC pulls | Startup overhead. Overkill for tiny files |
| **Spark Streaming** (`gluestreaming`) | Spark Structured Streaming, micro-batches | Continuous ETL from **Kinesis Data Streams, Amazon MSK, self-managed Kafka** into S3/JDBC/Catalog | Runs (and bills) the whole time it is up |
| **Python shell** (`pythonshell`) | One Python process, no Spark | Small jobs: call an API, run SQL against Redshift, move a few MB with pandas | **0.0625 DPU (default) or 1 DPU**, no bookmarks |
| **Ray** (`glueray`) | Distributed Python (Ray) | Scaling pandas-style Python | ⚠️ Maintenance mode (see below) |
| **Visual ETL** (Glue Studio) | Generates a Spark script from a canvas | Low-code pipelines built by less code-heavy teams | Still a Spark job underneath |
| **Notebooks / interactive sessions** | Spark on demand | Developing and debugging before scheduling | Close idle sessions or pay for them |

**Streaming details that get tested:**
- Data is processed in windows, **100 seconds by default** (`windowSize`). Make it shorter for fresher output, longer for bigger and cheaper batches.
- Progress is tracked with **Spark checkpoints** in an S3 `checkpointLocation`, **not job bookmarks**. A restarted job continues where it left off. To reprocess, delete the checkpoint folder.
- **G.025X** (quarter-DPU) workers exist **only for low-volume streaming jobs**.
- Kafka/MSK sources need a Glue **Kafka connection** (TLS bootstrap brokers; IAM, SASL/SCRAM, mTLS or Kerberos auth). Kinesis doesn't need a connection. If you attach a VPC connection anyway, you need a Kinesis **interface VPC endpoint**.
- Streaming jobs can use the Glue **Schema Registry** (Avro) and schema auto-detection. They can't join streams while schema detection is on.
- Glue 6.0 adds a **real-time mode** for stateless streaming with millisecond-level latency. For sub-second *stateful* processing the usual exam answer is still Managed Service for Apache Flink ([Guide 09](09-Managed-Service-for-Apache-Flink.md)).

**Python shell details:** Python **3.9** (3.6 reached end of life on June 1, 2026). With the *analytics* library set you get pandas, NumPy, boto3, **AWS SDK for pandas (awswrangler)**, PyAthena, redshift-connector and psycopg2 already installed, plus about 14 GiB of `/tmp`. Signal: *"lightweight"*, *"small dataset"*, *"run a SQL script against Redshift"*, *"lowest cost"* for a task that doesn't need distribution.

> ⚠️ **2026 status:** AWS Glue **Ray jobs** went into maintenance mode (**no new customers**) on **April 30, 2026**. The exam may still show Ray as an option. For new designs, treat it as a distractor and prefer Spark or Python shell (or EMR / SageMaker AI for Ray workloads).

**THE trap:** *"A small daily job downloads a 20 MB file from a REST API and loads it into S3 — cheapest option?"* Spark with 10 workers is wrong. Use a **Python shell job at 0.0625 DPU**, or Lambda if it finishes well inside 15 minutes ([Guide 17](17-Lambda-for-Data-Pipelines.md)).

## Capacity: DPUs, workers, versions, billing

A **DPU = 4 vCPU + 16 GB of memory.** From Glue 2.0 onward you don't set "max capacity" for Spark. You choose a **worker type** and a **number of workers**:

| Worker | DPU | vCPU / memory | Use |
|---|---|---|---|
| **G.025X** | 0.25 | 2 / 4 GB | Low-volume **streaming only** (Glue 3.0+) |
| **G.1X** | 1 | 4 / 16 GB | Default for most transforms, joins, queries |
| **G.2X** | 2 | 8 / 32 GB | Heavier transforms, memory-hungry joins |
| **G.4X** | 4 | 16 / 64 GB | Most demanding aggregations and joins (Glue 3.0+) |
| **G.8X** | 8 | 32 / 128 GB | Same, bigger (Glue 3.0+) |
| **G.12X / G.16X** | 12 / 16 | 48 / 192 GB · 64 / 256 GB | Very large, resource-intensive jobs (Glue 4.0+, selected Regions) |
| **R.1X / R.2X / R.4X / R.8X** | 1 / 2 / 4 / 8 | **Memory-optimized, 1 vCPU : 8 GB** (R.1X = 4 vCPU / 32 GB) | Jobs with recurring **out-of-memory** errors, heavy caching/shuffles (Glue 4.0+, selected Regions) |

G.12X, G.16X and all R types start more slowly and **don't support Flex**.

**Glue versions** (a version sets the Spark and Python runtimes):

| Glue version | Spark | Python | Notes |
|---|---|---|---|
| 3.0 | 3.1.1 | 3.7 | First version with Auto Scaling, Flex, G.025X |
| 4.0 | 3.3.0 | 3.10 | Native Redshift Spark integration; oldest version for R/G.12X/G.16X |
| 5.0 | 3.5.4 | 3.11 | Java 17 |
| 5.1 | 3.5.6 | 3.11 | **Default when no version is specified** |
| 6.0 (GA Aug 2026) | 4.1.1 | 3.13 | About **30% lower price per DPU-hour than 5.1**, Iceberg v3, ANSI SQL mode on by default, Scala 2.13 |

Glue 0.9, 1.0 and 2.0 reached **end of life on April 1, 2026**. For upgrades, Glue offers **generative AI upgrades for Apache Spark**, an agent that analyzes a PySpark job, writes an upgrade plan, and test-runs the upgraded code.

**Billing and run controls:**
- Spark jobs, streaming jobs, crawlers and interactive sessions bill **per second with a 1-minute minimum**. Standard list price is **$0.44 per DPU-hour** (us-east-1), **Flex is $0.29**, and R-type workers are **$0.52**.
- **Job timeout:** the maximum is **7 days (10,080 minutes)**. If left blank, the default is **2,880 minutes (48 h) for Glue 4.0 and earlier** and **480 minutes (8 h) for Glue 5.0 and later**. A streaming job with a blank timeout is restarted after 7 days (inside its maintenance window if you set one).
- **Retries:** 0–10 automatic restarts on failure. **Jobs that time out are not retried.**
- **Max concurrency:** **default 1**. A second run of the same job while one is active fails with a concurrency error. Raise it only if parallel runs are safe (for example, runs over different partitions).
- **Job run queuing:** optional. Runs that hit quota or capacity limits wait in a queue instead of failing.
- **Delay notification threshold:** if a run stays in STARTING/RUNNING/STOPPING longer than N minutes, Glue emits a delay event you can route through EventBridge to SNS.

## Auto Scaling and Flex — the two cost dials

**Auto Scaling** (Glue 3.0+, batch, streaming and interactive sessions): set `--enable-auto-scaling true`, and **NumberOfWorkers becomes the maximum**. Glue adds workers when a stage has parallelism to use and removes idle executors, and you're **billed per worker for the time each worker actually ran**. It helps most with uneven jobs: a driver listing millions of S3 objects while executors wait, a skewed stage, an over-provisioned job.

**Flex execution class** (`--execution-class FLEX`): runs on spare capacity at a **lower rate (up to ~34% cheaper)**. The trade-off: a run can **wait before it starts** (no billing while waiting), and capacity can be reclaimed mid-run. Glue keeps going on the remaining workers and backfills when it can. Eligible: **Glue 3.0+ Spark batch jobs on G.1X or G.2X**. **Not** for streaming, Python shell, or the new large and R worker types.

| Workload | Standard | Flex |
|---|---|---|
| Hourly job feeding a dashboard SLA | ✅ | ❌ start time not guaranteed |
| Nightly backfill, dev/test, one-time historical load | works, but costs more | ✅ *"non-urgent"*, *"most cost-effective"* |

**THE trap:** Flex on a job with *"must complete by 6 AM"* or downstream dependencies. Flex means *"can wait"*. Any SLA or time-sensitivity word rules it out.

## DynamicFrame vs DataFrame — the flexible tray

Spark's **DataFrame** needs one fixed schema before it reads a row. Real feeds are messier: `price` is an integer in one file and a string (`"N/A"`) in another. Glue's **DynamicFrame** is a self-describing collection of records. Each record carries its own schema, and a column with conflicting types becomes a **choice type** instead of failing the load. Think of it as a tray with adjustable dividers.

| Transform | What it does | Exam signal |
|---|---|---|
| **ResolveChoice** | Resolves choice types: `cast:long`, `make_cols` (price_int, price_string), `make_struct`, `project:type`, `match_catalog` | *"column has mixed data types"* |
| **ApplyMapping** | Rename columns, change types, drop unmapped columns, in one declarative list | *"rename and cast columns"* |
| **Relationalize** | Flattens nested JSON into a **root table plus child tables** for arrays, linked by keys, ready for a relational store | *"load deeply nested JSON into Redshift"* |
| **Unbox** | Parses a string field holding JSON/CSV into a struct | *"a column contains an embedded JSON string"* |
| **DropNullFields** | Drops fields that are null in every record | Clean sparse schemas |
| **SelectFields / DropFields / RenameField** | Column pruning and renaming | |
| **Filter / Map** | Row-level predicate or function | |
| **Join / SplitFields / SplitRows** | Combine frames, split by columns or predicate | *"integrate data from multiple sources"* (1.2.3) |
| **FillMissingValues, FindMatches (ML)** | Impute nulls, fuzzy dedup/record linkage | *"deduplicate records without a common key"* |

Move between the two with `dyf.toDF()` and `DynamicFrame.fromDF(df, glueContext, "name")`. **Drop to a DataFrame** for window functions, complex SQL, `repartition()`/`coalesce()`, broadcast hints, or anything else Spark has that DynamicFrame lacks. Come back to a DynamicFrame for Glue-specific writers (catalog updates, bookmarks-aware sinks).

**THE trap:** Glue bookmarks, `push_down_predicate`, catalog sinks and `connectionName` belong to the **GlueContext / DynamicFrame API**. A job that reads with plain `spark.read.parquet(...)` gets **no bookmark tracking**, and DynamicFrame reads **don't prune S3 columns** (all columns are read from files that pass the partition filter).

## Job bookmarks — the ticket spike

**Bookmarks** let a scheduled job process only what's new since the last successful run.

- **Supported:** **S3** sources (JSON, CSV, Avro, XML, Parquet, ORC), **JDBC** sources, and the Relationalize transform. Not Python shell jobs; streaming jobs use checkpoints instead.
- **S3 logic:** compares object **last-modified timestamps**. A file that's rewritten gets processed again.
- **JDBC logic:** uses **bookmark keys**. By default that's the primary key, *if* it increases (or decreases) sequentially with no gaps. Otherwise set `jobBookmarkKeys` (plus `jobBookmarkKeysSortOrder`), and the keys must be **strictly monotonically increasing or decreasing**. Bookmarks only see **new rows**, never updates to old ones. Updated rows need CDC ([Guide 10](10-DMS-Database-Ingestion.md)).
- **Option** `--job-bookmark-option`: `job-bookmark-enable`, `job-bookmark-disable` (**the default**), `job-bookmark-pause` (process incrementally without saving state; optional `job-bookmark-from`/`-to` run IDs define a window).
- **Reset** (`aws glue reset-job-bookmark`) reprocesses everything. **Rewind** goes back to a chosen earlier run for backfills. Neither deletes previous output, so write reprocessed data to a new target or dedupe. Deleting the job deletes its bookmark.

Bookmarks only work when **all three** pieces are in place:

```
--job-bookmark-option job-bookmark-enable   (job setting)
+ transformation_ctx="..." on each source you want tracked
+ job.init(...) at the start and job.commit() at the end
```

**THE trap:** *"Bookmarks are enabled but every run reprocesses all files."* The job almost always lacks **`job.commit()`** (state is never saved) or a **`transformation_ctx`** on the source (that source isn't tracked). Other causes: renaming the `transformation_ctx` or changing the source path (the old state no longer matches), reading through `spark.read`, `sampleQuery` (bookmarks are ignored with it), or Lake Formation cell-level filters (no bookmark support). Increasing workers or re-running the crawler fixes none of these.

## Reading efficiently — only fetch the shelves you need

| Problem | Fix | Detail |
|---|---|---|
| Table partitioned by `dt`, job only needs yesterday | **`push_down_predicate="dt='2026-09-26'"`** on `from_catalog` | Filters partition metadata **before** S3 listing. Only works on **partition columns**; Spark SQL syntax |
| Millions of partitions, even listing them is slow | **Partition index** + **`catalogPartitionPredicate`** in `additional_options` | Filtering happens **server-side in the Catalog** ([Guide 13](13-Glue-Data-Catalog-Crawlers.md)). Can be combined with push_down_predicate |
| Hundreds of thousands of tiny JSON/CSV files → too many tasks, driver OOM | **`groupFiles='inPartition'`** + **`groupSize`** (bytes) | Auto-enabled above **50,000 files**. Works for **CSV, JSON, XML, Ion, grokLog**, **not Parquet/ORC/Avro** |
| Driver OOM while a bookmark lists a huge partition | **`useS3ListImplementation=True`** | Lists S3 in batches instead of all at once |
| JDBC read is slow, one executor busy, the rest idle | **`hashfield`** (any evenly distributed column) or **`hashexpression`** (SQL returning an integer) + **`hashpartitions`** (**default 7**) | Runs parallel non-overlapping queries. Ignored for Redshift and S3 tables |
| Need only some rows/columns from the database | **`sampleQuery`** (+ `enablePartitioningForSampleQuery=True`, query ending in `WHERE`/`AND`) | Pushes the SQL down to the database. **Not compatible with bookmarks** |
| Custom/Marketplace JDBC connector | `query`, `filterPredicate`, `partitionColumn` + `lowerBound`/`upperBound`/`numPartitions` | Spark-style JDBC partitioning |
| Large DynamoDB table (>80 GB), don't touch production RCUs | **DynamoDB export connector** (`"dynamodb.export": "ddb"`) | Uses point-in-time export to S3 (**PITR must be on**) and consumes **no read capacity**. Add `dynamodb.simplifyDDBJson` |
| Smaller DynamoDB scans | ETL connector with **`dynamodb.throughput.read.percent`** (**default 0.5**, range 0.1–1.5) and `dynamodb.splits` | Keep the percentage low to avoid throttling the live application |

The DynamoDB, MongoDB and DocumentDB readers **don't support predicate pushdown**.

**THE trap:** *"The JDBC job is slow, so add more workers."* A plain JDBC read uses **one connection**, so extra workers sit idle. Fix it with **hashfield/hashpartitions**, then size workers to match.

## Writing — files, formats, and the catalog without a crawler

- **Format conversion (1.2.6):** write `format="parquet"` (Snappy compression) to turn CSV/JSON into a columnar format; see [Guide 03](03-Data-Formats-Compression.md). The Glue-optimized Parquet writer (`useGlueParquetWriter=True`; `glueparquet` is the legacy name) computes the schema on the fly, which is needed for catalog updates.
- **Partition output** with `partitionKeys=["dt"]` → Hive-style `dt=2026-09-26/` prefixes that Athena, Spectrum and EMR can prune.
- **Control file count:** `df.repartition(n)` (full shuffle, even sizes) or `df.coalesce(n)` (merge, no shuffle) before writing. Hundreds of 1 MB files hurt every downstream query. Aim for roughly 128 MB–1 GB files.
- **Update the Data Catalog from the job** (strong exam item): `getSink(..., enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE" | "LOG", partitionKeys=[...])` + `setCatalogInfo(...)` creates the table, adds **new partitions**, and (with `UPDATE_IN_DATABASE`, the default) updates the schema, all with **no crawler run**. Rules: **S3 targets only**; JSON, CSV, Avro or Parquet (Parquet via the Glue Parquet writer); schema updates require `partitionKeys` in the same order as the table; no nested-schema updates. `LOG` keeps the schema and still adds partitions.

The compact skeleton that shows up (in pieces) across exam items:

```python
import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glue_ctx = GlueContext(SparkContext.getOrCreate())
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)                  # loads bookmark state

orders = glue_ctx.create_dynamic_frame.from_catalog(
    database="raw_db", table_name="orders_csv",
    push_down_predicate="dt >= '2026-09-01'",     # prune partitions before S3 listing
    transformation_ctx="read_orders")             # bookmark key for this source

mapped = ApplyMapping.apply(frame=orders, mappings=[
    ("order_id", "string", "order_id", "long"),
    ("amount",   "string", "amount",   "double"),
    ("dt",       "string", "dt",       "string")],
    transformation_ctx="map_orders")

sink = glue_ctx.getSink(connection_type="s3",
    path="s3://amzn-s3-demo-bucket/curated/orders/",
    enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["dt"], compression="snappy",
    transformation_ctx="write_orders")
sink.setFormat("parquet", useGlueParquetWriter=True)
sink.setCatalogInfo(catalogDatabase="curated_db", catalogTableName="orders")
sink.writeFrame(mapped)

job.commit()                                      # persists bookmark state
```

Open table formats: add `--datalake-formats iceberg` (or `hudi`, `delta`) to get the libraries for MERGE, upserts and time travel. See [Guide 04](04-Open-Table-Formats-S3-Tables.md).

## Connections — the service door into your pantry

A **Glue connection** stores how to reach a data store: JDBC URL, credentials (ideally a **Secrets Manager** secret), and the **VPC, subnet and security group** to use. Types include **JDBC** (MySQL, PostgreSQL, Oracle, SQL Server, Redshift, Aurora/RDS), **Network** (VPC access only, e.g., for S3 behind a VPC or DynamoDB from a private subnet), **Kafka**, **MongoDB/DocumentDB**, and native connectors for **Snowflake, Google BigQuery, Teradata, SAP HANA, Vertica, Azure SQL, Azure Cosmos DB, OpenSearch**. Beyond those you can use **AWS Marketplace or custom connectors** (`marketplace.jdbc/spark/athena`, `custom.*`) and bring-your-own JDBC drivers (`customJdbcDriverS3Path`). Code can reuse a connection's settings with `useConnectionProperties` + `connectionName`.

How the network path works (detail in [Guide 38](38-Networking-for-Data-Pipelines.md)):

```
 Glue-managed Spark ──ENI (private IP, 1 subnet per run)──► your VPC
      │                                                       ├─► RDS / Redshift (SG allows Glue SG)
      │                                                       ├─► S3 via **gateway endpoint** (or NAT)
      │                                                       └─► on-prem DB via VPN / Direct Connect
      └─ security group needs a **self-referencing inbound rule: all TCP** (Spark nodes talk to each other)
```

- Glue places **elastic network interfaces (ENIs) with private IPs only** in the connection's subnet. **Each worker needs an ENI**, so a 100-worker job needs 100+ free IPs, and a job uses **one subnet per run**.
- The subnet has no public IPs, so a job inside the VPC reaches **S3 through an S3 gateway endpoint** and the **internet (SaaS APIs, public databases) through a NAT gateway**.
- On-premises databases that allowlist IPs (skill 1.1.8): route through a **NAT gateway and allowlist its Elastic IP**, or connect privately over **VPN/Direct Connect**.
- **Test connection** in the console validates network path and credentials before you run the job.

**THE trap:** *"The Glue job reaches RDS fine but hangs or fails writing to S3 after being attached to a VPC connection."* The private subnet has **no S3 gateway endpoint and no NAT**. Security groups aren't the problem.

## Job parameters worth recognizing

| Parameter | Does |
|---|---|
| `--job-bookmark-option` | enable / disable / pause bookmarks |
| `--enable-metrics` | Job profiling metrics in CloudWatch (presence flag) |
| `--enable-observability-metrics true` | Extra metrics: worker utilization, skewness (`glue.driver.skewness.*`), throughput, errors by category |
| `--enable-continuous-cloudwatch-log true` | Real-time driver/executor logs in CloudWatch Logs (otherwise logs appear only when the run ends) |
| `--enable-spark-ui true` + `--spark-event-logs-path s3://…` | Spark event logs to S3 (flushed every 30 s) for the Spark UI / history server |
| `--enable-job-insights` | Job run insights: root-cause hints for failures (on by default) |
| `--enable-auto-scaling true` | Auto Scaling, per-worker billing |
| `--additional-python-modules` | pip-install PyPI packages (`pkg==ver`) or S3 wheels |
| `--extra-py-files` / `--extra-jars` / `--extra-files` | Ship your own modules, JARs, config files |
| `--datalake-formats` | `iceberg`, `hudi`, `delta` |
| `--conf` | Spark configs (e.g., `spark.sql.shuffle.partitions`) |
| `--TempDir` | S3 scratch space (used for Redshift reads/writes) |
| `--enable-glue-datacatalog` | Use the Data Catalog as Spark's Hive metastore (Spark SQL on catalog tables) |
| `--enable-lakeformation-fine-grained-access` | Enforce Lake Formation row/column/cell permissions in the job |

## Troubleshooting — symptom → cause → fix (1.2.7, 3.3.6)

| Symptom | Likely cause | Fix |
|---|---|---|
| **Driver OOM** | `collect()`/`toPandas()` on big data; driver listing millions of small files; oversized broadcast join | Keep work on executors; `groupFiles` / `useS3ListImplementation`; drop the broadcast hint or lower `spark.sql.autoBroadcastJoinThreshold` |
| **Executor OOM / lost executors** | **Data skew** (one hot key); too-large partitions; memory-heavy caching | Salt skewed keys, let **AQE** handle skew joins, `repartition`, move to **G.2X/G.4X or R-type** workers ([Guide 16](16-Apache-Spark-Essentials.md)) |
| Job slow, most executors idle | Skew, or single-connection JDBC read | Observability skewness metrics; hashfield/hashpartitions |
| Job slow reading S3 | Small files; no partition pruning | groupFiles; compact upstream; push_down_predicate / catalogPartitionPredicate |
| Job slow overall, high CPU everywhere | Under-provisioned | More workers or a larger worker type; Auto Scaling |
| **"Subnet does not have enough free addresses"** | ENI/IP exhaustion (one ENI per worker) | Larger/secondary CIDR subnet, fewer workers, stagger concurrent runs |
| **S3 AccessDenied / KMS AccessDenied** | Job role lacks `s3:GetObject`/`PutObject` or `kms:Decrypt`/`GenerateDataKey`; bucket/key policy denies | Fix the **Glue job role** and the KMS key policy ([Guide 37](37-IAM-for-Data-Engineers.md), [Guide 39](39-Encryption-Key-Management.md)) |
| **JDBC connection timeout** | SG missing self-reference or DB SG doesn't allow the Glue SG; no route to on-prem; wrong subnet/AZ | Fix SG rules, routes, VPN/DX; **Test connection** |
| Can't reach S3 from a VPC job | No gateway endpoint / NAT | Add an S3 gateway endpoint |
| Duplicate data every run | Bookmark pieces missing | `transformation_ctx` + `job.commit()` + enable |
| `ConcurrentRunsExceededException` | Max concurrency 1 and overlapping schedule | Raise max concurrency, or fix the schedule/trigger |
| Job killed after 8 h | Glue 5.0+ **default timeout 480 min** | Set an explicit timeout |

**Where to look:** CloudWatch Logs `/aws-glue/jobs/output` and `/aws-glue/jobs/error` (continuous logging adds live streams); **job run insights** for the failing line and a suggested fix; the **Spark UI** for stage-level skew and spill; **job metrics and observability metrics** for utilization and skew; Glue Studio's **job monitoring dashboard** for run history, DPU-hours and failures; **EventBridge "Glue Job State Change"** events to alert on FAILED/TIMEOUT through SNS ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)). Glue also offers AI-driven troubleshooting and upgrade agents for Spark jobs.

## Studio, interactive sessions, and generative AI

- **Glue Studio visual ETL:** drag sources, transforms (including Data Quality and sensitive-data detection nodes) and targets onto a canvas. Studio generates the script, and you can switch to script mode.
- **Interactive sessions:** a serverless Spark kernel for Jupyter, the Glue Studio notebook, VS Code or SageMaker Unified Studio. Configure it with magics (`%glue_version`, `%worker_type`, `%number_of_workers`, `%idle_timeout`). Default **5 DPU**, billed per second (1-minute minimum). This replaced the legacy "development endpoints". Signal: *"develop and test ETL code interactively without provisioning a cluster"*.
- **Amazon Q data integration in AWS Glue:** describe the pipeline in natural language ("read orders from the catalog, drop nulls, write Parquet partitioned by date") and get a Glue job or notebook code. 🆕 **New in exam guide v1.1:** Amazon Q is now an in-scope service; see [Guide 19](19-GenAI-LLMs-Vectors.md).
- **Generative AI upgrades for Apache Spark:** automates version upgrades (analysis → plan → validated code changes) for PySpark jobs.
- In SageMaker Unified Studio, visual ETL and notebooks run on Glue under the hood ([Guide 41](41-SageMaker-Unified-Studio-Catalog-Governance.md)).

## Zero-ETL and SaaS connectors

Glue now offers **zero-ETL integrations** that replicate data from **SaaS applications** (such as **Salesforce, SAP, ServiceNow, Zendesk**) and from **DynamoDB** into **Amazon Redshift** or the **SageMaker lakehouse** (Iceberg tables on S3 in the Glue Data Catalog). You configure a source, a target and a refresh interval. There's no job code, and you pay an ingestion fee based on data volume. Glue ETL jobs can also read these SaaS apps through native connectors when you need transformations in the middle. Decision rule: *"replicate SaaS data into Redshift/lakehouse with the least operational overhead, no pipelines to maintain"* → **zero-ETL**. *"Custom transformations while ingesting"* → Glue job with a SaaS connector. *"Simple scheduled SaaS → S3 flows"* → AppFlow is also a classic answer ([Guide 11](11-DataSync-Transfer-Family-Snow-AppFlow.md)).

## Security for Glue jobs

- **Service role:** the job assumes an IAM role trusted by `glue.amazonaws.com`. It usually has the AWS managed **`AWSGlueServiceRole`** policy plus **least-privilege data access** (specific S3 prefixes, KMS keys, Secrets Manager secrets). The **person** who creates or updates the job needs **`iam:PassRole`** on that role. *"User gets AccessDenied when creating a job with an existing role"* → missing PassRole.
- **Security configuration** (attached to a job, crawler, or session) sets encryption for three things: **S3 output** (SSE-S3 or **SSE-KMS**), **CloudWatch Logs** (KMS), and **job bookmarks** (client-side KMS). Data Catalog metadata encryption is a separate, catalog-level setting ([Guide 13](13-Glue-Data-Catalog-Crawlers.md)).
- **Credentials:** store database passwords in **Secrets Manager** and reference the secret in the connection. Don't hard-code them in scripts or job parameters.
- **Lake Formation:** jobs can honor LF table/column permissions. Cell-level filtering needs FGAC mode and disables bookmarks, pushdown predicates and enableUpdateCatalog ([Guide 40](40-Lake-Formation.md)).

## Cost levers (summary — full treatment in [Guide 44](44-Cost-Optimization.md))

**Flex** for non-urgent jobs · **Auto Scaling** instead of guessing worker counts · **right-size workers** (G.1X by default; R-type only for real memory pressure) · **bookmarks + pushdown predicates** so you don't re-read old data · **Python shell (0.0625 DPU)** for small tasks · convert to **Parquet** and compact small files so downstream Athena/Spectrum scans cost less · **explicit timeouts** so a runaway job can't bill for days · stop idle interactive sessions · consider **Glue 6.0** for its lower per-DPU-hour price.

## Question patterns

> *"A nightly Glue job reads new CSV files from S3 and writes Parquet. Job bookmarks are enabled, yet each run reprocesses every file. The script creates its source with `create_dynamic_frame.from_catalog(database=..., table_name=...)`."* → **Add `transformation_ctx` to the source and call `job.commit()` at the end** (without either, bookmark state is never tracked or saved; re-crawling or resetting the bookmark doesn't help)

> *"A Glue job must copy only newly inserted rows from an RDS for PostgreSQL table every hour. The table has an auto-incrementing `order_id`. LEAST operational overhead?"* → **Glue job bookmarks on a JDBC source with `order_id` as the bookmark key** (monotonically increasing key; if the requirement includes *updates* and deletes, switch to DMS CDC)

> *"A Glue job reading a 2 TB RDS table runs for hours; CloudWatch shows one executor busy and the rest idle. MOST effective fix?"* → **Set `hashfield`/`hashexpression` and `hashpartitions` for parallel JDBC reads** (more workers or a larger worker type don't help a single connection)

> *"An S3 prefix receives about 500,000 small JSON files a day. The Glue job fails with driver out-of-memory errors while listing input."* → **Enable `groupFiles='inPartition'` with a `groupSize`, and `useS3ListImplementation`** (grouping cuts tasks and driver bookkeeping; a bigger worker type only postpones the failure)

> *"Analysts query a table partitioned by `year/month/day` covering 10 years. A daily Glue job needs only yesterday's data and currently scans everything."* → **`push_down_predicate` on the partition columns in `from_catalog`** (prunes partitions before S3 listing; a `Filter` transform after the read still scans everything)

> *"A table has more than 5 million partitions and the Glue job spends 20 minutes before reading data."* → **Create a partition index and use `catalogPartitionPredicate`** (server-side filtering in the Catalog instead of listing every partition to the driver)

> *"After each Glue ETL run, a crawler runs to register the new daily partitions, adding 15 minutes and cost. The team wants new partitions queryable in Athena immediately when the job finishes."* → **Write with `getSink(enableUpdateCatalog=True, partitionKeys=[...])` + `setCatalogInfo`** (the job adds partitions itself, so the crawler can be removed)

> *"A data engineer runs month-end historical reprocessing jobs that can finish any time over the weekend. MOST cost-effective?"* → **Flex execution class** (non-urgent + cost; Standard for anything SLA-bound; Flex needs G.1X/G.2X and Glue 3.0+)

> *"Some Glue job runs spend most of their time with few active executors, while others need 40 workers at peak. The team keeps over-provisioning."* → **Enable Auto Scaling with max workers = 40** (pay per worker-second actually used)

> *"A job that joins two large datasets repeatedly fails with executor OOM on one stage; the Spark UI shows one task processing 30× more data than the others."* → **Handle the skew: salt the hot key / enable AQE skew join, then consider R-type memory-optimized workers** (skew is the root cause; more G.1X workers leave the hot partition just as big)

> *"A Glue job needs to read an on-premises Oracle database reachable over AWS Direct Connect, and write Parquet to S3."* → **JDBC Glue connection in a private subnet routed to Direct Connect, SG with a self-referencing all-TCP rule, and an S3 gateway endpoint** (without the endpoint or NAT, the private subnet can't reach S3)

> *"A partner's SaaS API only accepts calls from allowlisted IP addresses. A Glue job must pull data from it."* → **Attach a VPC connection to the job, route through a NAT gateway, and allowlist the NAT gateway's Elastic IP** (Glue workers have no fixed public IP otherwise)

> *"Read a 500 GB DynamoDB table into Glue each night without affecting the production application's read capacity."* → **Glue DynamoDB export connector (`dynamodb.export = ddb`) with PITR enabled** (exports don't consume RCUs; lowering `throughput.read.percent` still consumes capacity and runs slowly)

> *"Nested clickstream JSON with arrays must be loaded into relational Redshift tables."* → **Relationalize transform** (produces a root table and child tables for arrays, linked by keys; Unbox parses embedded strings, it doesn't flatten)

> *"A source's `price` field is sometimes an integer and sometimes the string 'N/A'. The Glue job must produce a numeric column."* → **ResolveChoice with `cast:double`** (resolves the DynamicFrame choice type; the unparseable values become null)

> *"Continuously ingest records from an MSK topic, convert them to Parquet and write to S3 in near real time, serverless, with Spark transformations."* → **Glue streaming job with a Kafka connection, S3 checkpointLocation, and windowSize tuned** (for sub-second stateful analytics the answer is Managed Flink; for no-code delivery, Firehose)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| DPU | 4 vCPU + 16 GB |
| Default worker for most jobs | G.1X (1 DPU) |
| Low-volume streaming | G.025X (streaming only) |
| Recurring OOM, high memory-to-CPU | R-type workers (1 vCPU : 8 GB), Glue 4.0+ |
| Biggest general workers | G.12X / G.16X (Glue 4.0+, no Flex) |
| Billing | Per second, 1-minute minimum; $0.44/DPU-h standard |
| Non-urgent, cheapest | Flex (~34% less; G.1X/G.2X; Glue 3.0+) |
| Variable load, stop over-provisioning | Auto Scaling (workers = max, Glue 3.0+) |
| Default job timeout | 2,880 min (≤ Glue 4.0) / 480 min (Glue 5.0+); max 7 days |
| Tiny Python task | Python shell, 0.0625 DPU (no bookmarks) |
| Ray jobs | ⚠️ Maintenance since Apr 30, 2026 |
| Default Glue version | 5.1; 6.0 = Spark 4.1, ~30% cheaper |
| Process only new data | Job bookmarks (default = disabled) |
| Bookmarks reprocess everything | Missing `transformation_ctx` or `job.commit()` |
| Bookmark S3 logic | Object last-modified time |
| Bookmark JDBC logic | Strictly monotonic bookmark keys (new rows only) |
| Streaming progress tracking | Checkpoints (delete folder to reprocess) |
| Mixed types in a column | DynamicFrame choice → ResolveChoice |
| Rename + cast columns | ApplyMapping |
| Flatten nested JSON for Redshift | Relationalize |
| Read only some partitions | push_down_predicate (partition columns) |
| Millions of partitions | Partition index + catalogPartitionPredicate |
| Many small JSON/CSV files | groupFiles='inPartition' + groupSize (auto > 50,000 files) |
| Parallel JDBC read | hashfield/hashexpression + hashpartitions (default 7) |
| Push SQL to source DB | sampleQuery (not with bookmarks) |
| Big DynamoDB table, no RCU impact | Export connector (PITR required) |
| Add partitions without a crawler | enableUpdateCatalog + partitionKeys + setCatalogInfo |
| Glue in VPC can't reach S3 | S3 gateway endpoint (or NAT) |
| Glue SG requirement | Self-referencing inbound rule, all TCP |
| "Not enough free addresses" | Subnet IP exhaustion (1 ENI per worker) |
| On-prem DB allowlists IPs | NAT gateway Elastic IP |
| DB password storage | Secrets Manager in the Glue connection |
| AccessDenied creating job with role | iam:PassRole |
| Encrypt job output/logs/bookmarks | Glue security configuration (SSE-KMS, KMS) |
| Why did the job fail? | Job run insights + CloudWatch Logs + Spark UI |
| Skew / utilization metrics | --enable-observability-metrics |
| Alert on failed job | EventBridge Glue Job State Change → SNS |
| Iceberg/Hudi/Delta in Glue | --datalake-formats |
| Natural language → Glue ETL code | Amazon Q data integration in Glue |
| SaaS → Redshift/lakehouse, no code | Glue zero-ETL integration |

Every Glue job reads from and writes to the Data Catalog, so the natural next stop is [Guide 13 — Glue Data Catalog & Crawlers](13-Glue-Data-Catalog-Crawlers.md), which covers where those tables and partitions come from and how to keep them in sync.
