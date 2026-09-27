# 16 · Apache Spark Essentials — partitions, shuffles, and the one pile that ruins everything

> **Exam map:** D1 · Task 1.2, 1.4 — D3 · Task 3.4 · **Skills:** 1.2.5, 1.2.7, 1.4.1, 1.4.10, 3.4.5 · **Weight:** 🔥🔥 Medium · **Read time:** ~18 min

## The idea

**Apache Spark** is the distributed processing engine inside AWS Glue ETL, Amazon EMR, Athena for Apache Spark and SageMaker Unified Studio notebooks. *Distributed computing* (skill 1.4.10) means cutting one big job into many small pieces that run at the same time on many machines, then combining the results. Spark's job is to do that cutting and combining for you and to recover when a machine dies.

Picture an **exam-grading hall**. The **head examiner** (the **driver**) plans the work and hands out **bundles of papers** (**partitions**) to **graders** at desks (**executors**). Each grader has a few pairs of hands (**cores**), and each pair grades one bundle at a time (a **task**). While every grader works only on their own bundle (marking, filtering, fixing typos), everything flies. That's a **narrow transformation**. Then the head examiner says "regroup every paper by student surname" and all graders must carry papers across the hall to each other. That's a **shuffle** (a **wide transformation**): the slow, network-heavy, disk-spilling part. The real disaster is **data skew**. If half the students share one surname, the grader who gets that pile is still working at midnight while everyone else has gone home.

Almost every Spark question on DEA-C01 comes down to a few moves: keep work narrow, make shuffles small, split the giant pile (**salting**), photocopy small reference sheets to every desk (**broadcast join**), and give graders enough desk space (**memory**). This guide teaches those moves, the out-of-memory (OOM) playbook, and where the Spark UI lives in each AWS service.

## Architecture in one picture

```
          +-----------------------------+
          | DRIVER (SparkSession)       |  builds DAG, schedules tasks,
          | plans, collects results     |  holds broadcast vars & collect() output
          +--------------+--------------+
                         | cluster manager (YARN on EMR; Glue/EMR Serverless/Athena manage it for you)
     +-------------------+-------------------+
     v                   v                   v
 [EXECUTOR 1]        [EXECUTOR 2]        [EXECUTOR N]   JVM processes on worker nodes
 cores: T T T T      cores: T T T T      cores: T T T T  one Task per core, one partition per task
 memory + cache      memory + cache      memory + cache
```

- **Job → stages → tasks.** An **action** triggers a **job**. Spark cuts the job into **stages** at every **shuffle boundary**, and each stage runs **one task per partition**. So 1,000 partitions means 1,000 tasks in that stage, run as fast as your total cores allow.
- **Partitions are the unit of parallelism.** Too few means idle cores and giant tasks (OOM risk). Too many means scheduling overhead and tiny output files.
- **Lazy evaluation.** **Transformations** (`select`, `filter`, `withColumn`, `join`, `groupBy`) only build a plan (the **DAG**, directed acyclic graph). Nothing runs until an **action** (`count`, `show`, `collect`, `write`, `take`). This is why an error "appears" at `write()` even though the bug sits in a line far above it.
- **Narrow vs wide:**

| Narrow (no shuffle) | Wide (shuffle across the network) |
|---|---|
| `select`, `filter`/`where`, `withColumn`, `map`, `union`, `coalesce` | `groupBy`/`agg`, `join` (non-broadcast), `distinct`, `orderBy`/`sort`, `repartition`, window functions with `partitionBy` |

- **Catalyst optimizer** rewrites your DataFrame/SQL plan: **predicate pushdown**, **column pruning**, constant folding, join reordering. **Tungsten** runs it efficiently with a compact binary memory format and **whole-stage code generation**. `df.explain()` shows the physical plan (look for `PushedFilters`, `BroadcastHashJoin`, `Exchange` = shuffle).
- **DataFrames / Spark SQL vs RDDs:** DataFrames carry a schema, so Catalyst can optimize them. RDDs (resilient distributed datasets) are opaque objects Catalyst can't see into. **Prefer DataFrames/SQL.** Glue's **DynamicFrame** is a schema-flexible wrapper that converts to and from DataFrames ([Guide 12](12-AWS-Glue-ETL.md)).
- **Fault tolerance:** Spark remembers each partition's **lineage** (the steps that built it) and recomputes lost partitions on another executor. Failed tasks retry (`spark.task.maxFailures` default **4**).

## Partitioning: reading, shuffling, writing

**Reading.** Input partitions come from **file splits**. `spark.sql.files.maxPartitionBytes` (default **128 MB**) caps a split. **Splittable** inputs (Parquet/ORC by row group, uncompressed or bzip2 text) spread across many tasks. A **gzip** CSV/JSON file is **not splittable**, so one 20 GB `.gz` file becomes **one task** on one core. The fix is to store splittable columnar formats, or many moderately sized gzip files ([Guide 03 — Data Formats & Compression](03-Data-Formats-Compression.md)). Millions of tiny files have the opposite problem (per-file overhead, driver listing pressure). Glue's `groupFiles`/`groupSize` or compaction fix that.

**Shuffling.** `spark.sql.shuffle.partitions` defaults to **200** for DataFrame/SQL shuffles, whatever your data size. 200 partitions for 2 TB means roughly 10 GB tasks (spill and OOM). 200 partitions for 50 MB means 200 tiny tasks. **AQE** (below) resizes this at runtime, which is why modern Spark is more forgiving.

**Changing partition counts:**

| | `repartition(n)` / `repartition(col)` | `coalesce(n)` |
|---|---|---|
| Shuffle? | **Yes**, full shuffle | **No**, merges neighbouring partitions |
| Direction | Increase **or** decrease | **Decrease only** |
| Result | Evenly sized (or hash-partitioned by column) | Can be uneven; may **reduce upstream parallelism** |
| Use for | More parallelism, pre-partition by a join/write key, fix uneven partitions | Cheaply cut the number of **output files** at the end |

**Writing:**
- **Number of output files ≈ number of partitions at write time**, multiplied by the distinct `partitionBy` values each task touches. 2,000 partitions × 365 dates can produce hundreds of thousands of small files. Fix with `repartition("dt")` before `partitionBy("dt")`, `coalesce(n)`, the `REBALANCE` hint, or `maxRecordsPerFile` to cap file size.
- **`partitionBy("col")`** on write creates **directory partitions** (`dt=2026-09-01/`) that Athena, Glue and Redshift Spectrum prune. Choose **low-to-moderate cardinality** columns you filter on (date, region). Partitioning by `user_id` is the classic small-files disaster.
- **`bucketBy(n, "col")`** (with `saveAsTable`) hashes rows into a fixed number of files per partition, so later joins and aggregations on that key can **skip the shuffle**. Athena doesn't honour Spark bucketing semantics, so treat it as a Spark/Hive optimization.

## Data skew: the giant pile (skill 3.4.5)

**Skew** means a few keys hold far more rows than the rest, so after a shuffle a few partitions are enormous.

**Symptoms:** the stage is "99% done" for an hour, **one or a few straggler tasks** run far longer than the median, one executor OOMs or spills heavily while the others idle, and adding workers barely helps.

**Detection:** the **Spark UI → Stages → task summary metrics**: compare **max vs median** duration, shuffle read size and records. A max many times the median means skew. Glue's job observability metrics also include a **skewness** metric, and Glue's Spark UI shows the same task distribution. Profiling the key (`df.groupBy("k").count().orderBy(F.desc("count"))`) confirms which keys are hot.

**Fixes, from simplest to most surgical:**

| Fix | How | When |
|---|---|---|
| **Filter nulls / junk keys first** | `where(col("k").isNotNull())`. Handle null keys separately | Null or "unknown" keys often *are* the skew |
| **Broadcast the small side** | `broadcast(dim)`: no shuffle, so skew on the join key can't hurt | Other side is small (≤ ~10 MB default auto-threshold, larger with the hint if memory allows) |
| **AQE skew-join** | `spark.sql.adaptive.skewJoin.enabled` (on by default) splits oversized partitions at runtime. A partition is skewed if > **5×** the median **and** > **256 MB** | Sort-merge joins on Spark 3.x with AQE on. The first thing to confirm |
| **Salting** | Append a random 0..N-1 salt to the hot key and replicate the other side N times (join), or aggregate in two stages | Big-to-big joins or aggregations with a few extreme keys |
| **Isolate hot keys** | Process the top-k keys separately (e.g. broadcast-join just those) and `union` with the rest | A known handful of whales |
| **Better key / more partitions** | Composite keys, higher `shuffle.partitions` | Moderate skew, or the chosen key is simply too coarse |

**Salting a skewed join** (large `facts` with a hot `customer_id`, medium `customers`):

```python
from pyspark.sql import functions as F

SALT = 16
facts_s = facts.withColumn("salt", (F.rand() * SALT).cast("int"))            # 0..15 on the big side
cust_s = customers.withColumn("salt",
            F.explode(F.array([F.lit(i) for i in range(SALT)])))             # each row copied 16x
joined = facts_s.join(cust_s, on=["customer_id", "salt"], how="inner").drop("salt")
```

**Salting a skewed aggregation** (two-stage: partial counts per salt, then combine):

```python
partial = (clicks.withColumn("salt", (F.rand() * SALT).cast("int"))
                 .groupBy("page_id", "salt").agg(F.count("*").alias("c")))
views = partial.groupBy("page_id").agg(F.sum("c").alias("views"))
```

Salting works because the hot key's rows now spread across 16 partitions. The cost is replicating the smaller side 16 times, so pick the smallest salt factor that fixes the straggler.

**The other "skew" family on the exam.** It is the same problem (one key gets too much of the load) in different services, with different fixes:

| Service | Skew shows up as | Fix | Guide |
|---|---|---|---|
| Redshift | Uneven rows per slice (`skew_rows` in `SVV_TABLE_INFO`) | Better DISTKEY, or DISTSTYLE EVEN/AUTO | [23](23-Redshift-Architecture-Table-Design.md) |
| Kinesis Data Streams | Hot shard, `WriteProvisionedThroughputExceeded` | Higher-cardinality partition key, random suffix, split shard | [06](06-Kinesis-Data-Streams.md) |
| DynamoDB | Hot partition throttling | High-cardinality partition key, write sharding (suffixes) | [27](27-DynamoDB.md) |
| Data quality / sampling | Skewed distributions biasing samples | Stratified sampling, profiling | [33](33-Data-Quality.md) |

## Joins

| Strategy | How it works | Chosen when |
|---|---|---|
| **Broadcast hash join** | Small table sent in full to every executor; big table **never shuffles** | One side under `spark.sql.autoBroadcastJoinThreshold` (**10 MB** default), or a `broadcast()` / `/*+ BROADCAST */` hint |
| **Sort-merge join** | Both sides shuffled by key, sorted, merged | Default for large-to-large equi-joins |
| **Shuffle hash join** | Both sides shuffled; hash table built on the smaller per partition | One side much smaller but too big to broadcast (hint `SHUFFLE_HASH`, or AQE picks it) |
| Broadcast nested loop / cartesian | Every row vs every row | Non-equi joins. Avoid at scale |

```python
from pyspark.sql.functions import broadcast
enriched = orders.join(broadcast(stores), "store_id", "left")   # stores = small dimension
```

```sql
SELECT /*+ BROADCAST(s) */ o.*, s.region
FROM orders o JOIN stores s ON o.store_id = s.store_id;
```

Broadcast rules to know: the broadcast table is **built on the driver** and copied to every executor, so a "small" table that is really 5 GB causes **driver or executor OOM**. With an outer join only the non-preserved side can be broadcast (the right side of a LEFT join), and full outer joins can't broadcast. More join hygiene: **filter and project before joining** (Catalyst pushes filters down, but not through a Python UDF), join on columns of the **same type**, and deduplicate dimension keys first (duplicates multiply rows).

## Adaptive Query Execution (AQE)

**AQE** re-optimizes the plan **between stages using real runtime statistics**. It is **on by default since Spark 3.2** (`spark.sql.adaptive.enabled=true`), so it's on in Glue 4.0+, EMR 6.x/7.x and EMR Serverless.

| AQE feature | What it fixes | Setting |
|---|---|---|
| **Coalesce shuffle partitions** | Too many tiny post-shuffle partitions (the blunt 200 default) | `spark.sql.adaptive.coalescePartitions.enabled` (target ~**64 MB** via `advisoryPartitionSizeInBytes`) |
| **Switch join strategy** | A side turned out small after filtering → converts sort-merge to **broadcast** | `spark.sql.adaptive.autoBroadcastJoinThreshold` |
| **Skew join** | Splits oversized partitions into sub-tasks | `spark.sql.adaptive.skewJoin.enabled` (factor **5**, threshold **256 MB**) |

**THE trap:** "Spark 3 job has straggler tasks on a join, so rewrite it with salting" when the options also include "enable AQE / skew-join optimization." With a sort-merge join, AQE is the lower-effort first move. Salting is for cases AQE can't split (e.g. skewed aggregations, or when broadcast isn't possible and AQE is off or ineffective).

## Caching, persistence, checkpointing

- `df.cache()` / `df.persist(StorageLevel...)` keeps a DataFrame that is **reused several times** (iterative algorithms, one dataset feeding several outputs). DataFrame `cache()` defaults to **MEMORY_AND_DISK**. Caching is lazy and only materializes on the first action.
- `unpersist()` when done. Caching everything steals execution memory and **causes** spills and OOM.
- Don't cache data you read once. Reading Parquet from S3 again may beat pinning a huge cache.
- **Checkpointing** writes the data to reliable storage and **truncates lineage**. Use it for very long iterative lineages (plans that get slower every loop) and, in **Structured Streaming**, to record offsets and state (a different, mandatory kind of checkpoint).

## Memory and the OOM playbook

An executor's YARN/K8s container = **`spark.executor.memory`** (JVM heap) **+ `spark.executor.memoryOverhead`** (off-heap: Python worker processes, native buffers, network). Overhead defaults to **max(384 MiB, 10% of executor memory)**. Inside the heap, `spark.memory.fraction` (**0.6**) is the shared execution-plus-storage pool.

| Symptom | Likely cause | Fix |
|---|---|---|
| **"Container killed by YARN for exceeding memory limits"** | Off-heap usage (PySpark UDFs, pandas, Arrow) beyond overhead | Raise **`spark.executor.memoryOverhead`**, reduce cores per executor, avoid heavy Python UDFs |
| Executor `java.lang.OutOfMemoryError` in one or two tasks | **Skewed partition** or huge partition | Fix skew (above), more shuffle partitions, AQE |
| Executor OOM everywhere | Partitions too big for executor memory, over-caching | More partitions, bigger executors (Glue: **G.2X** or memory-optimized **R** workers), `unpersist` |
| **Driver** OOM | **`collect()` / `toPandas()`** on big data, broadcasting a large table, listing millions of small files, `spark.driver.maxResultSize` (**1g**) exceeded | Write results to S3 instead of collecting, `take(n)`/`limit`, don't broadcast big tables, compact small files / Glue file grouping, raise driver memory |
| Heavy "spill (disk)" in Spark UI, slow stages | Partitions larger than execution memory | Increase partitions or memory; the job survives but slowly |
| Lost executors / fetch failures mid-job | Spot reclaim, node loss, disk full | On-Demand core nodes, graceful decommissioning, more local disk |

Executor sizing: a common rule of thumb is ~**4–5 cores per executor** (enough parallelism without starving each task of memory). Memory per core = executor memory ÷ cores. **Dynamic allocation** adds and removes executors with demand. It's **off by default in open-source Spark** but **on by default on EMR**, EMR Serverless and Athena Spark. Glue's equivalent is **Auto Scaling** of workers ([Guide 12](12-AWS-Glue-ETL.md)).

> ⚠️ **2026 status:** **Spark 4.0/4.1** (EMR `emr-spark-8.0.0`, AWS Glue **6.0**, Aug 2026) turns **ANSI SQL mode on by default**. Bad casts, overflow and invalid dates now **raise errors** instead of silently returning NULL. Jobs migrated from Spark 3.5 (Glue 5.1, EMR 7.x) can start failing on dirty data. Fix the data, use `try_cast`, or set `spark.sql.ansi.enabled=false` deliberately. The exam pool predates this; know it for real life.

## Code optimization for runtime (skill 1.4.1)

1. **Built-in functions over Python UDFs.** A plain Python UDF ships every row to a Python process and back, and Catalyst can't see into it. If you must use Python, use **pandas/Arrow (vectorized) UDFs**.
2. **Select only needed columns and filter early.** This gives **column pruning**, **predicate pushdown** and **partition pruning** on Parquet/ORC (Glue adds catalog pushdown predicates).
3. **Broadcast small tables** and skip unneeded shuffles (`distinct`, `orderBy`).
4. **Never `collect()` large data.** Write to S3; use `take`/`show` for peeks.
5. **Cache only reused DataFrames**, and unpersist them.
6. **Use compressed columnar, splittable formats** (Parquet + Snappy/ZSTD), and Iceberg for upserts ([Guide 04](04-Open-Table-Formats-S3-Tables.md)).
7. **Right-size output files** (~128 MB–1 GB) with coalesce/repartition before write.
8. **No per-row API calls.** Use `mapPartitions`, batch lookups or broadcast joins. **Leave AQE on.**

## Structured Streaming in 90 seconds

Spark **Structured Streaming** treats a stream as an unbounded table processed in **micro-batches**. Relevant options: **triggers** (default: next batch as soon as the last finishes; `processingTime="1 minute"`; **`availableNow`** processes everything available and stops, which makes it a cheap incremental batch), **checkpointLocation** in S3 (offsets and state; required for recovery), **watermarks** (`withWatermark("event_time", "10 minutes")`, which bounds state and says how late data may arrive), and output modes append/update/complete. End-to-end **exactly-once** needs replayable sources (Kinesis/Kafka), checkpoints, and **idempotent or transactional sinks** (Iceberg/Delta/Hudi, or upserts). Glue streaming jobs and EMR run it. For sub-second, stateful event processing, **Managed Service for Apache Flink** is usually the exam answer ([Guide 09](09-Managed-Service-for-Apache-Flink.md)).

## Spark on AWS: where it runs and where the Spark UI is

| Service | Spark flavour | Spark UI / history |
|---|---|---|
| **AWS Glue** (5.1 = Spark 3.5.6 default; 6.0 = Spark 4.1.1) | Serverless, DPUs, DynamicFrames | Job run → **Spark UI** tab (Glue 3.0+). Enable `--enable-spark-ui` + `--spark-event-logs-path` (event logs to S3 every **30 s**), or self-host a History Server |
| **EMR on EC2** (7.x = Spark 3.5.x; `emr-spark-8.0.0` = Spark 4.0.2) | YARN, full control | **Persistent application UIs** (off-cluster, 30 days) or on-cluster via SSH tunnel (History Server port **18080**, YARN ResourceManager **8088**) |
| **EMR Serverless** | Per-job workers | Console job run → live **Spark UI** while running, **Spark History Server** after |
| **EMR on EKS** | Pods on your EKS | Spark History Server from the EMR console for the job run |
| **Athena for Apache Spark** | Interactive sessions, DPU-billed | Spark 3.5 engine: live Spark UI + History Server via notebooks or the `GetResourceDashboard` API. PySpark engine v3 (Spark 3.2.1) in Athena console notebooks |
| **SageMaker Unified Studio** 🆕 | Notebooks on Glue/EMR/Athena Spark compute | Links to the underlying engine's Spark UI |

Choosing among them is covered in [Guide 15 — Amazon EMR](15-Amazon-EMR.md) (decision table) and [Guide 45](45-Service-Selection-Decision-Guide.md).

## Question patterns

> *"A Glue Spark job joining clickstream events to a customer table stalls at 99% for an hour; the Spark UI shows one task processing 40 GB while the median task processes 200 MB."* → **Data skew on the join key: confirm AQE skew-join is enabled, and salt the hot key if it persists** (more workers don't help one giant task; the max-vs-median gap is the skew signal)

> *"A large fact table is joined to a 5 MB country lookup table, and the job shuffles terabytes."* → **Broadcast join (`broadcast(countries)` or a BROADCAST hint)** (the small side ships to every executor; the fact table never shuffles)

> *"A PySpark job reads one 30 GB gzip-compressed CSV file and uses only one executor core for the first stage."* → **Gzip isn't splittable: convert to Parquet or split into many files** (adding workers or raising shuffle.partitions doesn't change the one input split)

> *"A job writes 150,000 tiny Parquet files per day to S3, slowing Athena queries."* → **Reduce output partitions before writing (repartition by the partition column or coalesce), optionally maxRecordsPerFile** (file count follows partitions at write time)

> *"Executors fail with 'Container killed by YARN for exceeding memory limits' in a PySpark job using pandas inside a UDF."* → **Increase spark.executor.memoryOverhead** (Python workers live off-heap; raising heap alone doesn't help)

> *"The driver fails with OutOfMemoryError after calling toPandas() on the full result set."* → **Write the result to S3 (or aggregate first) instead of collecting to the driver** (bigger executors don't help a driver-side collect)

> *"A transformation uses a Python UDF to uppercase and trim strings on 2 billion rows and is slow."* → **Replace it with built-in functions (upper, trim)** (native functions run in the JVM with Catalyst/Tungsten; UDFs serialize every row to Python)

> *"Queries on partitioned Parquet read all columns and all partitions even though only 3 columns and one day are needed."* → **Select only required columns and filter on the partition column early (column pruning + partition pruning / pushdown predicates)** (LIMIT or caching doesn't cut the scan)

> *"A daily aggregation by page_id is dominated by one viral page; AQE is enabled but the stage still has one straggler."* → **Two-stage salted aggregation (group by page_id + salt, then by page_id)** (AQE's skew handling targets joins; skewed aggregations need salting)

> *"A job iterates 50 times over the same filtered DataFrame and each iteration re-reads S3."* → **cache()/persist() the reused DataFrame, unpersist when done** (and checkpoint if the lineage keeps growing)

> *"After migrating a job to a Spark 4 runtime, it fails on a CAST of malformed dates that used to produce NULLs."* → **ANSI mode is on by default in Spark 4: clean the data or use try_cast** (Glue 6.0 / emr-spark-8.0.0 behaviour change)

> *"A streaming Spark job must resume after failure without reprocessing or losing Kinesis records, writing to an Iceberg table."* → **checkpointLocation on S3 + transactional Iceberg sink (exactly-once), with a watermark for late data** (checkpoints track offsets and state; idempotent/transactional sinks prevent duplicates)

> *"Engineers need to see stage-level task timings for a Glue job that finished yesterday."* → **Glue Spark UI from the job run (Spark UI event logs written to S3)** (CloudWatch metrics alone don't show per-task distributions)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Unit of parallelism | Partition (1 task per partition per stage) |
| Stage boundary | Shuffle |
| Transformations vs actions | Lazy plan vs triggers execution |
| Wide (shuffle) | groupBy/join/distinct/orderBy/repartition |
| Prefer | DataFrames/SQL over RDDs and Python UDFs |
| Input split size | maxPartitionBytes 128 MB |
| gzip single big file | Not splittable → one task |
| Shuffle partitions default | 200 |
| Increase partitions / even out | repartition (shuffle) |
| Reduce output files cheaply | coalesce (no shuffle, decrease only) |
| Directory partitions on write | partitionBy (low-cardinality filter columns) |
| Pre-shuffled joins on Hive tables | bucketBy + saveAsTable |
| Cap rows per file | maxRecordsPerFile |
| Skew symptom | Straggler tasks, max ≫ median in Spark UI |
| Skew fixes | Filter nulls, broadcast, AQE skew join, salting, isolate hot keys |
| Salted join | Random salt on big side, explode salts on small side |
| Salted aggregation | Two-stage groupBy (key+salt, then key) |
| Broadcast threshold default | 10 MB (autoBroadcastJoinThreshold) |
| Large-to-large equi-join | Sort-merge join |
| AQE default | On since Spark 3.2 (coalesce, join switch, skew join) |
| AQE skew definition | > 5× median AND > 256 MB |
| "Container killed... memory limits" | Raise spark.executor.memoryOverhead |
| memoryOverhead default | max(384 MiB, 10%) |
| Driver OOM | collect()/toPandas(), big broadcast, too many small files |
| Reused DataFrame | cache/persist → unpersist |
| Long lineage / streaming state | Checkpoint |
| Dynamic allocation | Off in OSS; on in EMR; Glue = Auto Scaling |
| Spark 4 ANSI default | Bad casts error → try_cast / clean data |
| Streaming exactly-once | Checkpoints + replayable source + idempotent sink |
| Late data bound | Watermark |
| Glue Spark UI | --enable-spark-ui + event logs to S3 |
| EMR Spark UI after termination | Persistent app UIs (30 days) |
| Athena Spark UI | GetResourceDashboard / notebooks |

Spark tuning saves minutes, and those minutes are money, so the full per-service cost playbook (Glue Flex, EMR Spot, Athena bytes scanned, Redshift Serverless) comes together in [Guide 44 — Cost Optimization](44-Cost-Optimization.md).
