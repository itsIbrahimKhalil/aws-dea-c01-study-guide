# 09 · Amazon Managed Service for Apache Flink — the live scoring booth

> **Exam map:** D1 · Task 1.1, 1.2, 1.3 · **Skills:** 1.1.1, 1.1.12, 1.2.5, 1.3.2 · **Weight:** 🔥🔥 Medium · **Read time:** ~17 min

## The idea

Picture the **scoring booth at a live football match**. Plays happen on the pitch (the **event time**), but the video feed reaches the booth a little late, and sometimes a replay of an earlier play arrives after later ones (the **processing time**, and **out-of-order data**). The scorers keep a **scorecard** that carries forward all match (**state**). They total things per **period** (**windows**). At some point the head scorer declares *"everything up to minute 45 is in, close the first half"* (a **watermark**). Every few minutes someone **photographs the scorecard** (a **checkpoint**), so if the booth loses power they restore the photo and replay the feed from that moment. Nothing is double-counted and nothing is lost (**exactly-once**).

That booth is **Apache Flink**, an open-source engine for **stateful computations over unbounded streams**. **Amazon Managed Service for Apache Flink** (formerly **Amazon Kinesis Data Analytics for Apache Flink**, renamed **August 2023**; the exam and older questions may say "Kinesis Data Analytics") runs your Flink applications **serverlessly**. You upload code (Java, Scala, Python) or write Flink SQL, and AWS provisions compute, runs checkpoints, stores snapshots, patches and scales. It reads from Kinesis Data Streams, MSK/Kafka and other sources, and writes to S3, Firehose, KDS, OpenSearch, DynamoDB and more.

The exam uses Flink as the answer for *"real-time,"* *"stateful,"* *"windowed aggregations,"* *"anomaly detection on a stream,"* *"exactly-once,"* and *"event time / late data"*, and it uses it as a **distractor** when Lambda or Firehose would be simpler. This guide covers the stream-processing vocabulary from zero, Managed Flink's knobs, and the decision table for picking a stream processor.

> ⚠️ **2026 status:** **Amazon Kinesis Data Analytics for SQL Applications** (the old SQL-only service with "in-application streams") is **discontinued**. No new applications from **Oct 15, 2025**, and existing applications stopped running/were deleted from **Jan 27, 2026**. Migration target: **Managed Service for Apache Flink** (Flink SQL) or **Managed Flink Studio**. On the exam, treat "Kinesis Data Analytics for SQL" as a **legacy distractor**. If an old question clearly means "run SQL over a Kinesis stream," the modern equivalent is Managed Flink with Flink SQL.

## Stream-processing concepts from zero

### Stateless vs stateful (skill 1.1.12)

| | **Stateless** | **Stateful** |
|---|---|---|
| Each event processed | On its own | Using memory of previous events |
| Examples | Filter, mask a field, format conversion, route by type, enrich with a static lookup | Counts/sums per window, running averages, deduplication, sessionization, joins between streams, pattern detection |
| AWS fit | **Lambda**, Firehose Lambda transform, Glue streaming map | **Managed Flink** (durable keyed state), Lambda tumbling windows (tiny state, ≤ 15 min), Glue/Spark streaming (stateful operators) |
| Failure concern | Just retry | State must be **checkpointed** or it's lost or double-counted |

The more general idempotency, replay and delivery-semantics theory lives in [Guide 02](02-Data-Engineering-Fundamentals.md).

### Time: event time vs processing time

- **Event time** is when the event happened (a timestamp inside the record).
- **Processing time** is when the operator sees it.
- **Ingestion time** is when it entered the stream (Kinesis `ApproximateArrivalTimestamp`).

Mobile devices go offline, networks retry, and shards lag, so **events arrive late and out of order**. Aggregating by processing time gives wrong answers (a 10:02 purchase that arrives at 10:07 counts toward the wrong 5-minute bucket). **Event-time processing** is how you get correct results, and it's Flink's core strength.

### Watermarks and late data

A **watermark** is Flink's running declaration: *"I believe no more events with timestamp ≤ T will arrive."* You typically set it as `max seen timestamp − allowed out-of-orderness` (e.g., 30 s). When the watermark passes a window's end, Flink **fires** that window. Events arriving after that are **late**. You can keep the window open longer (**allowed lateness**, which updates the result) or divert late events to a **side output** for separate handling instead of silently dropping them.

### Windows

| Window | Shape | Use case |
|---|---|---|
| **Tumbling** | Fixed size, **non-overlapping** (every 5 min) | *"Sales per store every 5 minutes"* |
| **Sliding (hopping)** | Fixed size, **overlapping**, advances by a slide (10-min window every 1 min) | *"Rolling 10-minute average updated every minute,"* moving averages ([Guide 34](34-SQL-for-Data-Engineers.md) covers the SQL version) |
| **Session** | Closes after a **gap of inactivity** (30 min idle) | *"User browsing sessions,"* clickstream sessionization |
| **Global** | One window per key, with a custom trigger | Count-based triggers (*"every 100 events per key"*) |

### State, checkpoints, savepoints, exactly-once

- **Keyed state**: per-key memory (running count per `card_id`), partitioned the same way as the data, so it scales out with parallelism. It's backed by **RocksDB** on local storage in Managed Flink.
- **Checkpoints**: **automatic, periodic, consistent** snapshots of all operator state plus source positions (Kinesis sequence numbers, Kafka offsets), stored durably by the service. After a failure Flink restores the last checkpoint and **rewinds the sources** to match, so state and input stay consistent. That gives **exactly-once state**.
- **Savepoints** are user- or service-triggered snapshots used for **upgrades, code changes, scaling, and stop/restart**. Managed Flink calls them **snapshots**: it can take one automatically when you update or stop the application, and you restore from the latest (or a named) snapshot on start.
- **End-to-end exactly-once** also needs the **sink** to cooperate: **transactional** (two-phase-commit Kafka sink, file sinks that commit on checkpoint) or **idempotent** (upserts keyed by an ID into DynamoDB/OpenSearch). Kinesis and Firehose sinks are **at-least-once**, so make downstream consumers idempotent.
- **Backpressure**: a slow operator or sink makes upstream operators slow down instead of dropping data. Symptoms are growing source lag (Kinesis `millisBehindLatest`, Kafka consumer lag) and high busy/backpressured time in the Flink dashboard. Fixes: raise parallelism, fix the slow sink (batching, more shards or capacity), remove data skew on hot keys.

**THE trap:** *"Aggregate transactions per 5 minutes by when they occurred; devices can be up to 2 minutes late."* Lambda tumbling windows use **arrival time** and can't handle out-of-order data. The answer is **Managed Flink with event-time tumbling windows and a watermark allowing ~2 minutes of out-of-orderness**.

## Managed Service for Apache Flink: the service

**Application types:**
- **Flink applications**: a **JAR** (Java/Scala, DataStream API, Table API or Flink SQL inside the code) or a **Python (PyFlink)** ZIP uploaded to **S3**, then created with a runtime version, IAM service role, and properties. Runs continuously.
- **Studio notebooks**: **Apache Zeppelin**-based interactive notebooks for **Flink SQL, Python or Scala** over live streams. They're good for exploration and ad-hoc streaming queries, and a note can be **deployed as a long-running Flink application with durable state**. Signal: *"analysts want to interactively query a stream with SQL."*

**Capacity: KPUs (Kinesis Processing Units)**
- **1 KPU = 1 vCPU + 4 GB memory + 50 GB running application storage**.
- Each application is also charged **1 extra KPU for orchestration** (the job manager).
- **Parallelism** (default 1) × tasks; **ParallelismPerKPU** (default **1**, max **8**; raise it for I/O-bound work that waits on external calls). **KPUs = Parallelism ÷ ParallelismPerKPU**. Default quota is **64 KPUs** per application (raisable).
- **Automatic scaling** (on by default) adjusts parallelism from CPU usage: it scales up quickly after sustained high CPU and scales down gradually after a long quiet period. Scaling works by **snapshot + restart**, so there's a short processing pause. Parallelism can't exceed the job's **maxParallelism** (128 by default for new jobs), and changing maxParallelism later means you can't restore from old snapshots. You can also scale on a schedule or with your own CloudWatch-driven logic.
- Pricing: **KPU-hours** (including the orchestration KPU) plus running storage and durable backups. There's no charge when the application is stopped. A tiny always-on job still costs at least 2 KPUs, a cost consideration versus Lambda.

**Operational features:**
- **VPC configuration**: attach the application to your subnets and security groups to reach private resources (MSK, RDS, private OpenSearch). It then needs NAT or **VPC endpoints** to reach AWS APIs such as Kinesis or S3 ([Guide 38](38-Networking-for-Data-Pipelines.md)).
- **Monitoring**: CloudWatch metrics (`millisBehindLatest` for Kinesis sources, records lag for Kafka, `numberOfFailedCheckpoints`, `lastCheckpointDuration`, `downtime`, `fullRestarts`, CPU/heap utilization), CloudWatch Logs, and the **Apache Flink dashboard** ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)).
- **Runtimes**: AWS adds new Apache Flink releases over time and supports **in-place version upgrades** from a snapshot. Choose the newest supported runtime for new apps.
- **Security**: an IAM service execution role for sources and sinks, encryption of running storage and snapshots, and KMS for sources and sinks per their own settings ([Guide 39](39-Encryption-Key-Management.md)).

**Connectors you should recognize:** sources **Kinesis Data Streams** (polling or **EFO**), **MSK/Kafka**, S3 files, DynamoDB Streams. Sinks **Kinesis Data Streams**, **Firehose**, **S3** (FileSink, including Parquet), **OpenSearch**, **DynamoDB**, Kafka, JDBC, **Apache Iceberg**. Use **Glue Schema Registry** for Avro/JSON schemas.

**THE trap:** *"Flink application restarts repeatedly after a code change and loses its running totals."* Deploy with **snapshots enabled** and restore from the latest snapshot. Keep operator **UIDs** and state schema compatible. Starting "without snapshot" resets state. A related trap is that a job reading one KDS stream with parallelism far above the shard count leaves subtasks idle, because source parallelism beyond the shard (or partition) count doesn't help.

## Classic Managed Flink use cases

| Use case | Flink features used |
|---|---|
| **Real-time anomaly / fraud detection** (*"flag a card used in two countries within 10 minutes"*) | Keyed state per card, event-time windows, **CEP** (complex event processing) patterns |
| **Streaming ETL** (clean, enrich, convert, route to S3/Iceberg/OpenSearch) | Stateless maps plus exactly-once file/Iceberg sinks |
| **Rolling aggregations / live dashboards** | Sliding windows → OpenSearch or DynamoDB for serving |
| **Stream-stream joins** (orders ⋈ payments within 1 hour) | Interval joins with state and watermarks |
| **Enrichment with reference data** (product catalog) | Broadcast state, or async I/O lookups to DynamoDB/RDS |
| **Sessionization** | Session windows per user |
| **Real-time feature computation for ML** | Keyed state → feature store (training itself is out of scope) |

## Choosing a stream processor

| Option | Model | Best for | Watch out |
|---|---|---|---|
| **AWS Lambda** (Kinesis/MSK ESM) | Serverless per-batch functions | **Stateless** or simple per-record work; light aggregation with **tumbling windows ≤ 15 min** (arrival time, ≤ 1 MB state/shard) | 15-min max runtime, no event-time semantics, per-shard ordering constraints ([Guide 06](06-Kinesis-Data-Streams.md), [Guide 17](17-Lambda-for-Data-Pipelines.md)) |
| **Managed Service for Apache Flink** | Continuous, stateful, **sub-second** | **Complex stateful** logic, **event-time windows, watermarks, late data**, joins, CEP, **exactly-once** | Minimum 2 KPUs always on; Flink skills needed |
| **AWS Glue streaming ETL** | **Spark Structured Streaming micro-batches**, serverless | Streaming **ETL into the lake** (S3/Iceberg, Data Catalog integration), teams that know Spark, seconds-to-minutes latency | Not sub-second; DPU-based cost ([Guide 12](12-AWS-Glue-ETL.md)) |
| **EMR (Spark Streaming or Flink on EMR/EKS)** | Clusters you control | Full control of versions/configs, co-located big batch + streaming, Spot savings | Most operational overhead ([Guide 15](15-Amazon-EMR.md)) |
| **KCL application on ECS/EKS/EC2** | Your own consumer fleet | Custom logic in any language with checkpointing via DynamoDB | You own scaling, deployment, state |
| **Redshift streaming ingestion** | SQL materialized view reading KDS/MSK | Landing streams **straight into the warehouse** for near-real-time SQL analytics | Not a transformation engine: light SQL, refresh-based ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)) |
| **Amazon Data Firehose** | Managed delivery with light transforms | Delivery to S3/Redshift/OpenSearch/Splunk/Iceberg with Lambda transform, Parquet conversion, dynamic partitioning | No state, no windows, buffered latency ([Guide 07](07-Amazon-Data-Firehose.md)) |

**Decision shortcuts:**
- *"Real-time + stateful + event time / late data / exactly-once"* → **Managed Flink**.
- *"Simple per-record transform, serverless, low volume"* → **Lambda**.
- *"Streaming ETL into S3/Data Catalog; team knows Spark; minute-level latency fine"* → **Glue streaming**.
- *"Just deliver it (maybe convert to Parquet)"* → **Firehose**.
- *"Query streams with SQL in the warehouse"* → **Redshift streaming ingestion**.
- *"Interactive SQL on a live stream"* → **Managed Flink Studio notebook**.

**THE trap:** Flink as overkill. *"Remove PII fields from each record before storing in S3, least operational overhead"* → **Firehose with a Lambda transform**, not a Flink application. Stateless, delivery-bound work doesn't justify an always-on stateful engine.

## Designing for resiliency (skill 1.3.2)

- **Checkpointing on** (the default in Managed Flink), with the interval tuned (frequent checkpoints mean faster recovery but more overhead). Alarm on failed checkpoints and restarts.
- **Snapshots** before every update. Enable auto-snapshot on stop/update.
- **Source retention longer than your longest recovery.** Flink replays from checkpointed positions, so the KDS retention or Kafka retention must still hold that data.
- **Idempotent or transactional sinks** for correctness under replay.
- **Parallelism ≤ source shards/partitions** at the source, and **salt hot keys** to avoid skew ([Guide 33](33-Data-Quality.md) covers skew mechanisms).
- **Multi-AZ** is built into the service. For Regional DR, replicate the source (MSK Replicator, or a second KDS stream) and keep deployable code plus snapshots.

## Question patterns

> *"A payments company must flag cards used in two different countries within 10 minutes, in real time, with exactly-once state even during failures. Events can arrive out of order."* → **Managed Service for Apache Flink reading KDS, keyed state per card, event-time windows with watermarks, checkpointing** (stateful + event time + exactly-once. Lambda can't do out-of-order event-time logic.)

> *"Compute a 15-minute rolling average of sensor temperature, updated every minute, and push results to an operational dashboard with sub-second processing latency."* → **Managed Flink sliding (hopping) window → OpenSearch or DynamoDB** (sliding window = rolling average. Firehose can't window; Glue streaming micro-batches are slower.)

> *"Analysts want to explore a live Kinesis stream with SQL interactively, then promote a query to production with minimal effort."* → **Managed Service for Apache Flink Studio notebook, deployed as an application with durable state** (interactive Flink SQL → one-click deploy.)

> *"A team runs Kinesis Data Analytics for SQL applications and must keep processing after the service's discontinuation."* → **Migrate to Managed Service for Apache Flink using Flink SQL (or Studio)** (KDA for SQL: no new apps from Oct 15, 2025; shut down Jan 27, 2026.)

> *"A Flink application reading a 4-shard stream shows high millisBehindLatest; CPU is at 90% on all KPUs."* → **Increase parallelism (or enable/tune automatic scaling), and add shards if source parallelism is the limit** (CPU-bound means more KPUs. Adding sink capacity doesn't help a CPU bottleneck.)

> *"The application calls an external REST API to enrich each event; KPUs show low CPU but throughput is poor."* → **Raise ParallelismPerKPU (up to 8) and/or use async I/O** (I/O-bound tasks waste vCPU at 1 task per KPU.)

> *"After deploying a new version, a Flink app's aggregates reset to zero."* → **Restore from the latest snapshot on update (keep operator UIDs/state compatible)** (starting without a snapshot discards state.)

> *"Join a stream of orders with a stream of shipments arriving up to an hour later and emit late-delivery alerts."* → **Managed Flink interval join with event-time watermarks** (stream-stream join needs managed state for an hour. Lambda's 15-min windows and statelessness rule it out.)

> *"Continuously transform JSON from MSK into Parquet partitioned in S3 and register tables in the Glue Data Catalog. The team knows Spark and accepts ~1-minute latency; minimize operations."* → **AWS Glue streaming ETL job** (serverless Spark micro-batches, native Catalog integration. Flink would work but the Spark skillset and latency tolerance point to Glue.)

> *"Count events per device every 5 minutes; late events can be ignored; volume is low and the team wants the simplest serverless option."* → **Lambda event source mapping with a 300-second tumbling window** (simple arrival-time aggregation. Managed Flink is overkill here.)

> *"A Flink job must reach an Amazon RDS database in private subnets for reference data and also write to Kinesis."* → **Configure the application with VPC subnets/security groups plus a Kinesis interface VPC endpoint (or NAT)** (VPC-attached apps lose default internet paths to AWS APIs.)

> *"Sessionize website clickstream: group each user's events with sessions ending after 30 minutes of inactivity."* → **Managed Flink session windows keyed by user ID** (a session window closes on an inactivity gap. Tumbling windows would split sessions arbitrarily.)

> *"Guarantee end-to-end exactly-once results from Flink into Kafka."* → **Checkpointing + Kafka sink with exactly-once (transactional) delivery; consumers read committed** (checkpoints alone give exactly-once state; the sink must be transactional or idempotent.)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Real-time stateful processing, windows, joins | **Managed Service for Apache Flink** |
| Old name | Kinesis Data Analytics (for Apache Flink); renamed Aug 2023 |
| Kinesis Data Analytics for SQL | ⚠️ Discontinued (no new apps Oct 15, 2025; stopped Jan 27, 2026), a distractor |
| Interactive SQL on a stream | **Managed Flink Studio notebook** (Zeppelin), deploy as app |
| Event time vs processing time | Occurred vs observed. Correct aggregates need **event time** |
| Late / out-of-order events | **Watermarks** + allowed lateness / side outputs |
| Fixed non-overlapping buckets | **Tumbling window** |
| Rolling/moving average | **Sliding (hopping) window** |
| Inactivity-based grouping | **Session window** |
| Per-key memory | **Keyed state** (RocksDB) |
| Automatic recovery point | **Checkpoint** |
| Upgrade/restart with state | **Snapshot** (= Flink savepoint) |
| End-to-end exactly-once | Checkpoints + **transactional/idempotent sink** |
| Slow sink slows the pipeline | **Backpressure**, so scale/fix the sink, raise parallelism |
| 1 KPU | **1 vCPU + 4 GB memory + 50 GB storage** |
| Extra charge per app | **+1 orchestration KPU** |
| ParallelismPerKPU | Default **1**, max **8** (raise for I/O-bound) |
| KPU default quota | **64** per application |
| CPU-bound lag | Increase parallelism / automatic scaling |
| Private resources | Application **VPC configuration** + endpoints/NAT |
| Languages | Java, Scala, **Python (PyFlink)**, **Flink SQL/Table API** |
| Stateless per-record transform | **Lambda** (or Firehose Lambda transform) |
| Lambda stateful limit | Tumbling window **≤ 15 min**, 1 MB state, arrival time |
| Spark streaming ETL to the lake, serverless | **Glue streaming ETL** |
| Full control / Spot / custom versions | **EMR** (Spark Streaming or Flink) |
| SQL analytics on streams in warehouse | **Redshift streaming ingestion** |
| Deliver + light transform, no state | **Firehose** |
| Fraud / anomaly / CEP in real time | **Managed Flink** |

With the streaming block done (ingest in [Guide 06](06-Kinesis-Data-Streams.md) and [Guide 08](08-Amazon-MSK-Kafka.md), deliver in [Guide 07](07-Amazon-Data-Firehose.md), process here), the next source family is databases: change data capture and migrations in [Guide 10 — AWS DMS & database ingestion](10-DMS-Database-Ingestion.md).
