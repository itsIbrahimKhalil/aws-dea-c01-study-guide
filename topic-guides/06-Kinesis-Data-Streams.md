# 06 · Amazon Kinesis Data Streams — the toll highway with a dashcam

> **Exam map:** D1 · Task 1.1 — D2 · Task 2.1 · **Skills:** 1.1.1, 1.1.7, 1.1.9, 1.1.10, 1.1.11, 2.1.1 · **Weight:** 🔥🔥🔥 High · **Read time:** ~22 min

## The idea

Picture a **multi-lane toll highway**. Cars (records) enter at the on-ramps, each lane has a fixed capacity, and every car's **licence plate** decides which lane it must drive in, so cars with the same plate always follow each other in the same order. Above the highway is a **dashcam** that records everything and keeps the footage for a set number of days. Anyone with permission can watch the footage live, or **rewind** it and watch again from any point.

That is **Amazon Kinesis Data Streams (KDS)**. The lanes are **shards**, the licence plate is the **partition key**, the dashcam footage is the **retention period**, and the viewers are **consumers**: Lambda functions, Kinesis Client Library (KCL) applications, Amazon Data Firehose, Amazon Managed Service for Apache Flink, AWS Glue streaming jobs, Spark on EMR, Redshift streaming ingestion. Producers write records; KDS stores them durably across three Availability Zones in arrival order per shard; consumers read them **independently** and at their own pace. Reading doesn't delete anything. A record just ages out when retention expires.

That design gives KDS its two defining traits, and the exam leans on both: **many independent readers of the same data** (fan-out) and **replay** (rewind the dashcam). If a question needs neither, a simpler tool usually wins (Firehose for "just land it in S3", SQS for "one worker per message").

This guide shows you how to size lanes, pick partition keys, handle throttling, wire Lambda to a stream without a poison record freezing it, and choose between KDS, Firehose, MSK and SQS.

## Shards: the lane capacity math

A shard is a unit of capacity with hard limits:

| Direction | Per-shard limit | Notes |
|---|---|---|
| **Write** | **1 MB/s or 1,000 records/s** (whichever you hit first) | Exceed it → `ProvisionedThroughputExceededException` |
| **Read, shared (standard) consumers** | **2 MB/s** total, **5 `GetRecords` calls/s**, shared by **all** standard consumers | One `GetRecords` returns up to **10 MB / 10,000 records**. After a 10 MB call, further calls in the next **5 s** are throttled |
| **Read, enhanced fan-out (EFO)** | **2 MB/s per consumer per shard**, dedicated | Pushed over HTTP/2 via `SubscribeToShard` |

**Sizing a provisioned stream:** `shards = max(write MB/s ÷ 1, read MB/s ÷ 2)`, then check the 1,000 records/s limit too. Example: 5,000 records/s of 0.5 KB = 2.5 MB/s → 3 shards on bytes, but 5,000 records/s ÷ 1,000 = **5 shards**. **The record count decides here, not the bytes**, which is a common calculation trick.

**Partition key → shard.** KDS takes the **MD5 hash** of the partition key, which gives a 128-bit integer. Each shard owns a contiguous **hash key range**, and the record goes to the shard whose range contains the hash. Consequences:

- **Ordering is guaranteed only within a shard**, so effectively **per partition key**. Every event for `device-42` lands in the same shard, in order. There is **no global ordering** across shards.
- Every record gets a **sequence number**, unique per partition key within a shard and increasing over time. Consumers checkpoint by sequence number.
- **Hot shard**: a low-cardinality key (e.g., `country`, or a handful of big customers) sends too much traffic into one lane while the others sit idle. Fix it with a **higher-cardinality key** (device ID, session ID), or add a **random or calculated suffix** (`customer-7#3`) to spread one hot key over several shards. The cost is that ordering now only holds per suffixed key. You can also pass an `ExplicitHashKey` to target a hash value directly.

**THE trap:** *"Throttling on one shard while overall stream utilization is low."* Adding shards alone does **not** help if one partition key still hashes to one shard. **Fix the key distribution** (or split that specific hot shard in provisioned mode). Even on-demand streams can't carry a single partition key beyond **1 MB/s / 1,000 records/s**.

### Record size: current truth vs. legacy exam framing

- **Since Oct 28, 2025**, KDS supports records up to **10 MiB**. It's an opt-in, per-stream setting (`UpdateMaxRecordSize`), and the `PutRecords` request cap rose from **5 MiB to 10 MiB**. Shards absorb occasional large records using **burst capacity** (a shard can briefly burst toward 10 MiB/s but must average its 1 MiB/s over time). Large records are meant to be **intermittent**, not your steady diet.
- **Legacy exam framing:** for years the limit was **1 MB**, and *"messages larger than 1 MB → Amazon MSK"* was a classic signal. Older questions in the pool may still assume it. If a question states the 1 MB limit, answer inside that premise. If it only says *"payloads up to several MB, Kafka-compatible tooling"*, MSK is still a strong answer for other reasons (see [Guide 08](08-Amazon-MSK-Kafka.md)). For huge payloads the evergreen pattern is still **claim check**: put the object in S3 and stream a pointer.

## Capacity modes

| | **Provisioned** | **On-demand Standard** | **On-demand Advantage** |
|---|---|---|---|
| You manage | Shard count | Nothing | Nothing (optional warm throughput) |
| Scaling | Manual: `UpdateShardCount`, `SplitShard`/`MergeShards` (or your own automation) | Automatic. Starts at **4 MB/s write / 8 MB/s read**; handles up to **2× the peak of the previous 30 days** | Same auto-scaling, plus **warm throughput** you can pre-set before a known spike (and lower again to scale down) |
| Max per stream | Shard quota | **10 GB/s write / 20 GB/s read** in us-east-1, us-west-2, eu-west-1; **200 MB/s / 400 MB/s** elsewhere unless raised via support | Same limits |
| Billing | Per shard-hour + PUT payload units | Per stream-hour + per GB in/out | **No per-stream charge**; data prices **≥60% lower**; account commits to **25 MiB/s ingest + 25 MiB/s retrieval** |
| EFO consumers | **20** per stream | **20** | **50** |
| Pick when | Predictable traffic, fine-grained shard control, cheapest at steady high utilization | *"Unpredictable / spiky / new workload"*, *"no capacity planning"* | Many streams or many consumers with steady, sizable volume |

Operational facts worth points:
- Switch **provisioned ↔ on-demand at most twice per 24 hours** per stream. There's no downtime (status goes `UPDATING`) and the shard count carries over.
- On-demand throttles if traffic grows **beyond 2× the previous peak within ~15 minutes**. Producers must retry. For a known launch-day spike: pre-warm (Advantage warm throughput), or switch to provisioned ahead of time and over-provision.
- On-demand Advantage (Nov 2025) is an **account-level, per-Region setting** with a **24-hour minimum** before you can disable it. Treat it as a cost lever. It's too new to be a core exam topic.
- Default provisioned shard quota is now **20,000 shards per account** in us-east-1, us-west-2 and eu-west-1 (lower elsewhere); **50 on-demand streams** per account by default. Both can be raised.

**Resharding (provisioned):**
- `UpdateShardCount` (uniform scaling): at most **10 times per rolling 24 h**, and each call can go **up to double** or **down to half** the current count. Targets that are multiples of 25% finish faster.
- `SplitShard` divides one (hot) shard's hash range. `MergeShards` combines two **adjacent** cold shards.
- After a split or merge, the **parent shard is closed** but stays readable until its data expires. The KCL and Lambda read the **parent before the children**, which preserves per-key order.

**THE trap:** *"Traffic is unpredictable and the team wants no capacity management"* → **on-demand**, not "provisioned plus a CloudWatch alarm that calls UpdateShardCount". That Lambda-driven auto-scaler is the pre-2021 answer and adds operational overhead.

## Retention and replay (skill 1.1.11)

- Default **24 hours**, adjustable up to **8,760 hours (365 days)** with `IncreaseStreamRetentionPeriod` / `DecreaseStreamRetentionPeriod`. Retention above 24 h costs extra (extended retention up to 7 days, long-term retention beyond 7 days, each priced separately).
- **Decreasing** retention makes older records inaccessible almost immediately. Handle it with care.
- **Replay** means getting a shard iterator at a chosen position:

| Iterator type | Starts at |
|---|---|
| `TRIM_HORIZON` | Oldest record still retained (full replay) |
| `LATEST` | Only records arriving after now |
| `AT_SEQUENCE_NUMBER` / `AFTER_SEQUENCE_NUMBER` | A checkpoint (resume exactly where you left off) |
| `AT_TIMESTAMP` | A point in time (*"reprocess everything since 02:00 when the bug shipped"*) |

A shard iterator **expires 5 minutes** after it's issued. Lambda event source mappings accept `TRIM_HORIZON`, `LATEST` or `AT_TIMESTAMP` as the starting position.

**Replayability design rules:** make consumers **idempotent** (at-least-once delivery means duplicates happen: retries, resharding, producer retries). Set retention longer than your worst-case outage plus recovery time. For replay beyond a year, or for cheap bulk reprocessing, also archive the raw stream to S3 (via Firehose or KDS's own S3 delivery) and reprocess from there. See [Guide 02](02-Data-Engineering-Fundamentals.md) for delivery semantics and idempotency.

## Producers

| Producer | What it is | Exam signal |
|---|---|---|
| **`PutRecord` / `PutRecords` (SDK)** | `PutRecords` takes **up to 500 records** per call (request ≤ **10 MiB** now; 5 MiB in older material). **Partial failures are possible**, so check `FailedRecordCount` and retry only the failed entries | *"Custom app, simple integration"* |
| **Kinesis Producer Library (KPL)** | C++-backed library (Java) that does **aggregation** (packs many small user records into one KDS record), **collection** (batches records into `PutRecords` calls), automatic retries and rate limiting | *"High volume of small records, maximize throughput per shard, reduce cost"* |
| **Kinesis Agent** | Standalone Java agent that tails log files on servers and ships them to KDS or Firehose | *"Stream log files from EC2/on-prem servers with minimal code"* |
| **CloudWatch Logs subscription filter** | Pushes log events to KDS, Firehose or Lambda | *"Centralize logs cross-account in near real time"* |
| **AWS DMS** | KDS as a DMS **target** for CDC (change data capture) | *"Stream database changes to multiple consumers"* ([Guide 10](10-DMS-Database-Ingestion.md)) |
| **API Gateway** | AWS service proxy integration straight to `PutRecord` | *"Expose an HTTPS ingestion endpoint without servers"* |
| **DynamoDB** | Kinesis Data Streams for DynamoDB (table CDC into your stream) | See below |

**KPL aggregation has a catch: consumers must de-aggregate.** The **KCL de-aggregates automatically**, and Firehose de-aggregates KPL records from a KDS source. A **Lambda** consumer on a standard iterator gets the aggregated blob and must use the **KPL deaggregation module** in its code. Aggregation also trades a little latency for throughput (KPL's `RecordMaxBufferedTime` adds buffering delay).

> ⚠️ **2026 status:** **KCL 1.x and KPL 0.x reached end of support on Jan 30, 2026** (maintenance mode from Apr 17, 2025). Current versions are **KCL 3.x** and **KPL 1.x**. The concepts in exam questions are version-agnostic.

## Consumers

**Kinesis Client Library (KCL)** is the library for building your own consumer fleet (EC2, ECS, EKS):
- **Lease table in DynamoDB**: one lease per shard. Each shard is processed by **exactly one worker at a time**, and a worker can own many leases. **Checkpoints** (last processed sequence number) are stored there too. Start more workers and leases rebalance; a worker dies and another takes over its leases.
- So **more workers than shards** means idle workers. To scale a KCL app past the shard count, add shards.
- A unique **application name** means a separate lease table, which lets several independent KCL apps read the same stream. Lease table throttling in DynamoDB can stall a KCL app, so give it enough capacity (or on-demand).
- **KCL 3.x** (Nov 2024) balances leases by **worker CPU utilization** instead of equal lease counts, which AWS says cuts consumer compute cost by up to about a third. It adds worker-metrics and coordinator tables in DynamoDB.

**Other consumers** (which to choose lives in [Guide 09](09-Managed-Service-for-Apache-Flink.md)):
- **Lambda**: serverless, per-shard batches (next section).
- **Amazon Data Firehose**: zero-code delivery to S3, Redshift, OpenSearch, Splunk, Iceberg ([Guide 07](07-Amazon-Data-Firehose.md)). Firehose uses a shared-throughput read on the stream.
- **Managed Service for Apache Flink**: stateful, event-time windows, exactly-once processing.
- **AWS Glue streaming ETL / Spark Structured Streaming on EMR**: micro-batch Spark into the lake ([Guide 12](12-AWS-Glue-ETL.md), [Guide 15](15-Amazon-EMR.md)).
- **Redshift streaming ingestion**: a materialized view reads the stream directly with SQL ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)). Supports 10 MiB KDS records since Aug 2026.

> ⚠️ **2026 status:** In **August 2026** KDS gained **native delivery**: **streaming tables** (continuous delivery into **Apache Iceberg tables on Amazon S3 Tables**, with Parquet conversion and inline compaction) and **delivery to general purpose S3 buckets** (raw records, batched and compressed). Both need **on-demand** mode, offer a configurable **5–15 minute** freshness window and **exactly-once per shard** delivery, and **don't consume** the stream's read throughput or EFO slots. The v1.1 exam guide predates this, so on the exam *"stream to S3 with least overhead"* is still **Firehose**. In real designs, check both options.

## Enhanced fan-out and fan-in/fan-out (skill 1.1.10)

**Fan-out** means one stream feeding many consumers. With standard consumers, every app shares the shard's **2 MB/s and 5 calls/s**. Five apps polling each shard once per second already use up the call budget, and latency climbs (around 200 ms per poll cycle for one consumer, and much worse with several).

**Enhanced fan-out (EFO)** gives each registered consumer its own private exit ramp:
- **Dedicated 2 MB/s per shard per consumer**, **pushed** over a long-lived **HTTP/2** connection (`SubscribeToShard`; a subscription lives up to 5 minutes and is renewed). Latency is around **70 ms**.
- Register with `RegisterStreamConsumer`: **20** consumers per stream (**50** with On-demand Advantage).
- Costs extra (per consumer-shard-hour plus per GB retrieved in provisioned mode and On-demand Standard; no premium on Advantage).
- **Signals:** *"multiple consumer applications," "consumers throttled / ReadProvisionedThroughputExceeded," "reduce read latency," "each application needs full throughput."*

**Fan-in** means many producers writing into one stream, for example many accounts' CloudWatch Logs subscriptions or thousands of devices. Watch the partition keys, and size for the combined write rate.

**THE trap:** *"Add a third consumer application; existing consumers start getting throttled."* The answer is **enhanced fan-out**, not more shards. More shards add read capacity, but the root cause is several apps sharing 5 calls/s per shard. EFO removes the contention.

**Cross-account consumption:** since Nov 2023 KDS supports **resource-based policies** on streams and registered consumers. A Lambda function (or KCL app) in account B can read a stream in account A without assuming a role in A. Sharing an EFO consumer needs a policy on both the **stream ARN and the consumer ARN**. If the stream uses the **AWS managed key** (`aws/kinesis`), switch to a **customer managed KMS key** and grant the other account access to it.

## Lambda + Kinesis in depth (skill 1.1.7)

Lambda's **event source mapping (ESM)** does the polling for you. For a standard iterator it polls each shard (about once per second at base rate, faster while catching up), builds a batch, and invokes your function **synchronously**. Each batch comes from **one shard**, and by default **one batch per shard is processed at a time**, in order. The knobs:

| Setting | Range / default | Use it for |
|---|---|---|
| **BatchSize** | default **100**, max **10,000** | Fewer, fuller invocations (overall payload ≤ **6 MB**) |
| **MaximumBatchingWindowInSeconds** | **0–300 s** | Wait to gather fuller batches on low-traffic streams (cost vs. latency) |
| **ParallelizationFactor** | **1–10** concurrent batches **per shard** | Speed up a lagging consumer **without resharding**. **Order is still preserved per partition key** |
| **StartingPosition** | `TRIM_HORIZON`, `LATEST`, `AT_TIMESTAMP` | Replay vs. new-only |
| **BisectBatchOnFunctionError** | off by default | On error, split the batch in half repeatedly to isolate the bad record (doesn't use up retries) |
| **MaximumRetryAttempts** | **-1 (infinite, default)** up to **10,000** | Cap retries on a failing batch |
| **MaximumRecordAgeInSeconds** | **-1 (infinite, default)** up to **604,800 (7 days)** | Skip records too old to matter |
| **On-failure destination** | **SQS, SNS** (metadata only: shard ID + sequence range) or **S3** (metadata **plus the full batch**) | Keep discarded batches for later reprocessing |
| **FunctionResponseTypes = ReportBatchItemFailures** | off by default | Return the failed record's **sequence number**. Lambda checkpoints before the **lowest** reported number and retries from there, not the whole batch |
| **TumblingWindowInSeconds** | up to **900 s (15 min)** | **Stateful** aggregation across invocations; state ≤ **1 MB per shard** |
| **FilterCriteria** | JSON patterns | Drop irrelevant records before invocation (you pay less) |
| **EFO consumer ARN** as source | — | Dedicated throughput and lower latency for the function |

**THE trap, the poison batch:** with the defaults (retries **-1**, max age **-1**), a batch that keeps throwing an error is **retried until the records expire from the stream**. The shard is **blocked** the whole time, `IteratorAge` climbs, and every record behind it waits, even the good ones. The fix is a combination: **bisect on error + a bounded MaximumRetryAttempts and/or MaximumRecordAgeInSeconds + an on-failure destination (S3/SQS/SNS) + ReportBatchItemFailures**. "Increase the Lambda timeout" or "add shards" doesn't fix a poison record.

**The lag playbook.** `GetRecords.IteratorAgeMilliseconds` (the age of the newest record returned) growing means the consumer is falling behind. Fix it in roughly this order:
1. **Function errors?** If yes, it's the poison-batch fix above.
2. **Slow function?** Raise **ParallelizationFactor** (up to 10 per shard), tune memory/CPU, or raise **BatchSize**.
3. **Read contention with other consumers?** Use **EFO**.
4. **Truly out of capacity?** Add **shards** (provisioned) or use on-demand.
5. Check **Lambda concurrency**: shards × parallelization factor concurrent invocations must fit your reserved or account concurrency ([Guide 17](17-Lambda-for-Data-Pipelines.md)).

Alarm on the **Maximum** statistic. An iterator age past **~50% of retention** means data is at risk of expiring unread.

**Tumbling windows** give Lambda simple stateful aggregation (*"count events per device per 5 minutes"*). The limits are what the exam tests: max **15 minutes**, **1 MB** of state per shard, windows based on **arrival time in the stream (not event time)**, no support across resharding. Anything needing event-time windows, late data, sliding/session windows, joins or large state → **Managed Service for Apache Flink** ([Guide 09](09-Managed-Service-for-Apache-Flink.md)).

## Throttling and rate limits (skill 1.1.9)

| Symptom | Metric | Fixes |
|---|---|---|
| Producers get `ProvisionedThroughputExceededException` | `WriteProvisionedThroughputExceeded`, `PutRecords.ThrottledRecords` | **Retries with exponential backoff and jitter**; **batch with PutRecords**; **KPL aggregation** (fewer, fuller records beat the 1,000 records/s limit); **fix hot partition keys**; add shards or use **on-demand** |
| Consumers throttled | `ReadProvisionedThroughputExceeded` | **EFO** for multiple apps; fewer and larger `GetRecords` calls (respect **5/s/shard**); add shards |
| Consumers falling behind | `GetRecords.IteratorAgeMilliseconds` | The lag playbook above |

**Enhanced shard-level metrics** (`IncomingBytes`, `IncomingRecords`, `IteratorAgeMilliseconds`, `OutgoingBytes`/`Records`, and both throughput-exceeded metrics **per shard**) are opt-in via `EnableEnhancedMonitoring` and **cost extra**. They're how you **find the hot shard**. Stream-level metrics are free and arrive every minute. For EFO consumers, watch `SubscribeToShardEvent.MillisBehindLatest`. More playbooks in [Guide 32](32-Monitoring-Logging-Troubleshooting.md).

**THE trap:** *"Stream-level IncomingBytes is well under capacity, yet writes are throttled."* Stream-level averages hide one hot shard. **Enable enhanced shard-level metrics** to find it, then fix the key or split that shard.

## Security

- **Encryption at rest:** server-side encryption with **AWS KMS** (AWS managed `aws/kinesis` key or a customer managed key), applied as records are written. Producers and consumers need KMS permissions for a customer managed key. **In transit:** HTTPS/TLS endpoints. For client-side ("before transit", skill 4.3.4) encryption, encrypt the payload yourself before `PutRecord` ([Guide 39](39-Encryption-Key-Management.md)).
- **Private access:** **interface VPC endpoints (PrivateLink)** let producers and consumers in private subnets reach KDS without internet or NAT. Endpoint policies can restrict which streams are reachable ([Guide 38](38-Networking-for-Data-Pipelines.md)).
- **IAM:** producers need `kinesis:PutRecord(s)`. Consumers need `GetRecords`, `GetShardIterator`, `DescribeStream(Summary)`, `ListShards`, and for EFO, `SubscribeToShard` / `DescribeStreamConsumer`. Lambda's managed policy is `AWSLambdaKinesisExecutionRole`.

## DynamoDB change streams: two options

| | **DynamoDB Streams** | **Kinesis Data Streams for DynamoDB** |
|---|---|---|
| Retention | **24 hours** | Your KDS retention, **up to 1 year** |
| Consumers | Up to **2** simultaneous readers per shard | **5** per shard shared, or up to **20** with EFO |
| Ordering / dupes | Per-item order, **no duplicates** | Use the timestamp to order; **duplicates possible** |
| Processing | Lambda, KCL adapter | Lambda, KCL, Firehose, Flink, Glue streaming |

Pick KDS for DynamoDB when you need *"more than two consumers," "longer retention," "deliver changes to S3 via Firehose,"* or Flink analytics on table changes. Details are in [Guide 27](27-DynamoDB.md).

## KDS vs. Firehose vs. MSK vs. SQS

| | **Kinesis Data Streams** | **Amazon Data Firehose** | **Amazon MSK** | **Amazon SQS** |
|---|---|---|---|---|
| Model | Durable ordered log (shards) | Managed delivery pipe | Managed Apache Kafka log (partitions) | Queue |
| Consumers | Many, independent, **replay** | Fixed destinations only | Many consumer groups, **replay** | **One** consumer per message; deleted after processing |
| Ordering | Per shard / partition key | Not a guarantee you design around | Per partition | FIFO queues only |
| Latency | ~70–200 ms | Seconds to minutes (buffering) | Milliseconds | Milliseconds |
| Retention | 24 h to 365 days | None (24 h retry buffer) | Configurable, effectively unlimited with tiered storage | Up to 14 days |
| Ops overhead | Low (on-demand) to medium | Lowest | Medium (Serverless/Express lower) | Lowest |
| Pick when | Real-time AWS-native streaming, multiple consumers, replay | *"Load streaming data into S3/Redshift/OpenSearch/Splunk with least effort"* | *"Existing Kafka apps," "open-source/portable," Kafka Connect ecosystem* | Decoupling workers, task queues |

Deep dives are in [Guide 07](07-Amazon-Data-Firehose.md), [Guide 08](08-Amazon-MSK-Kafka.md) and [Guide 22](22-EventBridge-SNS-SQS.md).

## Question patterns

> *"Clickstream events must be processed by a fraud model, a real-time dashboard and an S3 archiver. The team also needs to reprocess the last 3 days after a bug fix."* → **Kinesis Data Streams with retention ≥ 3 days; consumers read independently; replay with `AT_TIMESTAMP`** (multiple consumers + replay are KDS's defining traits. SQS deletes on consume; Firehose alone can't replay or feed custom apps.)

> *"A Lambda consumer's IteratorAge keeps rising. The function isn't erroring; each invocation is just slow. Increase throughput with the LEAST operational effort, without changing the stream."* → **Raise the event source mapping's ParallelizationFactor** (up to 10 concurrent batches per shard, order kept per partition key. Resharding works too but changes the stream.)

> *"One malformed record causes a Lambda function to fail repeatedly and processing on that shard has stopped for hours."* → **Enable BisectBatchOnFunctionError, set MaximumRetryAttempts / MaximumRecordAgeInSeconds, add an on-failure destination (S3 or SQS), and use ReportBatchItemFailures** (defaults retry until the record expires, which blocks the shard. A longer timeout doesn't help.)

> *"Three applications consume a stream with the KCL. After a fourth was added, all four report ReadProvisionedThroughputExceeded. Each needs its full read throughput with low latency."* → **Register the consumers for enhanced fan-out** (dedicated 2 MB/s per shard per consumer, HTTP/2 push. Standard consumers share 5 calls/s per shard.)

> *"IoT devices send about 800 records/s per device-group key; a few device groups generate most traffic. Some shards are throttled while others idle."* → **Use a higher-cardinality partition key (device ID) or add a random suffix to hot keys** (hot-shard pattern. More shards alone don't redistribute a single hot key.)

> *"Producers send millions of 200-byte sensor readings per second and hit the 1,000 records/s per shard limit long before 1 MB/s. Reduce shard count and cost."* → **KPL aggregation (+ collection)**; consumers de-aggregate with the KCL (or the deaggregation module in Lambda) (records/s is the binding limit, and aggregation packs many user records into one KDS record.)

> *"A new mobile game's traffic is unknown and may spike 10× on launch day. The team doesn't want to manage capacity."* → **On-demand capacity mode**, with pre-warmed throughput if on On-demand Advantage (or switch to provisioned and over-provision before a known spike) (on-demand handles 2× the previous peak automatically; a sudden jump past that within ~15 min can throttle.)

> *"Every PutRecords call succeeds (HTTP 200), yet some records never appear downstream."* → **Check `FailedRecordCount` in the response and retry the failed entries with backoff** (PutRecords is not all-or-nothing. Throttled entries come back as per-record errors.)

> *"A Lambda function in the analytics account must consume a stream owned by the ingestion account with the LEAST complexity. The stream is encrypted with the AWS managed key."* → **Attach a resource-based policy to the stream for the Lambda execution role, and re-encrypt with a customer managed KMS key shared cross-account** (resource policies launched Nov 2023. The AWS managed key can't be shared.)

> *"Compute a per-device count every 5 minutes from a stream with minimal infrastructure; late-arriving events can be ignored."* → **Lambda event source mapping with a 300-second tumbling window** (stateful ≤ 15 min, arrival-time windows. If it needed event-time or late-data handling, it would be Managed Flink.)

> *"Consumers must never lose data even if a downstream database is down for up to 4 days."* → **Increase the stream retention period to at least 4 days (e.g., 5–7) and make consumers idempotent** (the default 24 h would expire unread records.)

> *"A throttling alarm fires, but stream-level metrics show 30% utilization. Identify the cause."* → **Enable enhanced (shard-level) monitoring and look at per-shard IncomingBytes / WriteProvisionedThroughputExceeded** (averages hide the hot shard.)

> *"Stream a relational database's changes to several independent consumers with replay for 7 days."* → **AWS DMS CDC task → Kinesis Data Streams (7-day retention)** (DMS supports KDS as a target; KDS provides fan-out and replay.)

> *"Legacy scenario: producers emit 3 MB messages and the stream's documented limit is 1 MB per record."* → **Amazon MSK (configurable message size) or store payloads in S3 and stream pointers** (classic framing. Today KDS can be raised to 10 MiB per record, but answer the premise the question gives you.)

> *"EC2 web servers write rotating log files; stream them into KDS with minimal code."* → **Kinesis Agent** (it tails files, handles rotation, retries and checkpoints.)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Per-shard write | **1 MB/s or 1,000 records/s** |
| Per-shard read (shared) | **2 MB/s, 5 GetRecords/s**, split across all standard consumers |
| EFO | **Dedicated 2 MB/s per consumer per shard**, HTTP/2 push, ~70 ms |
| Max EFO consumers | **20** per stream (**50** with On-demand Advantage) |
| Max record size | **10 MiB** opt-in since Oct 2025; classic exam value **1 MB** |
| PutRecords | ≤ **500 records**; partial failures, so retry failed entries |
| Ordering | Per shard / per partition key only |
| Partition key → shard | MD5 hash into the shard's hash key range |
| Hot shard | Higher-cardinality key, random suffix, split that shard |
| Unpredictable traffic, no capacity mgmt | **On-demand** (4 MB/s start, 2× previous peak) |
| On-demand max | **10 GB/s write / 20 GB/s read** (3 big Regions; 200/400 MB/s elsewhere by default) |
| Switch capacity mode | **Twice per 24 h** |
| UpdateShardCount | ≤ **10×/24 h**, max 2× up / ½ down per call |
| Retention | **24 h default → 365 days** |
| Replay from a time / checkpoint | `AT_TIMESTAMP` / `AT_`/`AFTER_SEQUENCE_NUMBER`; all → `TRIM_HORIZON` |
| Many small records, beat 1,000 rec/s | **KPL aggregation + collection** |
| KPL consumer side | KCL auto de-aggregates; Lambda needs deaggregation module |
| KCL coordination | **DynamoDB lease table**, one worker per shard lease, checkpoints |
| KCL workers > shards | Idle workers, so add shards |
| Lambda batch size | Default 100, max **10,000**; window ≤ **300 s**; payload ≤ 6 MB |
| Lambda consumer lagging, no errors | **ParallelizationFactor (1–10)** |
| Poison record blocks shard | **Bisect + max retries/record age + on-failure destination + ReportBatchItemFailures** |
| Failed batch with full records kept | On-failure destination **S3** (SQS/SNS get metadata only) |
| Stateful aggregation in Lambda | **Tumbling window ≤ 15 min, 1 MB state/shard** |
| Consumer lag metric | `GetRecords.IteratorAgeMilliseconds` (alarm on Maximum) |
| Write throttling | Backoff + jitter, PutRecords, KPL, fix key, more shards/on-demand |
| Read throttling, many apps | **Enhanced fan-out** |
| Find the hot shard | **Enhanced shard-level metrics** (extra cost) |
| Cross-account reader | **Resource-based policy** on stream (+ consumer ARN for EFO) + CMK |
| Private subnet access | **Interface VPC endpoint** (PrivateLink) |
| Encryption at rest | SSE with KMS |
| DDB changes, >2 consumers or >24 h | **Kinesis Data Streams for DynamoDB** |
| Stream → S3 with least effort (exam) | **Firehose** (KDS native S3/Iceberg delivery arrived Aug 2026) |
| Existing Kafka apps / Kafka API | **MSK** |

Kinesis Data Streams stores and replays the data but doesn't deliver it anywhere by itself (until 2026). The service that does the hauling to S3, Redshift and friends is next: [Guide 07 — Amazon Data Firehose](07-Amazon-Data-Firehose.md).
