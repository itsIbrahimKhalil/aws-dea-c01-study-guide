# Numbers to Know — DEA-C01 limits, defaults and quotas

Every number in this sheet was pulled from the topic guides (facts current to **September 2026**). The exam doesn't ask you to recite quotas. It uses them as **tie-breakers**: a 40-minute job rules out Lambda, a 4-hour restore rules out Deep Archive, a 7-day replay rules out SQS.

**How to use it:** read the *Exam angle* column first. That's the decision the number unlocks.

**⚠️ = changed recently.** The legacy value is given too, because question pools lag behind AWS. If a question only makes sense with the old number, answer the question it's asking. Don't argue with it.

---

## Kinesis Data Streams — [Guide 06](../topic-guides/06-Kinesis-Data-Streams.md)

| Number | What it is | Exam angle |
|---|---|---|
| **1 MB/s or 1,000 records/s** | Write capacity per shard (and per partition key, even on-demand) | Hot key throttles no matter how many shards you add |
| **2 MB/s, 5 GetRecords/s** | Shared read per shard, split across all standard consumers | 3+ consumer apps throttled → enhanced fan-out |
| **2 MB/s per consumer per shard** | Enhanced fan-out (EFO) dedicated throughput, ~70 ms push | "Each app needs its own throughput" |
| **20** (**50** with On-demand Advantage) | Max EFO consumers per stream | ⚠️ 50 is Nov 2025; 20 is the classic value |
| **10 MiB** ⚠️ | Max record size (opt-in per stream, Oct 2025) | Legacy value **1 MB** drove the old "records > 1 MB → MSK" rule. Now pick MSK for Kafka compatibility |
| **500 records / 10 MiB** | PutRecords request cap | Partial failures: retry only the failed entries |
| **24 h → 365 days** | Retention default → maximum | "Replay the last 7 days" → KDS with extended retention |
| **4 MB/s start, 2× previous peak** | On-demand mode initial write capacity and scaling | Unpredictable traffic, no shard management |
| **10 GB/s write / 20 GB/s read** | On-demand max per stream (3 big Regions; lower elsewhere by default) | On-demand scales very far |
| **Twice per 24 h** | Capacity-mode switches allowed | You can't flip modes freely |
| **10× per 24 h**, 2× up / ½ down per call | UpdateShardCount limits | Resharding is rate-limited |
| **20,000** | Default shard quota in major Regions | Soft limit |
| **Default 100, max 10,000** | Lambda ESM batch size for Kinesis | Plus batch window **≤ 300 s** |
| **1–10** | Lambda ParallelizationFactor | Consumer lagging with no errors → raise it |
| **-1 / -1** | Default ESM MaximumRetryAttempts / MaximumRecordAge (infinite) | Poison batch blocks the shard until records expire |
| **≤ 15 min, 1 MB state** | Lambda tumbling window limits | Longer or event-time windows → Flink |
| **Jan 30, 2026** ⚠️ | End of support for KCL 1.x / KPL 0.x | Upgrade libraries |

## Amazon Data Firehose — [Guide 07](../topic-guides/07-Amazon-Data-Firehose.md)

| Number | What it is | Exam angle |
|---|---|---|
| **1–128 MiB, 0–900 s** | S3 buffering hint ranges | Buffer flushes on size OR time, whichever comes first |
| **5 MiB / 300 s** | Default S3 buffer size / interval | "Near real-time", not real-time |
| **0 s** ⚠️ | Minimum buffer interval (not with dynamic partitioning) | Older material says the minimum is **60 s** |
| **≥ 64 MiB** (default 128) | Buffer size required for format conversion / dynamic partitioning | Bigger files, fewer small-file problems |
| **1,000 KiB** | Max record size (Direct PUT) | Batches: **500 records or 4 MiB** |
| **24 h** | How long Direct PUT data is kept if the destination is down | With a KDS source, re-read within stream retention |
| **500** | Default active partitions per stream (dynamic partitioning) | Too many keys → errors |
| **6 MB, ≤ 5 min, 3 retries** | Lambda transform payload / duration / default retries | Failures land in `processing-failed/` |
| **5 KB** | Ingestion billing rounding per record | Many tiny records cost more → aggregate |
| **Creation time only** | When dynamic partitioning can be enabled | Existing stream → create a new one |

## Amazon MSK — [Guide 08](../topic-guides/08-Amazon-MSK-Kafka.md)

| Number | What it is | Exam angle |
|---|---|---|
| **200 MBps in / 400 MBps out** | MSK Serverless throughput per cluster | Plus **2,400 partitions**, IAM auth only |
| **8 MiB** | MSK Serverless max message size | Kafka's configurable message size |
| **Up to 3×** | Express broker throughput per broker vs Standard (20× faster scaling) | High throughput, no storage management |
| **16 TiB** | EBS storage per Standard broker | Grows but never shrinks → tiered storage for long retention |
| **30 / 60** | Brokers per cluster (ZooKeeper / KRaft) | Kafka 4.x is KRaft-only |
| **RF=3, min.insync.replicas=2, acks=all** | No-data-loss producer settings | Durability question |

## Managed Service for Apache Flink — [Guide 09](../topic-guides/09-Managed-Service-for-Apache-Flink.md)

| Number | What it is | Exam angle |
|---|---|---|
| **1 vCPU + 4 GB + 50 GB** | One KPU (Kinesis Processing Unit) | Sizing and cost |
| **+1 KPU** | Orchestration KPU charged per application | Small apps cost at least 2 KPUs |
| **1 (max 8)** | ParallelismPerKPU default | Raise for I/O-bound apps |
| **64** | Default KPU quota per application | Soft limit |
| **Oct 15, 2025 / Jan 27, 2026** ⚠️ | KDA for SQL: no new apps / apps stopped | "Kinesis Data Analytics for SQL" is a distractor |

## Lambda — [Guide 17](../topic-guides/17-Lambda-for-Data-Pipelines.md)

| Number | What it is | Exam angle |
|---|---|---|
| **15 min** | Max runtime | Long or heavy ETL → Glue, EMR, Batch |
| **128 MB–10,240 MB** | Memory range (CPU scales with memory) | Memory fixes duration, not throttling |
| **512 MB–10,240 MB** | /tmp ephemeral storage | Not shared, not durable → EFS or S3 |
| **50 MB / 250 MB / 10 GB** | Zip / unzipped incl. layers / container image | Big dependencies → container image |
| **6 MB / 1 MB** ⚠️ | Sync / async invocation payload | Async was **256 KB** in older material |
| **1,000** | Default Regional concurrency (soft) | Throttles = concurrency problem |
| **+1,000 per 10 s** | Scaling rate per function | Burst behaviour |
| **2 retries, up to 6 h** | Async default retries / max event age | Then on-failure destination |
| **≥ 6× function timeout** | Recommended SQS visibility timeout for a Lambda consumer | Prevents duplicate processing |
| **2–1,000** | SQS ESM maximum concurrency | Caps consumers for one queue |

## SQS, SNS, EventBridge — [Guide 22](../topic-guides/22-EventBridge-SNS-SQS.md)

| Number | What it is | Exam angle |
|---|---|---|
| **1 MiB** ⚠️ | SQS max message (Aug 2025) | Legacy **256 KB**. Larger → Extended Client + S3 |
| **256 KB** | SNS and EventBridge max message | Still 256 KB. Use S3 pointer for big payloads |
| **30 s default, 12 h max** | SQS visibility timeout | Processed twice → raise it |
| **4 days default, 1 min–14 days** | SQS retention | No replay after delete |
| **≤ 20 s** | Long-polling wait | Cuts empty receives |
| **≤ 15 min** | Delay queue | Delay processing |
| **300 TPS (3,000 batched; 70,000 high-throughput)** | SQS FIFO throughput | Order + no duplicates |
| **5 min** | FIFO deduplication window | "Exactly-once" queue semantics |
| **5** | Targets per EventBridge rule | More → SNS fan-out or more rules |
| **24 h / 185 attempts** | EventBridge target retry policy | Then DLQ |
| **UTC** | Timezone of EventBridge scheduled rules and Glue cron | EventBridge Scheduler supports time zones |

## Step Functions — [Guide 20](../topic-guides/20-Step-Functions.md)

| Number | What it is | Exam angle |
|---|---|---|
| **1 year** | Standard workflow max duration | Long jobs, `.sync`, callbacks |
| **5 min** | Express workflow max duration | No `.sync`, no callbacks, no Distributed Map, no redrive |
| **256 KiB** | Payload per state | `States.DataLimitExceeded` → pass S3 URIs |
| **25,000 events** | Standard execution history limit | Split into child workflows |
| **90 days** | Standard execution history retention | Express → CloudWatch Logs |
| **40** | Inline Map concurrency | More → Distributed Map |
| **10,000** | Distributed Map concurrent child executions | Millions of S3 objects |
| **1 s, 3 attempts, ×2.0** | Retry defaults (interval, MaxAttempts, BackoffRate) | Add `JitterStrategy: FULL` |
| **14 days** | Redrive window (Standard) | Resume from the failed step |
| **60 s** | HTTP Task timeout | Third-party API without Lambda |

## AWS Glue — [Guide 12](../topic-guides/12-AWS-Glue-ETL.md), [Guide 13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md)

| Number | What it is | Exam angle |
|---|---|---|
| **1 DPU = 4 vCPU + 16 GB** | Data Processing Unit | G.1X = 1 DPU, G.2X = 2 DPU |
| **$0.44 / DPU-hour** | Standard Glue price; per second, 1-min minimum | Baseline for cost math |
| **$0.29 / DPU-hour (~34% less)** | Flex execution (Glue 3.0+, G.1X/G.2X only) | Non-urgent jobs. Any SLA word rules it out |
| **~30% lower** ⚠️ | Glue 6.0 DPU-hour price vs 5.1 (GA Aug 21, 2026) | Spark 4.1.1, Python 3.13, ANSI SQL default |
| **5.1** | Default Glue version when none is set | Spark 3.5.6 |
| **Apr 1, 2026** ⚠️ | End of life for Glue 0.9 / 1.0 / 2.0 | Upgrade questions |
| **0.0625 DPU** | Python shell minimum | Tiny non-Spark task; no bookmarks |
| **G.025X** | Smallest worker, streaming jobs only | Low-volume streaming |
| **1 vCPU : 8 GB** | R-type memory-optimized workers (Glue 4.0+) | Recurring OOM |
| **480 min** (5.0+) / **2,880 min** (≤ 4.0) | Default job timeout | "Job killed after 8 hours" |
| **10,080 min (7 days)** | Max job timeout | Streaming jobs restart after 7 days |
| **Disabled** | Default job bookmark setting | Enable it for incremental loads |
| **> 50,000 files** | Threshold for automatic file grouping | `groupFiles` + `groupSize` for small files |
| **7** | Default `hashpartitions` for parallel JDBC reads | Plain JDBC read = one connection |
| **5 DPU** | Interactive session default | Dev/test cost |
| **3** | Max partition indexes per table | Athena needs `partition_filtering.enabled` |
| **Apr 30, 2026** ⚠️ | Glue Ray jobs enter maintenance | Don't choose Ray for new designs |

## Amazon Athena — [Guide 26](../topic-guides/26-Amazon-Athena.md)

| Number | What it is | Exam angle |
|---|---|---|
| **$5 / TB scanned, 10 MB minimum** | Per-query pricing | Partition, Parquet, compress, select fewer columns |
| **$0.30 / DPU-hour, 4-DPU min, 1-min min** ⚠️ | Capacity reservations (Feb 2026) | Legacy minimum was **24 DPUs** |
| **20** | Workgroups per capacity reservation | Isolate dashboard concurrency |
| **30 min (up to 240)** | DML query timeout default (max via quota) | Long queries |
| **100** | Max partitions written by one CTAS / INSERT INTO / UNLOAD | Chain INSERT INTO batches |
| **60 min default, 7 days max** | Query result reuse window | Same query, data rarely changes |
| **24 h** | Managed query results retention (free) | No results bucket to manage |
| **5 days, keep 1** | Iceberg `VACUUM` default snapshot retention | Time travel beyond 5 days fails unless raised |
| **$0.35 / DPU-hour** | Athena for Apache Spark | Serverless PySpark notebooks |
| **Spark 3.2.1 / 3.5** | Athena console notebooks (engine v3) / Unified Studio notebooks | Version trivia, low priority |
| **Depth ≤ 10** | Athena recursive CTE limit | Hierarchies |

## Amazon Redshift — [Guide 23](../topic-guides/23-Redshift-Architecture-Table-Design.md), [Guide 24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md), [Guide 25](../topic-guides/25-Redshift-Performance-Operations-Security.md)

| Number | What it is | Exam angle |
|---|---|---|
| **4–512 RPU** (1,024 some Regions), **default 128** | Serverless base capacity | 4 RPU only for ≤ 32 TB managed storage |
| **16 GB** | Memory per RPU | Sizing |
| **Per second, 60-s minimum** | Serverless billing | Only while queries run |
| **Every 30 min, kept 24 h** | Serverless recovery points | Short-term restore |
| **~8 h or 5 GB/node** | Automated snapshot trigger (provisioned) | Retention **1 day default, up to 35** |
| **~10 min, 2×–4× limits** | Elastic resize | Beyond limits → classic resize |
| **99.99%** | Multi-AZ SLA (RA3/RG only) | AZ failure. Region failure → cross-Region snapshot copy |
| **1 MB** | Block size behind zone maps | Sorted data skips blocks |
| **1 MB–1 GB compressed** | COPY file size, count = multiple of slices | One huge gzip loads slowly |
| **≥ 128 MB** | Uncompressed CSV / Parquet / ORC auto-split threshold for COPY | |
| **6.2 GB** | Max UNLOAD file size | Parallel by default, files per slice |
| **16 MB** | Max SUPER value | Not a dist or sort key |
| **65,535 bytes** | Max VARCHAR (bytes, not characters) | Multibyte data |
| **8** | Max interleaved sort key columns | Rarely the right answer |
| **95%** | VACUUM default sort threshold | One VACUUM at a time |
| **~1 h per 24 h, up to 30 h** | Concurrency scaling free credit | Queue spikes |
| **24 h** | Redshift Data API result retention | SQL from Lambda / Step Functions |
| **1023** | Serializable isolation violation error | Retry, LOCK early, shorten transactions |
| **SNAPSHOT** ⚠️ | Default isolation for new clusters/Serverless | Older default **SERIALIZABLE** |
| **Jan 2025** ⚠️ | New clusters encrypted by default, private, `require_ssl=true` | Old material: "encryption must be chosen at launch" |
| **Apr 2025** ⚠️ | DC2 deprecated | DC2/DS2 answers are distractors |
| **Jun 30, 2026** ⚠️ | Python UDFs end of support | Use Lambda UDFs or SQL UDFs |
| **Apr 30, 2026** ⚠️ | COPY/UNLOAD client-side encryption ended | Use SSE-S3 / SSE-KMS |
| **Nov 2025 / Apr 2026** ⚠️ | Redshift Iceberg writes: CREATE/CTAS/INSERT, then UPDATE/DELETE/MERGE | Redshift can write lakehouse tables |

## DynamoDB — [Guide 27](../topic-guides/27-DynamoDB.md)

| Number | What it is | Exam angle |
|---|---|---|
| **400 KB** | Max item size | Bigger → S3 object + pointer |
| **1 RCU = 1 strong read/s ≤ 4 KB** | Read unit (2 eventual reads; transactional = 2 RCU) | Round up per item |
| **1 WCU = 1 write/s ≤ 1 KB** | Write unit (transactional = 2 WCU) | Round up per item |
| **3,000 RCU / 1,000 WCU (~10 GB)** | Per-partition ceiling | Hot key throttles despite table capacity |
| **4,000 writes/s, 12,000 reads/s** | On-demand starting throughput | Absorbs **2× previous peak**; pre-warm for known events |
| **1 MB** | Query/Scan page size (before filtering) | Filters don't save RCUs |
| **20 / 5** | Default GSIs / max LSIs per table | LSI only at creation; 10 GB item collection |
| **24 h, 2 readers per shard** (1 for global tables) | DynamoDB Streams | Replay or many consumers → KDS for DynamoDB |
| **Up to 1 year** | Kinesis Data Streams for DynamoDB retention | Many consumers, longer retention |
| **35 days** | PITR window (restore to a new table) | Accidental delete recovery |
| **Few days** | TTL deletion lag | "Exactly at midnight" → scheduled job |
| **100 items / 4 MB** | Transaction limits (2× cost) | All-or-nothing writes |

## Amazon S3 — [Guide 05](../topic-guides/05-S3-Data-Lake-Storage.md), [Guide 31](../topic-guides/31-Data-Lifecycle-Retention-Resiliency.md)

| Number | What it is | Exam angle |
|---|---|---|
| **50 TB** ⚠️ | Max object size (Dec 2025) | Legacy value **5 TB** |
| **5 GB** | Max single PUT | Bigger → multipart |
| **10,000 parts, 5 MiB–5 GiB** | Multipart limits; use from ~100 MB | |
| **3,500 / 5,500** | PUT-class / GET-class requests per second per prefix | 503 Slow Down → more prefixes, bigger files |
| **30 / 90 / 90 / 180 days** | Min storage duration: IA / Glacier IR / Glacier Flexible / Deep Archive | Short-lived data stays in Standard |
| **30 days** | Minimum age before lifecycle transition to IA | |
| **128 KB** ⚠️ | IA minimum billable object; objects < 128 KB not transitioned by lifecycle (since Sep 2024) | Aggregate tiny files first |
| **40 KB** | Per-object overhead for Glacier Flexible / Deep Archive | Millions of tiny objects cost more archived |
| **1–5 min / 3–5 h / 5–12 h** | Glacier Flexible restores: Expedited / Standard / Bulk | "Within 4 hours" |
| **≤ 12 h / ≤ 48 h** | Deep Archive restores: Standard / Bulk | Never meets a 4-hour target |
| **15 min** | Replication Time Control SLA | Compliance-grade replication |
| **7 days** | Max presigned URL lifetime | Temporary access, no AWS account |
| **Apr 2026** ⚠️ | SSE-C disabled by default on new buckets | |
| **Existing buckets** ⚠️ | Object Lock can now be enabled on existing buckets | Older material says new buckets only |
| **Jul 25, 2024** ⚠️ | S3 Select closed to new customers | Use Athena |

## S3 Tables, Iceberg and S3 Vectors — [Guide 04](../topic-guides/04-Open-Table-Formats-S3-Tables.md), [Guide 19](../topic-guides/19-GenAI-LLMs-Vectors.md)

| Number | What it is | Exam angle |
|---|---|---|
| **512 MB** (64–512 MB) | S3 Tables compaction target file size | Managed small-file fix |
| **Min 1 snapshot, max age 120 h** | S3 Tables default snapshot retention | Raise it before auditors need time travel |
| **3 days + 10 days** | S3 Tables unreferenced files: noncurrent after 3, deleted 10 later | |
| **10 / 10,000** | Table buckets per Region / tables per bucket (default) | |
| **Up to 3× / 10×** | S3 Tables query throughput / TPS vs self-managed Iceberg | Marketing claim, exam-friendly |
| **2 billion** | Vectors per S3 Vectors index | Cheapest at huge scale, sub-second |
| **10,000 / 1–4,096 dims** | Indexes per vector bucket / dimensions | Cosine or Euclidean |
| **1,024** (512, 256) | Titan Text Embeddings V2 default dims (8,192 tokens) | Same model for docs and queries |
| **~50%** | Bedrock batch inference discount vs on-demand | Millions of offline prompts |

## Security — KMS, Secrets Manager, Parameter Store, Lake Formation — [Guide 39](../topic-guides/39-Encryption-Key-Management.md), [Guide 37](../topic-guides/37-IAM-for-Data-Engineers.md), [Guide 40](../topic-guides/40-Lake-Formation.md)

| Number | What it is | Exam angle |
|---|---|---|
| **90–2,560 days (default 365)** | KMS customer managed key automatic rotation period (plus on-demand) | Rotation keeps old key material for decrypt |
| **4 KB** | Max plaintext for KMS `Encrypt` | Bigger → envelope encryption with data keys |
| **7–30 days (default 30)** | KMS key deletion waiting period | Crypto-shredding; cancellable until then |
| **10k / 20k / 100k rps** | Symmetric KMS request quota (by Region) | High-volume SSE-KMS → S3 Bucket Keys |
| **Every 4 hours** | Most frequent Secrets Manager rotation | Parameter Store has no native rotation |
| **10,000 params / 4 KB, free** | Parameter Store Standard tier | |
| **100,000 params / 8 KB** | Parameter Store Advanced tier | Can't downgrade to Standard |
| **50 / 1,000** | LF-Tags per resource / values per tag (soft) | |
| **1–5** | Lake Formation cross-account versions | v3 for direct IAM-role sharing, **v4** for hybrid mode, v5 for huge shares |
| **Glue 5.0+ / EMR 7.7+** | Minimum for LF fine-grained enforcement in Glue ETL / EMR on EKS | Old Glue versions ignore LF column rules |
| **Dec 31, 2024** ⚠️ | Lake Formation governed tables stopped | Distractor → Iceberg |

## EMR, Spark, compute — [Guide 15](../topic-guides/15-Amazon-EMR.md), [Guide 16](../topic-guides/16-Apache-Spark-Essentials.md), [Guide 18](../topic-guides/18-Containers-Batch-EC2-Compute.md)

| Number | What it is | Exam angle |
|---|---|---|
| **1 or 3** | EMR primary nodes (3 = HA) | Keep On-Demand |
| **2 minutes** | Spot interruption notice | Spot on task nodes only for critical jobs |
| **5 / 30** | Instance types per EMR instance fleet (see Guide 15 for when each applies) | Diversify Spot |
| **60 min default (1 min–7 days)** | EMR auto-termination idle timeout | Idle cluster cost |
| **15 min** | EMR Serverless auto-stop default | |
| **30 days** | EMR persistent application UIs after termination | Debug a finished cluster |
| **128 MB** | Spark `maxPartitionBytes` input split size | |
| **200** | Default `spark.sql.shuffle.partitions` | |
| **10 MB** | Default `autoBroadcastJoinThreshold` | Small table → broadcast join |
| **> 5× median AND > 256 MB** | AQE skew partition definition | AQE on by default since Spark 3.2 |
| **max(384 MiB, 10%)** | Default `memoryOverhead` | "Container killed … memory limits" |
| **2–10,000** | AWS Batch array job size | Fan out one program over N inputs |
| **16 vCPU / 120 GB; 20 → 200 GiB** | Fargate max task size; ephemeral storage default → max | |
| **3,000 IOPS / 125 MiB/s** | gp3 baseline | |

## OpenSearch — [Guide 29](../topic-guides/29-OpenSearch-Service.md)

| Number | What it is | Exam angle |
|---|---|---|
| **~10–30 GB / ~30–50 GB** | Target shard size: search / logs | Primary shard count is fixed at creation |
| **3** | Dedicated cluster manager nodes for production | |
| **99.99%** | Multi-AZ with Standby (3 AZs) | |
| **6 GiB** | Memory per OpenSearch Serverless OCU | Indexing and search scale separately |

## Aurora, RDS and purpose-built DBs — [Guide 28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md)

| Number | What it is | Exam angle |
|---|---|---|
| **256 TiB** ⚠️ | Aurora max storage (Jul 2025) | Older value 128 TiB |
| **6 copies / 3 AZs** | Aurora storage replication | |
| **15** | Aurora Replicas per cluster | Reader endpoint for extracts |
| **0–256 ACU**, auto-pause | Aurora Serverless v2 range | Scales to zero |
| **50 s** | MySQL `innodb_lock_wait_timeout` default | Blocking ETL updates |
| **`<->` / `<=>` / `<#>`** | pgvector L2 / cosine distance / negative inner product | |
| **Jun 20, 2025** ⚠️ | Timestream for LiveAnalytics closed to new customers | → Timestream for InfluxDB |

## Ingestion, transfer, monitoring, misc — [Guide 10](../topic-guides/10-DMS-Database-Ingestion.md), [Guide 11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md), [Guide 32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md), [Guide 36](../topic-guides/36-Programming-IaC-CICD.md)

| Number | What it is | Exam angle |
|---|---|---|
| **1 DCU = 2 GB, 1–384 DCU** | DMS Serverless capacity unit and range | Spiky replication, no sizing |
| **8 (max 49)** | DMS MaxFullLoadSubTasks | Tables loaded in parallel |
| **≤ 100 MB** | DMS limited LOB mode max LOB size | LOBs truncated |
| **~10.8 TB/day (~8 realistic)** | 1 Gbps link capacity | Online takes > ~1 week → offline device |
| **210 TB** | Snowball Edge Storage Optimized capacity | ⚠️ Snowball Edge closed to new customers |
| **1 s; alarms 10/30 s** | CloudWatch high-resolution metrics | Sub-minute monitoring |
| **Never expire** | CloudWatch Logs default retention (1 day–10 years settable) | Set retention in IaC |
| **5** | Subscription filters per log group | |
| **≤ 12 h lag, 1 active task** | CloudWatch Logs export task | Batch only; streaming → subscription filter |
| **90 days** | CloudTrail Event history (management events only) | Data events need a trail |
| **~10 yr / ~7 yr** | CloudTrail Lake retention options | ⚠️ Closed to new customers May 31, 2026 |
| **29 s / 30 s** | API Gateway REST / HTTP integration timeout | |
| **1,000** | Keys per S3 list page | "Only first 1,000 processed" → paginator |
| **130 min, 65 Q (50 scored)** | Exam length | Pass = **720** of 1,000, compensatory |
| **2 B rows / 2 TB** | Quick Sight SPICE per dataset (Enterprise) | Large dashboards |

---

Next: [keyword-to-service.md](keyword-to-service.md) for signal words, and [decision-flowcharts.md](decision-flowcharts.md) for the big choices.
