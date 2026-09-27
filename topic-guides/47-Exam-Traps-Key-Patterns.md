# 47 · Exam Traps & Key Patterns — watch the other hand

> **Exam map:** D1 · Tasks 1.1–1.4 — D2 · Tasks 2.1–2.4 — D3 · Tasks 3.1–3.4 — D4 · Tasks 4.1–4.5 (cross-cutting review) · **Skills:** all; heaviest on 1.1.x, 1.2.x, 2.1.x, 3.2.x, 4.2.x · **Weight:** 🔥🔥🔥 High · **Read time:** ~30 min

## The idea

A stage magician doesn't beat you with speed. She beats you with **misdirection**: while your eyes follow the flourishing right hand, the left hand does the real work. DEA-C01 questions are built the same way. Each one has a **flourish**, the answer that sounds powerful, familiar or thorough ("add more shards", "raise the memory", "write a Lambda function"). Somewhere in the stem there is also a **quiet hand**, one or two signal words that actually decide the answer (*"replay"*, *"least operational overhead"*, *"row-level"*, *"must complete by 6 AM"*).

This guide is the magician's notebook. It gathers the most-harvested traps from all the topic guides into one table, lists the AWS changes that make older prep material wrong, compresses the reference pipelines into one-liners, and ends with exam technique: how to read qualifiers, eliminate distractors, pace 65 questions in 130 minutes, and handle multiple-response items.

Use it the week before the exam. When a row surprises you, follow its link back to the owning guide. By the end you'll catch the misdirection in questions like *"throttled even though capacity is fine"*, *"bookmarks enabled but everything reprocesses"*, *"LIMIT to save cost"*, and *"governed tables for ACID"*.

## Greatest-hits trap table

Each row is a flourish the exam puts in front of you, followed by the quiet hand that actually decides the answer. They are grouped by theme so you can review one theme per sitting.

### Ingestion & streaming

| THE trap | The truth | Guide |
|---|---|---|
| One shard throttles, so add shards | A hot partition key stays on one shard. Fix the key or split that shard. Even on-demand caps one key at **1 MB/s / 1,000 rec/s** | [06](06-Kinesis-Data-Streams.md) |
| New consumer app causes throttling, so add shards | Apps share **2 MB/s, 5 reads/s** per shard. **Enhanced fan-out** gives each app its own pipe | [06](06-Kinesis-Data-Streams.md) |
| Default Lambda ESM settings are safe | Retries **-1** and max age **-1** mean a poison batch blocks the shard until records expire. Bisect + bounded retries + on-failure destination | [06](06-Kinesis-Data-Streams.md) |
| Firehose format conversion reads CSV | JSON input only. **Lambda CSV→JSON** first, then conversion with a Glue table | [07](07-Amazon-Data-Firehose.md) |
| Turn on dynamic partitioning for the existing stream | Creation time only. **Create a new stream** | [07](07-Amazon-Data-Firehose.md) |
| Firehose reaches a private Redshift cluster via NAT/endpoint | Firehose connects **in**. It needs a publicly accessible cluster plus the Firehose CIDR, or use Redshift streaming ingestion | [07](07-Amazon-Data-Firehose.md) |
| Firehose for "replay", "multiple consumers" or "sub-second" | Any of those words → **KDS** (often KDS → Firehose for the S3 leg) | [07](07-Amazon-Data-Firehose.md) |
| More Kafka consumers fixes lag | A group can't use more consumers than **partitions**. Add partitions | [08](08-Amazon-MSK-Kafka.md) |
| Lambda tumbling windows for late, out-of-order events | They use arrival time. **Flink event-time windows + watermarks** | [09](09-Managed-Service-for-Apache-Flink.md) |
| Flink for a stateless PII strip before S3 | Overkill. **Firehose + Lambda transform** | [09](09-Managed-Service-for-Apache-Flink.md) |
| DMS → S3 → Glue → COPY for Aurora → Redshift | Zero-ETL is the modern least-effort answer | [10](10-DMS-Database-Ingestion.md) |
| DMS writes Iceberg natively | No Iceberg target. DMS → S3 → Glue `MERGE INTO` Iceberg | [10](10-DMS-Database-Ingestion.md) |
| DataSync moves database rows | DataSync is files/objects. Rows + CDC → **DMS** | [11](11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| Snowball imports straight into Deep Archive | Lands in **S3**, then a Lifecycle rule transitions it | [11](11-DataSync-Transfer-Family-Snow-AppFlow.md) |

### Glue & Catalog

| THE trap | The truth | Guide |
|---|---|---|
| Bookmarks are on, so only new data is read | Needs `transformation_ctx` + `job.commit()` and the GlueContext reader. Default is **Disabled**; `spark.read` isn't tracked | [12](12-AWS-Glue-ETL.md) |
| Slow JDBC read → add workers | One connection by default. Use `hashfield`/`hashpartitions` | [12](12-AWS-Glue-ETL.md) |
| Flex for a job that "must finish by 6 AM" | Flex means *can wait*. Any SLA word rules it out | [12](12-AWS-Glue-ETL.md) |
| Spark cluster for a 20 MB API pull | **Python shell (0.0625 DPU)** or Lambda | [12](12-AWS-Glue-ETL.md) |
| Glue in a VPC hangs on S3 → security group issue | No route. Add an **S3 gateway endpoint** (or NAT) | [38](38-Networking-for-Data-Pipelines.md) |
| Rename crawled `col0` columns by hand | The next crawl undoes it. **Custom CSV classifier, "has heading"** | [13](13-Glue-Data-Catalog-Crawlers.md) |
| Crawl the whole bucket every 15 minutes | Slow and billed. **S3 event crawls, write-time registration, or projection** | [13](13-Glue-Data-Catalog-Crawlers.md) |
| Glue workflow calls Lambda, EMR, Redshift | Glue workflows orchestrate **Glue only**. Use Step Functions or MWAA | [21](21-MWAA-Glue-Workflows.md) |
| Spark straggler on a join → salt it | With AQE available, **enable skew-join AQE** first; salting is the fallback | [16](16-Apache-Spark-Essentials.md) |
| More DPUs/nodes fix skew | One hot key still lands on one worker. Fix distribution | [33](33-Data-Quality.md) |

### Athena & formats

| THE trap | The truth | Guide |
|---|---|---|
| `LIMIT 10` cuts cost | Bytes scanned aren't reduced on a full scan. Partitions, columnar formats, fewer columns | [26](26-Amazon-Athena.md) |
| Function on the partition column is harmless | `substr(dt,…)` or casts can defeat pruning. Filter the raw value | [26](26-Amazon-Athena.md) |
| New data returns zero rows → permissions | Partitions aren't registered. Register them or use projection | [26](26-Amazon-Athena.md) |
| Partition projection works for Spectrum/EMR too | **Athena-only.** Other engines need catalog partitions | [26](26-Amazon-Athena.md) |
| Budgets can cancel a runaway query | Only a **workgroup per-query data usage control** cancels | [26](26-Amazon-Athena.md) |
| One 20 GB `.csv.gz` → add workers | GZIP isn't splittable. Split files or convert to Parquet | [03](03-Data-Formats-Compression.md) |
| Renamed Parquet column → data corruption | Parquet reads by name. Old files lack the new name → view or Iceberg | [30](30-Data-Modeling-Schema-Evolution-Lineage.md) |
| Iceberg `DELETE` satisfies GDPR erasure | Old snapshots still hold the bytes. Delete → compact → expire snapshots → remove orphans | [04](04-Open-Table-Formats-S3-Tables.md) |
| Firehose/Athena writers maintain Iceberg | They don't compact or expire. Add table optimizers / `OPTIMIZE` + `VACUUM`, or S3 Tables | [04](04-Open-Table-Formats-S3-Tables.md) |
| "Upsert" means Hudi | Iceberg `MERGE INTO` covers most cases. Hudi only for record key + incremental-query signals | [04](04-Open-Table-Formats-S3-Tables.md) |

### Redshift

| THE trap | The truth | Guide |
|---|---|---|
| DISTKEY on the date you filter by | Filter column → **sort key**. Join column → **dist key** | [23](23-Redshift-Architecture-Table-Design.md) |
| Primary keys stop duplicates | PK/UNIQUE/FK are **informational**; only NOT NULL is enforced | [23](23-Redshift-Architecture-Table-Design.md) |
| Storage full, so add DC2 nodes | Move to **RA3 managed storage**; DC2 is deprecated | [23](23-Redshift-Architecture-Table-Design.md) |
| Ten parallel COPYs load faster | They serialize. **One COPY** over a prefix/manifest | [24](24-Redshift-Loading-Integration-Sharing.md) |
| UNLOAD + COPY to share with another team | Stale copies. **Data sharing** is live, no copy | [24](24-Redshift-Loading-Integration-Sharing.md) |
| Concurrency scaling speeds up one slow query | It fixes **queueing**. One slow query needs design or a resize | [25](25-Redshift-Performance-Operations-Security.md) |
| Multi-AZ covers a Region outage | AZ only. Region DR = **cross-Region snapshot copy** | [25](25-Redshift-Performance-Operations-Security.md) |
| A view per region/role to hide data | **RLS + DDM policies** on roles, least overhead | [25](25-Redshift-Performance-Operations-Security.md) |
| STL tables for a year of audit history | They keep days. **Enable audit logging** to S3/CloudWatch | [25](25-Redshift-Performance-Operations-Security.md) |

### Stores

| THE trap | The truth | Guide |
|---|---|---|
| DynamoDB throttles → raise table capacity | Hot partition key (per-partition ceiling) or an under-provisioned **GSI** | [27](27-DynamoDB.md) |
| Filter expressions save RCUs | Applied after the read. Fix keys or add an index | [27](27-DynamoDB.md) |
| TTL deletes at an exact time | Deletes within days. Exact timing → scheduled job | [27](27-DynamoDB.md) |
| Scan DynamoDB for analytics | **Export to S3** (no RCUs) + Athena, or zero-ETL | [27](27-DynamoDB.md) |
| Deep Archive meets a 4-hour restore | Never under **12 h**. Use Glacier Flexible (or Instant Retrieval) | [05](05-S3-Data-Lake-Storage.md) |
| Lifecycle expiration shrinks a versioned bucket | Adds delete markers. Add **NoncurrentVersionExpiration** | [05](05-S3-Data-Lake-Storage.md) |
| Changing default encryption re-encrypts old objects | New writes only. **Inventory → Batch Operations copy** | [05](05-S3-Data-Lake-Storage.md) |
| Replication is backup | It replicates deletes and corruption. Use PITR, versioning, AWS Backup | [31](31-Data-Lifecycle-Retention-Resiliency.md) |
| Lambda connection storm → bigger DB | **RDS Proxy** | [28](28-RDS-Aurora-Purpose-Built-DBs.md) |

### Orchestration

| THE trap | The truth | Guide |
|---|---|---|
| Express is cheaper, so use it | No `.sync`, no callbacks, no Distributed Map, **5-min cap** | [20](20-Step-Functions.md) |
| Redshift Data API has `.sync` | It doesn't (nor do crawlers). SDK call + Wait + Describe loop | [20](20-Step-Functions.md) |
| `States.DataLimitExceeded` → Express or bigger instance | **256 KiB** payload limit. Pass S3 URIs | [20](20-Step-Functions.md) |
| S3 notification → Step Functions, filtered by size | S3 can't target it or filter by size. **S3 → EventBridge** | [22](22-EventBridge-SNS-SQS.md) |
| SNS alone gives durable delivery | Add **SNS → SQS** so messages wait for the consumer | [22](22-EventBridge-SNS-SQS.md) |
| Lambda writes back to its trigger bucket | Infinite loop. Separate bucket or prefix | [17](17-Lambda-for-Data-Pipelines.md) |

### Security & governance

| THE trap | The truth | Guide |
|---|---|---|
| SCP allows `s3:*`, so the job works | SCPs **never grant**. The role still needs an identity policy | [37](37-IAM-for-Data-Engineers.md) |
| `s3:ListBucket` on `bucket/*` | Bucket-level action: **bucket ARN** + `s3:prefix` | [37](37-IAM-for-Data-Engineers.md) |
| IAM resource tags drive LF access | **LF-Tags** are a separate system. Use LF-TBAC | [40](40-Lake-Formation.md) |
| LF grants applied, but IAM users still see all | `IAMAllowedPrincipals` holds Super. Revoke it and clear defaults | [40](40-Lake-Formation.md) |
| Shared table visible in LF, so Athena sees it | Consumer needs a **resource link** | [40](40-Lake-Formation.md) |
| Macie scans RDS/Redshift | **S3 only.** Use Glue Detect PII in the job | [42](42-Privacy-PII-Masking-Sovereignty.md) |
| Hash when originals must be recoverable | Hashes are one-way. **Tokenize or encrypt** | [42](42-Privacy-PII-Masking-Sovereignty.md) |
| DDM or LF filters "remove" PII | They hide at read time. Remove or mask at **ingestion** | [42](42-Privacy-PII-Masking-Sovereignty.md) |
| Region-deny SCP blocks CRR/snapshot copy | Deny the **replication/copy actions** themselves | [42](42-Privacy-PII-Masking-Sovereignty.md) |
| Default CloudTrail shows who deleted objects | S3 object deletes are **data events**, off by default | [43](43-Audit-Logging-CloudTrail-Config.md) |
| Versioning or KMS proves logs weren't altered | **Log file integrity validation** proves it | [43](43-Audit-Logging-CloudTrail-Config.md) |
| Object Lock governance stops admins | Only **compliance mode** stops everyone, root included | [05](05-S3-Data-Lake-Storage.md) |
| Share data encrypted with an `aws/…` key cross-account | AWS managed keys can't be shared. Re-encrypt with a **customer managed key** | [39](39-Encryption-Key-Management.md) |
| Flip encryption on for a running RDS instance | No switch. **Snapshot → encrypted copy → restore** (Redshift, by contrast, modifies in place) | [39](39-Encryption-Key-Management.md) |
| Parameter Store rotates DB passwords | No native rotation. **Secrets Manager** | [39](39-Encryption-Key-Management.md) |

### Ops & cost

| THE trap | The truth | Guide |
|---|---|---|
| Lambda throttles → more memory | Throttles are **concurrency**. Memory fixes duration | [17](17-Lambda-for-Data-Pipelines.md) |
| Provisioned concurrency caps a function | It pre-warms. **Reserved concurrency** caps | [17](17-Lambda-for-Data-Pipelines.md) |
| Poll `GetJobRuns` to alert on failure | **EventBridge rule → SNS** | [32](32-Monitoring-Logging-Troubleshooting.md) |
| Export tasks for continuous log delivery | Batch only. **Subscription filter → Firehose** | [32](32-Monitoring-Logging-Troubleshooting.md) |
| Tag resources and costs appear per team | **Activate** the cost allocation tag first; not retroactive | [44](44-Cost-Optimization.md) |
| Private jobs reach S3 through NAT | Pay per GB. **Gateway endpoints are free** | [44](44-Cost-Optimization.md) |
| Custom code on EC2 "for flexibility" | Managed features win on *least operational overhead* | [45](45-Service-Selection-Decision-Guide.md) |

## Things that changed — don't be fooled by old prep material

Question pools lag AWS by months or years. When a stem only makes sense with an old number, answer the question as written. When options conflict, prefer the modern fact unless the stem clearly relies on the old one.

| Change (current fact) | Legacy value you'll still see | How the exam may frame it |
|---|---|---|
| Kinesis Data Streams records up to **10 MiB** (Oct 2025, opt-in per stream) | **1 MB** max record | *"Records up to 5 MB → MSK"* was the classic answer. Choose MSK today for **Kafka compatibility**. If the stem states a 1 MB KDS limit, go with it |
| SQS max message **1 MiB** (Aug 2025) | **256 KB** | *"Payload 700 KB"* → SQS directly now works; SNS/EventBridge are **still 256 KB**. S3 pointer pattern stays valid for larger payloads |
| Lambda async payload **1 MB** | **256 KB** | Rarely decisive; don't eliminate async Lambda on the old limit |
| Redshift default isolation **SNAPSHOT** (new clusters/Serverless) | **SERIALIZABLE** | Error **1023** still means serializable violation. Know both |
| Redshift encrypted by default, private, `require_ssl` (Jan 2025); existing cluster can be **modified** to KMS | "Choose encryption at launch; unload/reload to encrypt" | For Redshift, modify in place. That's still **not** true for RDS/Aurora (snapshot → encrypted copy → restore) |
| S3 max object **50 TB** (Dec 2025) | **5 TB** | Single PUT is still **5 GB**, multipart still recommended |
| Object Lock can be enabled on **existing** buckets | "New buckets only" | Both answers could appear. Compliance vs governance mode is what really gets tested |
| Athena capacity reservations **4-DPU minimum** (Feb 2026) | **24 DPUs** | Reservations are now plausible for smaller teams needing predictable concurrency |
| Lake Formation **governed tables discontinued** (Dec 31, 2024) | "Governed tables for ACID on S3" | Pure distractor. ACID on the lake → **Iceberg / S3 Tables** |
| **KDA for SQL** retired (no new apps Oct 2025, stopped Jan 2026) | "Kinesis Data Analytics SQL application" | Distractor. Stateful stream SQL → **Managed Flink** (Studio notebooks for interactive SQL) |
| **Glue 6.0** GA Aug 2026 (Spark 4.1.1, ~30% cheaper, ANSI SQL on); **5.1** is the default version | Glue 2.0/3.0/4.0 in older questions (0.9/1.0/2.0 EOL Apr 2026) | Version numbers are seldom decisive. Know Glue 5.0+ for LF fine-grained access and **480-min** default timeout |
| Firehose buffer down to **0 s** | **60 s** minimum | Firehose is still the *"near real-time"* answer, not sub-second |
| Glue Ray, S3 Select, S3 Object Lambda, Snowball Edge, Kendra, Q Business, CloudTrail Lake → maintenance / closed to new customers | Presented as active | Concept may still be tested (CloudTrail Lake and Kendra remain in scope). S3 Select → Athena. Data Pipeline is out of scope → Step Functions/MWAA |
| QuickSight → **Amazon Quick** (Quick Sight); Kinesis Data Firehose → **Amazon Data Firehose**; DataZone → **SageMaker Catalog** | Old names | Same products. Don't let a rename make you doubt a correct option |
| Redshift zero-ETL sources now include RDS Oracle, DynamoDB, SaaS, self-managed DBs; Redshift can **write Iceberg** | Aurora-only zero-ETL; Redshift reads lake only | Zero-ETL wins more "least effort replication" questions than before |

## Must-know combos

These reference pipelines show up in scenario after scenario. If a stem describes one end, you can usually predict the other.

- **Clickstream to lake, no code:** producers → **Firehose** (Lambda transform, format conversion to Parquet, dynamic partitioning) → S3 → Glue Catalog → Athena.
- **Replayable stream with many readers:** producers → **KDS on-demand** → Flink / Lambda consumers (EFO) + Firehose leg to S3 for the archive.
- **Stateful real-time analytics:** KDS or MSK → **Managed Flink** (event time, watermarks, checkpoints) → OpenSearch / DynamoDB / S3.
- **Database to warehouse, least effort:** Aurora / RDS / DynamoDB → **zero-ETL** → Redshift. Heterogeneous or on-prem → **DMS full load + CDC**.
- **CDC to lakehouse:** DMS → S3 (Op column + timestamp) → Glue job **`MERGE INTO` Iceberg** → Athena / Redshift.
- **Event-driven batch:** S3 upload → **EventBridge** rule → **Step Functions** (Glue `.sync` → crawler polling loop or catalog update → Athena/Redshift Data API step) → SNS on failure.
- **Nightly lake ETL:** EventBridge Scheduler → Glue (bookmarks, Flex if not urgent, Glue Data Quality with quarantine) → curated Parquet/Iceberg → Redshift Spectrum or COPY.
- **Warehouse archive tier:** Redshift **UNLOAD Parquet PARTITION BY** → S3 (lifecycle) → Spectrum external tables.
- **Streaming into warehouse:** KDS / MSK → **Redshift streaming ingestion** materialized view (private, no Firehose networking issue).
- **Governed lake:** register S3 in **Lake Formation** → LF-Tags + data filters → Athena / EMR runtime roles / Glue 5.0+ → cross-account via **RAM + resource links**.
- **PII pipeline:** **Macie** finds PII in S3 → EventBridge → Lambda applies **LF-Tags**; Glue **Detect PII** masks during ETL; Redshift **DDM** for per-role display.
- **Self-service data marketplace:** producers publish to **SageMaker Catalog** → consumers subscribe → approval triggers LF / Redshift grants automatically.
- **Logs to analytics:** CloudWatch Logs **subscription filter → Firehose** → S3 → Athena with partition projection. Ad hoc on log groups → Logs Insights.
- **Audit trail:** **organization multi-Region trail** + data events → log-archive account bucket (Object Lock) + **integrity validation**; Config for state and compliance.
- **RAG over documents:** S3 docs → **Bedrock Knowledge Bases** (chunking, embeddings) → OpenSearch Serverless / S3 Vectors / Aurora pgvector → `RetrieveAndGenerate`.
- **Offline LLM enrichment:** S3 JSONL → **Bedrock batch inference** → S3 → Glue / Athena. Orchestrated fan-out → Step Functions Distributed Map.

## Exam technique

### Read the qualifier first

The exam format: **65 questions** (50 scored + 15 unscored, unmarked), **130 minutes**, pass at **720/1,000**, compensatory scoring (see [Guide 01](01-Exam-Blueprint.md)). Most questions have two workable answers, and the qualifier picks between them.

| Qualifier | What it rewards | What it punishes |
|---|---|---|
| *"Least operational overhead"* / *"least effort"* / *"without managing servers"* | Managed or serverless features, zero-ETL, built-in integrations | Custom Lambda glue, EC2, self-managed clusters, polling scripts |
| *"Most cost-effective"* | Right-sizing, Flex, Spot, lifecycle, serverless for spiky loads, reservations for steady loads | Over-provisioned always-on clusters, premium tiers |
| *"Lowest latency"* / *"real-time"* | KDS/MSK + Lambda or Flink, DAX, MemoryDB, streaming ingestion | Firehose buffering, batch jobs, crawlers |
| *"Near real-time"* | Firehose, micro-batch | Over-engineered Flink |
| *"Minimal code changes"* | Same-API services (MSK for Kafka, Aurora for MySQL/Postgres) | Rewrites to a different SDK |
| *"Most secure"* / *"least privilege"* | Scoped roles, customer managed keys, endpoints, LF fine-grained grants | Broad managed policies, public access |

### Eliminate in two passes

1. **Hard constraints.** Cross out options that break a limit or a capability: Lambda past 15 minutes, Express waiting on `.sync`, Firehose delivering to DynamoDB, Macie scanning Redshift, projection for Spectrum, Deep Archive within 4 hours.
2. **Qualifier fit.** Of what's left, keep the one that matches the qualifier. When two options still fit, prefer the one with **fewer moving parts** and **no custom code**.

Other patterns worth knowing:
- The **most elaborate option** is often the misdirection. Four services chained with custom code rarely beat one managed feature.
- **Retired or renamed services** (Data Pipeline, governed tables, KDA for SQL, S3 Select) are almost always wrong in new designs.
- **"Add capacity"** answers (more shards, DPUs, nodes, RCUs) are usually wrong when the stem says *"even though utilization is low"*. Look for skew, a hot key, or a missing endpoint.
- **"It hangs, then times out"** points to networking. **"AccessDenied"** points to IAM, KMS or Lake Formation.

### Pacing

- **130 min ÷ 65 = 2 min per question.** First pass at ~1.5 min each, which banks ~30 minutes.
- If you can't decide in 2 minutes, pick your best guess, **flag it**, and move on. There's no penalty for guessing, so never leave a blank.
- Checkpoints: question 22 by ~45 min, question 44 by ~90 min.
- Use the banked time on flagged questions only. Change an answer only if you find a concrete reason (a missed qualifier or limit), not a vague feeling.

### Multiple-response questions

- The stem says how many to pick (*"Choose two"*). Every selected option must be correct. There is no partial credit.
- Judge each option **on its own** as true or false for the scenario. Don't pick two that overlap.
- Correct pairs are often **complementary halves** of one solution: a network fix plus an IAM fix, a producer change plus a consumer change, or detection plus enforcement.
- When one option is a superset of another, the superset is usually the better pick. Two options that contradict each other can't both be right.

## Question patterns

> *"A Kinesis stream shows write throttling on one shard while overall utilization is 15%. What should the engineer do FIRST?"* → **Change the partition key to a higher-cardinality value** (the crack: *"one shard"* + low overall utilization = hot key; resharding or on-demand alone keeps routing the same key to one shard).

> *"A company must stream JSON events to S3 as Parquet, partitioned by customer_id, with the LEAST operational overhead."* → **Firehose with record format conversion and dynamic partitioning (new stream)** (a Glue streaming job works but adds code and capacity to manage; KDS alone doesn't write to S3 in exam terms).

> *"A Glue job with bookmarks enabled reprocesses all S3 files every night."* → **Add `transformation_ctx` to the source and call `job.commit()`** (the crack: bookmarks rely on the GlueContext API; resetting the bookmark or adding workers doesn't help).

> *"Analysts add LIMIT 100 to Athena queries on unpartitioned CSV to reduce cost. Costs don't change. What reduces cost the MOST?"* → **Convert to partitioned, compressed Parquet** (LIMIT doesn't reduce bytes scanned on a full scan; a capacity reservation changes the billing model, not the scan).

> *"A Redshift fact table is KEY-distributed on sale_date. Queries for yesterday's sales are slow."* → **Redistribute on the join column and make sale_date the leading sort key** (all of yesterday lands on one slice; concurrency scaling doesn't fix a single slow query).

> *"Dashboards run hundreds of concurrent queries at 9 AM and wait in queue. Individual queries are fast."* → **Enable concurrency scaling** (the crack: queue time, not execution time; elastic resize is the answer when one query is slow).

> *"A DynamoDB table has ample provisioned WCU but writes throttle after a new GSI was added."* → **Increase the GSI's write capacity (or switch to on-demand)** (base-table writes must also be written to the index; raising table WCU is the flourish).

> *"A workflow must start a Glue job, wait for it, run a Redshift stored procedure, then notify on failure — serverless, least effort."* → **Step Functions Standard: Glue `.sync`, Redshift Data API with a Wait/Describe loop, Catch → SNS** (Glue workflows can't call Redshift; Express can't `.sync`).

> *"Files over 1 MB uploaded to a prefix must start a state machine."* → **S3 → EventBridge rule with a numeric filter on object size → Step Functions** (S3 event notifications can't target Step Functions or filter by size).

> *"Analysts with broad IAM Glue/S3 permissions still see restricted columns after Lake Formation column grants were added."* → **Revoke `IAMAllowedPrincipals` and clear the default IAM-only settings** (the LF grant is fine; IAM is still in control).

> *"A company must find PII across 2,000 S3 buckets continuously with the least effort and restrict access to tagged tables automatically."* → **Macie automated discovery → EventBridge → Lambda applying LF-Tags** (DataBrew profiles one dataset; Glue Detect PII runs inside a job, not across buckets).

> *"Auditors need to know who deleted objects from a bucket last week. Only the default CloudTrail configuration exists."* → **It can't be determined from CloudTrail. Enable S3 data events (or server access logs) now for the future** (Event history shows management events only; this is the trap that tests whether you'd claim otherwise).

> *"An existing unencrypted Redshift cluster must be encrypted with a customer managed KMS key with minimal effort."* → **Modify the cluster to use KMS encryption** (in-place migration; unload/reload is legacy thinking, and snapshot-restore is the RDS pattern).

> *"A team proposes AWS Lake Formation governed tables for ACID transactions on S3."* → **Use Apache Iceberg tables (or S3 Tables) instead** (governed tables were discontinued; old prep material still recommends them).

> *"A 900 KB message must be buffered for asynchronous processing. Which service accepts it without an S3 pointer?"* → **Amazon SQS (1 MiB limit)** (SNS and EventBridge remain 256 KB; older material says SQS is 256 KB too).

> *"A nightly Spark ETL of 5 TB has no deadline; minimize cost and cluster management."* → **Glue with Flex execution** (EMR on EC2 means cluster admin; Lambda can't run 5 TB for 15 minutes; Flex is right because nothing in the stem is time-critical).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| *"Even though capacity is fine"* | Hot key / skew / GSI, not more capacity |
| *"Hangs then times out"* in a VPC | Missing S3 gateway / interface endpoint or NAT |
| *"AccessDenied"* | IAM + resource policy + KMS key policy + LF grant |
| Replay, many consumers | KDS (not Firehose, not SQS) |
| Kafka, minimal code change | MSK |
| Near real-time to S3, no code | Firehose |
| Windows, event time, late data | Managed Flink |
| DB → Redshift, least effort | Zero-ETL |
| CDC to Iceberg | DMS → S3 → Glue MERGE |
| Bookmarks reprocess everything | `transformation_ctx` + `job.commit()` |
| Must finish by a deadline | Not Glue Flex |
| LIMIT to cut Athena cost | Doesn't work |
| Partition projection | Athena only |
| Filter column in Redshift | Sort key (join column = dist key) |
| Queueing at peak | Concurrency scaling |
| Live cross-account warehouse data | Redshift data sharing |
| Wait for a job in a workflow | Step Functions Standard `.sync` |
| Redshift Data API / crawler step | Polling loop, no `.sync` |
| Payload > 256 KiB between states | S3 URI |
| S3 event → Step Functions / size filter | S3 → EventBridge |
| Tag-based access, thousands of tables | LF-Tags |
| LF grants ignored | Revoke `IAMAllowedPrincipals` |
| PII across S3 | Macie |
| Remove PII from storage | Mask at ingestion (not DDM) |
| Who deleted objects | S3 data events (enable first) |
| Prove log integrity | CloudTrail log file validation |
| WORM, even root | Object Lock compliance |
| Throttled Lambda | Concurrency, not memory |
| NAT bill for S3 traffic | Gateway endpoint (free) |
| KDS max record | 10 MiB now (legacy 1 MB) |
| SQS max message | 1 MiB now (legacy 256 KB) |
| Redshift default isolation | SNAPSHOT now (legacy SERIALIZABLE) |
| Governed tables / KDA for SQL / Data Pipeline | Distractors |
| 65 Q / 130 min | ~2 min each, flag and move on |
| Multiple response | Judge each option alone; all must be right |

Close the loop by re-reading the domain weights and skill checklist in [Guide 01 — Exam Blueprint](01-Exam-Blueprint.md), then drill the cheat sheets in [`../cheat-sheets/`](../cheat-sheets/keyword-to-service.md).
