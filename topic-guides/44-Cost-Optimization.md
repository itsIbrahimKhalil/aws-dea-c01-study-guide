# 44 · Cost Optimization — every byte scanned, every idle hour, has a price tag

> **Exam map:** D1 · Task 1.2 — D2 · Task 2.3 — D3 · Task 3.2 · **Skills:** 1.2.4, 2.3.2, 3.2.5 (+ AWS Budgets & AWS Cost Explorer from the in-scope Cloud Financial Management services) · **Weight:** 🔥🔥 Medium · **Read time:** ~18 min

## The idea

Cost questions on DEA-C01 hide inside architecture questions. The qualifier *"MOST cost-effective"* appears all over the exam, and the right answer is usually the design that **stops paying for something you don't use**: bytes you didn't need to scan, servers idling overnight, premium capacity for work that could wait, or network paths that charge per gigabyte for no reason.

Think of it as **running a household's bills**. **AWS Budgets** is the alarm on your credit card ("tell me when groceries pass $500"). **Cost Explorer** is the bank-statement app with charts and a forecast. The **Cost and Usage Report** is the shoebox of itemized receipts you can load into a spreadsheet (Athena). **Cost allocation tags** are the name labels on each family member's grocery bags. The savings moves are all familiar: **turn off lights in empty rooms** (auto-terminate idle clusters, auto-pause), **buy in bulk** (Savings Plans, reservations), **use off-peak rates** (Spot, Glue Flex), **cook smaller portions** (partitioned Parquet so Athena scans less), and **stop paying the toll road** (VPC gateway endpoints instead of NAT).

This guide covers the monitoring tools, the commitment discounts, a per-service lever table, and the provisioned-versus-serverless trade-off behind skill 3.2.5.

## The Cloud Financial Management toolbox

| Tool | What it does | Exam signal |
|---|---|---|
| **AWS Budgets** | Set **cost, usage, Savings Plans (utilization/coverage) or Reservation (utilization/coverage)** budgets. Alert on **actual or forecasted** spend crossing thresholds via **email, Amazon SNS**, or Amazon Q Developer in chat applications (formerly AWS Chatbot). Filter by service, account, tag or Region | *"Notify when projected monthly spend exceeds X," "alert the team in Slack"* |
| **Budget actions** | When a threshold is hit, **automatically (or after approval)** apply an **IAM policy**, apply an **SCP** (Organizations), or **stop specific EC2/RDS instances** | *"Automatically prevent further provisioning when the budget is exceeded"* |
| **AWS Cost Explorer** | Interactive charts of past spend, **filter and group by service, linked account, Region, usage type, cost allocation tag**. Daily/monthly (and opt-in hourly) granularity, **forecasts**, **Savings Plans/RI recommendations** and utilization/coverage reports, **rightsizing recommendations** | *"Identify which service/team drove last month's increase," "forecast next quarter"* |
| **AWS Cost Anomaly Detection** | ML-based **monitors** (by service, account, cost category or tag) that flag unusual spend. Alert subscriptions: **immediate (SNS)** or daily/weekly email summaries | *"Detect unexpected spend spikes without setting static thresholds"* |
| **Cost and Usage Report (CUR 2.0) via AWS Data Exports** | The most granular billing data (line items, resource IDs, tags, pricing), delivered to **S3** (Parquet), queried with **Athena**, visualized in **Amazon Quick (Quick Sight)** dashboards. Data Exports can also produce a **FOCUS**-format export | *"Most detailed, resource-level cost data," "custom cost analysis with SQL"* |
| **Cost allocation tags** | **User-defined** (e.g., `team`, `project`) or **AWS-generated** tags. They **must be activated** in the Billing console (management/payer account) before they appear in Cost Explorer/CUR. They show up only after activation, typically within **24 hours**, and are **not retroactive** unless you request a backfill | *"Charge back data-lake costs per business unit"* |
| AWS Compute Optimizer / Cost Optimization Hub | Rightsizing (EC2, EBS, Lambda, ECS on Fargate, RDS) and a consolidated savings-recommendation view | *"Right-size over-provisioned instances"* |

**THE trap:** "Tag all resources with `team` and the cost report will show spend per team." Tags appear in billing only **after the tag key is activated as a cost allocation tag**, and only for usage from then on. Enforce consistent keys with **AWS Organizations tag policies**, and group spend with **AWS Cost Categories**.

**THE trap:** using **Cost Explorer** when the question asks for **per-resource, line-item, SQL-queryable** detail or joining costs with other data. That's **CUR 2.0 (Data Exports) → S3 → Athena**. Cost Explorer is the interactive, summarized view. Budgets *alert*. Anomaly Detection *finds surprises*. Only budget actions *enforce*.

## Commitment discounts: buying in bulk

| Commitment | Covers | Notes |
|---|---|---|
| **Compute Savings Plans** | **EC2** (any family, size, Region, OS), **AWS Fargate**, **AWS Lambda** | Most flexible; $/hour commitment for 1 or 3 years. Covers the **EC2 instances under EMR on EC2** and ECS/EKS on Fargate |
| **EC2 Instance Savings Plans** | EC2 in one instance family + Region | Deeper discount, less flexible |
| **Database Savings Plans** | **Aurora, RDS, DynamoDB, ElastiCache, DocumentDB, Neptune, Keyspaces, Timestream, AWS DMS** | Newer plan type. Commitment spans database services |
| **SageMaker Savings Plans** | SageMaker AI ML instances | Out of scope for depth |
| **Reserved capacity / reserved nodes** | **Redshift reserved nodes** (provisioned), **OpenSearch Service Reserved Instances**, **RDS RIs**, **ElastiCache/MemoryDB reserved nodes**, **DynamoDB reserved capacity** (provisioned mode) | 1- or 3-year terms for **steady, predictable 24×7** baselines |

Glue, Athena (SQL), Firehose and Step Functions have **no Savings Plan coverage**. You optimize them through how you use them (below), or with **Athena capacity reservations** for steady heavy query loads.

**Rule:** commit only to the **steady baseline**. Cover the spiky remainder with on-demand, Spot or serverless. *"Workload runs 24×7 at stable size for 3 years"* → reservations or Savings Plans. *"Unpredictable, intermittent"* → serverless or on-demand, no commitment.

## Per-service cost levers

### Amazon S3 (skill 2.3.2)
- **Storage classes + Lifecycle rules**: transition cold data (Standard → Standard-IA → Glacier Instant/Flexible → Deep Archive) and **expire** what you no longer need. Minimum storage durations: **30 days** (IA), **90** (Glacier Instant/Flexible), **180** (Deep Archive). Deleting early still bills the minimum.
- **Intelligent-Tiering** only for **unknown or changing** access patterns. It charges a per-object monitoring fee, and objects under 128 KB aren't tiered. Known patterns → lifecycle rules.
- **Abort incomplete multipart uploads** with a lifecycle rule. Orphaned parts are billed indefinitely.
- **Store analytics data as compressed columnar** (Parquet/ORC + Snappy/ZSTD). It's smaller to store **and** cheaper to scan in Athena, Spectrum and EMR.
- **S3 Storage Lens** shows org-wide usage and activity (find cold buckets, incomplete MPUs, noncurrent versions). Lifecycle-expire **noncurrent versions** in versioned buckets.
- **Requester Pays** buckets make the consumer pay request and transfer costs when sharing large datasets.
- Deep mechanics: [Guide 05](05-S3-Data-Lake-Storage.md), [Guide 31](31-Data-Lifecycle-Retention-Resiliency.md).

### Amazon Athena
- Pricing: **$5 per TB scanned**, **10 MB minimum** per query. DDL and failed queries aren't charged. **Every byte avoided is money.**
- **Partition** by the columns you filter on, and use **partition projection** for high-cardinality or fast-growing partitions. Use **Parquet/ORC** (column pruning plus predicate pushdown) and compression. Converting CSV to partitioned Parquet commonly cuts scanned bytes by **90%+**.
- **CTAS / INSERT INTO** to pre-aggregate or convert hot datasets, and **UNLOAD** results to Parquet.
- **Query result reuse** (up to **7 days** max age) returns cached results for identical queries at no scan cost.
- **Workgroups:** a **per-query data usage control** **cancels** any query that scans more than the limit. **Workgroup-wide data usage controls** raise **CloudWatch alarms / SNS** when total scanned per hour/day/etc. crosses a threshold (they alert, they don't cancel). Tag workgroups for chargeback.
- **Capacity reservations** for steady, heavy, concurrent workloads: **$0.30 per DPU-hour**, **4-DPU minimum**, 1-minute minimum. You pay for provisioned capacity instead of per-TB.
- **THE trap:** *"Add LIMIT 10 to reduce cost."* On an unpartitioned full scan, **LIMIT doesn't reduce bytes scanned**, because Athena still reads the columns across the files. Only **partition pruning, columnar formats, selecting fewer columns and compression** cut the bill. `SELECT *` on Parquet also throws away the columnar advantage.
- Details in [Guide 26](26-Amazon-Athena.md).

### AWS Glue
- **Right-size the worker type and count.** Enable **Auto Scaling** (Glue 3.0+) so workers are added and removed with the stages. Billing is per second with a **1-minute minimum**.
- **Flex execution** for non-urgent jobs (nightly batch, tests, backfills): **$0.29 vs $0.44 per DPU-hour** (~34% cheaper). Available on Glue 3.0+ with G.1X/G.2X only. Start time and runtime may vary.
- **Glue 6.0** (Aug 2026) is **~30% cheaper per DPU-hour than Glue 5.1**. Upgrading is now a cost lever too (newer than the exam pool).
- **Job bookmarks** process only new data instead of reprocessing everything. **Pushdown predicates** read only needed partitions. `groupFiles` handles small-file overhead.
- **Python shell jobs** (**0.0625** or 1 DPU) for small, non-distributed tasks (an API call, a small file, a Redshift SQL trigger) instead of a Spark job with a minimum of 2 workers.
- **Interactive sessions and notebooks:** set a short **idle timeout** so abandoned sessions stop billing.
- **Crawlers:** avoid frequent full recrawls. Use **incremental / S3 event-based crawls**, crawl only new folders, or skip crawlers entirely with partition projection or explicit `ALTER TABLE ADD PARTITION` from the ETL job.
- Details in [Guide 12](12-AWS-Glue-ETL.md) and [Guide 13](13-Glue-Data-Catalog-Crawlers.md).

### Amazon EMR
- **Spot for task nodes** (and for whole clusters only when jobs are interruptible). **Instance fleets** with many instance types and price-capacity-optimized allocation.
- **Graviton** instances (better price-performance). **Managed scaling** with an On-Demand cap (`MaximumOnDemandCapacityUnits`) so burst capacity comes from Spot.
- **Transient clusters** (terminate after steps) plus an **auto-termination idle timeout**. Keep data in S3 so clusters are disposable.
- **EMR Serverless** for intermittent jobs (no idle cluster; watch pre-initialized capacity, since warm workers bill while idle). **Compute Savings Plans / RIs** for the EC2 baseline of long-running clusters.
- Details in [Guide 15](15-Amazon-EMR.md).

### Amazon Redshift
- **Redshift Serverless** bills **RPU-hours per second (60-second minimum)** only while queries run. **Base capacity 4–512 RPU** (default 128), a **max RPU** ceiling, and **usage limits** (RPU-hours per day/week/month with actions: log, alert, or **turn off user queries**). Best for **spiky, intermittent, unpredictable** analytics.
- **Provisioned RA3 + reserved nodes** for **steady 24×7** load (1- or 3-year terms). RA3 separates compute from managed storage, so you size nodes for compute.
- **Pause and resume** provisioned clusters on a schedule (dev/test, business-hours-only). While paused you pay **storage only**.
- **Concurrency scaling:** each cluster accrues **1 hour of free credits per 24 hours** running, up to **30 hours**. Beyond that, per-second billing. Set a limit in WLM (usage limits) to cap spend.
- **Move cold data to S3** with **UNLOAD** (Parquet) and query it through **Spectrum** (billed per TB scanned) instead of paying for warehouse storage. Use **materialized views** so repeated heavy aggregations aren't recomputed.
- Details in [Guide 23](23-Redshift-Architecture-Table-Design.md) and [Guide 25](25-Redshift-Performance-Operations-Security.md).

### Streaming: Kinesis, Firehose, MSK
- **Kinesis Data Streams provisioned** (per **shard-hour** + PUT payload units) is cheapest for **steady, predictable** throughput when shards are right-sized. **On-demand** (per stream-hour + per GB in/out) suits **unknown or spiky** traffic and needs no shard math, but costs more at a high steady rate. Switch modes as the pattern matures.
- **KPL aggregation / PutRecords batching** packs small records into fewer, fuller payload units and fewer API calls.
- **Enhanced fan-out** costs extra (per consumer-shard-hour + per GB). Use it only when consumers need dedicated 2 MB/s/shard throughput or low latency. Otherwise shared-throughput polling is cheaper.
- **Reduce retention** to what replay really needs. Extended retention beyond 24 h is billed.
- **Firehose** (pay per GB ingested, no shards) vs **KDS + Lambda** writing to S3: for *"deliver to S3/Redshift/OpenSearch with no custom processing,"* Firehose is usually cheaper and **far less operational work**.
- **MSK Serverless** (pay per cluster-hour, partition-hour and GB) for variable or unknown Kafka loads. **Provisioned/Express brokers** for steady high throughput. **Tiered storage** for cheap long retention.
- Details in [Guide 06](06-Kinesis-Data-Streams.md), [07](07-Amazon-Data-Firehose.md), [08](08-Amazon-MSK-Kafka.md).

### AWS Lambda
- Billed per request + **GB-seconds (1 ms granularity)**. **Memory tuning** also buys CPU, so more memory can finish faster and cost *less*. Measure with Lambda Power Tuning.
- **arm64 (Graviton)** offers up to 34% better price-performance for most code.
- **Batching:** larger **batch sizes** and a **batching window** on Kinesis/SQS/DynamoDB Streams event source mappings mean fewer invocations.
- **Don't wait inside Lambda.** Polling a Glue job or sleeping bills idle duration. Use **Step Functions** `.sync` integrations or **waitForTaskToken**, or EventBridge events ([Guide 20](20-Step-Functions.md)).
- Details in [Guide 17](17-Lambda-for-Data-Pipelines.md).

### Amazon DynamoDB
- **On-demand** for unpredictable or spiky traffic (and new tables). **Provisioned + auto scaling** for steady, forecastable traffic. Add **reserved capacity** (or Database Savings Plans) for the long-term baseline.
- **TTL deletes are free** (no write capacity consumed). Use TTL to expire sessions and events instead of batch delete jobs.
- **Standard-IA table class** when **storage cost dominates** (large, rarely read tables). Storage is much cheaper, reads and writes cost more.
- **Export to S3** (needs PITR) for analytics. It **doesn't consume read capacity**, unlike a table **Scan** that burns RCUs and throttles production.
- Details in [Guide 27](27-DynamoDB.md).

### AWS Step Functions

| | **Standard** | **Express** |
|---|---|---|
| Billed per | **State transition** ($0.025 per 1,000) | **Requests + duration × memory** |
| Best for | Long-running (up to 1 year), exactly-once orchestration, low-to-moderate volume | **High-volume, short (≤ 5 min)** event processing |

A per-record workflow firing millions of times a day costs far less on **Express**. A nightly 6-hour ETL orchestration fits **Standard**.

### Data transfer and networking
- **THE trap:** private-subnet Glue/EMR/Lambda/ECS jobs reaching S3 or DynamoDB **through a NAT Gateway**, paying NAT **per-GB data processing** on terabytes. **S3 and DynamoDB gateway VPC endpoints are free** and keep traffic private. Interface endpoints (PrivateLink) for other services charge hourly + per GB, usually still cheaper than NAT for heavy traffic.
- Keep compute and data in the **same Region**. Cross-Region transfer and replication are billed per GB.
- **Cross-AZ traffic** is billed in each direction. Heavy chatty clusters (Kafka, EMR shuffle) belong in one AZ where resilience allows, or use rack/AZ-aware consumers.
- Data **into** AWS is free. Data **out to the internet** is billed. Aggregate and compress before exporting, and use CloudFront for repeated public downloads.
- Details in [Guide 38](38-Networking-for-Data-Pipelines.md).

### Amazon OpenSearch Service
- **Hot → UltraWarm → cold** tiers for aging logs (UltraWarm/cold store on S3-backed storage at much lower cost). **Index State Management** automates rollover and migration.
- **Reserved Instances** for steady domains. **OpenSearch Serverless** (billed in **OCUs**, OpenSearch Compute Units) for intermittent or unpredictable workloads without cluster sizing.
- Details in [Guide 29](29-OpenSearch-Service.md).

## Provisioned vs serverless economics (skill 3.2.5)

| Dimension | **Provisioned** (Redshift RA3, EMR on EC2, MSK provisioned, KDS provisioned, DynamoDB provisioned, OpenSearch domains) | **Serverless / on-demand** (Redshift Serverless, EMR Serverless, Glue, Athena, MSK Serverless, KDS on-demand, DynamoDB on-demand, OpenSearch Serverless, Lambda) |
|---|---|---|
| You pay for | Capacity, **used or not** | **Consumption** (RPU-s, DPU-s, TB scanned, GB, requests) |
| Cheapest when | **High, steady utilization (24×7)** | **Spiky, intermittent, unpredictable** or low-volume work |
| Discounts | **Reservations / Savings Plans**, Spot (EMR) | Few commitments (Athena capacity reservations; Database SP for DynamoDB) |
| Scaling | You size it (with auto scaling helpers) | Automatic, near-instant |
| Ops overhead | Higher (capacity planning, patching windows, tuning) | **Lowest** |
| Cold start / limits | Always warm | Possible start latency; per-service quotas; less low-level tuning |
| Risk | Paying for idle | **Runaway usage** → need usage limits, max capacity, workgroup controls |

Decision rule: **predictable and busy → provisioned + commitment. Unpredictable or mostly idle → serverless.** When the question also says *"least operational overhead,"* serverless wins even at similar cost. Always pair serverless with a **guard-rail**: Redshift Serverless usage limits and max RPU, EMR Serverless maximum capacity, Athena per-query limits, Lambda reserved concurrency, and Budgets alerts. More on the trade-off in [Guide 02](02-Data-Engineering-Fundamentals.md).

## Question patterns

> *"Analysts run ad hoc Athena queries on 50 TB of CSV logs in S3, filtering by date. Reduce query cost the MOST."* → **Convert to Parquet with compression, partitioned by date (CTAS or Glue), and query only needed columns** (LIMIT and result caching don't fix full scans; columnar + partitions cut bytes scanned by an order of magnitude)

> *"A company must ensure no single Athena query from the analyst team scans more than 1 TB, and wants an alert when the team's daily scan total exceeds 20 TB."* → **Athena workgroup: per-query data usage control (cancels the query) plus a workgroup-wide data usage alarm with SNS** (per-query controls cancel; workgroup-wide controls alert)

> *"Nightly Glue Spark jobs have no strict completion SLA. Reduce cost with the least effort."* → **Glue Flex execution** (~34% cheaper per DPU-hour; start/runtime may vary; standard class for SLA-bound jobs)

> *"A 24×7 Redshift workload runs at steady high utilization for the next three years."* → **Provisioned RA3 with 3-year reserved nodes** (Serverless pays a premium for elasticity you don't need)

> *"A BI team queries Redshift for 2 hours each morning; the warehouse is idle the rest of the day."* → **Redshift Serverless with a max RPU and usage limits** (per-second RPU billing only while queries run; pause/resume is the provisioned alternative)

> *"Glue jobs in private subnets read 40 TB/month from S3 and the NAT Gateway bill is huge."* → **Add an S3 gateway VPC endpoint (update route tables)** (gateway endpoints are free; NAT charges per GB processed)

> *"Finance needs daily, resource-level cost data joined with internal project metadata, queryable with SQL."* → **CUR 2.0 via AWS Data Exports to S3 (Parquet), queried with Athena, dashboards in Quick Sight** (Cost Explorer is summarized and interactive, not a SQL-joinable dataset)

> *"Leadership wants an email when forecasted monthly data-platform spend will exceed $50,000, and new EC2 launches blocked in dev accounts if it does."* → **AWS Budgets forecasted-cost alert plus a budget action applying an SCP/IAM policy** (Cost Explorer forecasts but doesn't alert or enforce)

> *"Costs must be reported per business unit; resources already carry a `bu` tag, but Cost Explorer can't group by it."* → **Activate `bu` as a cost allocation tag in the Billing console** (tags aren't visible in billing until activated; data accrues from activation)

> *"An unexplained jump in Kinesis and Lambda spend went unnoticed for two weeks; the team wants ML-based detection without static thresholds."* → **Cost Anomaly Detection with immediate SNS alerts** (Budgets needs fixed thresholds)

> *"A Lambda function starts a Glue job and polls every 30 seconds for up to 40 minutes until it finishes."* → **Orchestrate with Step Functions (`glue:startJobRun.sync`) instead of polling in Lambda** (Lambda bills idle wait time and caps at 15 minutes)

> *"A workflow processes millions of short IoT events per day, each through 5 steps taking under a second."* → **Step Functions Express workflows** (Standard charges per state transition; at this volume Express's request + duration pricing is far cheaper)

> *"A 10 TB DynamoDB table is rarely read after 90 days, but items must remain online; storage dominates the bill."* → **Switch to the DynamoDB Standard-IA table class** (or TTL + export to S3 if items can leave the table). A Scan-based archive job would burn read capacity

> *"Traffic to a new Kinesis stream is unknown and spiky; the team doesn't want to manage shards. Months later it's steady at a high rate."* → **Start in on-demand mode, then switch to provisioned mode with right-sized shards** (on-demand is simplest for unknown load; provisioned is cheaper for steady high throughput)

> *"A steady EMR on EC2 baseline plus Lambda and Fargate usage should be discounted with maximum flexibility across instance families and Regions."* → **Compute Savings Plans** (cover EC2, including under EMR, plus Fargate and Lambda; EC2 Instance SPs lock a family and Region)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Alert when spend/forecast crosses threshold | AWS Budgets (email/SNS) |
| Auto-enforce when budget hit | Budget actions (IAM policy, SCP, stop EC2/RDS) |
| Explore/group spend by service or tag, forecast | Cost Explorer |
| Unusual spend, no static threshold | Cost Anomaly Detection |
| Line-item, SQL-queryable cost data | CUR 2.0 / Data Exports → S3 → Athena → Quick Sight |
| Tags not showing in billing | Activate cost allocation tags (not retroactive) |
| EC2 + Fargate + Lambda flexible discount | Compute Savings Plans |
| RDS/Aurora/DynamoDB/DMS commitment | Database Savings Plans (or RIs / reserved capacity) |
| Redshift/OpenSearch/ElastiCache/MemoryDB steady | Reserved nodes / RIs |
| Athena cost | $5/TB scanned, 10 MB minimum |
| Cut Athena scan | Partitions/projection + Parquet/ORC + compression + fewer columns |
| LIMIT on full scan | Doesn't reduce bytes scanned (THE trap) |
| Cap one Athena query | Workgroup per-query data usage limit (cancels) |
| Team-wide Athena scan alert | Workgroup data usage alarm (SNS) |
| Repeated identical queries | Athena query result reuse (≤ 7 days) |
| Steady heavy Athena | Capacity reservations ($0.30/DPU-h, 4-DPU min) |
| Non-urgent Glue job | Flex ($0.29 vs $0.44/DPU-h) |
| Glue variable load | Auto Scaling; right worker type |
| Small non-Spark Glue task | Python shell (0.0625 DPU) |
| Glue 6.0 (Aug 2026) | ~30% lower DPU-hour than 5.1 |
| EMR savings | Spot task nodes, fleets, Graviton, managed scaling, transient + auto-terminate |
| Redshift spiky/intermittent | Serverless (RPU-seconds, 60 s min, usage limits, max RPU) |
| Redshift steady 24×7 | Provisioned RA3 + reserved nodes |
| Redshift business hours only (provisioned) | Pause/resume |
| Concurrency scaling free tier | 1 h credit per 24 h, up to 30 h |
| Cold warehouse data | UNLOAD to S3 + Spectrum |
| Kinesis steady vs spiky | Provisioned (right-sized shards) vs on-demand |
| Many tiny Kinesis records | KPL aggregation / batching |
| Stream to S3, no code, cheap ops | Firehose over KDS + Lambda |
| Lambda cheaper | Tune memory, arm64, batching window, no waiting |
| DynamoDB unpredictable vs steady | On-demand vs provisioned + auto scaling (+ reserved) |
| Free deletes | DynamoDB TTL |
| Storage-heavy, rarely read table | DynamoDB Standard-IA table class |
| Analytics on DynamoDB | Export to S3 (no RCUs), not Scan |
| High-volume short workflows | Step Functions Express |
| S3/DynamoDB from private subnet | Gateway endpoint (free), not NAT |
| Unknown S3 access pattern | Intelligent-Tiering; known → lifecycle rules |
| Orphaned upload parts | Lifecycle: abort incomplete multipart uploads |
| Consumers pay for shared data | S3 Requester Pays |
| Aging logs in OpenSearch | UltraWarm/cold + ISM |

Cost is one axis of every service choice, so the final step is weighing it against latency, operational overhead and governance in [Guide 45 — Service Selection Decision Guide](45-Service-Selection-Decision-Guide.md).
