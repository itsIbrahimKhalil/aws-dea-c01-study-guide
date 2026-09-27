# 29 · Amazon OpenSearch Service — the index at the back of every book

> **Exam map:** D2 · Task 2.1, 2.3 — D3 · Task 3.3 — D4 · Task 4.4 · **Skills:** 2.1.1, 2.3 (lifecycle), 3.3.8, 4.4.4, 2.1.8 · **Weight:** 🔥🔥 Medium · **Read time:** ~16 min

## The idea

Open any thick textbook to the **index at the back**: every important word is listed alphabetically with the pages it appears on. Finding "Kinesis" doesn't mean reading 900 pages; you look up one entry and jump. Now imagine that index **rewriting itself within a second** every time a new page is printed, spread across many librarians so no single one gets overwhelmed, with a wall of live charts showing which words are trending.

That's **Amazon OpenSearch Service**: managed **OpenSearch** (the open-source fork of Elasticsearch, supporting legacy Elasticsearch versions too) plus **OpenSearch Dashboards** (the visualization UI, formerly Kibana). Its core data structure is the **inverted index**, word → documents, which makes **full-text search, relevance ranking and fast aggregations over fresh data** its superpowers. Data engineers meet it in four roles:
1. **Log analytics / observability**: application, VPC, CloudTrail and WAF logs searchable within seconds, with dashboards and alerting.
2. **Full-text search**: product catalogs, documents, fuzzy matching, autocomplete over data synced from DynamoDB or RDS.
3. **Near-real-time dashboards** on streaming events.
4. **Vector search**: the k-NN engine behind many RAG (retrieval-augmented generation) apps and Bedrock Knowledge Bases.

It's **not** a system of record and not a cheap archive. The exam checks that you know when OpenSearch beats CloudWatch Logs Insights, Athena or Redshift, and how to keep its storage and shards under control.

## Core building blocks

- **Document**: a JSON record (one log line, one product).
- **Index**: a collection of documents with a **mapping** (the schema: field types like `text` for analyzed full text, `keyword` for exact values/aggregations, `date`, `knn_vector`). Dynamic mapping guesses types; explicit mappings avoid surprises such as a numeric field indexed as text.
- **Shards**: each index is split into **primary shards**, each with **replica shards** on *other* nodes for redundancy and extra read throughput.
  - The **primary shard count is fixed at index creation**. Changing it means `_reindex`, or the `_split`/`_shrink` APIs. The replica count can change any time.
  - **Sizing guidance:** roughly **10–30 GB per shard for search-heavy** workloads and **30–50 GB for log analytics**. Too many small shards waste heap ("oversharding"). Too few huge shards slow recovery and limit parallelism.
- **Index templates** apply settings and mappings to new indexes that match a pattern (`logs-*`). **Rollover** starts a new index when the current one hits a size, age or doc count, the standard pattern for time-series logs. **Data streams** wrap this up for append-only data.

## Domains: the provisioned model

A **domain** is your OpenSearch cluster:

| Component | What it does | Exam notes |
|---|---|---|
| **Data nodes** | Hold shards, run indexing and queries | Choose instance family by workload (memory-optimized for search/aggregations) |
| **Dedicated cluster manager nodes** (formerly "dedicated master") | Manage cluster state; no data | Use **3** for production (quorum survives one failure; never 2) |
| **UltraWarm nodes** | **Read-only** warm tier backed by **S3** with caching | Much cheaper per GB for older, still-queried logs |
| **Cold storage** | Indexes **detached** to S3; attach on demand to query | Cheapest; requires UltraWarm |
| **OpenSearch Optimized instances** (**OR1**, then **OR2/OM2** in 2025) | Local storage for speed + **synchronous copy to S3-backed managed storage** | Higher indexing throughput and durability for heavy log ingestion |

**Availability:** deploy across **3 AZs** with replicas. **Multi-AZ with Standby** keeps a full data copy in each of 3 AZs, with one AZ held as standby that takes over without rebalancing, backed by a **99.99% SLA**. Automated snapshots are taken to S3 every day. Manual snapshots go to your own S3 repository and are used for migration and DR.

**THE trap:** *"yellow cluster status on a single-node domain"* → replicas **can't be placed on the same node as their primary**. Add nodes (ideally across AZs) or set replicas to 0 for dev. It isn't data loss.

## Tiering and lifecycle (Task 2.3)

The warehouse analogy for logs: **hot shelves** by the door for this week's orders, **warm shelves** in the back for this quarter, **deep storage** for anything you might need someday, then the shredder.

```
hot (data nodes, read/write) ──7 days──▶ UltraWarm (read-only, S3-backed)
      ──90 days──▶ cold (detached in S3, attach to query) ──365 days──▶ delete
```

**Index State Management (ISM)** policies automate those moves with states and transitions: `rollover`, `force_merge`, `warm_migration`, `cold_migration`, `delete`, and more, triggered by index age or size. It's the OpenSearch equivalent of an S3 Lifecycle rule ([Guide 31](31-Data-Lifecycle-Retention-Resiliency.md)).

**THE trap:** *"keep 1 year of logs searchable in OpenSearch at lowest cost, with the last 7 days heavily queried"* → **hot → UltraWarm → cold via an ISM policy**. Adding more hot data nodes is the expensive wrong answer. If the old logs only need occasional SQL and not OpenSearch search, **archive to S3 and query with Athena**.

## OpenSearch Serverless

**Amazon OpenSearch Serverless** removes clusters. You create **collections** and AWS scales compute, measured in **OCUs** (OpenSearch Compute Units, each **6 GiB of memory** plus matching vCPU), **separately for indexing and search**, with storage decoupled into S3.

| Collection type | Use |
|---|---|
| **Time series** | Logs/metrics. Recent data hot, older data warm; append-heavy |
| **Search** | Full-text search apps. All data hot |
| **Vector search** | k-NN embeddings, including the default vector store for **Bedrock Knowledge Bases** quick-create |

- **Security is policy-based**: **encryption policies** (KMS key, required before creating a collection), **network policies** (public vs **VPC endpoint** access, per collection or for Dashboards), and **data access policies** (which IAM principals can do which index or collection operations).
- **Cost model**: you set **minimum and maximum OCUs** (per collection group, separately for indexing and search). The maximum is your budget cap. Historically serverless always kept a small floor of OCUs running even when idle, which surprised people with tiny workloads. Newer collection groups let the minimum go to 0, at the cost of cold-start delay.
- Pick serverless for **spiky or unpredictable** traffic and *"no cluster management."* Pick a **domain** when you need UltraWarm/cold tiers, specific plugins, fine-grained tuning, or steady high volume at the lowest unit cost.

## Getting data in

| Path | How it works | Pick when |
|---|---|---|
| **Amazon Data Firehose → OpenSearch** | Buffers by size/time, optional **Lambda transform**, **index rotation** (hourly/daily/weekly/monthly suffix), retries, and **S3 backup of failed (or all) documents** | *"Stream logs into OpenSearch with least operational overhead"* ([Guide 07](07-Amazon-Data-Firehose.md)) |
| **Amazon OpenSearch Ingestion** (OSI) | Managed, serverless **Data Prepper** pipelines: sources such as **S3** (via SQS notifications), **HTTP/OpenTelemetry**, **Kafka/MSK**, **Kinesis**, **DynamoDB** and **DocumentDB** (zero-ETL: initial export + change stream). Built-in processors for grok parsing, enrichment, routing and sampling | Parse, transform and route logs/traces without Logstash; **zero-ETL sync from DynamoDB/DocumentDB** |
| **CloudWatch Logs subscription filter** | Streams matching log events (to OpenSearch via Lambda/Firehose) | Existing CloudWatch log groups into near-real-time search ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)) |
| **Lambda** | Custom code posts `_bulk` requests (S3 events, DynamoDB Streams) | Small custom flows |
| **Logstash / Fluent Bit / OpenSearch clients** | Self-managed agents or apps | Existing ELK-style estates |
| **Direct query** | Query data **in place in S3** (plus CloudWatch Logs and Security Lake) from OpenSearch Dashboards without ingesting it, with optional accelerations (materialized views, skipping indexes) | Occasional investigation of cold data you didn't index |

**THE trap:** *"items in DynamoDB need fuzzy text search, kept in sync automatically, with no custom code"* → **DynamoDB zero-ETL to OpenSearch via OpenSearch Ingestion**. A Lambda on DynamoDB Streams works, but it's code to maintain.

## Security

- **Network**: choose **VPC access** (private IPs in your subnets, controlled by security groups) or **public access** (protected by a resource-based **domain access policy**, IP conditions and/or FGAC). The choice is **made at creation**; you can't switch a domain between public and VPC afterward.
- **Fine-grained access control (FGAC)**: OpenSearch-level roles with **index-level, document-level (DLS) and field-level (FLS) security plus field masking**. You **map IAM roles/users** (or SAML groups, internal users) to those roles. FGAC requires **encryption at rest, node-to-node encryption and HTTPS enforcement**.
- **Dashboards authentication**: **Amazon Cognito**, **SAML** (corporate IdP), or internal users via FGAC. The newer **OpenSearch UI** integrates with **IAM Identity Center**.
- **Encryption**: at rest with **KMS**, **node-to-node TLS**, **enforce HTTPS** with a minimum TLS policy.
- **Audit logs** (who queried or changed what) publish to **CloudWatch Logs** and require FGAC. Slow logs and error logs publish there too. API calls go to CloudTrail ([Guide 43](43-Audit-Logging-CloudTrail-Config.md)).

**THE trap:** *"analysts may search the index but must not see the `ssn` field, and regional managers only see their region's documents"* → **FGAC with field-level security/masking and document-level security**. IAM policies alone stop at index/API granularity.

## Operations and troubleshooting

| Signal | Meaning | Fix |
|---|---|---|
| **Cluster status red** | At least one **primary** shard unassigned. Some data unavailable, writes to it fail | Check for node loss or full disk; restore from snapshot if needed |
| **Cluster status yellow** | All primaries OK, some **replicas** unassigned | Add nodes/AZs; fix replica count |
| **High `JVMMemoryPressure`** (sustained above ~80%) | Heap exhausted: too many shards, huge aggregations, big bulk requests | Reduce shard count, scale up or out, tune queries |
| **HTTP 429 / `es_rejected_execution_exception`** | Thread-pool queues full, usually from **bulk indexing** faster than the cluster can absorb | Smaller or fewer concurrent bulk requests, **exponential backoff**, more or bigger data nodes, better shard balance |
| **Writes blocked** (`ClusterIndexWritesBlocked`, low `FreeStorageSpace`) | Storage nearly full; indexes go read-only | Delete old indexes (ISM), move to UltraWarm, add storage |
| **Too many shards** | Thousands of tiny daily indexes | Rollover by size, fewer primaries, shrink old indexes |

Set **CloudWatch alarms** on these metrics. AWS publishes a recommended set.

## Log-analytics decision (skills 3.3.8, 4.4.4)

| Need | Pick | Why |
|---|---|---|
| **Near-real-time** search across logs, interactive dashboards, full-text, alerting | **OpenSearch Service** | Indexed within seconds; Dashboards; anomaly detection and alerting plugins |
| Quick ad hoc queries on logs **already in CloudWatch Logs** | **CloudWatch Logs Insights** | Nothing to set up; pay per GB scanned; good for troubleshooting ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)) |
| Months or years of logs **archived in S3**, occasional SQL, cheapest | **Athena** (+ partition projection) | No ingestion, pay per TB scanned ([Guide 26](26-Amazon-Athena.md)) |
| **Massive** batch processing or complex transforms of big-data application logs | **EMR** (Spark) | Scale-out batch compute ([Guide 15](15-Amazon-EMR.md)) |
| Governed audit queries on CloudTrail events | CloudTrail Lake (no new customers since May 2026) / Athena on the trail bucket | [Guide 43](43-Audit-Logging-CloudTrail-Config.md) |

A common architecture uses both halves: **Firehose delivers logs to OpenSearch for the last 14 days** of fast search **and backs up everything to S3** for cheap long-term Athena queries.

## Vector engine (skill 2.1.8)

> 🆕 **New in exam guide v1.1:** vector index types (HNSW, IVF) and vector stores. Index theory, chunking and embeddings live in [Guide 19](19-GenAI-LLMs-Vectors.md).

- Enable **k-NN** on an index (`"index.knn": true`) and add a **`knn_vector`** field with a `dimension` (up to 10,000 floats). Query with a `knn` clause, `k` neighbors, optionally combined with filters, or with keyword search in **hybrid search**.
- **Engines**: **Faiss** (the usual default: HNSW and IVF, supports quantization) and **Lucene** (HNSW, efficient filtering, smaller indexes). The older **nmslib** engine is deprecated in recent OpenSearch versions, so don't choose it for new indexes.
- **Methods**: **HNSW** (graph, best recall/latency, more memory) and **IVF** (clusters vectors into lists, lower memory, needs training on sample data).
- **Memory-saving options**: scalar quantization (byte/fp16), **binary and product quantization (PQ)**, and **disk-based vector search**, which trade a little recall for much less RAM. k-NN graphs live **off the JVM heap**, so size memory for them.
- **Bedrock Knowledge Bases** create an **OpenSearch Serverless vector search collection** by default. Domains and other stores (Aurora pgvector, MemoryDB, S3 Vectors, Neptune Analytics) are the alternatives.

**THE trap:** *"cheapest vector storage for billions of rarely queried embeddings"* is **S3 Vectors**, not an always-on OpenSearch cluster. *"Low-latency hybrid (keyword + semantic) search"* is **OpenSearch**.

## Question patterns

> *"A company must search application logs within seconds of generation and build interactive dashboards, with least operational overhead for ingestion."* → **Firehose → OpenSearch Service with OpenSearch Dashboards** (near-real-time indexing; Athena over S3 isn't seconds-fresh or dashboard-interactive).

> *"Logs must be searchable in OpenSearch for 1 year; queries hit the last week 95% of the time. Minimize cost."* → **ISM policy: hot → UltraWarm → cold → delete** (read-only S3-backed tiers for older indexes).

> *"Security analysts occasionally investigate 3 years of VPC Flow Logs stored in S3. Cost is the priority."* → **Athena with partition projection** (loading 3 years into OpenSearch is far more expensive).

> *"Engineers want to quickly query Lambda logs already in CloudWatch Logs during an incident, without new infrastructure."* → **CloudWatch Logs Insights**.

> *"A log-analytics domain returns HTTP 429 errors during nightly bulk loads from a Glue job."* → **Reduce bulk request size/concurrency with exponential backoff, and scale out data nodes if sustained** (thread-pool rejections, not a permissions problem).

> *"A domain's status is red after a data node failed, and some queries fail."* → **Primary shards are unassigned. Restore node capacity and, if replicas were 0, restore from a snapshot**. Configure replicas across 3 AZs.

> *"Product descriptions in DynamoDB need typo-tolerant search, synced automatically with no custom code."* → **DynamoDB zero-ETL integration with OpenSearch via OpenSearch Ingestion**.

> *"A search workload is spiky and unpredictable, and the team wants no cluster sizing or patching."* → **OpenSearch Serverless (search collection)** with max OCU limits for cost control.

> *"Regional managers must only see documents for their own region, and the email field must be masked for analysts."* → **Fine-grained access control: document-level security + field masking, mapped to IAM roles**.

> *"The security team requires the OpenSearch domain to be unreachable from the internet."* → **Create the domain with VPC access** (decided at creation) with security groups; use FGAC and a restrictive access policy.

> *"A RAG application needs hybrid keyword + semantic retrieval with metadata filters, managed by Bedrock Knowledge Bases, with no cluster management."* → **OpenSearch Serverless vector search collection**.

> *"A 3-node production domain with one index of 5 primary shards has grown to 1 TB, and query latency is high. The team wants 20 primary shards."* → **Reindex (or `_split`) into a new index with more primaries**. Primary shard count can't be edited in place.

> *"Heavy log ingestion needs higher indexing throughput and durability without managing replica-heavy clusters."* → **OpenSearch Optimized instances (OR1/OR2) with S3-backed managed storage**.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Full-text search, relevance, fuzzy | OpenSearch Service |
| Logs searchable in seconds + dashboards | OpenSearch + Dashboards |
| Ad hoc on CloudWatch log groups | CloudWatch Logs Insights |
| Years of logs in S3, cheap SQL | Athena |
| Massive batch log crunching | EMR |
| Primary shard count | Fixed at creation → reindex / split |
| Shard size guidance | ~10–30 GB search, ~30–50 GB logs |
| Cluster manager nodes | 3 dedicated for production |
| Yellow status | Replicas unassigned (e.g., single node) |
| Red status | Primary shard(s) unassigned |
| 429 rejections | Smaller bulks, backoff, scale out |
| High JVMMemoryPressure | Too many shards / heavy queries → reduce or scale |
| Storage full | Writes blocked → ISM delete, UltraWarm, more storage |
| Older logs, read-only, cheaper | UltraWarm (S3-backed) |
| Cheapest, detached, query rarely | Cold storage |
| Automate hot → warm → cold → delete | ISM policy |
| New index per day/size | Index templates + rollover |
| 99.99% availability | Multi-AZ with Standby (3 AZs) |
| High-throughput ingest, S3 durability | OR1/OR2 OpenSearch Optimized instances |
| No clusters, spiky | OpenSearch Serverless (OCUs, 6 GiB each; indexing and search scale separately) |
| Serverless collection types | Time series / search / vector search |
| Serverless security | Encryption, network, data access policies |
| Stream into OpenSearch, no code | Firehose (index rotation, S3 backup) |
| Parse/transform pipelines, managed | OpenSearch Ingestion (Data Prepper) |
| DynamoDB/DocumentDB → OpenSearch sync | Zero-ETL via OpenSearch Ingestion |
| CloudWatch Logs → OpenSearch | Subscription filter |
| Query S3 without ingesting | Direct query |
| Index/document/field-level security | Fine-grained access control |
| Dashboards SSO | SAML / Cognito (IAM Identity Center for OpenSearch UI) |
| Private only | VPC domain (set at creation) |
| Who searched what | Audit logs → CloudWatch Logs (needs FGAC) |
| Vector search methods | HNSW (graph) / IVF (clusters) |
| Vector engines | Faiss, Lucene (nmslib deprecated) |
| Shrink vector memory | Quantization (byte/fp16/binary/PQ), disk-based |
| Bedrock KB default vector store | OpenSearch Serverless vector collection |

OpenSearch closes the storage-and-query block. Next, [Guide 30 — Data Modeling, Schema Evolution & Lineage](30-Data-Modeling-Schema-Evolution-Lineage.md) steps back to how you shape the data in all of these stores.
