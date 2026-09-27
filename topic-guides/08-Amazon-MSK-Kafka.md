# 08 · Amazon MSK & Apache Kafka — the records office of append-only ledgers

> **Exam map:** D1 · Task 1.1 — D2 · Task 2.1 — D4 · Task 4.1, 4.3 · **Skills:** 1.1.1, 1.1.10, 2.1.1, 4.1.x, 4.3.4 · **Weight:** 🔥🔥 Medium · **Read time:** ~18 min

## The idea

Imagine a **records office** full of **append-only ledgers**. Each subject (a **topic**, e.g., `orders`) is split across several ledgers (**partitions**) so many clerks can write at once. A clerk only ever **appends** a new numbered line (the **offset**) to the end of a ledger; nothing is edited in place. Every ledger is **photocopied to other branches** (**replicas** on other brokers), so losing a building loses nothing. Readers belong to **reading clubs** (**consumer groups**): within a club each ledger is read by exactly one member, who keeps a **bookmark** (the committed offset). Different clubs read the same ledgers independently, and anyone can move their bookmark back to reread. Old pages are discarded by **age or size**, not because someone read them.

That's **Apache Kafka**, the open-source distributed log that half the industry streams on. **Amazon MSK (Managed Streaming for Apache Kafka)** rents you the records office. AWS runs the brokers, replaces failed hardware, patches, and handles storage and encryption, while your applications keep speaking the **native Kafka API**. Conceptually it's the same design as Kinesis Data Streams ([Guide 06](06-Kinesis-Data-Streams.md)): a partition is a shard, an offset is a sequence number, a consumer group is a KCL application. The differences are *ecosystem, portability and control*.

On the exam, the word **"Kafka"** in a scenario (*existing Kafka producers*, *Kafka Connect*, *migrate on-prem Kafka*, *open-source compatibility*) almost always points at MSK. This guide teaches Kafka from zero, MSK's three flavours, security (a Domain 4 favourite), integrations, and the Kinesis-vs-MSK decision.

## Kafka concepts from zero

| Concept | What it means | Exam-relevant detail |
|---|---|---|
| **Broker** | A Kafka server storing partitions | MSK spreads brokers across 2 or 3 AZs (3 recommended) |
| **Topic / partition** | A named stream, split into ordered partitions | **Ordering is guaranteed only within a partition**. The record key's hash picks the partition, so the same key always lands in the same partition |
| **Replication factor (RF)** | Copies of each partition | **RF = 3** is the production norm. One replica is the **leader** (serves reads and writes), the others are **followers** |
| **ISR (in-sync replicas)** | Followers caught up with the leader | With `min.insync.replicas = 2` and producer **`acks=all`**, a write succeeds only when at least 2 replicas have it, so it survives a broker loss |
| **Producer acks** | `acks=0` fire-and-forget, `acks=1` leader only, `acks=all` all ISR | *"No data loss"* → **acks=all + min.insync.replicas=2 + RF=3** |
| **Idempotent producer** | `enable.idempotence=true` dedupes producer retries per partition | Together with **transactions** it gives Kafka's **exactly-once** semantics (EOS) |
| **Consumer group** | Consumers sharing a group ID split the partitions | **One partition → at most one consumer per group**. More consumers than partitions leaves idle consumers, so scale with more partitions |
| **Offset** | Position in a partition, committed per group | Reset to earliest/latest or a timestamp to **replay** |
| **Retention** | By **time** (`retention.ms`) and/or **size** (`retention.bytes`) | Data stays whether it's consumed or not. It can be very long, even "forever" with tiered storage |
| **Log compaction** | `cleanup.policy=compact` keeps the **latest value per key** | *"Keep the current state of each customer/key,"* changelog topics |
| **Metadata: ZooKeeper vs KRaft** | Old: a separate ZooKeeper ensemble. New: **KRaft**, a Raft-based controller quorum inside Kafka | Kafka **4.x is KRaft-only** |
| **Kafka Connect** | A framework for **source** connectors (DB/SaaS → Kafka) and **sink** connectors (Kafka → S3, OpenSearch, JDBC…) | Managed on AWS by **MSK Connect** |
| **Schema registry** | Stores Avro/JSON Schema/Protobuf schemas and enforces compatibility | **AWS Glue Schema Registry** works with MSK, KDS, Flink and Lambda ([Guide 13](13-Glue-Data-Catalog-Crawlers.md), [Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md)) |

**THE trap:** *"Consumers can't keep up; the team adds more consumer instances to the group, but lag doesn't improve."* A group can't use more consumers than there are **partitions**. The fix is **more partitions** (plus consumers), or making each consumer faster.

## MSK flavours

| | **MSK Provisioned — Standard brokers** | **MSK Provisioned — Express brokers** | **MSK Serverless** |
|---|---|---|---|
| You choose | Broker instance type/count, EBS storage | Broker size/count (**m7g** family); **no storage** to manage | Nothing (just the cluster) |
| Storage | EBS **1 GiB–16 TiB per broker**; **storage auto-scaling** (scale up only); optional **tiered storage** for cheap long retention | **Virtually unlimited, pay-as-you-go** storage | Managed; **unlimited retention** |
| Throughput | Per broker, depends on instance type | **Up to 3× more per broker** than Standard (e.g., **500 MBps** ingress sustained on the largest size); **up to 20× faster scaling**, **90% faster recovery** | **200 MBps in / 400 MBps out per cluster**; **5 MBps in / 10 MBps out per partition** |
| Limits | 90 brokers/account; **30 brokers/cluster (ZooKeeper) or 60 (KRaft)** | 3-AZ only; best-practice configs enforced; no maintenance windows | **2,400 partitions**, **8 MiB max message**, 500 consumer groups, 3,000 connections |
| Auth | IAM, SASL/SCRAM, mTLS, unauthenticated | IAM, SASL/SCRAM, mTLS | **IAM only** |
| Control | Full Kafka config tuning | Guard-railed configs | Minimal |
| Pick when | Need full control, specific Kafka features/versions, predictable load | High throughput, elastic scaling, less storage ops (launched Nov 2024) | *"Kafka without capacity planning," "least operational overhead,"* spiky or unknown load within the limits |

Version and metadata facts (2024–2026):
- **KRaft mode** has been supported on MSK since Kafka **3.7** (May 2024). Kafka **4.x drops ZooKeeper completely**, and **in-place ZooKeeper → KRaft migration** for existing MSK clusters arrived in Aug 2026. KRaft clusters allow **60 brokers** per cluster by default instead of 30.
- **Tiered storage** (Standard brokers) moves older log segments to a low-cost AWS-managed tier. Consumers read them transparently. Signal: *"retain Kafka data for months cheaply without adding brokers."*

**THE trap:** *"Keep a year of Kafka data on a Provisioned Standard cluster at the lowest cost"*. The answer is **tiered storage**, not bigger EBS volumes or more brokers. EBS storage can grow (auto-scaling) but **never shrink**.

## MSK Connect, Replicator, and migrations

- **MSK Connect** runs **Kafka Connect** workers for you: upload a connector plugin (JAR/ZIP) to S3, choose **auto-scaled** or **provisioned** capacity, and it manages the workers. Classic pairs: **Debezium source** (MySQL/PostgreSQL CDC → Kafka topics) and an **S3 sink** (topics → S3 in Avro/Parquet/JSON). Other sinks include OpenSearch and JDBC. Limits: up to 10 workers per connector by default.
- **MSK Replicator** does managed, serverless **cross-Region or same-Region replication** between MSK clusters, including topic configs and consumer offsets. Use it for **DR, active-active, or moving data closer to consumers**. Limits: up to **1 GB/s** ingress per replicator, 750 topics.
- **MirrorMaker 2** (MM2) is Kafka's open-source replication tool, usually run on MSK Connect or EC2. It's the classic answer for **migrating a self-managed/on-prem Kafka cluster to MSK** with minimal downtime.

> ⚠️ **2026 status:** MSK now also offers **managed data delivery channels** that write topics directly to **S3 buckets** or **Iceberg tables on S3 Tables** (5–15 minute freshness), similar to the Aug 2026 Kinesis feature. The v1.1 exam predates this. For *"MSK topic → S3 with no code,"* the exam answers are **Firehose with an MSK source** ([Guide 07](07-Amazon-Data-Firehose.md)) or an **MSK Connect S3 sink**.

## Security: authentication, authorization, encryption

**Client authentication options** (you can enable several per cluster):

| Method | How it works | Pick when |
|---|---|---|
| **IAM access control** | Clients sign with SigV4 using the MSK IAM auth library. **IAM policies authorize** Kafka actions (`kafka-cluster:Connect`, `WriteData`, `ReadData`, `CreateTopic`…) on cluster/topic/group ARNs | *"Use IAM roles, no Kafka ACLs to manage,"* **the only option for Serverless** |
| **SASL/SCRAM** | Username/password stored in **AWS Secrets Manager** (secret name prefixed `AmazonMSK_`, encrypted with a **customer managed KMS key**) and associated with the cluster. Authorize with **Kafka ACLs** | *"Existing apps use username/password," "rotate credentials centrally"* ([Guide 39](39-Encryption-Key-Management.md)) |
| **Mutual TLS (mTLS)** | Client certificates issued by **AWS Private CA**; authorize with Kafka ACLs | *"Certificate-based authentication"* |
| **Unauthenticated** | No client auth (network controls only) | Dev only. Never for public access |

**Encryption (skill 4.3.4):**
- **At rest:** always on, with **AWS KMS** (AWS managed key by default, or your customer managed key).
- **In transit, client ↔ broker:** `TLS` (default), `TLS_PLAINTEXT` (both allowed, useful during migration), or `PLAINTEXT`.
- **In transit, broker ↔ broker (in-cluster):** TLS **on by default**. The choice is made at creation, and turning it off buys a little latency at the cost of security.
- "Before transit" encryption means the producer encrypts the payload itself (client-side).

**Networking:**
- MSK lives **in your VPC**. Brokers get ENIs in your subnets, and **security groups** control client access (open the ports for your auth type, e.g., 9098 for IAM, 9096 for SCRAM, 9094 for TLS).
- **Multi-VPC private connectivity** (AWS PrivateLink) lets clients in **other VPCs or accounts** connect privately with IAM, SCRAM or mTLS auth, with a **cluster policy** granting access. Firehose uses it to read private clusters cross-account.
- **Public access** can be turned on for an existing provisioned cluster only when it's in **public subnets**, uses **TLS in transit**, has **authenticated** access (IAM, SCRAM or mTLS) with unauthenticated **off**, and (when using ACLs) `allow.everyone.if.no.acl.found=false`.

**THE trap:** *"Cross-account consumers need private access to an MSK cluster; minimize network management."* The answer is **MSK multi-VPC private connectivity** (PrivateLink, managed by MSK), not VPC peering plus hand-managed routes, and not public access.

## Message size: Kafka vs Kinesis

- Kafka's default max message is about **1 MB** (`message.max.bytes`), but it's **configurable** on MSK Provisioned through a custom cluster configuration (align `replica.fetch.max.bytes` and the clients' fetch/request sizes). **MSK Serverless caps messages at 8 MiB.**
- Kinesis Data Streams was **1 MB** for years (the source of the classic *"records > 1 MB → MSK"* signal) and supports **up to 10 MiB since Oct 2025** as a per-stream setting.
- For truly large payloads (images, documents), both use the **claim-check** pattern: store the object in S3 and send its key.

## Integrations

| Consumer / integration | Notes |
|---|---|
| **Lambda event source mapping (MSK or self-managed Kafka)** | Lambda polls topics as a consumer group. Batch size up to 10,000, batching window, starting position, and optional **provisioned mode** (dedicated event pollers) for high-throughput topics. A **Kafka topic, SQS, SNS or S3** can be an on-failure destination. The function needs VPC access to the brokers (or uses the ESM's networking) ([Guide 17](17-Lambda-for-Data-Pipelines.md)) |
| **Amazon Data Firehose** | MSK as source → S3 with no code ([Guide 07](07-Amazon-Data-Firehose.md)) |
| **Managed Service for Apache Flink** | Kafka source/sink connectors for stateful processing ([Guide 09](09-Managed-Service-for-Apache-Flink.md)) |
| **AWS Glue streaming ETL** | Kafka/MSK connection for Spark micro-batches into the lake ([Guide 12](12-AWS-Glue-ETL.md)) |
| **EMR (Spark Structured Streaming, Flink)** | Full control on clusters ([Guide 15](15-Amazon-EMR.md)) |
| **Redshift streaming ingestion** | Materialized view over an MSK topic. IAM or mTLS auth ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)) |
| **MSK Connect** | Managed Kafka Connect source/sink connectors |
| **Glue Schema Registry** | Schema enforcement for Avro/JSON/Protobuf producers and consumers |

## Monitoring

- **CloudWatch monitoring levels:** `DEFAULT` (free, cluster and broker basics), `PER_BROKER`, `PER_TOPIC_PER_BROKER`, `PER_TOPIC_PER_PARTITION` (more detail, extra cost).
- **Consumer lag metrics:** `MaxOffsetLag`, `SumOffsetLag`, `EstimatedMaxTimeLag` per consumer group and topic (partition-level `OffsetLag` / `EstimatedTimeLag` at the finest level). They're the MSK equivalent of Kinesis `IteratorAge`.
- **Health metrics to know:** `KafkaDataLogsDiskUsed` (disk filling, so expand storage or enable auto-scaling/tiered storage), `CpuUser`, `UnderReplicatedPartitions`, `OfflinePartitionsCount`, `ActiveControllerCount`.
- **Open monitoring with Prometheus:** JMX and Node exporters on brokers, scraped by self-managed Prometheus or **Amazon Managed Service for Prometheus**, visualized in **Amazon Managed Grafana**.
- **Broker logs** can go to CloudWatch Logs, S3 or Firehose. **CloudTrail** records MSK control-plane API calls (and Kafka data-plane actions authorized by IAM) ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)).

## Kinesis Data Streams vs Amazon MSK

| Dimension | **Kinesis Data Streams** | **Amazon MSK** |
|---|---|---|
| API / ecosystem | AWS-proprietary (SDK, KPL/KCL) | **Open-source Kafka API**: Kafka Connect, Kafka Streams, huge tool ecosystem |
| Portability | AWS only | Runs anywhere Kafka runs, so *"avoid lock-in," "hybrid," "migrate existing Kafka"* |
| Scaling unit | Shards; **on-demand** removes planning | Partitions + brokers; **Serverless/Express** reduce planning |
| Message size | 1 MB classic, **10 MiB** opt-in now | ~1 MB default, **configurable** (Serverless 8 MiB) |
| Retention | 24 h → **365 days** | Time/size based, **effectively unlimited** (tiered storage / Serverless / Express) |
| Tuning & control | Few knobs | **Deep Kafka configuration** (acks, compaction, replication, EOS transactions) |
| Ops overhead | **Lowest** | Low (Serverless) to medium (Provisioned) |
| Pricing model | Shard-hours or per-GB on-demand | Broker-hours + storage (Provisioned); cluster-hours + partitions + data (Serverless) |
| AWS integrations | Deepest native (Firehose, Lambda, Flink, Redshift, DynamoDB streams) | Broad and growing (Lambda, Firehose, Flink, Glue, Redshift, EMR) |

**Decision rule:** the scenario mentions **Kafka** (existing apps, Kafka Connect, MirrorMaker, open-source requirement) → **MSK**, and *"least operational overhead"* on top → **MSK Serverless** (if within its limits). If there's no Kafka signal and the question wants AWS-native, simple, serverless streaming → **Kinesis Data Streams (on-demand)**.

**THE trap:** *"Migrate on-prem Kafka producers and consumers to AWS with minimal code changes"* → **MSK** (same API, so just repoint the bootstrap servers). Kinesis would force rewrites to the Kinesis SDK/KCL, which isn't "minimal code changes".

## Question patterns

> *"A company runs Apache Kafka on premises with dozens of producer and consumer apps. It wants to move to AWS with the fewest code changes and no broker management."* → **Amazon MSK (Serverless if throughput/partitions fit, else Provisioned); migrate data with MirrorMaker 2** (same Kafka API. Kinesis requires rewrites.)

> *"Stream data with the Kafka API but without capacity planning or broker sizing, at moderate throughput."* → **MSK Serverless** (auto capacity, IAM auth only; 200 MBps in per cluster.)

> *"Capture changes from an Aurora MySQL database into Kafka topics with a managed connector, then land them in S3."* → **MSK Connect with a Debezium source connector + an S3 sink connector (or Firehose with MSK source)** (managed Kafka Connect.)

> *"Kafka producers must never lose acknowledged messages even if a broker fails."* → **RF = 3, `min.insync.replicas = 2`, producer `acks=all`** (plus the idempotent producer to avoid retry duplicates.)

> *"An MSK cluster must be read by a Lambda function and an analytics team in two other AWS accounts over private networking, using IAM auth."* → **Enable MSK multi-VPC private connectivity with a cluster policy** (PrivateLink. No peering, no public exposure.)

> *"Clients authenticate with usernames and passwords that security wants rotated and stored centrally."* → **SASL/SCRAM with credentials in AWS Secrets Manager (AmazonMSK_ prefix, customer managed KMS key) + Kafka ACLs** (IAM would need client code changes; mTLS needs certificates.)

> *"All data between clients and brokers and between brokers must be encrypted; data at rest must use a customer managed key."* → **TLS client-broker encryption + in-cluster TLS + KMS customer managed key at creation** (at-rest encryption is always on; pick the CMK.)

> *"The consumer group lag keeps growing although CPU on consumers is low; the topic has 6 partitions and the group has 12 consumers."* → **Increase the topic's partition count (and keep consumers ≤ partitions)** (six consumers are idle.)

> *"Retain 12 months of topic data for occasional replays at the lowest storage cost on a Standard-broker cluster."* → **Enable MSK tiered storage** (cheap long-term tier. EBS only grows and costs more.)

> *"Replicate topics continuously from an MSK cluster in us-east-1 to eu-west-1 for disaster recovery, with no replication infrastructure to run."* → **MSK Replicator** (serverless cross-Region replication including offsets. MM2 on EC2 is more ops.)

> *"Operations wants an alarm when consumers fall more than 5 minutes behind on an MSK topic."* → **CloudWatch alarm on `EstimatedMaxTimeLag` for the consumer group** (MSK's consumer-lag metrics. `MaxOffsetLag` counts messages, not time.)

> *"A broker's disk is almost full and the team wants to avoid manual storage operations in future."* → **Enable storage auto-scaling (or move to Express brokers with pay-as-you-go storage)** (watch `KafkaDataLogsDiskUsed`.)

> *"Stream processing must use Kafka Streams-style stateful joins with exactly-once guarantees on a managed service, reading from MSK."* → **Managed Service for Apache Flink with Kafka connectors** (managed stateful processing; exactly-once via checkpoints.)

> *"An application team needs a durable log to keep only the latest record per customer ID indefinitely."* → **Kafka topic with log compaction on MSK** (`cleanup.policy=compact`.)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| "Kafka," existing Kafka apps, Kafka Connect | **Amazon MSK** |
| Kafka with no capacity planning | **MSK Serverless** (IAM auth only) |
| Serverless limits | **200/400 MBps per cluster, 2,400 partitions, 8 MiB messages, unlimited retention** |
| High-throughput, elastic, no storage mgmt | **MSK Express brokers** (up to 3× throughput/broker, 20× faster scaling) |
| Full Kafka control | MSK Provisioned (Standard brokers) |
| Standard broker storage | EBS up to **16 TiB/broker**, auto-scaling grows only |
| Cheap long retention | **Tiered storage** |
| Brokers per cluster | **30 (ZooKeeper) / 60 (KRaft)** |
| ZooKeeper vs KRaft | KRaft since 3.7 on MSK; **Kafka 4.x KRaft-only** |
| Order guarantee | Per **partition** (same key → same partition) |
| More consumers than partitions | Extra consumers idle, so add partitions |
| No data loss writes | **acks=all + min.insync.replicas=2 + RF=3** |
| Exactly-once in Kafka | Idempotent producer + transactions |
| Keep latest value per key | **Log compaction** |
| Managed connectors (Debezium, S3 sink) | **MSK Connect** |
| Cross-Region/cluster replication, managed | **MSK Replicator** |
| Migrate self-managed Kafka | **MirrorMaker 2** |
| IAM-based auth, no ACLs | **IAM access control** |
| Username/password auth | **SASL/SCRAM + Secrets Manager (CMK)** |
| Certificate auth | **mTLS with AWS Private CA** |
| Encryption at rest | Always on, KMS |
| Encryption in transit | TLS client↔broker + in-cluster TLS |
| Cross-account private clients | **Multi-VPC private connectivity (PrivateLink)** |
| Public access prerequisites | Public subnets + TLS + authenticated access, unauthenticated off |
| Schema enforcement | **Glue Schema Registry** |
| Consumer lag metrics | `MaxOffsetLag`, `SumOffsetLag`, `EstimatedMaxTimeLag` |
| Monitoring levels | DEFAULT → PER_BROKER → PER_TOPIC_PER_BROKER → PER_TOPIC_PER_PARTITION |
| Prometheus metrics | **Open monitoring** (JMX/Node exporters) |
| MSK topic → S3, no code | **Firehose (MSK source)** or MSK Connect S3 sink |
| Kafka → Lambda | Lambda ESM for MSK / self-managed Kafka |
| Kafka → Redshift with SQL | **Redshift streaming ingestion** |
| No Kafka signal, AWS-native, least ops | **Kinesis Data Streams on-demand** |

Kafka and Kinesis move the events. Turning a moving stream into windows, joins and anomalies, with state that survives failures, is the job of the next guide: [Guide 09 — Amazon Managed Service for Apache Flink](09-Managed-Service-for-Apache-Flink.md).
