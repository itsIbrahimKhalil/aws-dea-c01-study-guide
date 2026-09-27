# 28 · RDS, Aurora & Purpose-Built Databases — the right tool from the right drawer

> **Exam map:** D1 · Task 1.1 — D2 · Task 2.1 · **Skills:** 2.1.1, 2.1.2, 2.1.3, 2.1.6, 1.1.9 (also touches 1.4.11) · **Weight:** 🔥🔥 Medium · **Read time:** ~18 min

## The idea

Picture a **workshop tool wall**. There's the trusty **all-purpose workbench** where you can build nearly anything, carefully and correctly: a **relational database** with tables, joins, transactions and SQL. Beside it hang **specialist tools**: a **label maker** that finds any drawer in a microsecond (in-memory key-value), a **filing cabinet of folders** that bend to any shape (documents), **string and pins on a corkboard** for tracing who-knows-whom (graphs), and a **conveyor-belt tally counter** that never stops writing (wide-column). Good craftspeople don't hammer screws. They match the tool to the job.

AWS's version is **Amazon RDS** (Relational Database Service: managed MySQL, PostgreSQL, MariaDB, Oracle, SQL Server, Db2) and **Amazon Aurora**, AWS's cloud-native MySQL- and PostgreSQL-compatible engine. Next to them sit the **purpose-built databases**: DynamoDB ([Guide 27](27-DynamoDB.md)), **MemoryDB**, **DocumentDB**, **Keyspaces**, **Neptune**, plus ElastiCache and Timestream on the edges.

> 🆕 **New in exam guide v1.1:** **Amazon Aurora** is now explicitly in scope, and skill 2.1.3 names **HNSW indexing on Aurora PostgreSQL** and **MemoryDB for fast key/value access**. Older prep material rarely covers either.

As a data engineer you mostly **pull data out of** these databases (safely, without hurting production), sometimes **land results in** them, must **not break them with connection storms or locks**, and must **pick the right one** from an access-pattern description. That's what this guide drills.

## RDS vs Aurora in one table

| | **RDS** (MySQL/PostgreSQL/MariaDB/Oracle/SQL Server/Db2) | **Aurora** (MySQL/PostgreSQL-compatible) |
|---|---|---|
| Storage | EBS volumes per instance. Multi-AZ keeps a synchronous standby | **Shared distributed cluster volume: 6 copies across 3 AZs**, self-healing, auto-grows up to **256 TiB** (doubled from 128 TiB in July 2025) |
| Read scaling | Read replicas (async; up to 15 for MySQL/MariaDB/PostgreSQL) | Up to **15 Aurora Replicas** sharing the same storage (replica lag typically milliseconds). **Reader endpoint** load-balances across them. Replica **auto scaling** |
| Failover | Multi-AZ standby promotion | A replica is promoted; the **cluster (writer) endpoint** follows it automatically |
| Serverless | No | **Aurora Serverless v2**: scales in fine-grained **ACUs** (Aurora Capacity Units, ~2 GiB memory each) from **0 to 256 ACUs**. Minimum **0** = **auto-pause** when idle (Nov 2024) |
| Multi-Region | Cross-Region read replicas | **Aurora Global Database**: storage-level replication to secondary Regions, typically under a second of lag, for fast DR and local reads |
| Extras | — | **Fast cloning** (copy-on-write, great for test copies of prod), **Backtrack** (Aurora MySQL: rewind the cluster in place without a restore), **I/O-Optimized** storage config (no per-I/O charges, pick when I/O is a big share of the bill), zero-ETL to Redshift |

Pick **Aurora** for high throughput, fast failover, many readers, serverless scaling or global replication. Pick **RDS** for commercial engines (Oracle, SQL Server, Db2), lift-and-shift compatibility, or small, cheap instances.

## RDS/Aurora as pipeline sources and targets

The golden rule: **never run heavy extracts against the primary writer.**

| Need | Answer |
|---|---|
| Nightly full extract without hurting OLTP | Point **Glue JDBC / DMS full load** at a **read replica** or the **Aurora reader endpoint** |
| Whole database into the lake as files | **Snapshot export to S3** writes **Apache Parquet** with no load on the live DB. Good for archives and one-off analytics |
| Ongoing CDC (inserts/updates/deletes) into S3, Redshift, Kinesis | **AWS DMS** full load + CDC (reads binlog / WAL; MySQL needs `binlog_format=ROW`, PostgreSQL needs logical replication) → [Guide 10](10-DMS-Database-Ingestion.md) |
| Near-real-time analytics copy with **no pipeline to manage** | **Zero-ETL integration Aurora (MySQL/PostgreSQL) / RDS → Amazon Redshift** |
| Occasional query from the warehouse without copying | **Redshift federated query** to Aurora/RDS PostgreSQL and MySQL ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)) |
| Ad hoc join of RDS with S3 data | **Athena federated query** (MySQL/PostgreSQL connectors) ([Guide 26](26-Amazon-Athena.md)) |
| Custom Spark transforms | **Glue JDBC connection** (VPC, self-referencing security group, S3 endpoint; parallel reads with `hashfield`/`hashpartitions`) ([Guide 12](12-AWS-Glue-ETL.md)) |
| Pull Aurora data into ML/LLM calls in SQL | Aurora ML integration (brief; [Guide 19](19-GenAI-LLMs-Vectors.md)) |

**THE trap:** *"minimize impact on the production database"* → the replica/reader endpoint, snapshot export, or zero-ETL. A Glue job hammering the writer endpoint is the wrong answer, even if it "works."

## Rate limits and connection storms (skill 1.1.9)

Relational engines have finite **connections** (`max_connections`, derived from instance memory by default) and finite CPU and I/O. Data pipelines break them in predictable ways:

- **Lambda fan-out**: 1,000 concurrent Lambda invocations each open a connection → `too many connections`. Fix: **RDS Proxy**, which **pools and reuses connections**, absorbs bursts, shortens failover (clients stay connected to the proxy), and supports **IAM authentication with credentials in Secrets Manager**. You can also cap Lambda **reserved concurrency** so the database can keep up.
- **Read overload**: add **read replicas** / **Aurora Replica auto scaling**, send reads to the reader endpoint, cache hot reads (ElastiCache).
- **Write overload**: scale the writer up (or add Serverless v2 ACUs), batch writes, buffer through **SQS**, and use exponential **backoff with jitter** on retries.
- **Glue/Spark parallel JDBC reads**: too many partitions means too many connections. Tune the parallelism.

**THE trap:** *"Lambda functions exhaust database connections during spikes"* → **RDS Proxy**. Raising `max_connections` or the instance size only postpones the failure.

## Locks, isolation and blocking (skill 2.1.6)

**Why locks exist:** two transactions updating the same row must not clobber each other. Engines use **row-level locks** for normal DML and **table-level locks** for DDL and explicit `LOCK TABLE`. **MVCC** (multi-version concurrency control), used by both PostgreSQL and InnoDB, lets **readers see a consistent snapshot without blocking writers**. Writers still block writers on the same rows.

| | PostgreSQL (RDS/Aurora) | MySQL InnoDB (RDS/Aurora) |
|---|---|---|
| **Default isolation** | **READ COMMITTED** | **REPEATABLE READ** |
| Lock wait limit | `lock_timeout` (0 = wait forever), `statement_timeout` | `innodb_lock_wait_timeout` (default **50 s**) |
| Deadlocks | Detected automatically; one transaction aborted | Detected automatically; one rolled back (error 1213) |
| Find blockers | `pg_stat_activity`, `pg_locks`, `pg_blocking_pids(pid)` | `performance_schema.data_locks` / `data_lock_waits`, `sys.innodb_lock_waits`, `SHOW ENGINE INNODB STATUS` |

```sql
-- Lock the rows you're about to change (pessimistic locking)
BEGIN;
SELECT * FROM inventory WHERE sku = 'A-100' FOR UPDATE;   -- exclusive row lock
UPDATE inventory SET qty = qty - 1 WHERE sku = 'A-100';
COMMIT;
-- FOR SHARE: others can read-lock too, but no one can update until you finish
-- FOR UPDATE SKIP LOCKED: job-queue pattern, grab unlocked rows only

-- PostgreSQL: who is blocking whom?
SELECT pid, pg_blocking_pids(pid) AS blocked_by, state, query
FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0;

-- Explicit table lock for a maintenance swap (PostgreSQL)
LOCK TABLE staging_orders IN ACCESS EXCLUSIVE MODE;

-- Application-defined mutex, e.g. "only one loader at a time"
SELECT pg_try_advisory_lock(42);
```

Pipeline lessons the exam tests:
- **Long-running transactions are poison.** An idle-in-transaction session holds locks and, on PostgreSQL, **stops VACUUM from cleaning dead rows** (bloat, transaction ID wraparound risk). Keep ETL transactions short and commit in batches.
- **DDL queues behind readers**: `ALTER TABLE` needs an exclusive lock, waits behind a long SELECT, and then *every new query queues behind the ALTER*. Set `lock_timeout` before DDL, and run migrations off-peak.
- **Deadlocks**: access rows in a consistent order, keep transactions small, retry on deadlock errors.
- **Diagnose** with **Performance Insights** (DB load by wait event, SQL and host, now surfaced through **CloudWatch Database Insights**). Lock waits show up as `Lock:transactionid` / `synch/...` waits.
- Redshift locking is a separate story ([Guide 25](25-Redshift-Performance-Operations-Security.md)).

**THE trap:** *"a nightly ETL UPDATE blocks the application for minutes"* → break it into **small committed batches**, set **lock timeouts**, schedule off-peak, or read from a **replica**. Raising the isolation level makes blocking worse, not better.

## Aurora PostgreSQL + pgvector (skill 2.1.3)

> 🆕 **New in exam guide v1.1:** vector indexes (HNSW, IVF) and Aurora PostgreSQL as a vector store.

**pgvector** is a PostgreSQL extension that adds a `vector` column type and similarity search. You can keep embeddings **next to your relational data** and filter with normal SQL (`WHERE tenant_id = 7`) in the same query.

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE items (id bigserial PRIMARY KEY, tenant_id int, body text,
                    embedding vector(1024));

-- HNSW: graph index, best recall/latency; can be built before or after loading
CREATE INDEX ON items USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
SET hnsw.ef_search = 100;                 -- higher = better recall, slower

-- IVFFlat: clusters vectors into lists; build AFTER loading data
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
SET ivfflat.probes = 10;                  -- lists searched per query

SELECT id, body FROM items
WHERE tenant_id = 7
ORDER BY embedding <=> :query_embedding   -- cosine distance
LIMIT 5;
```

| Operator | Distance | Operator class |
|---|---|---|
| `<->` | Euclidean (L2) | `vector_l2_ops` |
| `<=>` | Cosine distance | `vector_cosine_ops` |
| `<#>` | **Negative** inner product (smaller = more similar) | `vector_ip_ops` |

- **HNSW vs IVFFlat**: HNSW (Hierarchical Navigable Small World) gives better recall/latency trade-offs and needs no training step, at the cost of more memory and slower builds. IVFFlat builds faster with less memory but needs representative data *before* building, and recall depends on `probes`. The theory is in [Guide 19](19-GenAI-LLMs-Vectors.md).
- **Aurora Optimized Reads** (instances with local NVMe) can speed up vector queries whose index is larger than memory.
- **Amazon Bedrock Knowledge Bases** can use **Aurora PostgreSQL (pgvector) as the vector store**. Pick it when the team already runs Aurora and wants embeddings + SQL filters in one place.

## MemoryDB vs ElastiCache (skill 2.1.3)

**Amazon MemoryDB** (formerly "MemoryDB for Redis"; now **Valkey- and Redis OSS-compatible**) is an **in-memory database that is also durable**. Every write is committed to a **Multi-AZ transaction log** before it's acknowledged, so it can be the **primary system of record**.
- **Microsecond reads, single-digit-millisecond writes**, Redis data structures (strings, hashes, sorted sets, streams).
- **Vector search** with **HNSW and FLAT** indexes, for the highest-throughput, lowest-latency vector workloads (real-time recommendations, semantic caching).

**Amazon ElastiCache** (Valkey, Redis OSS, Memcached; serverless or node-based) is a **cache**: it sits in front of a database to speed up reads, and its data can be lost or rebuilt.

**THE trap:** *"microsecond key/value access AND data must survive failures without a separate database"* → **MemoryDB**. ElastiCache is the answer for *"cache query results in front of RDS/DynamoDB."* For DynamoDB specifically, a read cache usually means **DAX**.

## The other purpose-built databases

| Service | Model & API | Data-engineering signals |
|---|---|---|
| **Amazon DocumentDB** | JSON documents, **MongoDB-compatible** API | Flexible, nested JSON (catalogs, profiles, content). **Change streams** for CDC (feed Lambda/DMS/OpenSearch Ingestion). **Elastic clusters** shard to millions of reads/writes per second. Up to 15 replicas. **Vector search** (HNSW/IVFFlat) in newer versions. *"Migrate MongoDB with minimal code change"* |
| **Amazon Keyspaces** | Wide-column, **Apache Cassandra-compatible**, **CQL**, **serverless** | High-write, time-series-ish or IoT workloads already on Cassandra. On-demand or provisioned capacity, TTL, PITR. *"Run Cassandra workloads without managing clusters"* |
| **Amazon Neptune** | **Graph**: property graph (**Gremlin**, **openCypher**) and RDF (**SPARQL**) | Relationships are the query: **fraud rings**, social networks, recommendations, **knowledge graphs**, identity resolution, **data lineage**. **Bulk loader** ingests CSV/RDF from S3 (IAM role + S3 VPC endpoint). **Neptune Serverless**. **Neptune Analytics** = in-memory graph analytics (PageRank, community detection) **with vector search** (GraphRAG) |
| **Amazon ElastiCache** | In-memory cache (Valkey/Redis OSS/Memcached) | Cache-aside for hot reads, session store, leaderboards. Not the system of record |
| **Amazon Timestream** | Time-series | IoT/ops metrics with time functions. See the status note below |

> ⚠️ **2026 status:** **Timestream for LiveAnalytics** stopped accepting new customers on **June 20, 2025**; AWS points new users to **Timestream for InfluxDB**. Exam guide v1.1 dropped Timestream from its out-of-scope list without adding it to the in-scope list, so treat it as a plausible distractor. For time-series pipelines the exam usually wants Kinesis/Firehose → S3 + Athena, DynamoDB time-bucketed keys, or Keyspaces.

**Why graphs matter (skill 1.4.11):** a **graph** stores **nodes** (entities) and **edges** (relationships). Queries like *"find accounts within three hops sharing a device with a known fraudster"* are a handful of traversals in Gremlin or openCypher, but a self-join nightmare in SQL. When the exam says *"highly connected data"*, *"relationships"*, *"traverse"*, *"hops"* → **Neptune**.

## Choosing a database by access pattern

| Signal words in the question | Pick |
|---|---|
| Relational schema, joins, ACID transactions, existing MySQL/PostgreSQL app | **RDS** or **Aurora** |
| Oracle / SQL Server / Db2 licensing, lift-and-shift | **RDS** (commercial engine) |
| High throughput, 15 low-lag replicas, fast failover, global DR under 1 s lag | **Aurora** (+ Global Database) |
| Intermittent or unpredictable relational load, scale to zero | **Aurora Serverless v2** |
| Embeddings + SQL filters in the same DB, Bedrock KB vector store | **Aurora PostgreSQL + pgvector (HNSW)** |
| Key-value/document at any scale, single-digit ms, serverless | **DynamoDB** |
| Microsecond reads **and** durable primary store | **MemoryDB** |
| Cache in front of a database | **ElastiCache** (or DAX for DynamoDB) |
| MongoDB-compatible JSON documents | **DocumentDB** |
| Cassandra/CQL, serverless wide-column, massive writes | **Keyspaces** |
| Relationships, fraud rings, knowledge graph, lineage graph | **Neptune** (Analytics for graph algorithms + vectors) |
| Full-text search, log analytics | **OpenSearch Service** ([Guide 29](29-OpenSearch-Service.md)) |
| Analytical SQL over TBs/PBs | **Redshift** / **Athena** (not an OLTP database) |

## Question patterns

> *"A nightly Glue job extracting 500 GB from an Aurora PostgreSQL cluster slows the customer-facing app. Reduce impact with minimal changes."* → **Point the Glue JDBC connection at the Aurora reader endpoint** (replicas share storage and absorb the read load; scaling the writer costs more and still competes).

> *"Analysts need near-real-time analytics on Aurora MySQL order data in Redshift. The team does not want to build or maintain ETL pipelines."* → **Aurora zero-ETL integration with Amazon Redshift** (DMS CDC works but it's a pipeline to operate).

> *"Archive an entire RDS PostgreSQL database monthly to S3 in a columnar format for occasional Athena queries, without load on the instance."* → **Snapshot export to S3 (Parquet)**.

> *"A spike of 3,000 concurrent Lambda invocations causes 'too many connections' errors on RDS MySQL."* → **RDS Proxy** (pooling), optionally with Lambda reserved concurrency (a bigger instance just moves the ceiling).

> *"An ETL process running a single 40-minute UPDATE on a PostgreSQL table causes application timeouts. Which approach reduces blocking?"* → **Process in small batches with frequent commits and set lock_timeout** (long transactions hold row locks and stall VACUUM).

> *"A DBA needs to identify which session is blocking others on Aurora PostgreSQL."* → **Query pg_stat_activity with pg_blocking_pids() (or use Performance Insights lock-wait view)**.

> *"A retail app on Aurora PostgreSQL wants semantic product search combining embedding similarity with SQL filters on price and stock, with low latency at high recall."* → **pgvector with an HNSW index (e.g., `vector_cosine_ops`)** (vectors live beside the relational data; IVFFlat trades recall for build speed).

> *"A Bedrock Knowledge Base must use a vector store the company already operates and can query with SQL."* → **Aurora PostgreSQL with pgvector**.

> *"A payment service needs microsecond read latency for account balances, and the data must not be lost if a node fails. It will be the primary database."* → **Amazon MemoryDB** (durable Multi-AZ transaction log; ElastiCache is a cache, not a system of record).

> *"A fraud team must find rings of accounts connected through shared devices, phone numbers and addresses up to four hops away."* → **Amazon Neptune** (graph traversal with Gremlin/openCypher; relational self-joins explode).

> *"Load 2 billion relationship edges from CSV files in S3 into a graph database as fast as possible."* → **Neptune bulk loader** (loader endpoint + IAM role + S3 VPC endpoint).

> *"An on-premises Cassandra time-series workload must move to AWS without managing servers and with no application rewrite."* → **Amazon Keyspaces** (CQL-compatible, serverless).

> *"A content platform stores highly nested, varying JSON documents and uses the MongoDB driver."* → **Amazon DocumentDB** (MongoDB-compatible); change streams can feed downstream pipelines.

> *"A dev team needs a full copy of a 10 TB production Aurora cluster for testing every morning, fast and cheap."* → **Aurora fast cloning** (copy-on-write; you pay only for changed pages) rather than snapshot restore.

> *"An internal reporting database is used only a few hours a week, unpredictably. Minimize cost while keeping PostgreSQL compatibility."* → **Aurora Serverless v2 with minimum 0 ACUs (auto-pause)**.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Managed relational, commercial engines | RDS |
| 6 copies / 3 AZs, up to 256 TiB | Aurora storage |
| Up to 15 low-lag replicas, reader endpoint | Aurora Replicas |
| Scale in ACUs, down to 0 / auto-pause | Aurora Serverless v2 (0–256 ACU) |
| Cross-Region DR, sub-second replication | Aurora Global Database |
| Instant test copy | Aurora fast cloning |
| Rewind MySQL cluster in place | Aurora Backtrack |
| I/O-heavy bill | Aurora I/O-Optimized |
| Extract without hurting prod | Read replica / reader endpoint |
| DB → S3 Parquet, zero load | Snapshot export to S3 |
| CDC to lake/warehouse | DMS (full load + CDC) |
| Analytics copy, no pipeline | Zero-ETL → Redshift |
| Query RDS from Redshift / Athena in place | Redshift federated query / Athena federated query |
| Lambda connection storms | RDS Proxy (+ Secrets Manager/IAM auth) |
| Read overload | Replicas, replica auto scaling, cache |
| PostgreSQL default isolation | READ COMMITTED |
| MySQL InnoDB default isolation | REPEATABLE READ |
| Lock rows before updating | SELECT … FOR UPDATE (FOR SHARE for shared) |
| Queue workers without collisions | FOR UPDATE SKIP LOCKED |
| Lock wait limit | lock_timeout (PG) / innodb_lock_wait_timeout (MySQL, 50 s) |
| Find blockers | pg_blocking_pids / pg_locks / sys.innodb_lock_waits |
| DB load by wait event | Performance Insights (CloudWatch Database Insights) |
| Long transaction side effect (PG) | Blocks VACUUM → bloat |
| Vectors + SQL in Aurora | pgvector (HNSW or IVFFlat) |
| Cosine / L2 / inner product ops | `<=>` / `<->` / `<#>` (negative IP) |
| IVFFlat build timing | After data is loaded (lists/probes) |
| Durable in-memory primary DB, µs reads | MemoryDB (Valkey/Redis OSS; vector HNSW/FLAT) |
| Cache in front of DB | ElastiCache (DAX for DynamoDB) |
| MongoDB-compatible documents | DocumentDB (change streams, elastic clusters) |
| Cassandra / CQL serverless | Keyspaces |
| Graph: fraud rings, knowledge graph, lineage | Neptune (Gremlin/openCypher/SPARQL) |
| Graph algorithms + vector search | Neptune Analytics |
| Bulk graph load from S3 | Neptune bulk loader |
| Timestream LiveAnalytics (new customers) | Closed since June 2025 → Timestream for InfluxDB |

Relational and purpose-built stores answer precise questions fast. When the question becomes *"search every log line and document for these words, right now"*, you need an index built for search: [Guide 29 — OpenSearch Service](29-OpenSearch-Service.md).
