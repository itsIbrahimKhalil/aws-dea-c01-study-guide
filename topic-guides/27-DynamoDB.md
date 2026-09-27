# 27 · Amazon DynamoDB — the coat check that never loses a ticket

> **Exam map:** D1 · Task 1.1 — D2 · Task 2.1, 2.3, 2.4 · **Skills:** 1.1.1, 1.1.9, 2.1.1, 2.1.2, 2.3.4, 2.4.1 · **Weight:** 🔥🔥🔥 High · **Read time:** ~20 min

## The idea

Think of a giant **coat check** at a stadium. You hand over your coat and get a **ticket number**. Behind the counter are hundreds of **attendants**, each in charge of a range of hooks. A hashing rule decides which attendant gets which ticket, so the work spreads out. Hand in your ticket and your coat comes back in a blink, whether there are ten coats in the building or ten billion. But ask *"show me every blue coat bought in Italy"* and the whole system groans, because nobody filed the coats that way.

That's **Amazon DynamoDB**: a **serverless NoSQL key-value and document database** with **single-digit-millisecond** latency at any scale. The **ticket** is the **primary key**. The **attendants** are **partitions**, internal storage-and-throughput slices that DynamoDB creates and splits for you. You never patch, size or fail over a server. In return you must **design around your access patterns up front**, because DynamoDB is only fast for the questions you planned for.

For a data engineer, DynamoDB shows up in three roles: as a **source** (Streams, exports, zero-ETL into the lake or warehouse), as a **fast serving store** for pipeline outputs, state and metadata, and as a **throttling puzzle** (skill 1.1.9). This guide gives you the capacity math, key design, index traps, stream and TTL patterns, and the "get it into analytics" options.

## Tables, items, keys

- A **table** holds **items** (rows). Items hold **attributes**, and the schema is flexible: only the key attributes are required.
- **Max item size: 400 KB**, attribute names included. Bigger payloads (images, documents, large JSON) → **store the object in S3 and keep a pointer** (the S3 key plus metadata) in the item.
- **Primary key**, one of two shapes:
  - **Partition key only** (simple key). Its hash decides the partition. `GetItem` by key.
  - **Partition key + sort key** (composite key). Items that share a partition key form an **item collection**, stored sorted by sort key. `Query` gets ranges (`begins_with`, `between`, `<`, `>`) within one partition key.
- Partition key values can be up to **2,048 bytes**, sort keys up to **1,024 bytes**.

## Partitions and hot keys: why the key choice is everything

Every partition has a hard ceiling of **3,000 read units/s and 1,000 write units/s** (and around **10 GB** of data). DynamoDB adds partitions as throughput or storage grows. Your table can have a million units of capacity and still throttle if all traffic lands on **one partition key value**. Picture one attendant with a queue out the door while the others stand idle.

DynamoDB softens this in two ways:
- **Burst capacity** saves up to **300 seconds** of unused provisioned capacity for short spikes.
- **Adaptive capacity** (always on, free) instantly shifts throughput toward hot partitions and can isolate a very hot item onto its own partition. It still **can't exceed the 3,000/1,000 per-partition maximum**.

Fixes for hot partitions, in order of preference:
1. **Choose a high-cardinality partition key** that spreads requests evenly: `customer_id`, `device_id`, `order_id`. Avoid `status`, `country`, or today's date.
2. **Write sharding**: add a suffix to spread one logical key across many physical keys.
   - **Random suffix** (`2026-09-26#1` … `#200`) gives the best spread, but reads must query **all** suffixes and merge the results.
   - **Calculated suffix** (hash of `order_id` mod 200) spreads almost as well, and you can compute the suffix to `GetItem` a single record.
3. Cache hot reads with **DAX** (below).
4. Find the culprit with **CloudWatch Contributor Insights**, which shows the most-accessed and most-throttled keys.

**THE trap:** *"the table has plenty of provisioned capacity but still throttles"* → **a hot partition key**. Raising table capacity doesn't help, because the per-partition ceiling is the limit. Fix the key design or shard writes.

## Capacity math (you will be asked to compute this)

| Unit | Definition |
|---|---|
| **1 RCU** | **1 strongly consistent read/s** of an item up to **4 KB**, or **2 eventually consistent reads/s** |
| **Transactional read** | **2 RCUs** per 4 KB |
| **1 WCU** | **1 write/s** of an item up to **1 KB** |
| **Transactional write** | **2 WCUs** per 1 KB |

Always **round the item size up** to the next 4 KB (reads) or 1 KB (writes) *per item*.

**Worked example.** Items are **6 KB**. The app needs **120 strongly consistent reads/s** and **40 writes/s**.
- Reads: 6 KB → ceil(6/4) = **2 RCU** per read → 120 × 2 = **240 RCU**. Eventually consistent: half of that = **120 RCU**. Transactional: double = **480 RCU**.
- Writes: 6 KB → ceil(6/1) = **6 WCU** per write → 40 × 6 = **240 WCU**. Transactional: **480 WCU**.
- If each item were **6.2 KB**, a write would cost 7 WCU (round up), so 280 WCU.

On-demand mode uses the same arithmetic, but the units are called **read/write request units** and you're billed per request.

### Capacity modes

| | **On-demand** (default, recommended) | **Provisioned** (+ auto scaling) |
|---|---|---|
| Billing | Per request (RRU/WRU) | Per provisioned RCU/WCU-hour, used or not |
| Scaling | Instantly handles up to **2× the previous peak**. New tables start able to sustain **4,000 writes/s and 12,000 reads/s** | Auto scaling (Application Auto Scaling target tracking) adjusts toward a target utilization, with a lag of minutes |
| Throttles when | Traffic **more than doubles the previous peak within 30 minutes**, a per-partition hot key, or an optional **max throughput** cap you set | Consumption exceeds provisioned + burst, or a hot partition |
| Cheapest for | **Unpredictable, spiky, new, or low-utilization** workloads | **Steady, predictable** traffic, especially with **reserved capacity** (1- or 3-year commitment) |

- You can switch **provisioned → on-demand up to 4 times per 24 hours**, and on-demand → provisioned at any time.
- On-demand throughput prices were **cut roughly in half in Nov 2024**, which makes on-demand the right default for most workloads today.
- **Warm throughput** shows how much traffic a table or GSI can absorb *instantly*. You can **pre-warm** (raise it, for a one-time charge) before a known spike such as a product launch or a big migration backfill, without switching capacity mode. Warm throughput can't be lowered once raised.
- Default quotas: **40,000 read and 40,000 write units per table** (adjustable).

### Throttling toolkit (skill 1.1.9)

`ProvisionedThroughputExceededException` (or `ThrottlingException`) → the **AWS SDKs retry automatically with exponential backoff and jitter**. Keep that on, and add backoff in batch loaders (`BatchWriteItem` returns `UnprocessedItems`, which you must retry). Then work through the menu: fix hot keys, enable auto scaling, switch to on-demand, pre-warm, put **DAX** in front of read-heavy traffic, and smooth bulk loads with an **SQS buffer** or a rate limit in the writer (for example a Glue job's write-percent setting).

**THE trap:** *"a nightly bulk load throttles and starves the production app"* → throttle the writer (SQS buffer, lower parallelism, backoff), use **Import from S3** into a *new* table, or pre-warm. Don't answer "more RCUs" for a write problem.

## Secondary indexes

| | **GSI (global secondary index)** | **LSI (local secondary index)** |
|---|---|---|
| Keys | **Any** partition key + optional sort key | **Same partition key**, different sort key |
| When created | **Any time** (backfills) | **Only at table creation** |
| Consistency | **Eventually consistent only** | Eventual **or strongly consistent** |
| Capacity | **Its own** RCU/WCU (provisioned) | Shares the base table's |
| Limits | Default **20 per table** | **5 per table**. Item collection capped at **10 GB** per partition key value |

- **Projections** control which attributes are copied into the index: `KEYS_ONLY`, `INCLUDE` (named attributes) or `ALL`. Queries that need non-projected attributes can't fetch them from a GSI, so project what your access pattern reads. Smaller projections mean cheaper index writes and storage.
- **Sparse index**: only items that *have* the index key attribute appear in the index. Example: a GSI on `open_ticket_flag`, set only on open tickets, lets you find "all open tickets" without scanning every closed one.
- **Index overloading**: in single-table designs, one GSI with generic key names (`GSI1PK`/`GSI1SK`) serves several access patterns.

**THE trap:** **an under-provisioned GSI throttles writes to the base table.** Every base-table write that touches indexed attributes must also write to the GSI. If the GSI can't keep up, DynamoDB throttles the *table* write. *"Writes throttle even though the table's WCU is fine"* → **check the GSI's write capacity** (or use on-demand).

**THE trap:** needing **strongly consistent** reads on an alternate key → **LSI**, which is only possible if you defined it at table creation. Otherwise it means a new table and a migration. GSIs are always eventually consistent.

## Query, Scan and filter expressions

- **`Query`**: needs a partition key value. Optionally a sort-key condition. Reads only that item collection. This is the efficient path.
- **`Scan`**: reads **every item** in the table or index. It's expensive and slow. A **parallel scan** (`Segment` / `TotalSegments`) speeds up full exports but burns capacity faster. For analytics, use **Export to S3** instead.
- Both return at most **1 MB per call**. Paginate with **`LastEvaluatedKey`**.
- **`ProjectionExpression`** trims the attributes returned (network), **not** the capacity consumed.

**THE trap:** **filter expressions don't reduce consumed capacity.** `FilterExpression` is applied **after** items are read, so you pay RCUs for everything read before the filter, and the 1 MB page limit also applies *before* filtering (pages can come back nearly empty). If a filter throws away most items, the access pattern needs a **better key or a GSI**, not a filter.

## DynamoDB Streams and Kinesis: change data capture (skill 1.1.1)

**DynamoDB Streams** is an ordered log of item-level changes (`INSERT`, `MODIFY`, `REMOVE`):
- **24-hour retention**. Records appear **in order per item** and **exactly once** (no duplicates within the stream).
- **Stream view type**: `KEYS_ONLY`, `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`.
- Design for **no more than 2 simultaneous readers per shard** (1 for global tables). That's why fanning out to many consumers calls for something else.
- The classic consumer is a **Lambda trigger** (event source mapping with batch size, batching window, bisect-on-error, on-failure destination, event filtering). Details in [Guide 17](17-Lambda-for-Data-Pipelines.md).

**Kinesis Data Streams for DynamoDB** sends the same change records to a **Kinesis data stream you own**:

| | DynamoDB Streams | Kinesis Data Streams for DynamoDB |
|---|---|---|
| Retention | 24 hours | Up to **1 year** (365 days) |
| Consumers per shard | 2 | **5** shared, or **20 with enhanced fan-out** |
| Ordering / duplicates | Per-item order, no duplicates | **May be out of order and may have duplicates**. Use the record's approximate creation timestamp and idempotent consumers |
| Downstream | Lambda, KCL adapter | Lambda, **Firehose → S3/Redshift/OpenSearch**, Managed Flink, Glue streaming |

**THE trap:** *"replay changes from last week"* or *"five teams must consume the change feed"* → **Kinesis Data Streams for DynamoDB** (longer retention, more consumers). *"Strict per-item order, no duplicates, simple Lambda trigger"* → **DynamoDB Streams**.

Common pipeline: **DynamoDB → Kinesis Data Streams → Firehose → S3 (Parquet)** gives a near-real-time CDC archive for Athena ([Guide 06](06-Kinesis-Data-Streams.md), [Guide 07](07-Amazon-Data-Firehose.md)).

## TTL: expiring items for free (skill 2.3.4)

**Time to Live**: pick an attribute (for example `expireAt`) that holds a **Number in Unix epoch seconds**. When the time passes, a background process deletes the item, **typically within a few days**, and **consumes no WCUs**.
- Expired-but-not-yet-deleted items **still show up in reads**. Add a filter (`expireAt > :now`) if exactness matters. They also still count toward storage.
- An attribute that isn't a Number (or is in milliseconds by mistake) is ignored. Milliseconds read as dates tens of thousands of years in the future, so the item never expires.
- TTL deletes flow through **DynamoDB Streams** as `REMOVE` records with **`userIdentity.type = "Service"`** and **`principalId = "dynamodb.amazonaws.com"`**. That lets you tell them apart from user deletes.
- They're removed from GSIs/LSIs too, and replicated to global table replicas (replicated deletes are billed as replicated writes).

**Pattern: archive before it disappears.** **Streams → Lambda (filter on the TTL service identity) → S3 / Firehose**. You keep hot data small in DynamoDB and cold history cheaply in S3 for Athena. That's the exam's answer to *"delete sessions after 30 days but keep a copy for compliance."*

**THE trap:** *"items must be removed at exactly midnight"* → TTL **isn't precise**. Use a scheduled job (EventBridge Scheduler → Lambda doing deletes) or filter on read. TTL is the answer for *"automatically, at no cost, eventually."*

## Getting DynamoDB data into analytics

| Option | How | Pick when |
|---|---|---|
| **Export to S3** (full or incremental) | Needs **PITR enabled**. Reads from the backup, so **no RCUs consumed** and no impact on the table. Output: **DynamoDB JSON or Amazon Ion**. Incremental windows are **15 min to 24 h** | Periodic snapshots into the lake. Query with Athena or transform with Glue into Parquet/Iceberg |
| **Zero-ETL → Amazon Redshift** | Managed, continuous replication, landing within minutes | SQL analytics/joins in the warehouse without pipelines ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)) |
| **Zero-ETL → SageMaker lakehouse** (S3 / Iceberg) | Built on exports, refreshing roughly every 15–30 min, no table capacity used | Lakehouse analytics/ML on operational data |
| **Zero-ETL → OpenSearch** (via **OpenSearch Ingestion**) | PITR export for the initial load + Streams for changes | Full-text/fuzzy search over DynamoDB items ([Guide 29](29-OpenSearch-Service.md)) |
| **Athena federated query** | DynamoDB connector | Occasional ad hoc joins with S3 ([Guide 26](26-Amazon-Athena.md)) |
| Glue ETL DynamoDB connector | Reads the table (consumes RCUs; set read percent) or an export | Custom transforms |
| **Import from S3** | **CSV, DynamoDB JSON or Ion** (optionally GZIP/ZSTD) into a **new table only**. **No WCUs consumed** | Bulk-loading or migrating large datasets cheaply |

**THE trap:** *"analysts need ad hoc SQL, joins and aggregations over DynamoDB data without affecting production"* → **Export to S3 + Athena** or **zero-ETL to Redshift**. A `Scan` from a reporting tool eats production capacity, and DynamoDB can't do joins.

## Performance and resilience features

- **DAX (DynamoDB Accelerator)**: an in-memory, **write-through cache cluster in your VPC** with a DynamoDB-compatible API. Reads drop from milliseconds to **microseconds**. An **item cache** serves GetItem/BatchGetItem, and a **query cache** serves Query/Scan results. It only helps **eventually consistent** reads: **strongly consistent reads pass straight through to DynamoDB**. Best for read-heavy, repeated keys (hot products, leaderboards). It doesn't help write throttling. A cluster has 1 primary + up to 10 read replicas.
- **Global tables**: **multi-active** replicas in several Regions, each accepting writes. The default is **multi-Region eventual consistency** (conflicts resolved by **last writer wins**, replication typically within a second). An optional **multi-Region strong consistency** mode (2025) gives zero-RPO reads of the latest write in any Region at higher write latency. Transactions are ACID only within the originating Region. Use them for DR, low-latency global users, and data residency designs ([Guide 31](31-Data-Lifecycle-Retention-Resiliency.md)).
- **Backups**: **PITR** (continuous, restore to any second in a recovery window of up to **35 days**) restores into a **new table**. **On-demand backups** are kept until deleted. **AWS Backup** adds cross-Region/cross-account copies, vault lock and centralized policies.
- **Table classes**: **Standard** (default) vs **Standard-IA**. Standard-IA has much lower storage price but higher per-request price, so pick it for **storage-dominated** tables of rarely read data (old orders, logs kept in DynamoDB).
- **Transactions**: `TransactWriteItems` / `TransactGetItems` give ACID across up to **100 items / 4 MB**, at **2×** capacity cost. Use them for "debit A and credit B together."
- **Optimistic locking**: keep a `version` attribute and write with `ConditionExpression: version = :expected`, incrementing it on success. A conflicting writer gets `ConditionalCheckFailedException` and retries. Conditional writes are also how you make pipeline loads **idempotent** (`attribute_not_exists(pk)`).
- **PartiQL**: SQL-compatible `SELECT/INSERT/UPDATE/DELETE` syntax for DynamoDB. A `SELECT` without a key condition is still a **Scan**, and PartiQL doesn't add joins.

> ⚠️ **2026 status:** DynamoDB added native **vector indexes and a `SearchVectors` API** (GA Aug 2026, on-demand tables only). That's newer than exam guide v1.1, so expect vector-store answers on the exam to remain OpenSearch, Aurora pgvector, MemoryDB and S3 Vectors ([Guide 19](19-GenAI-LLMs-Vectors.md)).

## Data modeling: access patterns first (skill 2.4.1)

Relational design starts from entities. DynamoDB design starts from the **questions**: list every access pattern ("get customer profile", "list a customer's orders newest first", "orders by status for the warehouse") and shape keys and GSIs to answer each with one `GetItem` or `Query`.

**Single-table design** stores several entity types in one table with **overloaded, prefixed keys**:

| PK | SK | Other attributes |
|---|---|---|
| `CUSTOMER#123` | `PROFILE` | name, tier, email |
| `CUSTOMER#123` | `ORDER#2026-09-26#A17` | total, status, `GSI1PK=STATUS#SHIPPED` |
| `CUSTOMER#123` | `ORDER#2026-09-20#A09` | total, status |
| `ORDER#A17` | `ITEM#001` | sku, qty |

- *"Customer + all orders"* → `Query PK = CUSTOMER#123` (one request fetches the pre-joined item collection).
- *"Orders in September"* → `Query PK = CUSTOMER#123 AND begins_with(SK, 'ORDER#2026-09')` (composite sort key).
- *"All shipped orders"* → GSI on `GSI1PK`.
- **Adjacency lists** (PK = node, SK = related node) model many-to-many relationships and simple graphs.
- **Time series**: **time-bucketed keys** (`SENSOR#9#2026-09-26`) keep partitions bounded and avoid one ever-growing hot key. Often a new table per period, with TTL to age out old data.

**When NOT to use DynamoDB**: ad hoc analytics, multi-table joins, complex aggregations, access patterns nobody can predict, full-text search. Keep DynamoDB as the operational store and **export or zero-ETL** the data to S3/Athena, Redshift or OpenSearch ([Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md)).

## Security

- **Encryption at rest is always on**, with three key choices: **AWS owned key** (default, free), **AWS managed key** (`aws/dynamodb`), or a **customer managed KMS key** (your key policy and rotation, CloudTrail audit of key use). TLS in transit.
- **Fine-grained IAM**: the condition key **`dynamodb:LeadingKeys`** restricts a user to items whose partition key equals their identity (for example `${aws:PrincipalTag/tenant}`, or a Cognito identity ID). `dynamodb:Attributes` limits which attributes they can see. Tables also support **resource-based policies** for cross-account access ([Guide 37](37-IAM-for-Data-Engineers.md)).
- **Network**: a **gateway VPC endpoint** (free) keeps traffic from private subnets on the AWS network. Interface endpoints (PrivateLink) exist for on-premises/cross-VPC access. DAX lives inside your VPC.
- CloudTrail logs control-plane calls by default. Data-plane events (GetItem/PutItem) are opt-in data events.

## Question patterns

> *"A gaming app's DynamoDB table uses `game_id` as partition key. During a popular tournament, writes throttle even though consumed WCU is well below the table's provisioned WCU."* → **Hot partition, so redesign the key (high cardinality, e.g. `player_id`) or add write sharding** (per-partition 1,000 WCU ceiling; raising table WCU doesn't help).

> *"Items average 7 KB. The app performs 200 eventually consistent reads per second. How many RCUs are required?"* → **200 RCU** (7 KB rounds to 8 KB = 2 RCU strongly consistent, halved for eventual = 1 RCU per read × 200).

> *"A new product's traffic is unknown and spiky, with long idle periods. Minimize cost and operational effort."* → **On-demand capacity mode** (pay per request, no capacity planning; provisioned with auto scaling lags and bills idle capacity).

> *"Traffic is steady at ~5,000 WCU around the clock for the next three years. Most cost-effective?"* → **Provisioned capacity with reserved capacity** (steady, predictable use is the one place on-demand loses).

> *"After adding a GSI with low provisioned write capacity, the base table's writes began throttling."* → **Increase the GSI's WCU (or switch to on-demand)** (GSI back-pressure throttles base-table writes).

> *"A query needs strongly consistent reads by `customer_id` sorted by `order_date`, but the table's sort key is `order_id`. The table already exists."* → **A new table with an LSI (created at table creation), migrated via export/import** (a GSI can't do strong consistency; an LSI can't be added later).

> *"A developer applies a FilterExpression to a Query and is surprised that consumed RCUs didn't drop."* → **Filters apply after items are read. Redesign with a sort-key condition or a (sparse) GSI.**

> *"Session items must be deleted 24 hours after last activity at no extra cost, but compliance needs a copy of every expired session in S3."* → **TTL + DynamoDB Streams → Lambda filtering on `principalId dynamodb.amazonaws.com` → S3/Firehose** (TTL deletes are free and appear in the stream as service deletions).

> *"Multiple analytics teams must independently consume the table's change feed, and data must be replayable for 7 days."* → **Kinesis Data Streams for DynamoDB** (DynamoDB Streams keeps 24 h and supports ~2 readers per shard); make consumers idempotent because duplicates can occur.

> *"Load the table's full contents into S3 daily for Athena without impacting production throughput. LEAST operational overhead."* → **Enable PITR and use Export to S3 (full or incremental)** (no RCUs consumed; a Glue Scan-based job uses table capacity).

> *"Analysts want near-real-time SQL joins between DynamoDB orders and warehouse dimension tables without building pipelines."* → **DynamoDB zero-ETL integration with Amazon Redshift.**

> *"Users need fuzzy full-text search across product descriptions stored in DynamoDB."* → **Zero-ETL to OpenSearch Service via OpenSearch Ingestion** (DynamoDB has no full-text search).

> *"A read-heavy catalog API hits the same few thousand items constantly and needs microsecond latency."* → **DAX** (item cache; ElastiCache would need custom cache-aside code; DAX won't help strongly consistent reads or writes).

> *"Migrate 2 TB of CSV exports from a legacy system into a new DynamoDB table as cheaply as possible."* → **DynamoDB Import from S3** (new table, no WCU consumed; a BatchWriteItem loader costs write capacity and invites throttling).

> *"Two pipeline workers occasionally overwrite each other's updates to the same item."* → **Optimistic locking with a version attribute and a ConditionExpression** (a conflicting write fails with ConditionalCheckFailedException and retries).

> *"Multi-tenant SaaS: each tenant's users may read only items whose partition key equals their tenant ID."* → **IAM policy with the `dynamodb:LeadingKeys` condition** (fine-grained access control without separate tables).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Item > 400 KB | Store in S3, keep pointer in the item |
| 1 RCU | 1 strong read/s ≤ 4 KB (2 eventual; transactional = 2 RCU) |
| 1 WCU | 1 write/s ≤ 1 KB (transactional = 2 WCU) |
| Round sizes | Up, per item (4 KB reads, 1 KB writes) |
| Per-partition ceiling | 3,000 RCU / 1,000 WCU (~10 GB) |
| Throttles with spare table capacity | Hot partition key → high-cardinality key / write sharding |
| Find the hot key | CloudWatch Contributor Insights |
| Spiky / unknown traffic | On-demand (2× previous peak instantly) |
| Steady, predictable | Provisioned + auto scaling + reserved capacity |
| Known huge event coming | Pre-warm (warm throughput) |
| Throttling in code | SDK exponential backoff + jitter; retry UnprocessedItems |
| Base-table writes throttled after new index | Under-provisioned GSI |
| Alternate key, any time, eventual only | GSI (20 default) |
| Same PK, strong consistency, at creation only | LSI (5 max, 10 GB item collection) |
| Index only some items | Sparse index |
| Filter expression and cost | No RCU saving; fix keys/GSI |
| Page size | 1 MB per Query/Scan → LastEvaluatedKey |
| Full-table read for analytics | Export to S3 (PITR, no RCU), not Scan |
| Change feed, Lambda, per-item order | DynamoDB Streams (24 h, 2 readers/shard) |
| Longer retention / many consumers / Firehose | Kinesis Data Streams for DynamoDB (≤1 yr; dupes possible) |
| Auto-expire items free | TTL (epoch seconds, Number, deleted within days, no WCU) |
| Archive expired items | Streams (service principal identity) → Lambda/Firehose → S3 |
| SQL/joins without pipelines | Zero-ETL → Redshift |
| Lakehouse (Iceberg) copy | Zero-ETL → SageMaker lakehouse |
| Full-text search on items | Zero-ETL → OpenSearch (OpenSearch Ingestion) |
| Bulk load cheaply | Import from S3 (new table, CSV/DDB JSON/Ion) |
| Microsecond reads | DAX (eventual reads only; write-through) |
| Multi-Region active-active | Global tables (LWW; optional multi-Region strong consistency) |
| Restore to any second | PITR (up to 35 days, into a new table) |
| Rarely read, storage-heavy table | Standard-IA table class |
| All-or-nothing multi-item write | Transactions (100 items / 4 MB, 2× cost) |
| Prevent lost updates | Optimistic locking (version + condition expression) |
| Row-level tenant isolation | `dynamodb:LeadingKeys` |
| Private access from VPC | Gateway VPC endpoint |
| SQL-like syntax | PartiQL (still Scan without key) |

DynamoDB is the purpose-built champion for key-value access. Relational engines, graphs, documents and in-memory stores each win other access patterns, and those are next in [Guide 28 — RDS, Aurora & Purpose-Built Databases](28-RDS-Aurora-Purpose-Built-DBs.md).
