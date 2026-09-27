# 05 · S3 Data Lake Storage — the container port every pipeline docks at

> **Exam map:** D2 · Task 2.1, 2.3 — D1 · Task 1.1 — D4 · Task 4.1 · **Skills:** 2.1.3, 2.3.2, 2.3.3, 2.3.4, 2.3.6, 1.1.6, 4.1.5 (also supports 2.3.5, 2.4.5, 4.5.3) · **Weight:** 🔥🔥🔥 High · **Read time:** ~30 min

## The idea

Nearly every DEA-C01 architecture touches Amazon S3 (Simple Storage Service): it's the landing zone, the lake, the staging area for Redshift, the archive, and the log sink. You won't be asked what S3 *is*; you'll be asked which storage class, which lifecycle rule, why a query throttles, how to react when a file lands, how to prove data can't be altered, and how to give ten teams different access to one bucket.

The analogy: a **container port**. Each **object** is a shipping container, and its **key** (like `raw/sales/year=2026/month=09/day=26/part-0001.parquet`) is the label painted on its side. A **bucket** is one fenced yard, and **prefixes** are the **lanes** of stacked containers — each lane has its own cranes, so the more lanes you use, the faster the whole yard moves. Containers near the dock are instantly reachable but expensive to store (S3 Standard); containers trucked to an **inland depot** are cheap but must be **fetched back** before you can open them (Glacier restores). The **yard manager's schedule** moves ageing containers inland and scraps expired ones (lifecycle rules). A **customs seal** means nobody may open or scrap a container until a date (Object Lock). A **duplicate yard in another port** keeps copies (replication). An **arrival bell** rings when a container lands (event notifications), a **nightly manifest** lists every container (S3 Inventory), and a **work crew** can relabel or move millions of containers from that manifest (Batch Operations). Each shipping company gets **its own gate** with its own rules (access points).

After this guide you can answer storage-class, lifecycle, versioning, replication, WORM, partition-layout, throttling, event-trigger, inventory/bulk-operation and access-control questions.

## Fundamentals a data engineer must know

- **Objects, keys, prefixes**: S3 is a flat key-value store; "folders" are just key prefixes (`raw/sales/`). You replace objects whole — no editing bytes in place (appends exist only in S3 Express One Zone directory buckets).
- **Durability**: designed for **99.999999999% (11 nines)**; Standard stores data across **at least three Availability Zones** (One Zone classes use one).
- **Strong read-after-write consistency** for PUTs, overwrites, deletes and LIST — a job that lists a prefix right after writing sees the new objects. (Old "eventual consistency" questions are obsolete.)
- **Object size**: maximum **50 TB** (**48.8 TiB**, raised from 5 TB in Dec 2025); a **single PUT tops out at 5 GB**; **multipart upload** — up to **10,000 parts** of **5 MiB–5 GiB** — is recommended from about **100 MB** and required above 5 GB. Failed parts retry independently; **abort incomplete uploads** with lifecycle.
- **Conditional writes**: `If-None-Match: *` refuses to overwrite an existing key; `If-Match` with an ETag gives optimistic concurrency — handy for idempotent writers and lock files.
- **Byte-range fetches**: GET part of an object (`Range` header) to parallelize downloads or read just a Parquet footer.
- **Transfer Acceleration**: uploads from distant clients enter at CloudFront edge locations and ride the AWS backbone to one bucket.

## Storage classes

| Class | Min duration | Min billable size | AZs | Access | Pick when |
|---|---|---|---|---|---|
| **S3 Standard** | None | — | ≥3 | ms | Hot data, active lake zones, staging |
| **S3 Intelligent-Tiering** | None | Objects < **128 KB** not monitored (stay Frequent) | ≥3 | ms (optional archive tiers need restore) | **Unknown or changing** access; per-object monitoring fee |
| **S3 Standard-IA** | **30 days** | **128 KB** | ≥3 | ms + per-GB retrieval fee | Infrequent, must be instant |
| **S3 One Zone-IA** | **30 days** | **128 KB** | **1** | ms | Infrequent + **re-creatable** (derived data, secondary copies) |
| **S3 Glacier Instant Retrieval** | **90 days** | **128 KB** | ≥3 | **ms** | Archive read ~once a quarter, needs instant access |
| **S3 Glacier Flexible Retrieval** | **90 days** | 40 KB metadata overhead per object | ≥3 | Restore: **Expedited 1–5 min**, **Standard 3–5 h**, **Bulk 5–12 h** (free) | Archive, minutes-to-hours OK |
| **S3 Glacier Deep Archive** | **180 days** | 40 KB overhead | ≥3 | Restore: **Standard ≤ 12 h**, **Bulk ≤ 48 h** | Cheapest; compliance retention, rarely/never read |
| **S3 Express One Zone** | — | — | **1 (you choose the AZ)** | **Single-digit ms**, very high request rates | Hot, latency-sensitive analytics/ML scratch data co-located with compute |

**Intelligent-Tiering tiers**: Frequent → **Infrequent after 30** days without access → **Archive Instant Access after 90** days (all automatic, all millisecond access). Optional, opt-in **Archive Access** (after 90–730 days) and **Deep Archive Access** (after 180–730 days) tiers behave like Glacier Flexible/Deep Archive — you must restore before reading. No retrieval fees; objects move back to Frequent when accessed.

**S3 Express One Zone** lives in **directory buckets** (names like `analytics-scratch--use1-az4--x-s3`), is **single-AZ**, and is authorized with session-based credentials (`CreateSession`) for low latency. It's the answer for *"single-digit millisecond access"*, *"hot intermediate data for Spark/EMR/SageMaker training in the same AZ"*, *"very high request rates on small objects"*. Lifecycle on directory buckets supports **expiration and aborting multipart uploads only** (no transitions). Not a durability tier — lose the AZ, lose the data.

**Glacier restores**: a restore creates a **temporary copy** (you pay Standard storage for it for the days requested) while the archive stays put. Athena and Redshift Spectrum can't read Glacier Flexible/Deep Archive objects until restored (Athena can be told to read restored objects). Bulk restores of millions of objects → **S3 Batch Operations** restore job.

**THE trap:** Deep Archive **can never beat 12 hours** (Standard restore). *"Must be retrievable within 4 hours"* rules it out; Glacier Flexible Standard (3–5 h) or Expedited (1–5 min) fits. *"Instantly"* rules out both — use Glacier Instant Retrieval.

**THE trap:** transitioning **millions of tiny objects** to IA/Glacier can cost more than it saves — transition requests are charged per object, IA/Instant bill a 128 KB minimum, and Glacier Flexible/Deep add 40 KB overhead each. Aggregate small files first (see [Guide 03](03-Data-Formats-Compression.md)).

## Lifecycle mechanics (skills 2.3.2, 2.3.3)

A **lifecycle configuration** (up to **1,000 rules** per bucket) runs asynchronously, roughly daily. Each rule has a **filter** — prefix, one or more **tags**, and/or **object size** (`ObjectSizeGreaterThan` / `ObjectSizeLessThan`) — and actions:

| Action | What it does |
|---|---|
| **Transition** (current versions) | Move to a colder class after N days (or on a date) |
| **Expiration** (current versions) | Delete after N days; in a **versioned** bucket this just adds a **delete marker** (the data becomes noncurrent, still billed) |
| **NoncurrentVersionTransition** | Move old versions to colder classes |
| **NoncurrentVersionExpiration** | Permanently delete old versions `NoncurrentDays` after they became noncurrent; `NewerNoncurrentVersions` keeps the latest N (up to **100**) old versions |
| **ExpiredObjectDeleteMarker** | Remove delete markers that have no versions left behind them |
| **AbortIncompleteMultipartUpload** | Clean up abandoned multipart parts after N days (they're invisible in listings but billed) |

Rules the exam likes:

- Objects must sit **at least 30 days** in their current class before a transition to **Standard-IA or One Zone-IA**, and you can't chain transitions faster than a class's minimum duration in one rule (e.g., into Glacier Instant at day 4 then Deep Archive at day 20 isn't allowed — the second must be ≥ day 94).
- The ladder only goes **downhill**: Standard → IA/Intelligent-Tiering/One Zone-IA → Glacier Instant → Glacier Flexible → Deep Archive. Coming back up requires a **restore + copy**.
- **Since September 2024, objects smaller than 128 KB are not transitioned by default** to any class; override with an object-size filter if you really want them moved.
- Lifecycle is the tool for *"delete data after N days"* in S3 (compare **DynamoDB TTL** for items — [Guide 27](27-DynamoDB.md)). Strategy across services — tiering plans, legal holds vs retention, GDPR deletion — is in [Guide 31](31-Data-Lifecycle-Retention-Resiliency.md).

```json
{ "Rules": [{
    "ID": "raw-zone-tiering",
    "Filter": { "Prefix": "raw/" },
    "Status": "Enabled",
    "Transitions": [ { "Days": 30, "StorageClass": "STANDARD_IA" },
                     { "Days": 180, "StorageClass": "DEEP_ARCHIVE" } ],
    "NoncurrentVersionExpiration": { "NoncurrentDays": 30, "NewerNoncurrentVersions": 3 },
    "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 } }] }
```

**THE trap:** *"Lifecycle expiration is configured but storage costs keep growing in a versioned bucket."* Expiring current versions only adds delete markers; the old versions remain. Add **NoncurrentVersionExpiration** (and ExpiredObjectDeleteMarker cleanup).

**Known pattern → lifecycle; unknown pattern → Intelligent-Tiering.** If the question says access *"drops sharply after 30 days"*, a lifecycle rule is cheaper and deterministic; if it says *"unpredictable"*, pick Intelligent-Tiering.

## Versioning, delete markers, MFA Delete (skill 2.3.4)

- Bucket states: unversioned → **enabled** → **suspended** (never back to unversioned).
- Overwrite = new version on top. Delete without a version ID = **delete marker** on top (object "disappears" but every version remains; delete the marker to bring it back). Delete **with** a version ID = permanent removal of that version.
- **MFA Delete**: requires the root user's MFA to permanently delete versions or change versioning state; enabled by the **root user via CLI/API** only.
- Versioning is the prerequisite for **replication** and **Object Lock**. Pair it with noncurrent-version lifecycle rules or costs balloon.

## Replication (skill 2.3.6)

| Feature | Detail |
|---|---|
| **CRR / SRR** | Cross-Region (DR, compliance, latency) / Same-Region (aggregate logs, prod→test, account separation). **Versioning on source and destination**; an IAM role S3 assumes |
| New objects only | Replication applies to objects written **after** the rule exists |
| **S3 Batch Replication** | Replicates **existing** objects, previously failed ones, or re-replicates to new destinations |
| **Replication Time Control (RTC)** | **99.99% of objects within 15 minutes**, backed by an SLA, plus replication metrics and events — the *"predictable, compliance-grade replication time"* answer |
| **Delete marker replication** | Optional (off by default); permanent deletes of specific versions are **never** replicated (protects against malicious deletion) |
| Extras | Replicate to a different storage class (e.g., DR copy straight into Glacier), change replica ownership to destination account, SSE-KMS objects need explicit KMS configuration, two-way replication with replica modification sync |

Keep data inside allowed Regions by *not* configuring replication to disallowed ones and enforcing it with SCPs — see [Guide 42](42-Privacy-PII-Masking-Sovereignty.md).

## Object Lock: WORM for audit data

- **WORM** (write once, read many) per object version; requires **versioning**. Object Lock can be enabled at bucket creation **or on an existing versioned bucket** (the old "creation time only" rule is outdated).
- **Compliance mode**: no one — not even the root user — can delete or shorten retention until it expires.
- **Governance mode**: blocks most users, but principals with `s3:BypassGovernanceRetention` can override (good for testing a policy before going to compliance).
- **Legal hold**: indefinite, no expiry date, independent of retention; removed explicitly (`s3:PutObjectLegalHold`).
- Set a **default retention** on the bucket for new objects; apply retention/legal holds to existing objects with **S3 Batch Operations**.

**THE trap:** *"Audit logs must be immutable for 7 years and even administrators must not be able to delete them"* → **Object Lock in compliance mode** (governance can be bypassed; versioning + MFA Delete doesn't stop an admin with root MFA).

## Partitioning, prefixes and performance

- Request rates scale **per prefix**: at least **3,500 PUT/COPY/POST/DELETE** and **5,500 GET/HEAD** requests per second per partitioned prefix, with no limit on prefixes. S3 scales partitions automatically as load grows — sudden spikes can see **503 Slow Down** until it adapts.
- Fix throttling by **spreading keys across more prefixes** and using **exponential backoff with jitter** (the SDKs retry automatically); reduce request counts by writing bigger files.
- Lay out lakes with **Hive-style partitions** that match query filters: `s3://amzn-s3-demo-bucket/curated/orders/year=2026/month=09/day=26/`. Engines (Athena, Glue, Spectrum, EMR) skip whole prefixes; crawlers and `MSCK REPAIR TABLE` recognize `key=value` folders ([Guide 13](13-Glue-Data-Catalog-Crawlers.md), [Guide 26](26-Amazon-Athena.md)).
- **Avoid high-cardinality partition keys** (user ID, order ID, minute) — they create millions of tiny files and slow planning. Use bucketing or sorting instead ([Guide 03](03-Data-Formats-Compression.md)).
- Keep files in a partition flat (no extra sub-folders), roughly 128 MB–1 GB.

**THE trap:** *"Athena/Spark jobs hit 503 Slow Down on one prefix"* → the fix is more prefixes/partitions, fewer and larger files and backoff — **not** a bigger Athena workgroup or more EMR nodes, which only increase request pressure.

## Event-driven ingestion (skill 1.1.6)

| | **S3 Event Notifications** | **S3 → Amazon EventBridge** |
|---|---|---|
| Targets | **Lambda, SQS (standard), SNS (standard)** | 20+ AWS targets: Step Functions, Lambda, SQS, SNS, Kinesis, API destinations, other buses/accounts |
| Filtering | Key **prefix/suffix** only; overlapping filters for the same event type not allowed | Rich content filtering (key patterns, size, metadata, requester) |
| Fan-out | One destination per event-type/filter combo (use SNS for fan-out) | Many rules, each up to 5 targets |
| Extras | Simple, fastest to set up | **Archive & replay**, schema registry, cross-account routing |
| Enable | Per notification configuration | Turn on "Send notifications to Amazon EventBridge" for the bucket |

Both are **at-least-once** — events are **usually delivered within seconds but may take a minute or longer**, **duplicates can occur**, and **ordering isn't guaranteed** (use the `sequencer` field to order events for the same key). Design consumers to be idempotent. Burst of uploads → S3 → **SQS** → Lambda/Glue decouples and absorbs spikes. Glue crawlers can run in **S3 event mode** (consuming an SQS queue) to crawl only changed prefixes. Details: [Guide 22](22-EventBridge-SNS-SQS.md), [Guide 17](17-Lambda-for-Data-Pipelines.md).

## Seeing and changing billions of objects

- **S3 Inventory**: a **daily or weekly** report per bucket (or prefix) in **CSV, ORC or Parquet**, listing objects with metadata such as size, storage class, last modified, **encryption status**, **replication status**, Object Lock settings, Intelligent-Tiering tier and checksum algorithm. Delivered to a bucket; **query with Athena**. The cheap answer to *"find all unencrypted objects"* or *"how much data is in each class"* — far better than calling LIST on billions of keys.
- **S3 Metadata** (newer): near-real-time object metadata published as queryable Iceberg tables in an AWS managed table bucket — when you need fresher metadata than a daily inventory.
- **S3 Batch Operations**: one operation across billions of objects from a **manifest** (an S3 Inventory report, a CSV, or a generated manifest from filters): **copy** (e.g., to re-encrypt with SSE-KMS or change class), **invoke Lambda** per object, replace/delete **tags**, replace ACLs, **restore** from Glacier, set **Object Lock retention / legal hold**, and **replicate** (Batch Replication). It tracks progress, retries, and writes a **completion report**.
- **S3 Storage Lens**: organization-wide usage and activity dashboards; free metrics, or paid **advanced metrics & recommendations** (prefix-level detail, longer history) to spot cost outliers like incomplete multipart uploads or noncurrent version growth.

**THE trap:** *"Encrypt all existing objects with a new KMS key"* — changing default bucket encryption affects **new** writes only. Use **S3 Inventory → Batch Operations copy** in place with the new key.

## Access control for shared lakes (skill 4.1.5)

- **IAM identity policies + bucket policies** (resource-based, max **20 KB**) for cross-account access and conditions (`aws:SecureTransport`, `aws:SourceVpce`, `s3:x-amz-server-side-encryption`, `aws:PrincipalOrgID`). Evaluation logic and cross-account detail: [Guide 37](37-IAM-for-Data-Engineers.md).
- **Block Public Access**: account- and bucket-level, **on by default** for new buckets; overrides any public policy or ACL.
- **Object Ownership = Bucket owner enforced** (default for new buckets since April 2023): **ACLs disabled**, the bucket owner owns every object even when other accounts write — fixes the classic *"we can't read files another account uploaded"* problem.
- **S3 Access Points**: named network endpoints on a bucket, each with **its own policy** (e.g., one per team or application) and optionally restricted to a **VPC-only** network origin. Delegate access control from the bucket policy to access points so one giant bucket policy doesn't hit the size limit. Multi-Region Access Points route to the nearest replica bucket.
- **S3 Access Grants**: map identities from your **corporate directory via IAM Identity Center** (or IAM principals) to buckets/prefixes/objects with READ, WRITE or READWRITE; applications call `GetDataAccess` to receive **temporary, scoped credentials**. Pick it for *"thousands of users/groups from the IdP need prefix-level S3 access"*. For table/column/row permissions over catalogued data, use **Lake Formation** instead ([Guide 40](40-Lake-Formation.md)).
- **VPC gateway endpoint** for S3 (no charge) keeps traffic private; restrict buckets to it with `aws:SourceVpce` ([Guide 38](38-Networking-for-Data-Pipelines.md)).
- **Requester Pays**: the **requester** pays request and data-transfer costs (bucket owner still pays storage); requests must include the `x-amz-request-payer` header and anonymous access is not allowed — for sharing large public/partner datasets.
- **Presigned URLs**: time-limited access to one object for someone without AWS credentials; inherits the signer's permissions; up to **7 days** with long-term IAM user credentials (shorter when signed with temporary credentials).
- **Encryption** (depth in [Guide 39](39-Encryption-Key-Management.md)): all new objects are encrypted with **SSE-S3 by default**; choose **SSE-KMS** for key control and CloudTrail audit, with **S3 Bucket Keys** to cut KMS request costs (up to 99%); DSSE-KMS for dual-layer requirements. Since **April 2026, SSE-C is disabled by default on new buckets** (re-enable explicitly via the bucket encryption configuration). Enforce TLS by denying `aws:SecureTransport = false`.

## Data lake bucket layout best practices

- Separate **zones** by bucket (strongest isolation: distinct policies, KMS keys, replication and lifecycle) or at least by top-level prefix: `raw/`, `cleansed/`, `curated/`, plus `staging/` (short expiration), `athena-results/` (short expiration) and a separate **logs** bucket.
- **Raw**: versioning on, immutable, lifecycle to IA/Glacier as it ages, maybe Object Lock for regulated sources. **Curated**: Parquet/Iceberg, partitioned for queries, Standard or Intelligent-Tiering.
- Encrypt with **SSE-KMS + Bucket Keys**, a key per zone or data domain; register locations with **Lake Formation** for fine-grained access.
- Tag buckets/objects for cost allocation and ABAC; enable **S3 server access logs or CloudTrail data events** for audit ([Guide 43](43-Audit-Logging-CloudTrail-Config.md)).
- For managed Iceberg tables, consider **S3 Tables** table buckets instead of prefixes ([Guide 04](04-Open-Table-Formats-S3-Tables.md)); for vector embeddings, **Amazon S3 Vectors** (vector buckets, GA Dec 2, 2025, up to 2 billion vectors per index — see [Guide 19](19-GenAI-LLMs-Vectors.md)).

> ⚠️ **2026 status:** **S3 Select** (and S3 Glacier Select) has been **closed to new customers since July 25, 2024**. Older questions answer *"retrieve a subset of rows from one large CSV/JSON object"* with S3 Select; the current answer is **Athena** (or filtering in the client, e.g. with byte-range reads of Parquet). Treat S3 Select as legacy — right only if the question clearly assumes an existing user.

> ⚠️ **2026 status:** **S3 Object Lambda** entered **maintenance (no new customers) on Nov 7, 2025** — for transforming data on read (e.g., redacting PII), prefer processing into a separate curated/redacted copy, Lake Formation filters, or an API layer. The original vault-based **Amazon Glacier** service is also in maintenance since Nov 7, 2025; the **S3 Glacier storage classes are unaffected**.

## Question patterns

> *"Raw sensor files are queried heavily for 30 days, occasionally for a year, and must then be retained 7 years for compliance with retrieval within 48 hours. MOST cost-effective?"* → **Lifecycle: Standard → Standard-IA at 30 days → Glacier Deep Archive at 365 days → expire after 7 years** (known pattern → lifecycle; 48 h tolerance allows Deep Archive Bulk)

> *"A data lake's curated zone has unpredictable access — some datasets are hot for months, others go cold immediately. Minimize storage cost without retrieval delays or operational effort."* → **S3 Intelligent-Tiering** (unknown/changing access; automatic tiers keep millisecond access)

> *"A versioned raw bucket has a 90-day expiration rule, yet storage keeps growing."* → **Add NoncurrentVersionExpiration (and remove expired delete markers)** (expiration in a versioned bucket only creates delete markers)

> *"Thousands of abandoned multipart uploads from failed ingestion jobs are adding cost."* → **Lifecycle rule AbortIncompleteMultipartUpload** (parts are billed but invisible in listings)

> *"Compliance requires a copy of all new objects in another Region within 15 minutes, with monitoring of replication time."* → **CRR with S3 Replication Time Control** (RTC's 15-minute SLA + metrics; plain CRR is best effort)

> *"CRR was enabled on a bucket that already holds 200 TB; the historical objects must also be copied."* → **S3 Batch Replication** (replication rules only apply to new writes)

> *"Financial trade logs must be unchangeable for 7 years; no user, including the root user, may delete them."* → **S3 Object Lock in compliance mode with a 7-year default retention** (governance mode can be bypassed)

> *"A pipeline writes millions of objects under one prefix and Glue jobs intermittently fail with 503 Slow Down."* → **Distribute keys across more prefixes (e.g., partition paths), write fewer larger files, and retry with exponential backoff** (per-prefix request limits: 3,500 writes / 5,500 reads per second)

> *"When a CSV file lands in a raw prefix, a Step Functions workflow must start, but only for files larger than 1 MB whose key matches `raw/sales/*.csv`; events must be replayable after a downstream outage."* → **Enable EventBridge notifications on the bucket; EventBridge rule with content filtering targeting Step Functions; EventBridge archive** (S3 Event Notifications can't target Step Functions directly, filter on size, or replay)

> *"Security must identify every object in a 3-billion-object bucket that isn't encrypted with SSE-KMS, then re-encrypt them, with minimal custom code."* → **S3 Inventory (encryption status) queried with Athena → S3 Batch Operations copy job with SSE-KMS** (LIST calls and scripts don't scale; changing default encryption doesn't touch existing objects)

> *"Twelve analytics teams share one data lake bucket; the bucket policy is near its size limit and each team needs different prefix permissions, some only from their VPC."* → **One S3 access point per team with its own policy and VPC network origin** (delegates control out of the single bucket policy)

> *"A partner account uploads files to the company's bucket, and the company's Glue jobs get Access Denied on them."* → **Set Object Ownership to Bucket owner enforced (ACLs disabled)** (objects written by the partner become owned by the bucket owner)

> *"Employees authenticate through the corporate IdP via IAM Identity Center; thousands of directory groups need read or write access to specific S3 prefixes."* → **S3 Access Grants** (maps directory identities to prefixes and vends temporary credentials; IAM policies per group don't scale)

> *"A research institute shares a 500 TB public dataset but wants downloaders to pay the transfer costs."* → **Requester Pays bucket** (requesters must be authenticated AWS accounts)

> *"A Spark training job on EC2 re-reads small hot files thousands of times per second and needs single-digit millisecond latency; the data can be regenerated."* → **S3 Express One Zone directory bucket in the same AZ as the compute** (single-AZ is acceptable because the data is re-creatable)

> *"An application needs only the rows matching a filter from one 10 GB CSV object in S3, for a new AWS account."* → **Amazon Athena** (S3 Select is closed to new customers since July 2024)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Max object size | 50 TB (48.8 TiB) via multipart; single PUT ≤ 5 GB |
| Multipart | 10,000 parts, 5 MiB–5 GiB each; use from ~100 MB |
| Hot data | S3 Standard |
| Unknown / changing access | Intelligent-Tiering (no min duration; < 128 KB not tiered) |
| Infrequent, instant | Standard-IA (30 d min, 128 KB min) |
| Infrequent, re-creatable | One Zone-IA |
| Archive, ms access | Glacier Instant Retrieval (90 d) |
| Archive, minutes–hours | Glacier Flexible (Expedited 1–5 min / Std 3–5 h / Bulk 5–12 h) |
| Cheapest, ≥ 12 h OK | Glacier Deep Archive (Std ≤ 12 h / Bulk ≤ 48 h, 180 d) |
| Single-digit ms, co-located compute | S3 Express One Zone (directory bucket, 1 AZ) |
| Known ageing pattern | Lifecycle transitions |
| Transition to IA | Only after ≥ 30 days |
| Tiny objects in lifecycle | < 128 KB not transitioned by default (since Sept 2024) |
| Delete after N days | Lifecycle expiration (DynamoDB → TTL) |
| Versioned bucket still growing | NoncurrentVersionExpiration + ExpiredObjectDeleteMarker |
| Abandoned uploads | AbortIncompleteMultipartUpload |
| Accidental delete/overwrite | Versioning (+ MFA Delete) |
| DR copy in another Region | CRR (versioning both sides) |
| Replicate within 15 min, SLA | Replication Time Control |
| Replicate existing objects | S3 Batch Replication |
| WORM, nobody can delete | Object Lock compliance mode |
| WORM, privileged override | Object Lock governance mode |
| Litigation, no end date | Legal hold |
| 3,500 / 5,500 | PUT-class / GET-class requests per second per prefix |
| 503 Slow Down | More prefixes, bigger files, exponential backoff |
| Partition layout | Hive-style `year=/month=/day=` matching filters |
| File lands → Lambda/SQS/SNS | S3 Event Notifications (at-least-once, unordered) |
| Rich filters, many targets, replay | S3 → EventBridge |
| List/audit billions of objects | S3 Inventory (CSV/ORC/Parquet) + Athena |
| Bulk copy/tag/restore/lock/Lambda | S3 Batch Operations (manifest from Inventory) |
| Encrypt existing objects | Inventory → Batch Operations copy |
| Per-team policies, VPC-only | S3 Access Points |
| IdP users → prefix access | S3 Access Grants |
| Cross-account uploads unreadable | Object Ownership: bucket owner enforced |
| Requester pays transfer | Requester Pays |
| Temporary link, no AWS account | Presigned URL (≤ 7 days) |
| KMS request costs high | S3 Bucket Keys |
| SSE-C on new buckets | Disabled by default since April 2026 |
| Subset of rows from one object | Athena (S3 Select closed to new customers) |
| Managed Iceberg tables / vectors in S3 | S3 Tables (Guide 04) / S3 Vectors (Guide 19) |

With the storage layer mastered, move up to how data gets into it in real time: [Guide 06 — Kinesis Data Streams](06-Kinesis-Data-Streams.md).
