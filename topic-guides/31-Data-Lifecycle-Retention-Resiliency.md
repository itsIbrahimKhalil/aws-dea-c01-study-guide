# 31 · Data Lifecycle, Retention & Resiliency — counter, basement, storage unit, shredder, safe

> **Exam map:** D2 · Task 2.3 · **Skills:** 2.3.1, 2.3.2, 2.3.3, 2.3.4, 2.3.5, 2.3.6 · **Weight:** 🔥🔥 Medium · **Read time:** ~20 min

## The idea

Think about how a well-run household stores things. What you use daily sits on the **kitchen counter** — instant to grab, but counter space is expensive. Last season's things go to the **basement**: still in the house, a short walk away. Tax records from six years ago go to a **storage unit across town**: dirt cheap per box, but getting a box back takes a trip and a day's notice. Some documents must go through the **shredder** on a schedule — and some must be shredded *on request*, completely, including every photocopy. And the irreplaceable things live in a **fireproof safe in another city**, so one house fire can't wipe them out.

Data works exactly the same way. **Hot** data (counter) sits on fast, pricey storage; **warm** data (basement) on cheaper tiers still queryable in seconds; **cold** data (storage unit) in archives measured in hours. **Retention and expiration** (scheduled shredding) keep you cheap and compliant; **erasure on request** (GDPR/CCPA "delete my data") means finding *every copy*, including old versions and backups; and **resiliency** (the safe in another city) is replication, backups, and multi-AZ/multi-Region designs sized to a recovery point objective (**RPO** — how much data you can lose) and recovery time objective (**RTO** — how long you can be down).

This guide keeps S3 lifecycle mechanics brief (they live in [Guide 05 — S3 Data Lake Storage](05-S3-Data-Lake-Storage.md)) and focuses on *strategy across services*: which tier for which age, how to delete for real, and which resiliency feature meets which RPO/RTO. It lets you crack "most cost-effective retention", "delete a customer's data completely", and "survive a Region outage" questions.

## Hot, warm, cold — across the services

| Service | Hot (counter) | Warm (basement) | Cold (storage unit) |
|---|---|---|---|
| **S3** | Standard (or Express One Zone for ultra-low latency) | Standard-IA / Intelligent-Tiering | Glacier Instant Retrieval → Flexible Retrieval → Deep Archive |
| **Redshift** | Tables in Redshift Managed Storage | Query S3 data in place with **Spectrum** | **UNLOAD** old partitions to S3 as Parquet, drop them from local tables, keep them queryable via Spectrum |
| **OpenSearch Service** | Hot data nodes (read/write) | **UltraWarm** (read-only, S3-backed) | **Cold storage** (detached indices on S3, attach to query); automate with **Index State Management (ISM)** |
| **DynamoDB** | **Standard** table class | **Standard-IA** table class (much cheaper storage, pricier requests) | **TTL** + Streams → Lambda/Firehose → S3; or **export to S3** |
| **Kinesis Data Streams** | Default **24 h** retention | Extended up to **365 days** (extra cost) | Archive raw records to S3 via Firehose |
| **Amazon MSK** | Broker EBS storage | **Tiered storage** — older segments moved to a low-cost managed tier, read transparently | S3 via MSK Connect/Firehose |
| **CloudWatch Logs** | Log group, default retention **never expire** | Set retention **1 day to 10 years** | Export to S3 (`CreateExportTask`, batch) or subscription filter → Firehose → S3 (continuous) |
| **RDS / Aurora** | Live database | Read replicas for reporting | Automated backups (up to **35 days**), manual snapshots, **snapshot export to S3 as Parquet** |

Redshift load/unload (skill 2.3.1) in one breath: **COPY** loads from S3 in parallel (split files, columnar formats); **UNLOAD** writes query results to S3 in parallel, with `FORMAT PARQUET` and `PARTITION BY` to build a data lake archive. Depth in [Guide 24](24-Redshift-Loading-Integration-Sharing.md). Two details that bite: CloudWatch Logs deletion happens **up to 72 hours** after events pass retention, and DynamoDB TTL deletes expired items **typically within a few days** — neither is instant.

## S3 lifecycle strategy (skills 2.3.2, 2.3.3)

Design rules **per zone**, because each zone has a different access curve:

| Zone | Typical rule |
|---|---|
| **raw/landing** | Standard → Standard-IA at 30 d → Glacier Instant/Flexible at 90 d → Deep Archive at 1 y → expire at the retention limit (reprocessing source of truth) |
| **curated/analytics** | Stay in Standard (hot, queried daily) or Intelligent-Tiering if access is unpredictable |
| **tmp/staging, Athena query results** | **Expire after days** — never transition |
| **logs/audit** | Glacier classes + long expiration, maybe Object Lock |

Constraints the exam likes:
- **Minimum storage duration charges:** Standard-IA / One Zone-IA **30 days**, Glacier Instant and Flexible **90 days**, Deep Archive **180 days** — transition or delete earlier and you pay the rest. A single rule can't chain transitions faster than these minimums allow.
- Objects must sit **30 days** in Standard before a lifecycle transition to Standard-IA/One Zone-IA.
- **Objects under 128 KB don't transition by default** (since Sept 2024); override with an `ObjectSizeGreaterThan`/`ObjectSizeLessThan` filter. Tiny objects cost more in transition requests (and Glacier's **40 KB** per-object metadata overhead) than they save — aggregate them first.
- **Filters:** prefix, object tags, object size, or combinations (`And`).
- **Lifecycle actions are asynchronous.** Expiration and transitions are queued, so objects may linger past the date — but you **aren't billed for storage after the expiration date**. Never use lifecycle for "delete within the hour" requirements.
- **Versioned buckets:** `Expiration` on a current version only adds a **delete marker**. Clean up with **`NoncurrentVersionExpiration`** (`NoncurrentDays`, optionally `NewerNoncurrentVersions` to keep the last N), expired-delete-marker cleanup, and **`AbortIncompleteMultipartUpload`**.
- **Lifecycle vs Intelligent-Tiering:** *known* access curve → lifecycle (deterministic, no monitoring fee); *unknown/changing* → Intelligent-Tiering (no retrieval fees; objects under 128 KB not monitored).

A correct configuration (for `aws s3api put-bucket-lifecycle-configuration`):

```json
{
  "Rules": [
    {
      "ID": "raw-zone-tiering",
      "Filter": { "Prefix": "raw/" },
      "Status": "Enabled",
      "Transitions": [
        { "Days": 30,  "StorageClass": "STANDARD_IA" },
        { "Days": 90,  "StorageClass": "GLACIER_IR" },
        { "Days": 365, "StorageClass": "DEEP_ARCHIVE" }
      ],
      "Expiration": { "Days": 2555 },
      "NoncurrentVersionExpiration": { "NoncurrentDays": 30, "NewerNoncurrentVersions": 3 },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    },
    {
      "ID": "tmp-short-retention",
      "Filter": { "And": { "Prefix": "tmp/", "Tags": [ { "Key": "retention", "Value": "short" } ] } },
      "Status": "Enabled",
      "Expiration": { "Days": 7 }
    }
  ]
}
```

(Each hop respects the previous class's minimum: IA held 60 days, Glacier IR 275 days; ~7 years total retention.)

**THE trap:** transitioning a **tmp/** prefix whose files live 5 days into Standard-IA "to save money" — the 30-day minimum makes it *more* expensive. Short-lived data stays in Standard and expires.

## Versioning and DynamoDB TTL (skill 2.3.4)

**S3 Versioning** has three states: unversioned (default), **enabled**, **suspended** (never back to unversioned). Overwrites create new versions; a simple DELETE creates a **delete marker** on top and the old versions remain (and remain billable). Versioning is required for replication and Object Lock; **MFA Delete** protects against permanent version deletion. Pair versioning with noncurrent-version lifecycle rules or the bill grows forever.

**DynamoDB TTL** — pick a Number attribute holding an **epoch-seconds** timestamp; DynamoDB deletes expired items **in the background, without consuming write capacity**, typically **within a few days**. Consequences:
- Expired-but-not-yet-deleted items still come back in reads → add a **filter expression** (`expires_at > :now`) if exactness matters.
- TTL deletes appear in **DynamoDB Streams** as **service** deletions (not user deletes) → a Lambda on the stream can **archive expired items to S3** (or Firehose) before they're gone — the classic "expire from DynamoDB but keep history cheaply" pattern.
- With global tables, TTL deletes replicate to every replica (replicated writes are billed in the other Regions).

## Deleting data for business and legal requirements (skill 2.3.5)

Regulations such as the GDPR right to erasure (Article 17) and CCPA/CPRA deletion rights require removing a person's data on request (verify exact obligations with your legal team against the official texts). The engineering problem: **find every copy, delete it for real, and account for backups.**

1. **Find it.** Amazon Macie discovers PII in S3; Glue sensitive-data detection and catalog tags (LF-Tags such as `classification=pii`) map which tables/columns hold it; keep a subject-to-location index. ([Guide 42 — Privacy, PII & Sovereignty](42-Privacy-PII-Masking-Sovereignty.md))
2. **Delete it in each store:**

| Store | Real deletion |
|---|---|
| **S3, versioned** | Delete **every version ID** (`DeleteObject` with `versionId`), not just the current key; at scale, drive it from S3 Inventory with **S3 Batch Operations** (e.g., invoking a Lambda function) or noncurrent-version lifecycle rules |
| **Iceberg tables** | `DELETE FROM ... WHERE customer_id = ...` (Athena or Spark) → compact/rewrite (`OPTIMIZE`) so data files no longer hold the rows → **expire snapshots and remove orphan files** (Athena `VACUUM`; default keeps **5 days** of snapshots). Until snapshots expire, **time travel can still read the deleted rows** |
| **Plain Parquet/CSV lake** | Rewrite the affected files/partitions without the rows (Glue/EMR job) — no row-level delete exists |
| **Redshift** | `DELETE` marks rows; `VACUUM DELETE ONLY` reclaims them; old **snapshots** still contain them until they expire (manual snapshots default to **indefinite** retention) |
| **DynamoDB** | `DeleteItem` (or TTL); PITR (up to **35 days**) and on-demand backups still hold the item until they age out |
| **Backups / snapshots** | Document retention windows so erasure completes when backups expire; restoring an old backup must re-apply pending deletions |

3. **Crypto-shredding** — encrypt each subject's (or tenant's/dataset's) data with its own AWS KMS key or data key; to erase, **schedule the key for deletion** (waiting period **7–30 days, default 30**, key state *Pending deletion*, cancellable until then). After deletion, ciphertext everywhere — including backups — is unrecoverable. Great for data scattered across many copies.

**THE trap:** *"We deleted the customer's objects from the versioned bucket"* with a plain DELETE → only **delete markers** were added; every prior version still exists. Erasure means **deleting the versions**.

**THE trap:** Object Lock **compliance mode** blocks deletion by everyone, including root, until retention expires — you can't honor an erasure request against a compliance-locked object. Resolve the conflict in the design: keep PII out of WORM buckets (tokenize/pseudonymize first), or recognize that a legal retention obligation takes precedence for its duration. Note that Object Lock does **not** protect against deletion of the KMS key encrypting the objects.

**Legal holds** do the opposite of erasure: S3 Object Lock legal holds (no expiry, removed explicitly by anyone with `s3:PutObjectLegalHold`) and AWS Backup legal holds freeze data during litigation, overriding retention-based deletion.

## Retention and archiving

- **S3 Glacier Deep Archive** — cheapest for 7–10+ year retention; retrieval in hours (Standard ~12 h, Bulk ~48 h).
- **S3 Object Lock** (versioning required; can be enabled on new **or existing** buckets, never disabled): **compliance mode** = no one can delete or shorten retention (the SEC 17a-4-style WORM answer); **governance mode** = users with `s3:BypassGovernanceRetention` can override. Lifecycle rules still run on locked objects, but **cannot expire a locked version**.
- **CloudTrail**: Event history covers **90 days** of management events; for longer retention create a **trail to S3** (then lifecycle to Glacier). **Redshift**: automated snapshots default **1 day**, configurable **1–35 days** on RA3; manual snapshots kept until deleted; Serverless recovery points every **30 minutes**, kept **24 hours** (convert to a snapshot to keep longer).

> ⚠️ **2026 status:** the original vault-based **Amazon Glacier** service (vaults, archives, Vault Lock policies via the Glacier API) has been in **maintenance since Nov 7, 2025**. The **S3 Glacier storage classes** are unaffected and are the answer for new archives; treat "create a Glacier vault" options as legacy/distractor, and use S3 Object Lock or AWS Backup Vault Lock for WORM.

## Resiliency and availability (skill 2.3.6)

**S3 by class** — all classes are designed for **11 nines durability**; availability differs:

| Class | Designed availability | AZs |
|---|---|---|
| Standard | **99.99%** | ≥ 3 |
| Standard-IA, Intelligent-Tiering, Glacier Instant Retrieval | **99.9%** | ≥ 3 |
| One Zone-IA | **99.5%** | **1** — lost if the AZ is destroyed |
| Express One Zone | **99.95%** | **1** |
| Glacier Flexible / Deep Archive | 99.99% (after restore) | ≥ 3 |

One Zone classes are only for **re-creatable** data. For Region-level protection use **Cross-Region Replication (CRR)** — versioning on both buckets, replicates **new** objects only (existing ones need **S3 Batch Replication**), optional **Replication Time Control** for a 15-minute SLA. Versioning protects against accidental overwrite/delete.

**AWS Backup** — one place to define **backup plans** (schedule, retention, transition to cold storage), store recovery points in **backup vaults**, and make **cross-Region and cross-account copies** (often to a central backup account in AWS Organizations). **Backup Vault Lock** adds WORM (governance, or compliance mode that becomes immutable after its cooling-off period); **legal holds** freeze recovery points; **logically air-gapped vaults** add isolation against account compromise. Supported data services include **S3** (continuous backup/PITR), **DynamoDB**, **RDS, Aurora, DocumentDB, Neptune**, **Redshift (provisioned and Serverless)**, EBS/EC2, EFS, FSx, and more. Nuance: **DynamoDB cross-Region/cross-account copy requires AWS Backup "advanced features" for DynamoDB**; for Redshift cross-Region DR the classic answer is Redshift's **native cross-Region snapshot copy**.

**Per-service resiliency features:**
- **DynamoDB** — data replicated across 3 AZs automatically; **PITR** restores to any second in the last **35 days** (recovery period configurable **1–35 days**) into a **new table**, even in another Region; **global tables** = multi-active, multi-Region replication (asynchronous by default; **multi-Region strong consistency** GA June 2025 gives **RPO zero** across exactly three Regions).
- **RDS** — **Multi-AZ** = synchronous standby with automatic failover (HA, not read scaling); **read replicas** = asynchronous, can be cross-Region and promoted (read scaling + DR); automated backups with PITR, retention up to **35 days**.
- **Aurora** — six copies of data across **3 AZs**, up to **15** replicas; **Aurora Global Database** replicates to secondary Regions with typical **RPO ~1 second** and **RTO under 1 minute** for unplanned failover (switchover for planned moves: RPO 0).
- **Redshift** — RA3 **Multi-AZ** deployments run compute in **two AZs** behind one endpoint (RPO zero because data lives in RMS on S3; failover typically under a minute; 99.99% SLA); **cross-Region snapshot copy** for Regional DR (KMS-encrypted clusters need a snapshot copy grant in the destination Region); **cluster relocation** moves an RA3 cluster to another AZ.
- **Kinesis Data Streams** synchronously stores data across **3 AZs**; **MSK** — spread brokers over **3 AZs** with replication factor 3 and `min.insync.replicas=2`.
- **Pipelines** — retries with exponential backoff, idempotent writes, Step Functions `Retry`/`Catch`, DLQs, and replayable sources (Kinesis retention, raw S3 zone) so a failed stage can be rerun ([Guide 20](20-Step-Functions.md)).

| Need | Choice | RPO / RTO (typical) |
|---|---|---|
| Survive an AZ failure, relational | RDS Multi-AZ / Aurora | ~0 / 1–2 min |
| Survive a Region failure, relational, fastest | **Aurora Global Database** | ~1 s / < 1 min |
| Region DR, relational, cheaper | Cross-Region read replica or snapshot copy | minutes / tens of minutes+ |
| DynamoDB, multi-Region active-active | **Global tables** (MRSC for RPO 0) | ~seconds or 0 / near 0 |
| DynamoDB accidental delete/corruption | **PITR** (new table) | seconds / restore time |
| Redshift AZ failure | **Multi-AZ (RA3)** | 0 / ~1 min |
| Redshift Region failure | Cross-Region snapshot copy → restore | since last snapshot / restore time |
| S3 Region failure | **CRR** (+ RTC) | ~15 min with RTC / redirect clients |
| Centralized, compliant, immutable backups | **AWS Backup** + Vault Lock + cross-account copy | backup frequency / restore time |

**THE trap:** treating replication as backup. CRR and global tables faithfully replicate **deletes and corruption** too; point-in-time recovery (PITR, versioning, AWS Backup) is what gets you back to *before* the mistake.

## Question patterns

> *"Raw clickstream files in S3 are queried daily for 30 days, occasionally for 90 days, must be retained 7 years for audit, and retrieval for audit can take 48 hours. MOST cost-effective?"* → **Lifecycle: Standard → Standard-IA at 30 d → Glacier (Instant or Flexible) at 90 d → Deep Archive at 1 y → expire at ~7 y** (known curve → lifecycle; 48 h tolerance allows Deep Archive).

> *"Staging files in s3://…/tmp/ are needed for 3 days. Reduce cost."* → **Lifecycle expiration after 3 days, no transitions** (IA's 30-day minimum makes a transition cost more).

> *"A versioned bucket's storage keeps growing although a lifecycle rule 'expires' objects after 30 days."* → **Add NoncurrentVersionExpiration (and expired delete-marker cleanup)** — current-version expiration only adds delete markers.

> *"A data warehouse keeps 5 years of sales; queries touch the latest 12 months, older data is queried a few times a year. Reduce Redshift storage cost with the least effort."* → **UNLOAD older data to S3 as partitioned Parquet, drop it locally, query via Redshift Spectrum** (warm/cold in S3, hot in Redshift).

> *"Session records in DynamoDB must disappear 24 hours after creation at no extra write cost, and a copy must be kept in S3 for analytics."* → **TTL on an epoch-seconds attribute + DynamoDB Streams → Lambda (or Firehose) → S3** (TTL deletes are free, appear in Streams as service deletions; allow for deletion lag of up to a few days — filter expired items in reads).

> *"A customer invokes their right to erasure. Their records live in a versioned S3 bucket, an Iceberg table queried by Athena, and Redshift."* → **Delete all S3 object versions; Iceberg DELETE then compaction and VACUUM to expire snapshots; Redshift DELETE + VACUUM, and let snapshots age out** (delete markers and old snapshots still hold the data).

> *"PII is scattered across many S3 prefixes, backups and replicas; the company needs a way to make one tenant's data unrecoverable everywhere."* → **Crypto-shredding: per-tenant KMS key, schedule key deletion (7–30 days)** (deleting every copy individually is impractical).

> *"Trade records must be kept 7 years and nobody, including administrators or the root user, may delete them."* → **S3 Object Lock in compliance mode** (governance can be bypassed; the legacy Glacier vault is in maintenance).

> *"Records involved in a lawsuit must be preserved until the case ends; the end date is unknown."* → **Object Lock legal hold** (or AWS Backup legal hold for recovery points) — no expiry until removed.

> *"Centralize backups of RDS, DynamoDB and EFS across 40 accounts, copy them to another Region, and make them immutable against a compromised admin."* → **AWS Backup with organization backup policies, cross-Region/cross-account copy, Backup Vault Lock (compliance mode)**.

> *"A global application needs the relational database to survive a Regional outage with about one second of data loss and recovery in under a minute."* → **Aurora Global Database** (cross-Region read replicas lag more and need manual promotion).

> *"A DynamoDB table was corrupted by a bad batch job 3 hours ago."* → **PITR restore to a new table at a time before the job** (global tables would have replicated the corruption).

> *"Mission-critical Redshift RA3 warehouse must keep running if an Availability Zone fails, with no data loss."* → **Redshift Multi-AZ deployment** (cross-Region snapshot copy is for Regional DR, and restores take longer).

> *"Keep one year of logs searchable in OpenSearch at the lowest cost; almost all queries hit the last 7 days."* → **ISM policy hot → UltraWarm → cold → delete** ([Guide 29](29-OpenSearch-Service.md)).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Known access curve over time | S3 Lifecycle transitions |
| Unknown / changing access | Intelligent-Tiering |
| Data lives < 30 days | Stay in Standard, expire |
| Min durations | IA 30 d · Glacier IR/Flexible 90 d · Deep Archive 180 d |
| Tiny objects (< 128 KB) | Not transitioned by default; aggregate them |
| Lifecycle timing | Asynchronous; no storage billing after expiration date |
| Versioned bucket still growing | NoncurrentVersionExpiration + delete-marker cleanup |
| Abandoned multipart uploads | AbortIncompleteMultipartUpload |
| Old Redshift data, cheap but queryable | UNLOAD to Parquet + Spectrum |
| Older OpenSearch indices | UltraWarm → cold via ISM |
| Storage-heavy, rarely read DynamoDB table | Standard-IA table class |
| Auto-expire DynamoDB items, free | TTL (epoch seconds; deletes within a few days) |
| Archive expired DynamoDB items | Streams (service deletes) → Lambda/Firehose → S3 |
| Kafka data kept months cheaply | MSK tiered storage |
| CloudWatch Logs default retention | Never expire (set 1 day–10 years) |
| "Delete" in a versioned bucket | Delete marker only — delete every version ID |
| Erasure on Iceberg | DELETE → compaction → VACUUM (snapshot expiry) |
| Erasure in Redshift | DELETE + VACUUM; snapshots age out |
| Unrecoverable everywhere at once | Crypto-shredding (KMS key deletion, 7–30 d wait) |
| WORM, not even root | Object Lock compliance mode |
| WORM, privileged override | Object Lock governance mode |
| Litigation, unknown end | Legal hold |
| Immutable backups | AWS Backup Vault Lock |
| Central, cross-account/Region backups | AWS Backup (DynamoDB copy needs advanced features) |
| Legacy "Glacier vault" | Maintenance since Nov 2025 → S3 Glacier classes |
| One Zone-IA / Express One Zone | Re-creatable data only (single AZ) |
| S3 Region DR | CRR (versioning both sides; Batch Replication for existing) |
| DynamoDB oops restore | PITR up to 35 days → new table |
| DynamoDB multi-Region active-active | Global tables (MRSC = RPO 0) |
| Relational cross-Region, ~1 s RPO | Aurora Global Database |
| RDS HA vs read scaling | Multi-AZ vs read replicas |
| Redshift AZ failure | Multi-AZ (RA3) |
| Redshift Region DR | Cross-Region snapshot copy |
| Replication ≠ backup | Replicas copy deletes/corruption; use PITR/backups |

Lifecycle and resiliency protect data you already trust; the next step is making sure the data deserves that trust — continue with [Guide 32 — Monitoring, Logging & Troubleshooting](32-Monitoring-Logging-Troubleshooting.md) and [Guide 33 — Data Quality](33-Data-Quality.md).
