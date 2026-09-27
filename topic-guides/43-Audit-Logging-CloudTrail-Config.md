# 43 · Audit Logging, CloudTrail & Config — the flight recorder and the building inspector

> **Exam map:** D3 · Task 3.3 — D4 · Task 4.4, 4.5 · **Skills:** 3.3.1, 3.3.2, 3.3.5, 3.3.7, 3.3.8, 4.4.1, 4.4.2, 4.4.3, 4.4.4, 4.4.5, 4.5.4 · **Weight:** 🔥🔥🔥 High · **Read time:** ~22 min

## The idea

Auditing an AWS data platform takes two very different instruments, and the exam loves making you choose between them.

**AWS CloudTrail is the flight recorder.** Every API call ("who, did what, to which resource, from where, when, and did it succeed?") gets written to a tamper-evident log. `CreateTable`, `DeleteBucket`, `GetObject`, `StartJobRun`, `AssumeRole`. CloudTrail doesn't care what the plane looks like. It records every lever pulled.

**AWS Config is the building inspector.** It periodically photographs every resource's **configuration** (bucket encryption on? PITR enabled? Redshift publicly accessible?). It keeps a **timeline** of how each resource changed and grades it against **rules** ("all buckets must be encrypted"). Config tells you **what changed and whether it's compliant**. CloudTrail tells you **who called which API**. Each Config change record links to the CloudTrail event that caused it.

Around those two sit the **application's own logs**: Glue driver output, Lambda prints, Redshift user activity, database audit logs, VPC Flow Logs. These live in **CloudWatch Logs** or S3. Then comes the analyst's toolbox: Athena, CloudWatch Logs Insights, OpenSearch, EMR, CloudTrail Lake. After this guide you'll know which events CloudTrail records by default (and which cost extra), how to make logs tamper-evident and centralized, how to extract logs for an auditor, and which tool to query them with at each volume and latency.

## CloudTrail — what it records

| Event type | What | On by default? | Examples for data engineers |
|---|---|---|---|
| **Management events** (control plane) | Creating, configuring or deleting resources | **Yes**, shown in Event history. The first copy of management events in a trail is free | `CreateBucket`, `PutBucketPolicy`, `CreateCrawler`, `StartJobRun`, `CreateCluster`, `GrantPermissions` (Lake Formation), `CreateStream` |
| **Data events** (data plane) | Operations **on or inside** a resource. High volume | **No.** Enable per trail or event data store. **Charged extra** | S3 **object** `GetObject`/`PutObject`/`DeleteObject`, Lambda `Invoke`, DynamoDB **item** calls and Streams, Kinesis Data Streams (`PutRecord`, `GetRecords`), Firehose delivery streams, **Glue tables created by Lake Formation**, SNS `Publish`, SQS messages, Step Functions, Bedrock, **S3 Tables** and **S3 Vectors**, Redshift Data API, RDS Data API |
| **Insights events** | **Unusual API call rate or error rate** compared with the account's baseline | No (paid) | A script suddenly calls `DeleteObject` 10,000×, or a spike of `AccessDenied` |
| **Network activity events** | API calls made **through VPC endpoints**, including calls **denied by the endpoint policy** | No (paid) | Detect data-exfiltration attempts through an interface endpoint |

- **Event history** is always on, with no setup. It covers the **last 90 days** of **management events**, for **one account and one Region**, with simple attribute lookups in the console or the **`LookupEvents`** API (rate-limited, paginated). **Data events never appear in Event history.**
- **Trails** give you durable, long-term delivery.
  - A trail delivers log files (gzipped JSON) to **S3**, on average within about **5 minutes** of the call (not guaranteed).
  - Optionally it also streams to **CloudWatch Logs** (for metric filters and alarms) and sends **SNS** notifications on each log-file delivery.
  - A trail can be **single-Region** or **multi-Region** (recommended, and the console default).
  - An **organization trail**, created in the management account or a delegated administrator account, logs **every member account**. Members can't change or delete it.
- **Selectors.** **Advanced event selectors** filter on `eventCategory`, `resources.type`, `resources.ARN` (StartsWith/EndsWith and similar, no wildcards), `eventName`, `readOnly`, `userIdentity.arn` and more. Use them to log only write events on `arn:aws:s3:::amzn-s3-demo-bucket/finance/` and not every read of every object in the account. **Basic** selectors only cover S3 objects, Lambda and DynamoDB.
- **EventBridge** receives CloudTrail management events, so you can react **in near real time**. Example: a rule on `PutBucketPolicy` or `DeleteTrail` triggers Lambda or SNS.

**THE trap:** *"Find out who deleted objects from the finance bucket last week"* when only default settings exist. **Object deletes are data events, which aren't logged by default**, and Event history shows management events only. You needed a trail with **S3 data events** (or S3 server access logs) enabled **before** the incident. Nothing can recover the record afterwards.

## Making the record trustworthy (3.3.2, 4.4.1)

The **audit-grade trail recipe:**

1. **Organization, multi-Region trail** covering management events plus the data events you need. It also catches activity in Regions nobody meant to use.
2. **Deliver to a central log-archive account's S3 bucket.** Control Tower creates a dedicated **Log Archive** account. The bucket policy lets CloudTrail write, and workload-account admins can't read or delete.
3. **Encrypt with SSE-KMS** using a customer managed key. The default is SSE-S3, and SSE-KMS adds a second permission gate plus key-use auditing ([Guide 39](39-Encryption-Key-Management.md)).
4. **Log file integrity validation.** CloudTrail writes an hourly **digest file** of SHA-256 hashes, signed with RSA. `aws cloudtrail validate-logs` then proves whether any log file was **modified or deleted** after delivery.
5. **S3 Object Lock** (compliance mode) and/or **versioning + MFA Delete** on the log bucket for **WORM** retention, plus lifecycle rules to Glacier storage classes for multi-year retention cheaply ([Guide 05](05-S3-Data-Lake-Storage.md)).
6. **CloudWatch Logs integration + metric filters + alarms** for the famous CIS alarms: root login, IAM policy changes, `StopLogging`/`DeleteTrail`, console sign-in failures, KMS key disabling.
7. **AWS Config rule `cloud-trail-log-file-validation-enabled`** and **`multi-region-cloudtrail-enabled`** to prove the setup stays in place.

**THE trap:** "detect whether CloudTrail logs were tampered with" → **log file integrity validation (digest files)**, not "enable versioning" and not "encrypt with KMS." Versioning and Object Lock *prevent* tampering. Validation *proves* whether it happened. Encryption controls who can read.

## CloudTrail Lake (4.4.3)

**CloudTrail Lake** is a managed audit data lake inside CloudTrail. Events are converted to columnar **ORC** and stored in **event data stores**, which are **immutable** collections defined by advanced event selectors. You query them with **SQL** (Trino-compatible `SELECT`, including **JOINs across event data stores**), or write the query from an **English prompt** with the query generator.

| Feature | Detail |
|---|---|
| Sources | CloudTrail management, data and network activity events; **Insights** events; **AWS Config configuration items**; **Audit Manager evidence**; **non-AWS events** via integrations (partners or your own apps calling `PutAuditEvents`). Each event data store holds **one category** |
| Scope | **Organization event data stores** span all accounts and Regions |
| Retention | **One-year extendable retention** pricing: up to **3,653 days (~10 years)**. **Seven-year retention** pricing: up to **2,557 days (~7 years)** |
| Backfill | **Copy trail events** from S3 into an event data store for history |
| Dashboards | Managed dashboards, custom dashboards (SQL widgets), and a Highlights dashboard |
| Open access | **Federate** an event data store into the **Glue Data Catalog** (governed by Lake Formation) and query it with **Athena** |
| Cost | Pay for ingestion + storage (by pricing option) + **data scanned per query** |

> ⚠️ **2026 status:** CloudTrail Lake is **closed to new customers from May 31, 2026**. Existing customers continue as normal, but only get critical bug fixes and security updates. **Organization** event data stores keep covering new member accounts, and account-level ones don't. AWS recommends **Amazon CloudWatch** (its unified data store for security, operations and compliance logs, with OpenSearch-powered analytics, OCSF/OTel normalization and Apache Iceberg access) and offers **Export to CloudWatch**. CloudTrail itself (trails, Insights) is unaffected. Because skill **4.4.3 still names CloudTrail Lake**, expect it as a correct answer to *"centralized SQL queries over CloudTrail events across the organization, without building a pipeline."*

**CloudTrail Lake vs Athena on trail logs:** both use SQL. **Athena over the S3 trail bucket** is cheaper for occasional queries and gives you full control, but *you* create the table. Use **partition projection** (Region/date) so new days need no crawler or `MSCK REPAIR`. **CloudTrail Lake** needs no table management, is immutable, and holds Config items and external events too, but it costs more per GB ingested.

## AWS Config (4.5.4)

| Concept | Meaning |
|---|---|
| **Configuration recorder** | Records supported resource types, **continuously** or **daily** (periodic recording, cheaper) |
| **Configuration item (CI)** | Point-in-time snapshot of a resource's attributes, **relationships**, and the linked CloudTrail event ID |
| **Configuration history / timeline** | Every CI for a resource over time. The console timeline shows changes side by side, and **"who made it"** comes from CloudTrail |
| **Delivery channel** | **Configuration history files to S3** (every 6 hours) + **configuration snapshots** (on demand or scheduled) + SNS change notifications |
| **Relationships** | "This EBS volume is attached to this instance, encrypted with this KMS key." Needed for impact analysis |
| **Managed rules** | AWS-written checks, for example `s3-bucket-server-side-encryption-enabled`, `s3-bucket-level-public-access-prohibited`, `s3-bucket-ssl-requests-only`, `s3-bucket-versioning-enabled`, `cloudtrail-enabled`, `multi-region-cloudtrail-enabled`, `cloud-trail-log-file-validation-enabled`, `dynamodb-pitr-enabled`, `redshift-cluster-public-access-check`, `redshift-cluster-configuration-check`, `rds-storage-encrypted`, `rds-snapshots-public-prohibited`, `encrypted-volumes` |
| **Custom rules** | **Lambda**-backed (any logic) or **Custom Policy rules written in CloudFormation Guard** (declarative, no Lambda) |
| **Triggers** | **Configuration change** (evaluate when a matching resource changes) and/or **periodic** (every 1–24 h). **Detective** (evaluate deployed resources) vs **proactive** (evaluate before deployment via CloudFormation hooks/API) |
| **Remediation** | Attach **Systems Manager Automation** runbooks, manual or **automatic** with retries. Example: re-enable default encryption or block public access |
| **Conformance packs** | A YAML bundle of rules + remediations deployed as one unit, and **org-wide** (e.g., operational best practices for HIPAA, PCI DSS) |
| **Aggregators** | **Multi-account, multi-Region** view of configuration and compliance in one account (organization-wide) |
| **Advanced queries** | SQL-like `SELECT` over **current** configuration state, for one account or across an aggregator ("all Redshift clusters not encrypted") |

**Config vs CloudTrail — the one-line test:**

| Question asks… | Answer |
|---|---|
| *"Who deleted / modified / called…"*, *"which IAM principal…"*, *"from which IP…"* | **CloudTrail** |
| *"What did this resource's configuration look like last Tuesday?"*, *"when did encryption get turned off?"* | **Config** timeline |
| *"Continuously check that all buckets stay encrypted and auto-fix violations"* | **Config rule + SSM Automation remediation** |
| *"Show compliance across 60 accounts in one place"* | **Config aggregator** (or Security Hub) |
| *"Alert within seconds when someone changes a bucket policy"* | **EventBridge rule on the CloudTrail management event** (or a Config change-triggered rule) |

**THE trap:** using CloudTrail to answer *"is it compliant now?"* CloudTrail records calls. It doesn't evaluate state. And using Config to name *who* did it: Config links to CloudTrail for that.

## Application logs in CloudWatch Logs (3.3.7, 4.4.2)

CloudWatch Logs basics (depth in [Guide 32](32-Monitoring-Logging-Troubleshooting.md)):

- **Log groups and streams** hold events, with **retention** you set per group (1 day to 10 years, or never expire).
- Many data services write here: Glue (continuous logging), Lambda, Step Functions, MWAA task logs, EMR Serverless, DMS, Redshift and RDS logs if exported.
- Automation via IaC means creating log groups with retention and KMS keys in CloudFormation/CDK rather than letting services auto-create never-expiring groups.
- **Metric filters** turn log patterns into metrics for alarms. **Data protection policies** mask PII ([Guide 42](42-Privacy-PII-Masking-Sovereignty.md)).

**Centralized logging across accounts (4.4.5):**

```
Workload accounts: log groups ── subscription filter ──►  central account
                                                          Kinesis Data Streams / Firehose  ──► S3 (Parquet, partitioned)
                                                          (via a CloudWatch Logs destination)       │
                                                                                                    ├─► Athena / Glue catalog
                                                                                                    └─► OpenSearch (search, dashboards)
```

- **Subscription filters** stream matching events in near real time to **Kinesis Data Streams, Data Firehose, Lambda or OpenSearch** (via Lambda). Cross-account delivery uses a **CloudWatch Logs destination** in the receiving account.
- **CloudWatch cross-account observability** lets a monitoring account *view and query* logs, metrics and traces from source accounts without copying them. **Log centralization rules** can copy log data across accounts and Regions into one account.
- **VPC Flow Logs, MSK broker logs and API Gateway access logs** can go to CloudWatch Logs, S3 or Firehose, so choose S3/Firehose for cheap bulk retention.

## Other audit sources data engineers must know

| Source | What it captures | Where it goes / notes |
|---|---|---|
| **S3 server access logs** | Every request to a bucket (requester, operation, key, status, bytes) | Log files delivered, best effort and **delayed**, to a **different** bucket. No extra charge beyond storage. Query with Athena |
| **CloudTrail S3 data events** | Object-level API calls with **full IAM identity** | Near real time, **paid per event**, EventBridge-integrated, integrity validation |
| **Redshift audit logging** | **Connection log** (logins/disconnects), **user log** (user/permission changes), **user activity log** (every query, requires the `enable_user_activity_logging` parameter) | To **S3** or **CloudWatch Logs**. System tables (SYS/STL views) hold short-term history ([Guide 25](25-Redshift-Performance-Operations-Security.md)) |
| **RDS/Aurora database logs** | Engine logs + audit plugins (pgAudit for PostgreSQL, the audit plugin / advanced auditing for MySQL/Aurora MySQL) | Publish to CloudWatch Logs |
| **Database Activity Streams** (Aurora, some RDS engines) | Near-real-time stream of database activity, **encrypted with KMS**, separated from DBA control | To a **Kinesis data stream** → Firehose/S3 or a security tool. Signal: *"DBAs must not be able to tamper with the audit trail"* |
| **VPC Flow Logs** | IP traffic metadata (accepted/rejected) for VPC, subnet or ENI | CloudWatch Logs, S3, Firehose |
| **Amazon MSK broker logs** | Kafka broker logs | CloudWatch Logs, S3, Firehose |
| **OpenSearch audit logs** | Authentication, index and document access | CloudWatch Logs. **Requires fine-grained access control** |
| **API Gateway access/execution logs** | Who called your data API, latency, status | CloudWatch Logs or Firehose |
| **Lake Formation** | Grants/revokes and **`GetDataAccess`** credential vending | CloudTrail (management events) ([Guide 40](40-Lake-Formation.md)) |
| **Amazon EMR** | Step, application (Spark/YARN) and node logs | Archived to an **S3 log URI** ([Guide 15](15-Amazon-EMR.md)) |
| **Macie findings** | Sensitive data and bucket-posture findings | EventBridge, Security Hub |

**S3 server access logs vs CloudTrail data events:** use **access logs** for *cheap, complete, request-level history, fine to arrive late*. Use **CloudTrail data events** for *"which IAM principal," near real time, alerting via EventBridge, or tamper-evident* records. Many regulated shops enable both.

## Analyzing logs — pick the tool (3.3.8, 4.4.4, 4.4.5)

| Tool | Best for | Signal words |
|---|---|---|
| **Athena** | Ad hoc SQL over logs **in S3** (CloudTrail, S3 access logs, ALB, VPC Flow Logs), pay per TB scanned. Partition projection + Parquet keep it cheap ([Guide 26](26-Amazon-Athena.md)) | *"serverless," "SQL," "logs already in S3," "occasional/ad hoc," "cost-effective"* |
| **CloudWatch Logs Insights** | Interactive queries over **log groups** (filter, stats, parse), with no data movement | *"application logs in CloudWatch," "quickly troubleshoot," "least effort"* |
| **Amazon OpenSearch Service** | Full-text search, near-real-time log analytics, **dashboards**, anomaly detection over streaming logs ([Guide 29](29-OpenSearch-Service.md)) | *"search," "near real-time dashboards," "operational analytics," "Kibana-style"* |
| **Amazon EMR** (Spark, Hive, Presto/Trino) | **Very large** log volumes (many TB to PB/day), complex parsing, sessionization, ML features | *"petabytes of logs," "custom processing," "big data application logs"* |
| **CloudTrail Lake** | SQL over CloudTrail/Config/external audit events across the org, immutable, no pipeline | *"centralized audit queries," "organization-wide CloudTrail SQL"* |
| **Quick Sight dashboards** (on Athena) | Visual audit reporting for non-engineers ([Guide 35](35-Analytics-Visualization-Quick-Notebooks.md)) | *"auditors want dashboards"* |

A CloudTrail Athena query looks like this:

```sql
SELECT eventtime, useridentity.arn, eventname, requestparameters
FROM cloudtrail_logs
WHERE eventsource = 's3.amazonaws.com'
  AND eventname = 'DeleteObject'
  AND "timestamp" >= '2026/09/01'           -- partition-projected date column
ORDER BY eventtime DESC;
```

**THE trap:** reaching for **EMR** for a few GB of daily logs ("least operational overhead" → **Athena** or **Logs Insights**), or reaching for **Athena** when logs sit only in CloudWatch Logs. Athena doesn't query log groups natively. Export or subscribe them to S3 first, or just use **Logs Insights**.

## Extracting logs for audits (3.3.1)

| Need | Mechanism |
|---|---|
| Hand auditors CloudTrail history beyond 90 days | Trail delivery to **S3** (org trail → log-archive bucket). Share via bucket policy/presigned access, or query with Athena and export results |
| Quick lookup of recent management events | **Event history** / **`LookupEvents`** (90 days, one Region) |
| One-off copy of application logs from CloudWatch | **CloudWatch Logs export task to S3** (`CreateExportTask`, batch, can take hours, not real time) |
| Continuous copy of logs to S3 | **Subscription filter → Firehose → S3** |
| SQL extracts of audit events | **CloudTrail Lake** query, results saved to S3 |
| Resource configuration evidence | **Config history files / snapshots in S3**, advanced query results, **conformance pack** compliance reports |
| Database query audit | Redshift audit logs in S3, Database Activity Streams → Firehose → S3 |

**THE trap:** using a CloudWatch Logs **export task** for continuous, near-real-time delivery. Exports are batch jobs. The streaming answer is a **subscription filter to Firehose**.

## Question patterns

> *"A security team needs a record of every API call in all 40 accounts and all Regions, stored in a central account where workload admins can't alter it, and must be able to prove log files weren't modified."* → **Organization multi-Region trail → S3 bucket in a log-archive account (SSE-KMS, Object Lock) + log file integrity validation**. Per-account trails can be deleted by account admins, and versioning alone doesn't *prove* integrity.

> *"Auditors ask which IAM role read objects under `s3://amzn-s3-demo-bucket/payroll/` in the last month. Only a default trail exists."* → **Not possible retroactively**. Enable **CloudTrail S3 data events with an advanced selector on the payroll prefix** going forward. Management events and Event history don't include object reads.

> *"Log only write operations on one sensitive DynamoDB table, to control costs."* → **Advanced event selectors**: `resources.type = AWS::DynamoDB::Table`, `resources.ARN` equals the table, `readOnly = false`. Logging all data events wastes money.

> *"Alert the on-call engineer within a minute whenever anyone disables CloudTrail logging or changes a bucket policy."* → **EventBridge rule on the CloudTrail events (`StopLogging`, `DeleteTrail`, `PutBucketPolicy`) → SNS**. Athena queries are after the fact, and a Config periodic rule is slower.

> *"Detect a sudden, unusual spike in `DeleteObject` or AccessDenied API activity without writing thresholds."* → **CloudTrail Insights events**. Static metric-filter alarms need thresholds you'd have to guess.

> *"The company must continuously verify that all S3 buckets have default encryption and Block Public Access, and automatically fix violations across the organization."* → **AWS Config managed rules (conformance pack) + automatic SSM Automation remediation, with an aggregator for visibility**. CloudTrail can't evaluate state.

> *"An engineer needs to see how a Redshift cluster's configuration changed over the last 30 days and who made each change."* → **AWS Config configuration timeline** for the cluster, with **linked CloudTrail events** for the who.

> *"Security analysts want SQL queries over CloudTrail events from the whole organization, retained for 7 years, without building an ETL pipeline or managing tables."* → **CloudTrail Lake organization event data store** (seven-year retention pricing). Note it's closed to new customers since May 31, 2026, so a new customer would use CloudWatch, or Athena on the org trail bucket.

> *"CloudTrail logs in S3 are queried occasionally with Athena; queries scan too much data and new days need manual partition loading."* → **Athena table with partition projection on Region/date**. Crawlers and `MSCK REPAIR` add upkeep.

> *"Glue and Lambda logs from 25 accounts must land in one S3 data lake in near real time for analysis."* → **CloudWatch Logs subscription filters → cross-account destination → Firehose → S3**. Export tasks are batch and per account.

> *"Developers need to quickly find the error lines and count failures by job in last night's Glue job logs."* → **CloudWatch Logs Insights**. No data movement, least effort.

> *"The platform generates 30 TB of application logs per day requiring complex sessionization before loading aggregates into Redshift."* → **Amazon EMR (Spark) over logs in S3**. Logs Insights and Athena alone are the wrong fit for heavy custom processing at this scale.

> *"Operations wants near-real-time dashboards and full-text search over streaming application logs."* → **Amazon OpenSearch Service** (fed by Firehose or OpenSearch Ingestion).

> *"Record every query users run in Redshift for compliance and keep it for 5 years."* → **Enable Redshift audit logging including the user activity log (`enable_user_activity_logging`) to S3** + lifecycle to Glacier. System tables only keep a short history.

> *"DBAs must not be able to disable or alter the audit trail of activity on an Aurora PostgreSQL database."* → **Database Activity Streams** (KMS-encrypted stream to Kinesis outside DBA control). pgAudit logs are managed by the same admins.

> *"A cheap, complete record of all requests to a bucket is needed; a delay of hours is acceptable and IAM detail isn't required."* → **S3 server access logs** to a separate bucket, queried with Athena. CloudTrail data events cost more per event.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Who called which API | **CloudTrail** |
| What changed / is it compliant | **AWS Config** |
| Last 90 days, management events, no setup | CloudTrail **Event history** / `LookupEvents` |
| Object-level S3, Lambda invoke, DynamoDB items, Kinesis records | **Data events** (off by default, extra cost) |
| Filter data events to one prefix / writes only | **Advanced event selectors** (`resources.ARN`, `readOnly`) |
| Unusual API rate/error spikes | **CloudTrail Insights** |
| Calls through VPC endpoints (incl. denied) | **Network activity events** |
| All accounts, all Regions | **Organization multi-Region trail** |
| Prove logs weren't tampered with | **Log file integrity validation** (digest files, `validate-logs`) |
| Prevent log deletion | S3 **Object Lock** / MFA Delete + separate **log-archive account** |
| Encrypt trail logs with your key | **SSE-KMS** (default SSE-S3) |
| React to API calls in near real time | **EventBridge** rule on CloudTrail events |
| Alarm on log patterns | CloudTrail → CloudWatch Logs **metric filter + alarm** |
| SQL over CloudTrail logs in S3 | **Athena** + partition projection |
| Managed immutable SQL audit store, org-wide | **CloudTrail Lake** (⚠️ closed to new customers May 31, 2026 → CloudWatch) |
| CloudTrail Lake retention | Up to **~10 yr** (one-year extendable) / **~7 yr** (seven-year pricing) |
| Config item history over time | **Configuration timeline** |
| Continuous compliance check | **Config rule** (change-triggered or periodic) |
| Custom check without Lambda | Config **Guard custom policy rule** |
| Auto-fix a violation | Config remediation → **SSM Automation** |
| Rules as a deployable bundle | **Conformance pack** |
| Multi-account compliance view | **Config aggregator** |
| "All resources where…" SQL on current state | Config **advanced query** |
| Stream logs to another account/S3 | **Subscription filter** → Kinesis/Firehose (via destination) |
| One-off log copy to S3 | CloudWatch Logs **export task** (batch) |
| Query log groups interactively | **CloudWatch Logs Insights** |
| Search + real-time log dashboards | **OpenSearch** |
| Petabyte-scale log processing | **EMR** |
| Cheap, delayed, complete bucket request log | **S3 server access logs** (different bucket) |
| Redshift query audit | **User activity log** (`enable_user_activity_logging`) |
| Tamper-resistant DB activity audit | **Database Activity Streams** → Kinesis |
| OpenSearch audit logs prerequisite | Fine-grained access control |
| Lake Formation data access audit | CloudTrail (`GetDataAccess`) |
| EMR logs | S3 log URI |

Logs tell you what happened. Keeping the whole platform affordable while it happens is the job of [Guide 44 — Cost Optimization](44-Cost-Optimization.md).
