# 25 · Amazon Redshift Performance, Operations & Security — traffic control, maintenance crews and locked doors

> **Exam map:** D2 · Task 2.1, 2.3 — D3 · Task 3.3 — D4 · Task 4.2, 4.3, 4.4 · **Skills:** 2.1.6, 2.3.6, 3.3.4, 4.2.3, 4.3.1, 4.3.2, 4.4 · **Weight:** 🔥🔥🔥 High · **Read time:** ~24 min

## The idea

The distribution center from [Guide 23](23-Redshift-Architecture-Table-Design.md) is built and stocked ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)). Now it has to run well day after day, and that takes four crews:

- **Traffic control (workload management, WLM)**: orders queue at the front desk. Someone decides which queue an order joins, how many workers each queue gets, and which orders jump the line. When the line gets too long, you **rent a temporary second building** (concurrency scaling). Tiny orders get an express lane (short query acceleration). A repeat order is answered from a photocopy (the result cache).
- **The maintenance crew (VACUUM / ANALYZE)**: re-sorts shelves after new stock arrives, sweeps out deleted boxes and updates the inventory counts the planner relies on.
- **Door locks (locking and isolation)**: two workers mustn't rearrange the same shelf at once. Sometimes one has to wait, and sometimes one gets sent away with an error.
- **Security and insurance**: badges (IAM, RBAC), rooms only some people may enter (row-level security), labels that are blurred for most visitors (dynamic data masking), a safe (KMS encryption), CCTV (audit logs), and backup copies of the whole building in another city (snapshots).

This guide lets you crack questions on **slow queries and queueing**, **disk spill**, **concurrency spikes**, **serializable isolation errors and blocked sessions**, **DR with snapshots**, **encryption**, **database permissions** (skill 4.2.3), **masking and row filters** (skill 4.3.1) and **audit logging**.

## Workload management (WLM)

WLM decides **which queue a query joins, how many queries run at once, and how much memory each one gets**. It applies to **provisioned clusters**. Serverless manages all of this for you (see the end of this section).

| | **Automatic WLM** (default, recommended) | **Manual WLM** |
|---|---|---|
| Concurrency & memory | **Redshift decides** dynamically per query | **You fix** concurrency slots and **memory %** per queue |
| Your lever | **Query priority** per queue: `HIGHEST`, `HIGH`, `NORMAL` (default), `LOW`, `LOWEST` (plus `CRITICAL`, reserved for superusers) | Slots and memory per queue, and a timeout |
| Routing | Queues matched by **user groups / user roles** or a **query group** label (`SET query_group TO 'etl';`) | The same routing rules |
| Pick when | Almost always. *"Least effort"*, *"prioritize dashboards over ETL"* | You need hard, predictable resource reservation per workload |

**Query monitoring rules (QMR)** are guardrails attached to queues. Each rule is **predicate(s) → action**. Predicates are metrics such as `query_execution_time`, `query_cpu_time`, `scan_row_count`, `return_row_count`, `nested_loop_join_row_count` and `query_temp_blocks_to_disk`. The actions are:

- **LOG**: record the event in `STL_WLM_RULE_ACTION`
- **HOP**: move the query to the next matching queue (**manual WLM only**)
- **ABORT**: cancel the query
- **CHANGE PRIORITY**: raise or lower it (**automatic WLM**)

The classic use: *"abort any ad hoc query that scans more than 1 billion rows or runs for more than 30 minutes"*. The QMR templates in the console are the easy way to start.

**Short query acceleration (SQA)** runs queries that Redshift *predicts* will be short in a dedicated space, so they don't queue behind long ETL jobs. It's **on by default**. Signal: *"short dashboard queries wait behind long-running reports"* → **SQA** (and/or a higher priority for the BI queue).

**Concurrency scaling** is the temporary second building. When queries start **queueing** in a WLM queue that has **concurrency scaling mode = auto**, Redshift routes eligible queries to **transient extra clusters** in seconds. They see current data, and you're **billed per second only while those clusters run queries**. **Each cluster accrues up to 1 hour of free concurrency-scaling credit per day**, which covers most bursty workloads.

- **Reads and common writes** are eligible: **COPY, INSERT, DELETE, UPDATE, CTAS and VACUUM** (plus manual MV refresh). **Write** scaling is available **only on RA3/RG**.
- **Not eligible:** most DDL, queries on **interleaved-sort-key tables**, **temporary tables**, Python/Lambda UDFs, system tables, writes to **DISTSTYLE ALL** tables or tables with **IDENTITY** columns. The main cluster must be multi-node and within the node-count limits.
- The number of scaling clusters is capped by `max_concurrency_scaling_clusters` (default **1**).

**THE trap (concurrency scaling vs resize):** concurrency scaling fixes ***many* queries waiting at once** (queue time). It doesn't make ***one* big query** faster. A single slow query needs better table design, statistics, or a larger cluster (elastic resize, more RPUs). Signal words: *"spikes of concurrent users"*, *"queries queued at 9 a.m."* → concurrency scaling.

**Serverless:** there's no WLM to configure. Serverless scales RPUs automatically (between base and max capacity, guided by the price-performance target) and offers **query limits / monitoring rules per workgroup** (for example, a maximum execution time). Separate workloads get **separate workgroups**, linked by data sharing ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)).

## Result caching

The leader node keeps the **results of recent queries**. An identical query from a user with permission is answered **from cache without touching the compute nodes**, as long as **the underlying data hasn't changed** and the query doesn't use volatile functions (such as `GETDATE()`). It's automatic and free. Turn it off per session (`SET enable_result_cache_for_session TO off;`) when **benchmarking**, or your timings will be meaningless.

## VACUUM, ANALYZE and deep copy: the maintenance crew

Redshift **doesn't update in place**. An `UPDATE` is a delete plus an insert, and deleted rows are only *marked*. New rows land in an **unsorted region**. Over time, scans read ghost rows and zone maps lose their precision.

| Command | Does |
|---|---|
| `VACUUM FULL` (**default**) | Re-sorts **and** reclaims space from deleted rows |
| `VACUUM SORT ONLY` | Re-sorts only (fast when there are few deletes) |
| `VACUUM DELETE ONLY` | Reclaims space only (when sort order doesn't matter) |
| `VACUUM REINDEX` | Re-analyses **interleaved** sort keys, then does a full vacuum. The slowest |
| `VACUUM … TO 99 PERCENT` / `BOOST` | Sets the sort/delete threshold (default **95%**) / uses more resources to finish faster |

- **Automatic vacuum delete** and **automatic vacuum sort** run in the background during quiet periods, so modern clusters rarely need hand-run vacuums. After a **huge delete or load**, a manual VACUUM is still the fix. Check **`SVV_TABLE_INFO.unsorted`** and `vacuum_sort_benefit`.
- **Only one VACUUM runs on a cluster at a time.** Schedule big ones off-peak.
- **`ANALYZE`** refreshes the **statistics** the planner uses for join order and distribution decisions. **Automatic analyze** runs in the background. Run it manually after large loads (or use COPY's `STATUPDATE ON`). Stale stats show up as **`SVV_TABLE_INFO.stats_off`** close to 100, or a *"missing statistics"* alert in `STL_ALERT_EVENT_LOG`. `ANALYZE PREDICATE COLUMNS` limits the work to columns used in filters and joins.
- **Deep copy** (`CREATE TABLE new (LIKE old)` → `INSERT INTO new SELECT * FROM old` → swap names, or a CTAS) rebuilds a table fully sorted in one pass. It's **faster than vacuuming a badly unsorted large table** and it's the way to change keys that `ALTER TABLE` can't. The table can't take concurrent writes during the copy.
- **Loading in sort-key order** (for example, appending data by timestamp) keeps tables almost sorted, so they barely need vacuuming.

## Locks and isolation (skill 2.1.6)

### Isolation levels

Redshift offers two **serializable** isolation levels, and each transaction works on a **snapshot** of committed data taken at its first statement:

| Level | Behaviour | Default? |
|---|---|---|
| **SNAPSHOT** | Concurrent transactions may **both commit** as long as they don't write the same rows. Higher throughput | **Current default for newly created provisioned clusters and Serverless workgroups** |
| **SERIALIZABLE** | Stricter. If no serial order of the concurrent transactions could produce the result, one is **rolled back with error 1023** (prevents write skew) | The historical default. Many existing clusters and most prep material assume it |

Set per database: `CREATE DATABASE sales ISOLATION LEVEL SNAPSHOT;` or `ALTER DATABASE sales ISOLATION LEVEL SERIALIZABLE;`. Check with `STV_DB_ISOLATION_LEVEL`.

**Error 1023, "Serializable isolation violation on table …"**, means two concurrent transactions touched the same table in a way that can't be serialized, typically two ETL jobs that read and write overlapping tables. Remedies, in exam order:

1. **Serialize the writers**: run the jobs one after another (orchestration), or have each transaction **`LOCK` the tables it needs at the very start**, always in the same order.
2. **Keep transactions short** and move statements that don't need to be atomic *out* of the transaction.
3. **Retry** the failed transaction.
4. Where write-skew protection isn't required, consider **SNAPSHOT isolation**.

```sql
BEGIN;
LOCK public.orders, public.order_totals;     -- explicit lock: other writers wait instead of failing
DELETE FROM public.order_totals WHERE dt = '2026-09-26';
INSERT INTO public.order_totals
SELECT dt, SUM(amount) FROM public.orders WHERE dt = '2026-09-26' GROUP BY dt;
COMMIT;                                       -- locks are released at commit/rollback
```

### Lock modes

| Lock | Taken by | Blocks |
|---|---|---|
| **AccessExclusiveLock** | DDL: `ALTER TABLE`, `DROP`, `TRUNCATE`, `VACUUM` of some kinds | **Everything**, including reads |
| **ShareRowExclusiveLock** | Writes: `COPY`, `INSERT`, `UPDATE`, `DELETE` | Other writers and DDL. **Reads still work** |
| **AccessShareLock** | `SELECT` / `UNLOAD` | Only AccessExclusiveLock (a running SELECT makes an `ALTER`/`DROP` wait) |

**Finding and clearing a blocker:** a DDL or ETL step "hangs" because someone's session holds a lock (often an idle session with an open transaction from a SQL client).

```sql
SELECT table_id, last_update, lock_owner, lock_owner_pid FROM stv_locks;     -- locks currently held
SELECT xid, pid, txn_owner, lock_mode, relation, granted FROM svv_transactions
WHERE granted = 'f';                                                         -- who's waiting
SELECT PG_TERMINATE_BACKEND(12345);                                          -- end the blocking session (pid)
```

Also useful: `STV_SESSIONS` (active sessions), `PG_CANCEL_BACKEND(pid)` / `CANCEL pid` (cancel a query but keep the session). Lock mechanics in RDS/Aurora are covered in [Guide 28](28-RDS-Aurora-Purpose-Built-DBs.md).

## Diagnosing performance (skill 3.3.4)

| Tool | Tells you |
|---|---|
| `EXPLAIN` | The plan: join types, **redistribution labels** (DS_BCAST_INNER, DS_DIST_BOTH, see [Guide 23](23-Redshift-Architecture-Table-Design.md)), nested loops |
| **`SYS_QUERY_HISTORY`, `SYS_QUERY_DETAIL`, `SYS_LOAD_HISTORY`** | **SYS monitoring views**, which work on **both provisioned and Serverless**: runtimes, queue time, bytes scanned, spill, per-step detail |
| `SVL_QUERY_SUMMARY` / `SVL_QUERY_REPORT` | Per-step rows, bytes and **`is_diskbased = 't'`** (the step spilled to disk) |
| `STL_ALERT_EVENT_LOG` | Planner/runtime alerts: missing statistics, nested loop, very large broadcast/distribution, scanning deleted rows, with suggested fixes |
| `STL_WLM_QUERY` / `STV_WLM_QUERY_STATE` | Queue time vs execution time per WLM queue |
| `SVV_TABLE_INFO` | Skew (`skew_rows`), `unsorted`, `stats_off`, encoding, size |
| **Redshift Advisor** | Console recommendations (keys, compression, file splitting, statistics, WLM) |
| Console query monitoring | Visual plan and execution details, queue and concurrency charts |

STL/STV/SVL tables exist only on **provisioned** clusters and keep **only a few days** of history. For long-term analysis, use audit logs or copy the data out.

### Symptom → fix

| Symptom | Likely cause | Fix |
|---|---|---|
| High **queue time**, low execution time, at peak hours | Concurrency | **Concurrency scaling**, auto WLM priorities, SQA, or a separate warehouse via data sharing |
| **Disk-based** steps (`is_diskbased`), `query_temp_blocks_to_disk` | Not enough memory per query | Auto WLM (or more memory % for the queue in manual WLM), fewer concurrent slots, bigger nodes/RPUs, rewrite the query (select fewer columns, filter earlier) |
| Bad join order, "missing statistics" alert, `stats_off` high | **Stale statistics** | **ANALYZE** (or COPY `STATUPDATE ON`) |
| One slice runs much longer than the others, `skew_rows` high | **Distribution skew** | Higher-cardinality DISTKEY, or EVEN/AUTO |
| `DS_BCAST_INNER` / `DS_DIST_BOTH` on big joins | **Redistribution** | Co-locate the join: same DISTKEY on both tables, or ALL for the small dimension |
| Range query reads every block, `unsorted` high | Poor sort key / unsorted rows | Sort key on the filter column, **VACUUM SORT**, deep copy |
| Scans read many deleted rows | Heavy UPDATE/DELETE | **VACUUM DELETE** (auto vacuum), batch changes |
| Commit queue waits, slow loads | **Too many small commits** (row-by-row inserts, many tiny COPYs) | **Batch into fewer, larger COPYs/transactions** |
| Nested loop alert | Missing or incorrect join condition (Cartesian product) | Fix the join predicate |

## Snapshots, recovery and DR (skill 2.3.6)

| Feature | Provisioned | Serverless |
|---|---|---|
| Automatic backups | **Automated snapshots**, incremental, roughly **every 8 hours or every 5 GB of changed data per node**. **Retention 1 day by default, configurable up to 35 days** (you can only set 0, meaning off, on DC2) | **Recovery points every 30 minutes, kept 24 hours** (free). Convert one to a snapshot to keep it longer |
| Manual snapshots | Kept **until you delete them** (or for a set retention period). Can be **shared with other accounts** | Manual snapshots, same idea |
| Restore | **Always to a new cluster** (or a Serverless namespace). **Table-level restore** brings one table back into the existing warehouse | Restore a snapshot/recovery point to the namespace, or restore a single table |
| Cross-Region | **Cross-Region snapshot copy**: automatic copies to a destination Region with their own retention. For a **KMS customer managed key**, create a **snapshot copy grant** in the destination Region | Cross-Region snapshot copy is also supported |
| AZ resilience | **Multi-AZ** (RA3/RG), **cluster relocation** | Handled by the service |

**THE trap:** *"Recover from a Region-wide outage"* → **cross-Region snapshot copy**, then restore in the other Region. Multi-AZ protects against an **AZ** failure only. *"A user dropped one table"* → **table-level restore** from a snapshot, not a full cluster restore. Snapshot sharing across accounts also needs the KMS key to be shared. Data-sovereignty controls that stop snapshot copies leaving approved Regions are in [Guide 42](42-Privacy-PII-Masking-Sovereignty.md). RPO/RTO comparisons across data stores are in [Guide 31](31-Data-Lifecycle-Retention-Resiliency.md).

## Encryption (skill 4.3.2)

- **At rest:** **encrypted by default**. Since **January 2025**, new provisioned clusters and Serverless workgroups are encrypted even if you don't specify a key, using an **AWS-owned key** by default. For control, audit and rotation, choose a **customer managed KMS key** (it can live in another account). Redshift uses a **four-tier key hierarchy**: KMS root key → cluster encryption key → database encryption key → data block keys. Keys can be **rotated**, and the cluster briefly enters a `rotating-keys` state.
- **Encrypting an existing unencrypted cluster:** just **modify the cluster** to enable KMS encryption. Redshift **migrates the data to an encrypted cluster automatically**. On RA3/RG, reads and writes keep working during the migration. Unload/reload is no longer required.
- **HSM:** HSM-based key management needs AWS CloudHSM **Classic** (closed to new customers) and **isn't supported on DC2, RA3 or RG**. Treat it as a legacy distractor and pick **KMS**.
- **If a customer managed key is disabled**, the warehouse becomes `inaccessible-kms-key`. Restore the key within **14 days** or Redshift deletes the warehouse (after taking a backup).
- **In transit:** set **`require_ssl = true`** in the parameter group, which is on by default in the `default.redshift-2.0` parameter group used by new clusters since 2025. Clients use TLS (`sslmode=verify-full`). COPY/UNLOAD reach S3 over HTTPS. **UNLOAD output is SSE-S3 encrypted by default**, and `KMS_KEY_ID` gives SSE-KMS ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)). All the cross-service KMS detail is in [Guide 39](39-Encryption-Key-Management.md).

## Network controls

- **Publicly accessible** is **off by default** (since 2025). Keep warehouses in private subnets, and reach them through a VPN/Direct Connect, a bastion, or a **Redshift-managed VPC endpoint** (cross-VPC / cross-account access over PrivateLink).
- **Security groups** control inbound access on the port (default 5439). Allow-list only the application or BI subnets.
- **Enhanced VPC routing** forces **COPY/UNLOAD traffic through your VPC**, so S3 gateway endpoints, endpoint policies, NAT and VPC flow logs apply, instead of the traffic going over the public AWS network. Signal: *"all COPY/UNLOAD traffic must stay within the VPC and be auditable in flow logs"*. Details in [Guide 38](38-Networking-for-Data-Pipelines.md).

## Authentication

| Method | How it works | Pick when |
|---|---|---|
| Database user + password | `CREATE USER … PASSWORD`. Store the admin secret in **Secrets Manager** (Redshift can manage and rotate it) | Service accounts, legacy |
| **IAM temporary credentials** | `GetClusterCredentials` (provisioned: temporary password for a `DbUser`, optionally auto-created and added to `DbGroups`), **`GetClusterCredentialsWithIAM`** (database identity derived from the IAM identity), Serverless **`GetCredentials`**. JDBC/ODBC `jdbc:redshift:iam://` URLs do this for you | *"No long-lived database passwords"*, apps with IAM roles |
| **IdP federation** | Okta, Microsoft Entra ID, ADFS, Ping and others via SAML/OAuth through the Redshift JDBC/ODBC drivers, or a **native IdP** registered in Redshift. IdP groups map to database roles | Corporate SSO for analysts |
| **IAM Identity Center + trusted identity propagation** | Users sign in once, and their **own identity flows** from Quick Sight / Query Editor v2 / SageMaker Unified Studio into Redshift. Permissions, Lake Formation grants and audit logs then apply **per end user** rather than per shared service account | *"Audit and authorize each user's identity end to end across services"* |

The Data API can use a Secrets Manager secret or IAM temporary credentials ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)). The IAM policy side is in [Guide 37](37-IAM-for-Data-Engineers.md).

## Authorization inside the database (skill 4.2.3)

- **Users, groups, roles.** Groups are the legacy way to bundle users. **Role-based access control (RBAC)** is the modern way: `CREATE ROLE`, grant privileges to the role, then **`GRANT ROLE` to users or to other roles** (roles can nest).
- **System-defined roles:** `sys:monitor` (read catalog/system tables), `sys:operator` (+ ANALYZE, VACUUM, cancel queries), `sys:dba` (create/drop schemas, tables, views, procedures + operator), `sys:secadmin` (manage users/roles, **RLS and masking policies**, but **no access to user data unless granted**), `sys:superuser` (all system permissions). The `sys:secadmin` split is the **separation-of-duties** answer.
- **GRANT hierarchy:** `USAGE` on the **schema** *and* `SELECT` (or INSERT/UPDATE/DELETE) on the **table**. Missing schema USAGE is the classic *"permission denied even though I granted SELECT"* cause. **`ALTER DEFAULT PRIVILEGES`** grants on tables created in the future.
- **Column-level security:** `GRANT SELECT (col1, col2) ON table TO ROLE …`. Users simply can't select the other columns.

```sql
CREATE ROLE analyst;
GRANT USAGE ON SCHEMA sales TO ROLE analyst;
GRANT SELECT (order_id, order_date, region, amount) ON sales.orders TO ROLE analyst;   -- column-level
GRANT ROLE analyst TO alice;
ALTER DEFAULT PRIVILEGES IN SCHEMA sales GRANT SELECT ON TABLES TO ROLE analyst;
GRANT ROLE sys:secadmin TO security_officer;       -- manages policies, can't read data by default
```

## Row-level security and dynamic data masking (skill 4.3.1)

**Row-level security (RLS)** filters **which rows** a user sees. You define a policy predicate, attach it to a table for roles/users, and switch RLS on for the table:

```sql
CREATE RLS POLICY rls_own_region
WITH (region VARCHAR(20))
USING (region = current_user);          -- or compare against a lookup of the user's regions

ATTACH RLS POLICY rls_own_region ON sales.orders TO ROLE regional_manager;
ALTER TABLE sales.orders ROW LEVEL SECURITY ON;
```

Once RLS is **ON**, a user with **no applicable policy sees no rows** (deny by default). Multiple policies on the same table are combined with **AND** by default.

**Dynamic data masking (DDM)** changes **what a column value looks like** at query time, per role. The stored data is **unchanged**. The highest-**PRIORITY** policy attached for a user wins:

```sql
CREATE MASKING POLICY mask_ssn
WITH (ssn VARCHAR(11))
USING ('XXX-XX-' || SUBSTRING(ssn, 8, 4));          -- show last 4 digits only

CREATE MASKING POLICY unmask_ssn
WITH (ssn VARCHAR(11))
USING (ssn);                                         -- full value

ATTACH MASKING POLICY mask_ssn   ON hr.employees(ssn) TO PUBLIC;
ATTACH MASKING POLICY unmask_ssn ON hr.employees(ssn) TO ROLE payroll_admin PRIORITY 20;
```

Masking expressions can be **conditional** (`CASE` on other columns or on the user) and can call hashing functions (`SHA2`) for pseudonymization. Choosing a control:

| Need | Control |
|---|---|
| Hide entire columns from a role | **Column-level GRANT** |
| Show a column, but redacted/partial/hashed for most users | **Dynamic data masking** |
| Each manager sees only their region's rows | **Row-level security** |
| Govern S3/Spectrum and datashare data centrally, with row/cell filters | **Lake Formation** ([Guide 40](40-Lake-Formation.md)) |
| Permanently anonymize before the data even lands | ETL-time masking/tokenization ([Guide 42](42-Privacy-PII-Masking-Sovereignty.md)) |

**THE trap:** building **one view per region or per role** to hide rows or columns is the old, high-maintenance answer. **RLS + DDM policies attached to roles** give *least operational overhead* and apply to every query path.

## Audit logging (skill 4.4)

| Log | Captures | Needs |
|---|---|---|
| **Connection log** | Authentication attempts, connections, disconnections (who, from where) | Audit logging enabled |
| **User log** | Changes to database users (create, alter, drop) | Audit logging enabled |
| **User activity log** | **Every SQL statement** before it runs | Audit logging **plus** parameter **`enable_user_activity_logging = true`** |

- **Destinations:** an **S3 bucket** or **CloudWatch Logs**. CloudWatch gives near-real-time search, metric filters and alarms, plus retention settings. Serverless exports to CloudWatch Logs.
- **THE trap:** relying on system tables (`STL_QUERY`, `STL_CONNECTION_LOG`) for audits fails because they keep **only days** of history. For *"retain query history for one year for auditors"*, **enable audit logging to S3/CloudWatch** and analyse it with Athena or Logs Insights.
- **AWS CloudTrail** records **Redshift API calls** (`CreateCluster`, `ModifyCluster`, `RestoreFromClusterSnapshot`, Data API `ExecuteStatement` calls) but **not the SQL executed inside the database**. *"Who changed the cluster configuration?"* → CloudTrail. *"Who ran this SELECT?"* → the user activity log. See [Guide 43](43-Audit-Logging-CloudTrail-Config.md).

## Monitoring and events

- **CloudWatch metrics (provisioned):** `CPUUtilization`, `PercentageDiskSpaceUsed`, `DatabaseConnections`, `HealthStatus`, `QueryDuration`, `WLMQueueLength`, `WLMQueriesCompletedPerSecond`, `ReadIOPS`/`WriteIOPS`, `ConcurrencyScalingActiveClusters`/`ConcurrencyScalingSeconds`. **Serverless:** `ComputeCapacity` (RPUs in use), `ComputeSeconds`, `QueriesRunning`/`QueriesQueued`. Set alarms (for example, **disk over 80%**, or queue length) → SNS.
- **Events:** Redshift emits cluster/workgroup **event notifications** (maintenance, resize, snapshot, failures, zero-ETL integration state) through **SNS event subscriptions** and **Amazon EventBridge**. Data API completion events come through EventBridge too ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)).

## Question patterns

> *"Every morning at 9 a.m., hundreds of analysts run dashboards and queries wait in the WLM queue for minutes. Individual query execution time is fine. MOST cost-effective fix?"* → **Enable concurrency scaling on the queue** (queue time, not execution time, is the problem; free daily credits cover short bursts; a permanent resize wastes money off-peak).

> *"Short BI queries are delayed behind long-running ETL transformations on the same provisioned cluster."* → **Automatic WLM with a higher priority for the BI queue + short query acceleration.** Or isolate ETL on a separate warehouse with data sharing if the contention is chronic.

> *"The platform team wants to automatically cancel ad hoc queries that run longer than 30 minutes or scan more than 1 billion rows."* → **WLM query monitoring rule with the ABORT action** (HOP exists only in manual WLM; statement_timeout alone can't express row-scan limits).

> *"Two nightly ETL jobs updating overlapping tables intermittently fail with 'ERROR 1023: Serializable isolation violation'."* → **Serialize the jobs (orchestrate sequentially) or LOCK the tables at the start of each transaction; keep transactions short and retry.** SNAPSHOT isolation is an option when write-skew protection isn't needed.

> *"An ALTER TABLE in the deployment pipeline hangs indefinitely."* → **Find the session holding a lock (STV_LOCKS / SVV_TRANSACTIONS, granted = false) and end it with PG_TERMINATE_BACKEND.** Often it's an idle client session with an open transaction.

> *"Queries slowed down after a large daily load. SVV_TABLE_INFO shows stats_off = 90 and unsorted = 40%."* → **Run ANALYZE and VACUUM (SORT)** (or rely on COPY STATUPDATE plus automatic vacuum/analyze).

> *"SVL_QUERY_SUMMARY shows is_diskbased = true for the hash join steps of a critical query on a manual WLM cluster."* → **Give the queue more memory (fewer slots or a higher memory %) or switch to automatic WLM.** Also check for unnecessary columns and missing filters.

> *"A company must be able to recover its Redshift warehouse in another Region if the primary Region becomes unavailable. The cluster uses a customer managed KMS key."* → **Enable cross-Region snapshot copy with a snapshot copy grant in the destination Region**, then restore there. Multi-AZ doesn't cover Region failure.

> *"An analyst accidentally dropped a critical table an hour ago."* → **Table-level restore from the latest snapshot** (or, on Serverless, restore the table from a recovery point, available every 30 minutes for 24 hours).

> *"Security requires that an existing unencrypted provisioned cluster be encrypted with a customer managed key, with minimal effort."* → **Modify the cluster to enable KMS encryption with the CMK** (Redshift migrates the data automatically; unload/reload into a new cluster is no longer necessary; HSM isn't supported on RA3).

> *"Analysts must see customer email addresses masked except for the support-manager role, and queries must not need to change."* → **Dynamic data masking policy attached to PUBLIC, plus an unmasking policy attached to the support_manager role with higher PRIORITY.** Creating separate views per role takes more effort.

> *"Regional managers may only see rows for their own region in the sales table."* → **Row-level security: CREATE RLS POLICY, ATTACH to the managers' role, ALTER TABLE … ROW LEVEL SECURITY ON.**

> *"Security officers must manage users, roles and masking policies but must not be able to read business data."* → **Grant the sys:secadmin system role** (separation of duties; superuser would expose the data).

> *"Auditors require a record of every SQL statement run against the warehouse, kept for 1 year."* → **Enable audit logging to S3 (or CloudWatch Logs) with enable_user_activity_logging = true.** System tables keep only days of history, and CloudTrail doesn't capture SQL text.

> *"Applications should connect to Redshift without storing long-lived database passwords."* → **IAM authentication: GetClusterCredentials / GetClusterCredentialsWithIAM (Serverless: GetCredentials) through the IAM JDBC driver, or the Data API with IAM** (Secrets Manager rotation is the fallback if a password is unavoidable).

> *"All COPY and UNLOAD traffic between Redshift and S3 must go through the VPC so it is controlled by VPC endpoint policies and captured in flow logs."* → **Enable enhanced VPC routing** (plus an S3 gateway endpoint).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Default WLM | Automatic WLM with priorities (HIGHEST…LOWEST, NORMAL default) |
| Fixed memory/slots per workload | Manual WLM |
| Route queries to a queue | User groups/roles or `SET query_group` |
| Abort/log/hop runaway queries | Query monitoring rules (HOP = manual only, change priority = auto) |
| Short queries stuck behind long ones | Short query acceleration (on by default) |
| Many queries queueing at peak | Concurrency scaling (queue mode auto, ~1 free hour/day accrues) |
| Concurrency scaling for writes | COPY/INSERT/UPDATE/DELETE/CTAS/VACUUM, RA3/RG only |
| Not eligible for concurrency scaling | Interleaved sort keys, temp tables, Python/Lambda UDFs, most DDL |
| Serverless workload management | Automatic RPU scaling + workgroup query limits (no WLM) |
| Repeat identical query, data unchanged | Result cache (disable for benchmarks) |
| Reclaim deleted space + re-sort | VACUUM FULL (default, 95% threshold). One VACUUM at a time |
| Interleaved key maintenance | VACUUM REINDEX |
| Stale stats | ANALYZE (auto analyze, COPY STATUPDATE) |
| Badly unsorted huge table | Deep copy |
| Default isolation (new clusters/Serverless) | SNAPSHOT (SERIALIZABLE is the older default) |
| Error 1023 | Serializable violation: serialize/LOCK early, shorten, retry |
| Blocked session | STV_LOCKS / SVV_TRANSACTIONS → PG_TERMINATE_BACKEND |
| DDL lock | AccessExclusiveLock (blocks reads) |
| Write lock | ShareRowExclusiveLock (reads OK) |
| Query history on provisioned and Serverless | SYS_QUERY_HISTORY / SYS_QUERY_DETAIL |
| Disk spill | SVL_QUERY_SUMMARY is_diskbased → more memory per query |
| Planner warnings | STL_ALERT_EVENT_LOG |
| Automated snapshots | ~8 h or 5 GB/node of changes. 1-day default, up to 35 |
| Serverless recovery points | Every 30 min, kept 24 h |
| Region DR | Cross-Region snapshot copy (+ snapshot copy grant for a CMK) |
| AZ failure | Multi-AZ (RA3/RG) |
| Dropped one table | Table-level restore |
| Encrypt existing cluster | Modify cluster → KMS (automatic migration) |
| Encryption default | On since Jan 2025 (AWS-owned key unless you choose a CMK) |
| HSM | Not supported on RA3/RG/DC2. Distractor |
| Force TLS | require_ssl = true (default in default.redshift-2.0) |
| COPY/UNLOAD via VPC | Enhanced VPC routing |
| No DB passwords | GetClusterCredentials(WithIAM) / Serverless GetCredentials / Data API |
| Per-user identity end to end | IAM Identity Center trusted identity propagation |
| Modern permissions | RBAC: CREATE ROLE, GRANT ROLE |
| Security admin without data access | sys:secadmin |
| Permission denied despite SELECT | Missing USAGE on schema |
| Hide columns | Column-level GRANT |
| Redact values per role | Dynamic data masking (PRIORITY decides) |
| Filter rows per user | RLS policy + ROW LEVEL SECURITY ON (no policy = no rows) |
| Every SQL statement audited | User activity log (enable_user_activity_logging) → S3/CloudWatch |
| Who changed cluster settings | CloudTrail |
| Watch disk / queues / RPUs | CloudWatch: PercentageDiskSpaceUsed, WLMQueueLength, ComputeCapacity |

With Redshift tuned, secured and backed up, compare it with its serverless query sibling on the lake: [Guide 26 — Amazon Athena](26-Amazon-Athena.md).
