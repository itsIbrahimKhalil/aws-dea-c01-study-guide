# 23 · Amazon Redshift Architecture & Table Design — which shelf every box lands on

> **Exam map:** D2 · Task 2.1, 2.4 · **Skills:** 2.1.1, 2.1.2, 2.4.1, 2.4.5 · **Weight:** 🔥🔥🔥 High · **Read time:** ~22 min

## The idea

Picture a huge **distribution center**. At the front desk sits one **manager** who takes every order, works out the fastest way to fill it, splits it into tasks and hands those out. Behind the desk are **workers**, and each worker owns a few **aisles** of shelving. Every worker picks from their own aisles at the same time, then passes partial results back to the manager, who puts them together and hands the finished order to the customer. That is Amazon Redshift. The manager is the **leader node**, the workers are **compute nodes**, and each aisle is a **slice**. The design is **MPP** (massively parallel processing): many workers each scan their small share of the data at once, so a scan of billions of rows finishes in seconds.

Two things make or break a distribution center. First, **which aisle each box goes into**. If the boxes an order needs sit in the same aisle, one worker grabs them. If they're spread across the building, workers spend the day carting boxes to each other. In Redshift that's the **distribution style / distribution key**. Second, **how boxes are ordered on the shelves**. If every shelf carries a label like "orders from 1–7 March", a worker can skip whole shelves without opening a box. That's the **sort key**, and the shelf labels are **zone maps**. Add **columnar storage** (every shelf holds one attribute only: all prices together, all dates together) and **compression** (vacuum-packing the boxes), and you have the complete physical design story.

Redshift is a **columnar OLAP data warehouse**. OLAP means online analytical processing: big scans, aggregations and joins, as opposed to OLTP's small single-row transactions. Redshift speaks a PostgreSQL-flavoured SQL and connects over JDBC/ODBC. It comes in two shapes: **provisioned clusters**, where you pick node types and counts, and **Redshift Serverless**, where you pick a capacity range and AWS does the rest.

This guide lets you crack questions on **provisioned vs Serverless**, **node types and resizing**, **DISTSTYLE/DISTKEY/SORTKEY choices from a query description**, **EXPLAIN redistribution labels**, **skew**, **compression**, **SUPER and data types**, the **unenforced-constraints trap**, and **star-schema design in Redshift** (skill 2.4.1). Loading and integration are in [Guide 24](24-Redshift-Loading-Integration-Sharing.md). Performance tuning, operations and security are in [Guide 25](25-Redshift-Performance-Operations-Security.md).

## Architecture: leader, compute nodes, slices

```
                 SQL clients (JDBC/ODBC, Query Editor v2, Data API)
                                  |
                          +---------------+
                          |  LEADER NODE  |  parse -> plan -> compile -> coordinate -> final aggregate
                          +---------------+
                        /         |         \
             +-----------+  +-----------+  +-----------+
             | Compute 1 |  | Compute 2 |  | Compute 3 |   execute plan steps in parallel,
             | slice0  s1|  | s2     s3 |  | s4     s5 |   exchange rows when a join needs it
             +-----------+  +-----------+  +-----------+
                        \         |         /
                 Redshift Managed Storage (SSD cache on nodes + Amazon S3)   <- RA3 / RG
```

- **Leader node**: receives queries, parses them, builds the execution plan, compiles code, sends steps to compute nodes, runs the final aggregation/sort and returns results. On a multi-node cluster **you don't pay for the leader node**. A **single-node** cluster shares one node for leader and compute roles. It's fine for dev, but AWS doesn't recommend it for production.
- **Compute nodes**: execute the plan and store (or cache) the data.
- **Slices**: each compute node is split into slices, and each slice gets its own share of memory and disk. **Rows are distributed to slices**, and every slice works in parallel. This matters for loading: a `COPY` of many files gives each slice its own file to read (see [Guide 24](24-Redshift-Loading-Integration-Sharing.md)).

## Node types (provisioned)

| Family | Status (Sept 2026) | Storage model | Use when |
|---|---|---|---|
| **RA3** (`ra3.large`, `ra3.xlplus`, `ra3.4xlarge`, `ra3.16xlarge`) | Current. The classic exam answer | **Redshift Managed Storage (RMS)**: local SSD cache plus Amazon S3. **Compute and storage scale and bill separately** | Data growing fast, you query only part of it, you need data sharing / Multi-AZ / zero-ETL / concurrency scaling for writes |
| **RG** (`rg.large`, `rg.xlarge`, `rg.4xlarge`, `rg.12xlarge`) | **New in 2026** (Graviton) | Same RMS model as RA3 | AWS's recommended upgrade target now. Too new for the exam question pool |
| **DC2** (`dc2.large`, `dc2.8xlarge`) | **Deprecated** | Local NVMe SSD only. More storage means more nodes | Legacy only. Recognise it as a distractor |
| **DS2** | **Gone** ("no longer available") | Local HDD | Never |

RA3 specs worth knowing: `ra3.4xlarge` = 12 vCPU / 96 GiB / **4 slices**, `ra3.16xlarge` = 48 vCPU / 384 GiB / **16 slices**, and both allow up to **128 TB of managed storage per node**. `ra3.large` and `ra3.xlplus` also come as **single-node** options. With RMS you size the cluster for **compute** (how much you process each day) and pay for storage **by GB-month**, whether blocks sit in the SSD cache or in S3. Redshift uses block temperature and age to decide what stays hot on local SSD.

> ⚠️ **2026 status:** AWS announced the **DC2 deprecation in April 2025**. New DC2 clusters can no longer be created, and resizing an RA3/RG cluster *to* DC2 is blocked. Existing DC2 users are pushed to RG, RA3 or Serverless (an elastic resize or snapshot restore does the upgrade). **RG** (Graviton) launched **12 May 2026** with `rg.xlarge`/`rg.4xlarge`, and `rg.large`/`rg.12xlarge` were added **16 July 2026**. AWS claims up to **2.4x** RA3 performance at **30% lower price per vCPU**. RG also has an **integrated data lake engine**: Iceberg/Parquet queries run on the cluster's own nodes instead of the separate Redshift Spectrum fleet, so there's no per-TB Spectrum charge and cross-Region S3 queries work. On the exam, **"scale storage independently of compute" = RA3 with managed storage** (or Serverless). DC2 is only ever the wrong answer for growing data.

**THE trap:** *"Storage is nearly full but CPU is at 20%. What's the most cost-effective fix?"* Adding DC2 nodes buys compute you don't need. **Moving to RA3 (managed storage)** lets storage grow on S3 while compute stays the same size.

### Resizing, pausing, reserving

| Operation | What it does | Time / impact |
|---|---|---|
| **Elastic resize** | Add/remove nodes of the same type, or change node type (such as DC2→RA3). Redistributes **slices**, not rows, so slices per node can change | **~10 minutes on average**. Brief metadata-migration pause, then read/write while data rebalances in the background. RA3/RG can usually grow **up to 4x** (`ra3.xlplus`/`large`: 2x) and shrink to a quarter (or half). **Can't be cancelled**. AWS recommends trying it first |
| **Classic resize** | Any node count/type change, including outside elastic limits. Builds a new cluster and migrates data | Much longer (hours to days for non-RA3 targets). To RA3/RG it runs as a backup-and-restore, and the cluster is read/write within minutes while data migrates in the background |
| **Scheduled resize** | Elastic or classic on a timetable | *"Double capacity every month-end, shrink afterwards"* |
| **Pause / resume** | Stops compute billing, and you pay only for storage/backups | Dev/test clusters used during office hours |
| **Reserved nodes** | 1- or 3-year commitment for steady 24/7 clusters | Biggest discount for predictable workloads |

**Elastic resize is not a VACUUM.** It doesn't sort tables or reclaim space.

### Multi-AZ (provisioned RA3/RG)

A **Multi-AZ** deployment runs **equal compute in two Availability Zones behind one endpoint**. Both sets of nodes serve reads **and writes** in normal operation. Data lives once in RMS (S3 is regional), so it isn't copied between clusters. If one AZ fails, the other keeps working, and Redshift provisions new secondary compute in another AZ. The SLA rises to **99.99%** (single-AZ: **99.9%**). **Supported only on RA3/RG**, because it depends on managed storage. Signal: *"must survive an AZ failure with minimal downtime and no data loss"*. For **Region-level** DR you need cross-Region snapshot copy instead ([Guide 25](25-Redshift-Performance-Operations-Security.md)). A single-AZ RA3 cluster can also use **cluster relocation**, which moves it to another AZ after an AZ problem.

## Redshift Serverless

Serverless removes node planning altogether. It splits a warehouse into two objects:

| Object | Holds | Analogy |
|---|---|---|
| **Namespace** | **Storage side**: databases, schemas, tables, users, roles/permissions, the KMS key, snapshots, IAM roles for COPY/UNLOAD | The building and its stock |
| **Workgroup** | **Compute side**: RPU capacity settings, network (VPC, subnets, security groups, endpoint), usage limits, price-performance target | The shift of workers |

Each namespace pairs with one workgroup. If you need **separate compute for separate teams**, create several namespace/workgroup pairs and connect them with **data sharing** ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)).

**Capacity is measured in RPUs** (Redshift Processing Units). **1 RPU = 16 GB of memory.**

- **Base capacity**: the starting compute for queries. **Default 128 RPUs.** You can set it from **4 RPUs** up to **512 RPUs**, in steps of 8 from 8 upwards. Some large Regions go to **1024**, in steps of 32 above 512. Small bases carry limits: **4 RPU is only for up to 32 TB** of managed storage, 8–24 RPU up to 128 TB, and above 128 TB you need **at least 32 RPUs**.
- **Max capacity**: the ceiling Serverless may **scale up** to. It's your predictability guardrail.
- **Usage limits**: RPU-hours per **day/week/month**, with an action when breached: **log to system table**, **alert** (SNS) or **turn off user queries**. You can also set a limit on cross-Region data sharing transfer.
- **AI-driven scaling and optimization**: a per-workgroup **price-performance target** slider that runs from *Optimizes for cost* through **Balanced (the default for new workgroups)** to *Optimizes for performance*. Serverless learns from your workload and scales RPUs for complex queries and growing data. Max capacity and max RPU-hours are always enforced on top.

**Billing:** **RPU-hours, metered per second, with a 60-second minimum**, and **only while queries are running**. An idle workgroup doesn't cost compute. The traps: open transactions and connection-pool "heartbeat" queries keep RPUs busy. Storage is billed separately as RMS GB-month. **Data lake (Spectrum-style) and federated queries are billed as RPU time**, not per TB. Recovery points taken every 30 minutes and kept 24 hours are free ([Guide 25](25-Redshift-Performance-Operations-Security.md)). Serverless reservations give a discount for steady usage.

### Provisioned vs Serverless: the decision

| Signal in the question | Pick |
|---|---|
| *"Unpredictable / spiky / intermittent"*, *"without managing infrastructure"*, *"least operational overhead"*, new project with unknown sizing | **Serverless** |
| Steady, 24/7, well-understood workload, *"most cost-effective over 3 years"* | **Provisioned RA3/RG + reserved nodes** |
| Dev cluster used 9–5 | Provisioned with **pause/resume** on a schedule, or Serverless (costs nothing when idle) |
| Must cap spend on Serverless | **Max capacity + usage limits** ("turn off user queries") |
| Needs manual WLM queues or fine-grained node control | **Provisioned** (Serverless manages workload itself) |

## Columnar storage, 1 MB blocks and zone maps

Redshift stores **each column separately** in **1 MB blocks**. Three things follow:

1. A query touching 4 of 120 columns reads only those 4 columns' blocks. `SELECT *` on a wide table is the most wasteful thing you can do.
2. Values in a block share a type, so they **compress extremely well**, and compressed blocks mean less I/O.
3. For every block Redshift keeps the **min and max value** in its metadata: the **zone map**. With a range predicate (`WHERE sold_at >= '2026-09-01'`), whole blocks whose min/max range can't match are **skipped without being read**. Zone maps only prune well when the data is **sorted on that column**. Five years of data sorted by date lets a one-month query skip up to **~98%** of blocks. Unsorted, it may read them all.

## Compression encodings

| Encoding | Best for |
|---|---|
| **AZ64** | Amazon's own codec for **numeric, DATE, TIMESTAMP, TIMESTAMPTZ** (SMALLINT/INT/BIGINT/DECIMAL). High ratio, fast (SIMD) |
| **ZSTD** | General purpose, very good on **VARCHAR/CHAR** of mixed lengths (JSON strings, logs, comments). Rarely inflates data. Also supports SUPER |
| **LZO** | Long free-text strings. Supports SUPER |
| **BYTEDICT** | **Low-cardinality strings** (fewer than 256 distinct values per block: country, status) |
| **DELTA / DELTA32K** | Values that climb steadily (sequential IDs, timestamps) |
| **RUNLENGTH** | Long runs of the same value |
| **MOSTLY8/16/32** | A wide integer type holding mostly small values |
| **TEXT255 / TEXT32K** | VARCHAR with recurring words |
| **RAW** | No compression |

- **`ENCODE AUTO` is the default.** Redshift picks encodings and changes them over time.
- **THE trap:** specifying `ENCODE` on **any single column** turns off ENCODE AUTO **for the whole table**. Columns you leave unspecified then get fixed defaults: **sort key columns → RAW**, BOOLEAN/REAL/DOUBLE → RAW, integer/decimal/date/timestamp → AZ64, CHAR/VARCHAR → LZO. You can switch back with `ALTER TABLE … ALTER ENCODE AUTO`.
- **Leave the leading sort key column lightly compressed or RAW** (that's the default Redshift assigns when you specify encodings yourself). **Don't put RUNLENGTH on a sort key.** If the sort column compresses far more than the other columns, one block of it covers many more rows than their blocks do, and range-restricted scans lose efficiency.
- `ANALYZE COMPRESSION table` reports suggested encodings. COPY into an **empty** table can apply compression automatically (`COMPUPDATE`, see [Guide 24](24-Redshift-Loading-Integration-Sharing.md)).

## Distribution styles: which aisle a row lands on

The goal is to **co-locate the rows that get joined** so that no slice has to borrow rows from another, and to **spread rows evenly** so no slice becomes the straggler.

| Style | How rows are placed | Use when |
|---|---|---|
| **AUTO** (**default** when omitted) | Redshift chooses and changes it as the table grows | Most tables. The "least effort" answer |
| **EVEN** | Round-robin across slices | Table doesn't join, or no clear key. Perfectly balanced, never co-located |
| **KEY** | Hash of one column decides the slice. Equal values always land on the same slice | Large tables **joined on that column** (fact ↔ big dimension). Pick a **high-cardinality, evenly spread** join column |
| **ALL** | Full copy of the table on **every node** | **Small, slowly changing** dimensions joined to everything. Costs storage times the node count and slows writes |

**How AUTO evolves:** a small table **starts as ALL**. As it grows, Redshift may switch it to **KEY**, choosing the primary key (or a column of a composite primary key). If no column is a good key, it switches to **EVEN**. The changes happen in the background. You can see them in `SVL_AUTO_WORKER_ACTION`, and recommendations in `SVV_ALTER_TABLE_RECOMMENDATIONS`. `SVV_TABLE_INFO.diststyle` shows values such as `AUTO(ALL)` or `AUTO(KEY(customer_key))`.

**THE trap (skew):** a DISTKEY on a **low-cardinality or lopsided column** (`country`, `status`, a column that's mostly NULL) sends most rows to a few slices. Those slices do most of the work and everyone else waits. Detect it with **`SVV_TABLE_INFO.skew_rows`**: the ratio of rows on the fullest slice to rows on the emptiest. Close to **1.0 is healthy**. Fix it by choosing a high-cardinality key or EVEN.

**THE trap (date as DISTKEY):** don't distribute on the column you **filter** by. If the fact table is KEY-distributed on `sale_date`, "yesterday's sales" live on a single slice, and one worker does the whole query. **Filter columns belong in the sort key. Join columns belong in the dist key.**

### Reading EXPLAIN: redistribution labels

| Label | Meaning | Verdict |
|---|---|---|
| `DS_DIST_NONE` | Joined rows are already on the same slices (both tables KEY-distributed on the join column) | Good |
| `DS_DIST_ALL_NONE` | Inner table is DISTSTYLE ALL, so no movement | Good |
| `DS_DIST_INNER` / `DS_DIST_OUTER` | One side gets redistributed | Costly. Align the dist key |
| `DS_BCAST_INNER` | The **whole inner table is broadcast** to every node | Expensive on big tables |
| `DS_DIST_ALL_INNER` | Inner table sent to a single slice because the *outer* table is ALL | Bad. Runs serially. ALL belongs on the inner (dimension) side |
| `DS_DIST_BOTH` | **Both** tables redistributed | Worst. Neither is distributed on the join key |

Fix recipe: put the **same DISTKEY (the join column) on both large tables** so the join becomes `DS_DIST_NONE`, or make a small inner table **ALL** so it becomes `DS_DIST_ALL_NONE`.

## Sort keys: how the shelves are ordered

| Type | Behaviour | Use when |
|---|---|---|
| **Compound** (**default** type) | Sorted by column 1, then 2 within 1, and so on. **Column order matters**: predicates must use a **prefix** (the leading column) to benefit | Range filters on a leading timestamp, plus joins, GROUP BY, ORDER BY on the prefix. **Recommended when tables get regular INSERT/UPDATE/DELETE** |
| **Interleaved** | Gives **equal weight** to up to **8** columns, so filters on any subset benefit | Rare: large, mostly static tables filtered by several unrelated columns |
| **AUTO** (recommended) | Automatic table optimization picks the key, and it can use a **multidimensional data layout**: rows are grouped by the *repeated predicates* seen in your workload | Default choice. "Let Redshift decide" |

**THE trap (interleaved):** "interleaved is more flexible, so it's better" is wrong. It needs a slow **`VACUUM REINDEX`** to stay effective, loads and merges take longer, and it hurts on tables that are constantly appended to (a timestamp column that only ever increases is the worst case). **Concurrency scaling doesn't support queries on tables with interleaved sort keys.** Resizing DC2→RA3 even converts interleaved keys to compound automatically. Treat interleaved as a legacy niche. *"Most recent data is queried most"* or *"filters on a date range"* → **compound sort key led by the timestamp**.

## Automatic table optimization (ATO) and Advisor

**ATO** watches your queries and, when it predicts a gain, **automatically applies sort keys and distribution keys** to tables defined with `AUTO`, typically within hours, with minimal query impact. `ENCODE AUTO` handles compression the same way. New tables get AUTO when you omit the clauses. Existing tables can opt in with `ALTER TABLE t ALTER DISTSTYLE AUTO;` and `ALTER TABLE t ALTER SORTKEY AUTO;`. A table with explicit keys is left alone.

**Redshift Advisor** (console) analyses your cluster and *recommends* things: dist/sort keys, compression, splitting COPY files, table statistics and so on. **Advisor recommends, ATO applies.** An exam question asking for the fix with the *least operational overhead* for poorly chosen keys usually points to **AUTO/ATO**.

## Data types (and THE constraint trap)

| Type | Know this |
|---|---|
| **VARCHAR(n)** | Length is in **bytes**, not characters (UTF-8 multibyte, up to 4 bytes per character). **Max 65,535 bytes**. `VARCHAR` with no length = 256. `TEXT` becomes VARCHAR(256) |
| **CHAR(n)** | Fixed length, **single-byte characters only**, max **4,096 bytes**, blank-padded |
| SMALLINT / INTEGER / BIGINT / DECIMAL(p,s) / REAL / DOUBLE PRECISION | Use DECIMAL for money. Smaller types compress and scan faster |
| **DATE / TIMESTAMP / TIMESTAMPTZ / TIME / TIMETZ** | TIMESTAMPTZ is stored in UTC |
| **BOOLEAN** | |
| **SUPER** | **Semi-structured** (JSON, arrays, nested objects). Query with **PartiQL** dot/bracket navigation and unnesting. Load with `JSON_PARSE()` or COPY. **Up to 16 MB per value**. **Can't be a DISTKEY or SORTKEY** |
| **VARBYTE** | Binary data |
| **GEOMETRY / GEOGRAPHY** | Spatial |
| **HLLSKETCH** | HyperLogLog sketches for fast approximate distinct counts |
| **IDENTITY(seed, step)** | Auto-generated surrogate keys. Values are unique but **not guaranteed to be consecutive** (parallel loads leave gaps) |

```sql
-- SUPER: land JSON as-is, then navigate with PartiQL
CREATE TABLE raw_events (event_id BIGINT, payload SUPER);
INSERT INTO raw_events
  SELECT 1, JSON_PARSE('{"device":{"os":"ios"},"items":[{"sku":"A1","qty":2}]}');

SELECT e.event_id, e.payload.device.os, i.sku, i.qty
FROM raw_events e, e.payload.items i;          -- unnest the array
```

**THE trap (constraints):** **PRIMARY KEY, UNIQUE and FOREIGN KEY constraints are informational only. Redshift does not enforce them.** Duplicate "primary keys" load without complaint. The optimizer *trusts* them for planning (join elimination, uniqueness inference), so declaring a constraint your data violates can produce **wrong results**, such as a `SELECT DISTINCT` returning duplicates. Only **NOT NULL is enforced.** *"Duplicates appeared even though the table has a primary key"* means dedupe in the pipeline (MERGE / staging-table upsert, see [Guide 24](24-Redshift-Loading-Integration-Sharing.md)). Still **declare** PK/FK when your ETL guarantees them, because the planner benefits.

## Table and view types

| Object | Notes |
|---|---|
| **Permanent table** | Normal table in RMS, included in snapshots (`BACKUP NO` excludes it on DC2 only; RA3 snapshots include it) |
| **Temporary table** | `CREATE TEMP TABLE` (or a `#name`), visible only to your **session**, dropped at session end. Ideal for staging steps inside ELT procedures |
| **External table** | Metadata over files in S3 (Glue Data Catalog), queried through Redshift Spectrum. See [Guide 24](24-Redshift-Loading-Integration-Sharing.md) |
| **Materialized view** | Stored, refreshable query result for repeated heavy aggregations. See [Guide 24](24-Redshift-Loading-Integration-Sharing.md) |
| **Standard view** | Stored query, bound to its underlying objects. You can't drop a referenced table without dropping or cascading the view |
| **Late-binding view** | `CREATE VIEW … AS … WITH NO SCHEMA BINDING`. Not bound to underlying objects, which are checked at query time, so you can drop/recreate tables underneath. **Referenced tables must be schema-qualified.** **Views over external (Spectrum) tables must be late-binding** |

```sql
-- Hot data in Redshift + cold history in S3, one view (external tables need NO SCHEMA BINDING)
CREATE VIEW analytics.sales_all AS
  SELECT * FROM public.sales
  UNION ALL
  SELECT * FROM spectrum.sales_history
WITH NO SCHEMA BINDING;
```

## Designing a star schema for Redshift (skill 2.4.1)

A **star schema** has one central **fact** table (events and measures, billions of rows) surrounded by **dimension** tables (who, what, when, where). The general modelling theory (SCDs, snowflake vs star) lives in [Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md). Here are the Redshift-specific rules:

1. **Distribute the fact table on its most frequently joined, high-cardinality key**, usually the key of the **largest** dimension it joins to.
2. **KEY-distribute that large dimension on the same column**, so the biggest join is `DS_DIST_NONE`.
3. **Small, slowly changing dimensions → DISTSTYLE ALL** (or AUTO, which starts them as ALL).
4. **Sort the fact table by the time column** most queries filter on. Sort dimensions by their key.
5. **Denormalize where it removes joins.** Scans are cheap and joins across slices are not, so flattening a snowflake into wider dimensions often wins.
6. Keep variable, semi-structured attributes in a **SUPER** column rather than dozens of sparse columns.
7. Declare PK/FK (informational) so the planner can optimise, and enforce uniqueness in ETL.

```sql
CREATE TABLE dim_date (            -- ~4,000 rows, almost never changes
  date_key     INTEGER NOT NULL PRIMARY KEY,
  cal_date     DATE    NOT NULL,
  fiscal_qtr   CHAR(6),
  is_holiday   BOOLEAN
) DISTSTYLE ALL
  SORTKEY (date_key);

CREATE TABLE dim_customer (        -- 40 million rows, joined on every revenue query
  customer_key BIGINT      NOT NULL PRIMARY KEY,
  customer_id  VARCHAR(36) NOT NULL,
  segment      VARCHAR(20),
  country      CHAR(2),
  profile      SUPER                 -- loyalty/preferences JSON
) DISTKEY (customer_key)
  SORTKEY (customer_key);

CREATE TABLE fact_sales (          -- 5 billion rows
  sale_id      BIGINT IDENTITY(1,1),
  date_key     INTEGER  NOT NULL REFERENCES dim_date (date_key),
  customer_key BIGINT   NOT NULL REFERENCES dim_customer (customer_key),
  product_key  INTEGER  NOT NULL,
  sold_at      TIMESTAMP NOT NULL,
  quantity     SMALLINT,
  amount       DECIMAL(12,2)
) DISTKEY (customer_key)                    -- co-located with the big dimension
  COMPOUND SORTKEY (sold_at, product_key);  -- "last N days" filters skip blocks
```

Why these choices hold up:

- **fact ↔ dim_customer** share `customer_key` as DISTKEY, so the heavy join needs no redistribution.
- **dim_date** is tiny and static. As ALL it's on every node, so the join is `DS_DIST_ALL_NONE`.
- The fact table is **not** distributed on `date_key`/`sold_at`. A date DISTKEY would pile each day onto one slice. Time goes into the **sort key**, where zone maps turn *"last 7 days"* into a skim over a handful of blocks.
- No `ENCODE` clauses, so the tables stay on **ENCODE AUTO**.
- A mid-size `dim_product` could be ALL if it's small, or AUTO.

## Workload isolation (preview)

When ETL and dashboards fight over one warehouse, the exam's modern answers are:

- **Data sharing**: a producer warehouse (ETL) shares live data with one or more consumer warehouses (BI, data science). Each has **separate compute** reading the **same storage**, with no copies. With multi-warehouse writes, consumers can even write back. See [Guide 24](24-Redshift-Loading-Integration-Sharing.md).
- **Multiple Serverless workgroups** per team or workload, joined together with data sharing.
- **Within one cluster**: WLM queues/priorities, concurrency scaling and short query acceleration. See [Guide 25](25-Redshift-Performance-Operations-Security.md).

## Question patterns

> *"A query joining a 3-billion-row orders fact to a 50-million-row customers dimension is slow. EXPLAIN shows DS_DIST_BOTH on the join. Which change gives the BIGGEST improvement?"* → **Set DISTKEY to customer_id on both tables** (co-locating the join turns DS_DIST_BOTH into DS_DIST_NONE; ALL on a 50M-row dimension would multiply storage and slow loads; a bigger cluster just redistributes faster).

> *"A 2,000-row product-category table that changes monthly is joined to every fact query and shows DS_BCAST_INNER."* → **DISTSTYLE ALL** (or leave it AUTO, which starts small tables as ALL). The plan becomes DS_DIST_ALL_NONE.

> *"Analysts almost always query the last 7 days of a clickstream table, and scans read the entire table."* → **Compound sort key with the event timestamp as the leading column** (zone maps skip old blocks; a DISTKEY on the timestamp would create skew, and interleaved suits constant appends poorly).

> *"After the team set DISTKEY(region) on a large fact table, one slice runs far longer than the others. SVV_TABLE_INFO shows skew_rows = 18."* → **Redistribute on a high-cardinality column, or use EVEN/AUTO.** Region has few values, so rows pile onto a few slices.

> *"A company's Redshift data grows 40% per quarter while query volume stays flat. They run 16 dc2.8xlarge nodes mainly for disk space. MOST cost-effective?"* → **Move to RA3 (or RG) with managed storage via elastic resize** (pay for compute sized to the workload and storage by GB-month; adding DC2 nodes buys unneeded compute, and DC2 is deprecated anyway).

> *"A startup runs a few heavy reports at unpredictable times and wants a warehouse with the LEAST operational overhead, paying nothing while idle."* → **Redshift Serverless** (per-second RPU billing only while queries run, no node sizing). Set **max capacity** and **usage limits** to keep spend predictable.

> *"Finance requires that Serverless compute spending can never exceed a monthly amount; queries may stop when it's reached."* → **Usage limit on RPU-hours (monthly) with the 'turn off user queries' action**, plus max capacity (log/alert actions don't stop anything).

> *"Duplicate order_id values appear in a Redshift table whose DDL declares order_id as PRIMARY KEY."* → **Redshift doesn't enforce PK/UNIQUE constraints. Deduplicate in the load (MERGE or staging + delete/insert).** Only NOT NULL is enforced, and the optimizer trusting a false PK can even give wrong DISTINCT results.

> *"Mobile events arrive as JSON with nested arrays and frequently added attributes. Analysts need SQL access in Redshift without constant DDL changes."* → **Load into a SUPER column (JSON_PARSE / COPY) and query with PartiQL navigation and unnesting** (VARCHAR(65535) blobs need string parsing, and flattening every attribute into columns breaks with every schema change).

> *"A load into VARCHAR(50) fails for customer names in Japanese, although no name exceeds 30 characters."* → **VARCHAR length is in bytes, and multibyte characters take up to 4 bytes. Widen the column** (and CHAR can't hold multibyte characters at all).

> *"A view joining a local sales table with a Spectrum external table fails to create."* → **Create it as a late-binding view: WITH NO SCHEMA BINDING, with schema-qualified names.** Views over external tables must be late-binding.

> *"The data warehouse must keep running through an Availability Zone outage with no data loss and minimal downtime."* → **Redshift Multi-AZ deployment on RA3/RG** (active compute in two AZs, one endpoint, 99.99% SLA). Cross-Region snapshot copy is for **Region-level** DR, and relocation is slower.

> *"Month-end close needs double the compute for three days each month on a provisioned RA3 cluster, with minimal disruption."* → **Scheduled elastic resize** (about 10 minutes, keeps the endpoint; classic resize is slower). Concurrency scaling is the alternative when the problem is *concurrent query queueing* rather than per-query power.

> *"A team with no Redshift tuning expertise wants distribution and sort keys to adapt to the workload with the LEAST effort."* → **DISTSTYLE AUTO + SORTKEY AUTO + ENCODE AUTO (automatic table optimization)**. Advisor only recommends, and ATO applies the changes.

> *"A reporting table is filtered equally by customer_id, product_id or store_id, and the team is considering an interleaved sort key. What's the drawback?"* → **Interleaved keys need costly VACUUM REINDEX, slow loads on growing tables, and block concurrency scaling for those queries.** Prefer SORTKEY AUTO (its multidimensional layout targets repeated predicates) or a compound key on the most common filter.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Leader node | Parses, plans, coordinates, final aggregation. Not billed on multi-node clusters |
| Slice | Parallel unit inside a compute node. Rows are distributed to slices |
| Scale storage independently of compute | RA3/RG with Redshift Managed Storage (SSD cache + S3), or Serverless |
| DC2 / DS2 | DC2 deprecated (April 2025), DS2 gone. Distractors |
| RG instances (2026) | Graviton, ~2.4x RA3, integrated data lake engine, no Spectrum per-TB fee |
| Resize in minutes | Elastic resize (~10 min, 2x–4x growth limits, can't cancel) |
| Resize beyond elastic limits | Classic resize |
| Survive AZ failure | Multi-AZ (RA3/RG only), 99.99% SLA |
| Dev cluster idle nights | Pause/resume (or Serverless) |
| Steady 24/7, cheapest | Provisioned + reserved nodes |
| Spiky / unknown / least ops | Redshift Serverless |
| Namespace vs workgroup | Storage & objects vs compute & network |
| RPU | 16 GB memory. Base default 128, range 4–512 (1024 in some Regions) |
| Serverless billing | Per second, 60-s minimum, only while queries run. Storage separate |
| Cap Serverless spend | Max capacity + usage limits (log / alert / turn off user queries) |
| Price-performance target | AI-driven scaling slider, Balanced by default |
| Zone map | Min/max per 1 MB block, skips blocks when data is sorted |
| Default encoding setting | ENCODE AUTO (specifying any column's ENCODE disables it table-wide) |
| Numbers/dates encoding | AZ64 |
| Mixed/long strings | ZSTD (or LZO) |
| Low-cardinality strings | BYTEDICT |
| Sort key column encoding | RAW / light. Never RUNLENGTH |
| Default DISTSTYLE | AUTO (ALL → KEY → EVEN as the table grows) |
| Big fact ↔ big dimension join | KEY on the join column, both tables |
| Small static dimension | DISTSTYLE ALL |
| No joins / no good key | EVEN |
| Skew check | SVV_TABLE_INFO.skew_rows (≈1.0 healthy) |
| DS_DIST_NONE / DS_DIST_ALL_NONE | Good, co-located |
| DS_BCAST_INNER / DS_DIST_BOTH | Costly redistribution. Fix dist keys |
| "Recent data" / date-range filters | Compound sort key led by the timestamp |
| Interleaved | Up to 8 columns, VACUUM REINDEX, no concurrency scaling. Rarely right |
| Hands-off key tuning | AUTO keys = automatic table optimization. Advisor only recommends |
| JSON / nested / evolving | SUPER + JSON_PARSE + PartiQL (16 MB per value, not a dist/sort key) |
| VARCHAR limit | 65,535 **bytes** |
| PK / FK / UNIQUE | Informational only. NOT NULL is enforced |
| View on external table | Late-binding view: WITH NO SCHEMA BINDING |
| Session-only staging | Temporary table |
| Isolate ETL from BI | Data sharing / separate workgroups (then WLM inside one cluster) |

Physical design decides where the boxes sit. Next, learn how to get them into the building quickly and share them without copying: [Guide 24 — Redshift Loading, Integration & Sharing](24-Redshift-Loading-Integration-Sharing.md).
