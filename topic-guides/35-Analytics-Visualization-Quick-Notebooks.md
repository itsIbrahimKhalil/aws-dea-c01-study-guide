# 35 · Analytics, Visualization, Quick Sight & Notebooks — the tasting room at the end of the kitchen

> **Exam map:** D3 · Task 3.2 · **Skills:** 3.2.1, 3.2.2, 3.2.4, 3.2.5, 3.2.6 · **Weight:** 🔥🔥 Medium · **Read time:** ~16 min

## The idea

A restaurant kitchen spends all day washing, chopping and cooking, and the **tasting room** is where the result finally meets the customer. Some diners want a **plated tasting menu** they can't break (a polished dashboard). Some want to walk up to the **chef's counter** and poke at ingredients themselves (a notebook). Before anything leaves the kitchen, someone **tastes it and sends back what's off** (data verification and cleaning). The tasting room also has a choice of kitchen: one kept **hot all day** (provisioned) or one that **fires up only when an order arrives** (serverless).

On AWS the plated menu is **Amazon Quick Sight**, the BI component of **Amazon Quick**. The service was called **Amazon QuickSight** until Oct 9, 2025, then **Amazon Quick Suite**, now simply "Amazon Quick", and exam items may still say *QuickSight*. The chef's counter is a **notebook**: Athena for Apache Spark, Glue interactive sessions, EMR Studio, SageMaker Unified Studio or SageMaker AI Studio. The tasting step is **verification and cleaning** with Lambda, Athena SQL, Quick Sight data prep, Jupyter/pandas, SageMaker Data Wrangler and AWS Glue DataBrew.

This guide lets you crack: SPICE vs direct query, refresh choices, row- and column-level security, embedding, which notebook fits, how to verify and clean data, the analysis vocabulary (aggregation, rolling average, pivot), and provisioned vs serverless trade-offs for analytics.

## Amazon Quick and Quick Sight at a glance

**Amazon Quick** is a suite of agentic-AI workplace tools; **Quick Sight** is its business-intelligence piece: dashboards, analyses, pixel-perfect reports and natural-language Q&A. The other components each get one line here, because the exam only cares about BI:
- **Quick Index** — a unified index over your connected documents and data
- **Quick Research** — multi-step research agents
- **Quick Flows / Quick Automate** — lightweight and complex workflow automation
- **Chat agents and Spaces** — conversational assistants over your content

> ⚠️ **2026 status:** the rename changes nothing technical for the exam. "QuickSight", "Quick Suite" and "Quick Sight" all mean the same BI service. The standalone **Amazon Q Business** is in maintenance from Jul 30, 2026 (no new customers); Quick is AWS's forward path for that use case.

**Workflow vocabulary:** **data source** (connection) → **dataset** (the prepared table: joins, calculated fields, filters, RLS) → **analysis** (the authoring canvas) → **dashboard** (published, read-only for readers) → optional **topic** (a curated dataset for natural-language questions).

**Data sources:** Athena, Redshift (provisioned/Serverless), **S3 via a manifest file** (JSON listing file URIs/prefixes and format; up to 1,000 files per manifest), RDS/Aurora, OpenSearch Service, Snowflake, Databricks, Teradata, and SaaS sources (Salesforce, Jira, ServiceNow...), plus file uploads. **THE trap:** *"visualize Parquet data in S3"* → **Athena as the source**, since an S3 manifest reads CSV/TSV/JSON-style files, not a partitioned Parquet lake. Query the lake through Athena.

## SPICE vs direct query — the core decision

**SPICE** (Super-fast, Parallel, In-memory Calculation Engine) imports a copy of the data into Quick Sight's own in-memory columnar store. **Direct query** sends SQL to the source every time a visual renders.

| | SPICE | Direct query |
|---|---|---|
| Speed | Fastest; interactive even at scale | As fast as the source |
| Freshness | As fresh as the **last refresh** | **Live** |
| Source load | Queried only at refresh | Every dashboard view hits the source; can add cost (Athena bytes, Redshift concurrency) |
| Cost | SPICE capacity (GB per account/Region; each author includes an allowance, extra capacity bought separately) | Source compute |
| Limits | **Enterprise: up to 2 billion rows or 2 TB per dataset**; Standard: 25 M rows / 25 GB | Visual queries time out after **2 minutes** |
| Pick when | *"many users, fast dashboards, reduce load/cost on the source"* | *"must always show the latest data"*, data too big or too sensitive to copy |

**Refresh options (SPICE):**
- **Scheduled full refresh.** Standard edition is daily; Enterprise allows more frequent (hourly) schedules.
- **Incremental refresh** (Enterprise, SQL sources like Athena, Redshift and RDS). It re-ingests only a **look-back window** on a date column, so it's faster and cheaper and can run as often as every 15 minutes.
- **On-demand or API refresh** (`CreateIngestion`). Trigger it from the pipeline, for example as the last step of a Step Functions workflow or from an EventBridge rule after the Glue job succeeds.

**THE trap:** *"dashboards show yesterday's numbers although the Glue job loaded new data an hour ago"* → the dataset is **SPICE and hasn't refreshed**. Trigger a refresh via the API at the end of the pipeline, or schedule incremental refresh. Switching to direct query also works, but it moves every view's cost onto the source.

## Security, users and sharing

**Row-level security (RLS)** has two flavours:
- **User/group-based RLS:** a **rules (permissions) dataset** with a `UserName` or `GroupName` column plus the filter columns (e.g. `region`) is attached to the dataset. Each reader sees only matching rows. **Users not listed in the rules see no data.**
- **Tag-based RLS:** rules keyed on **session tags**. It's used for **anonymous embedding**, where viewers aren't Quick users (multi-tenant SaaS portals pass `tenant_id` tags when they generate the embed URL).

**Column-level security (CLS)** (Enterprise) restricts sensitive columns (salary, PII) to named users or groups; everyone else can't use them. **THE trap:** building separate datasets per audience, or relying on Lake Formation alone for dashboard viewers. In Quick Sight the viewer-level answer is **RLS/CLS on the dataset**.

**Users and editions:** roles are **Admin, Author, Reader**, plus **"Pro" variants** (Author Pro, Reader Pro, Admin Pro) that unlock generative-AI features. Enterprise edition is required for RLS/CLS, VPC connections, incremental refresh, ML insights, embedding and AD/SAML/IAM Identity Center integration. Readers are cheap per user, and **capacity (session) pricing** covers large or anonymous audiences. Pricing plan names changed with the Quick rename, so expect questions to test features, not prices.

**Embedding:**

| Type | Viewer | API | Typical signal |
|---|---|---|---|
| **Registered-user embedding** | Provisioned Quick users (via SSO) | `GenerateEmbedUrlForRegisteredUser` | *"internal portal, per-user permissions"* |
| **Anonymous embedding** | Anyone, no Quick user | `GenerateEmbedUrlForAnonymousUser` + **session capacity pricing** + **tag-based RLS** | *"external customers of a SaaS app, thousands of viewers"* |

**Private sources:** a **VPC connection** (Enterprise) creates ENIs in your VPC so Quick Sight reaches private RDS, Aurora or Redshift. Security groups must allow the traffic both ways. Public sources need the Quick Sight Regional IP range allowlisted ([Guide 38](38-Networking-for-Data-Pipelines.md)).

**Permissions for Athena, S3 and Lake Formation.** The Quick service role needs **Athena access plus S3 read on the data buckets and read/write on the Athena query-results bucket** (tick them under *Security & permissions*). It also needs **KMS decrypt** if the data is encrypted with a customer key. With **Lake Formation**, grant table/column permissions to the **Quick Sight user or group ARNs** (or the role used). **THE trap:** *"Athena works in the console, but Quick Sight shows access denied"* → the Quick Sight role lacks S3 access to the source or results bucket, or Lake Formation grants.

## Analysis features the exam names

- **ML insights** (Enterprise): **anomaly detection** (Random Cut Forest), **forecasting**, and **auto-narratives** (natural-language summaries in the dashboard). Signal: *"detect outliers in dashboards without data-science skills."*
- **Generative BI:** marketed as **Amazon Q in QuickSight**, now built into Quick Sight. It includes natural-language authoring of visuals and calculations, executive summaries, data stories, and **topics** that let business users ask questions in plain English. Signal: *"business users ask questions of the data in natural language."*
- **Calculated fields and table calculations:** `ifelse`, `coalesce`, `parseDate`, and window functions like `windowAvg`, `runningSum` and `periodOverPeriodDifference` for rolling averages and running totals without changing the source.
- **Pivot table visual:** rows × columns × values with subtotals. It's the no-SQL pivot.
- **Paginated reports:** pixel-perfect, printable, scheduled PDF/CSV reports (an add-on). Signal: *"monthly invoice-style reports emailed on a schedule."*
- **Alerts and email reports** on dashboards (threshold alerts on KPIs).

**DataBrew vs Quick Sight for visualization (skill 3.2.1):** **AWS Glue DataBrew** visualizes data *for preparation*: column profiles, distributions, correlations, missing-value and outlier statistics from **profile jobs** ([Guide 14](14-Glue-DataBrew-Data-Preparation.md)). **Quick Sight** visualizes data *for consumption*: shared, interactive dashboards. *"Understand data quality before cleaning"* → DataBrew profile. *"Share KPIs with executives"* → Quick Sight.

## Verifying and cleaning data (skill 3.2.2)

| Tool | Best for | Example |
|---|---|---|
| **Lambda** | Lightweight **per-file/per-event validation** at ingest | S3 event → Lambda checks header, row count and schema; moves bad files to `quarantine/` and alerts via SNS |
| **Athena SQL** | Set-based checks and **CTAS clean copies** over the lake | Null/duplicate/range counts; `try_cast` to find bad types; CTAS writes a cleaned Parquet table |
| **Quick Sight dataset prep** | Last-mile fixes for BI: rename, change types, calculated fields, filters, joins | `ifelse(isNull(country),'UNKNOWN',country)` |
| **Jupyter + pandas** (SageMaker/EMR/Glue notebooks) | Exploratory profiling and ad hoc fixes by engineers | `df.isna().sum()`, `df.drop_duplicates()` |
| **SageMaker Data Wrangler** (inside **SageMaker Canvas**) | Visual prep of **ML features** with 300+ built-in transforms and data-quality insights | Export the flow to a processing job or pipeline |
| **AWS Glue DataBrew** | **No-code** cleaning and profiling by analysts, with reusable recipes | Recipe: trim, dedupe, fill missing, mask PII |
| **AWS Glue Data Quality** | Rule-based checks inside pipelines (DQDL) | See [Guide 33](33-Data-Quality.md) |

**Athena checks and a clean CTAS copy:**

```sql
-- How dirty is it?
SELECT count(*) AS total_rows,
       count(*) - count(DISTINCT order_id)                     AS duplicate_ids,
       sum(CASE WHEN try_cast(amount AS decimal(10,2)) IS NULL THEN 1 ELSE 0 END) AS bad_amounts
FROM raw.orders;

-- Write a cleaned, typed, partitioned Parquet copy
CREATE TABLE clean.orders
WITH (format = 'PARQUET',
      external_location = 's3://amzn-s3-demo-bucket/clean/orders/',
      partitioned_by = ARRAY['order_date']) AS
SELECT DISTINCT order_id,
       lower(trim(email))                 AS email,
       try_cast(amount AS decimal(10,2))  AS amount,
       coalesce(country, 'UNKNOWN')       AS country,
       order_date                          -- partition column last
FROM raw.orders
WHERE try_cast(amount AS decimal(10,2)) IS NOT NULL;
```

`try_cast` returns NULL instead of failing the query on malformed values, which is the Athena answer to *"a few bad values make the query fail."*

**Cleansing techniques to name-match:**
- **Deduplication:** exact (`DISTINCT`, `ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC) = 1`) or fuzzy
- **Standardization:** trim, case, date formats, units, country codes
- **Missing values:** drop, default, or impute (mean/median/mode)
- **Outliers:** z-score/IQR, then flag, cap or remove
- **Type fixes:** `try_cast`, `parse_datetime`
- **Validation against reference data:** joins to lookup tables
- **Fuzzy matching / record linkage:** the **AWS Glue FindMatches ML transform**, which you train with labeled examples to link records that don't share a clean key (*"J. Smith, 12 Main St"* vs *"John Smith, 12 Main Street"*). **THE trap:** `DISTINCT` or exact-key dedup can't catch typos; *"no common unique identifier"* → FindMatches (or AWS Entity Resolution, a service outside the exam list).

## Notebooks for interactive exploration (skill 3.2.4)

**Athena for Apache Spark** runs PySpark in **notebooks without any cluster**. You create a **Spark-enabled workgroup**, open the **notebook editor** (Jupyter-compatible) and run cells as **calculations**. Sessions start in seconds and you pay **per DPU-hour** while they run. The console notebooks use **PySpark engine version 3**; the newer **Apache Spark 3.5** runtime is used from **SageMaker Unified Studio notebooks** or **Spark Connect** clients. It reads tables from the Glue Data Catalog and supports Iceberg/Hudi/Delta. Details are in [Guide 26](26-Amazon-Athena.md).

```python
# In an Athena Spark notebook, `spark` already exists
df = spark.read.table("sales_db.orders")
df.groupBy("country").agg({"amount": "avg"}).orderBy("avg(amount)", ascending=False).show(10)
```

| Notebook option | Compute | Pick when |
|---|---|---|
| **Athena for Apache Spark** | Serverless Spark, per DPU-hour | *"interactive PySpark exploration, no clusters, pay per use"* |
| **Glue interactive sessions** | Serverless Glue Spark behind a Jupyter kernel (Glue Studio notebooks, local IDE) | *"develop and test Glue ETL scripts interactively"*, then run the same code as a Glue job |
| **EMR Studio** | Managed Jupyter attached to **EMR on EC2, EMR on EKS or EMR Serverless** | Teams already on EMR; custom Spark configs, large clusters |
| **SageMaker Unified Studio notebooks** | JupyterLab in a **project**, with Athena Spark, Glue, EMR and Redshift compute plus a **SQL query editor** | *"one governed workspace for SQL, notebooks, visual ETL and AI"* ([Guide 41](41-SageMaker-Unified-Studio-Catalog-Governance.md)) |
| **SageMaker AI Studio notebooks** | ML-focused JupyterLab/Code Editor spaces | Model development (training itself is out of scope) |
| **Managed Service for Apache Flink Studio** | Zeppelin notebooks on streams | Interactive **streaming** SQL ([Guide 09](09-Managed-Service-for-Apache-Flink.md)) |

**SageMaker Unified Studio for analysts (skill 3.1.6 / 3.2):** a single web workspace, signed in via SSO. It has a **query editor** that runs SQL on **Athena and Redshift** over the lakehouse, **JupyterLab notebooks**, **visual ETL** flows, data discovery via **SageMaker Catalog**, and **Amazon Q** to generate SQL/code and explain results. Its governance constructs (domains, projects, subscriptions) belong to [Guide 41](41-SageMaker-Unified-Studio-Catalog-Governance.md).

> 🆕 **New in exam guide v1.1:** SageMaker Unified Studio is named in skill 3.1.6 (prepare data for transformation). Older prep material lacks it.

## Analysis concepts: aggregation, grouping, rolling averages, pivoting (skill 3.2.6)

| Concept | Meaning | SQL shape | Quick Sight |
|---|---|---|---|
| **Aggregation** | Collapse many rows into a summary value (SUM, COUNT, AVG, MIN, MAX, approx_distinct) | `SELECT sum(amount) FROM sales` | Value wells in any visual |
| **Grouping** | Aggregate **per group**, optionally with subtotals | `GROUP BY region, product` (`ROLLUP`/`CUBE`/`GROUPING SETS` for subtotals) | Group-by field wells |
| **Rolling (moving) average** | Average over a **sliding window** of recent rows/time, which smooths noise | `AVG(x) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` (7-day) | `windowAvg(sum(sales), [date ASC], 6, 0)` |
| **Pivoting** | Turn row values into **columns** (or back: unpivot) | `sum(CASE WHEN month='Jan' THEN amount END) AS jan`… | Pivot table visual |

**THE trap:** a rolling average is a **window function**, not a GROUP BY. Grouping collapses rows, while windows keep every row and add a computed column. A *"running total"* is `SUM() OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)`. Full SQL patterns are in [Guide 34](34-SQL-for-Data-Engineers.md).

## Provisioned vs serverless for analytics (skill 3.2.5)

| Service | Provisioned / always-on | Serverless / on-demand | Choose serverless when… | Choose provisioned when… |
|---|---|---|---|---|
| **Redshift** | RA3 clusters, per node-hour, **Reserved Instances**, pause/resume | **Redshift Serverless**, RPU-hours billed per second, **base 4–512 RPU** | Spiky, intermittent or unpredictable queries; dev/test; no capacity planning | Steady 24x7 heavy load where RIs are cheaper; fine-grained WLM control |
| **Athena** | **Capacity reservations** (dedicated DPUs, per DPU-hour, **4-DPU minimum**) | On-demand **$5 per TB scanned** | Ad hoc, low or irregular usage | Predictable heavy concurrency; guaranteed capacity; cost cap regardless of bytes scanned |
| **EMR** | EMR on EC2 (you size instances; Spot/RIs; long-running) | **EMR Serverless** (per vCPU/GB-second; optional **pre-initialized capacity** for warm starts) | Intermittent jobs, no cluster admin | Constant load, custom AMIs/bootstrap, HBase/long-lived services |
| **OpenSearch** | Managed **domains** (instances, UltraWarm/cold tiers) | **OpenSearch Serverless** collections (OCUs, auto-scale) | Variable or unpredictable search/log load | Steady large clusters; plugins or tuning not in Serverless |
| **Quick Sight** | **SPICE capacity** you buy per account/Region | Direct query (no copy, source pays) | — | Fast dashboards for many viewers |
| **Kinesis / MSK / DynamoDB** | Provisioned shards / brokers / RCU-WCU | On-demand / MSK Serverless / on-demand | Unknown or spiky traffic | Predictable steady throughput (cheaper) |

The general rule: **serverless trades a higher unit price for zero idle cost and zero capacity management**. Provisioned wins on **steady, high, predictable utilization** (especially with reservations) and when you need knobs serverless hides. Signals: *"unpredictable"*, *"intermittent"*, *"least operational overhead"* → serverless. *"Steady 24/7"*, *"predictable"*, *"reserved pricing"* → provisioned. See also [Guide 02](02-Data-Engineering-Fundamentals.md) and [Guide 44](44-Cost-Optimization.md).

## Tool choice — which screen?

| Need | Tool | Why not the others |
|---|---|---|
| Business dashboards, KPIs, many readers, embedding, RLS | **Quick Sight** | Grafana/CloudWatch aren't BI tools |
| Operational time-series across Prometheus, CloudWatch, OpenSearch, Athena, with SSO | **Amazon Managed Grafana** ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)) | Quick Sight isn't built for ops metrics |
| Log search, text exploration, security analytics on indices | **OpenSearch Dashboards** ([Guide 29](29-OpenSearch-Service.md)) | Needs data in OpenSearch |
| AWS resource metrics and alarm status | **CloudWatch dashboards** | Minimal setup, AWS-native |
| Ad hoc code-driven exploration | **Notebooks** (Athena Spark, SageMaker Unified Studio, EMR Studio) | Not for sharing polished views |
| Profile data before cleaning | **DataBrew profile job** | Quick Sight shows data, it doesn't profile it |

## Question patterns

> *"Hundreds of sales managers open the same dashboard every morning; it's slow and the Athena bill keeps rising. Data changes once per night."* → **Import into SPICE and refresh after the nightly load** (direct query re-scans S3 for every viewer).

> *"A dashboard must reflect Redshift data changed within the last minute."* → **Direct query to Redshift** (SPICE is only as fresh as its last refresh).

> *"A 1.5-billion-row SPICE dataset takes too long to refresh fully each hour; only the last day of records change."* → **Incremental refresh with a one-day look-back window** (Enterprise).

> *"Regional managers may see only their own region's rows in a shared dashboard."* → **User/group-based RLS with a rules dataset** (not one dashboard per region).

> *"A SaaS company embeds dashboards for thousands of external customers who have no Quick Sight users; each tenant sees only its own data."* → **Anonymous embedding with session capacity pricing + tag-based RLS**.

> *"HR analysts can view salary; everyone else using the same dataset must not."* → **Column-level security** on the dataset.

> *"Quick Sight must query an RDS for PostgreSQL instance in private subnets."* → **Quick Sight VPC connection** (Enterprise) with security groups allowing the traffic.

> *"Athena queries work in the console, but the Quick Sight dataset fails with access denied to the S3 location."* → **Grant the Quick Sight role access to the data and Athena results buckets (and Lake Formation/KMS as applicable).**

> *"Executives want to type questions like 'top 5 products by margin last quarter' and get visuals."* → **Quick Sight generative BI / topics (Amazon Q in Quick Sight)**.

> *"Detect unusual daily revenue drops in dashboards with no ML expertise."* → **Quick Sight ML insights anomaly detection** (not a SageMaker model).

> *"An analyst wants to explore Parquet data in S3 with PySpark interactively, without provisioning or managing clusters, paying only while code runs."* → **Athena for Apache Spark notebook** (EMR Studio implies a cluster or EMR app; Glue interactive sessions target ETL script development).

> *"An engineer needs to iteratively develop a Glue ETL script in Jupyter and then schedule it."* → **Glue interactive sessions** (same runtime as the job).

> *"Customer records from two acquisitions must be merged; names and addresses contain typos and there's no shared customer ID."* → **Glue FindMatches ML transform** (exact dedup and DISTINCT miss near-duplicates).

> *"A few malformed amount values make an Athena aggregation query fail; produce a clean Parquet table for BI."* → **CTAS with `try_cast` + filters writing Parquet**.

> *"Show a 7-day moving average of daily orders next to each day's value."* → **Window function `AVG() OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`** (GROUP BY would collapse days).

> *"Analytics queries run a few hours per week at unpredictable times; minimize cost and administration."* → **Redshift Serverless (or Athena on-demand)**, not an always-on provisioned cluster.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| QuickSight / Quick Suite / Quick Sight | Same BI service (Amazon Quick since Oct 2025) |
| Fast dashboards, many viewers, less source load | SPICE |
| Always-latest data | Direct query |
| SPICE size limit | Enterprise 2 B rows / 2 TB per dataset |
| Refresh only recent rows | Incremental refresh (look-back window) |
| Dashboard stale after pipeline load | Refresh SPICE via API at pipeline end |
| Lake data in Parquet → BI | Athena data source |
| S3 files directly | Manifest file (≤1,000 files) |
| Users see only their rows | RLS rules dataset (UserName/GroupName) |
| Anonymous embedded tenants | Tag-based RLS + anonymous embedding |
| Hide sensitive columns | Column-level security |
| Private RDS/Redshift source | VPC connection (Enterprise) |
| Access denied from Quick Sight | Quick role S3/Athena results/LF/KMS permissions |
| Natural-language questions | Generative BI / topics (Amazon Q in Quick Sight) |
| Outliers, forecasts, narratives | ML insights |
| Printable scheduled reports | Paginated reports |
| Visual profiling before cleaning | DataBrew profile job |
| No-code cleaning recipes | DataBrew |
| ML feature prep, visual | SageMaker Data Wrangler (in Canvas) |
| Validate each file at ingest | S3 event → Lambda |
| Clean copy of lake data with SQL | Athena CTAS + try_cast |
| Fuzzy dedup, no shared key | Glue FindMatches |
| Serverless PySpark notebook | Athena for Apache Spark |
| Develop Glue scripts interactively | Glue interactive sessions |
| Notebooks on EMR | EMR Studio |
| One governed workspace: SQL + notebooks + ETL + Q | SageMaker Unified Studio |
| Streaming SQL notebook | Managed Flink Studio |
| Moving average | Window function (ROWS BETWEEN n PRECEDING) |
| Rows → columns | Pivot (CASE aggregates / pivot visual) |
| Spiky, unpredictable analytics | Serverless (Redshift Serverless, Athena, EMR Serverless) |
| Steady 24x7 heavy load | Provisioned + reservations |
| Ops metrics dashboards | Managed Grafana / CloudWatch dashboards |
| Log exploration UI | OpenSearch Dashboards |

With the data verified and on screen, the remaining exam skill is choosing the right service for every stage of the pipeline. That's the capstone: [Guide 45 — Service Selection Decision Guide](45-Service-Selection-Decision-Guide.md).
