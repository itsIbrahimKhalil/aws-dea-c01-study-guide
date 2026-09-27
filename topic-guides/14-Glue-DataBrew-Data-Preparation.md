# 14 · Glue DataBrew & Data Preparation — the prep station before the cooking starts

> **Exam map:** D3 · Task 3.1, 3.2, 3.4 — D4 · Task 4.3 · **Skills:** 3.1.6, 3.2.1, 3.2.2, 3.4.2, 3.4.3, 4.3.1 · **Weight:** 🔥🔥 Medium · **Read time:** ~16 min

## The idea

A restaurant has a **prep station** that isn't the main kitchen line. Before anything is cooked, someone washes the vegetables, throws out bruised ones, trims everything to the same size and weighs the portions. That person doesn't need to be a chef. They need a good cutting board, a checklist, and a way to write down each step so the night shift does it the same way.

**AWS Glue DataBrew** is that prep station for data. It's a **visual, no-code** tool where an analyst opens a sample of a dataset in a spreadsheet-like grid, sees data-quality problems highlighted, and fixes them by clicking through **250+ built-in transforms** (remove duplicates, fix date formats, fill missing values, mask emails). Every click is saved as a step in a **recipe**, the written-down procedure. A **job** then replays the recipe on the full dataset on a schedule, with no code and no servers. DataBrew can also **profile** a dataset (a health report on every column) and **check it against data quality rules**.

The exam uses DataBrew as the answer when the question stresses *"no code"*, *"data analysts"*, *"visually"*, *"profile the data"*, *"identify PII"* or *"reusable cleaning steps"*. The competing tools are Glue Studio visual ETL, Glue Data Quality, SageMaker Data Wrangler, SageMaker Unified Studio and plain notebooks, and the exam checks that you can tell them apart. This guide gives you the DataBrew vocabulary, the profiling and PII features, and a decision table for the look-alikes.

## DataBrew building blocks

```
Dataset ──► Project (interactive session on a SAMPLE) ──► Recipe (ordered, versioned steps)
   │                                                           │
   ├──► Profile job (stats, quality, PII)                      └──► Recipe job (FULL data → outputs)
   └──► Ruleset (data quality rules, validated by profile jobs)       ▲ schedules (cron)
```

| Object | What it is | Remember |
|---|---|---|
| **Dataset** | A pointer to data, plus how to read it | Sources: **file upload, Amazon S3, Glue Data Catalog tables** (including Lake Formation-governed ones), **JDBC databases through Glue connections** (e.g., Redshift, RDS/Aurora engines), **Snowflake**, **AWS Data Exchange** and **Amazon AppFlow** (SaaS data). Formats include CSV/TSV, JSON/JSON Lines, Parquet, ORC, Avro, Excel. **Dynamic datasets** use parameters in the S3 path (e.g., a date) or "latest N files" to pick up new files automatically |
| **Project** | An interactive workspace: the grid, column stats, and a recipe you're building | Works on a **sample** (first N, last N, or random N rows; the default is a few hundred rows and the size is capped), never the full dataset |
| **Recipe** | An ordered list of transform steps | **Versioned** (working copy, then published versions). Reusable on other datasets with the same shape. Downloadable as JSON/YAML |
| **Recipe job** | Runs a published recipe on the **full** dataset | Outputs: **S3** (CSV, Parquet, JSON, Avro, ORC, XML, Tableau Hyper and more, with compression and **output partitioning by column**), **Glue Data Catalog tables**, **Redshift**, **Snowflake**, JDBC targets |
| **Profile job** | Computes a data profile of a dataset (a sample or the full data) | Also evaluates rulesets and detects PII |
| **Ruleset** | Data quality rules attached to a dataset | Validated when a profile job runs; results go to a validation report |
| **Schedule** | Cron-based schedule for recipe and profile jobs | Or trigger through Step Functions / EventBridge / MWAA |
| **Data lineage** | Visual graph: source → dataset → project/recipe → job → output | Quick "where did this file come from" view |

Jobs run on DataBrew-managed **nodes** (default **5**). You set the maximum capacity, retries and timeout. It's still serverless: nothing to patch or scale.

**Pricing (high level):** interactive project work is billed **per 30-minute session** ($1.00 in us-east-1), and jobs are billed **per node-hour** ($0.48; a 10-minute job on 5 nodes costs about $0.40). No charge while you aren't running sessions or jobs.

**THE trap:** *"The recipe worked in the project but the job output is wrong / has new errors."* The project only saw a **sample**. The full data contains values the sample didn't (a new date format, a rare null pattern). Profile the **full dataset** or use a **random** sample while building, and validate with rules.

## Transforms — what's in the toolbox

The 250+ transforms fall into families you should recognize:

| Family | Examples | Exam use |
|---|---|---|
| **Clean / standardize** | Trim whitespace, change case, remove special characters, replace values, standardize date/time formats, change data type | *"inconsistent formats"* |
| **Missing values** | Remove rows with missing values, fill with mean/median/mode/most frequent/custom value, flag missing | *"handle nulls"* |
| **Duplicates** | Remove duplicate rows or duplicate values in a column | *"deduplicate"* |
| **Outliers** | Detect by z-score / interquartile range, then remove, flag, clip or replace | *"remove anomalous values"* |
| **Structure** | Split column by delimiter, merge columns, nest/unnest JSON arrays and structs, pivot/unpivot, transpose, group-by aggregates, **join** and **union** with another dataset | *"flatten nested JSON"*, *"combine two datasets"* |
| **Feature/format** | One-hot encoding, binning, tokenization, scaling/normalization, date extraction (year, weekday) | ML-oriented prep |
| **Conditional / formula** | IF-style expressions, math and text functions | Derived columns |
| **PII** | Mask, hash, encrypt, substitute, shuffle (next section) | Skill 4.3.1 |

## Profiling and data consistency (skills 3.4.3, 3.2.1)

A **profile job** is the fastest route to *"investigate data consistency"*. It produces, per dataset and per column:
- **Dataset level:** row and column count, **duplicate rows**, missing-cell percentage, a **correlation matrix** between numeric columns, and a data-quality overview.
- **Column level:** data type, **missing/valid counts**, **distinct and unique values**, min/max/mean/median/mode, standard deviation, quartiles, **outliers**, skewness, most and least frequent values, value-length statistics.
- **Visuals** in the console: **histograms/value distributions**, **box plots** for spread and outliers, top-N value bars, and the correlation heat map.
- **PII detection:** flags columns that look like names, emails, phone numbers, credit card numbers, government IDs and similar entity types, so you know what to mask.
- Output is a JSON profile in S3 plus the visual dashboard. Configure it to profile a **sample** (cheaper) or the **full dataset** (complete).

**Visualization boundary (3.2.1):** DataBrew visuals are **diagnostic**. They show what the data *looks like* so you can clean it. Business dashboards, KPIs, drill-downs and sharing with executives belong in **Amazon Quick** (Quick Sight, formerly Amazon QuickSight). See [Guide 35](35-Analytics-Visualization-Quick-Notebooks.md). *"Understand distributions and quality issues before cleaning"* → DataBrew profile. *"Interactive dashboard for business users"* → Quick Sight.

## Data quality rules in DataBrew (skill 3.4.2)

A **ruleset** is a named list of rules attached to a dataset. A **profile job** evaluates it.
- Rules are **checks with thresholds**: *"`customer_id` has no missing values"*, *"duplicate rows = 0"*, *"`order_total` ≥ 0 for 100% of rows"*, *"`country` values in {US, CA, MX}"*, *"row count > 1000"*, value-length or regex checks, and **cross-column** comparisons. Several conditions can combine into one rule, and a threshold can be absolute or a percentage of rows.
- DataBrew **suggests** rules from the profile's statistics.
- Results: every rule gets **passed/failed**, written to a **validation report** in S3 and shown in the console. Outcomes can drive **EventBridge** rules, e.g., to send an SNS alert or stop a Step Functions pipeline when a rule fails.

DataBrew rules vs **AWS Glue Data Quality**: DataBrew rulesets fit analyst-owned, no-code checks on DataBrew datasets. **Glue Data Quality** uses **DQDL** (Data Quality Definition Language), runs **inside Glue ETL jobs** (the Evaluate Data Quality transform, quarantine bad rows mid-pipeline) and on **Data Catalog tables**, gives ML-based **rule recommendations** and **anomaly detection**, and publishes CloudWatch metrics and EventBridge events. For *"check data quality while the ETL job is processing"* (3.4.1), the answer is **Glue Data Quality**. Deep dive: [Guide 33](33-Data-Quality.md).

## PII handling transforms (skill 4.3.1)

DataBrew has a dedicated **PII** transform group, and profiling tells you which columns need it:

| Transform | Effect | Reversible? | Pick when |
|---|---|---|---|
| **MASK_CUSTOM / MASK_DELIMITER / MASK_RANGE / MASK_DATE** | Replace characters with a mask symbol: all, a range, between delimiters, or date parts (e.g., keep the last 4 digits of a card) | No | Show partial values to support staff; *"display only the last four digits"* |
| **CRYPTOGRAPHIC_HASH** | Keyed hash (HMAC) using a **secret stored in AWS Secrets Manager** | No (one-way) | **Pseudonymize** while keeping **joinability**: same input → same hash |
| **DETERMINISTIC_ENCRYPT / DETERMINISTIC_DECRYPT** | Encryption where the same plaintext always gives the same ciphertext (key held in Secrets Manager) | **Yes** | Need to **join/group** on encrypted values **and** recover originals later |
| **ENCRYPT / DECRYPT** | **Probabilistic** encryption with an **AWS KMS key**: same plaintext → different ciphertext each time | **Yes** (with KMS permissions) | Strongest confidentiality; no joins or grouping on the column |
| **REPLACE_WITH_RANDOM_BETWEEN / REPLACE_WITH_RANDOM_DATE_BETWEEN** | Substitute realistic random numbers or dates in a range | No | Test/dev data that must *look* real |
| **SHUFFLE_ROWS** | Shuffle a column's values across rows (optionally within groups) | No | Keep the distribution, break the link to the individual |
| **DELETE** column / replace with null | Remove the data entirely | No | Field not needed downstream (data minimization) |

**THE trap:** *"Analysts must join two datasets on email address, but must never see real emails."* Masking (`MASK_CUSTOM`) destroys joinability and probabilistic `ENCRYPT` gives a different value every time. The answer is a **deterministic** technique: **CRYPTOGRAPHIC_HASH** (one-way) or **DETERMINISTIC_ENCRYPT** (when an authorized team must be able to reverse it).

**THE trap:** DataBrew is **not** the discovery service for a whole data lake. *"Find PII across thousands of S3 buckets continuously"* → **Amazon Macie**. Inside Glue ETL pipelines use the **Detect Sensitive Data** transform, and in Redshift use **dynamic data masking**. Full privacy toolkit: [Guide 42](42-Privacy-PII-Masking-Sovereignty.md).

## Look-alike tools — which prep tool when?

| Tool | Who / how | Scale & output | Pick when the question says… |
|---|---|---|---|
| **Glue DataBrew** | Analysts, **no code**, visual grid | Serverless jobs on full data → S3/Catalog/Redshift/Snowflake | *"no-code"*, *"analysts"*, *"profile"*, *"250+ transforms"*, *"reusable recipe"*, *"mask PII visually"* |
| **Glue Studio visual ETL** ([Guide 12](12-AWS-Glue-ETL.md)) | Data engineers, low code, generates Spark | Production pipelines, bookmarks, catalog updates | *"visual ETL pipeline"*, *"incremental loads"*, *"join sources into a lake"* |
| **Glue Data Quality** ([Guide 33](33-Data-Quality.md)) | Rules in **DQDL** | In ETL jobs and on catalog tables; anomaly detection | *"quality checks during ETL"*, *"stop pipeline on bad data"*, *"rule recommendations"* |
| **SageMaker Data Wrangler** (inside **SageMaker Canvas**) | Data scientists, visual + optional code | ML feature prep; exports to pipelines/processing | *"prepare features for ML"*, *"bias/target leakage insights"*, *"export to SageMaker Pipelines"* |
| **SageMaker Unified Studio** ([Guide 41](41-SageMaker-Unified-Studio-Catalog-Governance.md)) | One IDE for SQL, notebooks, visual ETL (Glue-powered), with Amazon Q | Project-based, governed through SageMaker Catalog | *"single environment for data prep, analytics and ML with governed access"* |
| **Jupyter notebooks** (Glue interactive sessions, SageMaker AI, Athena Spark) ([Guide 35](35-Analytics-Visualization-Quick-Notebooks.md)) | Engineers/scientists, **code** (pandas/PySpark) | As big as the backing engine | *"custom logic in Python"*, *"exploratory analysis with code"* |
| **Athena SQL / Lambda** | SQL or code | Ad hoc checks, small event-driven cleaning | *"verify data with SQL"*, *"clean each file on arrival"* |

🆕 **New in exam guide v1.1:** skill 3.1.6 names **SageMaker Unified Studio** next to DataBrew for "prepare data for transformation". Expect options where Unified Studio is the answer because the scenario stresses one governed workspace across teams, and DataBrew the answer because it stresses no-code cleaning by analysts.

## SageMaker Data Wrangler essentials (skill 3.2.2)

- **Where it lives now:** Data Wrangler's data-prep experience is part of **Amazon SageMaker Canvas** (the older SageMaker Studio Classic version is legacy). You build a **data flow**: import (S3, Athena, Redshift, Snowflake, EMR, many SaaS sources) → transform → analyze → export.
- **Transforms:** 300+ built-in ML-oriented transforms (encode categorical, handle missing, handle outliers, balance data, featurize text/dates) plus custom PySpark/pandas/SQL snippets. You can also describe a transform in natural language.
- **Data Quality and Insights report:** summary stats, missing values, duplicates, anomalous samples, **target leakage** and quick-model feature importance. On the full dataset it runs as a **SageMaker Processing job**.
- **Scaling and export:** large datasets are processed with **EMR Serverless** or a configured **SageMaker Processing job**. Export the flow to **S3**, **SageMaker Pipelines** (generates a pipeline notebook), **Feature Store**, or Python code. It can also be scheduled.
- Exam framing: Data Wrangler = *ML-feature* preparation by data scientists. DataBrew = *general-purpose* cleaning and profiling by analysts. Model training itself is out of scope.

## Cleansing techniques — the catalog and the simplest tool for each

| Problem | Technique | Simplest tool |
|---|---|---|
| Exact duplicate rows | Remove duplicates on all or key columns | DataBrew (no code) · SQL `ROW_NUMBER()` in Athena/Redshift ([Guide 34](34-SQL-for-Data-Engineers.md)) · Spark `dropDuplicates` |
| Near-duplicates (typos, no shared key) | Fuzzy matching / record linkage | **Glue FindMatches** ML transform ([Guide 12](12-AWS-Glue-ETL.md)) |
| Inconsistent formats (dates, phone numbers, case) | Standardize/parse, change case, regex replace | DataBrew · Spark SQL functions |
| Missing values | Drop, fill (mean/median/mode/constant), flag, or leave with a quality rule | DataBrew · Glue `FillMissingValues` · pandas `fillna` |
| Outliers | z-score / IQR detection, then remove, clip or flag | DataBrew outlier transforms · Data Wrangler |
| Wrong or mixed types | Cast, resolve choice types | DataBrew change type · Glue **ResolveChoice** |
| Extra whitespace, special characters | Trim, remove characters | DataBrew · SQL `TRIM` / `REGEXP_REPLACE` |
| Values on different scales | Normalization (min-max) / standardization (z-score) | DataBrew scaling · Data Wrangler |
| Nested/semi-structured data | Unnest/flatten, split columns | DataBrew unnest · Glue Relationalize · Redshift SUPER/PartiQL |
| Invalid domain values | Validate against an allowed list; quarantine failures | DataBrew rules · **Glue Data Quality** in the job |
| Sensitive fields | Mask, hash, encrypt, drop | DataBrew PII transforms · Glue Detect Sensitive Data |
| Skewed samples hide problems | Random or stratified sampling, full-data profile ([Guide 33](33-Data-Quality.md)) | DataBrew random sample / full profile |

## Question patterns

> *"Business analysts with no coding experience must clean a weekly CSV extract (fix date formats, remove duplicates, fill missing regions) and have the same steps applied automatically every week."* → **DataBrew project to build a recipe, publish it, run a scheduled recipe job** (no-code + repeatable; Glue Studio targets engineers building pipelines)

> *"Before building a pipeline, a data engineer must quickly understand a new dataset's missing values, distributions, outliers and correlations with the LEAST effort."* → **DataBrew profile job** (automatic column statistics + visuals; writing Athena queries per column is more effort)

> *"A company must identify which columns in a new dataset contain PII before sharing it with a partner, using a no-code tool."* → **DataBrew profile job with PII detection, then PII transforms in a recipe** (Macie is for discovering sensitive data across S3 at scale, not in-flight prep)

> *"Analysts need a join key derived from customer email across two datasets, but the real email must never be recoverable."* → **CRYPTOGRAPHIC_HASH with a secret in Secrets Manager** (deterministic and one-way; masking breaks joins, probabilistic ENCRYPT gives a different value each time)

> *"A security team must be able to recover original SSNs later, while analysts can still group records by the protected SSN."* → **DETERMINISTIC_ENCRYPT (with DETERMINISTIC_DECRYPT for the authorized team)** (reversible + consistent ciphertext)

> *"Customer support dashboards must show only the last four digits of card numbers."* → **MASK_RANGE / MASK_CUSTOM** (partial masking; hashing hides the digits entirely)

> *"An analyst team must define rules (no nulls in `customer_id`, `amount` ≥ 0, row count within expected range) on a dataset and get a pass/fail report, without code."* → **DataBrew ruleset validated by a profile job** (skill 3.4.2; DQDL inside a Glue job is the engineer-owned alternative)

> *"Records failing quality checks must be routed to a quarantine location during the nightly Glue ETL job, and the pipeline must stop if more than 5% fail."* → **Glue Data Quality (Evaluate Data Quality transform with DQDL) in the job** (in-pipeline enforcement; DataBrew rules run as separate profile jobs)

> *"A recipe built in a DataBrew project works, but the scheduled job produces unexpected nulls in a date column."* → **The project sample missed formats present in the full data: profile the full dataset / use a random sample, and extend the recipe** (projects work on samples)

> *"Data scientists must prepare features for a SageMaker model, check for target leakage, and export the prep steps into SageMaker Pipelines."* → **SageMaker Data Wrangler (in SageMaker Canvas)** (ML-specific insights and Pipelines export; DataBrew has no target-leakage analysis)

> *"Executives need an interactive dashboard of weekly sales by region."* → **Amazon Quick (Quick Sight)** (DataBrew visuals are for profiling, not business dashboards)

> *"Data engineers, analysts and data scientists must prepare data with SQL, notebooks and visual ETL in one governed environment with project-based access to cataloged data."* → **SageMaker Unified Studio** (unified workspace; DataBrew is a single-purpose prep tool)

> *"New daily files arrive as `s3://…/sales/2026-09-26.csv`; the DataBrew job should automatically process the latest file."* → **DataBrew dynamic dataset with a date parameter / latest-file selection in the S3 path** (no manual dataset edits)

> *"Customer records from two systems contain the same people with slightly different spellings and no shared ID."* → **Glue FindMatches ML transform** (fuzzy matching; DataBrew's remove-duplicates handles exact matches only)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| No-code cleaning by analysts | Glue DataBrew |
| Built-in transforms | 250+ |
| Saved, repeatable cleaning steps | Recipe (versioned, publishable) |
| Interactive workspace | Project, which works on a **sample** |
| Apply recipe to full data on schedule | Recipe job + schedule |
| Column stats, distributions, outliers, correlations | Profile job |
| Find PII columns (no code) | DataBrew profile PII detection |
| PII across all S3 buckets | Amazon Macie |
| No-code quality rules + pass/fail report | DataBrew ruleset (validated by profile job) |
| Quality checks inside the ETL job | Glue Data Quality (DQDL) |
| Partial display (last 4 digits) | MASK_RANGE / MASK_CUSTOM |
| Irreversible, joinable pseudonym | CRYPTOGRAPHIC_HASH (secret in Secrets Manager) |
| Reversible + joinable | DETERMINISTIC_ENCRYPT |
| Reversible, strongest, no joins | ENCRYPT (probabilistic, KMS) |
| Realistic fake values | REPLACE_WITH_RANDOM_BETWEEN / _DATE_BETWEEN |
| Keep distribution, break linkage | SHUFFLE_ROWS |
| Don't need the field | DELETE column |
| Pick up newest file automatically | Dynamic dataset (path parameters) |
| DataBrew outputs | S3 (CSV/Parquet/JSON/Avro/ORC…), Catalog, Redshift, Snowflake, JDBC |
| DataBrew pricing | Per 30-min session ($1) + per node-hour ($0.48) |
| Where data came from | DataBrew lineage view |
| ML feature prep + target leakage | SageMaker Data Wrangler (in Canvas) |
| Data Wrangler full-data report | Data Quality and Insights report (processing job) |
| Export prep to ML pipeline | Data Wrangler → SageMaker Pipelines |
| One governed IDE: SQL + notebooks + visual ETL | SageMaker Unified Studio |
| Custom Python/Spark prep | Jupyter / Glue interactive sessions |
| Fuzzy dedup, no common key | Glue FindMatches |
| Business dashboards | Amazon Quick (Quick Sight) |

Clean data still has to be orchestrated, loaded and queried at scale. Next up is the heavy processing engine behind many of these jobs: [Guide 15 — Amazon EMR](15-Amazon-EMR.md).
