# 33 · Data Quality — the quality-control station on the factory line

> **Exam map:** D3 · Task 3.4 (with touches of 1.2, 3.1) · **Skills:** 3.4.1, 3.4.2, 3.4.3, 3.4.4, 3.4.5 · **Weight:** 🔥🔥🔥 High · **Read time:** ~20 min

## The idea

Imagine a **factory assembly line** that bottles juice. Nobody lets bottles walk straight from the filling machine onto delivery trucks. There's a **quality-control station**: an inspector checks every bottle for a cap (is anything missing?), the fill level (is the value in range?), the label (is it the right format?), and the batch number (have we seen this one twice?). Bad bottles go into a **reject bin** instead of the truck, and if too many are rejected, someone hits the **big red stop button** and halts the line. Once a day, a supervisor pulls a **random sample** of bottles to lab-test — and knows that testing only the first ten bottles of the morning would give a biased answer.

Data pipelines need the same station. The checks are **data quality rules**; the reject bin is a **quarantine prefix** or dead-letter queue; the red button is a **circuit breaker** that fails the job or stops the Step Functions workflow before bad data reaches the curated zone; the lab samples are **sampling techniques**; and when one filling nozzle does ten times the work of the others, that's **data skew**.

On AWS the main inspector is **AWS Glue Data Quality** (rules written in **DQDL**, the Data Quality Definition Language), with **AWS Glue DataBrew** for visual profiling and rules, plus in-pipeline checks in Firehose, DMS, Athena, Redshift, Step Functions, and Lambda. This guide lets you crack "check for empty fields while processing", "route bad records to quarantine", "detect unexpected drops in volume", "sample fairly", and "fix skew" questions.

## Quality dimensions — what the inspector measures

| Dimension | Question it answers | Example check |
|---|---|---|
| **Completeness** | Are required values present? | `order_id` never null; ≥ 95% of emails filled |
| **Accuracy** | Does the value match reality? | Totals reconcile with the source system |
| **Consistency** | Do values agree across fields/systems? | `ship_date ≥ order_date`; same customer count in source and target |
| **Validity** | Does the value conform to type/format/range/set? | `status` in allowed list; ZIP is 5 digits |
| **Uniqueness** | No unwanted duplicates? | `order_id` unique |
| **Timeliness / freshness** | Is the data recent enough? | Newest `event_ts` within 24 hours |
| **Integrity** | Do references resolve? | Every `order.customer_id` exists in `customers` |

The exam's phrase *"validation: completeness, consistency, accuracy, integrity"* maps straight onto this table.

## AWS Glue Data Quality

Built on the open-source **Deequ** library (from Amazon, runs on Spark), serverless, and pay-per-use. Rules are written in **DQDL**. A ruleset is a capitalized `Rules` list:

```
Rules = [
    IsComplete "order_id",
    IsUnique "order_id",
    IsPrimaryKey "customer_id",
    Completeness "email" > 0.95,
    ColumnValues "status" in ["NEW", "SHIPPED", "CANCELLED"],
    ColumnValues "amount" between 0 and 100000,
    ColumnValues "zip" matches "[0-9]{5}" with threshold > 0.99,
    ColumnLength "country_code" = 2,
    RowCount between 1000 and 5000000,
    DataFreshness "event_ts" <= 24 hours,
    ReferentialIntegrity "customer_id" "reference.customer_id" = 1.0,
    CustomSql "select count(*) from primary where ship_date < order_date" = 0,
    (IsComplete "discount") or (ColumnValues "discount" = 0),
    RowCount > avg(last(5)) * 0.8
]
Analyzers = [
    DistinctValuesCount "product_id",
    ColumnLength "customer_name"
]
```

What to know about the language:
- **Boolean rule types** (`IsComplete`, `IsUnique`, `IsPrimaryKey`) need no expression; **metric rule types** (`Completeness`, `Uniqueness`, `Mean`, `Sum`, `StandardDeviation`, `RowCount`, `DistinctValuesCount`, `ColumnLength`, `ColumnCorrelation`, `Entropy`) take an expression: `=`, `>`, `between x and y`, `in [...]`, `matches "regex"`, and `with threshold > 0.9` (fraction of rows that must pass).
- `where "<SparkSQL predicate>"` makes a rule conditional; `and` / `or` build **composite rules** (each side in parentheses).
- **Cross-dataset rules**: `ReferentialIntegrity`, `DatasetMatch`, `RowCountMatch`, `AggregateMatch`, `SchemaMatch` compare the primary dataset with a reference dataset (e.g., source vs. target after a migration).
- `CustomSql` runs SQL against the dataset aliased as `primary`.
- **Dynamic rules** compare to history: `RowCount > avg(last(5)) * 0.8` fails when volume drops more than 20% below the recent average. **Analyzers** only gather statistics (no pass/fail) and feed **ML-based anomaly detection**; `DetectAnomalies "RowCount"` turns an anomaly into a rule outcome. Dynamic rules, analyzers, and anomaly detection are **ETL-only** features.
- File-level rules (`FileFreshness`, `FileSize`, `FileUniqueness`, `FileMatch`) check S3 files before reading them.

### Two ways to run it

| | **Data Catalog mode (at rest)** | **ETL mode (in flight)** |
|---|---|---|
| Where | On a Glue Data Catalog table | Inside a Glue ETL job (Glue Studio **Evaluate Data Quality** transform or `EvaluateDataQuality` in code) |
| Start | **Rule recommendations**: Glue profiles the table and suggests a starter ruleset | Author rules (or paste recommended ones) |
| When | On demand or **scheduled** runs | Every job run, **before data lands** |
| Actions | Results, scores, CloudWatch metrics, EventBridge events | Same, **plus fail the job** (with or without loading) and **row-level outcomes** to split good and bad records |
| Pick when | *"Monitor existing tables without changing pipelines"* | *"Stop bad data from reaching the curated zone"* |

Each run produces a **data quality score** (percentage of rules passed), per-rule outcomes, and optional **CloudWatch metrics**; results are also emitted as **Amazon EventBridge events**, so a rule can alert through **SNS** or trigger remediation. Scores can be surfaced on assets in **Amazon SageMaker Catalog** so consumers see quality before subscribing ([Guide 41](41-SageMaker-Unified-Studio-Catalog-Governance.md)).

A Glue ETL job that quarantines bad rows (PySpark):

```python
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.transforms import SelectFromCollection

ruleset = """Rules = [ IsComplete "order_id", IsUnique "order_id",
                       ColumnValues "amount" > 0 ]"""

dq = EvaluateDataQuality().process_rows(
    frame=orders_dyf,
    ruleset=ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "orders_dq",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
)
rows = SelectFromCollection.apply(dfc=dq, key="rowLevelOutcomes").toDF()
good = rows.filter(rows.DataQualityEvaluationResult == "Passed")
bad  = rows.filter(rows.DataQualityEvaluationResult == "Failed")
good.drop("DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip",
          "DataQualityEvaluationResult").write.mode("append").parquet("s3://amzn-s3-demo-bucket/curated/orders/")
bad.write.mode("append").parquet("s3://amzn-s3-demo-bucket/quarantine/orders/")
```

`ruleOutcomes` (the other key) holds one row per rule — check it to raise an exception and fail the job, the circuit-breaker pattern.

**THE trap:** choosing a custom Lambda or hand-written PySpark assertions for *"validate completeness and uniqueness in a Glue pipeline with the least operational overhead"*. The managed answer is **Glue Data Quality** (Evaluate Data Quality transform / DQDL).

## DataBrew rules and profiles (skills 3.4.2, 3.4.3)

**AWS Glue DataBrew** is the no-code, visual option ([Guide 14](14-Glue-DataBrew-Data-Preparation.md)):
- **Profile jobs** compute column statistics — missing values, distinct counts, distributions, outliers, correlations — on the full dataset or a sample; the go-to for *"investigate data consistency"* by analysts.
- **Data quality rulesets** — rules with thresholds (e.g., "missing values in `email` < 5%", "duplicate rows = 0") attached to a dataset and evaluated by profile jobs; results can trigger EventBridge notifications.
- **Recipes** then clean what the profile found (fill nulls, dedupe, standardize formats, mask PII).

Glue Data Quality vs DataBrew: *"data engineers, in the Glue pipeline, code or Studio"* → Glue DQ; *"business analysts, visual, no code"* → DataBrew.

Other engines: **Deequ / PyDeequ** on **Amazon EMR** or any Spark — the open-source library underneath Glue DQ, for self-managed Spark clusters. **Great Expectations** is a popular third-party framework (runs in Glue/EMR/MWAA); **SageMaker Data Wrangler**'s **Data Quality and Insights report** profiles data for ML prep (missing values, outliers, target leakage, duplicates).

## In-pipeline checks (skill 3.4.1)

| Where | Mechanism |
|---|---|
| **Producers (streams)** | **Glue Schema Registry** serializers reject records not matching the registered schema ([Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md)) |
| **Amazon Data Firehose** | Lambda transform validates each record and returns `Ok`, `Dropped`, or **`ProcessingFailed`**; failed records land under the S3 **error output prefix** (`processing-failed/`) for inspection ([Guide 07](07-Amazon-Data-Firehose.md)) |
| **AWS DMS** | **Data validation** compares source and target rows during full load and CDC; mismatches recorded in a validation-failures table; validation-only tasks re-check later ([Guide 10](10-DMS-Database-Ingestion.md)) |
| **Athena / Redshift** | **Assertion queries** that must return 0 rows: `SELECT COUNT(*) FROM orders WHERE order_id IS NULL`, duplicate checks with `GROUP BY ... HAVING COUNT(*) > 1`, orphan checks with anti-joins ([Guide 34](34-SQL-for-Data-Engineers.md)) |
| **Lambda / Glue code** | Empty-field checks, type casts with try/catch, regex validation; bad records to a DLQ or quarantine prefix |
| **Step Functions** | Run the DQ step, then a **Choice** state on the result: publish on pass, quarantine + SNS alert on fail ([Guide 20](20-Step-Functions.md)) |
| **Glue Data Catalog / Iceberg** | Write to a staging branch/table, validate, then publish (write-audit-publish) |

**THE trap:** relying on Redshift `PRIMARY KEY`, `UNIQUE`, or `FOREIGN KEY` constraints to block duplicates or orphans. **Redshift does not enforce them** — they're informational hints for the optimizer, and declaring them on data that violates them can produce wrong query results. Only `NOT NULL` is enforced. Deduplicate in the load (staging table + `MERGE`/`ROW_NUMBER`) and validate with assertion queries or Glue DQ.

**Quarantine vs. drop vs. fail:** quarantine (keep bad rows in `s3://.../quarantine/` with the failure reason) when you must not lose data and need to reprocess; **fail the job / open the circuit breaker** when publishing partial or wrong data is worse than publishing late (financial reports); drop only when the rules say bad rows are worthless.

## Sampling techniques (skill 3.4.4)

Sampling = checking a subset to infer the whole — for profiling huge tables, validating migrations cheaply, or building training sets.

| Technique | How | Good for | Watch out |
|---|---|---|---|
| **Simple random** | Every row has equal probability | General profiling | May miss rare groups |
| **Systematic** | Every k-th row | Cheap, evenly spread | Periodic patterns in the data align with k → bias |
| **Stratified** | Random sample **within each group** (region, class) proportionally or with minimums | Rare classes, fraud labels, per-region accuracy | Needs the grouping column |
| **Cluster** | Randomly pick whole groups (e.g., 10 stores, or files/blocks) and take all rows | Cheap when data is organized by group | Groups may differ from each other |
| **Reservoir** | Keep a fixed-size uniform sample from a **stream of unknown length** | Kinesis/Kafka sampling | — |
| **First-N / head** | Take the top rows | Quick peek only | **Biased** |

**THE trap:** profiling or validating with the **first N rows**. Files are usually ordered (by time, by source system, by key), so the first rows represent one day or one partition — the sample says "no nulls" while yesterday's feed is full of them. Use random or stratified samples for decisions.

Where sampling shows up on AWS:
- **DataBrew**: project sessions work on a sample (default the first **500** rows; choose **first n** or **random n** rows); profile jobs can run on the full dataset or a custom sample size.
- **Athena**: `SELECT * FROM events TABLESAMPLE BERNOULLI (5)` keeps each row with 5% probability (true row-level random); `TABLESAMPLE SYSTEM (5)` samples whole chunks of data — faster and cheaper but clustered.
- **Spark (Glue/EMR)**: `df.sample(fraction=0.05, seed=42)` (random), `df.sampleBy("region", fractions={...})` (stratified).
- Stratified sampling is also the fix when a **biased sample** causes "skew" in ML training data — the exam may call that data skew too.

## Data skew mechanisms (skill 3.4.5)

Skew = work or data **unevenly spread** across partitions, shards, nodes, or keys, so one worker does most of the job while the others idle. Same disease, different organs:

| Where | Symptom | Fix |
|---|---|---|
| **Spark (Glue/EMR)** | One task runs for an hour while 199 finish in a minute; executor OOM on a join/groupBy | **Salting** hot keys (append a random suffix, aggregate twice), **broadcast join** the small side, enable **AQE (Adaptive Query Execution)** skew-join handling, repartition on a better key ([Guide 16](16-Apache-Spark-Essentials.md)) |
| **Redshift** | Uneven rows per slice (`skew_rows` in `SVV_TABLE_INFO`), slow joins | Change `DISTKEY` to a high-cardinality, evenly distributed column, or `DISTSTYLE EVEN`/`AUTO` ([Guide 23](23-Redshift-Architecture-Table-Design.md)) |
| **Kinesis Data Streams** | `WriteProvisionedThroughputExceeded` on one **hot shard** while others are idle | Higher-cardinality or randomized **partition key**, split the hot shard, or on-demand mode ([Guide 06](06-Kinesis-Data-Streams.md)) |
| **DynamoDB** | Throttling on one **hot partition** | Better partition key design, **write sharding** (key suffixes), caching reads with DAX ([Guide 27](27-DynamoDB.md)) |
| **S3 data lake** | A few giant partitions and many tiny ones | Re-partition, compaction, bucketing |
| **Samples / training data** | Model sees 99% of one class | Stratified sampling, rebalancing |

**THE trap:** answering skew with "add more workers/DPUs/nodes". More capacity doesn't help when **one key** holds most of the data — the hot partition still lands on one worker. Fix the **distribution** (salting, key choice, broadcast).

## Profiling and monitoring quality

- **Profiling**: DataBrew profile jobs (analysts), Glue DQ **rule recommendations** (engineers, Data Catalog), Data Wrangler Data Quality and Insights report (ML prep), Glue Data Catalog column statistics.
- **Monitoring**: Glue DQ publishes **CloudWatch metrics** (rules passed/failed, score) → CloudWatch alarms; **EventBridge rules** match DQ results events → **SNS** email/Slack, or Step Functions remediation; dashboards track the score trend; anomaly detection flags drift you didn't write rules for ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)).
- **Governance**: surface DQ scores in **SageMaker Catalog** so consumers judge fitness before subscribing.

## Question patterns

> *"A Glue ETL job loads orders into the curated zone. Records with a null order_id or a negative amount must not reach curated tables, must be kept for reprocessing, and the solution must need the least custom code."* → **Glue Data Quality Evaluate Data Quality transform with row-level outcomes; write failed rows to a quarantine S3 prefix** (managed rules + routing; custom Lambda validation is more code).

> *"Data stewards want quality checks on existing Glue Data Catalog tables, with a starting set of rules generated automatically, running nightly."* → **Glue Data Quality in Data Catalog mode with rule recommendations and a schedule** (no pipeline changes).

> *"The daily feed occasionally arrives with 40% fewer rows than usual, but the exact normal count changes over time. Alert automatically."* → **Dynamic rule `RowCount > avg(last(n)) * threshold` or anomaly detection with analyzers, results via EventBridge → SNS** (static `RowCount between` can't track a moving baseline).

> *"Business analysts with no coding skills must profile a new dataset for missing values and duplicates and define quality thresholds."* → **AWS Glue DataBrew profile job + data quality ruleset**.

> *"A Firehose delivery stream's Lambda transformation must reject malformed JSON without losing it."* → **Return `ProcessingFailed` for bad records; Firehose writes them to the S3 error output prefix** (dropping loses them).

> *"After migrating an Oracle database to Aurora PostgreSQL with DMS, the team must confirm every row matches."* → **Enable DMS data validation on the task** (or a validation-only task); Glue DQ `DatasetMatch`/`RowCountMatch` can reconcile curated copies.

> *"Duplicate order_id values appear in a Redshift table even though order_id is declared as the PRIMARY KEY."* → **Redshift doesn't enforce primary keys — deduplicate in the load (staging + MERGE / ROW_NUMBER) and add an assertion check** (don't expect a constraint error).

> *"A Step Functions pipeline must publish data only when quality checks pass and notify the team otherwise."* → **Glue DQ step → Choice state on the outcome → publish branch or quarantine + SNS branch** (circuit breaker).

> *"An analyst validated a 2-billion-row table by checking the first 10,000 rows and found no problems; downstream reports still break."* → **Use random or stratified sampling** (first-N follows file order and is biased).

> *"A fraud model needs a training sample that keeps enough of the rare fraud class from every region."* → **Stratified sampling by region and label** (simple random may miss rare strata).

> *"Estimate data quality on a huge Athena table as cheaply as possible, with a truly row-level random sample."* → **`TABLESAMPLE BERNOULLI (p)`** (SYSTEM is cheaper but samples blocks, not rows).

> *"A Glue Spark join on customer_id stalls at 99% with one long-running task; one customer accounts for 30% of rows."* → **Salt the hot key (or broadcast the smaller table) and enable AQE skew join** (adding DPUs leaves the hot key on one task).

> *"Redshift queries on a fact table are slow and SVV_TABLE_INFO shows high row skew."* → **Pick a more evenly distributed DISTKEY (or DISTSTYLE EVEN/AUTO)**.

> *"One Kinesis shard throttles while others are idle; the partition key is device_type with 4 values."* → **Use a high-cardinality partition key (e.g., device_id)** (resharding alone doesn't split 4 keys well).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Completeness, validity, uniqueness in a Glue job | Glue Data Quality (DQDL) |
| Engine under Glue DQ | Deequ (PyDeequ on EMR) |
| Ruleset syntax | `Rules = [ IsComplete "col", ... ]` |
| Null check | `IsComplete` / `Completeness "col" > 0.95` |
| Allowed values / regex / range | `ColumnValues ... in [...]` / `matches` / `between` |
| Duplicate check | `IsUnique` / `IsPrimaryKey` / `Uniqueness` |
| Volume check | `RowCount between x and y` |
| Moving baseline | Dynamic rules `avg(last(n))`, anomaly detection (ETL only) |
| Stale data | `DataFreshness "ts" <= 24 hours` |
| Orphan foreign keys | `ReferentialIntegrity` (or anti-join SQL) |
| Source vs target reconciliation | `DatasetMatch` / `RowCountMatch`; DMS data validation |
| Arbitrary SQL assertion | `CustomSql "select ... from primary"` |
| Existing tables, auto-suggested rules, scheduled | Glue DQ Data Catalog mode + recommendations |
| Split good/bad rows in ETL | Row-level outcomes → quarantine prefix |
| Stop the pipeline on bad data | Fail job / Step Functions Choice (circuit breaker) |
| Alert on DQ failure | EventBridge rule → SNS; CloudWatch metrics + alarms |
| Analysts, no code, profiling | DataBrew profile jobs + DQ rulesets |
| ML-prep profiling | Data Wrangler Data Quality and Insights report |
| Firehose bad records | `ProcessingFailed` → S3 error prefix |
| Stream schema enforcement | Glue Schema Registry |
| Redshift PK/UNIQUE/FK | Not enforced (only NOT NULL) |
| Quick peek sample | First-N — biased, never for decisions |
| Rare groups represented | Stratified sampling |
| Stream of unknown length | Reservoir sampling |
| Athena random rows | `TABLESAMPLE BERNOULLI (p)`; blocks = `SYSTEM` |
| Spark hot key | Salting, broadcast join, AQE |
| Redshift slice skew | Better DISTKEY / EVEN / AUTO |
| Kinesis hot shard | Higher-cardinality partition key |
| DynamoDB hot partition | Key redesign, write sharding |
| DQ visible to data consumers | DQ scores in SageMaker Catalog |

Quality checks tell you whether data is right; SQL is how you prove it and reshape it — continue with [Guide 34 — SQL for Data Engineers](34-SQL-for-Data-Engineers.md).
