# Study Plan — DEA-C01

Pick the plan that fits your background, follow the reading order, do the labs, and take the practice exams at the checkpoints. Every plan ends the same way: Practice Exam 02, the v1.1 drill, guide 47, then a rest day.

| Plan | Best for | Time per day |
|---|---|---|
| [6-week plan](#6-week-plan) | Data engineers who are new to AWS analytics services | ~1–1.5 h |
| [3-week plan](#3-week-plan) | People who already use AWS (for example, hold SAA-C03) | ~2 h |
| [10-day crash plan](#10-day-crash-plan) | People who use Glue, Redshift and Kinesis at work every day | ~3 h |

**Readiness signal:** **≥ 80%** on both practice exams, scored cold (no notes, first attempt), plus the ability to explain every row of the [numbers cheat sheet](cheat-sheets/numbers-to-know.md) without looking.

---

## 6-week plan

| Week | Theme | Guides | Labs | Checkpoint |
|---|---|---|---|---|
| 1 | Foundations & the lake | [01](topic-guides/01-Exam-Blueprint.md), [02](topic-guides/02-Data-Engineering-Fundamentals.md), [03](topic-guides/03-Data-Formats-Compression.md), [05](topic-guides/05-S3-Data-Lake-Storage.md), [04](topic-guides/04-Open-Table-Formats-S3-Tables.md), [13](topic-guides/13-Glue-Data-Catalog-Crawlers.md), [26](topic-guides/26-Amazon-Athena.md) | A, B | Explain "why Parquet + partitions + right-sized files" in 60 seconds |
| 2 | Processing | [12](topic-guides/12-AWS-Glue-ETL.md), [16](topic-guides/16-Apache-Spark-Essentials.md), [14](topic-guides/14-Glue-DataBrew-Data-Preparation.md), [15](topic-guides/15-Amazon-EMR.md), [17](topic-guides/17-Lambda-for-Data-Pipelines.md), [18](topic-guides/18-Containers-Batch-EC2-Compute.md), [10](topic-guides/10-DMS-Database-Ingestion.md) | C, D | Pick Lambda vs Glue vs EMR vs Batch for 5 made-up jobs |
| 3 | Streaming & orchestration | [06](topic-guides/06-Kinesis-Data-Streams.md), [07](topic-guides/07-Amazon-Data-Firehose.md), [08](topic-guides/08-Amazon-MSK-Kafka.md), [09](topic-guides/09-Managed-Service-for-Apache-Flink.md), [11](topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md), [20](topic-guides/20-Step-Functions.md), [21](topic-guides/21-MWAA-Glue-Workflows.md), [22](topic-guides/22-EventBridge-SNS-SQS.md) | E, G | Draw a pipeline: S3 upload → EventBridge → Step Functions → Glue → SNS on failure |
| 4 | Data stores & SQL | [23](topic-guides/23-Redshift-Architecture-Table-Design.md), [24](topic-guides/24-Redshift-Loading-Integration-Sharing.md), [25](topic-guides/25-Redshift-Performance-Operations-Security.md), [27](topic-guides/27-DynamoDB.md), [28](topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md), [29](topic-guides/29-OpenSearch-Service.md), [30](topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md), [34](topic-guides/34-SQL-for-Data-Engineers.md) | F | **[Practice Exam 01](practice-exams/practice-exam-01.md)**, then re-read the Question patterns for every miss |
| 5 | Operations, quality & security | [31](topic-guides/31-Data-Lifecycle-Retention-Resiliency.md), [32](topic-guides/32-Monitoring-Logging-Troubleshooting.md), [33](topic-guides/33-Data-Quality.md), [35](topic-guides/35-Analytics-Visualization-Quick-Notebooks.md), [36](topic-guides/36-Programming-IaC-CICD.md), [37](topic-guides/37-IAM-for-Data-Engineers.md), [38](topic-guides/38-Networking-for-Data-Pipelines.md), [39](topic-guides/39-Encryption-Key-Management.md) | H, J | Write a least-privilege S3 read policy for one prefix from memory |
| 6 | Governance, GenAI & final review | [40](topic-guides/40-Lake-Formation.md), [41](topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md), [42](topic-guides/42-Privacy-PII-Masking-Sovereignty.md), [43](topic-guides/43-Audit-Logging-CloudTrail-Config.md), [19](topic-guides/19-GenAI-LLMs-Vectors.md), [44](topic-guides/44-Cost-Optimization.md), [45](topic-guides/45-Service-Selection-Decision-Guide.md), [46](topic-guides/46-GapFill-Services.md), [47](topic-guides/47-Exam-Traps-Key-Patterns.md) | I, K, L | **[Practice Exam 02](practice-exams/practice-exam-02.md)** + **[v1.1 drill](practice-exams/v1.1-new-topics-drill.md)**. Book the exam if you're ≥ 80% on both |

---

## 3-week plan

| Day | Read | Do |
|---|---|---|
| 1 | [01](topic-guides/01-Exam-Blueprint.md), [02](topic-guides/02-Data-Engineering-Fundamentals.md), [03](topic-guides/03-Data-Formats-Compression.md) | Book a tentative exam date ~4 weeks out (a date focuses the mind) |
| 2 | [05](topic-guides/05-S3-Data-Lake-Storage.md), [04](topic-guides/04-Open-Table-Formats-S3-Tables.md) | Lab A |
| 3 | [13](topic-guides/13-Glue-Data-Catalog-Crawlers.md), [26](topic-guides/26-Amazon-Athena.md) | Lab B |
| 4 | [12](topic-guides/12-AWS-Glue-ETL.md) | Lab C |
| 5 | [16](topic-guides/16-Apache-Spark-Essentials.md), [14](topic-guides/14-Glue-DataBrew-Data-Preparation.md) | Lab D |
| 6 | [15](topic-guides/15-Amazon-EMR.md), [18](topic-guides/18-Containers-Batch-EC2-Compute.md) | Pocket-card quiz: guides 02–16 |
| 7 | [06](topic-guides/06-Kinesis-Data-Streams.md), [17](topic-guides/17-Lambda-for-Data-Pipelines.md) | Lab E (part 1) |
| 8 | [07](topic-guides/07-Amazon-Data-Firehose.md), [08](topic-guides/08-Amazon-MSK-Kafka.md), [09](topic-guides/09-Managed-Service-for-Apache-Flink.md) | Lab E (part 2) |
| 9 | [10](topic-guides/10-DMS-Database-Ingestion.md), [11](topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) | — |
| 10 | [20](topic-guides/20-Step-Functions.md), [21](topic-guides/21-MWAA-Glue-Workflows.md), [22](topic-guides/22-EventBridge-SNS-SQS.md) | Lab G |
| 11 | [23](topic-guides/23-Redshift-Architecture-Table-Design.md), [24](topic-guides/24-Redshift-Loading-Integration-Sharing.md) | Lab F (part 1) |
| 12 | [25](topic-guides/25-Redshift-Performance-Operations-Security.md), [34](topic-guides/34-SQL-for-Data-Engineers.md) | Lab F (part 2) |
| 13 | **[Practice Exam 01](practice-exams/practice-exam-01.md)** | Review every miss in the linked guide |
| 14 | [27](topic-guides/27-DynamoDB.md), [28](topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md), [29](topic-guides/29-OpenSearch-Service.md) | — |
| 15 | [30](topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md), [31](topic-guides/31-Data-Lifecycle-Retention-Resiliency.md), [33](topic-guides/33-Data-Quality.md) | Lab I |
| 16 | [32](topic-guides/32-Monitoring-Logging-Troubleshooting.md), [35](topic-guides/35-Analytics-Visualization-Quick-Notebooks.md), [36](topic-guides/36-Programming-IaC-CICD.md) | Lab J |
| 17 | [37](topic-guides/37-IAM-for-Data-Engineers.md), [38](topic-guides/38-Networking-for-Data-Pipelines.md), [39](topic-guides/39-Encryption-Key-Management.md) | — |
| 18 | [40](topic-guides/40-Lake-Formation.md), [42](topic-guides/42-Privacy-PII-Masking-Sovereignty.md), [43](topic-guides/43-Audit-Logging-CloudTrail-Config.md) | Lab H |
| 19 | [19](topic-guides/19-GenAI-LLMs-Vectors.md), [41](topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) | Labs K, L · **[v1.1 drill](practice-exams/v1.1-new-topics-drill.md)** |
| 20 | [44](topic-guides/44-Cost-Optimization.md), [45](topic-guides/45-Service-Selection-Decision-Guide.md), [46](topic-guides/46-GapFill-Services.md) | **[Practice Exam 02](practice-exams/practice-exam-02.md)** |
| 21 | [47](topic-guides/47-Exam-Traps-Key-Patterns.md) + [cheat sheets](cheat-sheets/) | Light review only. Exam in the next 1–3 days |

---

## 10-day crash plan

For people who already build on these services. Skim the pocket cards of topics you know, and read in full only where the pocket card surprises you.

| Day | Focus |
|---|---|
| 1 | [01 Blueprint](topic-guides/01-Exam-Blueprint.md) in full (especially the v1.1 and "what changed in AWS" tables) · pocket cards of 02, 03, 05 · read [04](topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| 2 | [12 Glue ETL](topic-guides/12-AWS-Glue-ETL.md), [13 Catalog & Crawlers](topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| 3 | [26 Athena](topic-guides/26-Amazon-Athena.md), [16 Spark](topic-guides/16-Apache-Spark-Essentials.md) · Lab A |
| 4 | [06 Kinesis](topic-guides/06-Kinesis-Data-Streams.md), [07 Firehose](topic-guides/07-Amazon-Data-Firehose.md), [17 Lambda](topic-guides/17-Lambda-for-Data-Pipelines.md) |
| 5 | [23](topic-guides/23-Redshift-Architecture-Table-Design.md), [24](topic-guides/24-Redshift-Loading-Integration-Sharing.md), [25](topic-guides/25-Redshift-Performance-Operations-Security.md) (Redshift) |
| 6 | [20](topic-guides/20-Step-Functions.md), [21](topic-guides/21-MWAA-Glue-Workflows.md), [22](topic-guides/22-EventBridge-SNS-SQS.md), [10](topic-guides/10-DMS-Database-Ingestion.md) · **[Practice Exam 01](practice-exams/practice-exam-01.md)** |
| 7 | [40 Lake Formation](topic-guides/40-Lake-Formation.md), [42](topic-guides/42-Privacy-PII-Masking-Sovereignty.md), [43](topic-guides/43-Audit-Logging-CloudTrail-Config.md) · pocket cards of 37, 38, 39 |
| 8 | [19](topic-guides/19-GenAI-LLMs-Vectors.md), [41](topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) (🆕) · **[v1.1 drill](practice-exams/v1.1-new-topics-drill.md)** · pocket cards of 27, 28, 29 |
| 9 | Pocket cards of 30–36 · **[Practice Exam 02](practice-exams/practice-exam-02.md)** |
| 10 | [44](topic-guides/44-Cost-Optimization.md), [45](topic-guides/45-Service-Selection-Decision-Guide.md), [46](topic-guides/46-GapFill-Services.md), [47](topic-guides/47-Exam-Traps-Key-Patterns.md) · [cheat sheets](cheat-sheets/) |

---

## Hands-on lab checklist

DEA-C01 rewards people who have actually *touched* the services: seen a crawler create twelve tables when you expected one, watched IteratorAge climb, hit an "Insufficient Lake Formation permissions" error. Each lab below takes 30–90 minutes in the console. The steps are kept high level on purpose; working out the details is part of the learning.

> **Cost guardrails, before you start:**
> 1. Create an **AWS Budgets** cost budget with an email alert at a small amount (e.g., USD 10). See [guide 44](topic-guides/44-Cost-Optimization.md).
> 2. Use **one Region** for everything and tag lab resources (e.g., `project=dea-lab`) so Cost Explorer can show you what you spent.
> 3. Delete these **as soon as each lab ends**, because they bill by the hour even when idle: MSK clusters, OpenSearch domains and OpenSearch Serverless collections (minimum capacity is billed continuously), Redshift *provisioned* clusters, EMR clusters, MWAA environments, NAT gateways, Aurora instances, provisioned Kinesis shards, and any tooling environments created by SageMaker Unified Studio projects.
> 4. Prefer serverless options (Athena, Glue, Redshift Serverless at its minimum base capacity, Kinesis on-demand, Firehose, Lambda) and small datasets (a few hundred MB is plenty).
> 5. Never put real personal data or credentials into a lab. Generate synthetic data.

| Lab | What you build | What to watch for (the exam lesson) | Guides |
|---|---|---|---|
| **A** — Lake basics | Upload a CSV dataset to `s3://…/raw/`, crawl it with a Glue crawler, query it in Athena. Then use CTAS to write a partitioned, Snappy-compressed Parquet copy to `curated/` and run the same query again | Compare **bytes scanned** before and after: columnar + partitioned + compressed. Try a crawler over mixed schemas and watch it create extra tables | [03](topic-guides/03-Data-Formats-Compression.md), [13](topic-guides/13-Glue-Data-Catalog-Crawlers.md), [26](topic-guides/26-Amazon-Athena.md) |
| **B** — Partition projection | Create a table over date-partitioned data with partition projection properties, add new date folders, and query without crawling | New partitions become queryable **without** a crawler or `MSCK REPAIR TABLE` | [13](topic-guides/13-Glue-Data-Catalog-Crawlers.md), [26](topic-guides/26-Amazon-Athena.md) |
| **C** — Glue ETL + bookmarks | A Glue Studio job (raw CSV → curated Parquet) with job bookmarks on. Run it, add a file, run it again. Then make the job update the Data Catalog itself (`enableUpdateCatalog`) | The second run processes **only the new file**. Open the job's metrics and Spark UI. Switch the job to **Flex** and see how start time changes | [12](topic-guides/12-AWS-Glue-ETL.md) |
| **D** — Data quality | Generate a Glue Data Quality ruleset from recommendations, run it, and publish results. Run a DataBrew profile job on the same data and apply a masking recipe to a synthetic email column | How DQDL rules read, where results land (CloudWatch/EventBridge), what a DataBrew profile shows | [14](topic-guides/14-Glue-DataBrew-Data-Preparation.md), [33](topic-guides/33-Data-Quality.md) |
| **E** — Streaming | A Kinesis Data Stream (on-demand) fed by a small boto3 `put_records` script, with a Lambda consumer. Then a Firehose stream from it to S3 with dynamic partitioning and JSON → Parquet conversion | **IteratorAge** when the Lambda is slow; how the Lambda event source mapping settings change behavior; Firehose's buffer-driven latency and output prefixes | [06](topic-guides/06-Kinesis-Data-Streams.md), [07](topic-guides/07-Amazon-Data-Firehose.md), [17](topic-guides/17-Lambda-for-Data-Pipelines.md) |
| **F** — Redshift Serverless | Create a workgroup at minimum base capacity. COPY Parquet from S3 using an IAM role, create an external schema over your Glue database (Spectrum), UNLOAD a query result to partitioned Parquet, build a materialized view, and add an RLS policy and a masking policy for two roles | The COPY/UNLOAD syntax, Spectrum joins to local tables, how RLS/DDM output differs by role. Check `SYS_QUERY_HISTORY` | [23](topic-guides/23-Redshift-Architecture-Table-Design.md), [24](topic-guides/24-Redshift-Loading-Integration-Sharing.md), [25](topic-guides/25-Redshift-Performance-Operations-Security.md) |
| **G** — Orchestration | A Step Functions state machine: Glue job (`.sync`) → Athena query (`.sync`) → SNS email on failure, with Retry and Catch. Trigger it from an EventBridge rule on S3 *Object Created* | How `.sync` waits, how Catch routes errors, what an execution history looks like. Break the Glue job on purpose and watch the retries | [20](topic-guides/20-Step-Functions.md), [22](topic-guides/22-EventBridge-SNS-SQS.md) |
| **H** — Lake Formation | Register the S3 location, remove IAMAllowedPrincipals from a database, grant column-level SELECT to an "analyst" role, add a row filter, and query in Athena as that role. Try LF-Tags | The classic "Insufficient Lake Formation permissions" error and the fix; how column and row filters show up in results | [40](topic-guides/40-Lake-Formation.md) |
| **I** — Iceberg & S3 Tables | An Iceberg table in Athena: INSERT, MERGE INTO, time-travel query, OPTIMIZE, VACUUM. Then create an S3 table bucket and a table, and query it from Athena | Snapshots, time travel, why DELETE doesn't purge files until snapshots expire | [04](topic-guides/04-Open-Table-Formats-S3-Tables.md), [26](topic-guides/26-Amazon-Athena.md) |
| **J** — Audit & monitoring | A CloudTrail trail with S3 data events for one bucket, CloudTrail logs queried in Athena, a CloudWatch Logs metric filter on Lambda errors with an alarm → SNS, and a Logs Insights query | Management vs data events, the delay before logs arrive, turning log patterns into alarms | [32](topic-guides/32-Monitoring-Logging-Troubleshooting.md), [43](topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| **K** — Vectors & RAG | A Bedrock Knowledge Base over a few documents in S3, using a low-cost vector store (e.g., S3 Vectors). Sync it, then test retrieval and change the chunking settings | Ingestion = parse → chunk → embed → store. What chunk size does to retrieval quality | [19](topic-guides/19-GenAI-LLMs-Vectors.md) |
| **L** — SageMaker Catalog | In SageMaker Unified Studio, create a domain, two projects, publish a Glue table as a catalog asset from one, and subscribe to it from the other | Publish → subscribe → approve → access is granted automatically (look for the Lake Formation grant) | [41](topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) |

---

## Final-week checklist

- [ ] Both practice exams ≥ 80% on the first attempt, and every miss reviewed in its linked guide
- [ ] v1.1 drill done: you can explain HNSW vs IVF, Iceberg snapshot expiry, and SageMaker Catalog subscriptions aloud
- [ ] Guide 47 (traps) read twice
- [ ] Every row of the [numbers cheat sheet](cheat-sheets/numbers-to-know.md) recalled without looking
- [ ] You can sketch, from memory: a streaming pipeline, a batch lake pipeline, a Redshift ELT flow, and a cross-account Lake Formation share
- [ ] Exam logistics sorted: ID ready. For online proctoring, run the system check on the exam computer and clear your desk

## Exam day

- **Pace:** 130 minutes for 65 questions. Aim to be at **Q22 by ~40 minutes** and **Q44 by ~85 minutes**, which leaves ~15 minutes for flagged questions.
- **Flag and move on** if you've spent 2+ minutes on a question. Later questions often jog your memory.
- **Answer every question.** A blank is scored wrong, and there's no penalty for guessing.
- **Multiple response:** the stem tells you how many to pick ("Select TWO"). Judge each option on its own.
- **Service names are abbreviated** on the exam. The Help button has the reference list.
