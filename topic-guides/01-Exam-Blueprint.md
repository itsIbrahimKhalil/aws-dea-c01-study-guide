# 01 · Exam Blueprint — the map before the territory

> **Exam map:** All domains · **Skills:** all 120 · **Weight:** read first · **Read time:** ~15 min

## The idea

Think of the exam guide as the **floor plan of the building you're about to be tested in**. Every question on DEA-C01 is written against one of the "skills" in AWS's official outline, and AWS tells you roughly how many questions come from each wing of the building. Most people study service by service and hope for the best. You're going to study **skill by skill**, with a map of which guide covers each one.

This guide has four jobs:
1. Give you the exam's hard facts (format, scoring, timing).
2. Show you what changed in the **v1.1 revision (December 12, 2025)**, because most prep material online still teaches v1.0.
3. Flag what has changed in AWS **since** the exam guide was written, so a renamed or retired service doesn't throw you on exam day.
4. Map **every skill ID** to the guide(s) in this repo that teach it.

## The exam at a glance

| Item | Detail |
|---|---|
| Exam code | **DEA-C01** (exam guide **v1.1**, published Dec 12, 2025) |
| Questions | **65** total: **50 scored + 15 unscored** (the unscored ones aren't marked, so treat every question as real) |
| Time | **130 minutes** (2 minutes per question) |
| Question types | **Multiple choice** (1 correct of 4) and **multiple response** (2+ correct of 5+) |
| Scoring | Scaled **100–1,000**, pass = **720**. **Compensatory**: you don't need to pass each domain, only the whole exam |
| Guessing | **No penalty**. An unanswered question is simply wrong, so answer everything |
| Cost | **USD 150** |
| Delivery | Pearson VUE test center or online proctored |
| Languages | English, Japanese, Korean, Simplified Chinese |
| Validity | **3 years** |
| Service names | The exam uses **abbreviated service names**. A reference list is available through the **Help** button during the exam, and you can review it beforehand on the AWS Certification site |
| Target candidate | 2–3 years of data engineering plus 1–2 years hands-on with AWS |

**Tip for non-native English speakers:** AWS offers an **ESL +30 minutes** exam accommodation. Request it in your AWS Certification account **before** you schedule.

### Domains and what they mean in questions

| Domain | Weight | ≈ scored questions (of 50) | What it really tests |
|---|---|---|---|
| **1. Data Ingestion and Transformation** | **34%** | ~17 | Picking and configuring ingestion (Kinesis, Firehose, MSK, DMS, AppFlow, DataSync), transformation (Glue, EMR, Lambda, Redshift SQL), orchestration (Step Functions, MWAA, Glue workflows, EventBridge), plus programming, IaC and CI/CD |
| **2. Data Store Management** | **26%** | ~13 | Choosing and designing stores (S3, Redshift, DynamoDB, RDS/Aurora, OpenSearch, vector stores), catalogs and partitions, lifecycle, data modeling, schema evolution, open table formats |
| **3. Data Operations and Support** | **22%** | ~11 | Automating and troubleshooting pipelines, querying and visualizing (Athena, Redshift, Quick Sight), monitoring and logging, data quality |
| **4. Data Security and Governance** | **18%** | ~9 | IAM, Lake Formation, KMS, Secrets Manager, masking and PII, audit logging, data sharing, sovereignty, SageMaker Catalog |

**What the exam leaves out** (per the official guide): ML training and inference, language-specific syntax trivia, and drawing business conclusions from data. You need to know *what* a PySpark job or SQL query does, but you won't be asked about semicolons.

## What changed in v1.1 (December 12, 2025)

AWS merged v1.0's separate "Knowledge of" and "Skills in" lists into one skills list per task, added eight new skills, and updated the service list. If your course or practice tests predate December 2025, **this table is what they're missing**.

### 🆕 New skills

| Skill | What it asks | Where to learn it |
|---|---|---|
| **1.2.10** | Integrate large language models (LLMs) for data processing | [19 — GenAI, LLMs & Vectors](19-GenAI-LLMs-Vectors.md) |
| **2.1.7** | Manage open table formats (e.g., Apache Iceberg) | [04 — Open Table Formats & S3 Tables](04-Open-Table-Formats-S3-Tables.md) |
| **2.1.8** | Describe vector index types (e.g., HNSW, IVF) | [19](19-GenAI-LLMs-Vectors.md), [29 — OpenSearch](29-OpenSearch-Service.md) |
| **2.2.6** | Create and manage business data catalogs (e.g., SageMaker Catalog) | [41 — SageMaker Unified Studio & Catalog](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| **2.4.6** | Describe vectorization concepts (e.g., Bedrock knowledge base) | [19](19-GenAI-LLMs-Vectors.md) |
| **4.1.7** | Use domains, domain units and projects in SageMaker Unified Studio | [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| **4.5.6** | Manage data access through SageMaker Catalog projects | [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| **4.5.7** | Describe governance data frameworks and data sharing patterns | [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |

### Reworded skills worth noticing

| Skill | The new wording's hint |
|---|---|
| **2.1.3** | Now cites **HNSW indexing with Aurora PostgreSQL** (pgvector) and **MemoryDB for fast key/value access**. Expect "pick the store for this access pattern" questions that include vector stores |
| **2.4.4** | Lineage now names **SageMaker Catalog** alongside SageMaker ML Lineage Tracking |
| **3.1.6** | Data prep now names **SageMaker Unified Studio** alongside Glue DataBrew |
| **3.2.3** | "Use SQL in **Redshift and Athena**" (previously Athena only) |
| **4.3.4** | "Encryption in transit **or before transit**", i.e., client-side encryption |
| **4.2.5 / 4.2.6** | Former "knowledge" items are now skills: apply **RBAC/tag-based/ABAC**, and **build least-privilege policies** |

### Service list changes

| Added to in-scope | Removed from in-scope |
|---|---|
| Amazon **Aurora**, Amazon **Q**, Amazon **Bedrock**, Amazon **Kendra**, AWS **Data Exchange**, Amazon **S3 Tables** | AWS **Cloud9**, AWS **CodeCommit**, AWS **Schema Conversion Tool (SCT)** |

One oddity: SCT left the service list, but skill **2.4.3** still says "perform schema conversion (for example, by using AWS SCT and AWS DMS Schema Conversion)". Know what SCT does. The managed successor is **DMS Schema Conversion**.

## What changed in AWS since the exam guide (verified September 2026)

Exam question pools lag behind AWS. When something below appears in a question, here's how to read it.

| Change | Date | How to treat it on the exam |
|---|---|---|
| QuickSight → **Amazon Quick Suite** → now **Amazon Quick**. The BI component is **Quick Sight** | Oct 2025 onward | Same product. "QuickSight" in a question = Quick Sight |
| Kinesis Data Firehose → **Amazon Data Firehose** | Feb 2024 | Same product. The exam guide still says "Kinesis Data Firehose" |
| Kinesis Data Analytics (Flink) → **Managed Service for Apache Flink** (Aug 2023). KDA **for SQL** applications: no new apps from Oct 15, 2025, and existing apps deleted from Jan 27, 2026 | 2023–2026 | "KDA for SQL" is a legacy distractor. Stateful streaming analytics → Managed Flink |
| SageMaker → **SageMaker AI** (ML), with **SageMaker Unified Studio**, lakehouse and **SageMaker Catalog** (built on DataZone) | Dec 2024 onward | "Amazon DataZone" in older questions ≈ SageMaker Catalog concepts |
| **SQS max message size 256 KiB → 1 MiB** | Aug 2025 | Older questions may still assume 256 KB. Offloading to S3 is still right for larger payloads |
| **S3 Select** closed to new customers | Jul 2024 | Filtering a large object with SQL → prefer **Athena** in new designs |
| **S3 Object Lambda**, Snowball Edge devices, Application Discovery Service, original **Amazon Glacier** vaults → maintenance (no new customers) | Nov 2025 | Concepts may still be tested. S3 Glacier *storage classes* are unaffected |
| **AWS Glue Ray jobs** → maintenance | Apr 2026 | Don't pick Ray for new Glue designs. Spark or Python shell instead |
| **CloudTrail Lake** closed to new customers | May 31, 2026 | Still explicitly in skill 4.4.3 ("centralized logging queries"), so still a valid exam answer |
| **Amazon Kendra**, Amazon Q Business → maintenance | Jul 30, 2026 | Kendra is still in v1.1 scope. Know it as managed enterprise/semantic search |
| **S3 Vectors** GA (vector buckets, up to 2 billion vectors per index) | Dec 2025 | New low-cost vector store option (see guide 19) |
| **MWAA Serverless** launched | Nov 2025 | Airflow without an always-on environment (see guide 21) |
| AWS Data Pipeline in maintenance since 2024; not in scope | 2024 | Classic distractor. Modern answer: Step Functions / MWAA / Glue workflows |

## Skill → guide traceability matrix

Use this as a checklist. When you can explain a skill aloud without notes, tick it.

### Domain 1: Data Ingestion and Transformation (34%)

| Skill | Short description | Primary guides |
|---|---|---|
| 1.1.1 | Read from streaming sources (Kinesis, MSK, DynamoDB Streams, DMS, Glue, Redshift) | [06](06-Kinesis-Data-Streams.md), [07](07-Amazon-Data-Firehose.md), [08](08-Amazon-MSK-Kafka.md), [10](10-DMS-Database-Ingestion.md), [24](24-Redshift-Loading-Integration-Sharing.md), [27](27-DynamoDB.md) |
| 1.1.2 | Read from batch sources (S3, Glue, EMR, DMS, Redshift, Lambda, AppFlow) | [05](05-S3-Data-Lake-Storage.md), [11](11-DataSync-Transfer-Family-Snow-AppFlow.md), [12](12-AWS-Glue-ETL.md), [15](15-Amazon-EMR.md), [24](24-Redshift-Loading-Integration-Sharing.md) |
| 1.1.3 | Batch ingestion configuration options | [11](11-DataSync-Transfer-Family-Snow-AppFlow.md), [12](12-AWS-Glue-ETL.md), [24](24-Redshift-Loading-Integration-Sharing.md) |
| 1.1.4 | Consume data APIs | [11](11-DataSync-Transfer-Family-Snow-AppFlow.md), [17](17-Lambda-for-Data-Pipelines.md), [36](36-Programming-IaC-CICD.md) |
| 1.1.5 | Schedulers (EventBridge, Airflow, job and crawler schedules) | [21](21-MWAA-Glue-Workflows.md), [22](22-EventBridge-SNS-SQS.md), [13](13-Glue-Data-Catalog-Crawlers.md) |
| 1.1.6 | Event triggers (S3 Event Notifications, EventBridge) | [22](22-EventBridge-SNS-SQS.md), [05](05-S3-Data-Lake-Storage.md) |
| 1.1.7 | Call Lambda from Kinesis | [06](06-Kinesis-Data-Streams.md), [17](17-Lambda-for-Data-Pipelines.md) |
| 1.1.8 | IP allowlists for data source connections | [38](38-Networking-for-Data-Pipelines.md), [11](11-DataSync-Transfer-Family-Snow-AppFlow.md), [07](07-Amazon-Data-Firehose.md) |
| 1.1.9 | Throttling and rate limits (DynamoDB, RDS, Kinesis) | [06](06-Kinesis-Data-Streams.md), [27](27-DynamoDB.md), [28](28-RDS-Aurora-Purpose-Built-DBs.md), [22](22-EventBridge-SNS-SQS.md) |
| 1.1.10 | Fan-in / fan-out for streaming distribution | [02](02-Data-Engineering-Fundamentals.md), [06](06-Kinesis-Data-Streams.md), [08](08-Amazon-MSK-Kafka.md), [22](22-EventBridge-SNS-SQS.md) |
| 1.1.11 | Replayability of ingestion pipelines | [02](02-Data-Engineering-Fundamentals.md), [06](06-Kinesis-Data-Streams.md), [22](22-EventBridge-SNS-SQS.md) |
| 1.1.12 | Stateful vs stateless data transactions | [02](02-Data-Engineering-Fundamentals.md), [09](09-Managed-Service-for-Apache-Flink.md) |
| 1.2.1 | Optimize container usage (EKS, ECS) | [18](18-Containers-Batch-EC2-Compute.md) |
| 1.2.2 | Connect to sources (JDBC, ODBC) | [12](12-AWS-Glue-ETL.md), [38](38-Networking-for-Data-Pipelines.md) |
| 1.2.3 | Integrate data from multiple sources | [12](12-AWS-Glue-ETL.md), [24](24-Redshift-Loading-Integration-Sharing.md), [26](26-Amazon-Athena.md) |
| 1.2.4 | Optimize processing costs | [44](44-Cost-Optimization.md), [12](12-AWS-Glue-ETL.md), [15](15-Amazon-EMR.md) |
| 1.2.5 | Choose transformation services (EMR, Glue, Lambda, Redshift) | [45](45-Service-Selection-Decision-Guide.md), [12](12-AWS-Glue-ETL.md), [15](15-Amazon-EMR.md), [17](17-Lambda-for-Data-Pipelines.md) |
| 1.2.6 | Transform between formats (CSV → Parquet) | [03](03-Data-Formats-Compression.md), [07](07-Amazon-Data-Firehose.md), [12](12-AWS-Glue-ETL.md), [26](26-Amazon-Athena.md) |
| 1.2.7 | Troubleshoot transformation failures and performance | [12](12-AWS-Glue-ETL.md), [16](16-Apache-Spark-Essentials.md), [32](32-Monitoring-Logging-Troubleshooting.md) |
| 1.2.8 | Create data APIs | [36](36-Programming-IaC-CICD.md) |
| 1.2.9 | Volume, velocity, variety | [02](02-Data-Engineering-Fundamentals.md) |
| 1.2.10 🆕 | Integrate LLMs for data processing | [19](19-GenAI-LLMs-Vectors.md) |
| 1.3.1 | Orchestration services (Lambda, EventBridge, MWAA, Step Functions, Glue workflows) | [20](20-Step-Functions.md), [21](21-MWAA-Glue-Workflows.md), [22](22-EventBridge-SNS-SQS.md) |
| 1.3.2 | Performant, available, scalable, resilient, fault-tolerant pipelines | [20](20-Step-Functions.md), [21](21-MWAA-Glue-Workflows.md), [31](31-Data-Lifecycle-Retention-Resiliency.md) |
| 1.3.3 | Serverless workflows | [20](20-Step-Functions.md), [17](17-Lambda-for-Data-Pipelines.md) |
| 1.3.4 | Notifications (SNS, SQS) | [22](22-EventBridge-SNS-SQS.md), [32](32-Monitoring-Logging-Troubleshooting.md) |
| 1.4.1 | Optimize code runtime | [16](16-Apache-Spark-Essentials.md), [34](34-SQL-for-Data-Engineers.md) |
| 1.4.2 | Lambda concurrency and performance | [17](17-Lambda-for-Data-Pipelines.md) |
| 1.4.3 | Languages (Python, SQL, Scala, R, Java, Bash, PowerShell) | [36](36-Programming-IaC-CICD.md) |
| 1.4.4 | Software engineering practices (version control, testing, logging, monitoring) | [36](36-Programming-IaC-CICD.md), [32](32-Monitoring-Logging-Troubleshooting.md) |
| 1.4.5 | IaC for data engineering solutions | [36](36-Programming-IaC-CICD.md) |
| 1.4.6 | AWS SAM packaging and deployment | [17](17-Lambda-for-Data-Pipelines.md), [36](36-Programming-IaC-CICD.md) |
| 1.4.7 | Storage volumes in Lambda | [17](17-Lambda-for-Data-Pipelines.md) |
| 1.4.8 | CloudFormation and CDK | [36](36-Programming-IaC-CICD.md) |
| 1.4.9 | CI/CD for data pipelines | [36](36-Programming-IaC-CICD.md) |
| 1.4.10 | Distributed computing | [02](02-Data-Engineering-Fundamentals.md), [16](16-Apache-Spark-Essentials.md) |
| 1.4.11 | Data structures and algorithms (graphs, trees) | [02](02-Data-Engineering-Fundamentals.md), [28](28-RDS-Aurora-Purpose-Built-DBs.md) |

### Domain 2: Data Store Management (26%)

| Skill | Short description | Primary guides |
|---|---|---|
| 2.1.1 | Storage services for cost and performance | [45](45-Service-Selection-Decision-Guide.md), [23](23-Redshift-Architecture-Table-Design.md), [27](27-DynamoDB.md), [28](28-RDS-Aurora-Purpose-Built-DBs.md), [05](05-S3-Data-Lake-Storage.md) |
| 2.1.2 | Configure stores for access patterns | [23](23-Redshift-Architecture-Table-Design.md), [27](27-DynamoDB.md), [28](28-RDS-Aurora-Purpose-Built-DBs.md), [15](15-Amazon-EMR.md) |
| 2.1.3 | Use cases incl. HNSW on Aurora PostgreSQL, MemoryDB key/value | [28](28-RDS-Aurora-Purpose-Built-DBs.md), [19](19-GenAI-LLMs-Vectors.md) |
| 2.1.4 | Migration tools in pipelines (Transfer Family) | [11](11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| 2.1.5 | Federated queries, materialized views, Spectrum | [24](24-Redshift-Loading-Integration-Sharing.md) |
| 2.1.6 | Manage locks (Redshift, RDS) | [25](25-Redshift-Performance-Operations-Security.md), [28](28-RDS-Aurora-Purpose-Built-DBs.md) |
| 2.1.7 🆕 | Manage open table formats (Iceberg) | [04](04-Open-Table-Formats-S3-Tables.md), [26](26-Amazon-Athena.md) |
| 2.1.8 🆕 | Vector index types (HNSW, IVF) | [19](19-GenAI-LLMs-Vectors.md), [29](29-OpenSearch-Service.md) |
| 2.2.1 | Consume data through catalogs | [13](13-Glue-Data-Catalog-Crawlers.md) |
| 2.2.2 | Technical catalogs (Glue Data Catalog, Hive metastore) | [13](13-Glue-Data-Catalog-Crawlers.md) |
| 2.2.3 | Schema discovery with crawlers | [13](13-Glue-Data-Catalog-Crawlers.md) |
| 2.2.4 | Synchronize partitions with a catalog | [13](13-Glue-Data-Catalog-Crawlers.md), [26](26-Amazon-Athena.md) |
| 2.2.5 | Source/target connections for cataloging | [13](13-Glue-Data-Catalog-Crawlers.md), [12](12-AWS-Glue-ETL.md) |
| 2.2.6 🆕 | Business data catalogs (SageMaker Catalog) | [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| 2.3.1 | Load/unload between S3 and Redshift | [24](24-Redshift-Loading-Integration-Sharing.md) |
| 2.3.2 | S3 Lifecycle tiering | [05](05-S3-Data-Lake-Storage.md), [31](31-Data-Lifecycle-Retention-Resiliency.md) |
| 2.3.3 | S3 Lifecycle expiration | [05](05-S3-Data-Lake-Storage.md), [31](31-Data-Lifecycle-Retention-Resiliency.md) |
| 2.3.4 | S3 versioning and DynamoDB TTL | [05](05-S3-Data-Lake-Storage.md), [27](27-DynamoDB.md), [31](31-Data-Lifecycle-Retention-Resiliency.md) |
| 2.3.5 | Delete data for business and legal requirements | [31](31-Data-Lifecycle-Retention-Resiliency.md), [04](04-Open-Table-Formats-S3-Tables.md) |
| 2.3.6 | Resiliency and availability | [31](31-Data-Lifecycle-Retention-Resiliency.md), [25](25-Redshift-Performance-Operations-Security.md) |
| 2.4.1 | Schema design for Redshift, DynamoDB, Lake Formation | [30](30-Data-Modeling-Schema-Evolution-Lineage.md), [23](23-Redshift-Architecture-Table-Design.md), [27](27-DynamoDB.md), [40](40-Lake-Formation.md) |
| 2.4.2 | Changes in data characteristics | [30](30-Data-Modeling-Schema-Evolution-Lineage.md) |
| 2.4.3 | Schema conversion (SCT, DMS Schema Conversion) | [10](10-DMS-Database-Ingestion.md), [30](30-Data-Modeling-Schema-Evolution-Lineage.md) |
| 2.4.4 | Data lineage (SageMaker ML Lineage Tracking, SageMaker Catalog) | [30](30-Data-Modeling-Schema-Evolution-Lineage.md), [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| 2.4.5 | Indexing, partitioning, compression, optimization | [30](30-Data-Modeling-Schema-Evolution-Lineage.md), [03](03-Data-Formats-Compression.md), [23](23-Redshift-Architecture-Table-Design.md) |
| 2.4.6 🆕 | Vectorization concepts (Bedrock knowledge base) | [19](19-GenAI-LLMs-Vectors.md) |

### Domain 3: Data Operations and Support (22%)

| Skill | Short description | Primary guides |
|---|---|---|
| 3.1.1 | Orchestrate pipelines (MWAA, Step Functions) | [20](20-Step-Functions.md), [21](21-MWAA-Glue-Workflows.md) |
| 3.1.2 | Troubleshoot Amazon managed workflows | [21](21-MWAA-Glue-Workflows.md), [20](20-Step-Functions.md), [32](32-Monitoring-Logging-Troubleshooting.md) |
| 3.1.3 | Call SDKs from code | [36](36-Programming-IaC-CICD.md) |
| 3.1.4 | Processing features of EMR, Redshift, Glue | [12](12-AWS-Glue-ETL.md), [15](15-Amazon-EMR.md), [24](24-Redshift-Loading-Integration-Sharing.md) |
| 3.1.5 | Consume and maintain data APIs | [36](36-Programming-IaC-CICD.md) |
| 3.1.6 | Prepare data (DataBrew, SageMaker Unified Studio) | [14](14-Glue-DataBrew-Data-Preparation.md), [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| 3.1.7 | Query data (Athena) | [26](26-Amazon-Athena.md) |
| 3.1.8 | Automate with Lambda | [17](17-Lambda-for-Data-Pipelines.md) |
| 3.1.9 | Events and schedulers (EventBridge) | [22](22-EventBridge-SNS-SQS.md) |
| 3.2.1 | Visualize (DataBrew, Quick Sight) | [35](35-Analytics-Visualization-Quick-Notebooks.md), [14](14-Glue-DataBrew-Data-Preparation.md) |
| 3.2.2 | Verify and clean data | [35](35-Analytics-Visualization-Quick-Notebooks.md), [14](14-Glue-DataBrew-Data-Preparation.md), [33](33-Data-Quality.md) |
| 3.2.3 | SQL in Redshift and Athena (queries, views) | [34](34-SQL-for-Data-Engineers.md), [26](26-Amazon-Athena.md), [24](24-Redshift-Loading-Integration-Sharing.md) |
| 3.2.4 | Athena notebooks with Apache Spark | [26](26-Amazon-Athena.md), [35](35-Analytics-Visualization-Quick-Notebooks.md) |
| 3.2.5 | Provisioned vs serverless tradeoffs | [02](02-Data-Engineering-Fundamentals.md), [35](35-Analytics-Visualization-Quick-Notebooks.md), [44](44-Cost-Optimization.md) |
| 3.2.6 | Aggregation, rolling averages, grouping, pivoting | [34](34-SQL-for-Data-Engineers.md), [35](35-Analytics-Visualization-Quick-Notebooks.md) |
| 3.3.1 | Extract logs for audits | [43](43-Audit-Logging-CloudTrail-Config.md) |
| 3.3.2 | Logging and monitoring for auditing and traceability | [32](32-Monitoring-Logging-Troubleshooting.md), [43](43-Audit-Logging-CloudTrail-Config.md) |
| 3.3.3 | Alerts from monitoring | [32](32-Monitoring-Logging-Troubleshooting.md), [22](22-EventBridge-SNS-SQS.md) |
| 3.3.4 | Troubleshoot performance | [32](32-Monitoring-Logging-Troubleshooting.md), [25](25-Redshift-Performance-Operations-Security.md), [16](16-Apache-Spark-Essentials.md) |
| 3.3.5 | Track API calls with CloudTrail | [43](43-Audit-Logging-CloudTrail-Config.md) |
| 3.3.6 | Troubleshoot and maintain pipelines (Glue, EMR) | [32](32-Monitoring-Logging-Troubleshooting.md), [12](12-AWS-Glue-ETL.md), [15](15-Amazon-EMR.md) |
| 3.3.7 | CloudWatch Logs for application data | [32](32-Monitoring-Logging-Troubleshooting.md) |
| 3.3.8 | Analyze logs (Athena, EMR, OpenSearch, Logs Insights) | [43](43-Audit-Logging-CloudTrail-Config.md), [29](29-OpenSearch-Service.md), [26](26-Amazon-Athena.md) |
| 3.4.1 | Data quality checks during processing | [33](33-Data-Quality.md) |
| 3.4.2 | Define data quality rules | [33](33-Data-Quality.md), [14](14-Glue-DataBrew-Data-Preparation.md) |
| 3.4.3 | Investigate data consistency | [33](33-Data-Quality.md), [14](14-Glue-DataBrew-Data-Preparation.md) |
| 3.4.4 | Data sampling techniques | [33](33-Data-Quality.md) |
| 3.4.5 | Data skew mechanisms | [33](33-Data-Quality.md), [16](16-Apache-Spark-Essentials.md) |

### Domain 4: Data Security and Governance (18%)

| Skill | Short description | Primary guides |
|---|---|---|
| 4.1.1 | Update VPC security groups | [38](38-Networking-for-Data-Pipelines.md) |
| 4.1.2 | IAM groups, roles, endpoints, services | [37](37-IAM-for-Data-Engineers.md), [38](38-Networking-for-Data-Pipelines.md) |
| 4.1.3 | Create and rotate credentials (Secrets Manager) | [39](39-Encryption-Key-Management.md) |
| 4.1.4 | IAM roles for Lambda, API Gateway, CLI, CloudFormation | [37](37-IAM-for-Data-Engineers.md) |
| 4.1.5 | Policies on S3 Access Points and PrivateLink | [37](37-IAM-for-Data-Engineers.md), [38](38-Networking-for-Data-Pipelines.md) |
| 4.1.6 | Managed vs unmanaged services | [02](02-Data-Engineering-Fundamentals.md) |
| 4.1.7 🆕 | Domains, domain units, projects in SageMaker Unified Studio | [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| 4.2.1 | Custom IAM policies | [37](37-IAM-for-Data-Engineers.md) |
| 4.2.2 | Store credentials (Secrets Manager, Parameter Store) | [39](39-Encryption-Key-Management.md) |
| 4.2.3 | Database users, groups, roles (Redshift) | [25](25-Redshift-Performance-Operations-Security.md) |
| 4.2.4 | Lake Formation permissions (Redshift, EMR, Athena, S3) | [40](40-Lake-Formation.md) |
| 4.2.5 | Role-based, tag-based, attribute-based authorization | [37](37-IAM-for-Data-Engineers.md), [40](40-Lake-Formation.md) |
| 4.2.6 | Least-privilege policies | [37](37-IAM-for-Data-Engineers.md) |
| 4.3.1 | Masking and anonymization | [42](42-Privacy-PII-Masking-Sovereignty.md), [25](25-Redshift-Performance-Operations-Security.md), [14](14-Glue-DataBrew-Data-Preparation.md) |
| 4.3.2 | Encrypt/decrypt with KMS | [39](39-Encryption-Key-Management.md) |
| 4.3.3 | Encryption across account boundaries | [39](39-Encryption-Key-Management.md) |
| 4.3.4 | Encryption in transit or before transit | [39](39-Encryption-Key-Management.md) |
| 4.4.1 | CloudTrail API tracking | [43](43-Audit-Logging-CloudTrail-Config.md) |
| 4.4.2 | CloudWatch Logs for application logs | [32](32-Monitoring-Logging-Troubleshooting.md), [43](43-Audit-Logging-CloudTrail-Config.md) |
| 4.4.3 | CloudTrail Lake centralized queries | [43](43-Audit-Logging-CloudTrail-Config.md) |
| 4.4.4 | Analyze logs (Athena, Logs Insights, OpenSearch) | [43](43-Audit-Logging-CloudTrail-Config.md) |
| 4.4.5 | Integrate services for logging (EMR for large volumes) | [43](43-Audit-Logging-CloudTrail-Config.md), [15](15-Amazon-EMR.md) |
| 4.5.1 | Permissions for data sharing (Redshift) | [24](24-Redshift-Loading-Integration-Sharing.md), [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| 4.5.2 | PII identification (Macie with Lake Formation) | [42](42-Privacy-PII-Masking-Sovereignty.md), [40](40-Lake-Formation.md) |
| 4.5.3 | Prevent backups/replication to disallowed Regions | [42](42-Privacy-PII-Masking-Sovereignty.md) |
| 4.5.4 | View configuration changes (AWS Config) | [43](43-Audit-Logging-CloudTrail-Config.md), [42](42-Privacy-PII-Masking-Sovereignty.md) |
| 4.5.5 | Maintain data sovereignty | [42](42-Privacy-PII-Masking-Sovereignty.md) |
| 4.5.6 🆕 | Data access via SageMaker Catalog projects | [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| 4.5.7 🆕 | Governance frameworks and data sharing patterns | [41](41-SageMaker-Unified-Studio-Catalog-Governance.md) |

## How DEA-C01 questions are built

Most questions have the same four parts. Learn to see them and you'll work much faster:

1. **Context:** "A retail company ingests clickstream events from its website…"
2. **Current state or pain:** "…the Lambda consumer is falling behind and IteratorAge keeps growing…"
3. **Constraints:** "…the solution must preserve ordering per session and must not require code changes…"
4. **Qualifier:** "Which solution meets these requirements with the **LEAST operational overhead**?"

The qualifier chooses between the two answers that "work". Here are the usual ones and what each tends to favor:

| Qualifier | Usually favors |
|---|---|
| *LEAST operational overhead* / *fewest management tasks* | The most managed or serverless option that meets every requirement: zero-ETL, Firehose, Glue, Athena, Redshift Serverless, Step Functions, managed features over custom code |
| *MOST cost-effective* | Pay-per-use for spiky or intermittent work, reserved or provisioned for steady 24×7 load, Spot for fault-tolerant compute, columnar + partitioned + compressed data, S3 lifecycle tiers |
| *Near real-time* | Firehose (buffered delivery in seconds to minutes), Redshift streaming ingestion, Glue streaming |
| *Real-time* / *sub-second* / *milliseconds* | Kinesis Data Streams or MSK with Lambda or Managed Flink consumers. DynamoDB or MemoryDB for serving |
| *Without code changes* / *existing Kafka / Spark / Airflow* | The managed open-source twin: MSK, EMR, MWAA |
| *Least privilege* / *fine-grained* | Scoped IAM policies, Lake Formation column/row/cell permissions, Redshift RLS/DDM |

## Suggested study-time split

| Area | Share of your time | Why |
|---|---|---|
| Glue (ETL + Catalog), Redshift, Athena, Kinesis/Firehose, S3 | **~40%** | These five show up across every domain |
| Orchestration, Lambda, EMR/Spark, DMS | **~20%** | Domain 1 depth |
| Security and governance (IAM, Lake Formation, KMS, logging, privacy) | **~20%** | Domain 4 and parts of Domain 3 |
| Data modeling, lifecycle, quality, SQL | **~12%** | Domain 2 and 3 fundamentals |
| v1.1 additions (Iceberg/S3 Tables, vectors, LLMs, SageMaker Catalog) | **~8%** | New content with few prep resources elsewhere |

Now open the [study index](00-README.md) and start with the fundamentals.
