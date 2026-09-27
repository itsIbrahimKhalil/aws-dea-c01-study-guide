# DEA-C01 Topic Guides — Study Index

46 self-contained topic guides plus an [exam blueprint](01-Exam-Blueprint.md) for the **AWS Certified Data Engineer – Associate (DEA-C01)** exam, **exam guide v1.1**. Every guide has the same shape:

1. **Exam map**: which domain, task and skill IDs the guide covers, how heavily it's tested, and how long it takes to read
2. **The idea**: the topic explained from zero, built on one real-world analogy
3. **Core concepts**: the facts, numbers and decision rules the exam tests, with **THE trap** callouts for the wrong answers it's designed to catch
4. **Question patterns**: realistic exam scenarios, each with its answer and the signal words that decide it
5. **Pocket card**: a keyword → answer table for fast review

Markers used throughout: 🔥 exam weight · ⚠️ **2026 status** (renamed, retired or closed to new customers) · 🆕 **new in exam guide v1.1**.

## Suggested reading order

### 🧭 Orientation (start here)
| # | Guide | Owns the questions about |
|---|---|---|
| 01 | [Exam Blueprint](01-Exam-Blueprint.md) | Format, scoring, v1.1 changes, AWS changes since, **every skill mapped to a guide** |
| 02 | [Data Engineering Fundamentals](02-Data-Engineering-Fundamentals.md) | Lake vs warehouse vs lakehouse, batch vs streaming, stateful vs stateless, replay, fan-out, provisioned vs serverless |
| 03 | [Data Formats & Compression](03-Data-Formats-Compression.md) | Parquet/ORC/Avro/CSV/JSON, splittable codecs, small-files problem, CSV → Parquet paths |

### 🪣 The data lake (heavily tested — do these early)
| # | Guide | Owns the questions about |
|---|---|---|
| 05 | [S3 for Data Lakes](05-S3-Data-Lake-Storage.md) | Storage classes, lifecycle, versioning, replication, Object Lock, prefixes & performance, events |
| 12 | [AWS Glue ETL](12-AWS-Glue-ETL.md) | Job types, workers/DPUs, Flex, bookmarks, pushdown predicates, connections, troubleshooting |
| 13 | [Glue Data Catalog & Crawlers](13-Glue-Data-Catalog-Crawlers.md) | Crawlers, partitions, partition projection, Schema Registry, catalog federation |
| 26 | [Amazon Athena](26-Amazon-Athena.md) | Cost/performance tuning, CTAS, workgroups, federated queries, Spark notebooks, Iceberg DML |
| 04 | [Open Table Formats & S3 Tables](04-Open-Table-Formats-S3-Tables.md) 🆕 | Iceberg (ACID, time travel, compaction), Hudi, Delta, S3 table buckets |

### 🌊 Streaming ingestion
| # | Guide | Owns the questions about |
|---|---|---|
| 06 | [Kinesis Data Streams](06-Kinesis-Data-Streams.md) | Shards, partition keys, on-demand vs provisioned, enhanced fan-out, Lambda tuning, throttling |
| 07 | [Amazon Data Firehose](07-Amazon-Data-Firehose.md) | Buffering, format conversion, dynamic partitioning, Redshift/OpenSearch/Iceberg delivery |
| 08 | [Amazon MSK (Kafka)](08-Amazon-MSK-Kafka.md) | Kafka concepts, provisioned/Express/Serverless, MSK Connect, Kinesis vs MSK |
| 09 | [Managed Service for Apache Flink](09-Managed-Service-for-Apache-Flink.md) | Windows, watermarks, state, exactly-once, choosing a stream processor |

### 🚚 Batch, database & SaaS ingestion
| # | Guide | Owns the questions about |
|---|---|---|
| 10 | [DMS & Database Ingestion](10-DMS-Database-Ingestion.md) | Full load + CDC, SCT vs DMS Schema Conversion, zero-ETL, snapshot export |
| 11 | [DataSync, Transfer Family, Snow & AppFlow](11-DataSync-Transfer-Family-Snow-AppFlow.md) | Online/offline/partner/SaaS transfer, consuming APIs, IP allowlisting |

### ⚙️ Processing & compute
| # | Guide | Owns the questions about |
|---|---|---|
| 15 | [Amazon EMR](15-Amazon-EMR.md) | Node types, Spot, instance fleets, EMR Serverless, EMR on EKS, security, logs |
| 16 | [Apache Spark Essentials](16-Apache-Spark-Essentials.md) | Partitions, shuffles, **data skew & salting**, broadcast joins, OOM fixes |
| 17 | [Lambda for Data Pipelines](17-Lambda-for-Data-Pipelines.md) | Limits, concurrency, event source mappings, /tmp vs EFS, **AWS SAM** |
| 14 | [Glue DataBrew & Data Preparation](14-Glue-DataBrew-Data-Preparation.md) | Recipes, profile jobs, DQ rules, PII transforms, Data Wrangler |
| 18 | [Containers, Batch & EC2](18-Containers-Batch-EC2-Compute.md) | ECS/EKS/Fargate, AWS Batch, choosing compute, optimizing containers |
| 19 | [GenAI, LLMs & Vectors](19-GenAI-LLMs-Vectors.md) 🆕 | LLMs in pipelines, embeddings, Bedrock Knowledge Bases, **HNSW vs IVF**, vector stores, Amazon Q, Kendra |

### 🎼 Orchestration & events
| # | Guide | Owns the questions about |
|---|---|---|
| 20 | [Step Functions](20-Step-Functions.md) | Standard vs Express, .sync / callbacks, Retry/Catch, Distributed Map |
| 21 | [MWAA & Glue Workflows](21-MWAA-Glue-Workflows.md) | Airflow DAGs, MWAA troubleshooting, MWAA Serverless, Glue triggers, orchestration choice |
| 22 | [EventBridge, SNS & SQS](22-EventBridge-SNS-SQS.md) | S3 events vs EventBridge, Scheduler, Pipes, alerts, buffering |

### 🏛️ Amazon Redshift (the most-tested data store)
| # | Guide | Owns the questions about |
|---|---|---|
| 23 | [Redshift Architecture & Table Design](23-Redshift-Architecture-Table-Design.md) | RA3/Serverless, distribution styles, sort keys, encodings, SUPER |
| 24 | [Redshift Loading, Integration & Sharing](24-Redshift-Loading-Integration-Sharing.md) | COPY/UNLOAD, Spectrum, federated queries, streaming ingestion, zero-ETL, MVs, data sharing, Data API |
| 25 | [Redshift Performance, Operations & Security](25-Redshift-Performance-Operations-Security.md) | WLM, concurrency scaling, VACUUM/ANALYZE, **locks**, snapshots, RBAC/RLS/DDM, audit logs |

### 🗄️ Other data stores
| # | Guide | Owns the questions about |
|---|---|---|
| 27 | [DynamoDB](27-DynamoDB.md) | Key design, hot partitions, GSI/LSI, capacity & throttling, Streams, TTL, export/import |
| 28 | [RDS, Aurora & Purpose-Built DBs](28-RDS-Aurora-Purpose-Built-DBs.md) | Sources for pipelines, **locks**, pgvector, MemoryDB, DocumentDB/Keyspaces/Neptune |
| 29 | [OpenSearch Service](29-OpenSearch-Service.md) | Shards, UltraWarm/cold, ISM, Serverless, ingestion, log analytics, vector engine |

### 📐 Modeling, lifecycle & quality
| # | Guide | Owns the questions about |
|---|---|---|
| 30 | [Data Modeling, Schema Evolution & Lineage](30-Data-Modeling-Schema-Evolution-Lineage.md) | Star schema, SCDs, schema evolution & compatibility modes, lineage |
| 31 | [Data Lifecycle, Retention & Resiliency](31-Data-Lifecycle-Retention-Resiliency.md) | Hot/cold tiering, legal deletion, AWS Backup, RPO/RTO |
| 33 | [Data Quality](33-Data-Quality.md) | Glue Data Quality (DQDL), DataBrew rules, sampling, skew |
| 34 | [SQL for Data Engineers](34-SQL-for-Data-Engineers.md) | Joins, window functions, rolling averages, pivots, MERGE/SCD2, stored procedures, optimization |

### 📊 Operations & analytics
| # | Guide | Owns the questions about |
|---|---|---|
| 32 | [Monitoring, Logging & Troubleshooting](32-Monitoring-Logging-Troubleshooting.md) | CloudWatch metrics/alarms/Logs/Insights, Grafana, troubleshooting playbooks |
| 35 | [Analytics & Visualization](35-Analytics-Visualization-Quick-Notebooks.md) | Amazon Quick / Quick Sight (SPICE, RLS), notebooks, cleaning data |
| 36 | [Programming, IaC & CI/CD](36-Programming-IaC-CICD.md) | CloudFormation/CDK, CodePipeline/CodeBuild, Git, SDK patterns, API Gateway data APIs |

### 🔐 Security & governance
| # | Guide | Owns the questions about |
|---|---|---|
| 37 | [IAM for Data Engineers](37-IAM-for-Data-Engineers.md) | Service roles, PassRole, least privilege, ABAC, cross-account, access points |
| 38 | [Networking for Data Pipelines](38-Networking-for-Data-Pipelines.md) | Security groups, VPC endpoints, PrivateLink, Glue/Redshift networking, IP allowlisting |
| 39 | [Encryption, KMS & Secrets](39-Encryption-Key-Management.md) | Key types, envelope encryption, cross-account keys, per-service encryption, rotation |
| 40 | [Lake Formation](40-Lake-Formation.md) | Column/row/cell permissions, LF-Tags, cross-account sharing, hybrid access |
| 41 | [SageMaker Unified Studio, Catalog & Governance](41-SageMaker-Unified-Studio-Catalog-Governance.md) 🆕 | Domains/units/projects, business catalog, subscriptions, data mesh, sharing patterns |
| 42 | [Privacy, PII, Masking & Sovereignty](42-Privacy-PII-Masking-Sovereignty.md) | Macie, masking vs tokenization vs salted hashing, Region restrictions |
| 43 | [Audit Logging: CloudTrail, Config & more](43-Audit-Logging-CloudTrail-Config.md) | Data events, CloudTrail Lake, AWS Config, analyzing logs |

### 🎯 Final review (read last, and again on exam morning)
| # | Guide | Owns the questions about |
|---|---|---|
| 44 | [Cost Optimization](44-Cost-Optimization.md) | Budgets, Cost Explorer, the cost lever for every service |
| 45 | [Service Selection Decision Guide](45-Service-Selection-Decision-Guide.md) | Every "which service?" decision in one place, plus flowcharts |
| 46 | [Gap-Fill Services](46-GapFill-Services.md) | Services that each show up for one or two questions, plus red-flag distractors |
| 47 | [Exam Traps & Key Patterns](47-Exam-Traps-Key-Patterns.md) | ⭐ The greatest-hits trap list and exam technique |

## Short on time?

**The 12 guides that cover the most questions:** [12 Glue ETL](12-AWS-Glue-ETL.md) · [13 Catalog & Crawlers](13-Glue-Data-Catalog-Crawlers.md) · [24 Redshift Loading](24-Redshift-Loading-Integration-Sharing.md) · [23 Redshift Design](23-Redshift-Architecture-Table-Design.md) · [26 Athena](26-Amazon-Athena.md) · [06 Kinesis Data Streams](06-Kinesis-Data-Streams.md) · [07 Firehose](07-Amazon-Data-Firehose.md) · [05 S3](05-S3-Data-Lake-Storage.md) · [20 Step Functions](20-Step-Functions.md) · [40 Lake Formation](40-Lake-Formation.md) · [45 Service Selection](45-Service-Selection-Decision-Guide.md) · [47 Exam Traps](47-Exam-Traps-Key-Patterns.md)

**If you already hold SAA-C03:** skim 05, 22, 37, 38 and 39 (their pocket cards are enough), then spend your time on Glue, Redshift, Athena, streaming, Lake Formation and the 🆕 guides.

## How to use these guides

1. **First pass:** read in the order above. Let the analogies do the heavy lifting and don't try to memorize yet.
2. **Second pass:** cover the right-hand column of each **pocket card** and quiz yourself from the keywords.
3. **Practice exams:** take [Practice Exam 01](../practice-exams/practice-exam-01.md) after your first pass. For every miss, go back to that guide's **Question patterns** section. The explanation for each pattern shows which signal words you skipped.
4. **Final week:** [Practice Exam 02](../practice-exams/practice-exam-02.md), the [v1.1 drill](../practice-exams/v1.1-new-topics-drill.md), the [cheat sheets](../cheat-sheets/) and guide 47.
5. **Exam morning:** guide 47 and the pocket cards only. Don't start anything new.
