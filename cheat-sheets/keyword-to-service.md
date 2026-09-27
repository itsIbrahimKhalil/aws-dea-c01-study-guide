# Keyword → Service — the master pocket card

This sheet merges the pocket cards from every topic guide into one deduplicated set. DEA-C01 questions are long, but the answer usually hangs on one or two **signal phrases**: *"replay"*, *"least operational overhead"*, *"Kafka"*, *"row-level"*, *"without managing servers"*. Learn to spot those phrases and you'll find the answer quickly.

**How to self-quiz:**
1. Cover the **Answer** and **Guide** columns with a sheet of paper (or shrink the browser window).
2. Read each signal and say the answer out loud, including *why* the runner-up loses.
3. Mark any row you miss and re-read that guide's pocket card. Do one theme per sitting, then shuffle themes the week before the exam.

Qualifiers that change the answer are covered in [Guide 47 — Exam Traps & Key Patterns](../topic-guides/47-Exam-Traps-Key-Patterns.md). Exact limits are in [numbers-to-know.md](numbers-to-know.md).

---

## 1. Ingestion & streaming

| Signal keyword | Answer | Guide |
|---|---|---|
| Multiple consumers + replay + ordering per key | Kinesis Data Streams | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Unpredictable traffic, no shard management | KDS on-demand | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Consumers throttled after adding another app | Enhanced fan-out | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| One shard throttled, stream mostly idle | Hot partition key → better key / split that shard | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Find the hot shard | Enhanced shard-level metrics | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Many tiny records, beat 1,000 records/s | KPL aggregation + collection | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Consumer lag metric | `GetRecords.IteratorAgeMilliseconds` | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Lambda consumer lagging, no errors | ParallelizationFactor (1–10) | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Poison record blocks shard | Bisect batch + bounded retries/age + on-failure destination | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Keep the full failed batch | On-failure destination S3 (SQS/SNS get metadata) | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| KCL coordination / checkpoints | DynamoDB lease table | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Cross-account stream reader | Resource-based policy on stream + CMK access | [06](../topic-guides/06-Kinesis-Data-Streams.md) |
| Stream → S3/Redshift/OpenSearch/Splunk, no code | Amazon Data Firehose | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| "Kinesis Data Firehose" in the question | Same product (renamed Feb 2024) | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| JSON → Parquet/ORC in flight | Firehose record format conversion + Glue table | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| CSV → Parquet in Firehose | Lambda transform (CSV→JSON) + format conversion | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| Partition S3 output by a field in the record | Firehose dynamic partitioning (creation time only) | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| Firehose transform failures | `processing-failed/` S3 prefix | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| Keep raw originals alongside transformed | Source record backup | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| Firehose → Redshift can't connect | Publicly accessible cluster + Firehose Region CIDR allowlisted | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| Stream → Iceberg / S3 Tables, no code | Firehose Iceberg destination | [07](../topic-guides/07-Amazon-Data-Firehose.md) |
| Firehose falling behind | `DataFreshness` metric | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Kafka API, existing Kafka apps, minimal code change | Amazon MSK | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| Kafka, no capacity planning | MSK Serverless (IAM auth only) | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| Kafka high throughput, elastic, no storage mgmt | MSK Express brokers | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| Long, cheap Kafka retention | MSK tiered storage | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| Consumers idle, lag not improving | More partitions (consumers ≤ partitions) | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| Managed Kafka connectors (Debezium, S3 sink) | MSK Connect | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| Cross-account private Kafka clients | MSK multi-VPC private connectivity | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| Cross-cluster/Region Kafka replication, managed | MSK Replicator | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| MSK topic → S3, no code | Firehose MSK source | [08](../topic-guides/08-Amazon-MSK-Kafka.md) |
| Stateful windows, joins, CEP, fraud in real time | Managed Service for Apache Flink | [09](../topic-guides/09-Managed-Service-for-Apache-Flink.md) |
| "Kinesis Data Analytics for SQL" | ⚠️ Discontinued → distractor; use Managed Flink | [09](../topic-guides/09-Managed-Service-for-Apache-Flink.md) |
| Late / out-of-order events | Event time + watermarks | [09](../topic-guides/09-Managed-Service-for-Apache-Flink.md) |
| Restart Flink app without losing state | Restore from snapshot (savepoint) | [09](../topic-guides/09-Managed-Service-for-Apache-Flink.md) |
| Interactive SQL on a live stream | Managed Flink Studio notebook | [09](../topic-guides/09-Managed-Service-for-Apache-Flink.md) |
| Stateless per-record filter/mask before S3 | Firehose + Lambda transform (not Flink) | [09](../topic-guides/09-Managed-Service-for-Apache-Flink.md) |
| DB migration, minimal downtime | DMS full load + CDC | [10](../topic-guides/10-DMS-Database-Ingestion.md) |
| No instance sizing, spiky replication | DMS Serverless | [10](../topic-guides/10-DMS-Database-Ingestion.md) |
| Convert schema + stored procedures | DMS Schema Conversion (ex-SCT) | [10](../topic-guides/10-DMS-Database-Ingestion.md) |
| Row-level correctness after migration | DMS data validation | [10](../topic-guides/10-DMS-Database-Ingestion.md) |
| Source-side vs target-side lag | `CDCLatencySource` vs `CDCLatencyTarget` | [10](../topic-guides/10-DMS-Database-Ingestion.md) |
| CDC into Iceberg lakehouse | DMS → S3 → Glue `MERGE INTO` (no native Iceberg target) | [10](../topic-guides/10-DMS-Database-Ingestion.md) |
| Aurora/RDS/DynamoDB → Redshift, least ops | Zero-ETL integration | [10](../topic-guides/10-DMS-Database-Ingestion.md) |
| Analytics copy of RDS, zero prod load | Snapshot export to S3 (Parquet) | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| NFS/SMB/HDFS share → S3, recurring, verified | DataSync | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| cron + `aws s3 sync` script | Trap → DataSync | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| Partners send files via SFTP/FTPS/AS2 | Transfer Family | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| Partner must allowlist our SFTP IPs | Transfer Family VPC endpoint + Elastic IPs | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| Petabytes, no usable bandwidth | Snow Family ⚠️ (closed to new) / Data Transfer Terminal | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| Snowball data → Glacier | Import to S3, then Lifecycle rule | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| SaaS (Salesforce etc.) → S3/Redshift, no code | AppFlow | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| SaaS → Redshift/lakehouse with CDC | Glue zero-ETL | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| Pull a REST API on a schedule | EventBridge Scheduler → Lambda (+ Secrets Manager, backoff) | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| HTTP → Kinesis with no Lambda | API Gateway REST service integration | [36](../topic-guides/36-Programming-IaC-CICD.md) |
| Global uploads to one bucket, faster | S3 Transfer Acceleration | [11](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md) |
| Logs → lake continuously | CloudWatch Logs subscription filter → Firehose → S3 | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |

## 2. Processing & compute

| Signal keyword | Answer | Guide |
|---|---|---|
| Serverless Spark ETL, least ops | AWS Glue | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Non-urgent Glue job, cheapest | Glue Flex | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Variable load, stop over-provisioning | Glue Auto Scaling | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Tiny Python task (download a small file) | Glue Python shell (0.0625 DPU) or Lambda | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Recurring OOM in Glue | R-type (memory-optimized) workers or bigger G workers | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Process only new files/rows | Glue job bookmarks | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Bookmarks enabled but full reprocess | Missing `job.commit()` or `transformation_ctx` | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Read only some partitions in Glue | `push_down_predicate` | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Many small input files in Glue | `groupFiles` + `groupSize` | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Slow JDBC read, idle workers | `hashfield` / `hashpartitions` | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Mixed types in a column | DynamicFrame `ResolveChoice` | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Flatten nested JSON for Redshift | Glue `Relationalize` | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Glue job alerting on failure | EventBridge Glue Job State Change → SNS | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| AccessDenied creating a job with a role | `iam:PassRole` | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Natural language → Glue code | Amazon Q data integration in Glue | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Open-source control: HBase, Trino, custom Spark | EMR on EC2 | [15](../topic-guides/15-Amazon-EMR.md) |
| Spark/Hive without clusters | EMR Serverless | [15](../topic-guides/15-Amazon-EMR.md) |
| Spark on existing Kubernetes | EMR on EKS | [15](../topic-guides/15-Amazon-EMR.md) |
| EMR node safe for Spot | Task nodes (On-Demand primary + core) | [15](../topic-guides/15-Amazon-EMR.md) |
| Run steps then shut down | Transient cluster + auto-termination | [15](../topic-guides/15-Amazon-EMR.md) |
| Least-effort EMR autoscaling | Managed scaling | [15](../topic-guides/15-Amazon-EMR.md) |
| Install software on every node | Bootstrap action | [15](../topic-guides/15-Amazon-EMR.md) |
| Per-job permissions on shared cluster | EMR runtime roles (+ Lake Formation) | [15](../topic-guides/15-Amazon-EMR.md) |
| Tables vanish with transient cluster | Use Glue Data Catalog as metastore | [15](../topic-guides/15-Amazon-EMR.md) |
| Straggler tasks, one key dominates | Data skew → AQE skew join / salting / broadcast | [16](../topic-guides/16-Apache-Spark-Essentials.md) |
| Small table joined to a big one | Broadcast join | [16](../topic-guides/16-Apache-Spark-Essentials.md) |
| Fewer output files, no shuffle | `coalesce` | [16](../topic-guides/16-Apache-Spark-Essentials.md) |
| Even out partitions | `repartition` | [16](../topic-guides/16-Apache-Spark-Essentials.md) |
| "Container killed … memory limits" | Raise `memoryOverhead` | [16](../topic-guides/16-Apache-Spark-Essentials.md) |
| Driver OOM | Avoid `collect()` / `toPandas()` on big data | [16](../topic-guides/16-Apache-Spark-Essentials.md) |
| Small, event-driven, < 15 min | Lambda | [17](../topic-guides/17-Lambda-for-Data-Pipelines.md) |
| Lambda throttles rising | Concurrency (reserved/Regional), not memory | [17](../topic-guides/17-Lambda-for-Data-Pipelines.md) |
| Cap a function's concurrency | Reserved concurrency | [17](../topic-guides/17-Lambda-for-Data-Pipelines.md) |
| Eliminate cold starts | Provisioned concurrency (or SnapStart) | [17](../topic-guides/17-Lambda-for-Data-Pipelines.md) |
| Partial batch failure from SQS/Kinesis | `ReportBatchItemFailures` | [17](../topic-guides/17-Lambda-for-Data-Pipelines.md) |
| Big shared model files for Lambda | EFS mount (or container image) | [17](../topic-guides/17-Lambda-for-Data-Pipelines.md) |
| Dependencies > 250 MB | Lambda container image | [17](../topic-guides/17-Lambda-for-Data-Pipelines.md) |
| Lambda exhausts DB connections | RDS Proxy | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| Function triggers itself on S3 | Separate bucket or prefix | [17](../topic-guides/17-Lambda-for-Data-Pipelines.md) |
| Containerized batch, retries, Spot, queues | AWS Batch | [18](../topic-guides/18-Containers-Batch-EC2-Compute.md) |
| Fan out one program over N inputs | Batch array job | [18](../topic-guides/18-Containers-Batch-EC2-Compute.md) |
| Serverless containers | Fargate (ECS/EKS) | [18](../topic-guides/18-Containers-Batch-EC2-Compute.md) |
| Container calls AWS APIs | ECS task role (execution role = pull/logs) | [18](../topic-guides/18-Containers-Batch-EC2-Compute.md) |
| Per-pod IAM on EKS | Pod Identity / IRSA | [18](../topic-guides/18-Containers-Batch-EC2-Compute.md) |
| No-code cleaning by analysts | Glue DataBrew | [14](../topic-guides/14-Glue-DataBrew-Data-Preparation.md) |
| Column stats, outliers, distributions | DataBrew profile job | [14](../topic-guides/14-Glue-DataBrew-Data-Preparation.md) |
| Fuzzy dedup, no common key | Glue FindMatches | [14](../topic-guides/14-Glue-DataBrew-Data-Preparation.md) |
| ML feature prep, visual | SageMaker Data Wrangler (in Canvas) | [14](../topic-guides/14-Glue-DataBrew-Data-Preparation.md) |
| Classify/extract/summarize text in a pipeline | Bedrock `InvokeModel` / `Converse` | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Millions of prompts offline, cheapest | Bedrock batch inference | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Docs/images/audio → structured fields | Bedrock Data Automation | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| LLM call from warehouse SQL | Redshift ML with Bedrock external model | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Bedrock `ThrottlingException` | Backoff + cross-Region inference / batch / quota | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Mask PII in prompts/responses | Bedrock Guardrails sensitive info filter | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |

## 3. Glue Data Catalog & crawlers

| Signal keyword | Answer | Guide |
|---|---|---|
| Hive-compatible metastore shared by Athena/EMR/Glue | Glue Data Catalog | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Discover schema of new data | Crawler | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Header row read as data, `col0…` | Custom CSV classifier "has heading" | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| JSON records inside an array | JSON classifier with JSONPath `$[*]` | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Custom log format | Grok classifier | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| `partition_0`, `partition_1` | Non-Hive paths → Hive-style `key=value` | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Crawler creates many tiny tables | Mixed formats/schemas under one prefix | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Crawler overwrites curated types | Schema change policy: add new columns only / log | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Fastest incremental crawl | S3 event mode (events → SQS) | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| `HIVE_PARTITION_SCHEMA_MISMATCH` | Partitions inherit schema from table | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Crawler AccessDenied on SSE-KMS | Role needs `kms:Decrypt` | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Millions of partitions, slow planning | Partition index (max 3/table) or projection | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Add partitions from the job itself | `enableUpdateCatalog` + `partitionKeys` | [12](../topic-guides/12-AWS-Glue-ETL.md) |
| Stream schema contract | Glue Schema Registry (Avro/JSON Schema/Protobuf) | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Consumers upgrade first | BACKWARD (default) | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Producers upgrade first | FORWARD | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| No new schema versions | DISABLED (not NONE) | [30](../topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md) |
| One view usable by many engines | Glue Data Catalog multi-dialect view | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |
| Better join plans, no query change | Column statistics | [13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md) |

## 4. Athena, formats & table formats

| Signal keyword | Answer | Guide |
|---|---|---|
| Ad hoc serverless SQL on S3, pay per query | Athena ($5/TB scanned) | [26](../topic-guides/26-Amazon-Athena.md) |
| Cut Athena cost #1 | Partition + Parquet/ORC + compression + fewer columns | [26](../topic-guides/26-Amazon-Athena.md) |
| `LIMIT` to save money | Doesn't cut bytes scanned | [26](../topic-guides/26-Amazon-Athena.md) |
| New data invisible / zero rows | Register partitions or use partition projection | [26](../topic-guides/26-Amazon-Athena.md) |
| Partitions with zero maintenance (Athena) | Partition projection (Athena-only) | [26](../topic-guides/26-Amazon-Athena.md) |
| Function on partition column | Breaks pruning; filter raw values | [26](../topic-guides/26-Amazon-Athena.md) |
| Cancel queries over X bytes | Workgroup per-query data usage control | [26](../topic-guides/26-Amazon-Athena.md) |
| Alert on team's daily scans | Workgroup data usage alert (SNS) | [26](../topic-guides/26-Amazon-Athena.md) |
| Predictable concurrency and cost | Capacity reservation (4-DPU min) | [26](../topic-guides/26-Amazon-Athena.md) |
| Same query repeated, data static | Query result reuse | [26](../topic-guides/26-Amazon-Athena.md) |
| CSV → Parquet with one SQL statement | CTAS | [26](../topic-guides/26-Amazon-Athena.md) |
| Export query results as Parquet, no table | UNLOAD | [26](../topic-guides/26-Amazon-Athena.md) |
| Join S3 with DynamoDB/RDS ad hoc | Athena federated query | [26](../topic-guides/26-Amazon-Athena.md) |
| Serverless PySpark notebooks | Athena for Apache Spark | [26](../topic-guides/26-Amazon-Athena.md) |
| Bad values break a cast | `try_cast` | [34](../topic-guides/34-SQL-for-Data-Engineers.md) |
| Query CloudTrail/ALB/VPC logs in S3 | Athena + partition projection | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| Filter rows inside one object with SQL | Athena (S3 Select ⚠️ closed) | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Analytics, few columns | Parquet (or ORC) | [03](../topic-guides/03-Data-Formats-Compression.md) |
| Whole records, streaming, schema evolution | Avro | [03](../topic-guides/03-Data-Formats-Compression.md) |
| JSON for Athena/Glue | JSON Lines (one object per line) | [03](../topic-guides/03-Data-Formats-Compression.md) |
| One huge `.gz` = one task | GZIP not splittable → split files / Parquet | [03](../topic-guides/03-Data-Formats-Compression.md) |
| Splittable text codec | BZIP2 | [03](../topic-guides/03-Data-Formats-Compression.md) |
| Quoted CSV fields with commas | OpenCSVSerDe | [03](../topic-guides/03-Data-Formats-Compression.md) |
| Malformed JSON breaks query | OpenX JSON SerDe `ignore.malformed.json` | [03](../topic-guides/03-Data-Formats-Compression.md) |
| Renamed Parquet column returns NULLs | Parquet reads by name → view / Iceberg | [30](../topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md) |
| Millions of tiny files | Compaction + bigger buffers | [03](../topic-guides/03-Data-Formats-Compression.md) |
| High-cardinality filter column | Bucketing / sorting, not partitioning | [30](../topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md) |
| ACID updates/deletes on S3 | Apache Iceberg | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| Upsert / CDC apply on the lake | Iceberg `MERGE INTO` | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| Query the table as of a past date | Iceberg time travel | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| Safe rename/drop/reorder columns | Iceberg schema evolution | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| Change partitioning without rewrite | Iceberg partition evolution | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| Iceberg small files / old snapshots | `OPTIMIZE` / `VACUUM` or table optimizers | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| Managed Iceberg, least overhead | Amazon S3 Tables | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| Record key + precombine, incremental queries | Apache Hudi | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| Existing Databricks / `_delta_log` | Delta Lake | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |
| "Governed tables" | ⚠️ Discontinued → Iceberg | [04](../topic-guides/04-Open-Table-Formats-S3-Tables.md) |

## 5. Redshift

| Signal keyword | Answer | Guide |
|---|---|---|
| Petabyte warehouse, BI concurrency | Amazon Redshift | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Spiky / unknown / least ops | Redshift Serverless | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Steady 24/7, cheapest | Provisioned RA3 + reserved nodes | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Storage full, CPU idle | RA3 managed storage | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Dev cluster idle at night | Pause/resume (or Serverless) | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Big fact ↔ big dimension join | DISTKEY on join column (both tables) | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Small static dimension | DISTSTYLE ALL | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Date-range filters | Compound sort key led by timestamp | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Rows piled on few slices | Skew → `SVV_TABLE_INFO.skew_rows`, fix DISTKEY | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Hands-off key tuning | AUTO keys (automatic table optimization) | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Nested / evolving JSON | SUPER + PartiQL | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Duplicate "primary keys" loaded | PK/UNIQUE/FK not enforced | [23](../topic-guides/23-Redshift-Architecture-Table-Design.md) |
| Fastest bulk load | COPY from S3 (split files) | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Concurrent COPYs to one table | One COPY with prefix/manifest | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Continuous S3 file loading, no code | Auto-copy (COPY JOB) | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Upsert | Staging table + MERGE | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Archive cold rows to lake | UNLOAD Parquet + Spectrum | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Join warehouse with S3 in place | Redshift Spectrum | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Query live RDS/Aurora from Redshift | Federated query | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Kinesis/MSK → Redshift, lowest latency | Streaming ingestion (materialized view) | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Repeated heavy aggregation | Materialized view (auto refresh/rewrite) | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Live data to another cluster/account, no copy | Data sharing | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Sell/license Redshift data | AWS Data Exchange datashare | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| SQL from Lambda/Step Functions, no drivers | Redshift Data API | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| ELT logic inside the warehouse | Stored procedure | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |
| Many queries queueing at peak | Concurrency scaling | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| One big slow query | Table design / resize, not concurrency scaling | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Short queries stuck behind long | Short query acceleration | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Abort/log runaway queries | Query monitoring rules | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Stale statistics | ANALYZE | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Reclaim deleted space, re-sort | VACUUM | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Error 1023 | Serializable isolation violation → retry / LOCK | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Region-wide DR | Cross-Region snapshot copy | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| User dropped one table | Table-level restore | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Filter rows per user | Row-level security (RLS) policy | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Redact column values per role | Dynamic data masking | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Audit every SQL statement for a year | Audit logging to S3/CloudWatch (not STL tables) | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| COPY/UNLOAD traffic inside VPC | Enhanced VPC routing | [38](../topic-guides/38-Networking-for-Data-Pipelines.md) |
| Encrypt an existing cluster | Modify cluster → KMS (in place) | [25](../topic-guides/25-Redshift-Performance-Operations-Security.md) |
| Natural language → SQL | Amazon Q generative SQL (Query Editor v2) | [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md) |

## 6. Other data stores

| Signal keyword | Answer | Guide |
|---|---|---|
| Store any format cheaply, future unknown use | S3 data lake | [02](../topic-guides/02-Data-Engineering-Fundamentals.md) |
| Unknown / changing access pattern | S3 Intelligent-Tiering | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Known ageing pattern | S3 Lifecycle transitions | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Archive, millisecond access | Glacier Instant Retrieval | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Cheapest, 12–48 h restore OK | Glacier Deep Archive | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Single-digit ms S3, co-located compute | S3 Express One Zone | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Versioned bucket still growing | NoncurrentVersionExpiration | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| 503 Slow Down | More prefixes, bigger files, backoff | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Bulk copy/tag/re-encrypt billions of objects | S3 Inventory → Batch Operations | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Consumers pay for downloads | Requester Pays | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Key-value, single-digit ms, any scale | DynamoDB | [27](../topic-guides/27-DynamoDB.md) |
| Throttles with spare table capacity | Hot partition key | [27](../topic-guides/27-DynamoDB.md) |
| Table writes throttle after new index | Under-provisioned GSI | [27](../topic-guides/27-DynamoDB.md) |
| Strong reads on alternate key | LSI (creation time only) | [27](../topic-guides/27-DynamoDB.md) |
| Change feed, per-item order, Lambda | DynamoDB Streams | [27](../topic-guides/27-DynamoDB.md) |
| Change feed, many consumers, replay | Kinesis Data Streams for DynamoDB | [27](../topic-guides/27-DynamoDB.md) |
| Free auto-expiry | DynamoDB TTL (not exact) | [27](../topic-guides/27-DynamoDB.md) |
| Analytics over DynamoDB | Export to S3 + Athena / zero-ETL → Redshift | [27](../topic-guides/27-DynamoDB.md) |
| Bulk load a new table | Import from S3 | [27](../topic-guides/27-DynamoDB.md) |
| Microsecond DynamoDB reads | DAX | [27](../topic-guides/27-DynamoDB.md) |
| Multi-Region active-active | DynamoDB global tables | [27](../topic-guides/27-DynamoDB.md) |
| Relational, managed, commercial engines | RDS | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| Extract without hurting production | Read replica / reader endpoint | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| Cross-Region relational DR, ~1 s RPO | Aurora Global Database | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| Durable in-memory DB, microseconds | MemoryDB | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| Cache in front of a database | ElastiCache (DAX for DynamoDB) | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| MongoDB-compatible | DocumentDB | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| Cassandra / CQL | Keyspaces | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| Fraud rings, multi-hop relationships | Neptune | [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md) |
| Full-text, fuzzy, relevance search | OpenSearch Service | [29](../topic-guides/29-OpenSearch-Service.md) |
| Logs searchable in seconds + dashboards | OpenSearch + Dashboards | [29](../topic-guides/29-OpenSearch-Service.md) |
| Hot → warm → cold → delete indices | ISM policy + UltraWarm/cold | [29](../topic-guides/29-OpenSearch-Service.md) |
| Yellow cluster on single node | Replicas unassigned (add nodes) | [29](../topic-guides/29-OpenSearch-Service.md) |
| Hide fields/docs per user in OpenSearch | Fine-grained access control | [29](../topic-guides/29-OpenSearch-Service.md) |
| DynamoDB → search, no code | Zero-ETL via OpenSearch Ingestion | [29](../topic-guides/29-OpenSearch-Service.md) |
| Managed RAG over S3 documents | Bedrock Knowledge Bases | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Vectors next to relational data | Aurora PostgreSQL + pgvector | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Hybrid keyword + vector search | OpenSearch (Faiss/Lucene) | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Cheapest billions of vectors, sub-second OK | S3 Vectors | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Single-digit ms vector search in memory | MemoryDB vector search | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Highest recall, lowest latency ANN | HNSW index | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Permission-aware enterprise doc search | Kendra ⚠️ (maintenance, still in scope) | [19](../topic-guides/19-GenAI-LLMs-Vectors.md) |
| Full history "as of" reporting | SCD Type 2 | [30](../topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md) |
| Analytics schema, fewer joins | Star schema | [30](../topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md) |
| Business dashboards embedded in a portal | Amazon Quick (Quick Sight) | [35](../topic-guides/35-Analytics-Visualization-Quick-Notebooks.md) |
| Dashboard stale after load | Refresh SPICE at pipeline end | [35](../topic-guides/35-Analytics-Visualization-Quick-Notebooks.md) |
| Viewers see only their rows | Quick Sight RLS | [35](../topic-guides/35-Analytics-Visualization-Quick-Notebooks.md) |

## 7. Orchestration & events

| Signal keyword | Answer | Guide |
|---|---|---|
| Serverless workflow across AWS services, retries, visual | Step Functions Standard | [20](../topic-guides/20-Step-Functions.md) |
| Wait for Glue/EMR/Athena/Batch job to finish | `.sync` integration (Standard only) | [20](../topic-guides/20-Step-Functions.md) |
| Pause for human approval / external system | `.waitForTaskToken` | [20](../topic-guides/20-Step-Functions.md) |
| High-volume, short (< 5 min) workflows | Step Functions Express | [20](../topic-guides/20-Step-Functions.md) |
| Different branches at once | Parallel state | [20](../topic-guides/20-Step-Functions.md) |
| Same steps per item in a list | Map state | [20](../topic-guides/20-Step-Functions.md) |
| Millions of S3 objects in parallel | Distributed Map | [20](../topic-guides/20-Step-Functions.md) |
| `States.DataLimitExceeded` | Pass S3 URIs (256 KiB limit) | [20](../topic-guides/20-Step-Functions.md) |
| Redshift Data API / crawler step | SDK call + Wait + describe polling loop | [20](../topic-guides/20-Step-Functions.md) |
| Resume from failed step | Redrive | [20](../topic-guides/20-Step-Functions.md) |
| Existing Airflow DAGs | Amazon MWAA | [21](../topic-guides/21-MWAA-Glue-Workflows.md) |
| Airflow without an always-on environment | MWAA Serverless | [21](../topic-guides/21-MWAA-Glue-Workflows.md) |
| Wait for an S3 file in Airflow | `S3KeySensor` (deferrable) | [21](../topic-guides/21-MWAA-Glue-Workflows.md) |
| DAG missing from UI | DAG processing logs (import errors) | [21](../topic-guides/21-MWAA-Glue-Workflows.md) |
| Only Glue jobs + crawlers | Glue workflow | [21](../topic-guides/21-MWAA-Glue-Workflows.md) |
| S3 arrivals start Glue workflow in batches | EventBridge event trigger (BatchSize/Window) | [21](../topic-guides/21-MWAA-Glue-Workflows.md) |
| "AWS Data Pipeline" | ⚠️ Distractor → Step Functions / MWAA | [21](../topic-guides/21-MWAA-Glue-Workflows.md) |
| React to AWS service state changes | EventBridge rule | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Job failed → notify people | EventBridge rule → SNS | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| S3 upload → Lambda/SQS/SNS, prefix/suffix | S3 Event Notifications | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| S3 upload → Step Functions, size filter, many targets | S3 → EventBridge | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Overlapping S3 notification filters | SNS fan-out or EventBridge | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Cron with time zones / one-time run | EventBridge Scheduler | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Reprocess past bus events | EventBridge archive & replay | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Queue → filter → enrich → target, no code | EventBridge Pipes | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Durable fan-out to many consumers | SNS → multiple SQS | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Buffer spikes, protect a database | SQS + rate-limited consumers | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Messages processed twice | Raise visibility timeout / idempotency | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Order + no duplicates | SQS FIFO (`MessageGroupId`) | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Poison message retried forever | DLQ + `maxReceiveCount` | [22](../topic-guides/22-EventBridge-SNS-SQS.md) |
| Preview IaC changes | CloudFormation change sets / `cdk diff` | [36](../topic-guides/36-Programming-IaC-CICD.md) |
| Keep data store when stack deleted | `DeletionPolicy: Retain/Snapshot` | [36](../topic-guides/36-Programming-IaC-CICD.md) |
| Serverless IaC + local testing | AWS SAM | [36](../topic-guides/36-Programming-IaC-CICD.md) |
| Only first 1,000 keys processed | SDK paginator | [36](../topic-guides/36-Programming-IaC-CICD.md) |

## 8. Security & governance

| Signal keyword | Answer | Guide |
|---|---|---|
| SCP allows but job still denied | SCPs never grant; fix the role policy | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| Cross-account access | Identity policy AND resource/trust policy | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| "Not authorized to perform iam:PassRole" | Grant `iam:PassRole` on the role ARN | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| ListBucket AccessDenied | Bucket ARN (no `/*`) + `s3:prefix` | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| Cross-account fails with `aws/` key | Customer managed KMS key | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| Least privilege from real usage | IAM Access Analyzer policy generation | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| One policy scales with tags (IAM) | ABAC | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| Restrict Regions org-wide | SCP `aws:RequestedRegion` | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| Outsiders must never reach our data | RCP + `aws:PrincipalOrgID` | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| Force TLS on a bucket | Deny `aws:SecureTransport = false` | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| Many teams, one bucket | S3 Access Points | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| Directory users → S3 prefixes | S3 Access Grants | [37](../topic-guides/37-IAM-for-Data-Engineers.md) |
| Column/row/cell access on lake tables | Lake Formation grants + data filters | [40](../topic-guides/40-Lake-Formation.md) |
| Tag-based access to thousands of tables | LF-Tags (LF-TBAC), not IAM ABAC | [40](../topic-guides/40-Lake-Formation.md) |
| LF grants ignored, IAM users see everything | Revoke `IAMAllowedPrincipals` | [40](../topic-guides/40-Lake-Formation.md) |
| Gradual IAM → LF migration | Hybrid access mode | [40](../topic-guides/40-Lake-Formation.md) |
| Shared table invisible in Athena | Resource link in consumer account | [40](../topic-guides/40-Lake-Formation.md) |
| Cross-account lake sharing | Lake Formation + AWS RAM | [40](../topic-guides/40-Lake-Formation.md) |
| Spark job must honor LF columns | Glue 5.0+ / EMR runtime roles / Athena SQL | [40](../topic-guides/40-Lake-Formation.md) |
| Who accessed data via LF | CloudTrail `GetDataAccess` | [40](../topic-guides/40-Lake-Formation.md) |
| Business catalog, glossary, request access | SageMaker Catalog (DataZone) | [41](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| Request → approve → automatic grants | SageMaker Catalog subscription | [41](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| One IDE: SQL + notebooks + ETL + GenAI | SageMaker Unified Studio | [41](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| Delegate admin per business unit | Domain units + owners | [41](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| Domain teams own data, central standards | Data mesh / federated hub-and-spoke | [41](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| Joint analysis, no raw data exposed | AWS Clean Rooms | [41](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md) |
| Column-level lineage in business catalog | SageMaker Catalog lineage (OpenLineage) | [30](../topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md) |
| Find PII across S3 at scale | Amazon Macie | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| PII in RDS/Redshift/streams during ETL | Glue Detect PII transform | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| Join on email without seeing it | Keyed hash / deterministic token | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| Must recover originals | Tokenization or encryption (not hashing) | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| Irreversibly remove PII from storage | Transform at ingestion (not DDM) | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| Mask PII in log events | CloudWatch Logs data protection policy | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| Block S3 cross-Region copies | Deny `s3:PutReplicationConfiguration` | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| Block Redshift cross-Region snapshots | Deny `redshift:EnableSnapshotCopy` | [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md) |
| Encrypt existing S3 objects with a new key | Inventory → Batch Operations copy | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| KMS request costs high on S3 | S3 Bucket Keys | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Encrypt more than 4 KB with KMS | Envelope encryption (data keys) | [39](../topic-guides/39-Encryption-Key-Management.md) |
| DB credentials with automatic rotation | Secrets Manager | [39](../topic-guides/39-Encryption-Key-Management.md) |
| Config values, free, no rotation | SSM Parameter Store | [39](../topic-guides/39-Encryption-Key-Management.md) |
| Cross-account KMS use | Customer managed key: key policy + IAM policy | [39](../topic-guides/39-Encryption-Key-Management.md) |
| Same key in several Regions | KMS multi-Region keys | [39](../topic-guides/39-Encryption-Key-Management.md) |
| Dedicated single-tenant HSM | CloudHSM (optionally as KMS custom key store) | [39](../topic-guides/39-Encryption-Key-Management.md) |
| Two layers of S3 encryption for compliance | DSSE-KMS | [39](../topic-guides/39-Encryption-Key-Management.md) |
| AWS must never see plaintext | Client-side encryption (Encryption SDK) | [39](../topic-guides/39-Encryption-Key-Management.md) |
| Encrypt an existing unencrypted RDS | Snapshot → encrypted copy → restore | [39](../topic-guides/39-Encryption-Key-Management.md) |
| Subscribe to vendor data, query in Redshift | AWS Data Exchange datashare | [46](../topic-guides/46-GapFill-Services.md) |
| Expose curated data to partner apps | API Gateway + Lambda / direct integration | [46](../topic-guides/46-GapFill-Services.md) |
| Central policy-based backups, WORM vault | AWS Backup + Vault Lock | [46](../topic-guides/46-GapFill-Services.md) |
| Shell on instances without SSH/bastion | Systems Manager Session Manager | [46](../topic-guides/46-GapFill-Services.md) |
| Private subnet job hangs reading S3 | S3 gateway endpoint (or NAT) | [38](../topic-guides/38-Networking-for-Data-Pipelines.md) |
| Private subnet → STS/KMS/Secrets Manager | Interface endpoints | [38](../topic-guides/38-Networking-for-Data-Pipelines.md) |
| Glue connection workers can't talk | Self-referencing security group rule | [38](../topic-guides/38-Networking-for-Data-Pipelines.md) |
| Fixed egress IP for allowlisting | NAT Gateway + Elastic IP | [38](../topic-guides/38-Networking-for-Data-Pipelines.md) |
| Block one malicious IP | NACL deny | [38](../topic-guides/38-Networking-for-Data-Pipelines.md) |
| Direct Connect must be encrypted | MACsec or VPN over DX | [38](../topic-guides/38-Networking-for-Data-Pipelines.md) |
| WORM, not even root can delete | S3 Object Lock compliance mode | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| WORM with privileged override | Object Lock governance mode | [05](../topic-guides/05-S3-Data-Lake-Storage.md) |
| Who called which API | CloudTrail | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| Who deleted S3 objects | CloudTrail S3 data events (enable beforehand) | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| Prove logs weren't tampered with | Log file integrity validation | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| All accounts, all Regions | Organization multi-Region trail | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| Is it compliant now / what changed | AWS Config | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| Auto-fix non-compliant resource | Config remediation → SSM Automation | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| Multi-account compliance view | Config aggregator | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |
| Managed SQL audit store | CloudTrail Lake ⚠️ (closed to new customers) | [43](../topic-guides/43-Audit-Logging-CloudTrail-Config.md) |

## 9. Ops, quality & cost

| Signal keyword | Answer | Guide |
|---|---|---|
| Alert on text in logs | Metric filter → alarm → SNS | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Alert if nightly job did NOT run | Treat missing data as breaching / EventBridge | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Seasonal thresholds | Anomaly detection alarm | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Ad hoc queries on log groups | CloudWatch Logs Insights | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Log bill grows forever | Set retention (default never expire) | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Central observability, no copy | Cross-account observability | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Ops dashboards, many sources, SSO | Amazon Managed Grafana | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| EC2 memory metric | CloudWatch agent | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Glue skew / idle workers | Glue observability metrics | [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md) |
| Completeness/uniqueness checks in Glue | Glue Data Quality (DQDL) | [33](../topic-guides/33-Data-Quality.md) |
| Moving baseline for volume | DQ dynamic rules / anomaly detection | [33](../topic-guides/33-Data-Quality.md) |
| Split good and bad rows | Row-level DQ outcomes → quarantine prefix | [33](../topic-guides/33-Data-Quality.md) |
| Stop pipeline on bad data | Fail job / Step Functions Choice | [33](../topic-guides/33-Data-Quality.md) |
| Source vs target reconciliation | `RowCountMatch` / `DatasetMatch` / DMS validation | [33](../topic-guides/33-Data-Quality.md) |
| Rare groups in a sample | Stratified sampling | [33](../topic-guides/33-Data-Quality.md) |
| Top-N per group | `ROW_NUMBER() OVER (PARTITION BY …)` | [34](../topic-guides/34-SQL-for-Data-Engineers.md) |
| Rolling 7-day average | Window `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` | [34](../topic-guides/34-SQL-for-Data-Engineers.md) |
| `NOT IN` returns nothing | NULL in subquery → `NOT EXISTS` | [34](../topic-guides/34-SQL-for-Data-Engineers.md) |
| Sum too high after join | Join fan-out | [34](../topic-guides/34-SQL-for-Data-Engineers.md) |
| Alert on spend threshold | AWS Budgets | [44](../topic-guides/44-Cost-Optimization.md) |
| Auto-enforce at budget | Budget actions | [44](../topic-guides/44-Cost-Optimization.md) |
| Unusual spend, no fixed threshold | Cost Anomaly Detection | [44](../topic-guides/44-Cost-Optimization.md) |
| Line-item, SQL-queryable costs | CUR 2.0 / Data Exports → Athena | [44](../topic-guides/44-Cost-Optimization.md) |
| Tags missing in billing | Activate cost allocation tags | [44](../topic-guides/44-Cost-Optimization.md) |
| NAT bill high from S3 traffic | S3/DynamoDB gateway endpoints (free) | [44](../topic-guides/44-Cost-Optimization.md) |
| Steady 24/7 load, minimize cost | Provisioned + reservations | [02](../topic-guides/02-Data-Engineering-Fundamentals.md) |
| Spiky / intermittent / unknown | Serverless / on-demand | [02](../topic-guides/02-Data-Engineering-Fundamentals.md) |
| Duplicates possible downstream | Idempotent writes / MERGE on natural key | [02](../topic-guides/02-Data-Engineering-Fundamentals.md) |
| Architecture review against best practice | Well-Architected Tool + Data Analytics Lens | [45](../topic-guides/45-Service-Selection-Decision-Guide.md) |

---

Next: [numbers-to-know.md](numbers-to-know.md) for the limits behind these answers, and [decision-flowcharts.md](decision-flowcharts.md) for the big choices drawn as trees.
