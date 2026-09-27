# Decision Flowcharts — the eight big choices

Most DEA-C01 questions come down to one fork in the road. Each tree below starts at the first question worth asking and stops at the answer the exam wants. Read top to bottom. When a question stem includes a signal from a diamond, take that branch.

The charts are simplified on purpose. When two answers both fit, the qualifier decides (*least operational overhead*, *most cost-effective*, *lowest latency*). See [Guide 47 — Exam Traps & Key Patterns](../topic-guides/47-Exam-Traps-Key-Patterns.md) for how qualifiers work, and [Guide 45 — Service Selection](../topic-guides/45-Service-Selection-Decision-Guide.md) for the full matrices.

---

## 1. Streaming ingestion

Pick the pipe by what happens after the data arrives: delivery only, replay by many apps, Kafka compatibility, or stateful analytics. Owners: [Guide 06](../topic-guides/06-Kinesis-Data-Streams.md), [07](../topic-guides/07-Amazon-Data-Firehose.md), [08](../topic-guides/08-Amazon-MSK-Kafka.md), [09](../topic-guides/09-Managed-Service-for-Apache-Flink.md).

```mermaid
flowchart TD
    A[Streaming data arrives] --> B{"Existing Kafka apps or Kafka API?"}
    B -- Yes --> C{"Want no capacity planning?"}
    C -- Yes --> C1["MSK Serverless"]
    C -- No --> C2["MSK Provisioned or Express"]
    B -- No --> D{"Only deliver to S3, Redshift, OpenSearch, Splunk?"}
    D -- "Yes, near real-time OK" --> E["Amazon Data Firehose"]
    D -- No --> F{"Many consumers, replay, or per-key order?"}
    F -- Yes --> G{"Traffic predictable?"}
    G -- No --> G1["KDS on-demand"]
    G -- Yes --> G2["KDS provisioned"]
    F -- No --> H{"Windows, joins, state, event time?"}
    H -- Yes --> I["KDS or MSK into Managed Flink"]
    H -- No --> J["KDS + Lambda for stateless per-record work"]
    G1 --> K["Add Firehose leg for S3 archive"]
    G2 --> K
```

## 2. Batch processing engine

Start with size and runtime, then with how much control you need. Owners: [Guide 12](../topic-guides/12-AWS-Glue-ETL.md), [15](../topic-guides/15-Amazon-EMR.md), [17](../topic-guides/17-Lambda-for-Data-Pipelines.md), [18](../topic-guides/18-Containers-Batch-EC2-Compute.md).

```mermaid
flowchart TD
    A[Batch transform needed] --> B{"Small and under 15 minutes?"}
    B -- Yes --> B1["Lambda"]
    B -- No --> C{"Pure SQL on data already in place?"}
    C -- "Yes, in the lake" --> C1["Athena CTAS or INSERT INTO"]
    C -- "Yes, in the warehouse" --> C2["Redshift ELT or stored procedure"]
    C -- No --> D{"Analysts, no code?"}
    D -- Yes --> D1["Glue DataBrew"]
    D -- No --> E{"Spark or Python ETL, least ops?"}
    E -- Yes --> F{"Time-critical?"}
    F -- No --> F1["Glue with Flex"]
    F -- Yes --> F2["Glue standard with Auto Scaling"]
    E -- No --> G{"Need HBase, Trino, custom builds, Spot control?"}
    G -- Yes --> G1["EMR on EC2"]
    G -- No --> H{"Arbitrary container or legacy binary?"}
    H -- Yes --> H1["AWS Batch"]
    H -- No --> H2["EMR Serverless"]
```

## 3. Orchestration service

The question to ask is what the workflow has to call and who already owns the code. Owners: [Guide 20](../topic-guides/20-Step-Functions.md), [21](../topic-guides/21-MWAA-Glue-Workflows.md), [22](../topic-guides/22-EventBridge-SNS-SQS.md).

```mermaid
flowchart TD
    A[Need to coordinate steps] --> B{"Just one trigger: a schedule or event?"}
    B -- Yes --> B1["EventBridge Scheduler or rule"]
    B -- No --> C{"Existing Airflow DAGs or Python-coded DAGs?"}
    C -- Yes --> C0{"Runs occasionally, no always-on env?"}
    C0 -- Yes --> C1["MWAA Serverless"]
    C0 -- No --> C2["Amazon MWAA"]
    C -- No --> D{"Only Glue jobs and crawlers?"}
    D -- Yes --> D1["Glue workflow"]
    D -- No --> E{"Must wait for jobs, callbacks, or run over 5 min?"}
    E -- Yes --> E1["Step Functions Standard"]
    E -- No --> F{"High volume, short, event-driven?"}
    F -- Yes --> F1["Step Functions Express"]
    F -- No --> E1
    E1 --> G{"Millions of items in parallel?"}
    G -- Yes --> G1["Distributed Map"]
```

## 4. Query engine

Where the data lives and how often you query it decide the engine. Owners: [Guide 26](../topic-guides/26-Amazon-Athena.md), [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md), [29](../topic-guides/29-OpenSearch-Service.md), [32](../topic-guides/32-Monitoring-Logging-Troubleshooting.md).

```mermaid
flowchart TD
    A[Need to query data] --> B{"Data in CloudWatch Logs only?"}
    B -- Yes --> B1["CloudWatch Logs Insights"]
    B -- No --> C{"Full-text, fuzzy, or log search in seconds?"}
    C -- Yes --> C1["OpenSearch Service"]
    C -- No --> D{"Data in S3?"}
    D -- Yes --> E{"Ad hoc or occasional?"}
    E -- Yes --> E1["Athena"]
    E -- No --> F{"Heavy BI concurrency, joins, dashboards?"}
    F -- Yes --> F1["Load to Redshift, or Spectrum for cold data"]
    F -- No --> E1
    D -- No --> G{"Live operational DB joined with lake?"}
    G -- "From Redshift" --> G1["Redshift federated query"]
    G -- "Ad hoc" --> G2["Athena federated query"]
    G -- "Constantly at scale" --> G3["Zero-ETL into Redshift"]
```

## 5. Data store by access pattern

Start from how the data is read, not from what it looks like. Owners: [Guide 27](../topic-guides/27-DynamoDB.md), [28](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md), [23](../topic-guides/23-Redshift-Architecture-Table-Design.md), [05](../topic-guides/05-S3-Data-Lake-Storage.md), [19](../topic-guides/19-GenAI-LLMs-Vectors.md).

```mermaid
flowchart TD
    A[Choose a store] --> B{"Raw, any format, future unknown use?"}
    B -- Yes --> B1["S3 data lake, Iceberg or S3 Tables for ACID"]
    B -- No --> C{"Large scans and aggregates for BI?"}
    C -- Yes --> C1["Redshift"]
    C -- No --> D{"Key-value lookups at any scale, ms latency?"}
    D -- Yes --> D0{"Microseconds needed?"}
    D0 -- "Cache OK" --> D1["DynamoDB + DAX"]
    D0 -- "Durable primary" --> D2["MemoryDB"]
    D0 -- No --> D3["DynamoDB"]
    D -- No --> E{"Relational, joins, transactions?"}
    E -- Yes --> E1["Aurora or RDS"]
    E -- No --> F{"Multi-hop relationships?"}
    F -- Yes --> F1["Neptune"]
    F -- No --> G{"Text or log search?"}
    G -- Yes --> G1["OpenSearch"]
    G -- No --> H{"Vector embeddings?"}
    H -- "Beside SQL data" --> H1["Aurora PostgreSQL pgvector"]
    H -- "Billions, lowest cost" --> H2["S3 Vectors"]
    H -- "Hybrid search" --> H3["OpenSearch vector engine"]
```

## 6. Partition-sync method

New S3 folders are invisible until the catalog knows about them. The choice depends on engines and layout. Owners: [Guide 13](../topic-guides/13-Glue-Data-Catalog-Crawlers.md), [26](../topic-guides/26-Amazon-Athena.md).

```mermaid
flowchart TD
    A[New partitions land in S3] --> B{"Only Athena queries this table?"}
    B -- Yes --> B1{"Partition values predictable?"}
    B1 -- Yes --> B2["Partition projection, nothing to register"]
    B1 -- No --> C
    B -- No --> C{"Does the writer control catalog updates?"}
    C -- Yes --> C1["Write-time registration: Glue enableUpdateCatalog, Firehose, Iceberg"]
    C -- No --> D{"Hive-style key=value paths?"}
    D -- No --> D1["ALTER TABLE ADD PARTITION with LOCATION"]
    D -- Yes --> E{"Schema may change too?"}
    E -- Yes --> E1["Crawler, S3 event mode"]
    E -- No --> E2["MSCK REPAIR TABLE, small tables only"]
    C1 --> F{"Millions of partitions, slow planning?"}
    E1 --> F
    F -- Yes --> F1["Add partition index, max 3 per table"]
```

## 7. Access control layer

Pick the layer closest to the data being protected. Owners: [Guide 37](../topic-guides/37-IAM-for-Data-Engineers.md), [40](../topic-guides/40-Lake-Formation.md), [25](../topic-guides/25-Redshift-Performance-Operations-Security.md), [42](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md).

```mermaid
flowchart TD
    A[Restrict data access] --> B{"Where does the data live?"}
    B -- "S3 files, bucket or prefix level" --> C["IAM + bucket policy, Access Points, Access Grants"]
    B -- "Catalog tables in the lake" --> D{"Row, column, or cell level?"}
    D -- No --> D0["IAM on Glue + S3 is enough"]
    D -- Yes --> E{"Thousands of tables by classification?"}
    E -- Yes --> E1["Lake Formation LF-Tags"]
    E -- No --> E2["Lake Formation grants + data filters"]
    B -- "Redshift tables" --> F{"What must be restricted?"}
    F -- Rows --> F1["Redshift RLS policy"]
    F -- "Mask values per role" --> F2["Dynamic data masking"]
    F -- "Whole columns" --> F3["Column-level GRANT"]
    B -- "Dashboards" --> G["Quick Sight RLS and CLS"]
    B -- "OpenSearch index" --> H["Fine-grained access control"]
    E1 --> X{"PII must be gone from storage?"}
    F2 --> X
    X -- Yes --> X1["Mask or tokenize at ingestion"]
```

## 8. Cross-account data sharing

Live sharing beats copies. The engine the consumer uses decides the mechanism. Owners: [Guide 41](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md), [40](../topic-guides/40-Lake-Formation.md), [24](../topic-guides/24-Redshift-Loading-Integration-Sharing.md).

```mermaid
flowchart TD
    A[Share data with another account] --> B{"External third party, paid or licensed?"}
    B -- Yes --> B1["AWS Data Exchange"]
    B -- No --> C{"Must analyze jointly without exposing raw rows?"}
    C -- Yes --> C1["AWS Clean Rooms"]
    C -- No --> D{"Self-service requests with approval workflow?"}
    D -- Yes --> D1["SageMaker Catalog subscriptions"]
    D -- No --> E{"Data is in Redshift?"}
    E -- Yes --> E1["Redshift data sharing, live, no copy"]
    E -- No --> F{"Catalog tables in the lake?"}
    F -- Yes --> F1["Lake Formation grants via AWS RAM"]
    F1 --> F2["Consumer creates resource link"]
    F -- No --> G["Bucket policy or Access Point + customer managed KMS key"]
```

---

Related sheets: [keyword-to-service.md](keyword-to-service.md) · [numbers-to-know.md](numbers-to-know.md)
