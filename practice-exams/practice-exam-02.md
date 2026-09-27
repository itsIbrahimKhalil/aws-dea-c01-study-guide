# Practice Exam 02 — DEA-C01 (65 questions)

> Written for the **AWS Certified Data Engineer – Associate (DEA-C01)** exam guide **v1.1 (December 12, 2025)**. Service facts checked as of **September 2026**. Service names use the current name first, with the older name in parentheses where the question pool may still use it.

## How to take this exam

- **Time:** 130 minutes for 65 questions, which is the real exam's time and length. Use a timer.
- **No notes, no console, no search.** Answer from memory, the way you would at the test center.
- **Flag and review.** Mark any question you're not sure about, keep going, and come back to your flagged questions when you reach the end. On the real exam, time spent stuck on one hard question is time taken from easier ones.
- **Check answers only at the end.** Each answer sits in a collapsed block. Don't open any of them until you have answered all 65, then score yourself with the answer key and the domain table at the bottom.
- **Question types:** *multiple choice* (one correct answer out of four) and *multiple response* (the stem tells you how many to pick, for example "(Select TWO.)"). A multiple-response question counts as correct only if every option you pick is right.
- **Scoring guidance:**
  - **≥ 80% (52/65 or more):** you are likely ready.
  - **70–80% (46–51):** review your weak domains using the linked guides, then take another practice exam.
  - **< 70% (45 or fewer):** you need more study. Work through the topic guides for the domains where you scored lowest.

Domain mix (same weighting as the real exam): Domain 1 Ingestion & Transformation = 22 · Domain 2 Data Store Management = 17 · Domain 3 Operations & Support = 14 · Domain 4 Security & Governance = 12.

---

### Question 1
A logistics company runs a self-managed Apache Kafka cluster on premises. Dozens of producer and consumer applications use the standard Kafka client libraries, several stream processors are built with the Kafka Streams API, and Kafka Connect connectors move data into downstream databases. The company wants to move this platform to AWS without managing broker hosts. Which solution meets these requirements with the LEAST application change?

- **A.** Create an Amazon Kinesis Data Streams stream in on-demand mode, rewrite the producers with the Kinesis Producer Library, and rewrite the consumers with the Kinesis Client Library.
- **B.** Create an Amazon MSK cluster, point the existing clients at the MSK bootstrap brokers, and run the existing connectors on MSK Connect.
- **C.** Create an Amazon SQS FIFO queue for each Kafka topic and use message group IDs so that consumers keep per-key ordering.
- **D.** Create an Amazon Data Firehose (Kinesis Data Firehose) stream with Direct PUT for each topic and deliver the records to Amazon S3 for the consumers to read.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Amazon MSK runs open-source Apache Kafka, so the existing Kafka clients, Kafka Streams applications, and Kafka Connect connectors keep working after a configuration change to the bootstrap servers. MSK Connect runs the connectors as a managed service. The decision comes down to protocol and API compatibility with Kafka.

**Why not the others:**
- **A** — Kinesis is a good streaming service, but every producer and consumer would have to be rewritten, and Kafka Streams has no drop-in Kinesis equivalent.
- **C** — SQS is a message queue with no Kafka API, no replayable log, and no Kafka Streams support. It would mean a full redesign.
- **D** — Firehose is a delivery service that writes to destinations. It doesn't give consumers a Kafka-style subscription model.

> Note: Kinesis Data Streams now accepts records up to **10 MiB** (October 2025), so "records larger than 1 MB → MSK" is a legacy rule. Kafka compatibility is the reason that still holds.

**Signal words:** *"Kafka Streams API"*, *"Kafka Connect"*, *"LEAST application change"* · **Skill:** 1.1.1 · **Review:** [Guide 08 — Amazon MSK & Kafka](../topic-guides/08-Amazon-MSK-Kafka.md)
</details>

---

### Question 2
An ecommerce company stores 40 million product-description embeddings in an Amazon Aurora PostgreSQL-Compatible Edition database by using the pgvector extension. A semantic search feature must return the 10 most similar products with low latency and high recall, by cosine distance. New products are inserted all day, and the team does not want to rebuild the index regularly as the data distribution changes. Which indexing approach should the data engineer use?

- **A.** Create an IVFFlat index with `vector_cosine_ops` after the initial load, and schedule a weekly REINDEX so that the list centroids reflect newly inserted products.
- **B.** Create a B-tree index on the embedding column and order the query results by cosine distance.
- **C.** Don't create a vector index. Use exact nearest-neighbor sequential scans and scale up the Aurora instance to add memory.
- **D.** Create an HNSW index on the embedding column with `vector_cosine_ops`, and tune `ef_search` to balance recall against latency.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** HNSW (Hierarchical Navigable Small World) builds a layered proximity graph. It gives one of the best speed-to-recall trade-offs for approximate nearest-neighbor search, has no training step, and adds new rows to the graph as they are inserted. `ef_search` sets how widely a query searches the graph, so you can trade some latency for higher recall.

**Why not the others:**
- **A** — IVFFlat groups vectors into lists around centroids that are computed from the data present at build time. As new data changes the distribution, recall drops until the index is rebuilt, which is the maintenance the team wants to avoid.
- **B** — A B-tree index can't serve nearest-neighbor searches on vectors.
- **C** — An exact scan gives perfect recall, but scanning 40 million vectors on every query can't meet a low-latency requirement.

**Signal words:** *"inserted all day"*, *"does not want to rebuild the index"*, *"low latency and high recall"* · **Skill:** 2.1.8 · **Review:** [Guide 19 — GenAI, LLMs & Vectors](../topic-guides/19-GenAI-LLMs-Vectors.md)
</details>

---

### Question 3
A data engineering team runs an Amazon MWAA environment in private subnets with no route to the internet, as required by company security policy. The team added two Python packages to `requirements.txt` and updated the environment. The logs now show that pip can't reach the Python Package Index, and DAGs that import the packages fail with `ModuleNotFoundError`. The network design must not change. What should the data engineer do?

- **A.** Package the dependencies as wheel files in `plugins.zip`, and install them from the local plugins directory with `--no-index` and `--find-links`.
- **B.** Change the environment's Apache Airflow web server access mode from private network to public network so that the Airflow components can download the packages.
- **C.** Upgrade the environment class to `mw1.large` so that the workers have enough memory and CPU to build the packages during installation.
- **D.** Add a startup shell script to the environment that runs `pip install` for the two packages from the Python Package Index before the scheduler and workers start.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** An MWAA environment without internet access can't download packages from PyPI. The documented approach is to package the dependencies as wheels in `plugins.zip`, which MWAA extracts on every Airflow component, and to tell pip to install from that local directory instead of the index.

**Why not the others:**
- **B** — The web server access mode only controls who can reach the Airflow UI. It doesn't give the scheduler or workers an outbound internet route.
- **C** — The installation fails because of network access, not because of CPU or memory.
- **D** — A startup script runs on the same hosts with the same lack of internet access, so `pip install` from PyPI still fails.

**Signal words:** *"no route to the internet"*, *"can't reach the Python Package Index"*, *"network design must not change"* · **Skill:** 3.1.2 · **Review:** [Guide 21 — MWAA & Glue Workflows](../topic-guides/21-MWAA-Glue-Workflows.md)
</details>

---

### Question 4
A data engineer's IAM policy allows `glue:CreateJob` and `glue:StartJobRun`. When the engineer tries to create an AWS Glue job that uses an existing IAM role named `GlueEtlRole`, the request fails with an AccessDenied error that refers to the role. The security team requires least privilege. What should be added to the data engineer's permissions?

- **A.** The `AWSGlueServiceRole` AWS managed policy, attached directly to the data engineer's IAM identity.
- **B.** A statement in the trust policy of `GlueEtlRole` that allows the data engineer's principal to call `sts:AssumeRole`.
- **C.** `iam:PassRole` on the ARN of `GlueEtlRole`, with an `iam:PassedToService` condition of `glue.amazonaws.com`.
- **D.** `iam:CreateRole` and `iam:AttachRolePolicy`, so that the data engineer can create a dedicated role for each new job.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** When you assign a role to a service resource (a Glue job, an EMR cluster, a Lambda function), you are passing that role to the service. That requires `iam:PassRole` on the specific role. The `iam:PassedToService` condition ensures that the role can be passed only to AWS Glue, which keeps the grant least privilege.

**Why not the others:**
- **A** — That managed policy is written for the Glue service role, not for the person creating jobs. It doesn't grant `iam:PassRole` on this role.
- **B** — The engineer doesn't need to assume the role. Glue assumes it, and the role's trust policy already names the Glue service.
- **D** — Letting the engineer create roles and attach policies is much broader than needed, and it still doesn't give `PassRole`.

**Signal words:** *"uses an existing IAM role"*, *"AccessDenied … refers to the role"*, *"least privilege"* · **Skill:** 4.1.4 · **Review:** [Guide 37 — IAM for Data Engineers](../topic-guides/37-IAM-for-Data-Engineers.md)
</details>

---

### Question 5
A company processes order events from an Amazon Kinesis Data Streams stream with an AWS Lambda function. The function's event source mapping uses default settings. The IteratorAge metric for one shard has been growing for hours, and the logs show the same batch failing repeatedly because one malformed record raises an exception. The company wants processing to continue past bad records, and it wants to keep every failed record for later investigation. Which combination of actions should the data engineer take? (Select TWO.)

- **A.** Increase the parallelization factor of the event source mapping to 10 so that more batches run at once.
- **B.** Turn on the `BisectBatchOnFunctionError` setting for the event source mapping.
- **C.** Register the function as an enhanced fan-out consumer of the stream.
- **D.** Increase the function timeout to the maximum of 15 minutes.
- **E.** Set `MaximumRetryAttempts` to a small value with an Amazon S3 on-failure destination.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, E**

**Why B and E:** With default settings, Lambda retries a failing Kinesis batch until the records expire from the stream, which blocks that shard. Bisecting the batch on error splits it in half repeatedly until the bad record is isolated, so the good records in the batch get processed. A finite retry count stops the blocking, and an on-failure destination keeps a record of what failed. With an S3 destination, Lambda writes the whole failed batch to the bucket; SQS and SNS destinations receive only metadata about the batch.

**Why not the others:**
- **A** — A higher parallelization factor processes more batches per shard at the same time, but records with the same partition key stay in order, so the poison batch is still retried forever.
- **C** — Enhanced fan-out gives the consumer more read throughput. It doesn't change how errors are retried.
- **D** — The function fails because of the data, not because it runs out of time.

**Signal words:** *"same batch failing repeatedly"*, *"continue past bad records"*, *"keep every failed record"* · **Skill:** 1.1.7 · **Review:** [Guide 17 — Lambda for Data Pipelines](../topic-guides/17-Lambda-for-Data-Pipelines.md)
</details>

---

### Question 6
A media company manages Apache Iceberg tables in an S3 general purpose bucket. Its data engineers maintain Spark jobs that compact small files, expire old snapshots, and delete orphan files, and these jobs often fail or fall behind. The company wants a storage option that handles Iceberg table maintenance automatically by default, with the tables still queryable from Amazon Athena and Amazon Redshift. Which solution meets these requirements with the LEAST operational overhead?

- **A.** Convert the tables to AWS Lake Formation governed tables so that Lake Formation compacts the data files automatically in the background.
- **B.** Create an Amazon S3 table bucket, integrate it with AWS analytics services through the AWS Glue Data Catalog, and migrate the tables into it.
- **C.** Add S3 Lifecycle rules that expire data files older than 30 days, and move the remaining files to the S3 Intelligent-Tiering storage class automatically.
- **D.** Move the tables to an S3 Express One Zone directory bucket, and run the existing maintenance jobs on a schedule on an Amazon EMR cluster.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Amazon S3 Tables stores Iceberg tables in table buckets, and S3 runs compaction, snapshot management, and unreferenced file removal for them automatically (on by default, adjustable per table). After you integrate table buckets with AWS analytics services through the Glue Data Catalog, Athena, Redshift, EMR, and Quick Sight can query the tables.

**Why not the others:**
- **A** — Lake Formation governed tables were discontinued (writes and queries stopped at the end of 2024). Iceberg replaced them.
- **C** — Lifecycle rules don't read Iceberg metadata. Expiring data files that snapshots still reference corrupts the tables.
- **D** — The team would still own and run the maintenance jobs, which is the problem the company wants to get rid of.

**Signal words:** *"maintenance automatically by default"*, *"Iceberg"*, *"LEAST operational overhead"* · **Skill:** 2.1.7 · **Review:** [Guide 04 — Open Table Formats & S3 Tables](../topic-guides/04-Open-Table-Formats-S3-Tables.md)
</details>

---

### Question 7
An energy company sends smart-meter readings to an Amazon Kinesis Data Streams stream. Analysts need the readings to be queryable in an existing Amazon Redshift Serverless workgroup within seconds of arrival. The company wants to use Amazon Redshift streaming ingestion and doesn't want to stage the data in Amazon S3. Which combination of steps should the data engineer take? (Select THREE.)

- **A.** Create an IAM role that allows reads from the stream, and associate the role with the Redshift Serverless namespace.
- **B.** Create an Amazon Data Firehose (Kinesis Data Firehose) stream that reads from the Kinesis stream and delivers to Redshift through an intermediate S3 bucket.
- **C.** Run `CREATE EXTERNAL SCHEMA ... FROM KINESIS` in Redshift, specifying the IAM role.
- **D.** Schedule a COPY command that runs every minute and references the ARN of the Kinesis stream as its source.
- **E.** Run an AWS Glue crawler against the stream and create a Redshift Spectrum external table from the resulting Data Catalog table.
- **F.** Create a materialized view that selects from the stream through the external schema, parses the payload with `JSON_PARSE`, and specifies `AUTO REFRESH YES`.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, C, F**

**Why A, C, and F:** Streaming ingestion reads directly from the stream into Redshift. Redshift needs an IAM role with Kinesis read permissions, an external schema that maps the stream (`FROM KINESIS`, or `FROM MSK` for Kafka), and a materialized view that holds the ingested records. `AUTO REFRESH YES` keeps loading new records with latency of seconds. The payload arrives as `VARBYTE`, so you parse it (for example with `JSON_PARSE` into `SUPER`).

**Why not the others:**
- **B** — Firehose works, but it buffers the data and stages it in S3, which the requirements rule out.
- **D** — COPY loads from S3, DynamoDB, EMR, or remote hosts. It can't read a Kinesis stream.
- **E** — Crawlers and Spectrum work with data at rest in S3, not with a live stream.

**Signal words:** *"within seconds"*, *"streaming ingestion"*, *"not stage the data in Amazon S3"* · **Skill:** 1.1.1 · **Review:** [Guide 24 — Redshift Loading, Integration & Sharing](../topic-guides/24-Redshift-Loading-Integration-Sharing.md)
</details>

---

### Question 8
A data engineer uses an Amazon Athena CREATE TABLE AS SELECT (CTAS) query to convert three years of CSV sales data to Apache Parquet, partitioned by `sale_date`. The query fails with the error `HIVE_TOO_MANY_OPEN_PARTITIONS`. The team wants to finish the conversion in Athena without building a separate ETL job. What should the data engineer do?

- **A.** Increase the DML query timeout for the workgroup so that the CTAS query has enough time to finish writing every partition.
- **B.** Replace the partitioning with bucketing on `sale_date`, and set `bucket_count` to the number of distinct dates in the data.
- **C.** Create the table with CTAS for the first 100 dates, then add the remaining dates with INSERT INTO statements of at most 100 partitions each.
- **D.** Request a Service Quotas increase for the number of partitions that one CTAS query is allowed to write, and rerun the original query after approval.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** A single Athena CTAS or INSERT INTO query can write to at most 100 partitions. Three years of daily partitions is more than a thousand. The documented workaround is to create the table with a CTAS query covering up to 100 partitions, then append the rest in batches of INSERT INTO statements.

**Why not the others:**
- **A** — The error is about open partition writers, not about running time.
- **B** — The same limit covers combinations of partitions and buckets, so a thousand buckets fails too.
- **D** — This is a fixed engine limitation with a documented workaround, not a quota you can raise.

**Signal words:** *"HIVE_TOO_MANY_OPEN_PARTITIONS"*, *"finish the conversion in Athena"* · **Skill:** 3.2.3 · **Review:** [Guide 26 — Amazon Athena](../topic-guides/26-Amazon-Athena.md)
</details>

---

### Question 9
A streaming media company captures player events in Amazon Kinesis Data Streams. The analytics team needs metrics for each viewer session, where a session ends after 30 minutes without events from that viewer, even if the session itself lasts several hours. Events can arrive up to 2 minutes late, and results must be based on event time. Which solution meets these requirements?

- **A.** Use an AWS Lambda function with a 15-minute Kinesis tumbling window to aggregate events per viewer, and carry each viewer's state from one window to the next in the function.
- **B.** Use Amazon Data Firehose (Kinesis Data Firehose) with a 900-second buffer interval, and aggregate each buffered batch with a Lambda transformation function.
- **C.** Use an Amazon Managed Service for Apache Flink application with 30-minute tumbling windows keyed by viewer ID and processing-time semantics.
- **D.** Use an Amazon Managed Service for Apache Flink application with session windows keyed by viewer ID, a 30-minute gap, and watermarks that allow 2 minutes of lateness.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** A session window has no fixed length. It closes after a period of inactivity (the gap) for each key, which is exactly how the viewer session is defined. Flink keeps per-key state durably through checkpoints, and event-time processing with watermarks handles the 2 minutes of lateness.

**Why not the others:**
- **A** — Lambda tumbling windows are fixed-length (up to 15 minutes) and aggregate per shard. Tracking multi-hour, per-viewer sessions with late events would take a lot of custom state management.
- **B** — Firehose buffers are delivery batches, not per-key windows, and they don't understand event time.
- **C** — Fixed 30-minute tumbling windows split long sessions into pieces, and processing time ignores late events.

**Signal words:** *"session ends after 30 minutes without events"*, *"event time"*, *"arrive up to 2 minutes late"* · **Skill:** 1.1.12 · **Review:** [Guide 09 — Managed Service for Apache Flink](../topic-guides/09-Managed-Service-for-Apache-Flink.md)
</details>

---

### Question 10
A gaming company is building a real-time leaderboard service. The application code already uses Redis sorted sets, reads must complete in microseconds, and the leaderboard is the system of record, so no acknowledged write can be lost if a node fails. The company wants a fully managed service. Which data store meets these requirements?

- **A.** Amazon MemoryDB, with a Multi-AZ cluster that commits every write to a durable transaction log.
- **B.** Amazon ElastiCache (Redis OSS) with replicas in several Availability Zones and daily automatic snapshots.
- **C.** Amazon DynamoDB with DynamoDB Accelerator (DAX) in front of the table to serve microsecond reads.
- **D.** Amazon Aurora PostgreSQL-Compatible Edition with one read replica in each Availability Zone.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** MemoryDB is an in-memory database compatible with Valkey and Redis OSS. It serves reads in microseconds and stores every write durably in a Multi-AZ transaction log before acknowledging it, so it can be the primary database, not just a cache. The existing sorted-set code works unchanged.

**Why not the others:**
- **B** — ElastiCache is a cache. Replication is asynchronous and snapshots run at intervals, so recent acknowledged writes can be lost when a node fails.
- **C** — DynamoDB doesn't support the Redis API or sorted sets, so the code would have to be rewritten. DAX speeds up reads, not durable writes.
- **D** — Aurora is durable, but it responds in milliseconds and has no Redis data structures.

**Signal words:** *"Redis sorted sets"*, *"microseconds"*, *"system of record … no acknowledged write can be lost"* · **Skill:** 2.1.3 · **Review:** [Guide 28 — RDS, Aurora & Purpose-Built DBs](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md)
</details>

---

### Question 11
A healthcare company stores patient records in Amazon Redshift. Analysts must see only the last four digits of the `ssn` column, the fraud team must see the full value, and all other users must see a fixed placeholder string. The company wants to keep one table with no duplicate views, and it wants the rules managed through database roles. What should the data engineer implement?

- **A.** Create three views that present the `ssn` column in different formats, and grant each role SELECT on only its own view.
- **B.** Create masking policies for the `ssn` column, and attach them to the analyst role, the fraud role, and PUBLIC with priorities.
- **C.** Use column-level GRANT statements so that only the fraud role can select the `ssn` column, and revoke the column from every other role.
- **D.** Create a row-level security policy that removes rows with a populated `ssn` value for every role except the fraud role.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Redshift dynamic data masking applies masking policies at query time. Each policy is an expression, such as a partial reveal, a full value, or a constant. You attach the policies to a column for specific roles, users, or PUBLIC, and a priority decides which one wins when a user holds more than one role. The table stays the same, and no views are needed.

**Why not the others:**
- **A** — This is the duplicated-view design the company wants to avoid, and it's harder to keep consistent.
- **C** — Column-level GRANTs are all or nothing. Analysts couldn't see the last four digits.
- **D** — Row-level security filters rows, not column values. Analysts would lose whole records.

**Signal words:** *"last four digits"*, *"one table with no duplicate views"*, *"through database roles"* · **Skill:** 4.3.1 · **Review:** [Guide 25 — Redshift Performance, Operations & Security](../topic-guides/25-Redshift-Performance-Operations-Security.md)
</details>

---

### Question 12
An online retailer runs its order system on Amazon Aurora MySQL-Compatible Edition. The business wants order data available in Amazon Redshift for dashboards within seconds to minutes of each transaction. The data engineering team doesn't want to build or operate any ETL pipelines, and the analytics queries must not run against the production database. Which solution meets these requirements?

- **A.** Use Amazon Redshift federated queries so that the dashboards read the Aurora tables directly through an external schema.
- **B.** Use AWS DMS with change data capture to write changes to Amazon S3, and load the files into Redshift with scheduled COPY commands.
- **C.** Create an Aurora zero-ETL integration with Amazon Redshift, and create a destination database in Redshift from the integration.
- **D.** Schedule an AWS Glue job with job bookmarks every 15 minutes to read new orders over JDBC and load them into Redshift.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** A zero-ETL integration replicates data and schema changes from Aurora to Redshift continuously, typically within seconds, and AWS manages the replication. Dashboards query the copy in Redshift, so the production database doesn't get the analytics load.

**Why not the others:**
- **A** — Federated queries run against the live Aurora database on every dashboard refresh. That puts analytics load on production.
- **B** — This works, but it is a pipeline the team would have to build, schedule, and operate.
- **D** — This is a custom batch pipeline with 15-minute latency, and each run queries the production database.

**Signal words:** *"doesn't want to build or operate any ETL pipelines"*, *"within seconds to minutes"*, *"must not run against the production database"* · **Skill:** 1.2.3 · **Review:** [Guide 10 — DMS & Database Ingestion](../topic-guides/10-DMS-Database-Ingestion.md)
</details>

---

### Question 13
A data engineer uses AWS Glue DataBrew to prepare a customer dataset. Before the data is published, the team must confirm that `customer_id` is never missing and that at least 99% of `email` values match a valid pattern. The team also wants column statistics such as distinct counts and value distributions. The checks must be reusable for future versions of the dataset without writing code. Which combination of steps meets these requirements? (Select TWO.)

- **A.** Create a recipe job that fills missing `customer_id` values and applies a transform that reformats the `email` column.
- **B.** Create a DataBrew ruleset for the dataset that defines the two checks and their thresholds.
- **C.** Configure an AWS Glue crawler with a custom classifier that rejects rows that contain invalid email values.
- **D.** Run a DataBrew profile job on the dataset with the ruleset attached, so that it produces a data profile and a validation report.
- **E.** Enable Amazon Macie automated sensitive data discovery on the source bucket to validate the email column.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, D**

**Why B and D:** In DataBrew, a ruleset holds reusable data quality rules, each with a check and a threshold, such as "no missing values" or "99% of values match a pattern". A profile job calculates column statistics and correlations. When a ruleset is attached, the job also checks the rules and writes a validation report, so each new version of the dataset can be checked the same way.

**Why not the others:**
- **A** — Recipe transforms change the data. They don't measure or report whether it meets a threshold, and filling IDs would hide the problem.
- **C** — Classifiers decide the schema and format of files. They don't validate row values.
- **E** — Macie finds sensitive data such as PII. It doesn't check business-rule compliance or format thresholds.

**Signal words:** *"at least 99%"*, *"column statistics"*, *"reusable … without writing code"* · **Skill:** 3.4.2 · **Review:** [Guide 14 — Glue DataBrew & Data Preparation](../topic-guides/14-Glue-DataBrew-Data-Preparation.md)
</details>

---

### Question 14
A marketing team needs Salesforce opportunity records copied to an Amazon S3 data lake every day. Each run should transfer only the records created or changed since the previous run, the files must be in Apache Parquet format, and the team has no developers to write or maintain code. Which solution meets these requirements with the LEAST operational overhead?

- **A.** Create an Amazon AppFlow flow with Salesforce as the source, a daily schedule trigger with incremental transfer, and Amazon S3 as the destination with Parquet as the output format.
- **B.** Create an AWS Lambda function that calls the Salesforce REST API, stores the last-modified timestamp in DynamoDB, and writes Parquet files to S3, invoked once a day by an EventBridge schedule.
- **C.** Create an AWS DataSync task that uses the Salesforce organization as its source location and the S3 bucket as its destination location, and schedule it to run daily.
- **D.** Create an AWS Transfer Family SFTP server, and ask the Salesforce administrator to export the opportunity report every day and upload it to the server.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** Amazon AppFlow is a no-code service for moving data between SaaS applications and AWS. It has a Salesforce connector, schedule-triggered flows can pull only new and changed records, and it can write Parquet files to S3.

**Why not the others:**
- **B** — This works, but it is custom code for authentication, pagination, and state tracking that someone has to maintain, and the team has no developers.
- **C** — DataSync moves files and objects between storage systems such as NFS, SMB, HDFS, and object stores. It has no Salesforce connector.
- **D** — This is a manual process with no incremental logic and no Parquet output.

**Signal words:** *"Salesforce"*, *"only the records created or changed"*, *"no developers"* · **Skill:** 1.1.2 · **Review:** [Guide 11 — DataSync, Transfer Family, Snow & AppFlow](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md)
</details>

---

### Question 15
A financial services company runs dozens of AWS accounts in AWS Organizations and creates new accounts every month. A regulation requires that no customer data is processed or stored outside `eu-central-1` and `eu-west-1`. Account administrators have full IAM permissions in their own accounts. The control must be preventive and must apply to new accounts automatically. What should the data engineer recommend?

- **A.** Attach an IAM permissions boundary to every role in each account that allows actions only in the two approved Regions.
- **B.** Deploy an AWS Config rule to each account through a conformance pack that detects resources created outside the approved Regions and sends a notification through Amazon SNS.
- **C.** Disable every other AWS Region on each account's Account settings page, and repeat the process whenever a new account is created.
- **D.** Attach an SCP to the organization root that denies actions when `aws:RequestedRegion` isn't an approved Region, using `NotAction` to exempt global services.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Service control policies (SCPs) set the maximum permissions for every principal in the member accounts, including account administrators, and an SCP attached at the root or an OU covers new accounts as soon as they join. The standard data-residency SCP denies requests to other Regions, with `NotAction` exceptions for global services such as IAM, Organizations, Route 53, and CloudFront.

**Why not the others:**
- **A** — Administrators with full IAM permissions can remove or skip permissions boundaries, and each new role needs one attached.
- **B** — AWS Config detects noncompliant resources after they exist. It doesn't prevent them.
- **C** — Only opt-in Regions can be disabled. The Regions enabled by default can't be turned off, and the manual step doesn't cover new accounts.

**Signal words:** *"preventive"*, *"new accounts automatically"*, *"administrators have full IAM permissions"* · **Skill:** 4.5.5 · **Review:** [Guide 42 — Privacy, PII, Masking & Sovereignty](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md)
</details>

---

### Question 16
A company uses the AWS Glue Schema Registry with Apache Avro schemas for events on Amazon MSK. Producer teams always release schema changes first. Dozens of consumer applications owned by other teams upgrade on their own schedules, so at any time a consumer might be using any earlier schema version. Every consumer must be able to read data produced with the newest schema. Which compatibility mode meets this requirement while placing the fewest restrictions on schema changes?

- **A.** BACKWARD
- **B.** FORWARD_ALL
- **C.** FORWARD
- **D.** FULL

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Forward compatibility means that consumers on an older schema can read data written with a newer one, which fits the case where producers upgrade first. The `_ALL` variants check a new version against every earlier version, not just the previous one. Because consumers might be on any earlier version, FORWARD_ALL is required. FULL_ALL would also work, but it restricts changes more.

**Why not the others:**
- **A** — BACKWARD guarantees that consumers on the new schema can read old data, which is the case where consumers upgrade first.
- **C** — FORWARD checks only against the previous version, so a consumer two or more versions behind could break.
- **D** — FULL checks both directions, but only against the previous version, so it has the same gap as FORWARD and is stricter.

**Signal words:** *"producer teams always release schema changes first"*, *"any earlier schema version"*, *"fewest restrictions"* · **Skill:** 2.4.2 · **Review:** [Guide 13 — Glue Data Catalog & Crawlers](../topic-guides/13-Glue-Data-Catalog-Crawlers.md)
</details>

---

### Question 17
A company runs a long-running Amazon EMR cluster for nightly Apache Spark jobs. The jobs store intermediate data in HDFS. The company can accept longer runtimes, but not job failures caused by lost HDFS data or by losing the cluster itself. The company wants to reduce compute cost. Which configuration is the MOST cost-effective while meeting these requirements?

- **A.** Run the primary, core, and task nodes on Spot Instances, and enable termination protection on the cluster to guard against interruptions.
- **B.** Run the primary node and the core nodes on Spot Instances, and run the task nodes on On-Demand Instances.
- **C.** Run the primary node and the core nodes on On-Demand Instances, and add task nodes that run on Spot Instances.
- **D.** Run the primary node on a Spot Instance, and run the core nodes and the task nodes on On-Demand Instances.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Core nodes hold HDFS blocks, and the primary node manages the cluster, so losing either can fail jobs or the whole cluster. Task nodes only run compute, not HDFS. When a Spot task node is reclaimed, Spark reschedules its tasks elsewhere and the job runs longer instead of failing. Spot task nodes give the savings without the data risk.

**Why not the others:**
- **A** — Termination protection doesn't stop EC2 from reclaiming Spot capacity. Losing Spot core or primary nodes can lose HDFS data or the whole cluster.
- **B** — This puts the Spot risk on the nodes that store HDFS and run the cluster, and pays On-Demand prices for the nodes that could safely use Spot.
- **D** — If the Spot primary node is reclaimed, the cluster terminates.

**Signal words:** *"intermediate data in HDFS"*, *"can accept longer runtimes"*, *"MOST cost-effective"* · **Skill:** 1.2.4 · **Review:** [Guide 15 — Amazon EMR](../topic-guides/15-Amazon-EMR.md)
</details>

---

### Question 18
A retail company has an Amazon Redshift table `daily_sales(store_id, sales_date, revenue)` with exactly one row per store per day and no missing days. An analyst needs, for each store and day, the average revenue over that day and the six days before it. Which SQL expression gives the correct value?

- **A.** `AVG(revenue) OVER (PARTITION BY store_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`
- **B.** `AVG(revenue) OVER (PARTITION BY store_id ORDER BY sales_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)`
- **C.** `AVG(revenue) OVER (PARTITION BY sales_date ORDER BY store_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`
- **D.** `AVG(revenue)` with `GROUP BY store_id, DATE_TRUNC('week', sales_date)`

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** A 7-day rolling average is the current row plus the 6 rows before it, calculated separately for each store (`PARTITION BY store_id`) in date order. With one row per day and no gaps, a row-based frame equals seven calendar days.

**Why not the others:**
- **B** — `7 PRECEDING` plus the current row covers eight days.
- **C** — This partitions by date, so it averages across stores on the same day, not across days for each store.
- **D** — This gives a fixed calendar-week average, one value per store per week, not a rolling value for each day.

**Signal words:** *"that day and the six days before it"*, *"for each store and day"* · **Skill:** 3.2.6 · **Review:** [Guide 34 — SQL for Data Engineers](../topic-guides/34-SQL-for-Data-Engineers.md)
</details>

---

### Question 19
A logistics company receives shipment manifests from 300 external partners. The partners can only push files over SFTP with SSH key authentication. The files must land in Amazon S3, and each new file must automatically start a processing step. The company doesn't want to manage servers or patch operating systems. Which solution meets these requirements with the LEAST operational overhead?

- **A.** Launch Amazon EC2 instances in an Auto Scaling group that run OpenSSH, mount the S3 bucket with a FUSE-based file system, and start the processing step from a cron job that runs every minute.
- **B.** Deploy an AWS DataSync agent in the company's VPC, and give each partner its own DataSync location to upload files to.
- **C.** Create an AWS Transfer Family SFTP server that stores files in Amazon S3, add service-managed users with the partners' public keys, and start processing with a managed workflow.
- **D.** Create an Amazon AppFlow flow for each partner with SFTP as the source and Amazon S3 as the destination, triggered on a schedule.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** AWS Transfer Family provides fully managed SFTP, FTPS, FTP, and AS2 endpoints that read and write directly to S3 or EFS. Service-managed users authenticate with SSH public keys, and a managed workflow (or an S3 event) starts processing after each upload. There are no servers to patch.

**Why not the others:**
- **A** — This meets the functional needs but adds exactly the server management the company wants to avoid.
- **B** — DataSync runs transfer tasks between storage locations that you define. It isn't an SFTP endpoint that partners can push files to.
- **D** — AppFlow connects to SaaS applications. It doesn't give partners an SFTP server to upload to.

**Signal words:** *"only push files over SFTP"*, *"SSH key authentication"*, *"doesn't want to manage servers"* · **Skill:** 2.1.4 · **Review:** [Guide 11 — DataSync, Transfer Family, Snow & AppFlow](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md)
</details>

---

### Question 20
An analyst must be able to list and download objects only under the `reports/finance/` prefix of the `amzn-s3-demo-bucket` bucket by using the AWS CLI. The analyst must not be able to list or read any other prefix. Which statements should the data engineer include in the analyst's IAM policy? (Select TWO.)

- **A.** Allow `s3:ListBucket` on `arn:aws:s3:::amzn-s3-demo-bucket` with a `StringLike` condition on `s3:prefix` of `reports/finance/*`.
- **B.** Allow `s3:ListBucket` on `arn:aws:s3:::amzn-s3-demo-bucket/reports/finance/*`.
- **C.** Allow `s3:GetObject` on `arn:aws:s3:::amzn-s3-demo-bucket/*`.
- **D.** Allow `s3:*` on `arn:aws:s3:::amzn-s3-demo-bucket` and `arn:aws:s3:::amzn-s3-demo-bucket/*` with a `StringLike` condition on `s3:prefix` of `reports/finance/*`.
- **E.** Allow `s3:GetObject` on `arn:aws:s3:::amzn-s3-demo-bucket/reports/finance/*`.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, E**

**Why A and E:** `s3:ListBucket` is a bucket-level action, so its resource must be the bucket ARN, and the `s3:prefix` condition limits which keys can be listed. `s3:GetObject` is an object-level action, so its resource is the object ARN pattern under the allowed prefix.

**Why not the others:**
- **B** — ListBucket applies to the bucket, not to object ARNs, so this statement never matches a list request.
- **C** — This lets the analyst read every object in the bucket.
- **D** — `s3:*` is far too broad. The `s3:prefix` key applies only to list requests, so the statement's intent is unclear and it isn't least privilege.

**Signal words:** *"list and download … only under"*, *"must not be able to list or read any other prefix"* · **Skill:** 4.2.6 · **Review:** [Guide 37 — IAM for Data Engineers](../topic-guides/37-IAM-for-Data-Engineers.md)
</details>

---

### Question 21
An AWS Glue workflow contains two crawlers that catalog the `orders` and `customers` datasets, and a Glue ETL job that joins the two. The job must start only after both crawlers have finished successfully in the same workflow run, and it must run exactly once per run. What should the data engineer configure?

- **A.** Two conditional triggers, one watching each crawler for a SUCCEEDED state, each of which starts the ETL job.
- **B.** A scheduled trigger that starts the ETL job 45 minutes after the crawlers usually start their runs.
- **C.** An Amazon EventBridge rule that starts the ETL job as soon as the `orders` crawler emits a Succeeded state-change event for the current run.
- **D.** A conditional trigger with the ALL logical operator that watches both crawlers for a SUCCEEDED state and then starts the ETL job.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Glue conditional triggers can watch several jobs or crawlers. With the ALL operator, the trigger fires only when every watched item reaches the specified state in the current workflow run, and it starts the job once.

**Why not the others:**
- **A** — Each trigger fires on its own, so the job would start twice, and the first run could start before the second crawler finishes.
- **B** — A fixed delay doesn't know whether the crawlers succeeded or how long they actually took.
- **C** — This reacts to only one of the two crawlers.

**Signal words:** *"only after both crawlers"*, *"exactly once per run"* · **Skill:** 1.3.1 · **Review:** [Guide 21 — MWAA & Glue Workflows](../topic-guides/21-MWAA-Glue-Workflows.md)
</details>

---

### Question 22
A company runs an on-premises Apache Cassandra cluster for a time-series application that uses Cassandra Query Language (CQL) drivers. The company wants to move to AWS, stop managing clusters and nodes, have capacity adjust automatically to traffic, and change as little application code as possible. Which database should the data engineer choose?

- **A.** Amazon Keyspaces (for Apache Cassandra) in on-demand capacity mode.
- **B.** Amazon DynamoDB in on-demand capacity mode, with the data access layer rewritten for the DynamoDB API.
- **C.** Amazon DocumentDB (with MongoDB compatibility) running as an elastic cluster.
- **D.** Amazon Neptune Serverless, with the queries rewritten in openCypher.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** Amazon Keyspaces is a serverless, Cassandra-compatible database. Existing CQL drivers and queries work with few changes, there are no nodes to manage, and on-demand mode scales capacity with traffic.

**Why not the others:**
- **B** — DynamoDB is serverless too, but moving from CQL to the DynamoDB API is a rewrite.
- **C** — DocumentDB is compatible with MongoDB, not with Cassandra or CQL.
- **D** — Neptune is a graph database, which doesn't fit a time-series workload and would need a full rewrite.

**Signal words:** *"CQL drivers"*, *"change as little application code as possible"*, *"stop managing clusters"* · **Skill:** 2.1.2 · **Review:** [Guide 28 — RDS, Aurora & Purpose-Built DBs](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md)
</details>

---

### Question 23
A company's Amazon Redshift provisioned cluster serves business intelligence dashboards. Every Monday morning, hundreds of concurrent read queries wait in the WLM queue for several minutes. The cluster is lightly used the rest of the week. The company wants consistent dashboard performance during these spikes and wants to pay for extra capacity only while it is in use. What should the data engineer do?

- **A.** Perform an elastic resize every Monday morning to double the number of nodes, and resize the cluster back each Monday afternoon.
- **B.** Enable concurrency scaling for the WLM queue that serves the dashboard queries.
- **C.** Enable short query acceleration (SQA) and raise its maximum runtime to the duration of the longest dashboard query.
- **D.** Create a second manual WLM queue for the dashboard users and increase that queue's slot count to 50.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Concurrency scaling automatically adds temporary cluster capacity when queries start queuing, sends eligible queries to it, and removes it when the queue clears. You pay per second only while the extra capacity is running, and clusters also earn free concurrency-scaling credits. This matches short, predictable bursts.

**Why not the others:**
- **A** — Resizing is a scheduled workaround with its own operational steps and brief interruptions, and you pay for the extra nodes for the whole window.
- **C** — SQA only prioritizes short queries, and its maximum runtime is capped at a few seconds. It doesn't add capacity.
- **D** — More slots split the same memory and CPU into smaller pieces, so each query gets fewer resources.

**Signal words:** *"hundreds of concurrent read queries wait"*, *"pay for extra capacity only while it is in use"* · **Skill:** 3.3.4 · **Review:** [Guide 25 — Redshift Performance, Operations & Security](../topic-guides/25-Redshift-Performance-Operations-Security.md)
</details>

---

### Question 24
An AWS Glue ETL job uses a JDBC connection to read from an Amazon RDS for PostgreSQL database in a private subnet and writes the results to Amazon S3. The Glue connection uses the same security group as the database, and the VPC has no NAT gateway. Test runs show two problems: the job's connections to the database time out, and the job reports that it can't find an S3 endpoint or NAT gateway for the connection's subnet. Which combination of actions will resolve both problems? (Select TWO.)

- **A.** Turn on the Glue job's option to assign a public IP address so that the job can reach Amazon S3 over the internet.
- **B.** Add an inbound rule to the database security group that allows port 5432 from `0.0.0.0/0`.
- **C.** Add a self-referencing inbound rule for all TCP traffic to the security group that the Glue connection uses.
- **D.** Create an S3 gateway VPC endpoint and associate it with the route table of the connection's subnet.
- **E.** Move the Glue connection to a public subnet whose route table points to an internet gateway.

<details>
<summary><b>Show answer</b></summary>

**Answer: C, D**

**Why C and D:** Glue places elastic network interfaces in the connection's subnet. Its Spark components must be able to talk to each other, so the connection's security group needs a self-referencing inbound rule for all TCP. Because the database uses the same group, that rule also allows the traffic to the database. Glue needs a route to S3 from that subnet, and without a NAT gateway, an S3 gateway endpoint provides it privately.

**Why not the others:**
- **A** — Glue jobs don't have an option to assign public IP addresses.
- **B** — Opening the database to the internet is unsafe, and it doesn't fix Glue's worker-to-worker traffic or the S3 route.
- **E** — Glue network interfaces don't get public IP addresses, so an internet gateway route alone doesn't provide internet access.

**Signal words:** *"same security group as the database"*, *"can't find an S3 endpoint or NAT gateway"* · **Skill:** 1.2.2 · **Review:** [Guide 38 — Networking for Data Pipelines](../topic-guides/38-Networking-for-Data-Pipelines.md)
</details>

---

### Question 25
An AWS Glue job must pull data from a partner's REST API. The partner accepts connections only from a small set of static public IP addresses that it adds to an allowlist. The job currently runs without any VPC connection. What should the data engineer do so that the partner can allowlist the job's traffic?

- **A.** Send the partner the AWS Glue service IP address ranges for the Region, as published in the `ip-ranges.json` file, and ask the partner to refresh its allowlist whenever the file changes.
- **B.** Allocate an Elastic IP address and associate it with the Glue job in the job properties so that all job traffic uses that address.
- **C.** Create an interface VPC endpoint for the partner's API in the company's VPC, and give the partner the endpoint's private IP addresses.
- **D.** Attach a Glue network connection in a private subnet that routes internet traffic through a NAT gateway with an Elastic IP address, and give the partner that address.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** When a Glue job uses a network connection, its traffic comes from network interfaces in your subnet. If that subnet routes internet traffic through a NAT gateway, all outbound calls use the NAT gateway's Elastic IP address, which is one stable address the partner can allowlist. The same pattern works for Lambda functions and other VPC workloads.

**Why not the others:**
- **A** — Those ranges are large, shared with every AWS customer, and change over time. Allowlisting them effectively opens the API to anyone on AWS.
- **B** — Glue jobs have no setting for attaching an Elastic IP address.
- **C** — An interface endpoint only works if the partner offers its API as an AWS PrivateLink service. The partner allowlists public addresses, so it doesn't.

**Signal words:** *"static public IP addresses"*, *"allowlist"* · **Skill:** 1.1.8 · **Review:** [Guide 38 — Networking for Data Pipelines](../topic-guides/38-Networking-for-Data-Pipelines.md)
</details>

---

### Question 26
A company wants to build a retrieval augmented generation (RAG) assistant over 50,000 internal policy documents stored in Amazon S3. The documents change every week. The company wants AWS to handle document parsing, chunking, embedding generation, and vector storage, and it doesn't want to train or fine-tune models. Which solution meets these requirements with the LEAST operational overhead?

- **A.** Fine-tune a foundation model in Amazon Bedrock on the documents every week, and have the assistant call the most recent custom model.
- **B.** Write an AWS Glue job that splits the documents, calls an embeddings model for each chunk, and loads the vectors into Aurora PostgreSQL with pgvector, and write the retrieval code in Lambda.
- **C.** Create an Amazon Bedrock knowledge base with the S3 bucket as a data source, an embeddings model, and a vector store, and sync the data source after each weekly update.
- **D.** Create an Amazon Kendra index for the documents, and connect the assistant to the Kendra Retrieve API to fetch relevant passages.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** A Bedrock knowledge base is managed RAG. It reads documents from the data source, chunks them using a strategy you choose (fixed-size, hierarchical, semantic, or none), creates embeddings, and writes them to a vector store such as OpenSearch Serverless, S3 Vectors, or Aurora PostgreSQL. A sync (ingestion job) processes only added, changed, and deleted documents.

**Why not the others:**
- **A** — Fine-tuning is training, which the company rules out. It also doesn't retrieve current documents or show sources.
- **B** — This is a valid do-it-yourself RAG pipeline, but the company would own all of the chunking, embedding, loading, and retrieval code.
- **D** — ⚠️ Amazon Kendra has been in maintenance mode and closed to new customers since July 30, 2026, so it's the wrong choice for a new solution, even though it's still on the exam's service list.

**Signal words:** *"handle document parsing, chunking, embedding generation, and vector storage"*, *"doesn't want to train or fine-tune"*, *"change every week"* · **Skill:** 2.4.6 · **Review:** [Guide 19 — GenAI, LLMs & Vectors](../topic-guides/19-GenAI-LLMs-Vectors.md)
</details>

---

### Question 27
AWS Glue jobs write their logs to Amazon CloudWatch Logs. After a failed overnight run, a data engineer must quickly count the ERROR messages for each job run over the last 24 hours and find the most frequent error messages. The analysis is a one-time investigation, and the engineer doesn't want to move or copy the logs. Which approach meets these requirements with the LEAST effort?

- **A.** Export the log groups to Amazon S3, create a table over the exported files, and query them with Amazon Athena.
- **B.** Create a metric filter on the log groups that counts lines containing ERROR, and graph the resulting metric on a dashboard.
- **C.** Create a subscription filter that streams the log groups to an Amazon OpenSearch Service domain, and build a dashboard.
- **D.** Run a CloudWatch Logs Insights query that filters for ERROR and aggregates with `stats count()` by log stream and message.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** CloudWatch Logs Insights queries log data where it already is, with filter, parse, and stats commands, and returns results within seconds. It's built for one-time investigations over recent logs.

**Why not the others:**
- **A** — Exporting copies the data, can take a while, and means creating a table, which is more effort than needed.
- **B** — A metric filter counts only events that arrive after the filter is created, and it can't show the individual messages.
- **C** — Streaming logs into OpenSearch copies the data and requires a domain, which is too much for one investigation.

**Signal words:** *"one-time investigation"*, *"doesn't want to move or copy the logs"*, *"LEAST effort"* · **Skill:** 3.3.8 · **Review:** [Guide 32 — Monitoring, Logging & Troubleshooting](../topic-guides/32-Monitoring-Logging-Troubleshooting.md)
</details>

---

### Question 28
An auditor asks a data engineering team to show every change made during the past 6 months to the configuration of the S3 buckets in a data lake account, including bucket policies and default encryption settings. The auditor also wants any bucket that stops enforcing encryption to be flagged continuously from now on. AWS Config and AWS CloudTrail have been enabled in the account since it was created. What should the team use?

- **A.** The AWS Config configuration timeline for each bucket, plus an AWS Config managed rule that evaluates bucket encryption settings.
- **B.** CloudTrail event history in the console, filtered for `PutBucketPolicy` and `PutBucketEncryption` events and exported to CSV for the auditor each month.
- **C.** Amazon Macie automated sensitive data discovery, with findings sent to AWS Security Hub for review.
- **D.** S3 server access logging for each bucket, with the log files queried through Amazon Athena.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** AWS Config records a configuration item every time a supported resource changes, and the configuration timeline shows the full state of the resource at each point, linked to the CloudTrail event that caused the change. Config rules evaluate resources continuously and flag noncompliant ones, such as buckets without the required encryption.

**Why not the others:**
- **B** — Event history covers only the last 90 days and lists API calls, not the resulting configuration state. It also can't evaluate compliance going forward.
- **C** — Macie finds sensitive data in objects. It doesn't track configuration history.
- **D** — Server access logs record object and bucket requests, not configuration history, and they don't evaluate compliance.

**Signal words:** *"every change … to the configuration"*, *"flagged continuously"* · **Skill:** 4.5.4 · **Review:** [Guide 43 — Audit Logging, CloudTrail & Config](../topic-guides/43-Audit-Logging-CloudTrail-Config.md)
</details>

---

### Question 29
An AWS Lambda function enriches incoming records by looking up values in a 6 GB reference dataset. A separate process updates the dataset every hour, and every concurrent invocation must see the latest version. Downloading the file from Amazon S3 in each new execution environment adds too much latency. Which solution meets these requirements?

- **A.** Increase the function's ephemeral storage (`/tmp`) to 10,240 MB and download the dataset from Amazon S3 during function initialization.
- **B.** Package the dataset in a Lambda layer, publish a new layer version every hour, and update the function configuration so that it always uses the newest layer version.
- **C.** Connect the function to the VPC, mount an Amazon EFS file system through an access point, and have the update process write the dataset to that file system.
- **D.** Build the dataset into the function's container image, and redeploy the function after each hourly update of the dataset.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Amazon EFS is shared, persistent storage. Every execution environment of the function mounts the same file system, so updates are visible to all invocations right away and nothing is downloaded per environment. Lambda mounts EFS through an access point and must be connected to the VPC to reach the mount targets.

**Why not the others:**
- **A** — `/tmp` is private to each execution environment. Every environment would download its own copy (the latency the team wants to avoid), and copies go stale after each hourly update.
- **B** — Layers are limited to 250 MB unzipped for the function and its layers together, and republishing every hour is awkward.
- **D** — This technically works, but hourly redeployments add latency and operational churn, and environments running the old image still use stale data.

**Signal words:** *"every concurrent invocation must see the latest version"*, *"6 GB"*, *"updates the dataset every hour"* · **Skill:** 1.4.7 · **Review:** [Guide 17 — Lambda for Data Pipelines](../topic-guides/17-Lambda-for-Data-Pipelines.md)
</details>

---

### Question 30
A company enabled versioning on an S3 bucket that stores raw ingestion files, and a lifecycle rule expires current object versions after 30 days. Storage costs keep rising anyway. S3 Storage Lens shows that most of the bucket's bytes are noncurrent object versions and incomplete multipart uploads. Which lifecycle actions should the data engineer add? (Select TWO.)

- **A.** A `NoncurrentVersionExpiration` action that permanently deletes noncurrent versions a set number of days after they become noncurrent.
- **B.** A transition that moves current object versions to the S3 Glacier Deep Archive storage class 1 day after creation.
- **C.** A rule that enables S3 Object Lock in governance mode on the bucket so that old versions are retained in a controlled way.
- **D.** An `AbortIncompleteMultipartUpload` action that removes the parts of uploads that aren't completed within a few days.
- **E.** An `ExpiredObjectDeleteMarker` action on its own, with no other changes to the existing rule.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, D**

**Why A and D:** In a versioned bucket, expiring a current version only adds a delete marker, and the data stays on as a noncurrent version that you keep paying for. `NoncurrentVersionExpiration` removes those versions. Parts from incomplete multipart uploads are also billed until they are aborted, and `AbortIncompleteMultipartUpload` cleans them up.

**Why not the others:**
- **B** — This changes the class of current objects, and objects moved that quickly are subject to the class's minimum storage duration. It doesn't touch the noncurrent bytes or the multipart parts.
- **C** — Object Lock prevents deletion, which makes the problem worse.
- **E** — This removes a delete marker only when no noncurrent versions remain behind it. It doesn't free the noncurrent bytes.

**Signal words:** *"versioning"*, *"noncurrent object versions and incomplete multipart uploads"* · **Skill:** 2.3.4 · **Review:** [Guide 05 — S3 Data Lake Storage](../topic-guides/05-S3-Data-Lake-Storage.md)
</details>

---

### Question 31
A telecom company wants to generate a short summary and a sentiment label for each of about 3 million customer support transcripts every night by using a foundation model in Amazon Bedrock. The results are needed by the next morning, not in real time. The company wants the lowest cost and as little custom throttling and retry logic as possible. Which solution meets these requirements?

- **A.** Invoke an AWS Lambda function for each transcript that calls the Bedrock InvokeModel API on demand, with exponential backoff for throttling errors.
- **B.** Write the prompts as JSONL records to Amazon S3, and submit an Amazon Bedrock batch inference job that writes the results to S3.
- **C.** Purchase Provisioned Throughput for the model, and call the model from an Amazon EMR cluster that processes the transcripts overnight.
- **D.** Train a custom summarization and sentiment model in Amazon SageMaker AI every night on the latest transcripts, and run it against the new data.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Bedrock batch inference takes a JSONL file of model inputs from S3, runs them asynchronously, and writes the outputs to S3. For supported models it costs less than on-demand invocation (about half), and Bedrock handles scheduling and throughput, so the pipeline doesn't need its own throttling logic. A Step Functions or EventBridge workflow can submit the job and watch for completion.

**Why not the others:**
- **A** — Millions of on-demand calls cost more and hit throttling quotas, which means writing retry logic.
- **C** — Provisioned Throughput is paid for by the hour for a committed term. That's expensive for a nightly burst, and the EMR cluster adds work.
- **D** — Model training is out of scope for this task and much more work than calling an existing foundation model.

**Signal words:** *"3 million … every night"*, *"not in real time"*, *"lowest cost"* · **Skill:** 1.2.10 · **Review:** [Guide 19 — GenAI, LLMs & Vectors](../topic-guides/19-GenAI-LLMs-Vectors.md)
</details>

---

### Question 32
A company runs a shared Amazon EMR on EC2 cluster whose workload varies widely during the day. The team maintains custom automatic scaling rules based on CloudWatch metrics, and the rules often scale too late. The company wants EMR to resize the cluster automatically based on the workload, keep core node capacity On-Demand, and cap the total size of the cluster. What should the data engineer do?

- **A.** Enable EMR managed scaling, and set the minimum, maximum, maximum core node, and maximum On-Demand capacity limits.
- **B.** Tune the existing custom automatic scaling rules to use shorter CloudWatch evaluation periods and larger scaling adjustments for each rule.
- **C.** Schedule an AWS Lambda function with Amazon EventBridge to resize the task instance group every hour based on the previous day's usage pattern.
- **D.** Move all the jobs to a single EMR Serverless application and set the application's maximum capacity to the current size of the cluster.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** EMR managed scaling watches workload metrics at a fine grain and adds or removes capacity automatically, with no rules to maintain. Its limits cap the total cluster size, cap how much of it can be core nodes, and cap how much can be On-Demand, so any capacity above the On-Demand limit comes from Spot Instances.

**Why not the others:**
- **B** — Tuning the rules still depends on rules the team maintains and on metrics that respond slowly, which is the current problem.
- **C** — A schedule based on yesterday's pattern doesn't react to today's actual workload.
- **D** — EMR Serverless scales automatically, but it means migrating every job off the shared cluster and has no core nodes to keep On-Demand. The company asked for the cluster to resize.

**Signal words:** *"resize the cluster automatically based on the workload"*, *"keep core node capacity On-Demand"*, *"cap the total size"* · **Skill:** 3.1.4 · **Review:** [Guide 15 — Amazon EMR](../topic-guides/15-Amazon-EMR.md)
</details>

---

### Question 33
A sales table in Amazon Redshift contains rows for 12 regions. Each sales representative may see only the rows for the regions assigned to them in a `region_assignments` table, which changes every week as territories move. All representatives query the same table through a BI tool that connects with their own database users. Which solution meets these requirements with the LEAST ongoing maintenance?

- **A.** Create one view per region with a WHERE clause on the region column, and update the GRANT statements on the twelve views every week when the territories change.
- **B.** Create dynamic data masking policies that replace the values in rows from other regions with NULL for each representative.
- **C.** Create an RLS policy that looks up the user's regions in `region_assignments` by `current_user`, attach it to their role, and enable RLS on the table.
- **D.** Register the table with AWS Lake Formation, and create a data filter for each representative that lists that representative's regions.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** A Redshift row-level security (RLS) policy is a predicate that's evaluated at query time. It can look up the current user in a mapping table, so weekly territory changes are just updates to `region_assignments`. The policy itself, and every query, stay the same. Attach the policy to a role and turn on RLS for the table.

**Why not the others:**
- **A** — Twelve views plus a weekly round of GRANT changes is the ongoing maintenance the question asks you to avoid.
- **B** — Masking changes column values. It doesn't remove rows, and the representatives would still see other regions' records.
- **D** — Lake Formation data filters apply to Data Catalog tables that engines such as Athena, EMR, and Redshift Spectrum query, not to a local Redshift table. A filter per person would also need constant updates.

**Signal words:** *"only the rows"*, *"changes every week"*, *"LEAST ongoing maintenance"* · **Skill:** 4.2.3 · **Review:** [Guide 25 — Redshift Performance, Operations & Security](../topic-guides/25-Redshift-Performance-Operations-Security.md)
</details>

---

### Question 34
A company stores application logs in an Amazon OpenSearch Service domain with one index per day. Logs are searched heavily for the first 7 days. After that they are only searched occasionally and can be read-only, and they must be deleted after 90 days. The domain already has dedicated master nodes and UltraWarm nodes. The company wants the MOST cost-effective solution, with no manual steps. What should the data engineer do?

- **A.** Run a cron job on an Amazon EC2 instance that calls the `_reindex` API to copy indexes older than 7 days to a smaller domain and then deletes the original indexes when they are 90 days old.
- **B.** Take a manual snapshot to Amazon S3 each day, delete indexes older than 7 days from the domain, and restore snapshots whenever someone needs to search older logs.
- **C.** Add an S3 Lifecycle rule to the bucket that holds the domain's automated snapshots so that snapshot data moves to S3 Glacier Flexible Retrieval after 7 days.
- **D.** Create an Index State Management (ISM) policy that moves indexes to UltraWarm after 7 days and deletes them after 90 days, and apply it to new indexes with an ISM template.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Index State Management automates index lifecycles. You define states (for example hot → warm → delete) and the conditions for moving between them, such as index age. The `warm_migration` action moves an index to UltraWarm, which is read-only, S3-backed storage that costs much less per GB than hot storage and can still be searched. An ISM template attaches the policy to each new daily index automatically.

**Why not the others:**
- **A** — Custom scripts on a second domain add cost and operational work.
- **B** — Restoring before every search is manual and slow, and the requirement says no manual steps.
- **C** — The service manages automated snapshots. They aren't stored in a bucket you control, and moving snapshots doesn't reduce the storage cost of indexes on the domain.

**Signal words:** *"searched occasionally … read-only"*, *"deleted after 90 days"*, *"no manual steps"* · **Skill:** 2.1.1 · **Review:** [Guide 29 — OpenSearch Service](../topic-guides/29-OpenSearch-Service.md)
</details>

---

### Question 35
A retail company streams clickstream JSON events into Amazon Kinesis Data Streams. Each event must be enriched by joining it with a product reference table in Amazon S3, deduplicated by event ID, and written to S3 as Parquet files partitioned by hour, within a few minutes of arrival. The team wants to reuse its existing PySpark transformation code and doesn't want to manage clusters. Which solution meets these requirements?

- **A.** Create an Amazon Data Firehose (Kinesis Data Firehose) stream that reads from the Kinesis stream, rewrite the transformation logic as a Lambda function, and enable record format conversion to Parquet.
- **B.** Create an AWS Glue streaming ETL job that reads from the Kinesis stream, runs the PySpark transformations in micro-batches with an S3 checkpoint location, and writes partitioned Parquet files.
- **C.** Create a long-running Amazon EMR on EC2 cluster that runs the PySpark code as a Spark Structured Streaming application, and write the output to S3.
- **D.** Schedule an AWS Glue Python shell job every 5 minutes with Amazon EventBridge that reads new records with the GetRecords API and writes Parquet files to S3.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Glue streaming ETL jobs run Spark Structured Streaming serverlessly against Kinesis Data Streams, Amazon MSK, or Apache Kafka. They process micro-batches (a window size you choose), store progress in a checkpoint location so they can recover, and can join with static reference data. The existing PySpark code carries over.

**Why not the others:**
- **A** — Firehose can convert to Parquet, but the Spark logic would have to be rewritten as a Lambda function, which rules out reusing the code.
- **C** — This runs the code, but the team would have to manage a cluster.
- **D** — Python shell jobs don't run Spark, and polling shards by hand means managing iterators and checkpoints yourself.

**Signal words:** *"reuse its existing PySpark"*, *"within a few minutes"*, *"doesn't want to manage clusters"* · **Skill:** 1.1.1 · **Review:** [Guide 12 — AWS Glue ETL](../topic-guides/12-AWS-Glue-ETL.md)
</details>

---

### Question 36
An AWS Glue Spark job reads one day of JSON data from Amazon S3 with a Spark DataFrame reader (`spark.read.json`). The data is about 2 million objects, most under 20 KB, written by an Amazon Data Firehose stream with a 1 MiB buffer size. The job fails with driver out-of-memory errors while the executors are mostly idle. Which actions address the root cause? (Select TWO.)

- **A.** Double the number of G.1X workers allocated to the job so that more executors are available.
- **B.** Read the data as a Glue DynamicFrame with `groupFiles` set to `inPartition` and a `groupSize` of about 128 MB.
- **C.** Increase the Firehose buffer size and interval so that the stream writes fewer, larger objects.
- **D.** Increase the job timeout so that the driver has more time to list and plan all of the input files.
- **E.** Change the job to the Flex execution class so that it runs on spare capacity at lower cost.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, C**

**Why B and C:** This is the small-files problem. The driver has to track millions of files and splits, and it runs out of memory while the executors wait. Grouping files on read (a Glue DynamicFrame feature) combines many small files into each task, which reduces the load on the driver. Fixing the source so it writes larger objects removes the problem for every downstream reader.

**Why not the others:**
- **A** — More executors don't reduce the driver's memory load, and the executors are already idle.
- **D** — The job fails from memory, not from time.
- **E** — Flex lowers cost for jobs that don't need to start right away. It doesn't change driver memory use.

**Signal words:** *"2 million objects, most under 20 KB"*, *"driver out-of-memory"*, *"executors are mostly idle"* · **Skill:** 3.3.6 · **Review:** [Guide 12 — AWS Glue ETL](../topic-guides/12-AWS-Glue-ETL.md)
</details>

---

### Question 37
A company's security team created an organization event data store in AWS CloudTrail Lake in 2025. Auditors now want the team to run SQL queries across 7 years of management events from every account in the organization. The team doesn't want to create or maintain tables, partitions, or crawlers, and the events must be kept in a store that users can't modify. What should the team do?

- **A.** Query CloudTrail event history in each member account, export the results as CSV files, and combine them in a shared S3 bucket.
- **B.** Set the retention period of the organization event data store to 7 years, and run the audit queries in CloudTrail Lake.
- **C.** Create an organization trail that delivers to Amazon S3, catalog the log files with an AWS Glue crawler, and query them with Amazon Athena.
- **D.** Create an AWS Config aggregator for the organization, and run advanced queries against it to find the API activity the auditors need.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** A CloudTrail Lake event data store is a managed, immutable store of events. An organization event data store collects events from every member account, retention can be set to many years, and you query it with SQL directly, with no tables or partitions to manage.

**Why not the others:**
- **A** — Event history covers only the last 90 days in each account.
- **C** — This works, but the team would have to maintain crawlers, tables, and partitions, which the requirements rule out.
- **D** — Config advanced queries return current resource configuration, not a history of API calls.

> ⚠️ **2026 status:** CloudTrail Lake has been closed to new customers since May 31, 2026. Existing customers (like this company) keep using it, and AWS points new customers to CloudWatch for similar capabilities. The exam guide still names it in skill 4.4.3, so know it.

**Signal words:** *"SQL queries across 7 years"*, *"every account in the organization"*, *"no tables, partitions, or crawlers"*, *"can't modify"* · **Skill:** 4.4.3 · **Review:** [Guide 43 — Audit Logging, CloudTrail & Config](../topic-guides/43-Audit-Logging-CloudTrail-Config.md)
</details>

---

### Question 38
Five independent consumer applications read the same Amazon Kinesis Data Streams stream with the shared-throughput GetRecords API. The consumers receive `ReadProvisionedThroughputExceeded` errors, and end-to-end latency is more than 1 second. Each application needs its own read throughput and a latency of about 70 ms. Which solution meets these requirements?

- **A.** Increase the stream's data retention period to 7 days so that consumers that fall behind can catch up later.
- **B.** Double the number of shards in the stream, and configure each consumer to call GetRecords more often.
- **C.** Register each application as an enhanced fan-out consumer, and read the data with SubscribeToShard.
- **D.** Add an Amazon SQS queue for each application, and use a Lambda function to copy every record from the stream to all five queues.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Enhanced fan-out gives each registered consumer its own 2 MB/s of read throughput per shard. Records are pushed over HTTP/2 (SubscribeToShard), with typical latency around 70 ms. Consumers no longer compete for the shard's shared read limits.

**Why not the others:**
- **A** — Longer retention doesn't add read throughput or lower latency.
- **B** — More shards add capacity, but every consumer still shares each shard's GetRecords limits, and polling more often causes more throttling. Latency stays in the hundreds of milliseconds.
- **D** — This adds code, cost, and extra hops, which increases latency.

**Signal words:** *"five independent consumer applications"*, *"its own read throughput"*, *"about 70 ms"* · **Skill:** 1.1.10 · **Review:** [Guide 06 — Kinesis Data Streams](../topic-guides/06-Kinesis-Data-Streams.md)
</details>

---

### Question 39
Analysts using Amazon Redshift occasionally need to join the current state of a small `customer_status` table in Amazon RDS for PostgreSQL with historical fact tables in Redshift. Results must reflect the RDS data as of query time. The database team won't allow any replication to be configured on the RDS instance, and the data engineering team doesn't want to build a pipeline. Which solution meets these requirements?

- **A.** Create a Redshift external schema `FROM POSTGRES` with a Secrets Manager secret, and join the federated table in queries.
- **B.** Create a zero-ETL integration from the RDS for PostgreSQL instance to Redshift, and query the replicated copy of the table.
- **C.** Unload the RDS table to Amazon S3 every night, and query the files through a Redshift Spectrum external table.
- **D.** Export RDS snapshots to Amazon S3 every day, and build a Redshift materialized view on a Spectrum table over the export.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** Redshift federated queries let Redshift query live tables in RDS or Aurora PostgreSQL and MySQL through an external schema, with credentials kept in Secrets Manager. No data is copied or replicated, and results reflect the source at query time, which suits occasional joins with a small table.

**Why not the others:**
- **B** — Zero-ETL is a managed replication integration, and the database team doesn't allow replication. Results would also lag slightly behind the source instead of reflecting it at query time.
- **C** — A nightly unload returns data up to a day old and is a pipeline to maintain.
- **D** — Daily snapshot exports are stale, and the setup is a pipeline.

**Signal words:** *"as of query time"*, *"won't allow any replication"*, *"occasionally"* · **Skill:** 2.1.5 · **Review:** [Guide 24 — Redshift Loading, Integration & Sharing](../topic-guides/24-Redshift-Loading-Integration-Sharing.md)
</details>

---

### Question 40
A data science team wants to explore a data lake interactively with PySpark: run notebook cells, see results in seconds, and chart samples of the data. The team doesn't want to provision or manage clusters, and it wants to pay only for the compute used during its sessions. The data is cataloged in the AWS Glue Data Catalog. Which solution meets these requirements?

- **A.** Launch a long-running Amazon EMR on EC2 cluster with JupyterHub, and give each data scientist a notebook user on the cluster.
- **B.** Create AWS Glue Python shell jobs for each analysis step, and run them from the Glue console as each step is needed.
- **C.** Use Amazon Redshift query editor v2 notebooks, and query the data lake through Redshift Spectrum with SQL cells.
- **D.** Use Amazon Athena for Apache Spark notebooks, which run PySpark in serverless sessions against the Data Catalog tables.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Athena for Apache Spark runs interactive Spark code serverlessly. Sessions start in seconds, capacity scales automatically, and you pay for the compute (DPUs) used during each session. You can use Jupyter-compatible notebooks in the Athena console or SageMaker Unified Studio notebooks, where Athena Spark is the default runtime.

**Why not the others:**
- **A** — A long-running cluster means cluster management and paying for idle time.
- **B** — Python shell jobs are batch jobs without Spark or interactive cells.
- **C** — These notebooks run SQL, not PySpark.

**Signal words:** *"interactively with PySpark"*, *"doesn't want to provision or manage clusters"*, *"pay only for … sessions"* · **Skill:** 3.2.4 · **Review:** [Guide 26 — Amazon Athena](../topic-guides/26-Amazon-Athena.md)
</details>

---

### Question 41
A company launches a transient Amazon EMR cluster every night. The cluster uses instance groups, and its task instance group requests Spot Instances of a single instance type. The cluster often can't get enough Spot capacity, which delays jobs. The company wants to keep using Spot for task capacity and improve its chances of getting that capacity. What should the data engineer do?

- **A.** Set a higher maximum Spot price for the task instance group so that the cluster outbids other accounts' Spot requests for the same instance type and keeps its capacity when prices rise.
- **B.** Change the task instance group to a larger instance type in the same family so that fewer Spot Instances are needed.
- **C.** Purchase Reserved Instances for the task instance type in the Region, and keep the Spot request as it is.
- **D.** Use instance fleets that list several similar instance types across multiple subnets, with the price-capacity-optimized allocation strategy for Spot.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Instance fleets let EMR choose from several instance types and Availability Zones (through the subnets you list). The price-capacity-optimized strategy draws from the Spot pools with the most available capacity and a low price. Diversifying across instance types is the main way to get Spot capacity and reduce interruptions.

**Why not the others:**
- **A** — Spot prices aren't set by bidding. A higher maximum price doesn't create capacity in a pool that has none.
- **B** — Changing to another single type keeps the dependence on one capacity pool.
- **C** — Reserved Instances are a billing discount for On-Demand usage. They don't improve Spot availability.

**Signal words:** *"single instance type"*, *"can't get enough Spot capacity"* · **Skill:** 1.3.2 · **Review:** [Guide 15 — Amazon EMR](../topic-guides/15-Amazon-EMR.md)
</details>

---

### Question 42
An Amazon Data Firehose stream in account 111122223333 delivers data to an S3 bucket in account 444455556666. The bucket's default encryption is SSE-KMS with a customer managed key in account 444455556666. Deliveries fail with a KMS AccessDenied error. The bucket policy already allows the Firehose role to write objects. Which changes are required? (Select TWO.)

- **A.** In account 444455556666, add a statement to the key policy that allows the Firehose role from account 111122223333 to use `kms:GenerateDataKey` and `kms:Decrypt`.
- **B.** Change the bucket's default encryption to the AWS managed key `aws/s3` in account 444455556666.
- **C.** Enable S3 Bucket Keys on the destination bucket to reduce the number of requests sent to AWS KMS.
- **D.** In account 111122223333, add an IAM policy to the Firehose role that allows `kms:GenerateDataKey` and `kms:Decrypt` on the key ARN in account 444455556666.
- **E.** Convert the key to a multi-Region key, and create a replica of it in account 111122223333 so that Firehose can use a local copy of the key in its own account.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, D**

**Why A and D:** Using a KMS key from another account takes permission on both sides. The key policy in the key's account must allow the external principal, and that principal's IAM policy in its own account must allow the KMS actions on the key ARN. Firehose needs to create data keys to encrypt the objects it writes.

**Why not the others:**
- **B** — The key policies of AWS managed keys can't be changed, so they can't be used across accounts.
- **C** — Bucket Keys reduce KMS request volume and cost. They don't grant permission.
- **E** — Multi-Region replica keys live in other Regions of the same account, not in another account.

**Signal words:** *"customer managed key in account 444455556666"*, *"KMS AccessDenied"*, *"bucket policy already allows"* · **Skill:** 4.3.3 · **Review:** [Guide 39 — Encryption, KMS & Secrets](../topic-guides/39-Encryption-Key-Management.md)
</details>

---

### Question 43
A company keeps a customer dimension table in Amazon Redshift. Customers change their addresses several times a year. The finance team must report sales by the address that was valid when each sale happened, and also by each customer's current address. Which dimension design meets these requirements?

- **A.** Overwrite the address in place with a MERGE statement on each load so that the dimension always holds each customer's latest address.
- **B.** Add a surrogate key, effective dates, and a current flag. On each change, expire the old row and insert a new row, and link facts to the surrogate key.
- **C.** Add a `previous_address` column, and on each change move the current address into it before writing the new address into the address column.
- **D.** Keep only current values in the dimension, and store every change event in an S3 audit log that analysts join to through Redshift Spectrum whenever they need history.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** This is a Type 2 slowly changing dimension (SCD2). Each version of a customer gets its own row and surrogate key, with a validity period. Facts join to the version that was current when the sale happened, and filtering on the current flag gives current-address reporting.

**Why not the others:**
- **A** — Type 1 overwrites history, so point-in-time reporting isn't possible.
- **C** — Type 3 keeps only one previous value, which isn't enough for several changes a year.
- **D** — Reconstructing point-in-time addresses from raw change logs in every query is complex and slow. It isn't a dimensional design.

**Signal words:** *"valid when each sale happened"*, *"several times a year"*, *"and also by … current address"* · **Skill:** 2.4.1 · **Review:** [Guide 30 — Data Modeling, Schema Evolution & Lineage](../topic-guides/30-Data-Modeling-Schema-Evolution-Lineage.md)
</details>

---

### Question 44
A data team is building a serverless pipeline from three AWS Lambda functions, an AWS Step Functions state machine, and an Amazon DynamoDB table. The team wants to define all of the resources in one template with shorthand syntax, test the functions locally before deploying, and deploy repeatably from a CI pipeline. Which approach meets these requirements with the LEAST effort?

- **A.** Write an AWS SAM template with `AWS::Serverless::*` resources, test with `sam local invoke`, and deploy with `sam build` and `sam deploy`.
- **B.** Create the resources in the AWS Management Console, record every step in a runbook, and have an engineer repeat the steps for each environment.
- **C.** Publish each function to the AWS Serverless Application Repository, and deploy the applications from the console in each environment.
- **D.** Write a plain CloudFormation template with `AWS::Lambda::Function` resources, zip and upload the code to S3 by hand before each deployment, and test only after deploying to a development account.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** AWS SAM is a CloudFormation extension built for serverless applications. Its resource types shorten function, state machine, and table definitions. The SAM CLI builds and packages the code, runs functions locally in containers, and deploys through CloudFormation, so the same commands work in a CI pipeline.

**Why not the others:**
- **B** — Console steps can't be reliably repeated and aren't infrastructure as code.
- **C** — The Serverless Application Repository is for publishing and sharing applications (and is out of scope for this exam). It doesn't give you local testing or a CI deployment workflow.
- **D** — Plain CloudFormation works, but it's more verbose, packaging is manual, and there's no local testing.

**Signal words:** *"shorthand syntax"*, *"test the functions locally"*, *"Lambda … Step Functions … DynamoDB"* · **Skill:** 1.4.6 · **Review:** [Guide 17 — Lambda for Data Pipelines](../topic-guides/17-Lambda-for-Data-Pipelines.md)
</details>

---

### Question 45
An Amazon Athena table `monthly_sales(region, sales_month, revenue)` has `sales_month` values of `'JAN'`, `'FEB'`, and `'MAR'`, with one or more rows per region and month. A report needs exactly one row per region with the columns `jan_revenue`, `feb_revenue`, and `mar_revenue`. Which query produces this output?

- **A.** `SELECT region, sales_month, SUM(revenue) FROM monthly_sales GROUP BY region, sales_month`
- **B.** `SELECT region, SUM(revenue) OVER (PARTITION BY region, sales_month) FROM monthly_sales`
- **C.** `SELECT region, SUM(CASE WHEN sales_month = 'JAN' THEN revenue END) AS jan_revenue, SUM(CASE WHEN sales_month = 'FEB' THEN revenue END) AS feb_revenue, SUM(CASE WHEN sales_month = 'MAR' THEN revenue END) AS mar_revenue FROM monthly_sales GROUP BY region`
- **D.** `SELECT region, ARRAY_AGG(revenue ORDER BY sales_month) AS revenues FROM monthly_sales GROUP BY region`

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Pivoting rows into columns in Athena uses conditional aggregation. Each CASE expression keeps only one month's values, and grouping by region collapses the result to one row per region. Athena (Trino) has no PIVOT keyword, but Redshift does, and the CASE pattern works in both.

**Why not the others:**
- **A** — This returns one row per region and month (long format), not one row per region.
- **B** — A window function keeps every input row and adds a column. It doesn't collapse or pivot.
- **D** — This returns one array column instead of three named columns, and months with several rows put several values in the array.

**Signal words:** *"exactly one row per region"*, *"columns jan_revenue, feb_revenue, mar_revenue"* · **Skill:** 3.2.6 · **Review:** [Guide 34 — SQL for Data Engineers](../topic-guides/34-SQL-for-Data-Engineers.md)
</details>

---

### Question 46
A payments company wants to detect fraud rings by finding accounts that share devices, phone numbers, or payment cards within four hops of a flagged account. The queries run during transaction authorization, so they must return in milliseconds, and the relationships change constantly. Which data store should the data engineer choose?

- **A.** Amazon DynamoDB with an adjacency-list design and global secondary indexes, with each hop traversed in application code.
- **B.** Amazon Redshift with recursive common table expressions that run on a schedule and store the results in a table.
- **C.** Amazon OpenSearch Service with one document per account that nests all related devices, phones, and cards.
- **D.** Amazon Neptune, with accounts, devices, phones, and cards modeled as a graph and queried with Gremlin or openCypher.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Multi-hop relationship traversal is what graph databases do best. Neptune stores vertices and edges and runs traversal queries (Gremlin, openCypher, SPARQL) with low latency, even on highly connected data that changes all the time.

**Why not the others:**
- **A** — An adjacency list in DynamoDB can model edges, but four-hop traversals take many round trips and a lot of custom code.
- **B** — Redshift is a batch analytics engine. Scheduled results are stale and not fast enough for authorization-time queries.
- **C** — Nested documents duplicate relationships and can't traverse several hops efficiently.

**Signal words:** *"within four hops"*, *"share devices, phone numbers"*, *"milliseconds"* · **Skill:** 2.1.3 · **Review:** [Guide 28 — RDS, Aurora & Purpose-Built DBs](../topic-guides/28-RDS-Aurora-Purpose-Built-DBs.md)
</details>

---

### Question 47
A company uses one Amazon SageMaker Unified Studio domain so that data assets can be discovered across the whole company. The Finance and Marketing business units want their own leads to decide who can create projects in their area and which projects can create business glossaries and metadata forms, without going to the central domain administrators each time. What should the data engineer set up?

- **A.** A separate SageMaker Unified Studio domain for each business unit, with that unit's lead assigned as the domain administrator.
- **B.** A domain unit for each business unit, owned by that unit's lead, who assigns the project creation, glossary creation, and metadata forms creation policies.
- **C.** An LF-Tag for each business unit in AWS Lake Formation, with each lead granted permission to associate the tag with tables and to grant tag-based access to other users.
- **D.** An AWS account for each business unit, with IAM policies in each account that control which users can call the project creation APIs.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Domain units organize a SageMaker Unified Studio domain into business units and teams, and domain unit owners get delegated authority over them. Owners assign authorization policies such as the project creation policy and project membership policy to users, and the glossary creation and metadata forms creation policies to projects. Because everything stays in one domain, the shared catalog still works across the company.

**Why not the others:**
- **A** — Separate domains split the catalog, which breaks the requirement that assets be discoverable across the company.
- **C** — LF-Tags control data permissions on catalog resources. They don't control who can create projects or glossaries.
- **D** — IAM policies in separate accounts don't map to Unified Studio's project and glossary authorization. This is heavy and misses the requirement.

> 🆕 **New in exam guide v1.1:** skill 4.1.7 covers domains, domain units, and projects in SageMaker Unified Studio.

**Signal words:** *"one … domain"*, *"their own leads to decide"*, *"who can create projects … glossaries and metadata forms"* · **Skill:** 4.1.7 · **Review:** [Guide 41 — SageMaker Unified Studio, Catalog & Governance](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md)
</details>

---

### Question 48
A genomics company packages a CPU-intensive file-processing tool as a Docker image in Amazon ECR. Several times a week, researchers submit bursts of up to 5,000 independent jobs, each running 20–90 minutes. The company needs job queuing, automatic retries, the option to use Spot capacity to reduce cost, and no cluster management. Which solution meets these requirements?

- **A.** Use AWS Batch with a managed compute environment that uses Spot capacity, and submit the work as array jobs to a job queue with a retry strategy.
- **B.** Deploy the container image as an AWS Lambda function, and invoke one function for each file from an Amazon SQS queue.
- **C.** Run the image as an Amazon ECS service on AWS Fargate, and change the service's desired task count by hand before and after each burst of submitted jobs.
- **D.** Build a self-managed Kubernetes cluster on Amazon EC2, and run the work as Kubernetes Jobs with the Cluster Autoscaler.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** AWS Batch is built for containerized batch work. It queues jobs, provisions and scales managed compute environments (EC2, Spot, or Fargate), retries failed jobs, handles dependencies and array jobs, and scales back to zero when the queue is empty.

**Why not the others:**
- **B** — Lambda functions can run for at most 15 minutes, and these jobs run up to 90.
- **C** — ECS services are for long-running tasks. Scaling by hand isn't a job queue and gives no per-job retries.
- **D** — A self-managed cluster is exactly the cluster management the company wants to avoid.

**Signal words:** *"bursts of up to 5,000 independent jobs"*, *"20–90 minutes"*, *"job queuing, automatic retries … Spot"* · **Skill:** 1.2.1 · **Review:** [Guide 18 — Containers, Batch & EC2 Compute](../topic-guides/18-Containers-Batch-EC2-Compute.md)
</details>

---

### Question 49
Analysts use Amazon Athena to query clickstream data in Amazon S3. They now need to run occasional ad hoc SQL joins between the clickstream data and an orders table in Amazon DynamoDB. The company doesn't want to build a pipeline or copy the DynamoDB data. Which solution meets these requirements?

- **A.** Use Athena Federated Query with the DynamoDB data source connector registered as a data source, and join across the two catalogs in SQL.
- **B.** Run an AWS Glue crawler on the DynamoDB table, and query the resulting Data Catalog table directly from Athena as if it were a regular S3-backed table.
- **C.** Turn on a daily DynamoDB export to Amazon S3, and create an Athena table over the exported files for the analysts to use.
- **D.** Create a Redshift Spectrum external table that points to the DynamoDB table, and run the joins from Amazon Redshift instead.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** Athena Federated Query uses data source connectors to run SQL against sources outside S3, such as DynamoDB, RDS, and many others, and to join them with data lake tables in one query. Nothing is copied, and results reflect the source when the query runs. Athena can now create managed connectors with no Lambda function in your account.

**Why not the others:**
- **B** — A crawler can catalog a DynamoDB table, but Athena can't read DynamoDB through a regular catalog table. It needs a connector.
- **C** — Exports copy the data, which the company doesn't want, and the copy is up to a day old.
- **D** — Redshift Spectrum reads files in S3, not DynamoDB tables.

**Signal words:** *"occasional ad hoc SQL joins"*, *"DynamoDB"*, *"doesn't want to … copy"* · **Skill:** 3.1.7 · **Review:** [Guide 26 — Amazon Athena](../topic-guides/26-Amazon-Athena.md)
</details>

---

### Question 50
An Amazon Redshift provisioned cluster created in 2021 runs two concurrent jobs that update different rows of the same large table. One of the jobs regularly fails with `ERROR: 1023 DETAIL: Serializable isolation violation`. The company wants both jobs to keep running at the same time and both to succeed, with minimal code changes. What should the data engineer do?

- **A.** Add a LOCK statement for the table at the start of both transactions so that the second job waits until the first one commits.
- **B.** Run VACUUM and ANALYZE on the table before each job starts so that the table's statistics and sort order are current.
- **C.** Change the database's isolation level to SNAPSHOT with an ALTER DATABASE statement.
- **D.** Increase the number of query slots in the WLM queue that runs the two jobs so that both get more memory.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Under SERIALIZABLE isolation, Redshift cancels one of two concurrent transactions when their combined effect couldn't have come from running them one after the other. Under SNAPSHOT isolation, concurrent writes to different rows of the same table both succeed. Since May 2024, SNAPSHOT is the default for new provisioned clusters and Serverless workgroups, but a cluster created earlier keeps SERIALIZABLE until you change it with ALTER DATABASE.

**Why not the others:**
- **A** — LOCK is a documented fix for error 1023, but it forces the jobs to run one after the other, which breaks the requirement that they run at the same time.
- **B** — Maintenance operations don't affect transaction isolation conflicts.
- **D** — Slots and memory affect performance, not isolation conflicts.

**Signal words:** *"created in 2021"*, *"Serializable isolation violation"*, *"keep running at the same time"* · **Skill:** 2.1.6 · **Review:** [Guide 25 — Redshift Performance, Operations & Security](../topic-guides/25-Redshift-Performance-Operations-Security.md)
</details>

---

### Question 51
A research institute must move 80 TB from an on-premises NFS file server to Amazon S3 over an existing AWS Direct Connect connection. After the initial copy, S3 must be kept in sync with each day's changes for three months until cutover. Transfers must verify data integrity and must be capped at half of the connection's bandwidth. Which solution meets these requirements with the LEAST operational overhead?

- **A.** Run `aws s3 sync` from a cron job on an on-premises server, and write a script that compares object checksums after every run.
- **B.** Deploy an AWS DataSync agent, and schedule a daily NFS-to-S3 task with data verification and a bandwidth limit.
- **C.** Order AWS Snowball Edge devices for the initial copy, and ship a new device every week to capture the changes until cutover.
- **D.** Create an AWS Transfer Family SFTP server, and have the file server upload new and changed files with a scheduled SFTP script.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** AWS DataSync is built for online transfers from NFS, SMB, HDFS, and object storage to AWS. An agent reads the source, tasks can run on a schedule and copy only changed data, integrity is checked during and after the transfer, and a bandwidth limit can be set on each task.

**Why not the others:**
- **A** — This works, but scripts for scheduling, checksums, and throttling are more to maintain than DataSync's built-in features.
- **C** — ⚠️ Snowball Edge devices have been closed to new customers since November 2025. Weekly shipping also can't keep S3 in sync every day.
- **D** — Transfer Family is an endpoint for partners pushing files. It has no incremental sync, verification, or bandwidth control for this job.

**Signal words:** *"on-premises NFS"*, *"kept in sync with each day's changes"*, *"verify data integrity"*, *"capped"* · **Skill:** 1.1.3 · **Review:** [Guide 11 — DataSync, Transfer Family, Snow & AppFlow](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md)
</details>

---

### Question 52
Business analysts at a company can't find trusted datasets, because tables in the AWS Glue Data Catalog have only technical names. The governance team wants a business glossary, required metadata such as data owner and sensitivity to be filled in before an asset is published, and a request-and-approve workflow for access that grants permissions automatically after approval. Which solution meets these requirements?

- **A.** Add descriptions and custom table properties to the Glue Data Catalog tables, and publish a wiki page that explains each table and names an owner to contact for access.
- **B.** Create LF-Tags for sensitivity in AWS Lake Formation, and grant analysts DESCRIBE permission on every table so that they can browse the catalog.
- **C.** Use Amazon SageMaker Catalog with a business glossary, required metadata forms, published assets, and subscription requests that owners approve.
- **D.** Run Amazon Macie on the data lake buckets, and use its findings to tag each S3 object with a sensitivity label that analysts can search.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** SageMaker Catalog (built on Amazon DataZone) is a business data catalog. It adds glossaries and metadata forms on top of technical metadata imported from Glue and Redshift, and it can require forms to be completed before publishing. Consumers subscribe to assets and owners approve, after which access is granted automatically through Lake Formation or Redshift.

**Why not the others:**
- **A** — The Glue Data Catalog is a technical catalog, with no glossary, enforced metadata, or subscription workflow.
- **B** — LF-Tags manage permissions. They don't give analysts business context or an approval workflow.
- **D** — Macie finds sensitive data, but it doesn't make datasets discoverable or manage access requests.

> 🆕 **New in exam guide v1.1:** skill 2.2.6 covers creating and managing business data catalogs such as SageMaker Catalog.

**Signal words:** *"business glossary"*, *"required metadata … before an asset is published"*, *"request-and-approve workflow"* · **Skill:** 2.2.6 · **Review:** [Guide 41 — SageMaker Unified Studio, Catalog & Governance](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md)
</details>

---

### Question 53
An AWS Glue Spark job joins a 3 TB `events` table with a 2 TB `sessions` table on `session_key`. One key value, used for anonymous traffic, accounts for about 40% of the rows. The Spark UI shows one task running for hours while the others finish in minutes. The job sets `spark.sql.adaptive.enabled` to `false`, left over from an old tuning exercise. Which actions will reduce the impact of the skew? (Select TWO.)

- **A.** Repartition both DataFrames on `session_key` before the join so that the rows are spread across more partitions.
- **B.** Add more workers to the job and raise `spark.sql.shuffle.partitions` so that the long-running task gets more executor capacity and finishes sooner.
- **C.** Salt the skewed key by adding a random suffix on the `events` side, copying the matching `sessions` rows once for each suffix, and joining on the salted key.
- **D.** Add a broadcast hint for the `sessions` table so that the join runs as a broadcast hash join.
- **E.** Turn on adaptive query execution with skew join handling so that Spark splits oversized partitions at run time.

<details>
<summary><b>Show answer</b></summary>

**Answer: C, E**

**Why C and E:** Salting spreads one hot key across many partitions, so the work is shared by many tasks. Adaptive query execution (AQE) with skew join handling detects oversized shuffle partitions during a sort-merge join and splits them automatically. Both are standard fixes for data skew.

**Why not the others:**
- **A** — Repartitioning on the skewed key sends every row with that key to the same partition again.
- **B** — One task runs in one executor, so more workers leave the hot partition as large as before.
- **D** — A 2 TB table is far too large to broadcast to every executor.

**Signal words:** *"about 40% of the rows"*, *"one task running for hours"* · **Skill:** 3.4.5 · **Review:** [Guide 16 — Apache Spark Essentials](../topic-guides/16-Apache-Spark-Essentials.md)
</details>

---

### Question 54
A central analytics team in one AWS account runs an Amazon Redshift RA3 provisioned cluster with curated sales tables. A subsidiary in another AWS account runs Amazon Redshift Serverless and needs live, read-only access to those tables without copying any data. The central team must control exactly which schemas are shared. What should the data engineer do?

- **A.** Unload the curated tables to Amazon S3 every night, and give the subsidiary's account read access to the bucket for Redshift Spectrum queries.
- **B.** Share a manual snapshot of the cluster with the subsidiary's account, and have the subsidiary restore it every day.
- **C.** Create an external schema in the subsidiary's workgroup that uses a federated query to reach the central cluster's endpoint.
- **D.** Create a datashare with the chosen schemas, grant it to the subsidiary's account, and have the subsidiary create a database from it.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Redshift data sharing gives consumers live, transactionally consistent access to the producer's data without copying it. The producer decides which objects go into the datashare. For cross-account sharing, the producer grants the datashare to the consumer account (and authorizes it), and a consumer administrator associates it and creates a database from it. RA3, RG, and Serverless all support data sharing.

**Why not the others:**
- **A** — This copies the data and is up to a day old.
- **B** — Restoring snapshots copies the data, is stale, and costs money to run.
- **C** — Redshift federated queries reach RDS and Aurora PostgreSQL or MySQL, not another Redshift warehouse.

**Signal words:** *"live, read-only access"*, *"without copying any data"*, *"another AWS account"* · **Skill:** 4.5.1 · **Review:** [Guide 24 — Redshift Loading, Integration & Sharing](../topic-guides/24-Redshift-Loading-Integration-Sharing.md)
</details>

---

### Question 55
A nightly batch job loads about 50 million items into an Amazon DynamoDB table with `BatchWriteItem`. The table uses provisioned capacity sized for light daytime traffic. During the load, the job receives `ProvisionedThroughputExceededException` errors and responses that contain `UnprocessedItems`, and some items are never written. The load runs only once a night, and the company wants to keep cost low. Which actions should the data engineer take? (Select TWO.)

- **A.** Increase the batch size to 100 items per `BatchWriteItem` request so that the load needs fewer requests.
- **B.** Resubmit the `UnprocessedItems` from each response with exponential backoff and jitter until every item is written.
- **C.** Add a global secondary index to the table so that the writes are spread across more partitions.
- **D.** Switch the table to on-demand capacity mode so that it can absorb the nightly burst without provisioning for the peak all day.
- **E.** Add a DynamoDB Accelerator (DAX) cluster in front of the table so that the writes are cached before reaching DynamoDB.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, D**

**Why B and D:** `BatchWriteItem` can partly succeed. Items that were throttled come back in `UnprocessedItems`, and the caller has to retry them with backoff or they are lost. On-demand capacity follows traffic automatically and bills per request, which suits a once-a-night burst better than provisioning for the peak around the clock. For very large first-time spikes, warm throughput can prepare the table in advance.

**Why not the others:**
- **A** — `BatchWriteItem` accepts at most 25 items per request.
- **C** — A GSI adds write work to every item, which makes throttling worse and costs more.
- **E** — DAX is a read cache. Writes still go to the table and use its capacity.

**Signal words:** *"UnprocessedItems"*, *"items are never written"*, *"only once a night"*, *"keep cost low"* · **Skill:** 1.1.9 · **Review:** [Guide 27 — DynamoDB](../topic-guides/27-DynamoDB.md)
</details>

---

### Question 56
An AWS Glue ETL job writes new hourly partitions to `s3://amzn-s3-demo-bucket/events/year=YYYY/month=MM/day=DD/hour=HH/`. Amazon Athena queries don't see the newest hour until an hourly crawler runs, and crawler costs are rising. The team wants new partitions to be queryable in Athena as soon as the data is written, without any crawlers. Which solutions meet these requirements? (Select TWO.)

- **A.** Configure partition projection on the Athena table, with ranges for the year, month, day, and hour keys and a `storage.location.template` property.
- **B.** Run `MSCK REPAIR TABLE` once a day from a scheduled query so that all partitions created that day are added in one pass.
- **C.** Have the Glue job write through a Data Catalog sink with `enableUpdateCatalog` and `partitionKeys` so that it registers each new partition as it writes.
- **D.** Keep the crawler, but schedule it to run every 5 minutes instead of every hour so that it adds new partitions sooner.
- **E.** Enable S3 Transfer Acceleration on the bucket so that newly written objects are visible to Athena sooner.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, C**

**Why A and C:** Partition projection lets Athena work out partition locations from table properties at query time, so no partition metadata needs to be added or synced. Alternatively, the writer can register partitions in the Data Catalog as it writes them (the Glue sink's `enableUpdateCatalog` option), so they are visible immediately. Either one removes the crawler. Note that projection works only in Athena, while registered partitions also work for Redshift Spectrum and EMR.

**Why not the others:**
- **B** — A daily repair leaves up to a day of partitions invisible, and it gets slow on large tables.
- **D** — This still uses a crawler, and running it more often raises the cost.
- **E** — Transfer Acceleration speeds up long-distance uploads. It has nothing to do with catalog partitions.

**Signal words:** *"as soon as the data is written"*, *"without any crawlers"*, *"year=…/hour=…"* · **Skill:** 2.2.4 · **Review:** [Guide 13 — Glue Data Catalog & Crawlers](../topic-guides/13-Glue-Data-Catalog-Crawlers.md)
</details>

---

### Question 57
On-call data engineers must get an email within minutes whenever any AWS Glue job in the account fails or times out. The solution must need no custom code, and it must cover new jobs automatically. What should the data engineer configure?

- **A.** A CloudWatch Logs metric filter on the Glue error log groups that matches the word "Exception", with a CloudWatch alarm that notifies an SNS topic with email subscriptions.
- **B.** An Amazon EventBridge rule that matches Glue Job State Change events with a state of FAILED or TIMEOUT and targets an SNS topic with email subscriptions.
- **C.** An AWS Lambda function that runs every 5 minutes, calls `GetJobRuns` for each job, and publishes any failures to an SNS topic.
- **D.** An Amazon SQS queue that receives Glue job events, which the on-call engineers check from the SQS console during their shifts.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Glue sends job state-change events to EventBridge automatically. One rule with an event pattern on the detail type and the FAILED and TIMEOUT states catches every job, including new ones, and sends the event to SNS, which emails the subscribers. There's no code to write.

**Why not the others:**
- **A** — Log text matching is noisy, can miss timeouts, and depends on log format rather than job state.
- **C** — This is custom polling code to maintain, and it has to track new jobs.
- **D** — SQS doesn't push email. Someone has to poll it.

**Signal words:** *"any AWS Glue job"*, *"fails or times out"*, *"no custom code"*, *"email"* · **Skill:** 3.3.3 · **Review:** [Guide 22 — EventBridge, SNS & SQS](../topic-guides/22-EventBridge-SNS-SQS.md)
</details>

---

### Question 58
A company has an SCP that denies requests to any Region other than `eu-central-1` and `eu-west-1`. An audit finds two problems, both configured by account administrators: an S3 bucket in `eu-central-1` that replicates objects to a bucket in `us-east-1` owned by an external partner account, and an Amazon Redshift cluster in `eu-west-1` with cross-Region snapshot copy to `us-east-1` turned on. The company must prevent configurations like these in the future. Which actions should the data engineer take? (Select TWO.)

- **A.** Add an explicit Deny for `us-east-1` to the existing SCP, in addition to the `aws:RequestedRegion` condition that it already contains.
- **B.** Add an SCP statement that denies `s3:PutReplicationConfiguration` to every principal except a designated platform role whose replication rules go through review.
- **C.** Deploy an AWS Config rule that reports any bucket with a replication configuration, and email the noncompliance report to the security team every week for review.
- **D.** Require default encryption with an AWS KMS key that exists only in the EU Regions on every bucket that holds customer data.
- **E.** Add an SCP statement that denies `redshift:EnableSnapshotCopy` so that no principal can turn on cross-Region snapshot copy.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, E**

**Why B and E:** Both configurations were made through API calls to an approved EU Region, so a Region-based deny never applied to them. The data then leaves through the service itself. To stop this, deny the actions that configure the cross-Region copy, which here are S3 replication configuration and Redshift snapshot copy, with a controlled exception where the business needs one.

**Why not the others:**
- **A** — The calls went to EU endpoints, so a deny on `us-east-1` requests still never matches them.
- **C** — This detects the problem after the fact. It doesn't prevent it.
- **D** — Replication can re-encrypt objects with a key in the destination Region, so this doesn't stop the copies.

**Signal words:** *"replicates objects to a bucket in us-east-1"*, *"cross-Region snapshot copy"*, *"prevent"* · **Skill:** 4.5.3 · **Review:** [Guide 42 — Privacy, PII, Masking & Sovereignty](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md)
</details>

---

### Question 59
Each month, a company must run an AWS Lambda validation function against 2 million CSV objects under an S3 prefix. The processing must be highly parallel and serverless, progress and failed items must be visible in one place, and the run should count as successful if fewer than 1% of the items fail. Which solution meets these requirements with the LEAST operational overhead?

- **A.** Use an AWS Step Functions inline Map state that iterates over a list of all object keys passed in the execution input.
- **B.** Use a script to send every object key to an Amazon SQS queue, and have the Lambda function consume the queue through an event source mapping with a dead-letter queue.
- **C.** Use one AWS Glue Python shell job that loops through the object keys and invokes the Lambda function synchronously for each key.
- **D.** Use an AWS Step Functions Distributed Map state that reads the S3 prefix, batches the items for Lambda, and sets a tolerated failure percentage of 1%.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Distributed Map is built for large-scale parallel processing. It reads items straight from S3 (a prefix, a manifest, or CSV or JSON files), runs up to thousands of child workflow executions in parallel, batches items, tracks each item's result, and fails the run only when failures exceed the tolerated threshold.

**Why not the others:**
- **A** — Inline Map has limited concurrency and must fit every key into the state's input payload, so it can't handle 2 million items.
- **B** — This is parallel, but there's no single view of the run's progress or overall success, and you have to write the loading script.
- **C** — A single loop is sequential and slow, and it's custom code to maintain.

**Signal words:** *"2 million … objects"*, *"highly parallel"*, *"fewer than 1% of the items fail"* · **Skill:** 1.3.3 · **Review:** [Guide 20 — Step Functions](../topic-guides/20-Step-Functions.md)
</details>

---

### Question 60
A company is migrating an on-premises Oracle database to Amazon Aurora PostgreSQL-Compatible Edition. It must convert schemas, views, stored procedures, and functions, and it wants a report of the items that need manual work. The security team doesn't allow desktop migration tools on engineers' laptops and wants a managed, console-based solution. What should the data engineer use?

- **A.** AWS DMS Schema Conversion to assess and convert the schema and code objects, followed by AWS DMS tasks to migrate the data.
- **B.** The AWS Schema Conversion Tool (AWS SCT) installed on each engineer's workstation, connected to both the source and the target databases.
- **C.** An AWS DMS task with the target table preparation mode set to drop and re-create tables, so that DMS creates the target schema during the full load.
- **D.** An AWS Glue crawler that catalogs the Oracle schema, and a Glue job that creates matching tables in Aurora from the Data Catalog definitions.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** DMS Schema Conversion is the managed, console-based successor to AWS SCT. It assesses the source, reports what can be converted automatically and what needs manual work, converts schema and code objects such as procedures and functions, and applies them to the target. DMS then migrates the data (full load plus CDC if needed).

**Why not the others:**
- **B** — SCT converts code, but it's a desktop tool, which the security team doesn't allow. It was also removed from the in-scope service list in v1.1, although skill 2.4.3 still names it.
- **C** — DMS creates basic tables only. It doesn't convert views, procedures, or functions.
- **D** — Crawlers and Glue jobs deal with table structure and data, not with converting database code.

**Signal words:** *"stored procedures, and functions"*, *"report of the items that need manual work"*, *"managed, console-based"* · **Skill:** 2.4.3 · **Review:** [Guide 10 — DMS & Database Ingestion](../topic-guides/10-DMS-Database-Ingestion.md)
</details>

---

### Question 61
A data engineer is building data quality checks for a 5 TB transactions dataset and wants to develop them on a sample. About 2% of the rows come from a small region that has historically had the most quality problems. The sample must represent every region in its correct proportion, including the small region. Which sampling technique should the data engineer use?

- **A.** Take the first 1 million rows of the dataset in the order that the files are stored and listed in Amazon S3.
- **B.** Take every 1,000th row after sorting the rows by the name of the file they came from.
- **C.** Use stratified sampling: group the rows by region and take the same sampling fraction from each group.
- **D.** Take a simple random sample of 0.01% of the rows without regard to region.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Stratified sampling divides the population into groups (strata) and samples each one separately, which guarantees that each region appears in its true proportion. Small groups can't be underrepresented by chance. Spark's `sampleBy` is one way to do it.

**Why not the others:**
- **A** — The first rows reflect how the files were written (often one time period or one source), not the whole dataset.
- **B** — Systematic sampling ordered by file can follow patterns in how the files were produced.
- **D** — A simple random sample is correct on average, but a very small sample can underrepresent a 2% group by chance, so it isn't guaranteed.

**Signal words:** *"represent every region in its correct proportion"*, *"small region"* · **Skill:** 3.4.4 · **Review:** [Guide 33 — Data Quality](../topic-guides/33-Data-Quality.md)
</details>

---

### Question 62
A company has 40 project teams and adds new ones every week. Each team's AWS Glue jobs and AWS Secrets Manager secrets are tagged `project=<name>`. Engineers sign in through IAM Identity Center, which passes a `project` session tag. The security team wants a single policy that lets each engineer manage only their own project's resources and that doesn't need updating when teams are added. What should the data engineer implement?

- **A.** An IAM role for each project, with a policy that lists the ARNs of that project's jobs and secrets, updated whenever a team creates a new resource.
- **B.** A resource-based policy on each secret and each job that lists the IAM roles allowed to use it, updated as teams are added.
- **C.** A permissions boundary for each engineer that lists the ARNs of the resources that belong to the engineer's current project.
- **D.** A policy that allows actions only when `aws:ResourceTag/project` matches `${aws:PrincipalTag/project}`, and denies changes to the `project` tag.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** This is attribute-based access control (ABAC). One policy compares a tag on the principal with a tag on the resource, so new teams and new resources are covered automatically as long as they are tagged. Conditions on tag-changing actions (such as `aws:RequestTag` and `aws:TagKeys`) stop users from retagging resources to get around the policy.

**Why not the others:**
- **A** — Role-based access with per-resource ARNs has to be updated for every new team and every new resource.
- **B** — Resource policies on every resource grow with each team, and not every resource type supports them.
- **C** — Boundaries only cap permissions, and ARN lists still need constant updates.

**Signal words:** *"adds new ones every week"*, *"single policy"*, *"tagged"*, *"session tag"* · **Skill:** 4.2.5 · **Review:** [Guide 37 — IAM for Data Engineers](../topic-guides/37-IAM-for-Data-Engineers.md)
</details>

---

### Question 63
An AWS Lambda function consumes an Amazon SQS standard queue and writes each message to an Amazon RDS for PostgreSQL database. During bursts, Lambda scales to hundreds of concurrent executions, and the database rejects new connections. The company must protect the database, must not lose messages, and wants few code changes. Which actions should the data engineer take? (Select TWO.)

- **A.** Set maximum concurrency on the SQS event source mapping to a value that the database can handle.
- **B.** Configure provisioned concurrency of 500 for the function so that execution environments are initialized before bursts.
- **C.** Increase the function's memory to 10,240 MB so that each invocation finishes faster and holds its connection for less time.
- **D.** Reduce the queue's visibility timeout to 5 seconds so that messages are retried quickly when a connection fails.
- **E.** Put Amazon RDS Proxy in front of the database, and point the function at the proxy endpoint to pool connections.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, E**

**Why A and E:** Maximum concurrency on an SQS event source mapping limits how many function instances the queue can invoke. Messages above the limit stay in the queue and aren't lost. RDS Proxy pools and reuses database connections, so many short-lived Lambda connections share a small pool. Together they protect the database with only a change to the connection endpoint.

**Why not the others:**
- **B** — Provisioned concurrency prepares more environments in advance. It doesn't cap concurrency.
- **C** — More memory doesn't reduce the number of concurrent connections during a burst.
- **D** — A short visibility timeout causes duplicate processing and more load on the database.

**Signal words:** *"hundreds of concurrent executions"*, *"rejects new connections"*, *"must not lose messages"* · **Skill:** 1.4.2 · **Review:** [Guide 17 — Lambda for Data Pipelines](../topic-guides/17-Lambda-for-Data-Pipelines.md)
</details>

---

### Question 64
A data platform team runs AWS Glue, Amazon EMR, and Amazon Athena workloads for several projects in one AWS account. Every resource has a `project` tag, which has been activated as a cost allocation tag for over a year. Finance wants an email whenever a project's forecasted monthly spend exceeds its allocation, and the team wants to see which services drove each project's cost over the past 6 months. What should the data engineer do?

- **A.** Create a CloudWatch billing alarm on the EstimatedCharges metric for each project, and look at the monthly bill PDF to break down costs by service.
- **B.** Create AWS Budgets cost budgets filtered on the `project` tag with forecasted-spend alerts, and use Cost Explorer grouped by service for each project.
- **C.** Turn on the AWS Trusted Advisor cost optimization checks, and subscribe the finance team to the weekly Trusted Advisor summary emails.
- **D.** Deploy an AWS Config rule that requires the `project` tag, and use Athena queries over CloudTrail logs to estimate each project's resource usage.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** AWS Budgets can filter a budget by cost allocation tag and alert on actual or forecasted spend by email or SNS. Cost Explorer shows historical costs filtered by the tag and grouped by service, which answers the question about what drove each project's cost.

**Why not the others:**
- **A** — Billing alarms work on actual estimated charges (total or per service), not on forecasts for a tag, and a PDF bill isn't an analysis tool.
- **C** — Trusted Advisor suggests optimizations. It doesn't track spend against a project allocation.
- **D** — Config and CloudTrail show configuration and activity, not cost.

**Signal words:** *"forecasted monthly spend exceeds its allocation"*, *"which services drove each project's cost"*, *"cost allocation tag"* · **Skill:** 1.2.4 · **Review:** [Guide 44 — Cost Optimization](../topic-guides/44-Cost-Optimization.md)
</details>

---

### Question 65
An insurance company wants to license third-party weather data. The provider offers its product through AWS Data Exchange as Amazon Redshift datashares. Analysts want to query the data directly from the company's Amazon Redshift Serverless workgroup, join it with internal tables, and see the provider's updates immediately, without building any loading pipeline. What should the data engineer do?

- **A.** Subscribe to the product in AWS Data Exchange, then create a database in Redshift from the datashare that the subscription makes available, and query it.
- **B.** Subscribe to the product, set up automatic export of each new revision to Amazon S3, and load the exported files into Redshift every night with a scheduled COPY command.
- **C.** Ask the provider to copy the data into an S3 bucket in the company's account, and query it with Redshift Spectrum external tables.
- **D.** Create an Amazon AppFlow flow that pulls the weather data from the provider's public API into Amazon S3 every hour, and load it into Redshift.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** With AWS Data Exchange for Amazon Redshift, a subscription gives the subscriber a Redshift datashare. The subscriber creates a database from it and queries the provider's live data in place, with no copies and no loading. Data Exchange handles the entitlement, the subscription terms, and billing.

**Why not the others:**
- **B** — Automatic export to S3 applies to file-based data sets. It would also mean copying and delaying the data.
- **C** — This skips the licensed product and its entitlement management, and it adds a copy.
- **D** — This isn't how the product is offered, and it builds the pipeline the analysts want to avoid.

> 🆕 **New in exam guide v1.1:** AWS Data Exchange is now in scope. It supports file, API, Amazon Redshift, and Amazon S3 data sets, plus AWS Lake Formation data sets (in preview).

**Signal words:** *"AWS Data Exchange as Amazon Redshift datashares"*, *"query the data directly"*, *"without building any loading pipeline"* · **Skill:** 2.1.5 (also 4.5.7) · **Review:** [Guide 24 — Redshift Loading, Integration & Sharing](../topic-guides/24-Redshift-Loading-Integration-Sharing.md)
</details>

---

## Answer key

| Q | Answer | Domain | Skill |
|---|---|---|---|
| 1 | B | 1 | 1.1.1 |
| 2 | D | 2 | 2.1.8 |
| 3 | A | 3 | 3.1.2 |
| 4 | C | 4 | 4.1.4 |
| 5 | B, E | 1 | 1.1.7 |
| 6 | B | 2 | 2.1.7 |
| 7 | A, C, F | 1 | 1.1.1 |
| 8 | C | 3 | 3.2.3 |
| 9 | D | 1 | 1.1.12 |
| 10 | A | 2 | 2.1.3 |
| 11 | B | 4 | 4.3.1 |
| 12 | C | 1 | 1.2.3 |
| 13 | B, D | 3 | 3.4.2 |
| 14 | A | 1 | 1.1.2 |
| 15 | D | 4 | 4.5.5 |
| 16 | B | 2 | 2.4.2 |
| 17 | C | 1 | 1.2.4 |
| 18 | A | 3 | 3.2.6 |
| 19 | C | 2 | 2.1.4 |
| 20 | A, E | 4 | 4.2.6 |
| 21 | D | 1 | 1.3.1 |
| 22 | A | 2 | 2.1.2 |
| 23 | B | 3 | 3.3.4 |
| 24 | C, D | 1 | 1.2.2 |
| 25 | D | 1 | 1.1.8 |
| 26 | C | 2 | 2.4.6 |
| 27 | D | 3 | 3.3.8 |
| 28 | A | 4 | 4.5.4 |
| 29 | C | 1 | 1.4.7 |
| 30 | A, D | 2 | 2.3.4 |
| 31 | B | 1 | 1.2.10 |
| 32 | A | 3 | 3.1.4 |
| 33 | C | 4 | 4.2.3 |
| 34 | D | 2 | 2.1.1 |
| 35 | B | 1 | 1.1.1 |
| 36 | B, C | 3 | 3.3.6 |
| 37 | B | 4 | 4.4.3 |
| 38 | C | 1 | 1.1.10 |
| 39 | A | 2 | 2.1.5 |
| 40 | D | 3 | 3.2.4 |
| 41 | D | 1 | 1.3.2 |
| 42 | A, D | 4 | 4.3.3 |
| 43 | B | 2 | 2.4.1 |
| 44 | A | 1 | 1.4.6 |
| 45 | C | 3 | 3.2.6 |
| 46 | D | 2 | 2.1.3 |
| 47 | B | 4 | 4.1.7 |
| 48 | A | 1 | 1.2.1 |
| 49 | A | 3 | 3.1.7 |
| 50 | C | 2 | 2.1.6 |
| 51 | B | 1 | 1.1.3 |
| 52 | C | 2 | 2.2.6 |
| 53 | C, E | 3 | 3.4.5 |
| 54 | D | 4 | 4.5.1 |
| 55 | B, D | 1 | 1.1.9 |
| 56 | A, C | 2 | 2.2.4 |
| 57 | B | 3 | 3.3.3 |
| 58 | B, E | 4 | 4.5.3 |
| 59 | D | 1 | 1.3.3 |
| 60 | A | 2 | 2.4.3 |
| 61 | C | 3 | 3.4.4 |
| 62 | D | 4 | 4.2.5 |
| 63 | A, E | 1 | 1.4.2 |
| 64 | B | 1 | 1.2.4 |
| 65 | A | 2 | 2.1.5 |

## Score by domain

| Domain | Questions | Your correct | % |
|---|---|---|---|
| 1 — Data Ingestion and Transformation | 22 (Q1, 5, 7, 9, 12, 14, 17, 21, 24, 25, 29, 31, 35, 38, 41, 44, 48, 51, 55, 59, 63, 64) | | |
| 2 — Data Store Management | 17 (Q2, 6, 10, 16, 19, 22, 26, 30, 34, 39, 43, 46, 50, 52, 56, 60, 65) | | |
| 3 — Data Operations and Support | 14 (Q3, 8, 13, 18, 23, 27, 32, 36, 40, 45, 49, 53, 57, 61) | | |
| 4 — Data Security and Governance | 12 (Q4, 11, 15, 20, 28, 33, 37, 42, 47, 54, 58, 62) | | |
| **Total** | **65** | | |

> Remember that the real exam uses a scaled score (100–1,000, pass = 720) and includes 15 unscored questions, so a raw percentage here is only a readiness signal. Study any domain where you scored below 70%, even if your total is good, starting with the guides linked in the questions you missed.
