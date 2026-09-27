# Practice Exam 01 — DEA-C01 (65 questions)

> Written for **AWS Certified Data Engineer – Associate (DEA-C01)**, exam guide **v1.1 (Dec 12, 2025)**. Facts checked as of **September 2026**. Service names are current, with the older name in parentheses where the exam's question pool may still use it.

## How to take this exam

- **Time yourself: 130 minutes for 65 questions.** That matches the real exam, about 2 minutes per question.
- **No notes, no docs, no search.** Answer from memory, the way you will on test day.
- **Flag and move on.** If a question takes more than about 3 minutes, pick your best guess, flag it, and come back at the end. Wrong answers cost nothing extra, so never leave one blank.
- **Don't open the answers until you finish all 65.** Checking each one as you go trains recognition, not recall.
- **Question types:** *multiple choice* has one correct answer out of four. *Multiple response* has two or more correct answers out of five or more; the stem says how many to pick ("Select TWO." / "Select THREE."). You get no partial credit.
- **Read the last sentence first.** The qualifier (*MOST cost-effective*, *LEAST operational overhead*, *lowest latency*) usually decides between two workable options.

**Scoring guidance**

| Score | What it means |
|---|---|
| **≥ 80% (52/65 or more)** | Likely ready. Review what you missed, then book the exam. |
| **70–80% (46–51)** | Close. Use the domain table at the end to find weak domains and re-read those guides. |
| **< 70% (45 or fewer)** | More study needed. Work through the topic guides for your weakest domain first, then retake. |

The real exam reports a scaled score from 100 to 1,000 (passing is 720). A raw percentage here is only a rough guide.

---

### Question 1
A logistics company ingests vehicle telemetry into an Amazon Kinesis Data Streams stream that has 8 shards in provisioned mode. An AWS Lambda function consumes the stream through an event source mapping with a batch size of 100. During daily peaks, the `IteratorAge` metric climbs above 10 minutes. The function's average duration is stable, no errors or throttles are reported, and write throughput is well below the shards' capacity. Records for the same vehicle must still be processed in order.

Which change will reduce the processing lag with the LEAST operational overhead?

- **A.** Configure reserved concurrency of 100 for the Lambda function so that more copies of the function can process the stream at the same time.
- **B.** Increase the parallelization factor on the event source mapping.
- **C.** Register the function as an enhanced fan-out consumer so that it gets dedicated read throughput for each shard.
- **D.** Split every shard with `UpdateShardCount` to double the shard count, and change the producers' partition key scheme to spread vehicles across the new shards.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** A rising `IteratorAge` with stable duration and no errors means the consumer can't keep up. By default Lambda processes only one batch at a time per shard. The parallelization factor (1–10) lets the event source mapping run up to 10 concurrent batches per shard. Lambda still sends records with the same partition key to the same batch in order, so per-vehicle ordering is kept, and it's a single configuration change.

**Why not the others:**
- **A** — Kinesis event source mapping concurrency is capped at shards × parallelization factor. Reserved concurrency can't raise it.
- **C** — Enhanced fan-out fixes read contention between several consumers. This single consumer is limited by processing concurrency, not read throughput.
- **D** — More shards would add concurrency, but resharding and producer changes cost far more effort and money than one setting.

**Signal words:** *"IteratorAge climbs"*, *"duration is stable, no errors"*, *"in order"*, *"LEAST operational overhead"* · **Skill:** 1.1.7 · **Review:** [Guide 17 — Lambda for Data Pipelines](../topic-guides/17-Lambda-for-Data-Pipelines.md)
</details>

---

### Question 2
A company loads a daily extract into an Amazon Redshift provisioned cluster that has 16 slices. The extract arrives as a single 60 GB GZIP-compressed CSV file in Amazon S3. The `COPY` command takes more than an hour, and monitoring shows that most slices are idle during the load.

Which change will MOST improve load performance?

- **A.** Run 16 concurrent `COPY` commands against the same table, one for each slice, so that every slice loads part of the single file in parallel.
- **B.** Decompress the file before loading so that Redshift doesn't spend CPU time on decompression.
- **C.** Convert the extract to newline-delimited JSON and load it with `COPY` using the `'auto'` JSON option.
- **D.** Split the extract into a multiple of 16 compressed files of similar size and load them with a single `COPY` command.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** `COPY` spreads work across slices one file at a time, and a GZIP file can't be split, so one slice does all the work. Splitting the data into a multiple of the slice count, with files of similar size (roughly 1 MB–1 GB each after compression), keeps every slice busy. A single `COPY` with a common prefix or a manifest is the recommended pattern.

**Why not the others:**
- **A** — Concurrent `COPY` commands into one table are serialized, and they still can't split one GZIP file.
- **B** — An uncompressed single file is still loaded by a single slice, and it moves more bytes from S3.
- **C** — The format doesn't fix the single-file bottleneck, and JSON parsing is slower than CSV.

**Signal words:** *"single 60 GB file"*, *"most slices are idle"* · **Skill:** 2.3.1 · **Review:** [Guide 24 — Redshift Loading, Integration & Sharing](../topic-guides/24-Redshift-Loading-Integration-Sharing.md)
</details>

---

### Question 3
A financial services company's data lake has 600 AWS Glue Data Catalog tables in 40 databases, and dozens of new tables are added every month. Access rules depend on two attributes: the data domain (for example, payments or lending) and the sensitivity level. Today a Lake Formation administrator grants permissions table by table, and analysts often wait days before they can query new tables.

Which solution will scale permissions with the LEAST ongoing administrative effort?

- **A.** Define LF-Tags for domain and sensitivity, assign them to databases so that tables inherit them, and grant permissions to roles with LF-Tag expressions.
- **B.** Grant each analyst role Lake Formation permissions on all tables in every database with the "All tables" wildcard, and add IAM policies that deny access to sensitive tables by name.
- **C.** Create IAM policies that allow `s3:GetObject` on the S3 prefixes for each domain and sensitivity level, and attach them to the analyst roles.
- **D.** Share each database with the analyst roles through AWS RAM, and create named column-level grants for the sensitive columns.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** Lake Formation tag-based access control (LF-TBAC) grants permissions on tag expressions such as `domain=payments AND sensitivity=internal` rather than on named tables. Tags assigned to a database are inherited by its tables and columns, so a new table becomes accessible to the right roles the moment it's created. This is the pattern AWS recommends for large, growing catalogs.

**Why not the others:**
- **B** — Deny-by-name IAM policies need updating for every new sensitive table, and IAM policies don't control Lake Formation data permissions.
- **C** — S3 prefix access bypasses the catalog permission model and gives no table- or column-level control.
- **D** — RAM is for cross-account sharing, and named grants per column are the same per-resource effort the company wants to avoid.

**Signal words:** *"600 tables"*, *"new tables every month"*, *"based on attributes"*, *"LEAST ongoing administrative effort"* · **Skill:** 4.2.5 · **Review:** [Guide 40 — Lake Formation](../topic-guides/40-Lake-Formation.md)
</details>

---

### Question 4
An analytics team uses Amazon Athena to query 5 TB of uncompressed CSV logs in Amazon S3. Most queries select 6 of the 90 columns and filter on `event_date`. Query costs and run times are growing every month.

Which action will reduce the cost of these queries the MOST?

- **A.** Compress the existing CSV files with GZIP so that Athena reads fewer bytes from Amazon S3.
- **B.** Turn on query result reuse in the workgroup so that repeated queries return cached results.
- **C.** Use a CTAS statement to rewrite the data as Snappy-compressed Parquet partitioned by `event_date`.
- **D.** Create views that select only the 6 needed columns so that Athena scans only those columns.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Athena bills per byte scanned. Parquet is columnar, so a query that reads 6 of 90 columns reads only those column chunks. Partitioning by `event_date` lets Athena skip every partition outside the filter. Together they typically cut the bytes scanned by more than 90%, and a single CTAS statement does the conversion.

**Why not the others:**
- **A** — GZIP shrinks the files, but Athena still reads every column of every row, and GZIP files can't be split for parallel reads.
- **B** — Result reuse helps only when the exact same query is re-run within the reuse window.
- **D** — A view over CSV still reads whole rows. Row-based formats can't prune columns.

**Signal words:** *"6 of the 90 columns"*, *"filter on event_date"*, *"cost … the MOST"* · **Skill:** 3.1.7 · **Review:** [Guide 26 — Amazon Athena](../topic-guides/26-Amazon-Athena.md)
</details>

---

### Question 5
A SaaS company streams JSON clickstream events through Amazon Data Firehose (Kinesis Data Firehose) to Amazon S3. Each event has a `tenant_id` field. Data currently lands as GZIP-compressed JSON under the default `YYYY/MM/DD/HH` prefixes, so Amazon Athena queries for a single tenant scan every tenant's data. The company wants the data stored in a columnar format and partitioned by `tenant_id` and day.

Which solution meets these requirements with the LEAST operational overhead?

- **A.** Add a Lambda transformation to the existing stream that converts records to Parquet and writes each tenant's records to its own S3 prefix with `PutObject` calls.
- **B.** Keep the current stream, and run a nightly AWS Glue job that reads the new objects, converts them to Parquet, and writes them partitioned by `tenant_id`.
- **C.** Create a new Firehose stream with dynamic partitioning that extracts `tenant_id` with inline JQ parsing, and enable record format conversion to Parquet.
- **D.** Configure S3 Event Notifications on the bucket to invoke a Lambda function that copies each new object to a prefix for its tenant.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Firehose dynamic partitioning reads keys from each record (inline JQ parsing works for JSON, with no Lambda needed) and writes to prefixes such as `tenant_id=!{partitionKeyFromQuery:tenant_id}/dt=!{timestamp:yyyy-MM-dd}/`. Record format conversion turns the JSON into Parquet using a schema from a Glue Data Catalog table. Dynamic partitioning can only be turned on when a stream is created, which is why a new stream is needed. Both features require a buffer size of at least 64 MiB.

**Why not the others:**
- **A** — A Firehose transformation Lambda returns records to Firehose. Having it write S3 objects itself is custom code that bypasses Firehose buffering and retries.
- **B** — It works, but it adds a second pipeline to run and leaves the data unpartitioned until the nightly run.
- **D** — Copying objects per tenant doesn't work when objects mix tenants, it doesn't produce a columnar format, and it doubles storage.

**Signal words:** *"partitioned by tenant_id"*, *"columnar"*, *"LEAST operational overhead"* · **Skill:** 1.2.6 · **Review:** [Guide 07 — Amazon Data Firehose](../topic-guides/07-Amazon-Data-Firehose.md)
</details>

---

### Question 6
A healthcare company must store signed consent forms in Amazon S3 for 10 years. Regulators require that no user, including the AWS account root user, can delete or overwrite a form during the retention period.

Which solution meets this requirement?

- **A.** Enable S3 Object Lock on the bucket with a default retention period of 10 years in compliance mode.
- **B.** Enable S3 Object Lock in governance mode with a 10-year default retention, and deny `s3:BypassGovernanceRetention` to all IAM roles.
- **C.** Enable versioning with MFA delete, and apply a bucket policy that denies `s3:DeleteObject` to all principals.
- **D.** Place a legal hold on every uploaded object, and allow only the compliance team to call `s3:PutObjectLegalHold`.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** In compliance mode, no one can overwrite or delete a locked object version or shorten its retention until the period ends, not even the root user. Object Lock requires versioning, which is turned on automatically when you enable Object Lock. A default retention setting on the bucket applies the lock to every new object.

**Why not the others:**
- **B** — Governance mode can be bypassed by any principal with the bypass permission, and IAM deny statements don't restrict the root user.
- **C** — The root user can delete versions with MFA, and an administrator can change the bucket policy.
- **D** — A legal hold has no retention period and can be removed by anyone with `s3:PutObjectLegalHold`.

**Signal words:** *"including the root user"*, *"cannot delete or overwrite"*, *"10 years"* · **Skill:** 2.3.5 · **Review:** [Guide 05 — S3 Data Lake Storage](../topic-guides/05-S3-Data-Lake-Storage.md)
</details>

---

### Question 7
An AWS Glue Spark job runs every hour. It reads all CSV files under an S3 prefix, converts them to Parquet, and appends the output to a curated table. The job gets slower every day, and the curated table contains duplicates because each run reprocesses files that earlier runs already handled.

Which change will make each run process only new files with the LEAST development effort?

- **A.** Add an S3 Lifecycle rule that deletes raw files one hour after they are created so that only new files remain in the prefix.
- **B.** Add a `push_down_predicate` on the ingestion timestamp so that the job reads only recent data.
- **C.** Enable daily S3 Inventory reports, and change the script to compare each report against a list of processed object keys that the job stores in an Amazon DynamoDB table.
- **D.** Enable job bookmarks for the job, and make sure the script sets `transformation_ctx` on the source and calls `job.commit()`.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Job bookmarks store state about what each run processed, and for S3 sources they skip files that earlier successful runs already read. Bookmarks are off by default. They need a `transformation_ctx` on the source so that Glue can track state, and `job.commit()` at the end so that the state is saved only when the run succeeds.

**Why not the others:**
- **A** — Deleting raw data destroys the replay source and races with late or slow runs.
- **B** — Pushdown predicates filter on catalog partition columns. The files aren't partitioned by ingestion time, and a time window can miss or repeat files.
- **C** — It works, but it's custom state tracking that rebuilds what bookmarks already provide.

**Signal words:** *"reprocesses files"*, *"only new files"*, *"LEAST development effort"* · **Skill:** 1.1.3 · **Review:** [Guide 12 — AWS Glue ETL](../topic-guides/12-AWS-Glue-ETL.md)
</details>

---

### Question 8
A data platform team gives each of three business units its own Amazon Athena workgroup. Last month, one analyst's query scanned 60 TB by mistake. Any single query in the marketing workgroup that scans more than 2 TB must now stop automatically. The marketing team lead must also receive an email when the workgroup's total scanned data exceeds 20 TB in a day.

Which combination of actions will meet these requirements? (Select TWO.)

- **A.** Create an AWS Budgets cost budget for Athena with a budget action that cancels running queries when the threshold is crossed.
- **B.** Set a per-query data usage control of 2 TB on the marketing workgroup.
- **C.** Attach an IAM policy to the analysts' roles with a condition that allows `athena:StartQueryExecution` only for queries that scan less than 2 TB.
- **D.** Assign the marketing workgroup to an Athena capacity reservation so that the amount of data each query can scan is capped.
- **E.** Configure a workgroup-wide data usage alert for 20 TB per day that notifies an Amazon SNS topic the team lead subscribes to.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, E**

**Why B and E:** A workgroup's **per-query data usage control** cancels any query that scans more than the limit. **Workgroup-wide data usage alerts** track the total scanned per period and trigger a CloudWatch alarm that notifies SNS. Workgroup-wide alerts only notify; they don't cancel queries. The two controls together cover both requirements.

**Why not the others:**
- **A** — Budgets actions can apply IAM or SCP policies or stop some resources, but they can't cancel Athena queries, and cost data arrives with a delay.
- **C** — IAM has no condition key for bytes scanned. The scan size isn't known when the query starts.
- **D** — Capacity reservations change billing to DPU-hours and set concurrency, but they don't cap bytes scanned per query.

**Signal words:** *"single query … stop automatically"*, *"email when the workgroup's total exceeds"* · **Skill:** 3.1.7 · **Review:** [Guide 26 — Amazon Athena](../topic-guides/26-Amazon-Athena.md)
</details>

---

### Question 9
Account A owns an S3 bucket that uses SSE-KMS with a customer managed key in Account A. An AWS Glue job in Account B runs with an IAM role that must read the objects. The bucket policy already grants the Account B role `s3:GetObject` and `s3:ListBucket`, but the job fails with an `AccessDenied` error from AWS KMS.

Which combination of steps will resolve the error while following the principle of least privilege? (Select TWO.)

- **A.** Add a statement to the key policy in Account A that allows the Account B role to use `kms:Decrypt`.
- **B.** Re-encrypt the objects with the AWS managed key `aws/s3` so that Account B can decrypt them without key policy changes.
- **C.** Add `kms:Decrypt` on the key's ARN to the IAM policy attached to the Glue job role in Account B.
- **D.** Enable S3 Bucket Keys on the bucket so that the Glue role no longer needs to call AWS KMS.
- **E.** Create a customer managed key in Account B, and add a KMS grant that lets it decrypt data encrypted by Account A's key.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, C**

**Why A and C:** Cross-account KMS access needs permission on both sides. The **key policy** in the owning account must allow the external principal (or the external account, which then delegates to it), and the **IAM policy** in the calling account must allow the action on that specific key ARN. Scoping both to `kms:Decrypt` and the one role meets least privilege.

**Why not the others:**
- **B** — AWS managed keys can't be used across accounts because their key policies can't be edited.
- **D** — S3 Bucket Keys reduce the number of KMS requests, but the reader still needs permission to decrypt with the key.
- **E** — A key in another account can't decrypt ciphertext produced by Account A's key.

**Signal words:** *"customer managed key in Account A"*, *"role in Account B"*, *"AccessDenied from AWS KMS"* · **Skill:** 4.3.3 · **Review:** [Guide 39 — Encryption, KMS & Secrets](../topic-guides/39-Encryption-Key-Management.md)
</details>

---

### Question 10
A retailer wants to replicate 40 tables from an on-premises MySQL 8.0 database into an Amazon S3 data lake as Parquet files. After an initial copy, inserts, updates, and deletes must be replicated continuously, with no custom code. A data engineer created an AWS DMS task of type full load and CDC, with an S3 target endpoint that writes Parquet. The full load succeeds, but the task fails as soon as it switches to ongoing replication.

What should the data engineer do?

- **A.** Enable supplemental logging on the source database for all columns of the replicated tables.
- **B.** Enable binary logging on the source database with `binlog_format` set to `ROW` and `binlog_row_image` set to `FULL`.
- **C.** Change the target endpoint to CSV, because DMS can write ongoing changes to Amazon S3 only in CSV format.
- **D.** Replace the task with a full-load-only task that runs every 15 minutes and overwrites the target prefix each time.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** DMS reads MySQL changes from the binary log, and CDC requires row-based logging with full row images. Binary log retention must also be long enough for DMS to read the logs. Once the source is configured, the same full-load-and-CDC task writes change records, with an operation column for inserts, updates, and deletes, to S3 as Parquet.

**Why not the others:**
- **A** — Supplemental logging is the prerequisite for **Oracle** sources, not MySQL.
- **C** — The DMS S3 target supports Parquet for both full load and CDC.
- **D** — Repeated full loads put heavy load on the source, lose the history of changes, and leave the data stale between runs.

**Signal words:** *"MySQL"*, *"full load succeeds … fails when it switches to ongoing replication"* · **Skill:** 1.1.1 · **Review:** [Guide 10 — DMS & Database Ingestion](../topic-guides/10-DMS-Database-Ingestion.md)
</details>

---

### Question 11
A company is designing an Amazon Redshift schema. Most queries join a 4-billion-row `sales` fact table to an 80-million-row `customer` dimension on `customer_id`. Almost every query filters `sales` on a range of `sale_date`. Queries also join several small dimension tables (under 10,000 rows each, rarely updated).

Which combination of design choices will provide the BEST query performance? (Select THREE.)

- **A.** Distribute both the `sales` table and the `customer` table on `customer_id` (`DISTSTYLE KEY`).
- **B.** Use `DISTSTYLE ALL` for the `customer` table so that every node has a full copy.
- **C.** Use `DISTSTYLE ALL` for the small dimension tables.
- **D.** Distribute the `sales` table on `sale_date` so that each day's rows are stored together.
- **E.** Define an interleaved sort key on the `sales` table that includes every column used in a `WHERE` clause.
- **F.** Make `sale_date` the leading column of the `sales` table's sort key.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, C, F**

**Why A, C, and F:** Distributing the two large tables on their join column puts matching rows on the same slice, so the big join needs no data redistribution. Small, rarely updated dimensions are cheap to copy to every node with `ALL`, which makes their joins local too. A sort key that starts with `sale_date` lets zone maps skip blocks outside the date range.

**Why not the others:**
- **B** — Copying an 80-million-row table to every node costs a lot of storage and slows loads, and the `KEY` design already co-locates the join.
- **D** — Range filters on date would put the work on the few slices that hold those days, and daily volumes cause skew.
- **E** — Interleaved sort keys across many columns weaken each column's pruning, need costly `VACUUM REINDEX`, and are a poor fit for the date-range pattern here.

**Signal words:** *"joined on customer_id"*, *"range of sale_date"*, *"small, rarely updated"* · **Skill:** 2.4.1 · **Review:** [Guide 23 — Redshift Architecture & Table Design](../topic-guides/23-Redshift-Architecture-Table-Design.md)
</details>

---

### Question 12
A media company's ingestion bucket receives thousands of objects each hour. Three teams need to react to new objects independently. A Step Functions workflow must start only for `.mp4` objects larger than 1 GB. A Lambda function must process `.csv` objects under the `landing/` prefix. An audit team must be able to replay the last 30 days of object-created events after a downstream outage.

Which solution meets these requirements with the LEAST operational overhead?

- **A.** Turn on Amazon EventBridge notifications for the bucket, create rules that filter on key and object size for each target, and create an event archive with 30-day retention.
- **B.** Configure S3 Event Notifications with suffix filters that publish to an Amazon SNS topic, and subscribe the Step Functions workflow and the Lambda function to the topic with subscription filter policies.
- **C.** Create three S3 Event Notification configurations, one for each team, with prefix and suffix filters that route events to Step Functions, Lambda, and an S3 audit bucket.
- **D.** Enable CloudTrail data events for the bucket, and create an EventBridge rule on `PutObject` API calls that starts the workflow and invokes the function.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** With EventBridge turned on for the bucket, every S3 event goes to the default event bus. Rules can match on key prefix and suffix and on numeric fields such as object size, and each rule can target Step Functions, Lambda, and many other services directly. An EventBridge archive stores events so they can be replayed later.

**Why not the others:**
- **B** — SNS doesn't give a replay window for standard topics, and Step Functions can't subscribe to SNS directly.
- **C** — S3 Event Notifications can't filter on object size, can't target Step Functions or S3, and can't replay events.
- **D** — CloudTrail data events add cost and delivery delay, and they're an indirect path when native S3-to-EventBridge integration exists.

**Signal words:** *"larger than 1 GB"*, *"replay the last 30 days"*, *"three teams … independently"* · **Skill:** 1.1.6 · **Review:** [Guide 22 — EventBridge, SNS & SQS](../topic-guides/22-EventBridge-SNS-SQS.md)
</details>

---

### Question 13
A retail company stores one row per store per day in a `daily_sales` table (`store_id`, `sales_date`, `revenue`) in Amazon Redshift. There are no missing days. An analyst needs each store's 7-day rolling average of revenue, including the current day.

Which expression returns the correct value?

- **A.** `AVG(revenue) OVER (PARTITION BY sales_date ORDER BY store_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`
- **B.** `AVG(revenue) OVER (PARTITION BY store_id ORDER BY sales_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)`
- **C.** `SUM(revenue) OVER (PARTITION BY store_id ORDER BY sales_date ROWS UNBOUNDED PRECEDING) / 7`
- **D.** `AVG(revenue) OVER (PARTITION BY store_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** A rolling average is computed separately for each store, so the window is partitioned by `store_id` and ordered by date. With one row per day, a frame of the current row plus the 6 rows before it covers exactly 7 days.

**Why not the others:**
- **A** — Partitioning by date averages across *stores* on the same day, not across days for one store.
- **B** — 7 preceding rows plus the current row is an 8-day window.
- **C** — `UNBOUNDED PRECEDING` gives a running total since the first day, and dividing by 7 isn't an average.

**Signal words:** *"each store's"*, *"7-day rolling average"*, *"including the current day"* · **Skill:** 3.2.6 · **Review:** [Guide 34 — SQL for Data Engineers](../topic-guides/34-SQL-for-Data-Engineers.md)
</details>

---

### Question 14
An IoT company stores sensor data in Amazon S3 under `s3://amzn-s3-demo-bucket/telemetry/dt=YYYY-MM-DD/hour=HH/`. The Glue Data Catalog table has more than 200,000 partitions, and a Glue crawler that runs every 30 minutes adds new ones. Analysts query only with Amazon Athena. They complain that new hours of data are missing until the crawler runs, and that query planning is slow.

Which solution will make new data queryable as soon as it lands and reduce planning time, with the LEAST operational overhead?

- **A.** Run `MSCK REPAIR TABLE` from an EventBridge scheduled rule every 5 minutes.
- **B.** Enable partition projection on the table with date and integer projection types and a `storage.location.template`.
- **C.** Increase the crawler frequency to every 5 minutes, and configure the crawler to crawl new folders only.
- **D.** Create a partition index on `dt` and `hour`, and add S3 event notifications that invoke a Lambda function to call `BatchCreatePartition` for each new prefix.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** With partition projection, Athena calculates partition values and locations from table properties instead of looking them up in the Data Catalog. A new `dt`/`hour` prefix is queryable the moment data arrives, with no crawler or `ADD PARTITION` call, and planning skips the costly partition lookup on large tables. It fits here because the partition values are predictable and only Athena reads the table.

**Why not the others:**
- **A** — `MSCK REPAIR TABLE` scans the whole table location and gets slower as partitions grow. Data is still missing for up to 5 minutes.
- **C** — A crawler still leaves a delay, costs more when run more often, and doesn't make planning faster.
- **D** — It works, but it's custom code to maintain. Partition projection makes both parts unnecessary for an Athena-only table.

**Signal words:** *"200,000 partitions"*, *"only with Amazon Athena"*, *"as soon as it lands"*, *"predictable"* layout · **Skill:** 2.2.4 · **Review:** [Guide 13 — Glue Data Catalog & Crawlers](../topic-guides/13-Glue-Data-Catalog-Crawlers.md)
</details>

---

### Question 15
A customer support organization has 4 million archived support tickets stored as text files in Amazon S3. For each ticket it wants a two-sentence summary and a category from a company-specific taxonomy of 25 categories that is described in natural language. There is no latency requirement; the results will be loaded into Amazon Redshift over the next week. The company has no labeled training data.

Which solution is the MOST cost-effective?

- **A.** Train a custom text classification model in Amazon SageMaker AI, and run a batch transform job over the tickets.
- **B.** Purchase Provisioned Throughput for a foundation model in Amazon Bedrock, and have an AWS Glue job call `InvokeModel` for each ticket.
- **C.** Write the prompts to JSONL files in S3, and run an Amazon Bedrock batch inference job that writes its output to S3.
- **D.** Send each ticket to an Amazon SQS queue, and have a Lambda function call the Bedrock `InvokeModel` API on demand for every message.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Bedrock batch inference (`CreateModelInvocationJob`) reads JSONL prompt records from S3, runs them asynchronously, and writes the responses to S3. For supported models it's priced well below on-demand invocation, and Bedrock handles scaling and retries. A foundation model can summarize and follow a taxonomy written in plain language without any training, and the output files are ready for `COPY`.

**Why not the others:**
- **A** — Training needs labeled data the company doesn't have, a classifier can't write summaries, and model training is out of scope for the data engineer role.
- **B** — Provisioned Throughput is a time-based commitment meant for steady production traffic, which is costly for a one-off job.
- **D** — It works, but it pays full on-demand prices and needs custom code to handle throttling and retries.

**Signal words:** *"no latency requirement"*, *"4 million"*, *"no labeled training data"*, *"MOST cost-effective"* · **Skill:** 1.2.10 · 🆕 v1.1 skill · **Review:** [Guide 19 — GenAI, LLMs & Vectors](../topic-guides/19-GenAI-LLMs-Vectors.md)
</details>

---

### Question 16
Auditors require a record of every read and write of objects in a specific S3 bucket that holds financial statements, including the IAM principal, source IP address, and time of each request. Log files must be tamper-evident. The account already has a multi-Region CloudTrail trail that records management events.

Which solution meets these requirements at the LOWEST cost?

- **A.** Add S3 data events to the existing trail with an advanced event selector scoped to the bucket's ARN, and enable log file integrity validation.
- **B.** Enable S3 server access logging on the bucket, deliver the logs to a separate logging bucket, and protect the log objects with S3 Object Lock in compliance mode.
- **C.** Enable CloudTrail Insights events on the trail to capture unusual read and write activity against the bucket.
- **D.** Add S3 data events for all current and future S3 buckets in the account to the trail, and query the logs with Amazon Athena.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** Object-level API calls (`GetObject`, `PutObject`, `DeleteObject`) are CloudTrail **data events**, which trails don't log by default. An advanced event selector on `resources.ARN` limits logging, and cost, to the one bucket. Log file integrity validation adds signed digest files that prove whether log files were changed or deleted.

**Why not the others:**
- **B** — Server access logs are delivered on a best-effort basis, so they can't guarantee *every* request, and they have no integrity-validation mechanism.
- **C** — Insights detects unusual API *volumes*. It doesn't record each request.
- **D** — Logging data events for every bucket charges for far more events than the auditors need.

**Signal words:** *"every read and write of objects"*, *"specific S3 bucket"*, *"tamper-evident"*, *"LOWEST cost"* · **Skill:** 4.4.1 · **Review:** [Guide 43 — Audit Logging, CloudTrail & Config](../topic-guides/43-Audit-Logging-CloudTrail-Config.md)
</details>

---

### Question 17
A Lambda function consumes an Amazon Kinesis data stream through an event source mapping that uses the default retry settings. Now and then a malformed record makes the function throw an exception, and the affected shard stops progressing for days while the rest of the stream keeps moving. The team must keep valid records flowing, keep the full content of failed batches for later analysis, and must not change the function code.

Which combination of event source mapping changes meets these requirements? (Select TWO.)

- **A.** Enable `BisectBatchOnFunctionError`.
- **B.** Configure a dead-letter queue in the function's asynchronous invocation settings.
- **C.** Increase the stream's data retention period to 365 days.
- **D.** Set `MaximumRetryAttempts` to a low value, and add an S3 bucket as the on-failure destination.
- **E.** Add `ReportBatchItemFailures` to the event source mapping's function response types.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, D**

**Why A and D:** By default Lambda retries a failing batch until the records expire, which blocks the shard. **Bisect on error** splits the failing batch in half repeatedly, so good records around the bad one get processed; splitting doesn't use up the retry quota. **A retry limit plus an on-failure destination** lets the mapping move on, and an **S3** destination stores the full failed batch along with its metadata. (SQS and SNS destinations receive only metadata such as shard ID and sequence numbers.)

**Why not the others:**
- **B** — Stream event source mappings invoke the function synchronously, so the function's asynchronous dead-letter queue is never used.
- **C** — Longer retention makes the blockage last longer, not shorter.
- **E** — Partial batch responses work only if the function code returns `batchItemFailures`, which is a code change.

**Signal words:** *"malformed record … shard stops"*, *"full content of failed batches"*, *"must not change the function code"* · **Skill:** 1.1.7 · **Review:** [Guide 17 — Lambda for Data Pipelines](../topic-guides/17-Lambda-for-Data-Pipelines.md)
</details>

---

### Question 18
A smart-meter company writes readings to a DynamoDB table in on-demand mode. The partition key is `reading_date` (for example, `2026-09-26`), and the sort key is `meter_id#timestamp`. Writes are throttled every day, even though the table's total traffic is far below account quotas. Most reads fetch the recent readings of a single meter.

Which design change will eliminate the throttling and support the read pattern?

- **A.** Switch the table to provisioned capacity mode with auto scaling, and set the maximum write capacity well above the daily peak.
- **B.** Add a global secondary index that uses `meter_id` as its partition key, and send the application's writes to the index.
- **C.** Put DynamoDB Accelerator (DAX) in front of the table to absorb the write traffic.
- **D.** Recreate the table with `meter_id` as the partition key and the reading timestamp as the sort key.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Every write on a given day goes to the same partition key value, so all traffic hits one partition and runs into its per-partition throughput limit, whatever the table's capacity mode. A high-cardinality key such as `meter_id` spreads writes across many partitions. A timestamp sort key serves "recent readings for one meter" with an efficient `Query` using `ScanIndexForward=false`.

**Why not the others:**
- **A** — Table-level capacity doesn't raise the throughput limit of a single hot partition.
- **B** — Applications can't write to a GSI. Every base-table write still lands on the hot date key.
- **C** — DAX is a read-through and write-through cache. Writes still go to the table.

**Signal words:** *"partition key is reading_date"*, *"throttled … far below quotas"*, *"single meter"* · **Skill:** 2.4.1 · **Review:** [Guide 27 — DynamoDB](../topic-guides/27-DynamoDB.md)
</details>

---

### Question 19
An AWS Glue Spark job joins 2 billion order-line rows with a 300-million-row seller table on `seller_id`. Two marketplace sellers account for 45% of all order lines. In the Spark UI, all tasks of the join stage finish within minutes except a few that run for more than two hours. Adding more workers doesn't help.

Which approach will MOST effectively reduce the job's runtime?

- **A.** Call `coalesce()` on the order-line DataFrame before the join to reduce the number of partitions.
- **B.** Salt the join key by adding a random suffix to the skewed sellers' keys and replicating the matching seller rows for each suffix.
- **C.** Broadcast the order-line DataFrame to every executor so that the join no longer needs a shuffle.
- **D.** Change the worker type from G.1X to G.4X and double the number of workers so that the long-running tasks get more memory and CPU.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** This is data skew. Every row for a hot key hashes to the same partition, so one task does most of the work no matter how many workers there are. Salting spreads a hot key across N sub-keys on the large side and replicates the matching rows on the small side, so the join still matches correctly while the work spreads over many tasks. (Spark's adaptive query execution skew-join handling can help in the same way, but salting is the reliable, explicit fix.)

**Why not the others:**
- **A** — Fewer partitions concentrate the work even more.
- **C** — You broadcast the *small* side of a join. Two billion rows won't fit in executor memory.
- **D** — Bigger or more workers don't split a single partition's work. The hot tasks stay hot.

**Signal words:** *"45% of all order lines"*, *"a few tasks run for hours"*, *"more workers doesn't help"* · **Skill:** 3.4.5 · **Review:** [Guide 16 — Apache Spark Essentials](../topic-guides/16-Apache-Spark-Essentials.md)
</details>

---

### Question 20
An AWS Glue job reads a Data Catalog table that is partitioned by `year`, `month`, and `day` and holds 6 years of data. Each run needs only the previous day's data. The script loads the table with `create_dynamic_frame.from_catalog` and then applies a `Filter` transform on the partition columns. Most of the 45-minute runtime is spent listing and reading files that the filter later discards.

Which change will reduce the runtime the MOST?

- **A.** Enable Auto Scaling and raise the maximum number of workers so that more executors share the listing and reading work.
- **B.** Convert the DynamicFrame to a Spark DataFrame with `toDF()`, and apply the filter with a Spark SQL `WHERE` clause.
- **C.** Pass a `push_down_predicate` on `year`, `month`, and `day` to `create_dynamic_frame.from_catalog`.
- **D.** Run a Glue crawler before each job run so that the catalog lists only the latest partitions.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** A pushdown predicate prunes partitions using catalog metadata *before* any S3 listing or reading, so the job opens only the one matching day. A `Filter` transform runs *after* the data has been read. For tables with a very large number of partitions, adding a partition index with `catalogPartitionPredicate` moves the filtering into the Data Catalog as well.

**Why not the others:**
- **A** — More workers read unneeded data faster but still read it, and listing is largely done on the driver.
- **B** — By the time `toDF()` runs, the DynamicFrame has already loaded the full table.
- **D** — Crawlers add partitions. They don't remove old ones, and the catalog should keep them anyway.

**Signal words:** *"only the previous day"*, *"Filter transform"*, *"listing and reading files that the filter discards"* · **Skill:** 1.4.1 · **Review:** [Guide 12 — AWS Glue ETL](../topic-guides/12-AWS-Glue-ETL.md)
</details>

---

### Question 21
A company keeps a `customer` table as an Apache Iceberg table in the Glue Data Catalog and queries it with Amazon Athena. Every night, a CDC file of inserts, updates, and deletes must be applied to the table. Auditors must be able to query the table as it existed at the end of the previous quarter. After months of nightly changes, queries have slowed because the table has many small data files and delete files.

Which combination of actions meets these requirements? (Select TWO.)

- **A.** Run `MSCK REPAIR TABLE` after each nightly load so that Athena discovers the new data files.
- **B.** Apply each CDC file with a `MERGE INTO` statement that updates, deletes, and inserts the matching rows.
- **C.** Set `vacuum_max_snapshot_age_seconds` to 86400 (1 day), and run `VACUUM` every night to keep the metadata small.
- **D.** Run `OPTIMIZE ... REWRITE DATA USING BIN_PACK` on a regular schedule.
- **E.** Convert the table to a Hive-style Parquet table, and rewrite the affected partitions with `INSERT OVERWRITE`.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, D**

**Why B and D:** `MERGE INTO` applies inserts, updates, and deletes to an Iceberg table in one ACID transaction. `OPTIMIZE ... BIN_PACK` compacts small files and applies the accumulated delete files, which restores read performance. Every commit creates a snapshot, so auditors can use `FOR TIMESTAMP AS OF` as long as quarter-end snapshots are kept.

**Why not the others:**
- **A** — Iceberg tracks its files in its own metadata. `MSCK REPAIR TABLE` is for Hive-style partitions.
- **C** — A 1-day snapshot age would expire the quarter-end snapshots and break time travel. Retention must cover at least one full quarter. (The default is 5 days, which is also too short here.)
- **E** — Hive tables can't do row-level changes or time travel.

**Signal words:** *"inserts, updates, and deletes"*, *"as it existed at the end of the previous quarter"*, *"many small data files and delete files"* · **Skill:** 2.1.7 · 🆕 v1.1 skill · **Review:** [Guide 04 — Open Table Formats & S3 Tables](../topic-guides/04-Open-Table-Formats-S3-Tables.md)
</details>

---

### Question 22
A company stores the credentials for an Amazon RDS for PostgreSQL database in AWS Secrets Manager and has turned on automatic rotation every 30 days with a Lambda rotation function. The function runs in private subnets of the database's VPC so that it can reach the database. The VPC has no NAT gateway or internet gateway. Rotation fails, and the function's logs show timeouts when it calls the Secrets Manager API.

Which solution will fix rotation while keeping all traffic private?

- **A.** Create an interface VPC endpoint for Secrets Manager in the function's subnets with private DNS enabled, and allow HTTPS from the function's security group.
- **B.** Create a gateway VPC endpoint for Secrets Manager, and add it to the route tables of the function's subnets.
- **C.** Increase the rotation function's timeout to 15 minutes and its memory to 1,024 MB.
- **D.** Move the credentials to an AWS Systems Manager Parameter Store `SecureString` parameter, and rotate the password with a Lambda function that an EventBridge schedule invokes every 30 days.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** The rotation function must reach both the database and the Secrets Manager API. Without internet access, the private path to Secrets Manager is an interface VPC endpoint (AWS PrivateLink). Private DNS lets the SDK's default endpoint name resolve to it, and the endpoint's security group must allow HTTPS (443) from the function.

**Why not the others:**
- **B** — Gateway endpoints exist only for Amazon S3 and DynamoDB.
- **C** — The calls time out because there is no network path. More time won't create one.
- **D** — Parameter Store has no native rotation, so this swaps a managed feature for custom code and still needs a network path.

**Signal words:** *"private subnets"*, *"no NAT gateway"*, *"timeouts when it calls the Secrets Manager API"* · **Skill:** 4.1.3 · **Review:** [Guide 39 — Encryption, KMS & Secrets](../topic-guides/39-Encryption-Key-Management.md)
</details>

---

### Question 23
A data engineer is building an AWS Step Functions Standard workflow. The first state starts an AWS Glue job, and the next state runs an Amazon Athena query on the job's output. The Glue task uses the default request-response integration, so the Athena query often runs before the job finishes. Transient Glue failures must be retried up to 3 times with exponential backoff. The operations team must receive one notification, and only when the workflow fails after all retries.

Which combination of changes meets these requirements with the LEAST operational overhead? (Select THREE.)

- **A.** Change the Glue task to the `.waitForTaskToken` integration pattern, and pass the task token to the Glue job as an argument.
- **B.** Change the Glue task resource to `arn:aws:states:::glue:startJobRun.sync`.
- **C.** Add a `Retry` field to the Glue task with `MaxAttempts` set to 3, an `IntervalSeconds` value, and a `BackoffRate` of 2.
- **D.** Add a loop of a Wait state and a Lambda function that calls `GetJobRun` until the job reaches a terminal state.
- **E.** Add a `Catch` field that matches `States.ALL` and moves to a task that publishes to an Amazon SNS topic.
- **F.** Create an EventBridge rule that matches Glue job state-change events with a `FAILED` state and targets the SNS topic.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, C, E**

**Why B, C, and E:** The `.sync` ("run a job") pattern makes Step Functions wait until the Glue run finishes and fails the state if the run fails. `Retry` with `BackoffRate` 2 gives exponential backoff between attempts. `Catch` runs only after retries are used up, so the SNS task fires once per failed workflow.

**Why not the others:**
- **A** — The Glue integration doesn't support the callback (`.waitForTaskToken`) pattern, and it would need custom code in the job.
- **D** — A polling loop works, but it's the hand-built version of what `.sync` does for you.
- **F** — The rule fires on *every* failed attempt, including ones that will be retried, and it misses failures in the Athena step.

**Signal words:** *"runs before the job finishes"*, *"exponential backoff"*, *"only when the workflow fails after all retries"* · **Skill:** 1.3.2 · **Review:** [Guide 20 — Step Functions](../topic-guides/20-Step-Functions.md)
</details>

---

### Question 24
A data engineer must add data quality checks to an AWS Glue ETL job for an orders dataset. The requirements are:
- `order_id` must never be null and must never repeat.
- At least 95% of rows must have a value in `email`.
- `order_total` must be greater than 0.
- The dataset must not be empty.

Which DQDL ruleset meets all of the requirements?

- **A.** `Rules = [ IsComplete "order_id", Uniqueness "order_id" > 0.95, Completeness "email" >= 0.95, ColumnValues "order_total" > 0, RowCount > 0 ]`
- **B.** `Rules = [ Completeness "order_id" >= 0.95, IsUnique "order_id", IsComplete "email", ColumnValues "order_total" > 0, RowCount > 0 ]`
- **C.** `Rules = [ IsComplete "order_id", IsUnique "order_id", Completeness "email" >= 0.95, ColumnValues "order_total" > 0, RowCount > 0 ]`
- **D.** `Rules = [ IsComplete "order_id", IsUnique "order_id", Completeness "email" >= 0.95, ColumnValues "order_total" >= 0, RowCount >= 0 ]`

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** `IsComplete` passes only if the column has no nulls, and `IsUnique` passes only if every value is distinct. `Completeness "email" >= 0.95` expresses "at least 95%". `ColumnValues "order_total" > 0` rejects zero and negative totals, and `RowCount > 0` fails an empty dataset. In a Glue job, the Evaluate Data Quality transform can fail the job or route failed rows using row-level results.

**Why not the others:**
- **A** — `Uniqueness > 0.95` allows up to 5% duplicate order IDs.
- **B** — The rules are swapped: `order_id` may be 5% null, and every row would need an email.
- **D** — `>= 0` accepts zero totals, and `RowCount >= 0` passes an empty dataset.

**Signal words:** *"never be null and never repeat"*, *"at least 95%"*, *"greater than 0"*, *"not be empty"* · **Skill:** 3.4.2 · **Review:** [Guide 33 — Data Quality](../topic-guides/33-Data-Quality.md)
</details>

---

### Question 25
A company's Amazon Redshift data warehouse holds historical sales. Current inventory lives in an Amazon Aurora PostgreSQL database and changes every few seconds. Analysts need to join live inventory levels with warehouse sales in a single Redshift query, without building a pipeline or copying inventory data into Redshift.

Which solution meets these requirements?

- **A.** Create an Amazon Redshift Spectrum external schema that points to the Aurora PostgreSQL database.
- **B.** Create a zero-ETL integration from Aurora PostgreSQL to Amazon Redshift, and query the replicated tables.
- **C.** Export Aurora snapshots to Amazon S3 every hour, and query the exported files with Redshift Spectrum.
- **D.** Create an external schema for Aurora PostgreSQL with Redshift federated queries, and join it in SQL.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Federated queries let Redshift query live tables in Aurora or RDS for PostgreSQL and MySQL through an external schema, with credentials stored in Secrets Manager. Redshift pushes filters down to the source and joins the results with local tables. No data is copied, so the inventory values are current as of the query.

**Why not the others:**
- **A** — Spectrum reads files in Amazon S3 through the Data Catalog. It can't query an operational database.
- **B** — Zero-ETL *replicates* data into Redshift, which the requirement rules out, even though it needs no pipeline code.
- **C** — Hourly snapshots aren't live, and the export process is a pipeline.

**Signal words:** *"live inventory"*, *"single Redshift query"*, *"without copying"* · **Skill:** 2.1.5 · **Review:** [Guide 24 — Redshift Loading, Integration & Sharing](../topic-guides/24-Redshift-Loading-Integration-Sharing.md)
</details>

---

### Question 26
A manufacturing company sends machine events to a Kinesis data stream with 20 shards in provisioned mode, using the `machine_type` attribute as the partition key. There are 4 machine types. Producers receive `ProvisionedThroughputExceededException` errors, but CloudWatch shows that most shards receive almost no traffic.

What should a data engineer do to resolve the errors?

- **A.** Increase the number of shards to 40 so that the stream has more total write capacity.
- **B.** Use a high-cardinality value, such as `machine_id`, as the partition key.
- **C.** Register each consumer application for enhanced fan-out so that reads no longer compete for shard throughput.
- **D.** Switch the stream to on-demand capacity mode so that Kinesis adds shards automatically as traffic grows.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Kinesis hashes the partition key to choose a shard, so 4 distinct keys can use at most 4 of the 20 shards. Those shards hit their per-shard write limit while the others sit idle. A high-cardinality key spreads records evenly across all shards. Ordering is still kept per machine, which is usually what matters.

**Why not the others:**
- **A** — More shards don't help when only 4 hash values are in use.
- **C** — Enhanced fan-out affects reads. The errors come from the write side.
- **D** — On-demand mode still maps each key to one shard, and a single hot key is still limited to one shard's throughput.

**Signal words:** *"4 machine types"*, *"most shards receive almost no traffic"*, *"ProvisionedThroughputExceededException"* · **Skill:** 1.1.9 · **Review:** [Guide 06 — Kinesis Data Streams](../topic-guides/06-Kinesis-Data-Streams.md)
</details>

---

### Question 27
A company's data lake spans 300 S3 buckets, and new datasets arrive every week. Lake Formation permissions are already granted with LF-Tag expressions that exclude tables tagged `classification=pii`. The security team must continuously find objects that contain PII, such as passport and bank account numbers, while keeping costs predictable. Glue Data Catalog tables whose data contains PII must then be restricted automatically.

Which combination of steps meets these requirements with the LEAST operational overhead? (Select TWO.)

- **A.** Schedule Glue crawlers with custom classifiers that match PII patterns, and mark matching tables as sensitive.
- **B.** Enable Amazon GuardDuty S3 Protection, and forward its findings to AWS Security Hub.
- **C.** Enable Amazon Macie automated sensitive data discovery for the account.
- **D.** Run a weekly Athena query with regular expressions against every table to find values that look like PII.
- **E.** Create an EventBridge rule for Macie findings that invokes a Lambda function to assign the `classification=pii` LF-Tag to the affected tables.

<details>
<summary><b>Show answer</b></summary>

**Answer: C, E**

**Why C and E:** Macie automated sensitive data discovery samples objects across all buckets every day, so coverage is continuous and costs stay predictable, and it detects passport and bank account numbers with managed data identifiers. Macie publishes findings to EventBridge. A rule plus a small Lambda function can tag the matching tables, and the existing LF-Tag grants then block access with no further permission changes.

**Why not the others:**
- **A** — Crawler classifiers infer schemas and formats. They don't inspect values for PII.
- **B** — GuardDuty detects threats such as unusual access. It doesn't classify data content.
- **D** — Regex scans over every table are custom, costly on large data, and miss new data between runs.

**Signal words:** *"300 S3 buckets"*, *"continuously"*, *"predictable"* cost, *"restricted automatically"* · **Skill:** 4.5.2 · **Review:** [Guide 42 — Privacy, PII, Masking & Sovereignty](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md)
</details>

---

### Question 28
An e-commerce company runs its product catalog on Amazon Aurora PostgreSQL. It will store 20 million product embeddings in the same database with the pgvector extension so that similarity searches can be joined with inventory tables. New products are inserted continuously throughout the day. Searches must return the most accurate nearest neighbors at the lowest query latency, and the database instances have plenty of memory.

Which index should the data engineer create on the embedding column?

- **A.** An HNSW index that uses the operator class matching the distance metric in the queries.
- **B.** An IVFFlat index with `lists` set to 1,000, created when the table is first defined and before any data is loaded.
- **C.** A B-tree index on the embedding column so that nearest-neighbor lookups can use an ordered index scan.
- **D.** An IVFFlat index with a low `probes` setting so that each query searches as few clusters as possible.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** HNSW (Hierarchical Navigable Small World) builds a multi-layer proximity graph that gives the best recall-versus-latency trade-off of the pgvector index types. It needs no training step and handles continuous inserts well. Its costs are more memory and slower index builds, and the scenario says memory is plentiful. The operator class (for example, cosine or L2) must match the distance operator that queries use, or the index isn't used.

**Why not the others:**
- **B** — IVFFlat learns cluster centroids from the data present when the index is built. Built on an empty table, the clusters are meaningless and recall collapses.
- **C** — B-tree indexes order scalar values. They can't answer distance queries over vectors.
- **D** — Fewer probes lowers latency by searching fewer clusters, which lowers recall. That's the opposite of "most accurate".

**Signal words:** *"most accurate … lowest query latency"*, *"inserted continuously"*, *"plenty of memory"* · **Skill:** 2.1.8 · 🆕 v1.1 skill · **Review:** [Guide 19 — GenAI, LLMs & Vectors](../topic-guides/19-GenAI-LLMs-Vectors.md)
</details>

---

### Question 29
A company runs 30 nightly AWS Glue 5.0 Spark jobs on G.1X workers. The jobs start at 11 PM and must finish before 7 AM, and they usually take less than 2 hours. Input volume can vary by a factor of 10 from one night to the next, and each job has a fixed 40 workers, so executors sit idle on light nights.

Which combination of changes will reduce cost with the LEAST effort? (Select TWO.)

- **A.** Change the worker type to G.4X and halve the number of workers.
- **B.** Run the jobs with the Flex execution class.
- **C.** Rewrite the jobs as Python shell jobs to avoid Spark overhead.
- **D.** Enable Auto Scaling, and treat the configured number of workers as the maximum.
- **E.** Move the jobs to a long-running Amazon EMR cluster that uses On-Demand Instances.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, D**

**Why B and D:** **Flex** runs non-urgent Spark jobs (Glue 3.0+, G.1X and G.2X) on spare capacity at a lower DPU-hour price. Start times can be delayed and runs can take longer, and the 8-hour window easily absorbs that. **Auto Scaling** (Glue 3.0+) adds and removes workers during the run, with the configured number as the maximum, so light nights no longer pay for idle executors. Both are job settings, with no code changes.

**Why not the others:**
- **A** — Larger workers cost more per worker. Resizing doesn't solve the variable volume, and Flex doesn't support G.4X.
- **C** — Python shell jobs run on a single small node and can't process Spark-scale data in parallel.
- **E** — An always-on cluster pays for idle time between nightly runs and adds cluster management.

**Signal words:** *"must finish before 7 AM … take less than 2 hours"* (not urgent), *"volume varies by 10x"*, *"idle executors"*, *"LEAST effort"* · **Skill:** 1.2.4 · **Review:** [Guide 12 — AWS Glue ETL](../topic-guides/12-AWS-Glue-ETL.md)
</details>

---

### Question 30
A data engineer runs 12 Lambda functions in an ingestion pipeline, and each function writes to its own CloudWatch Logs log group. After a deployment, the engineer must quickly find the 10 most frequent error messages across all 12 functions over the last 6 hours.

Which solution requires the LEAST effort?

- **A.** Export the 12 log groups to Amazon S3, and query the exported files with Amazon Athena.
- **B.** Create a subscription filter on each log group that streams events to an Amazon OpenSearch Service domain, and build a dashboard of error messages.
- **C.** Create a metric filter on each log group for the word ERROR, and graph the metrics on a CloudWatch dashboard.
- **D.** Select all 12 log groups in CloudWatch Logs Insights, and run a query that filters for errors and counts them by message.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Logs Insights queries many log groups at once with no setup. A query such as `filter @message like /ERROR/ | stats count(*) as n by @message | sort n desc | limit 10` answers the question in seconds, and you pay only for the data scanned.

**Why not the others:**
- **A** — An export task takes time, and it's a whole pipeline for a one-off question.
- **B** — Setting up a domain and subscriptions is a large effort for an ad hoc investigation.
- **C** — Metric filters count matches but can't group by message text, and they record only events that arrive after the filter is created.

**Signal words:** *"quickly"*, *"across all 12"* log groups, *"most frequent error messages"*, *"LEAST effort"* · **Skill:** 3.3.8 · **Review:** [Guide 32 — Monitoring, Logging & Troubleshooting](../topic-guides/32-Monitoring-Logging-Troubleshooting.md)
</details>

---

### Question 31
An AWS Glue ETL job writes partitioned Parquet data to Amazon S3 every hour and creates new values of a `region_code` partition that aren't known in advance. The table is queried by Amazon Athena, Amazon Redshift Spectrum, and Spark on Amazon EMR. A nightly crawler adds the new partitions, so recent data is missing for hours. The team wants new partitions registered in the Data Catalog as soon as each job run finishes, without running crawlers.

What should the data engineer do?

- **A.** Enable partition projection on the table with an enum projection type for `region_code`.
- **B.** Add an S3 event notification that starts the crawler whenever an object is written to the table's prefix.
- **C.** Write through a Data Catalog sink with `enableUpdateCatalog` set to true and `partitionKeys` set to `region_code`.
- **D.** Run `MSCK REPAIR TABLE` from the Glue job through the Athena JDBC driver after each write.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** When a Glue job writes through a catalog sink (for example, `getSink` with `enableUpdateCatalog=True` and `partitionKeys`), Glue registers new partitions, and can optionally update the schema, in the same run. Every engine that reads the Data Catalog sees them at once, with no crawler.

**Why not the others:**
- **A** — Partition projection works only in Athena, not in Spectrum or EMR, and enum values must be listed in advance, which these can't be.
- **B** — Starting a crawler for every object causes overlapping crawler runs (a crawler can't run twice at once) and still leaves a delay.
- **D** — It works for Athena, but it adds a second tool and a full scan of the table location after every run.

**Signal words:** *"not known in advance"*, *"Athena, Redshift Spectrum, and Spark on EMR"*, *"as soon as each job run finishes"*, *"without running crawlers"* · **Skill:** 2.2.4 · **Review:** [Guide 13 — Glue Data Catalog & Crawlers](../topic-guides/13-Glue-Data-Catalog-Crawlers.md)
</details>

---

### Question 32
A Lambda function enriches streaming records by looking up values in a 30 GB set of reference files that is refreshed weekly. Hundreds of concurrent invocations need low-latency read access to the same files, and loading them from Amazon S3 at the start of each invocation is too slow.

Which solution meets these requirements?

- **A.** Package the reference files in a Lambda layer, and attach the layer to the function.
- **B.** Mount an Amazon EFS file system to the function through an EFS access point.
- **C.** Increase the function's ephemeral storage, and copy the files to `/tmp` during initialization.
- **D.** Move the files to the S3 Glacier Instant Retrieval storage class so that each invocation can read them with millisecond latency.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Amazon EFS gives every execution environment shared, persistent file access, mounted under `/mnt/...`, and the weekly refresh happens once in one place. The function must be connected to the VPC that contains the file system's mount targets.

**Why not the others:**
- **A** — Layers count toward the 250 MB unzipped deployment package limit.
- **C** — `/tmp` holds at most 10,240 MB, and each execution environment would have to copy the files separately.
- **D** — It's still an S3 read on every invocation, and Glacier Instant Retrieval adds per-GB retrieval charges for hot data.

**Signal words:** *"30 GB"*, *"hundreds of concurrent invocations"*, *"same files"* · **Skill:** 1.4.7 · **Review:** [Guide 17 — Lambda for Data Pipelines](../topic-guides/17-Lambda-for-Data-Pipelines.md)
</details>

---

### Question 33
A company uses Amazon SageMaker Unified Studio. The finance project owns several AWS Glue tables that it has published to Amazon SageMaker Catalog with business glossary terms. Analysts in a marketing project must be able to find these tables, request access with a business justification, and start querying after a finance data owner approves, without filing tickets for manual Lake Formation grants.

Which solution meets these requirements with the LEAST operational overhead?

- **A.** Have the marketing project submit subscription requests for the assets in SageMaker Catalog; after finance approves, access is granted to the marketing project automatically.
- **B.** Share the finance Glue database with the marketing account through AWS RAM, and have the Lake Formation administrator grant `SELECT` on each table to every analyst's IAM role after an email approval.
- **C.** Copy the finance tables into the marketing project's S3 location every night with a Glue job, and catalog the copies in the marketing project.
- **D.** Add the marketing analysts as contributors to the finance project so that they inherit the project's data permissions.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** SageMaker Catalog (built on Amazon DataZone) has a publish, subscribe, and approve workflow. For managed assets such as Glue and Redshift tables, approving a subscription automatically fulfills it: the catalog grants Lake Formation (or Redshift) permissions to the subscribing project. Access is tied to the project, so people who join or leave the project gain or lose access without new grants.

**Why not the others:**
- **B** — This is the manual ticket-and-grant process the company wants to stop.
- **C** — Copies create stale, duplicated data and a pipeline to run, and they bypass the owner's approval.
- **D** — Contributors get access to everything in the finance project, not only the requested tables, and there's no per-asset approval.

**Signal words:** *"find … request access … after the owner approves"*, *"without manual Lake Formation grants"* · **Skill:** 4.5.6 · 🆕 v1.1 skill · **Review:** [Guide 41 — SageMaker Unified Studio, Catalog & Governance](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md)
</details>

---

### Question 34
Industrial sensors send comma-separated text records to an Amazon Data Firehose (Kinesis Data Firehose) stream through Direct PUT. The stream delivers to Amazon S3. The analytics team wants Firehose to write the data as Apache Parquet so that Amazon Athena queries scan less data, without adding another processing service after delivery.

Which combination of steps must the team take? (Select TWO.)

- **A.** Add a Firehose data transformation Lambda function that converts each CSV record into a JSON object.
- **B.** Choose a CSV deserializer in the record format conversion settings so that Firehose can read the comma-separated fields directly.
- **C.** Define the record schema in an AWS Glue Data Catalog table, and reference that table in the record format conversion settings.
- **D.** Enable dynamic partitioning with inline JQ parsing so that Firehose splits each CSV line into named columns before conversion.
- **E.** Lower the S3 buffer size to 1 MiB so that any records that fail conversion are isolated in smaller files.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, C**

**Why A and C:** Firehose record format conversion accepts **JSON input only**. Its deserializers are the OpenX JSON SerDe and the Apache Hive JSON SerDe. CSV must first be turned into JSON with a Lambda transformation. The conversion also needs a **schema from a Glue Data Catalog table** to map fields to Parquet (or ORC) columns.

**Why not the others:**
- **B** — There is no CSV deserializer for format conversion.
- **D** — JQ expressions parse JSON, not CSV, and dynamic partitioning chooses S3 prefixes. It doesn't create columns.
- **E** — Format conversion requires a buffer size of at least 64 MiB.

**Signal words:** *"comma-separated"*, *"write the data as Apache Parquet"*, *"without adding another processing service"* · **Skill:** 1.2.6 · **Review:** [Guide 07 — Amazon Data Firehose](../topic-guides/07-Amazon-Data-Firehose.md)
</details>

---

### Question 35
An online retailer stores shopping-cart items in a DynamoDB table. Items must be removed automatically about 90 days after they are created, without consuming the table's write capacity. For compliance, every removed item must be archived to Amazon S3.

Which combination of steps meets these requirements with the LEAST operational overhead? (Select TWO.)

- **A.** Schedule a Lambda function that scans the table every night, writes items older than 90 days to S3, and then deletes them with `BatchWriteItem`.
- **B.** Enable Time to Live (TTL) on an attribute that stores each item's expiry time as a Number in Unix epoch seconds.
- **C.** Enable Time to Live (TTL) on an attribute that stores each item's expiry time as an ISO 8601 date string.
- **D.** Export the table to S3 every day with a point-in-time export, and then remove items older than 90 days with a script that calls `DeleteItem` for each one.
- **E.** Enable DynamoDB Streams with old images, and use a Lambda function that filters for TTL deletions to write the expired items to S3.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, E**

**Why B and E:** TTL deletes expired items in the background at no write-capacity cost, and the TTL attribute must be a **Number in epoch seconds**. Deletions usually happen within a few days of expiry, which fits "about 90 days". TTL deletions appear in DynamoDB Streams with `userIdentity.principalId` set to `dynamodb.amazonaws.com`. An event filter on that value lets a Lambda function archive only the expired items, for example through Firehose to S3.

**Why not the others:**
- **A** — Scans and deletes consume read and write capacity, and it's custom code to maintain.
- **C** — TTL ignores attributes that aren't Numbers in epoch seconds, so items would never expire.
- **D** — Daily full exports cost far more than needed, and the deletes still consume write capacity.

**Signal words:** *"removed automatically"*, *"without consuming write capacity"*, *"every removed item must be archived"* · **Skill:** 2.3.4 · **Review:** [Guide 27 — DynamoDB](../topic-guides/27-DynamoDB.md)
</details>

---

### Question 36
A company's Amazon Quick Sight (formerly Amazon QuickSight) dashboards use direct query against Amazon Athena. About 400 users open the dashboards throughout the day, and every visual load runs Athena queries, so dashboards are slow and Athena costs keep rising. The underlying data is refreshed by a pipeline once an hour.

Which solution will improve dashboard performance and reduce cost the MOST?

- **A.** Keep direct query, and turn on Athena query result reuse in the workgroup that Quick Sight uses.
- **B.** Load the data into an Amazon Redshift provisioned cluster, and switch the dashboards to direct query against Redshift.
- **C.** Create a dedicated Athena workgroup for Quick Sight with a per-query data usage control to cap each query's scan.
- **D.** Import the dataset into SPICE, and schedule a refresh every hour.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** SPICE is Quick Sight's in-memory engine. Dashboards read from SPICE instead of querying the source, so visuals load fast and Athena runs only during refreshes, not on every page view. An hourly refresh matches how often the data changes, and incremental refresh can shorten each refresh where the dataset supports it.

**Why not the others:**
- **A** — Result reuse helps only when queries are identical within the reuse window, and every visual still waits on Athena.
- **B** — An always-on cluster adds cost and still runs a query for every visual.
- **C** — Capping scans limits the damage from a bad query, but it doesn't make dashboards faster or reduce the number of queries.

**Signal words:** *"every visual load runs Athena queries"*, *"refreshed … once an hour"*, *"performance and cost"* · **Skill:** 3.2.1 · **Review:** [Guide 35 — Analytics, Visualization & Notebooks](../topic-guides/35-Analytics-Visualization-Quick-Notebooks.md)
</details>

---

### Question 37
A company's Kinesis data stream has 10 shards. Five independent applications poll the stream with `GetRecords`. The applications report `ReadProvisionedThroughputExceeded` errors and 1–2 seconds of delay between a record being written and being read. Each application needs its own read throughput and a propagation delay of about 70 ms.

Which solution meets these requirements?

- **A.** Increase the number of shards to 25 so that the polling applications share more total read capacity.
- **B.** Increase how often each application calls `GetRecords` so that it picks up new records sooner.
- **C.** Register each application as an enhanced fan-out consumer that reads with `SubscribeToShard`.
- **D.** Create a Firehose stream for each application that reads from the data stream and delivers records to that application's own S3 bucket.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** With enhanced fan-out, each registered consumer gets its own **2 MB/s per shard** of read throughput. Records are pushed over HTTP/2 through `SubscribeToShard`, with a typical delay of about 70 ms. Standard polling consumers share one 2 MB/s per shard and a small number of `GetRecords` calls per second per shard. A stream supports 20 enhanced fan-out consumers (50 in On-demand Advantage mode), so five is well within the limit.

**Why not the others:**
- **A** — Consumers still share each shard's read limit, and polling delay stays near a second.
- **B** — More frequent calls hit the per-shard `GetRecords` limit sooner and cause more throttling.
- **D** — Firehose buffering adds seconds to minutes of delay, and the applications would read from S3 instead of the stream.

**Signal words:** *"five independent applications"*, *"ReadProvisionedThroughputExceeded"*, *"own read throughput"*, *"~70 ms"* · **Skill:** 1.1.10 · **Review:** [Guide 06 — Kinesis Data Streams](../topic-guides/06-Kinesis-Data-Streams.md)
</details>

---

### Question 38
A `sales` table is registered in AWS Lake Formation and queried with Amazon Athena. EU analysts must see only rows where `region = 'EU'`, and they must not see the `ssn` or `date_of_birth` columns. Global analysts can see all rows but must not see the `ssn` column. The data must stay in a single table.

Which solution meets these requirements with the LEAST operational overhead?

- **A.** Create an Athena view for each analyst group that selects the allowed rows and columns, and give each group's IAM role access only to its view.
- **B.** Create Lake Formation data filters that combine a row filter expression with excluded columns, and grant `SELECT` on the table to each group through its filter.
- **C.** Write each region's rows to a separate S3 prefix, and add bucket policy statements that allow each group's role to read only its prefix.
- **D.** Use Amazon Macie to find the sensitive columns, and have Macie mask them for the EU analysts' role.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Lake Formation data filters give **cell-level security**: a row filter expression combined with column inclusion or exclusion on the same table. You grant `SELECT` through the filter, and Athena enforces it for that principal automatically. One table, two filters, and no copies.

**Why not the others:**
- **A** — Views multiply objects to maintain, and without Lake Formation permissions the roles could still query the base table or read S3 directly.
- **C** — Prefix-level S3 access can't hide columns and breaks the single-table requirement.
- **D** — Macie discovers and reports sensitive data. It doesn't mask query results.

**Signal words:** *"only rows where"*, *"must not see … columns"*, *"single table"* · **Skill:** 4.2.4 · **Review:** [Guide 40 — Lake Formation](../topic-guides/40-Lake-Formation.md)
</details>

---

### Question 39
A versioned S3 bucket stores reports. Reports are read often for 30 days, then rarely until day 180. After that they must be kept for audits until they are 7 years old, and a retrieval time of up to 12 hours is acceptable. Overwrites create noncurrent versions, which only need to be kept for 30 days, and expired delete markers are piling up.

Which S3 Lifecycle configuration is the MOST cost-effective?

- **A.** Transition current versions to S3 Standard-IA at 30 days and to S3 Glacier Deep Archive at 180 days, expire them at 2,555 days, expire noncurrent versions 30 days after they become noncurrent, and remove expired object delete markers.
- **B.** Transition current versions to S3 Standard-IA at 15 days and to S3 Glacier Flexible Retrieval at 180 days, expire them at 2,555 days, and expire noncurrent versions after 30 days.
- **C.** Transition current versions to S3 One Zone-IA at 30 days and to S3 Glacier Deep Archive at 180 days, and expire them at 2,555 days.
- **D.** Transition current versions to S3 Intelligent-Tiering on day 0 without the optional archive tiers, expire them at 2,555 days, and expire noncurrent versions after 30 days.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** Standard-IA suits rarely read data after the 30-day minimum. Glacier Deep Archive is the cheapest class, and its standard retrievals finish within 12 hours. Expiring a current version in a versioned bucket only adds a delete marker, so you also need the **noncurrent version expiration** rule and the **expired delete marker** cleanup to actually free the storage.

**Why not the others:**
- **B** — Lifecycle rules can't move objects to Standard-IA before they are 30 days old, and Flexible Retrieval costs more than Deep Archive when 12 hours is acceptable.
- **C** — Noncurrent versions and delete markers would pile up forever, and One Zone-IA gives up multi-AZ resilience for audit data.
- **D** — Without the archive tiers, Intelligent-Tiering never gets close to Deep Archive prices, it adds a per-object monitoring fee, and the delete markers aren't cleaned up.

**Signal words:** *"versioned"*, *"12 hours is acceptable"*, *"noncurrent versions … 30 days"*, *"delete markers"* · **Skill:** 2.3.2 · **Review:** [Guide 05 — S3 Data Lake Storage](../topic-guides/05-S3-Data-Lake-Storage.md)
</details>

---

### Question 40
An AWS Glue Spark job uses `spark.read.json()` to read an S3 prefix that holds about 2 million JSON files of roughly 10 KB each. The job fails with a driver `OutOfMemoryError` before any data is processed.

Which change will fix the failure with the LEAST effort?

- **A.** Change the worker type to a memory-optimized R.2X worker so that the driver has more memory to track file metadata during planning.
- **B.** Double the number of workers so that more executors share the work of reading the files.
- **C.** Increase the job timeout, and turn on continuous logging to capture the driver's memory usage.
- **D.** Read the files with a Glue DynamicFrame that sets `groupFiles` to `inPartition` with a `groupSize`, and use `useS3ListImplementation`.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Spark's file reader tracks every file on the driver and creates a task for each small file, so millions of tiny files exhaust driver memory. Glue's S3 file grouping combines many files into each task, which Glue turns on automatically above 50,000 files for DynamicFrame reads. `useS3ListImplementation` lists S3 in batches, which reduces driver memory further.

**Why not the others:**
- **A** — A bigger driver only delays the failure as the file count grows, and it costs more on every run.
- **B** — This is a driver problem. More executors don't reduce driver memory use.
- **C** — The job fails from memory pressure, not from running out of time.

**Signal words:** *"2 million … 10 KB"* files, *"driver OutOfMemoryError"*, *"before any data is processed"* · **Skill:** 1.2.7 · **Review:** [Guide 12 — AWS Glue ETL](../topic-guides/12-AWS-Glue-ETL.md)
</details>

---

### Question 41
An AWS Glue job writes a line that contains `RECORD_REJECTED` to CloudWatch Logs each time it drops an invalid record. The data team wants an email whenever more than 100 records are rejected within 5 minutes.

Which combination of steps meets this requirement with the LEAST operational overhead? (Select TWO.)

- **A.** Create a metric filter on the job's log group that matches `RECORD_REJECTED` and publishes a count to a custom metric.
- **B.** Create a subscription filter that streams the log group to a Lambda function, which counts rejections and publishes to Amazon SNS when the count passes 100.
- **C.** Enable CloudTrail data events for CloudWatch Logs so that each rejected record is recorded as an event.
- **D.** Create a CloudWatch alarm on the metric (Sum greater than 100 over 5 minutes) whose action notifies an SNS topic with an email subscription.
- **E.** Create an EventBridge rule that matches log lines containing `RECORD_REJECTED` and targets an SNS topic.

<details>
<summary><b>Show answer</b></summary>

**Answer: A, D**

**Why A and D:** A **metric filter** turns matching log lines into a CloudWatch metric with no code. A **CloudWatch alarm** on the metric's Sum over a 5-minute period sends a notification to SNS, which delivers the email. Both are managed configuration.

**Why not the others:**
- **B** — It works, but custom counting code adds effort compared with a metric filter.
- **C** — CloudTrail records API calls, not the contents of application log lines.
- **E** — Log lines aren't events on an EventBridge bus, so EventBridge can't match their text.

**Signal words:** *"more than 100 … within 5 minutes"*, *"email"*, *"LEAST operational overhead"* · **Skill:** 3.3.3 · **Review:** [Guide 32 — Monitoring, Logging & Troubleshooting](../topic-guides/32-Monitoring-Logging-Troubleshooting.md)
</details>

---

### Question 42
A company is starting a new analytics platform built on Apache Iceberg tables that will be queried with Amazon Athena and Amazon EMR. The team doesn't want to build or schedule jobs for compaction, snapshot expiration, or cleanup of unreferenced files. It also wants to control access to individual tables with IAM policies.

Which storage approach meets these requirements with the LEAST operational overhead?

- **A.** Store the Iceberg tables in a general purpose S3 bucket, and schedule a Glue Spark job that runs the `rewrite_data_files` and `expire_snapshots` procedures every night.
- **B.** Create a table bucket in Amazon S3 Tables, create the tables in a namespace, and use the integration with AWS analytics services.
- **C.** Store the Iceberg data files in an S3 Express One Zone directory bucket so that queries get single-digit-millisecond latency.
- **D.** Create AWS Lake Formation governed tables, which provide automatic compaction and ACID transactions.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Amazon S3 Tables stores Iceberg tables in **table buckets** as first-class resources with their own ARNs, so IAM policies can grant access per table or namespace. S3 Tables runs **compaction, snapshot management, and unreferenced file removal automatically**. Through the AWS analytics integration (the `s3tablescatalog` in the Glue Data Catalog), Athena and EMR can query the tables directly.

**Why not the others:**
- **A** — It works, but it's the maintenance job the team wants to avoid, and access control stays at the object and prefix level.
- **C** — Directory buckets speed up object access, but they don't manage tables or run table maintenance.
- **D** — ⚠️ Lake Formation governed tables were discontinued (writes and queries stopped Dec 31, 2024). Use Iceberg instead.

**Signal words:** *"new"* platform, *"don't want to build or schedule"* maintenance, *"individual tables with IAM policies"* · **Skill:** 2.1.7 · 🆕 v1.1 skill · **Review:** [Guide 04 — Open Table Formats & S3 Tables](../topic-guides/04-Open-Table-Formats-S3-Tables.md)
</details>

---

### Question 43
A marketing team needs the Opportunity and Account objects from its Salesforce organization copied to Amazon S3 as Parquet every day, with only records that changed since the previous run. The team has no developers, and the solution must not require custom code.

Which solution meets these requirements with the LEAST operational overhead?

- **A.** Schedule a Lambda function with EventBridge that calls the Salesforce REST API, pages through the changed records, and writes them to S3 as JSON.
- **B.** Create an AWS DMS task with a Salesforce source endpoint and an S3 target endpoint that writes Parquet.
- **C.** Create an Amazon AppFlow flow with a Salesforce source, a daily schedule with incremental transfer, and an S3 destination in Parquet format.
- **D.** Write an AWS Glue Python shell job that calls the Salesforce Bulk API and converts the results to Parquet with pandas.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Amazon AppFlow is a managed, no-code integration service for SaaS applications. A scheduled flow can transfer only records that changed since the last run, based on a timestamp field. It writes to S3 in Parquet, CSV, or JSON, and AppFlow handles authentication, pagination, and API limits.

**Why not the others:**
- **A** — It's custom code, and it doesn't produce Parquet.
- **B** — Salesforce isn't an AWS DMS source. DMS migrates databases.
- **D** — It's custom code for a problem AppFlow solves with configuration.

**Signal words:** *"Salesforce"*, *"only records that changed"*, *"no custom code"* · **Skill:** 1.1.2 · **Review:** [Guide 11 — DataSync, Transfer Family, Snow & AppFlow](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md)
</details>

---

### Question 44
An enterprise uses a single Amazon SageMaker Unified Studio domain. The finance and marketing business units each want their own leads to decide who can create projects in their part of the organization, without making those leads domain administrators. A central governance team must remain the only group that can create business glossary terms.

Which solution meets these requirements?

- **A.** Create a domain unit for each business unit, make the leads owners of their domain units, and grant project creation policies on those units.
- **B.** Create a separate SageMaker Unified Studio domain for each business unit, and make each business unit's lead the administrator of that domain.
- **C.** Add the finance and marketing leads as domain administrators, and ask them to create projects only for their own business unit.
- **D.** Attach IAM policies to the leads' roles that allow `datazone:CreateProject` only on resources tagged with their business unit's name.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** Domain units form a hierarchy inside a domain that mirrors the organization. Domain unit owners receive delegated authority, and **authorization policies** attached to units, such as project creation or glossary creation, decide who can do what. The leads control project creation in their own units, and a glossary creation policy granted only to the governance team keeps that power central.

**Why not the others:**
- **B** — Separate domains split the catalog and the governance rules the company wants to share.
- **C** — Domain administrators control the entire domain, which gives the leads far too much access.
- **D** — IAM policies don't manage Unified Studio's own authorization model for projects.

**Signal words:** *"single domain"*, *"leads … decide who can create projects"*, *"without making them domain administrators"* · **Skill:** 4.1.7 · 🆕 v1.1 skill · **Review:** [Guide 41 — SageMaker Unified Studio, Catalog & Governance](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md)
</details>

---

### Question 45
A Step Functions workflow must process 3 million JSON objects under an S3 prefix, sending each object to a Lambda function. The team needs thousands of objects processed in parallel, must track the result of each item, and wants the run to succeed as long as no more than 1% of items fail. The current design, an inline Map state, fails because the execution exceeds its event history quota.

Which solution meets these requirements?

- **A.** Keep the inline Map state, and set `MaxConcurrency` to 0 to remove the concurrency limit.
- **B.** Replace the Map state with a Parallel state that has 1,000 branches, each of which processes a share of the objects.
- **C.** Replace the workflow with S3 Event Notifications that invoke the Lambda function once for each object, and track failures in a DynamoDB table.
- **D.** Use a Map state in Distributed mode with an S3 `ItemReader`, child workflow executions, and `ToleratedFailurePercentage` set to 1.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Distributed Map runs each item, or batch of items, as a **separate child workflow execution**, so the parent's event history no longer grows with the item count. It supports up to 10,000 parallel child executions. `ItemReader` lists the S3 prefix (or reads an S3 Inventory manifest, CSV, or JSON), and `ToleratedFailurePercentage` sets the failure threshold. `ResultWriter` can save each item's result to S3.

**Why not the others:**
- **A** — An inline Map runs inside the parent execution, so its history still grows with the items, and its concurrency is far lower.
- **B** — Parallel branches are fixed in the definition, and the history quota still applies.
- **C** — Existing objects don't raise new events, and this drops the orchestration and failure threshold.

**Signal words:** *"3 million objects"*, *"thousands in parallel"*, *"no more than 1% of items fail"*, *"event history quota"* · **Skill:** 3.1.1 · **Review:** [Guide 20 — Step Functions](../topic-guides/20-Step-Functions.md)
</details>

---

### Question 46
An AWS Glue job must pull data from a partner's PostgreSQL database that is reachable over the internet. The partner's firewall accepts connections only from IP addresses on an allowlist, and the partner will add only a small number of fixed addresses.

Which solution meets these requirements?

- **A.** Send the partner the current AWS IP address ranges for AWS Glue in the Region, and have the partner allowlist all of them.
- **B.** Configure the Glue connection to use a public subnet whose route table sends internet traffic to an internet gateway.
- **C.** Configure the Glue connection in a private subnet that routes internet traffic to a NAT gateway, and allowlist the NAT gateway's Elastic IP.
- **D.** Assign an Elastic IP address to the Glue job in its job properties, and give that address to the partner.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** A Glue job that uses a connection creates elastic network interfaces in the chosen subnet, and those interfaces get only private IP addresses. Routing that subnet's internet traffic through a **NAT gateway** makes all outbound connections come from the NAT gateway's **Elastic IP**, one fixed address the partner can allowlist.

**Why not the others:**
- **A** — The ranges are huge, shared with other customers, and change over time, which is a security risk and doesn't meet the "small number" requirement.
- **B** — Glue's network interfaces don't receive public IP addresses, so a public subnet gives no internet access.
- **D** — Glue jobs have no setting for assigning an Elastic IP.

**Signal words:** *"allowlist"*, *"small number of fixed addresses"* · **Skill:** 1.1.8 · **Review:** [Guide 38 — Networking for Data Pipelines](../topic-guides/38-Networking-for-Data-Pipelines.md)
</details>

---

### Question 47
A manufacturer wants a generative AI assistant that answers technicians' questions from 200,000 PDF and HTML equipment manuals stored in Amazon S3. It needs managed document parsing, chunking, embedding, and vector storage. New and changed manuals must be picked up without reprocessing the whole collection. This is a new solution, and the company wants the LEAST operational overhead.

Which solution meets these requirements?

- **A.** Create an Amazon Kendra index with an S3 data source connector, and send the retrieved passages to a foundation model in the application code.
- **B.** Create an Amazon Bedrock knowledge base with the S3 bucket as its data source, an embeddings model, and a supported vector store, and sync it when manuals change.
- **C.** Fine-tune a foundation model on the manuals with Amazon Bedrock model customization, and repeat the fine-tuning every month.
- **D.** Write an AWS Glue job that splits the manuals into passages, calls an embeddings model for each passage, and stores the vectors as Parquet in S3 for Amazon Athena to search.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** A Bedrock knowledge base is managed retrieval-augmented generation (RAG). It parses documents, **chunks** them (fixed-size, hierarchical, semantic, or none), creates **embeddings**, and writes them to a vector store such as OpenSearch Serverless, Aurora PostgreSQL, or S3 Vectors. Each **sync (ingestion job)** processes only documents that were added, changed, or deleted.

**Why not the others:**
- **A** — ⚠️ Amazon Kendra has been closed to new customers since July 30, 2026, so it's a poor choice for a new build. It also leaves the generation step to your own code.
- **C** — Fine-tuning is model training. It doesn't cite sources and goes stale between runs.
- **D** — It's a custom pipeline, and Athena has no approximate nearest-neighbor vector search.

**Signal words:** *"managed … chunking, embedding, and vector storage"*, *"picked up without reprocessing"*, *"new solution"* · **Skill:** 2.4.6 · 🆕 v1.1 skill · **Review:** [Guide 19 — GenAI, LLMs & Vectors](../topic-guides/19-GenAI-LLMs-Vectors.md)
</details>

---

### Question 48
A payments company streams card transactions into Kinesis Data Streams. For each card, it must continuously compute the count and total amount over a sliding 10-minute window and raise an alert within one second when thresholds are exceeded. Events can arrive up to 30 seconds out of order, and the aggregations must stay correct after failures, with exactly-once state.

Which solution meets these requirements?

- **A.** Build an Amazon Managed Service for Apache Flink (formerly Kinesis Data Analytics) application that uses event-time sliding windows, watermarks, and checkpointing.
- **B.** Use a Lambda event source mapping with tumbling windows, and keep each card's running totals in the state that Lambda passes between invocations.
- **C.** Deliver the stream through Amazon Data Firehose with a Lambda transformation that calculates the aggregates for each buffered batch.
- **D.** Deliver the stream to Amazon S3 with Firehose, and run an Athena query every minute that calculates the 10-minute aggregates.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** This is stateful stream processing. Flink keeps keyed state per card, supports **sliding windows on event time**, uses **watermarks** to handle late or out-of-order events, and **checkpoints** its state for exactly-once consistency after failures. Latency is below a second.

**Why not the others:**
- **B** — Lambda windows are tumbling (non-overlapping) windows, based on processing time, and capped at 15 minutes. They can't produce sliding event-time windows.
- **C** — Firehose transformations are stateless per batch and can't keep state across batches.
- **D** — Minute-level batch queries miss the one-second alert requirement.

**Signal words:** *"sliding 10-minute window"*, *"out of order"*, *"exactly-once state"*, *"within one second"* · **Skill:** 1.1.12 · **Review:** [Guide 09 — Managed Service for Apache Flink](../topic-guides/09-Managed-Service-for-Apache-Flink.md)
</details>

---

### Question 49
An Amazon MWAA environment uses the `mw1.small` environment class with a maximum of 2 workers. Every night at 1 AM, about 80 tasks from several DAGs become ready to run at the same time. The tasks stay in the queued state for a long time, and CloudWatch shows worker CPU near 100%. The scheduler is healthy, and the DAGs parse without errors.

What should the data engineer do to reduce the queuing?

- **A.** Increase the `dagbag_import_timeout` Airflow configuration option so that the scheduler has more time to parse the DAGs.
- **B.** Move the DAG files from the `dags/` folder into `plugins.zip` so that workers load them faster.
- **C.** Restart the environment every night before 1 AM so that workers start with free resources.
- **D.** Raise the environment's maximum worker count, and move to a larger environment class if needed.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** MWAA scales workers automatically between the minimum and maximum you set, based on queued and running tasks. With a maximum of 2 small workers, the environment can't add capacity for an 80-task burst. Raising the maximum lets autoscaling absorb the peak and scale back down afterward. A larger class also increases how many tasks each worker runs and the scheduler's capacity.

**Why not the others:**
- **A** — Parsing isn't the problem. The DAGs load fine.
- **B** — `plugins.zip` holds custom operators and hooks. Where DAG files live doesn't change worker capacity.
- **C** — A restart doesn't add workers, and the queue builds again at 1 AM.

**Signal words:** *"maximum of 2 workers"*, *"80 tasks"*, *"queued"*, *"worker CPU near 100%"* · **Skill:** 3.1.2 · **Review:** [Guide 21 — MWAA & Glue Workflows](../topic-guides/21-MWAA-Glue-Workflows.md)
</details>

---

### Question 50
A Quick Sight dataset imported into SPICE holds sales for all regions. About 300 regional managers use a shared dashboard and must see only rows for their own region. Managers are already organized into one Quick Sight group per region.

Which solution meets these requirements with the LEAST operational overhead?

- **A.** Create a separate dataset and dashboard for each region, and share each dashboard only with that region's group.
- **B.** Create Lake Formation data filters on the source table for each region, and grant each group's IAM role access through its filter.
- **C.** Add row-level security to the dataset with a permissions dataset that maps each group to its region value.
- **D.** Add a filter control to the dashboard that defaults to each manager's region.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** Quick Sight **row-level security (RLS)** uses a permissions dataset (rules) that maps users or groups to the field values they may see. It applies to SPICE and direct-query datasets alike and filters every visual automatically. One dataset and one dashboard serve all regions, and a new manager needs only group membership.

**Why not the others:**
- **A** — Dozens of copies to build and keep in sync.
- **B** — A SPICE import runs under one identity, so source-side filters can't tell dashboard readers apart.
- **D** — Filter controls are for convenience, not security. Users can change them to see other regions.

**Signal words:** *"imported into SPICE"*, *"see only rows for their own region"*, *"groups per region"* · **Skill:** 4.2.5 · **Review:** [Guide 35 — Analytics, Visualization & Notebooks](../topic-guides/35-Analytics-Visualization-Quick-Notebooks.md)
</details>

---

### Question 51
An Amazon Redshift provisioned cluster is running out of storage. It holds 5 years of sales, but data older than 2 years is queried only a few times a month. Those queries must still run from Redshift SQL and sometimes join old and recent data. The company wants to keep the current cluster size and minimize cost.

Which combination of steps meets these requirements? (Select TWO.)

- **A.** Take a manual snapshot that contains the old data, and restore it to a separate cluster whenever someone needs to query it.
- **B.** Resize the cluster by adding nodes so that all 5 years fit in managed storage.
- **C.** `UNLOAD` rows older than 2 years to Amazon S3 as Parquet partitioned by year and month, and then delete them from the table.
- **D.** Define an external table in the Data Catalog over the unloaded files, and query it through Amazon Redshift Spectrum, for example in a late-binding view with `UNION ALL`.
- **E.** Create a federated query external schema that points to the unloaded Parquet files in Amazon S3.

<details>
<summary><b>Show answer</b></summary>

**Answer: C, D**

**Why C and D:** `UNLOAD ... FORMAT PARQUET PARTITION BY (...)` writes compressed columnar files to S3 in parallel, and deleting those rows (followed by `VACUUM`) frees cluster storage. **Spectrum** external tables let Redshift query and join the S3 data with local tables. A late-binding view with `UNION ALL` hides the split from users, and partitioned Parquet keeps Spectrum scan costs low.

**Why not the others:**
- **A** — Restoring a cluster takes time and money for every query, and it can't join with the live cluster.
- **B** — Adding nodes breaks the "keep the current cluster size" requirement and costs the most.
- **E** — Federated queries reach RDS and Aurora databases, not files in S3.

**Signal words:** *"queried only a few times a month"*, *"must still run from Redshift SQL"*, *"keep the current cluster size"* · **Skill:** 2.3.1 · **Review:** [Guide 24 — Redshift Loading, Integration & Sharing](../topic-guides/24-Redshift-Loading-Integration-Sharing.md)
</details>

---

### Question 52
An operations dashboard reads from Amazon Redshift. Events arrive in a Kinesis data stream, and today Firehose stages them in Amazon S3 and loads them with `COPY`, so dashboards lag by several minutes. The team wants the events queryable in Redshift within seconds, with no staging in S3 and the LEAST operational overhead.

Which solution meets these requirements?

- **A.** Set the Firehose buffering interval for the Redshift destination to 0 seconds so that records are copied as soon as they arrive.
- **B.** Use Amazon Redshift streaming ingestion with an external schema mapped to the stream and a materialized view that refreshes automatically.
- **C.** Add a Lambda consumer to the stream that inserts each record into Redshift with the Redshift Data API.
- **D.** Create a Redshift Spectrum external table that reads the Kinesis stream directly at query time.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Streaming ingestion reads Kinesis Data Streams (or Amazon MSK) directly into a Redshift materialized view. You create an external schema `FROM KINESIS` and a materialized view with `AUTO REFRESH YES`. Records are queryable within seconds, with no S3 staging and no Firehose.

**Why not the others:**
- **A** — Firehose to Redshift always stages in S3 and runs `COPY`, which adds delay and can't meet a no-staging requirement.
- **C** — Row-by-row inserts are slow and costly in a columnar warehouse, and it's custom code.
- **D** — Spectrum reads files in S3. It can't read Kinesis streams.

**Signal words:** *"within seconds"*, *"no staging in S3"*, *"Kinesis … Redshift"* · **Skill:** 1.1.1 · **Review:** [Guide 24 — Redshift Loading, Integration & Sharing](../topic-guides/24-Redshift-Loading-Integration-Sharing.md)
</details>

---

### Question 53
A table in the AWS Glue Data Catalog was deleted yesterday, and the security team needs to know which IAM principal deleted it. The account has no custom CloudTrail trails or other logging configured.

Which action identifies the principal with the LEAST effort?

- **A.** Search CloudTrail Event history for `DeleteTable` events from `glue.amazonaws.com`.
- **B.** Search the CloudWatch Logs log groups of the account's Glue jobs for the table name.
- **C.** Turn on AWS Config, and review the configuration timeline for the deleted table.
- **D.** Review the S3 server access logs for the bucket that holds the table's data.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** CloudTrail Event history is on by default and keeps **90 days of management events** at no charge. `DeleteTable` is a management event, and the record shows the caller's identity, source IP address, and time. (Data events never appear in Event history, but this call isn't a data event.)

**Why not the others:**
- **B** — Job logs show what jobs did, not who called catalog APIs from the console, CLI, or SDK.
- **C** — AWS Config records changes only after recording is turned on, so it can't show yesterday.
- **D** — The table's metadata lives in the Data Catalog, not S3. Deleting it doesn't touch the bucket.

**Signal words:** *"which IAM principal"*, *"yesterday"*, *"no … trails configured"* · **Skill:** 3.3.5 · **Review:** [Guide 43 — Audit Logging, CloudTrail & Config](../topic-guides/43-Audit-Logging-CloudTrail-Config.md)
</details>

---

### Question 54
A nightly ETL job on Amazon Redshift runs `TRUNCATE` and then loads a staging table, but tonight it has been waiting for more than an hour. A data engineer suspects that a BI user opened a transaction on the table hours ago and never committed it.

What should the data engineer do to confirm and release the blockage?

- **A.** Run `VACUUM` on the staging table so that Redshift releases the locks that the old transaction holds.
- **B.** Reboot the cluster during business hours so that every open session and transaction is ended at once.
- **C.** Increase the number of concurrency slots in the ETL job's WLM queue.
- **D.** Query `STV_LOCKS` or `SVV_TRANSACTIONS` to find the session holding the lock, and end it with `PG_TERMINATE_BACKEND`.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** `TRUNCATE` needs an exclusive lock, so it waits behind any open transaction that holds a lock on the table. The lock system views show which process ID holds the lock and how long it has been held. `PG_TERMINATE_BACKEND(pid)` ends that session and rolls back its open transaction, and the ETL continues.

**Why not the others:**
- **A** — `VACUUM` reclaims space and re-sorts rows. It doesn't release other sessions' locks, and it would need a lock itself.
- **B** — A reboot disconnects every user to fix a single session.
- **C** — The job isn't waiting for a WLM slot. It's waiting for a table lock.

**Signal words:** *"waiting"*, *"opened a transaction … never committed"* · **Skill:** 2.1.6 · **Review:** [Guide 25 — Redshift Performance, Operations & Security](../topic-guides/25-Redshift-Performance-Operations-Security.md)
</details>

---

### Question 55
A team is building a serverless pipeline made up of a Lambda function, a Step Functions state machine, and a DynamoDB table. It wants to define all three in one template with short, serverless-specific syntax, test the Lambda function locally before committing, and deploy through its CI/CD pipeline.

Which solution meets these requirements?

- **A.** Write an AWS CloudFormation template that embeds the function code with the `ZipFile` property, and deploy it with CloudFormation StackSets.
- **B.** Define the resources in an AWS CodeDeploy AppSpec file, and use CodeDeploy to roll out new versions of the function.
- **C.** Write an AWS SAM template with `AWS::Serverless::Function`, `AWS::Serverless::StateMachine`, and `AWS::Serverless::SimpleTable`, test with `sam local invoke`, and deploy with `sam deploy`.
- **D.** Publish the pipeline as an application in the AWS Serverless Application Repository, and deploy it from there.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** AWS SAM extends CloudFormation with short resource types for serverless applications. `sam local invoke` runs the function in a local container, and `sam build` and `sam deploy` package and deploy the stack from a CI/CD pipeline such as CodePipeline or CodeBuild.

**Why not the others:**
- **A** — Plain CloudFormation is more verbose, and it has no local testing. StackSets deploy across accounts and Regions, which isn't needed here.
- **B** — An AppSpec file controls how CodeDeploy shifts traffic between versions. It doesn't define state machines or tables.
- **D** — The Serverless Application Repository shares finished applications. It isn't a way to build and test them, and it's out of scope for the exam.

**Signal words:** *"Lambda, Step Functions, DynamoDB"*, *"one template"*, *"test locally"* · **Skill:** 1.4.6 · **Review:** [Guide 17 — Lambda for Data Pipelines](../topic-guides/17-Lambda-for-Data-Pipelines.md)
</details>

---

### Question 56
A market-data company wants to sell curated datasets to external customers. Subscribers must query the live data from their own Amazon Redshift warehouses without copying files. The company wants AWS to handle subscription terms, entitlements, and billing, and access must end automatically when a subscription expires.

Which solution meets these requirements?

- **A.** Create a Redshift datashare, and share it directly with each subscriber's AWS account after the customer signs a contract.
- **B.** Publish a data product in AWS Data Exchange that is backed by an Amazon Redshift datashare.
- **C.** `UNLOAD` the datasets to Amazon S3 every day, and send each subscriber presigned URLs to the files.
- **D.** Register the datasets with AWS Lake Formation, and share the tables with subscriber accounts through AWS RAM.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** AWS Data Exchange for Amazon Redshift packages a datashare as a product that subscribers find and buy. Data Exchange manages the subscription terms, entitlements, and billing through AWS Marketplace, and it grants and revokes datashare access as subscriptions start and end. Subscribers query the provider's live data with no copies.

**Why not the others:**
- **A** — Direct data sharing is live, but it has no billing, entitlements, or automatic expiry, so you'd build those yourself.
- **C** — Files are copies, the data isn't live, and there's no subscription management.
- **D** — Lake Formation and RAM sharing are meant for trusted accounts, usually inside your organization, and have no commercial terms or billing.

**Signal words:** *"sell … to external customers"*, *"live"*, *"billing"*, *"access must end automatically"* · **Skill:** 4.5.7 · 🆕 v1.1 skill · **Review:** [Guide 41 — SageMaker Unified Studio, Catalog & Governance](../topic-guides/41-SageMaker-Unified-Studio-Catalog-Governance.md)
</details>

---

### Question 57
Business analysts with no coding skills receive a supplier price file every week. They need to see data quality statistics such as missing values, duplicates, and value distributions. Every week, the file must be checked automatically against thresholds such as "no more than 2% of prices missing", with the results shown visually.

Which combination of steps meets these requirements? (Select TWO.)

- **A.** Build an AWS Glue Studio notebook that runs Deequ checks on each week's file.
- **B.** Create an AWS Glue DataBrew profile job on the dataset, and schedule it to run every week.
- **C.** Explore the file in an Amazon Athena notebook for Apache Spark, and rerun the notebook every week.
- **D.** Turn on Quick Sight ML-powered anomaly detection for a dashboard built on the file.
- **E.** Define a DataBrew data quality ruleset with the thresholds, and attach it to the profile job.

<details>
<summary><b>Show answer</b></summary>

**Answer: B, E**

**Why B and E:** A **DataBrew profile job** calculates column statistics such as missing values, duplicates, distributions, and correlations, and shows them visually with no code. A **data quality ruleset** attached to the profile job checks the thresholds on every scheduled run and reports pass or fail for each rule.

**Why not the others:**
- **A** — Notebooks and Deequ require code.
- **C** — Spark notebooks require code and a manual rerun every week.
- **D** — Anomaly detection flags unusual metric values in a dashboard. It doesn't check data quality thresholds.

**Signal words:** *"no coding skills"*, *"statistics such as missing values"*, *"checked automatically against thresholds"* · **Skill:** 3.4.2 · **Review:** [Guide 14 — Glue DataBrew & Data Preparation](../topic-guides/14-Glue-DataBrew-Data-Preparation.md)
</details>

---

### Question 58
A company is migrating an Oracle database to Amazon Aurora PostgreSQL. It must convert the schemas and the PL/SQL stored procedures, functions, and packages to PostgreSQL. The team doesn't want to install or maintain any client software for the conversion.

Which solution meets these requirements?

- **A.** Use AWS DMS Schema Conversion in a DMS migration project to assess and convert the schema and code objects.
- **B.** Install the AWS Schema Conversion Tool on an Amazon EC2 instance, and run the conversion from there.
- **C.** Create an AWS DMS replication task that uses the "drop tables on target" preparation mode so that DMS creates the target schema.
- **D.** Run an AWS Glue crawler against the Oracle database, and create the Aurora tables from the table definitions that it catalogs.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** DMS Schema Conversion is the managed, console-based successor to the downloadable AWS SCT. Inside a DMS migration project, it produces an assessment report and converts schemas and code objects, such as procedures, functions, and packages, for heterogeneous migrations like Oracle to Aurora PostgreSQL.

**Why not the others:**
- **B** — AWS SCT converts code too, but it's client software you install and maintain, even on EC2.
- **C** — DMS creates only basic tables and primary keys. It doesn't convert stored code, secondary indexes, or other objects.
- **D** — Crawlers catalog table metadata. They can't convert procedural code.

**Signal words:** *"PL/SQL … procedures"*, *"don't want to install or maintain any client software"* · **Skill:** 2.4.3 · **Review:** [Guide 10 — DMS & Database Ingestion](../topic-guides/10-DMS-Database-Ingestion.md)
</details>

---

### Question 59
A company runs a large, shared Amazon EKS cluster for its microservices, and the platform team manages it with mature tooling. The data team now needs to run Apache Spark batch jobs on the same cluster with EMR's optimized Spark runtime, without building or managing new cluster infrastructure.

Which solution meets these requirements?

- **A.** Launch a new Amazon EMR on EC2 cluster in the same VPC, and submit the Spark jobs to it as steps.
- **B.** Rewrite the Spark jobs as AWS Glue ETL jobs so that they run on serverless Glue capacity.
- **C.** Install the open-source Spark operator on the EKS cluster, and manage the Spark images and upgrades yourself.
- **D.** Register an EKS namespace as an Amazon EMR on EKS virtual cluster, and submit the jobs with `StartJobRun`.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** EMR on EKS runs EMR's Spark runtime on an existing EKS cluster. A namespace is registered as a **virtual cluster**, and jobs are submitted through the EMR API, with EMR handling images, logs, and job lifecycle while pods share the cluster's capacity.

**Why not the others:**
- **A** — It adds a separate cluster to run and pay for, and doesn't use the existing EKS cluster.
- **B** — Glue doesn't run on the company's EKS cluster, and rewriting the jobs is extra work.
- **C** — It works, but it means building and maintaining Spark images and upgrades yourself, without EMR's runtime.

**Signal words:** *"same cluster"* as EKS, *"EMR's optimized Spark runtime"*, *"without … new cluster infrastructure"* · **Skill:** 1.2.1 · **Review:** [Guide 18 — Containers, Batch & EC2 Compute](../topic-guides/18-Containers-Batch-EC2-Compute.md)
</details>

---

### Question 60
A European company has 50 AWS accounts in AWS Organizations. For data sovereignty, no one, including account administrators, may create data stores or other resources in any Region except `eu-central-1` and `eu-west-1`. The control must be preventive and apply to every current and future account.

Which solution meets these requirements?

- **A.** Deploy AWS Config rules to every account that detect resources created outside the two Regions and notify the security team.
- **B.** Attach an IAM permissions boundary to every role in every account that allows actions only in the two Regions.
- **C.** Attach a service control policy to the organization root that denies actions when `aws:RequestedRegion` isn't one of the two Regions, and exempt global services.
- **D.** Add a bucket policy to every S3 bucket that denies requests coming from any Region except the two approved Regions.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** SCPs set the maximum permissions for every principal in member accounts, including administrators and the member accounts' root users. An SCP at the root applies to all current and future accounts. The `aws:RequestedRegion` condition blocks API calls to other Regions. Global services such as IAM, Organizations, and CloudFront must be exempted so that they keep working.

**Why not the others:**
- **A** — Config rules detect problems after the fact. They don't prevent them.
- **B** — Administrators can remove boundaries, new roles can be created without them, and it's hard to enforce at scale.
- **D** — It covers only S3, and bucket policies don't control where other resources are created.

> **Watch out:** a Region-deny SCP doesn't stop S3 Cross-Region Replication or Redshift snapshot copy that's configured from an *allowed* Region. To block replication or backups to disallowed Regions, also deny those configuration actions, such as `s3:PutReplicationConfiguration`.

**Signal words:** *"including account administrators"*, *"preventive"*, *"every current and future account"* · **Skill:** 4.5.3 · **Review:** [Guide 42 — Privacy, PII, Masking & Sovereignty](../topic-guides/42-Privacy-PII-Masking-Sovereignty.md)
</details>

---

### Question 61
On Monday mornings from 9 to 10 AM, hundreds of dashboard queries run at the same time against an Amazon Redshift provisioned cluster, and many wait in queues. Each query finishes quickly once it starts, and the cluster is lightly used for the rest of the week.

Which solution handles the peak MOST cost-effectively with the LEAST operational overhead?

- **A.** Run an elastic resize to double the number of nodes, and keep the larger cluster.
- **B.** Enable concurrency scaling for the WLM queue that runs the dashboard queries.
- **C.** Schedule a classic resize before 9 AM every Monday, and another one afterward to return to the original size.
- **D.** Create a second provisioned cluster for BI users, and give it access to the data through a Redshift datashare.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Concurrency scaling adds transient cluster capacity automatically when queries start queuing, and removes it when the burst ends. You pay per second only while it runs, and each cluster earns free concurrency scaling credits every day that often cover short daily peaks. It's a single WLM queue setting.

**Why not the others:**
- **A** — You pay for double the capacity all week to handle one hour.
- **C** — A classic resize takes hours and limits the cluster while it runs, so it doesn't fit a regular one-hour peak.
- **D** — A second always-on cluster costs far more than burst capacity.

**Signal words:** *"hundreds … at the same time"*, *"wait in queues"*, *"finishes quickly once it starts"*, *"lightly used the rest of the week"* · **Skill:** 3.3.4 · **Review:** [Guide 25 — Redshift Performance, Operations & Security](../topic-guides/25-Redshift-Performance-Operations-Security.md)
</details>

---

### Question 62
About 200 external partners upload daily files over SFTP by using their existing SSH keys, and they can't change their client software. The files must land in Amazon S3 for processing, in a separate folder for each partner. The company doesn't want to run any servers.

Which solution meets these requirements?

- **A.** Create an AWS Transfer Family SFTP server with Amazon S3 storage and service-managed users that have SSH public keys and home directories mapped to prefixes.
- **B.** Run an SFTP server on Amazon EC2 instances in an Auto Scaling group behind a Network Load Balancer, and mount the S3 bucket on each instance with an open-source file system driver.
- **C.** Ask each partner to install an AWS DataSync agent in their own environment and create a task that copies their files to the company's bucket.
- **D.** Generate presigned S3 URLs for each partner every day, and have the partners upload their files with HTTPS `PUT` requests.

<details>
<summary><b>Show answer</b></summary>

**Answer: A**

**Why A:** AWS Transfer Family is a fully managed SFTP, FTPS, FTP, and AS2 endpoint that writes directly to S3 or EFS. Service-managed users authenticate with SSH public keys, and logical home directories keep each partner in its own prefix. Partners keep their existing SFTP clients.

**Why not the others:**
- **B** — It's a fleet of servers to patch, scale, and secure, which the company wants to avoid.
- **C** — Partners would have to install new software, which breaks the "can't change their client" requirement.
- **D** — Presigned URLs use HTTPS, not SFTP, so partners would have to change their tools.

**Signal words:** *"SFTP"*, *"existing SSH keys"*, *"land in Amazon S3"*, *"doesn't want to run any servers"* · **Skill:** 2.1.4 · **Review:** [Guide 11 — DataSync, Transfer Family, Snow & AppFlow](../topic-guides/11-DataSync-Transfer-Family-Snow-AppFlow.md)
</details>

---

### Question 63
An Amazon Redshift `customers` table has a `card_number` column. Analysts must see only the last four digits, and the fraud team must see the full number. Both groups query the same table. The company doesn't want to create views or copies, and the rule must apply to every query against the column.

Which solution meets these requirements?

- **A.** Grant analysts `SELECT` on every column except `card_number` by using column-level privileges.
- **B.** Create a view that returns only the last four digits with `SUBSTRING`, and grant analysts access to the view instead of the table.
- **C.** Encrypt `card_number` on the client side with an AWS KMS key before loading, and give only the fraud team permission to decrypt with the key.
- **D.** Create a dynamic data masking policy that shows only the last four digits, and attach it to the column for the analysts' role.

<details>
<summary><b>Show answer</b></summary>

**Answer: D**

**Why D:** Redshift dynamic data masking applies a masking policy (`CREATE MASKING POLICY`, then `ATTACH MASKING POLICY ... TO role`) to a column when a query runs, based on the user's role. Analysts get a partly masked value, while the fraud team, with no masking policy or an unmasked policy at higher priority, sees the real number. There are no views or copies, and every query path is covered.

**Why not the others:**
- **A** — Column privileges hide the entire column. Analysts need the last four digits.
- **B** — The requirement rules out views, and a view doesn't stop direct queries against the table.
- **C** — Ciphertext isn't partly readable, and decryption would have to happen outside SQL.

**Signal words:** *"only the last four digits"*, *"same table"*, *"no views or copies"* · **Skill:** 4.3.1 · **Review:** [Guide 25 — Redshift Performance, Operations & Security](../topic-guides/25-Redshift-Performance-Operations-Security.md)
</details>

---

### Question 64
A data science team runs Apache Spark and Apache Hive jobs a few times a week. Job sizes are unpredictable. Today the team keeps a 10-node Amazon EMR on EC2 cluster running around the clock, and it sits idle most of the time. The team wants to minimize cost and stop managing clusters.

Which solution meets these requirements?

- **A.** Keep the cluster, and enable EMR managed scaling with a minimum of one core node.
- **B.** Rewrite both the Spark and the Hive jobs as AWS Glue ETL jobs.
- **C.** Create Amazon EMR Serverless applications for Spark and Hive with automatic start and automatic stop.
- **D.** Keep the cluster, and move its core and task nodes to Spot Instances in instance fleets.

<details>
<summary><b>Show answer</b></summary>

**Answer: C**

**Why C:** EMR Serverless runs Spark and Hive jobs without clusters. Applications start automatically when a job is submitted and stop after they have been idle, and you pay only for the vCPU, memory, and storage that jobs use. It suits sporadic, unpredictable workloads.

**Why not the others:**
- **A** — The primary node and minimum capacity still run and cost money around the clock, and the team still manages a cluster.
- **B** — Glue runs Spark but not Hive, so the Hive jobs would need rewriting.
- **D** — Spot lowers the price per hour, but the cluster still runs idle, and Spot interruptions add risk.

**Signal words:** *"a few times a week"*, *"unpredictable"*, *"sits idle"*, *"stop managing clusters"*, *"Hive"* · **Skill:** 3.2.5 · **Review:** [Guide 15 — Amazon EMR](../topic-guides/15-Amazon-EMR.md)
</details>

---

### Question 65
A company runs 150 Apache Airflow DAGs on a self-managed Airflow installation on Amazon EC2. Patching, scaling, and upgrading the installation takes too much of the team's time. The team wants a managed service and minimal changes to the DAG code, and wants to keep using its existing Airflow operators and Python dependencies.

Which solution meets these requirements?

- **A.** Rewrite each DAG as an AWS Step Functions state machine, and schedule the state machines with EventBridge Scheduler.
- **B.** Create an Amazon MWAA environment, upload the DAGs to its S3 `dags/` folder, and list the dependencies in `requirements.txt`.
- **C.** Convert the DAGs to AWS Glue workflows with scheduled and conditional triggers.
- **D.** Replace each Airflow task with a Lambda function, and invoke the functions on schedules defined in EventBridge Scheduler.

<details>
<summary><b>Show answer</b></summary>

**Answer: B**

**Why B:** Amazon Managed Workflows for Apache Airflow (MWAA) runs standard Apache Airflow and manages the scheduler, workers, web server, and metadata database for you. DAGs are deployed by copying them to the environment's S3 `dags/` folder, Python dependencies go in `requirements.txt`, and custom plugins go in `plugins.zip`. Existing DAGs usually run with few or no changes.

**Why not the others:**
- **A** — Rewriting 150 DAGs is a new orchestration build, not a migration with minimal changes.
- **C** — Glue workflows orchestrate only Glue jobs and crawlers, not general Airflow operators.
- **D** — This throws away the DAGs' dependencies, retries, and monitoring, and it's a full rewrite.

**Signal words:** *"Apache Airflow DAGs"*, *"managed service"*, *"minimal changes"*, *"existing Airflow operators"* · **Skill:** 1.3.1 · **Review:** [Guide 21 — MWAA & Glue Workflows](../topic-guides/21-MWAA-Glue-Workflows.md)
</details>

---

## Answer key

| Q | Answer | Domain | Skill |
|---|---|---|---|
| 1 | B | 1 | 1.1.7 |
| 2 | D | 2 | 2.3.1 |
| 3 | A | 4 | 4.2.5 |
| 4 | C | 3 | 3.1.7 |
| 5 | C | 1 | 1.2.6 |
| 6 | A | 2 | 2.3.5 |
| 7 | D | 1 | 1.1.3 |
| 8 | B, E | 3 | 3.1.7 |
| 9 | A, C | 4 | 4.3.3 |
| 10 | B | 1 | 1.1.1 |
| 11 | A, C, F | 2 | 2.4.1 |
| 12 | A | 1 | 1.1.6 |
| 13 | D | 3 | 3.2.6 |
| 14 | B | 2 | 2.2.4 |
| 15 | C | 1 | 1.2.10 |
| 16 | A | 4 | 4.4.1 |
| 17 | A, D | 1 | 1.1.7 |
| 18 | D | 2 | 2.4.1 |
| 19 | B | 3 | 3.4.5 |
| 20 | C | 1 | 1.4.1 |
| 21 | B, D | 2 | 2.1.7 |
| 22 | A | 4 | 4.1.3 |
| 23 | B, C, E | 1 | 1.3.2 |
| 24 | C | 3 | 3.4.2 |
| 25 | D | 2 | 2.1.5 |
| 26 | B | 1 | 1.1.9 |
| 27 | C, E | 4 | 4.5.2 |
| 28 | A | 2 | 2.1.8 |
| 29 | B, D | 1 | 1.2.4 |
| 30 | D | 3 | 3.3.8 |
| 31 | C | 2 | 2.2.4 |
| 32 | B | 1 | 1.4.7 |
| 33 | A | 4 | 4.5.6 |
| 34 | A, C | 1 | 1.2.6 |
| 35 | B, E | 2 | 2.3.4 |
| 36 | D | 3 | 3.2.1 |
| 37 | C | 1 | 1.1.10 |
| 38 | B | 4 | 4.2.4 |
| 39 | A | 2 | 2.3.2 |
| 40 | D | 1 | 1.2.7 |
| 41 | A, D | 3 | 3.3.3 |
| 42 | B | 2 | 2.1.7 |
| 43 | C | 1 | 1.1.2 |
| 44 | A | 4 | 4.1.7 |
| 45 | D | 3 | 3.1.1 |
| 46 | C | 1 | 1.1.8 |
| 47 | B | 2 | 2.4.6 |
| 48 | A | 1 | 1.1.12 |
| 49 | D | 3 | 3.1.2 |
| 50 | C | 4 | 4.2.5 |
| 51 | C, D | 2 | 2.3.1 |
| 52 | B | 1 | 1.1.1 |
| 53 | A | 3 | 3.3.5 |
| 54 | D | 2 | 2.1.6 |
| 55 | C | 1 | 1.4.6 |
| 56 | B | 4 | 4.5.7 |
| 57 | B, E | 3 | 3.4.2 |
| 58 | A | 2 | 2.4.3 |
| 59 | D | 1 | 1.2.1 |
| 60 | C | 4 | 4.5.3 |
| 61 | B | 3 | 3.3.4 |
| 62 | A | 2 | 2.1.4 |
| 63 | D | 4 | 4.3.1 |
| 64 | C | 3 | 3.2.5 |
| 65 | B | 1 | 1.3.1 |

## Score by domain

| Domain | Questions | Your correct | % |
|---|---|---|---|
| 1 · Data Ingestion and Transformation (34%) | 22 — Q1, 5, 7, 10, 12, 15, 17, 20, 23, 26, 29, 32, 34, 37, 40, 43, 46, 48, 52, 55, 59, 65 | | |
| 2 · Data Store Management (26%) | 17 — Q2, 6, 11, 14, 18, 21, 25, 28, 31, 35, 39, 42, 47, 51, 54, 58, 62 | | |
| 3 · Data Operations and Support (22%) | 14 — Q4, 8, 13, 19, 24, 30, 36, 41, 45, 49, 53, 57, 61, 64 | | |
| 4 · Data Security and Governance (18%) | 12 — Q3, 9, 16, 22, 27, 33, 38, 44, 50, 56, 60, 63 | | |
| **Total** | **65** | | |

A multiple-response question counts as correct only if you chose exactly the right set of options. If one domain scores more than 10 points below your total, start there: each answer's **Review** link points to the guide that covers it.
