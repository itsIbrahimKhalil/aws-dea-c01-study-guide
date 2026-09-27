# 07 · Amazon Data Firehose — the courier van that leaves when it's full or the timer rings

> **Exam map:** D1 · Task 1.1, 1.2 — D2 · Task 2.4 · **Skills:** 1.1.1, 1.1.8, 1.2.6, 2.4.5 · **Weight:** 🔥🔥🔥 High · **Read time:** ~18 min

## The idea

Kinesis Data Streams ([Guide 06](06-Kinesis-Data-Streams.md)) is a highway with a dashcam. It keeps traffic moving and recorded, but it doesn't take anything anywhere. **Amazon Data Firehose** (formerly **Amazon Kinesis Data Firehose**, renamed **February 2024**; the exam guide and many questions still say "Kinesis Data Firehose") is the **courier van**. Parcels (records) are loaded at the depot. The van **leaves when it's full or when its timer rings, whichever comes first**. On the way it can stop at a **repackaging station** (a Lambda transform, or conversion to Parquet), sort parcels into **labelled bins** by address (dynamic partitioning), and drop anything undeliverable into a **returns bin** (an S3 error prefix). Then it unloads at a fixed list of destinations: S3, Redshift, OpenSearch, Splunk, HTTP endpoints, partner SaaS tools, and Apache Iceberg tables.

Firehose is **fully managed and serverless**: **no shards, no capacity planning, no consumer code**. It scales automatically. The price of that simplicity is that it's **near real-time**, not real-time. It buffers, so latency runs from seconds to minutes. It doesn't let you replay, it doesn't let your own apps read from it, and it doesn't promise strict ordering or exactly-once delivery.

Exam questions reach for Firehose whenever they say *"load streaming data into S3 / Redshift / OpenSearch / Splunk,"* *"least operational overhead,"* *"no code,"* *"convert to Parquet on the way,"* or *"partition by customer in S3."* This guide covers the buffer math, the transformation and partitioning features, the delivery details (including the IP-allowlisting skill 1.1.8), and the traps that separate Firehose from KDS.

## Sources: what can load the van

| Source | How | Notes |
|---|---|---|
| **Direct PUT** | `PutRecord` / `PutRecordBatch` from the SDK, or services that write directly: **CloudWatch Logs** subscription filters, **CloudWatch Metric Streams**, **EventBridge**, **AWS IoT** rules, **AWS WAF** logs, **SNS**, **API Gateway** access logs, **Kinesis Agent**, Fluent Bit/Fluentd, Lambda | Record ≤ **1,000 KiB**; `PutRecordBatch` ≤ **500 records or 4 MiB**. Per-stream default throughput in us-east-1, us-west-2, eu-west-1 is **500,000 records/s, 2,000 requests/s, 5 MiB/s** (lower elsewhere). Firehose raises it automatically when throttled, or you request more |
| **Kinesis Data Streams** | Firehose reads the stream as a consumer | No Firehose throughput quota applies. **De-aggregates KPL records** automatically |
| **Amazon MSK** | Firehose reads a topic from a provisioned or Serverless MSK cluster (private clusters via MSK multi-VPC connectivity), including cross-account | Classic pairing is **MSK → Firehose → S3**. Record ≤ **10 MB** (≤ **6 MB** if a Lambda transform is on) |
| **Databases (CDC)** | Firehose connects to MySQL/PostgreSQL, takes an initial snapshot, then streams changes into Iceberg tables | Launched in **preview** at re:Invent 2024. Not a mainstream exam answer; for exam CDC think **DMS** ([Guide 10](10-DMS-Database-Ingestion.md)) |

**THE trap:** Direct PUT and a KDS source are **mutually exclusive**. A Firehose stream whose source is a Kinesis data stream **doesn't accept PutRecord calls**. Producers write to the KDS stream and Firehose reads from it.

## Destinations: where the van unloads

- **Amazon S3**: the default and most common. Supports custom prefixes, compression (GZIP, Snappy, ZIP, Hadoop-compatible Snappy), format conversion and dynamic partitioning.
- **Amazon Redshift** (provisioned or Serverless): Firehose **writes to an intermediate S3 bucket first, then issues a `COPY`** into the table. It's micro-batch loading on your behalf.
- **Amazon OpenSearch Service** (public or **VPC** domains) and **OpenSearch Serverless** collections: index rotation options, and failed documents go to S3 ([Guide 29](29-OpenSearch-Service.md)).
- **Splunk** and **Splunk Observability Cloud**.
- **HTTP endpoint**: any HTTPS endpoint that follows the Firehose request/response contract.
- **Partners**: Datadog, Dynatrace, LogicMonitor, New Relic, Coralogix, Elastic, Honeycomb, Logz.io, MongoDB Cloud, Sumo Logic, **Snowflake**.
- **Apache Iceberg tables**: self-managed Iceberg tables in S3 (Glue Data Catalog) or **Amazon S3 Tables**. One stream can **route records to multiple tables** and apply **inserts, updates and deletes**, with Lake Formation fine-grained access supported. Compaction and snapshot expiry are yours on self-managed tables and automatic on S3 Tables ([Guide 04](04-Open-Table-Formats-S3-Tables.md)).

> 🆕 **New in exam guide v1.1:** open table formats (skill 2.1.7) and S3 Tables are now in scope. *"Stream events into Iceberg tables / S3 Tables with least effort"* → **Firehose with the Iceberg destination**.

**THE trap:** Firehose destinations are a **fixed menu**. *"Deliver to DynamoDB,"* *"to RDS,"* *"to our own consumer app,"* or *"to a second Kinesis stream"* aren't native Firehose destinations. Use KDS + Lambda, or an HTTP endpoint if the target speaks that contract.

## Buffering: the van's two triggers

Firehose delivers a batch when **either** the **buffer size** or the **buffer interval** is reached, **whichever comes first**. Values are *hints*, and Firehose may deliver early under load.

| Destination | Buffer size | Buffer interval | Defaults |
|---|---|---|---|
| **S3** | **1–128 MiB** | **0–900 s** | **5 MiB / 300 s** |
| S3 with **format conversion** or **dynamic partitioning** | **64–128 MiB** | 0–900 s | **128 MiB** |
| **HTTP endpoint** / partners | 1–64 MiB | 0–900 s | 5 MiB / 300 s |
| **Splunk** | 1–5 MB | 0–60 s | 5 MB / 60 s |
| **OpenSearch** | 1–100 MB | 0–900 s | — |
| **Redshift** | Governed by the S3 staging buffer, then `COPY` | — | — |

- **Zero buffering** (interval **0 s**, available since late 2023) gives the lowest-latency delivery for time-sensitive use cases. Expect seconds, not milliseconds, and many small objects in S3.
- **Big buffers mean fewer, larger files**, which is better for Athena/Spark performance and cheaper PUT requests ([Guide 03](03-Data-Formats-Compression.md) covers the small-files problem). **Small buffers mean fresher data but more objects.**
- *"Data must appear in S3 within 60 seconds"* → set the interval ≤ 60 s. *"Queries on the delivered files are slow because of millions of tiny objects"* → increase the buffer size and interval, or compact downstream.

## Transformations

### Lambda transform (the repackaging station)

Firehose buffers records (Lambda buffer **0.2–3 MB**, default **1 MB**; interval **0–900 s**, default **60 s**; smaller defaults for Splunk/Snowflake), invokes your function **synchronously** (payload ≤ **6 MB** each way, function runtime ≤ **5 minutes**), and expects every record back with this contract:

```json
{ "records": [
  { "recordId": "<same id as input>",
    "result": "Ok | Dropped | ProcessingFailed",
    "data": "<base64 transformed payload>",
    "metadata": { "partitionKeys": { "customer_id": "42" } } } ] }
```

- `Ok` means deliver it, `Dropped` means intentionally filter it out (not an error), `ProcessingFailed` means send it to the error location.
- Invocation failures are retried (**3 times** by default). Records that still fail land in S3 under the **`processing-failed/`** prefix with error metadata.
- Common uses: CSV/log lines → JSON, enrichment, masking PII before storage, filtering noise, extracting partition keys.
- Blueprints exist for common log formats (Apache, syslog, CloudWatch Logs).

**Built-in CloudWatch Logs decompression + message extraction:** subscription-filter data arrives gzip-compressed and wrapped in an envelope. Firehose can **decompress it and extract just the log messages** natively (billed per GB). You no longer need a Lambda function for that.

### Record format conversion (skill 1.2.6)

- Converts **JSON → Apache Parquet or Apache ORC** before writing to **S3**.
- **Needs a schema from an AWS Glue Data Catalog table** (it can live in another account or Region). Deserializer: **OpenX JSON SerDe** (default choice) or **Hive JSON SerDe** (custom timestamp formats). Serializer: Parquet or ORC SerDe, with compression such as Snappy.
- **Input must be JSON.** *CSV, syslog or other text → use a Lambda transform to JSON first, then convert.*
- Buffer size minimum **64 MiB** when enabled.

**THE trap:** *"Convert incoming CSV records to Parquet with Firehose"*. Format conversion alone **can't** read CSV. The complete answer is **Lambda transform (CSV → JSON) + record format conversion (Glue table schema) → S3**. Another trap is a missing or wrong Glue table: fields not in the schema are silently dropped from the output.

### Dynamic partitioning (skill 2.4.5)

Groups records **in flight** by keys taken from the data and writes each group to its own S3 prefix, so the lake is partitioned for Athena without a separate job:

```text
Prefix:       customer_id=!{partitionKeyFromQuery:customer_id}/year=!{partitionKeyFromQuery:year}/
Error prefix: errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}/
```

- **Keys come from inline parsing** (built-in **jq** expressions such as `.customer_id`, **JSON only**) or from a **Lambda** function returning `metadata.partitionKeys` (for non-JSON, compressed or encrypted data). You can use both.
- Namespaces: `!{partitionKeyFromQuery:key}`, `!{partitionKeyFromLambda:key}`. `!{timestamp:...}` (arrival time) works in any custom prefix. An **error output prefix** is required.
- **S3 destination only.** It **can only be enabled when the stream is created** and **can't be disabled** afterwards (keys and prefix expressions can be edited later).
- Default **500 active partitions** per stream (partitions open in the current buffer). Beyond that, records go to the error prefix, so avoid very high-cardinality keys like `order_id`.
- KPL-aggregated or multi-JSON records need **multi-record deaggregation** enabled first. Use the **new-line delimiter** option so downstream readers can split records.
- Costs extra (per GB, per 1,000 S3 objects delivered, and per jq processing hour).

**Without dynamic partitioning**, S3 prefixes are time-based only: the default `YYYY/MM/DD/HH` (UTC arrival time) or custom `!{timestamp:...}` expressions. That's not Hive-style `key=value` unless you write it that way, so crawlers or partition projection must match ([Guide 13](13-Glue-Data-Catalog-Crawlers.md)).

**THE trap:** *"An existing Firehose stream must start partitioning data by customer_id."* You **can't switch dynamic partitioning on** for an existing stream. **Create a new stream** with it enabled. And if the key is only in event time inside the payload, the default time prefix won't do: Firehose's `YYYY/MM/DD/HH` is **arrival time**. Use a jq key on the event timestamp.

## Delivery, failures and backup

| Destination | How failures behave |
|---|---|
| **S3** | Firehose retries. With a **Direct PUT** source, data is held **up to 24 hours** while the destination is unavailable, then **lost**. With a **KDS source**, it can re-read for as long as the **stream's retention** allows |
| **Redshift** | S3 staging + `COPY`. Configurable **retry duration 0–7,200 s**. Batches that still fail are listed as **manifest files in the S3 `errors/` prefix** for manual `COPY` later. Data stays in the intermediate bucket |
| **OpenSearch** | Retry duration **0–7,200 s**; failed documents go to the S3 backup prefix |
| **Splunk / HTTP / partners** | Retries up to a configurable duration, waiting for acknowledgements; failures go to the S3 backup |

- **Source record backup:** keep an untouched copy of the **raw incoming records** in S3 (all records for the S3 destination; *failed only* or *all* for OpenSearch, Splunk and HTTP-type destinations). Signal: *"retain the original untransformed data."*
- **Error logging** to CloudWatch Logs; key metrics include `DeliveryToS3.DataFreshness` (age of the oldest undelivered record), `DeliveryToS3.Success`, `DeliveryToRedshift.Success`, and `ThrottledRecords` for Direct PUT ([Guide 32](32-Monitoring-Logging-Troubleshooting.md)).
- **Semantics:** **at-least-once**. Retries can create **duplicates**, and **ordering isn't guaranteed** at the destination. Dedupe downstream (e.g., Iceberg `MERGE`, Redshift staging-table upsert) if it matters.

### Networking and IP allowlisting (skill 1.1.8)

Firehose delivers from AWS-owned infrastructure, so the destination's firewall must let it in:

- **Redshift (provisioned or Serverless)**: the cluster/workgroup must be **publicly accessible**, and its security group must **allow the Firehose CIDR block for that Region** (one /27 per Region, e.g., `52.70.63.192/27` in us-east-1), plus the S3 staging permissions. For a Redshift that must stay private, use **Redshift streaming ingestion** from KDS/MSK instead ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)).
- **Splunk in a VPC**: must be publicly reachable; allowlist the **Firehose CIDR blocks for Splunk** in that Region.
- **Public HTTP endpoints / public Snowflake**: there's **no Firehose-specific IP range**, so you'd allow the published **AWS IP ranges** for the Region. **Private Snowflake** uses **PrivateLink** (allow Firehose's VPC endpoint IDs in Snowflake network rules).
- **OpenSearch in a VPC**: Firehose creates **ENIs in your subnets** (VPC delivery, billed per AZ-hour + per GB). Leave enough free IPs, and allow the ENIs' security group into the domain. **HTTP endpoints inside a VPC aren't supported.**
- **Producers in private subnets**: create an **interface VPC endpoint** (`com.amazonaws.<region>.kinesis-firehose`) so `PutRecord` never touches the internet.

**THE trap:** *"Firehose can't load into a Redshift cluster in a private subnet; COPY never starts."* The answer is **make it publicly accessible and allowlist the Region's Firehose CIDR in the security group** (or rethink with streaming ingestion). A NAT gateway or an S3 gateway endpoint doesn't help. Firehose is connecting **in**, not your cluster connecting out.

## Security

- **IAM role**: Firehose assumes a **service role** you provide (trust principal `firehose.amazonaws.com`) with permissions for the source (KDS/MSK read), destination (S3 put, etc.), Glue (format conversion), Lambda (invoke), KMS and CloudWatch Logs ([Guide 37](37-IAM-for-Data-Engineers.md)). **Cross-account delivery** to S3 works with the role plus a bucket policy.
- **Encryption at rest in Firehose**: for **Direct PUT**, enable SSE with a KMS key (AWS owned or customer managed). With a **KDS source**, Firehose relies on the **stream's** SSE. At the destination, S3 objects get SSE-S3 or SSE-KMS per your configuration ([Guide 39](39-Encryption-Key-Management.md)).
- In transit: TLS to Firehose endpoints and to destinations.

## Pricing (high level)

No per-stream or hourly fee for the base service. You pay for **ingestion per GB**, and for Direct PUT and KDS sources **each record is rounded up to 5 KB**, so millions of tiny records cost more than the same bytes in bigger records (MSK sources aren't rounded). Optional features add charges: **format conversion** (per GB), **dynamic partitioning** (per GB + per 1,000 objects + jq hours), **VPC delivery** (per GB + per AZ-hour), **Iceberg** and **Snowflake** delivery (per GB), **CloudWatch Logs decompression** (per GB). *"Reduce Firehose cost for many tiny records"* → **batch or aggregate records on the producer side before sending.**

## Firehose vs. Kinesis Data Streams: the boundary line

| Need | Firehose | Kinesis Data Streams |
|---|---|---|
| Land data in S3/Redshift/OpenSearch/Splunk, no code | **Yes** | Needs a consumer (or Firehose) |
| Custom consumer applications, several of them | No | **Yes** |
| **Replay** / reprocess history | No (the 24 h buffer is only for retries) | **Yes, up to 365 days** |
| **Ordering** per key | Not guaranteed | **Per shard/partition key** |
| **Sub-second** latency | No (buffered; seconds at best) | **Yes** |
| Capacity management | None | Shards or on-demand |
| Transform en route | Lambda, Parquet/ORC, dynamic partitioning | Via consumers |

**THE trap:** *"Near real-time"* plus *"least operational overhead"* plus a Firehose destination means **Firehose**. But add any one of *"replay," "multiple consuming applications," "ordered per device," "sub-second,"* or *"custom processing before storage by several teams"*, and the answer becomes **KDS** (often **KDS → Firehose** for the S3 leg).

> ⚠️ **2026 status:** since **August 2026**, Kinesis Data Streams can itself deliver to **general purpose S3 buckets** and to **Iceberg streaming tables on S3 Tables** (on-demand streams only, 5–15 min freshness, no Lambda transforms). The v1.1 exam predates this. Keep answering **Firehose** for "stream to S3 with least effort", and remember that only Firehose offers Lambda transforms, JSON→Parquet with a Glue schema, dynamic partitioning, and non-S3 destinations.

## Question patterns

> *"A company streams application logs and needs them in S3 for Athena queries, delivered within 5 minutes, with the LEAST operational overhead."* → **Amazon Data Firehose (Direct PUT or a CloudWatch Logs subscription) → S3** (fully managed, auto-scaling, buffers to fewer files. KDS + custom consumer adds code; Glue streaming is heavier.)

> *"JSON clickstream arrives via Firehose. Analysts need Snappy-compressed Parquet in S3 to cut Athena scan costs."* → **Enable record format conversion to Parquet using a Glue Data Catalog table schema** (built-in conversion. No EMR or Glue job needed.)

> *"Records arrive as CSV lines. They must be stored as Parquet with minimal custom infrastructure."* → **Firehose Lambda transform (CSV → JSON) + record format conversion to Parquet** (conversion only accepts JSON input.)

> *"Data must land in S3 partitioned as tenant_id=/date= so each tenant's queries scan only its data, without a post-processing job."* → **Firehose dynamic partitioning with jq inline parsing on tenant_id and the event date** (it must be set at stream creation. Arrival-time prefixes alone don't partition by tenant.)

> *"Stream IoT events into Amazon Redshift with the LEAST effort. The cluster is in a VPC."* → **Firehose → Redshift (S3 staging + COPY); make the cluster publicly accessible and allow the Region's Firehose CIDR in its security group** (skill 1.1.8. If it can't be public, Redshift streaming ingestion from KDS is the alternative.)

> *"Firehose delivery to Redshift fails intermittently when the cluster is paused for maintenance. The team needs to recover the batches that failed."* → **Load the manifest files from the S3 `errors/` prefix with COPY** (after the retry duration of up to 7,200 s, Firehose writes manifests. The data is still in the intermediate bucket.)

> *"A Lambda transform sometimes throws exceptions; ops need to find and reprocess the affected records."* → **Look in the S3 `processing-failed/` prefix and in CloudWatch Logs error logging** (failed records are written there with error metadata after Firehose's retries.)

> *"The existing Firehose stream must begin writing to per-customer prefixes."* → **Create a new Firehose stream with dynamic partitioning enabled and switch producers** (it can't be enabled on an existing stream.)

> *"Security requires a copy of the raw, untransformed events in addition to the enriched output."* → **Enable source record backup to a separate S3 bucket/prefix** (the transform output goes to the destination; the backup keeps originals.)

> *"Stream data into Apache Iceberg tables in S3 Tables, routing orders and refunds to different tables and applying deletes, with no clusters to manage."* → **Firehose with the Apache Iceberg Tables destination (S3 Tables)** (multi-table routing plus insert/update/delete. Glue streaming or Flink would work but add operational overhead.)

> *"The same click events must feed a fraud-detection app in under a second and also be archived to S3."* → **KDS with a real-time consumer (Lambda/Flink) + Firehose reading the stream for the S3 archive** (Firehose alone can't serve sub-second custom consumers.)

> *"CloudWatch Logs from 20 accounts must be centralized in one S3 bucket as plain log lines, with minimal custom code."* → **Cross-account CloudWatch Logs subscription filters → Firehose (with built-in decompression and message extraction) → S3** (no Lambda needed to un-gzip the envelope.)

> *"An on-premises Kafka migration to MSK is done. The team now needs topic data in S3 with no connector code to maintain."* → **Firehose with an Amazon MSK source → S3** (managed, no MSK Connect sink to operate.)

> *"Athena queries over Firehose-delivered data are slow; S3 holds millions of 1 MB objects."* → **Increase Firehose buffer size and interval (up to 128 MiB / 900 s), and use Parquet** (bigger, columnar files. Or compact downstream.)

> *"A Firehose stream uses Direct PUT to S3. The S3 bucket policy was broken for 30 hours. What happened to the data?"* → **Records older than 24 hours were lost** (Direct PUT buffers only 24 h. A KDS source would have allowed re-reading within the stream's retention.)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Stream → S3/Redshift/OpenSearch/Splunk, no code, least overhead | **Firehose** |
| Old name in questions | Kinesis Data Firehose (renamed Feb 2024) |
| Latency class | **Near real-time** (buffered; zero buffering = seconds) |
| Buffer trigger | **Size OR interval, whichever first** |
| S3 buffer | **1–128 MiB, 0–900 s; default 5 MiB / 300 s** |
| Format conversion / dynamic partitioning buffer | **≥ 64 MiB** (default 128) |
| JSON → Parquet/ORC | Record format conversion + **Glue Data Catalog table** |
| CSV → Parquet | **Lambda (CSV→JSON) + format conversion** |
| Lambda transform contract | `recordId` + `result` (**Ok / Dropped / ProcessingFailed**) + base64 `data` |
| Lambda transform limits | 6 MB payload, ≤ 5 min, 3 retries default |
| Transform failures | S3 **`processing-failed/`** prefix |
| Partition by key in S3 | **Dynamic partitioning** (jq for JSON, Lambda otherwise) |
| Enable dynamic partitioning later? | **No**, creation-time only, can't disable |
| Prefix syntax | `!{partitionKeyFromQuery:k}`, `!{partitionKeyFromLambda:k}`, `!{timestamp:yyyy}` |
| Active partitions | **500** default per stream |
| Keep raw originals | **Source record backup** to S3 |
| Redshift path | **S3 staging → COPY**; failures → `errors/` manifests |
| Redshift/Splunk network | **Publicly accessible + allowlist Firehose Region CIDR** |
| OpenSearch in VPC | Firehose ENIs in your subnets (VPC delivery) |
| Private producers | Interface VPC endpoint `kinesis-firehose` |
| Destination down, Direct PUT | Data kept **24 h** |
| Destination down, KDS source | Re-read within **stream retention** |
| Direct PUT record / batch | **1,000 KiB**; **500 records or 4 MiB** |
| KPL-aggregated input | Firehose de-aggregates (KDS source) |
| Iceberg / S3 Tables sink | **Firehose Iceberg destination** (multi-table, upserts/deletes) |
| MSK topic → S3, no code | **Firehose MSK source** |
| CloudWatch Logs gzip envelope | Built-in **decompression + message extraction** |
| Many tiny records cost | 5 KB rounding, so batch/aggregate |
| Replay, multiple consumers, ordering, sub-second | **Not Firehose → KDS** |
| Delivery semantics | At-least-once; duplicates possible |

If "Kafka" appears in the question, the delivery van still works, but the highway is different: [Guide 08 — Amazon MSK and Apache Kafka](08-Amazon-MSK-Kafka.md).
