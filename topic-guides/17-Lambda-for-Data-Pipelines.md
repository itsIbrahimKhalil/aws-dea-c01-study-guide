# 17 · AWS Lambda for Data Pipelines — the pop-up food stall of compute

> **Exam map:** D1 · Task 1.1, 1.3, 1.4 — D3 · Task 3.1 · **Skills:** 1.1.7, 1.3.3, 1.4.2, 1.4.6, 1.4.7, 3.1.8 · **Weight:** 🔥🔥🔥 High · **Read time:** ~22 min

## The idea

Picture a **pop-up food stall** that appears the instant a customer walks up, cooks one order, and folds away when the street is empty. You never rent a kitchen, never pay for idle staff, and on a festival night hundreds of identical stalls pop up side by side. That is **AWS Lambda**: you hand AWS a function, AWS runs a copy in an isolated **execution environment** each time an **event** arrives, and you pay per request plus per millisecond of runtime at the memory size you chose.

Stalls have rules, and the exam is built on them. Each order must be finished in **15 minutes**. Each stall has a small counter (**/tmp** scratch space) that vanishes when the stall folds, unless you plug it into a shared warehouse (**Amazon EFS**). The street has a **maximum number of stalls** (concurrency); you can **reserve** spaces for your favorite vendor, or pay to keep some stalls **pre-heated** (provisioned concurrency) so the first customer doesn't wait while the grill warms up (a **cold start**). And because orders sometimes get delivered twice, a good cook checks the ticket number before cooking again (**idempotency**).

In data pipelines Lambda is the **glue and the light-duty worker**: reacting to S3 uploads, consuming Kinesis/SQS batches, pulling APIs on a schedule, transforming Firehose records, and kicking off Glue, Athena, Redshift or Step Functions. It is **not** the engine for big or long ETL. This guide covers the limits, concurrency, event sources, error handling, storage, networking and **AWS SAM** packaging you need to crack Lambda questions.

## The limits that decide answers

| Limit | Value |
|---|---|
| **Timeout** | Up to **900 s (15 min)** per invocation |
| **Memory** | **128 MB – 10,240 MB** (1 MB steps); **CPU scales with memory** (~1 vCPU at 1,769 MB, up to 6 vCPUs at max) |
| **/tmp ephemeral storage** | **512 MB (default) – 10,240 MB** |
| **Deployment package (.zip)** | **50 MB** zipped direct upload; **250 MB** unzipped **including layers** (larger zips go via S3, but the 250 MB cap still applies) |
| **Container image** | Up to **10 GB** |
| **Layers** | **5** per function |
| **Payload** | **6 MB** request/response for synchronous invokes; **1 MB** for asynchronous invokes (raised from 256 KB) |
| **Environment variables** | **4 KB** total |
| **Concurrency** | **1,000** concurrent executions per Region by default (soft; raise to tens of thousands) |
| **Scaling rate** | Each function can add **1,000 execution environments every 10 seconds** |

Older questions may still say 256 KB for async payloads; the logic is unchanged — big payloads go to **S3, and you pass a pointer**.

**THE trap:** *"A nightly job transforms 200 GB of CSV to Parquet; it sometimes runs 40 minutes."* Lambda can't run past 15 minutes and has limited memory and disk. **Big or long ETL → AWS Glue, EMR (Serverless), or AWS Batch** ([Guide 18](18-Containers-Batch-EC2-Compute.md), [Guide 12](12-AWS-Glue-ETL.md)). Lambda's sweet spot is **small, event-driven, short** work. If a workflow is long but made of short steps, orchestrate the steps with **Step Functions** ([Guide 20](20-Step-Functions.md)).

> ⚠️ **2026 status:** two re:Invent 2025 features stretch the model — **Lambda durable functions** (checkpoint-and-replay workflows that can run up to **one year**, with free waits) and **Lambda Managed Instances** (functions on Lambda-managed EC2 capacity; async/ESM invocations up to 90 minutes). They are newer than the exam guide. For DEA-C01 answers, keep the 15-minute rule and pick Glue/EMR/Batch for heavy ETL and Step Functions for orchestration.

## Concurrency and cold starts (1.4.2)

**Concurrency** = the number of requests being processed at the same moment ≈ requests per second × average duration in seconds.

| Control | What it does | Use it to |
|---|---|---|
| **Reserved concurrency** | Sets aside N from the Regional pool **and caps** the function at N. Free. Setting it to **0** stops all invocations | **Guarantee** capacity for a critical function, or **throttle** a function so it can't overwhelm a downstream database/API (e.g., cap at 20 to protect RDS) |
| **Provisioned concurrency** | Keeps N execution environments **initialized** on a version/alias; costs money while configured; can scale on a schedule/target via Application Auto Scaling | Remove **cold starts** for latency-sensitive paths (APIs, strict SLAs) |
| **SnapStart** | Snapshots the initialized environment when you publish a version and resumes from it | Cut cold starts cheaply for **Java 11+, Python 3.12+, .NET 8+**. Not with provisioned concurrency, EFS, or /tmp > 512 MB |
| **SQS maximum concurrency** | ESM-level cap (**2–1,000**) on concurrent invocations one queue can drive | Limit a queue consumer without the throttling side effects of reserved concurrency |

**What happens when throttled** (429 `TooManyRequestsException`):

| Invocation type | Throttle behavior |
|---|---|
| **Synchronous** (API Gateway, SDK `RequestResponse`) | Error returned to the caller immediately — **caller must retry** |
| **Asynchronous** (S3, SNS, EventBridge) | Lambda keeps the event in its internal queue and **retries for up to 6 hours** (max event age) |
| **Event source mapping — SQS** | Messages stay in the queue and reappear after the **visibility timeout** |
| **Event source mapping — Kinesis / DynamoDB Streams** | Lambda retries the batch; the shard waits, **IteratorAge climbs** |

**THE trap:** raising memory to fix a **throttling** problem. Throttles are a **concurrency** issue (Regional limit, reserved concurrency set too low). Memory fixes **Duration** and timeouts.

**THE trap:** using provisioned concurrency to *limit* a function. It pre-warms, it doesn't cap — the cap is **reserved concurrency** (or SQS maximum concurrency).

## How events reach Lambda — three invocation models

| Model | Sources | Who retries | Error handling |
|---|---|---|---|
| **Synchronous** | API Gateway, ALB, SDK/CLI `RequestResponse`, Step Functions Task (sync) | The **caller** | Caller sees the error |
| **Asynchronous** | **S3 Event Notifications, SNS, EventBridge**, CloudWatch Logs subscriptions | **Lambda**: **2 retries** by default (configurable 0–2), with backoff | **Maximum event age** 60 s–6 h; failed events to an **on-failure destination** or DLQ |
| **Event source mapping (ESM, polling)** | **SQS, Kinesis Data Streams, DynamoDB Streams, MSK / self-managed Kafka, Amazon MQ, DocumentDB change streams** | Lambda's pollers | Source-specific (below) |

**Async destinations vs DLQs:** a **dead-letter queue** (SQS or SNS) receives only the failed event payload. **Destinations** are the newer, richer option — **on success and on failure**, to **SQS, SNS, EventBridge, another Lambda function**, or **S3** (failure only) — and include the invocation record (request, response, error). *"Capture failed events with the error details"* or *"route successful results to the next step"* → **destinations**.

### SQS event source mapping

- **Batch size** up to **10,000** (standard queues; FIFO max **10**) with a **batching window** up to **300 s** (batches over 10 need a window of at least 1 s).
- Messages up to **1 MiB** (SQS raised its limit from 256 KiB in Aug 2025).
- **Partial batch failures:** enable **`ReportBatchItemFailures`** and return the IDs of failed messages, so only those return to the queue — otherwise one bad message makes the **whole batch** reprocess.
- **Visibility timeout ≥ 6 × the function timeout** (plus the batching window) so in-flight messages don't reappear and get processed twice.
- **Poison messages:** configure a **redrive policy (DLQ) on the source queue** with `maxReceiveCount` — for SQS, the DLQ lives on the queue, not in Lambda's async settings.
- Scaling: default ESM scales pollers up automatically (up to **1,250** concurrent invokes by default); **provisioned mode** (dedicated pollers, min/max) scales faster for spiky queues. Maximum concurrency and provisioned mode are mutually exclusive.
- FIFO queues: order preserved per **message group ID**; concurrency ≤ number of active message groups.

### Kinesis and DynamoDB Streams (1.1.7) — summary

Lambda reads each shard in order via an ESM: **batch size** up to 10,000, **batching window** up to 300 s, **parallelization factor 1–10** (concurrent batches per shard, order kept per partition key), **bisect batch on error**, **maximum retry attempts**, **maximum record age**, **on-failure destination (SQS, SNS or S3 — S3 receives the full batch)**, tumbling windows for simple aggregations, and **enhanced fan-out** consumers for dedicated throughput. The health metric is **IteratorAge** — rising means Lambda is falling behind (errors blocking the shard, or not enough parallelism). A single poison record blocks its shard until it succeeds or expires, which is why bisect + retry limits + on-failure destinations matter. Full depth: [Guide 06 — Kinesis Data Streams](06-Kinesis-Data-Streams.md) and [Guide 27 — DynamoDB](27-DynamoDB.md).

### Other sources worth knowing

- **MSK / self-managed Kafka:** ESM with consumer groups, batch settings and on-failure destinations; **provisioned mode** for throughput; metric **OffsetLag** ([Guide 08](08-Amazon-MSK-Kafka.md)).
- **DocumentDB change streams:** ESM that delivers change events for near-real-time sync.
- **S3:** async; filter by **prefix/suffix**; for richer filtering or multiple targets route through **EventBridge** ([Guide 22](22-EventBridge-SNS-SQS.md)).
- **API Gateway:** synchronous; **29 s** default integration timeout, so long work must go async (queue it, return a job ID).

**THE trap:** the **recursive S3 loop** — a function triggered by `ObjectCreated` on a bucket writes its output back to the **same bucket/prefix**, triggering itself forever. Write to a **different bucket** or a prefix excluded by the trigger's filter. Lambda's recursive-loop detection can stop some runaway loops, but design it out.

## Idempotency and at-least-once delivery

Async invokes, SQS and streams are **at-least-once**: retries, visibility timeouts and batch reprocessing mean the **same event can arrive twice**. Make handlers **idempotent**:
- Use natural keys and **upserts** (DynamoDB `PutItem` keyed on event ID, conditional writes `attribute_not_exists`), or MERGE into tables.
- Write outputs with **deterministic S3 keys** (re-running overwrites instead of duplicating).
- **Powertools for AWS Lambda** (Python, TypeScript, Java, .NET) provides an **idempotency** utility (DynamoDB-backed record of processed payloads), plus structured **logging**, custom **metrics** (EMF) and **tracing** — the *"best practice with least custom code"* answer for these concerns.

Concepts of delivery semantics: [Guide 02 — Fundamentals](02-Data-Engineering-Fundamentals.md).

## Packaging: layers and container images

- **Layers** share libraries across functions (max 5 per function, all within the 250 MB unzipped limit). AWS publishes a managed **AWS SDK for pandas** (awswrangler) layer — pandas, PyArrow and helpers to read/write S3, Athena, Glue Catalog and Redshift — perfect for small Parquet conversions.
- **Container images** (up to **10 GB**, stored in **Amazon ECR**) when dependencies blow past 250 MB (big ML libraries, native binaries) or you want one Docker-based build pipeline.

*"Deployment package exceeds 250 MB"* → **container image**, not "more layers" (layers count toward the same 250 MB).

## Storage inside Lambda (1.4.7)

| Option | Scope and lifetime | Size | Pick when |
|---|---|---|---|
| **/tmp (ephemeral storage)** | Private to **one execution environment**; survives **warm** reuse of that environment, gone when it's recycled | **512 MB–10,240 MB** | Scratch space: unzip an archive, stage a file for conversion, cache a lookup file between warm invokes |
| **Amazon EFS mount** | **Shared** by all concurrent environments and **many functions**; **persistent** | Elastic | Large reference data, ML models or libraries loaded by many invocations; files that must outlive one invocation or be shared. Requires the function **in a VPC** (same VPC as mount targets) and an **EFS access point**; mount path under `/mnt/` |
| **Amazon S3 via SDK** | Durable object storage, any consumer | Unlimited | Inputs and outputs of the pipeline — the default answer for data in/out |

**THE trap:** treating /tmp as shared or durable. Two concurrent invocations never see each other's /tmp, and the next cold start starts empty. Shared + persistent = **EFS**; pipeline data = **S3**.

**THE trap:** *"Lambda must read a 3 GB model file at every invocation."* Packaging it (250 MB limit) fails; downloading from S3 each time is slow. **Mount EFS** (or use a container image) — and cache in /tmp for warm invokes if it fits.

## Networking: VPC access and database connections

- By default Lambda runs outside your VPC with internet access. Attach it to a **VPC** to reach private resources (RDS, Redshift, ElastiCache, EFS). Lambda uses shared **Hyperplane ENIs** created when you configure the function (no per-invocation ENI delay).
- A VPC-attached function has **no internet or public AWS endpoint access** by itself: add a **NAT gateway** (for internet APIs; its **Elastic IP** is what partners allowlist — [Guide 11](11-DataSync-Transfer-Family-Snow-AppFlow.md)) or **VPC endpoints** (gateway endpoints for **S3/DynamoDB**, interface endpoints for Secrets Manager, SQS, Kinesis…). Networking depth: [Guide 38](38-Networking-for-Data-Pipelines.md).
- **RDS Proxy** pools database connections so a burst of concurrent functions doesn't exhaust `max_connections`; supports IAM auth and Secrets Manager.
- Alternatives that avoid connections entirely: the **Redshift Data API** and **RDS Data API** (HTTPS calls, async results).

**THE trap:** *"A Lambda function in a private subnet times out calling S3."* No route to S3 → add an **S3 gateway endpoint** (or NAT). It's not a memory or IAM problem if the call hangs rather than returning AccessDenied.

## Data-processing patterns and anti-patterns (3.1.8)

**Good fits:**
- **S3 → Lambda** per-object work on small files: validate, unzip, convert a small CSV to Parquet (AWS SDK for pandas), extract metadata, write to the curated prefix.
- **Kinesis / DynamoDB Streams → Lambda → DynamoDB/S3/OpenSearch**: lightweight enrichment, filtering, fan-out ([Guide 06](06-Kinesis-Data-Streams.md)).
- **SQS → Lambda**: buffered, rate-controlled workers (cap with maximum concurrency).
- **Scheduled API pulls**: EventBridge Scheduler → Lambda → REST API → S3 raw ([Guide 11](11-DataSync-Transfer-Family-Snow-AppFlow.md)).
- **Firehose data transformation**: Firehose invokes Lambda on buffered batches to reshape records before delivery ([Guide 07](07-Amazon-Data-Firehose.md)).
- **Orchestration glue**: start a Glue job or crawler, run an Athena query, call the **Redshift Data API**, publish SNS alerts, or start a Step Functions execution — while the heavy work runs elsewhere.
- **Athena federated query connectors** run as Lambda functions to query DynamoDB, RDS, CloudWatch Logs and more in place ([Guide 26](26-Amazon-Athena.md)).

**Anti-patterns:** long or large-volume ETL (→ Glue/EMR/Batch); joins across big datasets (→ Spark/Redshift/Athena); holding state between invocations in memory (→ DynamoDB/S3); polling loops that wait inside the function (→ Step Functions wait states or `.sync` integrations); one Lambda per row on millions of rows (→ batch it).

## AWS SAM — package and deploy serverless pipelines (1.4.6)

The **AWS Serverless Application Model (SAM)** is a CloudFormation extension: a template with **`Transform: AWS::Serverless-2016-10-31`** uses short serverless resource types that CloudFormation expands into full resources (functions, roles, permissions, event mappings) at deploy time. It pairs with the **SAM CLI** for build, local testing and deployment.

| SAM resource | Expands to / used for |
|---|---|
| `AWS::Serverless::Function` | Lambda function + execution role + triggers from its **`Events`** property (S3, SQS, Kinesis, DynamoDB, Schedule/ScheduleV2, Api, EventBridgeRule…) |
| `AWS::Serverless::StateMachine` | Step Functions state machine (definition file + substitutions, policies, events) |
| `AWS::Serverless::SimpleTable` | DynamoDB table with a single primary key |
| `AWS::Serverless::Api` / `HttpApi` | API Gateway REST / HTTP APIs |
| `AWS::Serverless::LayerVersion` | Lambda layer |
| `AWS::Serverless::Connector` | Least-privilege permissions between two resources |

**Policy templates** grant scoped IAM permissions in one line: `S3ReadPolicy`, `S3CrudPolicy`, `DynamoDBCrudPolicy`, `DynamoDBReadPolicy`, `SQSPollerPolicy`, `LambdaInvokePolicy`, `StepFunctionsExecutionPolicy`, and more. A **`Globals`** section sets shared defaults (runtime, memory, timeout, architecture).

A mini pipeline — JSON lands in S3, a function writes orders to DynamoDB, and a nightly state machine runs a follow-up step:

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Globals:
  Function:
    Runtime: python3.13
    Architectures: [arm64]
    MemorySize: 512
    Timeout: 60
Resources:
  RawBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-raw-${AWS::AccountId}"
  OrdersTable:
    Type: AWS::Serverless::SimpleTable
    Properties:
      PrimaryKey: {Name: order_id, Type: String}
  IngestFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: src/ingest/
      Handler: app.handler
      Environment:
        Variables: {TABLE_NAME: !Ref OrdersTable}
      Policies:
        - S3ReadPolicy: {BucketName: !Sub "${AWS::StackName}-raw-${AWS::AccountId}"}
        - DynamoDBCrudPolicy: {TableName: !Ref OrdersTable}
      Events:
        NewFile:
          Type: S3
          Properties:
            Bucket: !Ref RawBucket
            Events: s3:ObjectCreated:*
            Filter: {S3Key: {Rules: [{Name: suffix, Value: .json}]}}
  NightlyWorkflow:
    Type: AWS::Serverless::StateMachine
    Properties:
      DefinitionUri: statemachine/nightly.asl.json
      DefinitionSubstitutions: {IngestFunctionArn: !GetAtt IngestFunction.Arn}
      Policies:
        - LambdaInvokePolicy: {FunctionName: !Ref IngestFunction}
      Events:
        Nightly:
          Type: ScheduleV2
          Properties: {ScheduleExpression: "cron(0 2 * * ? *)"}
```

(The policy repeats the bucket-name expression instead of `!Ref RawBucket` to avoid a circular dependency between the bucket's notification and the function's role.)

**SAM CLI workflow:**

| Command | Does |
|---|---|
| `sam init` | Scaffold a project from a template |
| `sam validate` | Check the template |
| `sam build` | Resolve dependencies and build artifacts into `.aws-sam/` (optionally in a container) |
| `sam local invoke` / `sam local start-api` / `sam local generate-event` | Run functions and APIs **locally in Docker** with sample events (S3, SQS, Kinesis…) |
| `sam package` | Upload artifacts to S3 and output a packaged template (`sam deploy` now does this for you) |
| `sam deploy --guided` | Interactive first deploy; saves answers to **`samconfig.toml`**; deploys via a **CloudFormation change set** |
| `sam sync --watch` | Fast dev-loop sync of code/resources to a **development** stack (not for production) |
| `sam logs`, `sam delete`, `sam pipeline init` | Tail logs, tear down, generate CI/CD pipeline config |

Signals: *"package and deploy Lambda functions, a state machine and a DynamoDB table as one unit," "test the function locally before deploying," "least effort IaC for serverless."* SAM vs CDK vs raw CloudFormation, and CI/CD, are in [Guide 36 — Programming, IaC & CI/CD](36-Programming-IaC-CICD.md).

**THE trap:** thinking SAM is a separate deployment engine. SAM templates **are CloudFormation** (after the transform) — rollbacks, change sets, stacks and drift detection all apply.

## Monitoring and cost

**CloudWatch metrics:** `Invocations`, **`Duration`**, **`Errors`**, **`Throttles`**, **`ConcurrentExecutions`**, **`IteratorAge`** (streams), `OffsetLag` (Kafka), `AsyncEventAge` and `DestinationDeliveryFailures` / `DeadLetterErrors` (async). Logs go to **CloudWatch Logs** (set retention; JSON structured logging). **Lambda Insights** (an extension layer) adds system-level metrics — memory, CPU, network, cold starts — for right-sizing. Troubleshooting playbooks: [Guide 32](32-Monitoring-Logging-Troubleshooting.md).

**Cost** = requests + **GB-seconds** (memory × duration, billed per 1 ms):
- **arm64 (Graviton)** — cheaper per GB-second and often faster: the easy win for Python/Node/Java data code.
- **Right-size memory** — more memory means more CPU, so a CPU-bound transform can finish faster and **cost the same or less**; use **AWS Lambda Power Tuning** or **Compute Optimizer** to find the sweet spot.
- **Batch** events (SQS/Kinesis batch size and window) to amortize per-invocation overhead.
- Provisioned concurrency and SnapStart caching cost money — use them only for latency requirements.

## Question patterns

> *"A Lambda function consuming a Kinesis stream falls behind during peaks; IteratorAge keeps rising, but there are no errors."* → **Increase the ESM parallelization factor (and/or batch size), or add shards** (more concurrent batches per shard; memory alone won't add parallelism).

> *"One malformed record causes a Kinesis-triggered function to retry the same batch for hours, blocking the shard."* → **Enable bisect-batch-on-error, set maximum retry attempts/record age, and add an on-failure destination** (isolates and parks the poison record).

> *"An SQS-triggered function processes batches of 100; when one message fails, all 100 are reprocessed and duplicates appear downstream."* → **Enable ReportBatchItemFailures and make the handler idempotent** (only failed IDs return to the queue).

> *"SQS messages are processed twice even though the function succeeds; the function runs ~4 minutes."* → **Raise the queue's visibility timeout to at least six times the function timeout** (messages reappear while still in flight).

> *"A Lambda function writes to an RDS for PostgreSQL database. Traffic spikes create thousands of concurrent invocations and the database runs out of connections."* → **RDS Proxy, and cap the function with reserved concurrency** (pooling + a ceiling that protects the database).

> *"A function that calls a partner API limited to 50 concurrent requests is overwhelming the API."* → **Set reserved concurrency (or SQS maximum concurrency) to 50** (the cap; provisioned concurrency doesn't limit).

> *"A latency-sensitive API on Lambda (Java) has unacceptable cold starts at the first requests each morning, at the LOWEST cost."* → **SnapStart** (provisioned concurrency also works but costs more continuously; SnapStart suits Java/Python/.NET).

> *"Failed S3-triggered invocations must be captured with the error message and original event for later reprocessing."* → **Asynchronous on-failure destination (SQS, SNS, EventBridge or S3)** (destinations include error context; a DLQ holds only the payload).

> *"A daily job converts a 150 GB CSV dataset in S3 to partitioned Parquet; runtime is about 45 minutes."* → **AWS Glue Spark job (or EMR Serverless)** (over Lambda's 15-minute limit and memory/disk ceiling).

> *"Many Lambda functions must load the same 4 GB reference dataset quickly at each invocation."* → **Mount an Amazon EFS file system through an access point (function in the VPC)** (shared, persistent, beyond package and /tmp practicality).

> *"A function temporarily downloads a 2 GB archive, extracts it and uploads the contents to S3."* → **Increase /tmp ephemeral storage (up to 10,240 MB)** (per-invocation scratch; EFS is overkill).

> *"A Lambda function in a private subnet must read objects from S3 but times out; there is no NAT gateway."* → **Add an S3 gateway VPC endpoint** (private route to S3, no NAT charges).

> *"Deploy a serverless pipeline — two Lambda functions, a DynamoDB table and a Step Functions state machine — as one versioned unit, and test the functions locally first."* → **AWS SAM template; `sam build`, `sam local invoke`, `sam deploy --guided`** (serverless shorthand over CloudFormation, local Docker testing).

> *"The dependencies of a Python transformation function (with native libraries) total 1.2 GB."* → **Package as a container image in ECR** (250 MB unzipped cap applies to zip + layers; images go to 10 GB).

> *"An S3-triggered function writes processed output to the same bucket, and invocations and costs explode."* → **Write to a different bucket or a prefix excluded by the trigger's filter** (recursive trigger loop).

> *"Reduce the cost of a CPU-bound Python function without changing code, while keeping performance."* → **Switch to arm64 (Graviton) and right-size memory with Lambda Power Tuning** (better price-performance; more memory can shorten duration).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Max runtime | 15 min → heavier/longer ETL = Glue / EMR / Batch |
| Memory / CPU | 128 MB–10,240 MB; CPU scales with memory |
| /tmp | 512 MB–10,240 MB, per environment, not shared |
| Zip / unzipped / image | 50 MB / 250 MB incl. layers / 10 GB |
| Payload sync / async | 6 MB / 1 MB (older material: 256 KB) |
| Default Regional concurrency | 1,000 (soft) |
| Scaling rate | +1,000 environments per function every 10 s |
| Guarantee or cap concurrency | Reserved concurrency (0 = off) |
| Eliminate cold starts | Provisioned concurrency |
| Cheaper cold-start fix (Java/Python/.NET) | SnapStart |
| Throttles metric rising | Concurrency problem, not memory |
| Sync throttle | 429 to caller; caller retries |
| Async retries | 2 by default; max event age up to 6 h |
| Failed async events with context | On-failure destination (SQS/SNS/EventBridge/Lambda/S3) |
| Success routing | On-success destination |
| SQS partial failures | ReportBatchItemFailures |
| SQS duplicates while running | Visibility timeout ≥ 6× function timeout |
| SQS poison messages | Redrive policy / DLQ on the queue |
| Cap one queue's consumers | SQS ESM maximum concurrency (2–1,000) |
| Stream consumer lagging | IteratorAge → parallelization factor / shards |
| Poison record blocks shard | Bisect batch + max retries/age + on-failure destination |
| Duplicate deliveries | Idempotent handler (Powertools idempotency, upserts) |
| Shared, persistent files / big models | EFS (VPC + access point) |
| Pipeline input/output | S3 via SDK |
| pandas/PyArrow quickly | AWS SDK for pandas managed layer |
| Dependencies > 250 MB | Container image (ECR) |
| Private subnet → S3 | S3 gateway endpoint (or NAT) |
| Private subnet → internet API | NAT gateway (EIP for allowlisting) |
| Connection storms on RDS | RDS Proxy (+ reserved concurrency) |
| Query Redshift without connections | Redshift Data API |
| Self-triggering S3 loop | Separate bucket/prefix |
| Long multi-step workflow | Step Functions orchestrating short functions |
| Serverless IaC shorthand | SAM (Transform: AWS::Serverless-2016-10-31) |
| One-line scoped permissions | SAM policy templates (S3ReadPolicy, DynamoDBCrudPolicy) |
| Test locally | sam local invoke / start-api (Docker) |
| First deploy, save config | sam deploy --guided → samconfig.toml |
| Fast dev iteration | sam sync --watch (dev only) |
| System-level metrics | Lambda Insights |
| Cheaper per GB-second | arm64 / Graviton + power tuning |

When the job outgrows the food stall — longer than 15 minutes, bigger than 10 GB of memory, or needing a custom container — move up the compute ladder in [Guide 18 — Containers, Batch & EC2 Compute](18-Containers-Batch-EC2-Compute.md).
