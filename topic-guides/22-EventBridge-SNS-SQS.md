# 22 · EventBridge, SNS & SQS — the switchboard, the megaphone, and the waiting room

> **Exam map:** D1 · Task 1.1, 1.3 — D3 · Task 3.1, 3.3 · **Skills:** 1.1.6, 1.1.9, 1.1.10, 1.3.4, 3.1.9, 3.3.3 · **Weight:** 🔥🔥 Medium · **Read time:** ~20 min

## The idea

A modern data pipeline is **reactive**. A file lands, a job finishes, a query fails, and something should happen next. Three services carry those signals, and one analogy covers all three. Picture a **busy hospital**:

- **Amazon EventBridge** is the **switchboard operator**. Every department reports what just happened ("patient admitted", "lab result ready"). The operator checks a rulebook and connects each report to the right people. It doesn't store the calls (unless you ask it to archive them) and it doesn't do the work. It **routes**.
- **Amazon SNS (Simple Notification Service)** is the **PA megaphone**. It sends one announcement to every subscriber at once: pagers, email, queues, functions. It doesn't keep announcements (standard topics don't persist).
- **Amazon SQS (Simple Queue Service)** is the **waiting room**. Work waits in line, however long the line gets, until a doctor is free to take the next patient. Each patient goes to **one** doctor.

Two vocabulary pairs frame the exam questions. An **event** is a fact about the past ("Object Created"); a **command** is a request to do something ("run job X"). **Choreography** means services react to each other's events with no central brain (EventBridge/SNS). **Orchestration** means a central coordinator drives each step and tracks state ([Step Functions](20-Step-Functions.md), [MWAA](21-MWAA-Glue-Workflows.md)). Most real pipelines combine them: *events trigger orchestrations*.

With this guide you can answer: *"start processing when a file lands"* (S3 notifications vs EventBridge), *"alert when a Glue job fails"*, *"schedule with time zones"*, *"protect the database from write spikes"*, *"fan out to several consumers"*, and SQS's visibility timeout, DLQ, and message-size traps.

## S3 → something: Event Notifications vs EventBridge

| | **S3 Event Notifications (direct)** | **S3 → EventBridge** |
|---|---|---|
| Setup | Notification config per event type + destination | Turn on **"Send notifications to Amazon EventBridge"** on the bucket. **All** S3 events flow to the default bus |
| Destinations | **Lambda, SQS (standard only, no FIFO), SNS (standard topics)** | Anything EventBridge targets: **Step Functions, Glue workflows**, SQS **FIFO**, Firehose, Kinesis, ECS, Batch, API destinations, Redshift Data API… |
| Filtering | Key **prefix and suffix only** (no wildcards). **Overlapping prefix/suffix rules for the same event type are rejected** | Rich content filtering: key prefix/suffix/**wildcard**, **object size (numeric)**, source IP, requester, reason, etc. |
| Multiple consumers | One destination per non-overlapping filter (fan out via SNS) | Many rules, **up to 5 targets per rule** |
| Extras | Lowest latency, simplest | **Archive & replay**, cross-account/cross-Region routing, schema discovery |
| Event names | `s3:ObjectCreated:*`, `s3:ObjectRemoved:*`, restore, replication, lifecycle… | detail-types **"Object Created"**, "Object Deleted", "Object Restore Completed", "Object Storage Class Changed", … |

Both are delivered **at least once**, usually within seconds, so consumers must be **idempotent**.

**THE trap:** *"Two teams each need notifications for `.csv` uploads under `raw/`, but the second configuration fails to save."* → overlapping filters for the same event type aren't allowed. Fix it with **SNS fan-out** (one notification → topic → many subscribers) or **EventBridge** (many rules on the same event).

**THE trap:** *"Trigger a Step Functions workflow when a file lands, only for files over 1 MB."* → S3 notifications can't target Step Functions directly and can't filter on size. **S3 → EventBridge rule (numeric filter on `detail.object.size`) → state machine.**

**THE trap:** *"Lambda writes its output back into the bucket that triggers it"* → an infinite loop. Use separate buckets or non-overlapping prefixes.

## EventBridge — the switchboard

**Event buses:** the **default** bus (AWS service events land here), **custom** buses (your application events, sent with `PutEvents`), and **partner** buses (SaaS sources such as Salesforce, Datadog, Zendesk). Events are JSON with `source`, `detail-type`, and `detail`. The maximum event size is 256 KB.

**Rules** match events with an **event pattern** and send them to **up to 5 targets**. (A rule can instead run on a schedule; that is the legacy approach, and EventBridge Scheduler is preferred.) Pattern operators to recognize:

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": { "name": ["amzn-s3-demo-bucket"] },
    "object": {
      "key":  [{ "wildcard": "raw/orders/*.csv" }],
      "size": [{ "numeric": [">", 1048576] }]
    }
  }
}
```

| Operator | Syntax example |
|---|---|
| Exact / OR list | `"state": ["FAILED", "TIMEOUT"]` |
| prefix / suffix | `[{"prefix": "raw/"}]`, `[{"suffix": ".parquet"}]` |
| wildcard | `[{"wildcard": "raw/*/2026-*.csv"}]` |
| anything-but | `[{"anything-but": "SUCCEEDED"}]` (also with prefix/suffix/wildcard) |
| numeric | `[{"numeric": [">", 0, "<=", 5]}]` |
| exists | `[{"exists": false}]` |
| equals-ignore-case | `[{"equals-ignore-case": "failed"}]` |
| IP range | `[{"cidr": "10.0.0.0/24"}]` |
| $or across fields | `"$or": [{"a": [...]}, {"b": [...]}]` |

**Targets relevant to data pipelines:** Lambda (async), **Step Functions** (async start), **SQS** (standard, fair, FIFO), **SNS**, **Kinesis Data Streams**, **Amazon Data Firehose** (formerly Kinesis Data Firehose), **ECS task**, **AWS Batch job queue**, **Glue workflow** (starts a workflow with an EVENT trigger), **Redshift Data API** (run SQL on a provisioned cluster or Serverless workgroup), SageMaker AI pipeline, CodeBuild/CodePipeline, CloudWatch Logs, another event bus, and **API destinations** (any HTTPS endpoint, with auth stored in a *connection*).

**Input transformer:** reshapes the event before delivery. You extract fields with an `InputPathsMap` and fill an `InputTemplate`, for example to turn a verbose event into a clean "Job X failed at T" message for SNS or to pass only the S3 key to a target.

**Delivery reliability:** failed deliveries are retried with backoff and jitter for up to **24 hours and 185 attempts** by default (both configurable per target). After that the event is **dropped unless the target has a DLQ** (an SQS queue). *"No events may be lost if a target is down"* → configure the **retry policy + DLQ**.

**Archive & replay:** archive all events, or a filtered subset, from a bus with a retention period, then **replay** a time window back onto the bus. That serves replayability (skill 1.1.11): *"reprocess yesterday's events after fixing a bug"*.

**Schema registry & discovery:** automatically infers schemas of events on a bus and generates code bindings.

**Cross-account / cross-Region:** a rule can target an **event bus in another account** (the receiving bus needs a resource policy) or **another Region**. That's the standard pattern for a **central data-platform account** collecting events from many producer accounts.

### AWS service events worth knowing (the alerting backbone)

| Source | detail-type | Typical rule |
|---|---|---|
| `aws.glue` | **Glue Job State Change** (`FAILED`, `TIMEOUT`, `STOPPED`, `SUCCEEDED`) | FAILED/TIMEOUT → SNS alert / Lambda remediation |
| `aws.glue` | **Glue Crawler State Change**; **Glue Data Catalog Table State Change** | Crawler Succeeded → start next job; new partition → notify |
| `aws.glue-dataquality` | **Data Quality Evaluation Results Available** | Score below threshold → quarantine workflow + SNS |
| `aws.emr` | **EMR Step Status Change**, EMR Cluster State Change; EMR Serverless Job Run State Change | Step FAILED → SNS |
| `aws.athena` | **Athena Query State Change** | FAILED → alert; SUCCEEDED → next step |
| `aws.redshift-data` | **Redshift Data Statement Status Change** (when the statement is submitted with events enabled) | Long SQL finished → trigger downstream (no polling) |
| `aws.dms` | DMS Replication Task State Change | Task stopped/failed → SNS |
| `aws.states` | **Step Functions Execution Status Change** | FAILED / TIMED_OUT → SNS |
| `aws.s3` | **Object Created** (bucket EventBridge setting on) | Start ingestion |

Pattern (skills **1.3.4 / 3.3.3**): **service state change → EventBridge rule (filter on `detail.state`) → SNS topic (email/SMS/chat) or Lambda**. No polling, no code in the job. Metric-based alerting (e.g., Kinesis iterator age) uses **CloudWatch alarms → SNS** instead. See [Guide 32](32-Monitoring-Logging-Troubleshooting.md).

## EventBridge Scheduler — the clock (skill 1.1.5 / 3.1.9)

A dedicated, serverless scheduler, separate from rules:
- **One-time** (`at(2026-10-01T09:00:00)`), **rate** (`rate(15 minutes)`), and **cron** (`cron(0 2 * * ? *)`) schedules.
- **Time zones with daylight-saving handling.** Scheduled *rules* run in UTC only.
- **Flexible time windows**: fire at some point within, say, a 15-minute window, to spread load.
- **Universal targets**: call almost any AWS API directly (e.g., `glue:StartJobRun`, `states:StartExecution`, `sqs:SendMessage`), plus templated targets.
- Retry policy + **DLQ**, and scale to millions of schedules. Schedules can be grouped, and one-time schedules can delete themselves after running.

**Scheduler vs scheduled rule:** new designs → **Scheduler** (time zones, one-time schedules, scale, more targets). Scheduled rules are the legacy approach and still work.

## EventBridge Pipes — point-to-point with no glue code

**Pipes** connect **one source to one target**, with optional **filter → enrich → transform** in between:

```
Source ──► Filter ──► Enrichment ──► Target
SQS, Kinesis Data Streams,   (event   Lambda, Step Functions   Step Functions, Lambda, SQS, SNS,
DynamoDB Streams, Amazon MSK, pattern) (Express), API Gateway,  Kinesis, Firehose, EventBridge bus,
self-managed Kafka, Amazon MQ          API destination          ECS, Batch, Redshift Data API, …
```

Use it when you would otherwise write a Lambda whose only job is to poll a stream, drop some records, call an API, and forward the result. *"DynamoDB Streams changes must be filtered to only `INSERT`s, enriched from a REST API, and sent to a Step Functions workflow, with minimal code"* → **EventBridge Pipes**. Rules are **many-to-many routing on a bus**; Pipes are **polling point-to-point integrations**.

## SNS — the megaphone

- **Topics:** **Standard** (very high throughput, at-least-once, best-effort order) vs **FIFO** (strict ordering per **message group**, deduplication, **300 msg/s per message group**, and **3,000 msg/s or 20 MB/s per topic** by default, higher with per-message-group throughput scope). FIFO topics fan out to SQS queues and support **message archiving and replay**.
- **Subscriptions:** **Lambda, SQS, HTTP/S, email / email-JSON, SMS, mobile push, Amazon Data Firehose** (to land notifications in S3/Redshift/OpenSearch).
- **Message filtering:** a **filter policy** per subscription, **attribute-based** (default, on message attributes) or **payload-based** (`FilterPolicyScope: MessageBody`). Each subscriber gets only the messages it cares about, with no filtering code in consumers.
- **Fan-out:** SNS → several SQS queues (each consumer has its own durable buffer) → independent processing. This is the classic fan-out for skill **1.1.10**.
- **Reliability:** SNS retries failed deliveries using per-protocol retry policies. After retries run out, messages go to a **subscription-level DLQ** (an SQS queue on the subscription's redrive policy), if one is configured.
- **Security:** **SSE with KMS**. A KMS key policy must allow the publishing service, such as S3, EventBridge, or CloudWatch.
- **Message size:** **256 KiB**. Larger payloads use the **SNS Extended Client Library** (payload in S3, up to 2 GB), or the usual pattern of putting the data in S3 and publishing a pointer.
- **Raw message delivery** for SQS/HTTP/Firehose subscribers strips the SNS JSON envelope.

> ⚠️ **2026 status:** **SNS Message Data Protection** (PII detection/masking inside SNS messages) entered **maintenance on Apr 30, 2026**, with no new customers. Treat it as a likely distractor. For PII discovery and masking use Macie, Glue sensitive-data detection, or DataBrew (see [Guide 42](42-Privacy-PII-Masking-Sovereignty.md)).

**THE trap:** *"A single consumer must reliably process every notification even if it is down for an hour"* → SNS alone has no durable storage for standard topics. Use **SNS → SQS** (or EventBridge → SQS) so the queue holds messages until the consumer returns.

## SQS — the waiting room

| | **Standard** | **FIFO** |
|---|---|---|
| Throughput | Nearly unlimited | **300 TPS per API action** (3,000 msg/s with batches of 10). **High-throughput mode** reaches up to **70,000 TPS** (700,000 msg/s batched) in the largest Regions, lower elsewhere |
| Delivery | **At-least-once** (duplicates possible) | **Exactly-once processing** (5-minute deduplication window via `MessageDeduplicationId` or content-based dedup) |
| Order | Best-effort | Strict **within a message group** (`MessageGroupId`) |
| Noisy neighbors | **Fair queues** (standard queue + `MessageGroupId`) reduce one tenant's backlog impact on others | Parallelism = number of active message groups |

**Key settings (bold = numbers the exam loves):**

| Setting | Values | Why it matters |
|---|---|---|
| Visibility timeout | Default **30 s**, max **12 h** | Received messages are hidden, not deleted. If processing outlasts it, the message reappears and is **processed twice** → raise it (for Lambda, set it to about **6× the function timeout**) |
| Retention | Default **4 days**, **1 minute – 14 days** | Not a database; set it for the longest expected outage |
| Long polling | `ReceiveMessageWaitTimeSeconds` up to **20 s** | Fewer empty receives → lower cost |
| Delay queue / message timer | **0 – 15 minutes** | Postpone processing (e.g., wait for an eventually consistent upstream) |
| Max message size | **1 MiB** (raised from 256 KiB on **Aug 4, 2025**) | Older questions say 256 KB. Bigger → **Extended Client Library** (Java/Python) stores the payload in **S3** (up to 2 GB) |
| DLQ | Redrive policy with **`maxReceiveCount`** | Poison messages move aside after N failed receives. The DLQ must be the **same type** (FIFO → FIFO DLQ). **DLQ redrive** moves messages back to the source queue after the fix |
| Batching | Up to **10 messages** per Send/Receive/Delete call | Throughput and cost |
| Encryption | SSE-SQS (default) or SSE-KMS | KMS key policy must allow producers such as SNS or EventBridge |

**SQS as a shock absorber (skill 1.1.9, throttling & rate limits).** A burst of 50,000 writes per second would throttle DynamoDB or exhaust RDS connections. Put **SQS in front** and let consumers drain it at a controlled rate: Lambda event source mapping with **maximum concurrency**, or a fixed worker fleet. The queue absorbs the spike and the database sees a steady rate. Pair it with **exponential backoff with jitter** in the consumers (see [Guide 36](36-Programming-IaC-CICD.md)).

**Lambda + SQS in one paragraph** (depth in [Guide 17](17-Lambda-for-Data-Pipelines.md)): the event source mapping polls for you, batches up to 10,000 messages from standard queues (10 for FIFO) with a batching window, should use **`ReportBatchItemFailures`** so one bad message doesn't make the whole batch retry, supports **maximum concurrency** to cap pressure on downstream systems, and handles messages up to 1 MiB.

**THE trap:** *"Messages are processed more than once"* → visibility timeout shorter than processing time (or no idempotency on a standard queue). *"Must never process duplicates, and order matters per customer"* → **FIFO with `MessageGroupId` = customer ID**. *"One failing message blocks progress / retries forever"* → **DLQ with maxReceiveCount**.

## Choosing the messaging service

| Service | Model | Consumers | Retention / replay | Ordering | Pick when |
|---|---|---|---|---|---|
| **SQS** | Queue, consumers **pull** | **One** consumer per message | Up to 14 days; no replay once deleted | FIFO per group | Decouple, buffer spikes, work queues, protect databases |
| **SNS** | Pub/sub, **push** | **Many** subscribers at once | None (except FIFO archive) | FIFO per group | Alerts, fan-out to queues/functions/email |
| **EventBridge** | Event bus, rules | Up to 5 targets per rule, many rules | Archive & replay | Not guaranteed | React to **AWS service/SaaS events**, content-based routing, cross-account buses, schedules |
| **Kinesis Data Streams** | Ordered stream (shards) | Many readers of the **same** data | 24 h – 365 days, **replay by position** | Per shard | Real-time streaming analytics, multiple independent consumers, high-volume ordered data — [Guide 06](06-Kinesis-Data-Streams.md) |
| **Amazon MSK** | Kafka topics/partitions | Many consumer groups | Configurable / tiered storage | Per partition | Kafka compatibility, open-source ecosystem, lift-and-shift Kafka — [Guide 08](08-Amazon-MSK-Kafka.md) |

Rules of thumb: **"replay a stream from a point in time / several apps read the same records"** → Kinesis or MSK, not SQS. **"React when a Glue job fails"** → EventBridge. **"Notify humans"** → SNS. **"Buffer and throttle"** → SQS.

## Question patterns

> *"Data engineers must be emailed within minutes whenever any AWS Glue ETL job fails or times out, with the LEAST operational overhead."* → **EventBridge rule on "Glue Job State Change" with `state` FAILED/TIMEOUT → SNS topic with email subscriptions** (no polling code; CloudWatch alarms on job metrics are more work).

> *"New CSV files in an S3 prefix must start a Step Functions workflow; only files larger than 10 MB should trigger it."* → **Enable EventBridge on the bucket; rule with prefix + numeric size filter → Step Functions** (S3 notifications can't filter on size or target Step Functions).

> *"Uploads to one bucket must trigger three independent consumers (Lambda, an SQS-based loader, and an audit service) using S3 Event Notifications, but a second overlapping notification can't be saved."* → **S3 notification → SNS topic → fan out to the three subscribers** (or switch to EventBridge with several rules).

> *"A burst of IoT writes causes DynamoDB throttling during peaks; the writes can be delayed a few minutes."* → **Buffer writes in SQS and consume with a rate-limited consumer (Lambda ESM maximum concurrency)** (a queue smooths spikes; raising capacity for rare peaks costs more).

> *"Some SQS messages are processed twice by a Lambda consumer whose function runs up to 5 minutes."* → **Increase the queue's visibility timeout (about 6× the function timeout) and make processing idempotent.**

> *"Records that repeatedly fail parsing keep being retried and delay good records."* → **Configure a DLQ with maxReceiveCount (and ReportBatchItemFailures for Lambda); redrive after fixing.**

> *"An order pipeline must process events exactly once and in order per customer, at 1,000 messages per second."* → **SQS FIFO with MessageGroupId = customerId, batching or high-throughput mode.**

> *"A nightly Glue job must run at 01:30 local time in Europe/Berlin, adjusting for daylight saving time."* → **EventBridge Scheduler with a time zone and a universal target (`glue:StartJobRun`)** (scheduled rules and Glue cron schedules are UTC).

> *"A bug in a consumer corrupted two days of processed events; re-send those events after deploying the fix."* → **EventBridge archive and replay** (for streams, Kinesis replay by position).

> *"Changes captured by DynamoDB Streams must be filtered to INSERT events, enriched with customer data from an internal API, and delivered to a Step Functions workflow without custom polling code."* → **EventBridge Pipes (source DynamoDB Streams, filter, API destination enrichment, Step Functions target).**

> *"A Redshift stored procedure takes 20 minutes; the next step should start when it finishes, without a polling loop."* → **Submit via the Redshift Data API with events enabled; EventBridge rule on "Redshift Data Statement Status Change" → next step.**

> *"Application events from 30 producer accounts must be processed centrally in a data-platform account."* → **Cross-account EventBridge: rules in producer accounts target the central account's event bus (resource policy allows them).**

> *"A notification payload is 900 KB. Which service accepts it directly?"* → **SQS (1 MiB max since Aug 2025)**. SNS and EventBridge are still 256 KB, so use S3 plus a pointer (or the extended client libraries).

> *"Each subscriber team should receive only the notifications for its business unit from one shared topic, without code changes in the publishers."* → **SNS subscription filter policies** (attribute- or payload-based).

> *"Two analytics applications need to independently read the same clickstream records and reprocess the last 3 days on demand."* → **Kinesis Data Streams** (SQS delivers each message to one consumer and can't replay).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| React to AWS service events (Glue/EMR/Athena/DMS/Step Functions) | EventBridge rule (default bus) |
| Job failed → notify people | EventBridge rule → SNS |
| S3 upload → Lambda/SQS/SNS, prefix/suffix only | S3 Event Notifications |
| S3 upload → Step Functions / Glue workflow / size filter / many targets | S3 → EventBridge |
| S3 notification to FIFO queue | Not supported directly → via EventBridge |
| Overlapping S3 notification filters | Not allowed → SNS fan-out or EventBridge |
| Targets per rule | 5 |
| Pattern operators | prefix, suffix, wildcard, anything-but, numeric, exists, equals-ignore-case, cidr, $or |
| Target down, don't lose events | Retry policy (24 h / 185 attempts) + DLQ |
| Reprocess past events | EventBridge archive & replay |
| Central account collects events | Cross-account event bus + resource policy |
| Reshape event for target | Input transformer |
| Call any HTTPS API from a rule | API destination (+ connection) |
| Run SQL on an event | Redshift Data API target |
| Cron with time zones / one-time schedule | EventBridge Scheduler |
| Spread scheduled load | Flexible time window |
| Stream/queue → filter → enrich → target, no code | EventBridge Pipes |
| One message → many subscribers | SNS |
| Per-subscriber filtering | SNS filter policy (attributes or body) |
| Durable fan-out | SNS → multiple SQS queues |
| Ordered pub/sub | SNS FIFO → SQS FIFO |
| SNS max message | 256 KiB (extended client via S3) |
| SNS Message Data Protection | ⚠️ Maintenance from Apr 30, 2026 |
| Buffer spikes / protect DB | SQS + rate-limited consumers |
| Processed twice | Raise visibility timeout (default 30 s, max 12 h) |
| Poison messages | DLQ + maxReceiveCount; DLQ redrive |
| Empty receives cost | Long polling (≤ 20 s) |
| Delay processing | Delay queue (≤ 15 min) |
| Retention | 4 days default, 1 min – 14 days |
| SQS max message | 1 MiB (was 256 KB); larger → Extended Client + S3 |
| Order + no duplicates | SQS FIFO (300 TPS; 3,000 batched; high-throughput mode) |
| Multiple readers + replay | Kinesis Data Streams / MSK |

Events and schedules start pipelines. Deploying those pipelines repeatably, testing them, and calling AWS APIs correctly from code is covered in [Guide 36 — Programming, IaC & CI/CD](36-Programming-IaC-CICD.md).
