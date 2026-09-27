# 21 · Amazon MWAA & AWS Glue Workflows — Python-coded railways and Glue-only tram lines

> **Exam map:** D1 · Task 1.1, 1.3 — D3 · Task 3.1 · **Skills:** 1.1.5, 1.3.1, 3.1.1, 3.1.2 · **Weight:** 🔥🔥🔥 High · **Read time:** ~22 min

## The idea

Think of a **railway network**. The timetable says which trains run when. The signal rules say a train may leave only after the ones it depends on have arrived. A dispatcher enforces the timetable, and a crew of drivers actually moves the trains. **Apache Airflow** is that network for data work. You write the timetable and signal rules as Python code, called a **DAG (Directed Acyclic Graph)**: tasks joined by arrows, with no loops. The **scheduler** is the dispatcher, the **workers** are the drivers, and the **web UI** is the station board where you watch every train. **Amazon MWAA (Managed Workflows for Apache Airflow)** is AWS running that railway for you. AWS handles the scheduler, workers, metadata database, and web server. You bring the DAG files.

**AWS Glue workflows** are a small **tram line inside one city**. They're cheap and simple, and they run only **Glue jobs and Glue crawlers**, linked by Glue triggers. They're great when every stop is Glue. Once the route needs Lambda, EMR, or Redshift, you need a bigger network: MWAA or Step Functions.

With this guide you can answer: *"existing Airflow DAGs, least effort to migrate"* → MWAA. *"DAG doesn't show up / requirements fail / tasks stuck queued"* → the MWAA troubleshooting table. *"chain Glue crawler → job → crawler, start on S3 file arrival in batches"* → Glue workflow with an EventBridge trigger. *"which orchestrator?"* → the decision table at the end.

## Airflow from zero

| Concept | What it is | Exam-relevant detail |
|---|---|---|
| **DAG** | A Python file defining tasks + dependencies + schedule | Lives in the `dags/` folder; parsed continuously by the scheduler (top-level code runs on **every parse**, so keep it light) |
| **Task / Operator** | An operator is a template for one unit of work; a task is an operator instance | `PythonOperator`, `BashOperator`, plus AWS provider operators (below) |
| **Sensor** | A task that **waits for a condition** (a file exists, a job finished) | `S3KeySensor` (supports wildcards) waits for a file to land |
| **Deferrable operator / triggerer** | The task hands its wait to the async **triggerer** process and frees its worker slot | `deferrable=True` on AWS operators and sensors. On MWAA the triggerer runs alongside the scheduler |
| **Hook** | Low-level client wrapper used by operators (e.g., `S3Hook`) | Reuses Airflow connections |
| **Connection / Variable** | Stored credentials and endpoints / config key-values | Back them with **AWS Secrets Manager** (`SecretsManagerBackend`), not plaintext in the metadata DB |
| **XCom** | "Cross-communication": small values passed between tasks via the metadata DB | **Small data only** (IDs, S3 keys, row counts). Never datasets. Pass S3 paths |
| **Schedule** | cron (`0 2 * * *`), presets (`@daily`), timedelta, custom **timetables**, or **data-aware** scheduling | Data-aware: a DAG runs when an upstream task updates a **Dataset** (Airflow 2.4+), renamed **Asset** in Airflow 3 |
| **catchup / backfill** | Catchup creates runs for every missed interval since `start_date`; backfill re-runs a historical range on purpose | Airflow 3 defaults catchup to **False** (Airflow 2 defaulted to True). A surprise flood of old runs = catchup |
| **Retries** | `retries`, `retry_delay`, `retry_exponential_backoff` per task | Set in `default_args` |
| **Dependencies** | `extract >> transform >> load`, `[a, b] >> c`; trigger rules (`all_success` default, `one_failed`, `all_done`) | Use `all_done` for a cleanup task that must always run |
| **Concurrency controls** | **Pools** (slots shared across DAGs), `max_active_runs`, `max_active_tasks` | Pools protect a fragile source DB from too many parallel tasks |
| **SLAs** | Airflow 2 `sla` / `sla_miss_callback` | **Removed in Airflow 3**, replaced by **Deadline Alerts**. Older questions may still mention SLAs |
| **TaskGroups** | Visual grouping of tasks | SubDAGs are gone in Airflow 3 |

A minimal data DAG (Airflow 3 style):

```python
from airflow.sdk import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
import pendulum

with DAG("orders_daily", schedule="0 2 * * *",
         start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False,
         default_args={"retries": 2}) as dag:
    wait = S3KeySensor(task_id="wait_for_file", bucket_name="amzn-s3-demo-bucket",
                       bucket_key="raw/orders/{{ ds }}/*.csv", wildcard_match=True,
                       deferrable=True)
    etl = GlueJobOperator(task_id="csv_to_parquet", job_name="orders-csv-to-parquet")
    check = AthenaOperator(task_id="row_count", query="SELECT count(*) FROM curated.orders",
                           database="curated", output_location="s3://amzn-s3-demo-bucket/athena/")
    wait >> etl >> check
```

### AWS provider operators worth recognizing (`apache-airflow-providers-amazon`)

| Work | Operator (sensor) |
|---|---|
| Glue job / crawler | `GlueJobOperator` (`GlueJobSensor`); `GlueCrawlerOperator` is **deprecated** in current provider releases in favor of `GlueCrawlerRunOperator` (+ Create/Update) |
| Glue Data Quality, DataBrew | `GlueDataQualityRuleSetEvaluationRunOperator`, `GlueDataBrewStartJobOperator` |
| EMR on EC2 | `EmrCreateJobFlowOperator`, **`EmrAddStepsOperator`** (`EmrStepSensor`), `EmrTerminateJobFlowOperator` |
| EMR Serverless / EMR on EKS | **`EmrServerlessStartJobOperator`** (`EmrServerlessJobSensor`) / `EmrContainerOperator` |
| Athena | **`AthenaOperator`** |
| Redshift | **`RedshiftDataOperator`** (Data API: no JDBC connection or VPC path needed); `S3ToRedshiftOperator` (COPY), `RedshiftToS3Operator` (UNLOAD) |
| S3 | `S3CreateObjectOperator`, `S3CopyObjectOperator`, `S3DeleteObjectsOperator`, `S3ListOperator`, **`S3KeySensor`** |
| Step Functions | **`StepFunctionStartExecutionOperator`** (`StepFunctionExecutionSensor`) |
| Lambda, Batch, ECS, SageMaker, Bedrock, DMS | `LambdaInvokeFunctionOperator`, `BatchOperator`, `EcsRunTaskOperator`, SageMaker/Bedrock/DMS operators |

The pattern to take away: **MWAA orchestrates; it doesn't crunch.** Heavy transforms run on Glue/EMR/Redshift, launched by operators. Workers only submit and monitor.

## Amazon MWAA — the managed railway

### Anatomy of an environment

```
 S3 bucket (versioning ON, block public access, same Region)
   ├─ dags/            ← DAG .py files (picked up automatically)
   ├─ requirements.txt ← extra Python packages (pin versions, include --constraint)
   ├─ plugins.zip      ← custom operators/hooks/binaries
   └─ startup.sh       ← optional shell script run on each component before Airflow starts
          │
          ▼
 MWAA environment (in YOUR VPC: 2 private subnets in 2 AZs + security group)
   scheduler(s) + triggerer · Celery workers (autoscale) · web server(s)
   Aurora PostgreSQL metadata DB (AWS-managed, not in your account)
   execution role (IAM) → what DAG tasks may call
```

- **S3 bucket**: must have **Bucket Versioning enabled** and **Block all public access**, and be in the **same Region**. `requirements.txt`, `plugins.zip`, and the startup script are referenced by **S3 object version**. After uploading a new one, **update the environment** to the new version, which takes roughly **10–30 minutes**. DAG files in `dags/` sync without an update.
- **requirements.txt**: from Airflow 2.7.2 on, include a **`--constraint`** line (MWAA adds one if you don't). Pin versions. Test locally with the **`aws-mwaa-docker-images`** project before deploying.
- **Startup script**: installs Linux runtimes, sets environment variables, and runs on **scheduler, worker, and web server** before requirements install. Its output goes to `startup_script_execution_ip` log streams.
- **Execution role**: the IAM role every task assumes. It needs the bucket, CloudWatch Logs, the environment's SQS queue (Celery), KMS if you use a CMK, **plus every service your DAGs touch** (Glue, EMR, Redshift Data API, Secrets Manager…). It is **one role shared by all DAGs** in the environment, which is a least-privilege weakness that MWAA Serverless fixes.
- **Networking**: **two private subnets in different AZs** plus a security group with a **self-referencing** all-traffic inbound rule (5432 to the metadata DB, 443). Private subnets reach AWS services via a **NAT gateway** or, with no internet, via **VPC endpoints** (S3 gateway endpoint plus interface endpoints for CloudWatch Logs/Monitoring, SQS, KMS, ECR, Airflow endpoints, …).
- **Web server access mode**: **Public network** (UI reachable over the internet, still behind IAM login) or **Private network** (UI only from inside the VPC, e.g., via Client VPN or a bastion). Newer Airflow versions also offer a combined mode.
- **Environment classes**: **mw1.micro, mw1.small, mw1.medium, mw1.large, mw1.xlarge, mw1.2xlarge**. Default tasks per worker are **3 / 5 / 10 / 20 / 40 / 80** and DAG capacity grows from about 25 to 4,000. mw1.micro doesn't autoscale.
- **Scaling**: workers autoscale between **min and max workers (1–25 by default)**; **schedulers 2–5** (default 2; micro 1); web servers can also autoscale for REST API/CLI load. The class sets per-container size; min/max workers set horizontal scale.
- **Versions**: MWAA supports several **Airflow 2.x and 3.x** versions (Airflow 3 arrived on MWAA with **v3.0.6 in Oct 2025**; 2026 releases added later 3.x lines). **Minor-version upgrades and downgrades** happen in place. A **major** move (2 → 3) means a **new environment** and DAG migration.
- **Programmatic access**: `CreateCliToken` (run Airflow CLI commands; at most about 4 concurrent CLI commands on the web server), `CreateWebLoginToken` (UI or REST session), and **`InvokeRestApi`** (call the Airflow REST API with plain **IAM/SigV4** credentials, `airflow:InvokeRestApi`, 10-second timeout, 6 MB response). *"A Lambda must trigger a DAG run when a file arrives"* → Lambda calls **`InvokeRestApi`** (`POST /dags/{id}/dagRuns`).
- **Monitoring**: five CloudWatch log groups, `airflow-<env>-DAGProcessing`, `-Scheduler`, `-Task`, `-WebServer`, `-Worker`, each **off until you enable it**, at a chosen level (INFO…CRITICAL). CloudWatch metrics cover queued/running tasks, scheduler heartbeat, parse times, and worker counts.

### Troubleshooting MWAA (skill 3.1.2)

| Symptom | Where to look | Usual cause → fix |
|---|---|---|
| **DAG not visible in the UI** | **DAGProcessing** log group; "Broken DAG" banner | Python syntax/import error, a missing package, a file outside `dags/`, or the DAG object not created at module level → fix the code; add the package to requirements |
| **requirements.txt install failed** / tasks fail with `ModuleNotFoundError` | Scheduler/worker log group → **`requirements_install_ip`** stream | Unpinned or conflicting versions, missing `--constraint`, no internet from private subnets → pin versions, use the constraints file, test with `aws-mwaa-docker-images`, host wheels in `plugins.zip` or a private repo, then **update the environment** to the new S3 version |
| Changed requirements/plugins "did nothing" | Environment details | Uploaded a new file but didn't point the environment at the **new S3 object version** / run an update |
| **Tasks stuck in queued** | CloudWatch queued/pending metrics | Not enough worker capacity (**raise max workers / min workers**, larger class), pool slots exhausted, `max_active_tasks`/`parallelism` too low, autoscaling lag from bursty starts (stagger schedules) |
| Tasks stuck "running", workers don't scale down | UI task states | Stranded tasks → clear or mark them so autoscaling can release workers |
| **"The scheduler does not appear to be running"** / heartbeat gaps | Scheduler logs, metrics | Security group missing self-referencing/5432 access, dependency install failures crashing components, overloaded scheduler (too many or heavy DAG files) → fix networking, lighten top-level DAG code, add schedulers |
| **AccessDenied** in task logs | Task log group | **Execution role** missing the action (e.g., `glue:StartJobRun`, `secretsmanager:GetSecretValue`, KMS decrypt) |
| Tasks time out calling AWS APIs or PyPI | Worker logs | Private subnets with **no NAT and no VPC endpoint** for that service → add a NAT gateway or interface endpoints |
| Can't open the Airflow UI | — | Private web server mode (needs VPN/bastion), or IAM principal lacks `airflow:CreateWebLoginToken` for that environment/role |
| Environment creation stuck/failed | CloudTrail, environment status | Bucket not versioned, subnets not private/in two AZs, missing execution-role permissions |

## 🆕-ish: Amazon MWAA Serverless (launched Nov 17, 2025)

A **deployment option with no environment to size**. You submit a **workflow definition in YAML** (the open-source **DAG Factory** format). Existing Python DAGs can be converted with AWS's **Python-to-YAML converter**. It runs on **Airflow 3 (Python 3.12)** and schedules through **EventBridge Scheduler** (cron, rate, one-time, time zones).

| | MWAA Serverless | Provisioned MWAA |
|---|---|---|
| Capacity | None to manage; compute provisioned **per task** at run time | Always-on environment (class + min/max workers) |
| Cost | **Pay only for task execution time** | Hourly for environment, schedulers, web servers, extra workers, even when idle |
| Isolation | **Each workflow has its own IAM execution role** and isolated compute; VPC optional | One execution role and shared workers for all DAGs |
| Authoring | YAML (DAG Factory), AWS provider operators; versioned, immutable workflow versions | Python DAGs, any operator, custom plugins |
| UI | **No Airflow web UI** (console, CloudWatch Logs, CloudTrail) | Full Airflow UI |
| Latency | Per-task startup overhead | Fast when worker capacity exists |

AWS added **PythonOperator and BashOperator** support (code bundles from S3) in August 2026. Custom plugins, community operators, and the Airflow UI still point to provisioned MWAA. **Choose Serverless** for sporadic or variable AWS-native pipelines, for cost-sensitive dev/test, and for **per-workflow least privilege** across teams. **Choose provisioned** for custom operators and plugins, UI-driven operations, or constant high volume.

## AWS Glue workflows — the Glue-only tram line

A **workflow** is a container of **Glue jobs, Glue crawlers, and Glue triggers**, shown as a graph in the Glue console, with run history and status for each node.

**Triggers:**

| Trigger | Fires when | Notes |
|---|---|---|
| **Scheduled** | cron expression (`cron(0 2 * * ? *)`) | Also works as a standalone trigger for a single job or crawler |
| **Conditional** | Watched jobs/crawlers reach states | Job states `SUCCEEDED`, `FAILED`, `TIMEOUT`, `STOPPED`; crawler states `SUCCEEDED`, `FAILED`, `CANCELLED`. Logic **ALL** (join) or **ANY** |
| **On-demand** | You start it (console/API/CLI) | Stays in `CREATED` |
| **EventBridge event** (`EVENT`) | An EventBridge rule targets the workflow (**start trigger only**) | Optional batching: **BatchSize** (required when batching) + **BatchWindow** (default and max **900 s**). Whichever is met first starts the run. EventBridge needs `glue:notifyEvent` |

Other pieces:
- **Workflow run properties**: key/value state shared by all jobs in a run. Jobs read and update them with `GetWorkflowRunProperties` / `PutWorkflowRunProperties` (e.g., pass the batch date or the S3 prefix to process).
- **Max concurrent runs** can be set per workflow. Failed runs can be **resumed** from the failed nodes. Keep each workflow to **≤ 100** jobs + crawlers + triggers.
- **Blueprints**: parameterized templates that generate workflows (e.g., "for each of these 30 tables, create crawler → job → crawler").
- Dependency chains must descend from a **scheduled or on-demand** start trigger. Only two crawlers per trigger.

```
[EVENT trigger: batch 50 objects or 900 s] → Crawler(raw) ─SUCCEEDED→ Job(clean) ─SUCCEEDED→ Crawler(curated)
                                                                     └─FAILED→ Job(notify-or-quarantine)
```

**THE trap: Glue workflows orchestrate only Glue components.** *"Run a Glue job, then invoke a Lambda, then run a Redshift stored procedure, then send an SNS message"* → **Step Functions** (or MWAA). A Glue workflow can't call Lambda, EMR, or Redshift as nodes. (A Glue Python shell job *could* call them, but that's the hacky distractor.) Failure alerting for Glue workflows usually runs through **EventBridge "Glue Job State Change" rules → SNS** (see [Guide 22](22-EventBridge-SNS-SQS.md)).

**THE trap: "S3 file arrival starts a Glue job".** Standalone Glue triggers are schedule/conditional/on-demand. The event-driven start is an **EventBridge event trigger on a workflow** (with batching to avoid one run per file), or EventBridge → Step Functions → Glue.

## Scheduling jobs and crawlers (skill 1.1.5)

| Need | Tool |
|---|---|
| Run a Glue job or crawler on a timetable, Glue-only | **Glue scheduled trigger** / **crawler schedule** (cron) |
| Time-zone-aware, one-time or recurring schedule for *any* target (Step Functions, Lambda, ECS, Glue API via universal targets) | **EventBridge Scheduler** |
| Schedule plus rich dependencies, backfills, sensors | **Airflow schedule** (cron/timetable/Asset-aware) in MWAA |
| Run when new data arrives, not on a clock | **Event-driven**: S3 → EventBridge → Step Functions / Glue workflow EVENT trigger; Airflow Assets or `S3KeySensor` |
| Crawl only what changed | Crawler with **S3 event mode** (SQS-backed incremental crawls), see [Guide 13](13-Glue-Data-Catalog-Crawlers.md) |

## The orchestration decision table

| Option | Pick when | Loses when |
|---|---|---|
| **Step Functions** | AWS-native, serverless, visual; `.sync` waits on Glue/EMR/Athena/Batch; callbacks; Distributed Map fan-out; *"least operational overhead"* | Team already has Airflow DAGs; needs Python-coded scheduling semantics (backfill, catchup) |
| **MWAA (provisioned)** | **Existing Airflow DAGs** to migrate; open-source/portable orchestration; hybrid and third-party systems via community providers; complex schedules and backfills | Occasional simple pipelines (always-on cost); "no infrastructure to manage" wording |
| **MWAA Serverless** | Airflow semantics without sizing an environment; sporadic runs; per-workflow IAM isolation | Need the Airflow UI, custom plugins, or constant high throughput |
| **Glue workflows** | **Only Glue jobs + crawlers**, simple dependency graph, no extra service | Any non-Glue step |
| **EventBridge rules / Scheduler** | **Routing and triggering**: start something on an event or a clock | Multi-step **stateful** orchestration with waits, retries across steps, branching |
| **Lambda chaining** (function calls function) | Rarely. Tiny, short, two-step glue | Anything real: 15-min cap, hidden logic, no visual state, **anti-pattern** |
| **AWS Data Pipeline** | Never for new designs | ⚠️ Maintenance mode (closed to new customers since 2024), **not in scope**. Treat as a distractor; the modern answer is Step Functions / MWAA / Glue workflows |

Quick rule: **"existing Airflow" → MWAA. "Serverless, AWS-native, waits for jobs" → Step Functions. "Only Glue" → Glue workflow. "Trigger on event/time" → EventBridge (plus one of the above).** See the full comparison in [Guide 45](45-Service-Selection-Decision-Guide.md).

## Question patterns

> *"A company runs 200 Apache Airflow DAGs on self-managed EC2 and wants to move to AWS with the LEAST refactoring and operational overhead."* → **Amazon MWAA** (same DAG code; Step Functions would mean rewriting every pipeline in ASL).

> *"After a developer uploaded a new DAG file to the MWAA bucket, it doesn't appear in the Airflow UI."* → **Check the DAGProcessing logs for import/syntax errors** (and that the file is under `dags/`); DAG files don't need an environment update.

> *"A new library was added to requirements.txt in S3, but tasks still fail with ModuleNotFoundError."* → **Update the environment to the new requirements.txt S3 version, then check the `requirements_install_ip` log stream** (pin versions with the constraints file).

> *"MWAA tasks sit in the queued state for a long time every morning at 02:00 when 40 DAGs start together."* → **Increase maximum (and minimum) workers or the environment class, and stagger schedules** (worker capacity/autoscaling lag, not a scheduler bug).

> *"MWAA runs in private subnets without internet access; tasks calling Glue and Secrets Manager time out."* → **Create interface VPC endpoints for those services (and S3 gateway endpoint)**, or add a NAT gateway.

> *"An Airflow task fails with AccessDenied when calling glue:StartJobRun."* → **Add the permission to the MWAA execution role** (tasks run with that role, not the user's).

> *"A pipeline has three Glue crawlers and two Glue ETL jobs with dependencies, must start when files land in S3, and should run once per batch of files, not per file. Least effort."* → **Glue workflow with an EventBridge event trigger using BatchSize/BatchWindow** (Glue-only steps; batching avoids hundreds of runs).

> *"The ETL must run a Glue job, then invoke a Lambda function to call a partner API, then run a Redshift stored procedure."* → **Step Functions** (Glue workflows can't include Lambda or Redshift steps).

> *"Store Airflow connection passwords securely with automatic rotation for an MWAA environment."* → **Secrets Manager as the Airflow secrets backend** (not Airflow Variables in plaintext, not environment variables).

> *"Task A computes a 3 GB DataFrame that task B needs."* → **Write it to S3 and pass the S3 key via XCom** (XCom is for small metadata in the metadata DB).

> *"Teams share one MWAA environment; security wants each pipeline to access only its own buckets, and pipelines run only a few times a week."* → **MWAA Serverless** (per-workflow IAM execution roles, pay-per-task; provisioned MWAA has one shared execution role and idle cost).

> *"An external application must trigger a DAG run using IAM credentials, without managing web tokens."* → **MWAA `InvokeRestApi`** with `airflow:InvokeRestApi` permission.

> *"A DAG waits hours for a partner file; the waiting sensors are consuming all worker slots."* → **Deferrable sensors (`deferrable=True`) so the triggerer does the waiting** (or reschedule mode); workers are freed.

> *"A solutions architect proposes AWS Data Pipeline to schedule nightly EMR and Redshift activities for a new project."* → **Reject: Data Pipeline is in maintenance mode; use Step Functions or MWAA** (with EventBridge Scheduler if only a clock trigger is needed).

> *"Run a Glue crawler every day at 01:00 in the Europe/Paris time zone, handling daylight saving, and also start a Step Functions workflow on the same schedule."* → **EventBridge Scheduler** (time-zone-aware cron with universal targets); Glue cron schedules are UTC.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Existing Airflow DAGs / open-source orchestrator | MWAA |
| DAG = Python file of tasks + dependencies | Airflow core concept |
| Wait for a file in S3 | `S3KeySensor` (deferrable) |
| Free worker slots while waiting | Deferrable operators + triggerer |
| Pass small values between tasks | XCom (S3 keys, not data) |
| Credentials for DAGs | Secrets Manager backend |
| Run when upstream data updates | Datasets (Airflow 2) / Assets (Airflow 3) |
| Unexpected flood of historical runs | catchup (Airflow 3 default False) |
| Limit parallel hits on a source DB | Airflow pool |
| SLAs in Airflow 3 | Removed → Deadline Alerts |
| MWAA bucket requirements | Versioning ON, block public access, same Region |
| New requirements/plugins version | Update environment (10–30 min) |
| Package conflicts | Pin + `--constraint`; test with aws-mwaa-docker-images |
| DAG missing from UI | DAGProcessing logs (import/syntax) |
| requirements errors | `requirements_install_ip` log stream |
| Tasks stuck queued | More max/min workers, bigger class, pools, stagger |
| AccessDenied in task | MWAA execution role |
| Private subnets, no internet | VPC endpoints (or NAT) |
| MWAA networking | 2 private subnets, 2 AZs, self-referencing SG |
| Environment classes | mw1.micro → mw1.2xlarge (3→80 tasks/worker) |
| Workers / schedulers | 1–25 workers; 2–5 schedulers |
| Trigger DAG with IAM creds | `InvokeRestApi` |
| No environment, pay per task, per-workflow IAM | MWAA Serverless (YAML, Airflow 3, no UI) |
| Only Glue jobs + crawlers | Glue workflow |
| Conditional Glue trigger | Watch job/crawler states, ALL or ANY |
| Start Glue workflow on S3 events in batches | EVENT trigger, BatchSize + BatchWindow (≤ 900 s) |
| Share state between Glue jobs in a run | Workflow run properties |
| Template many similar workflows | Glue blueprints |
| Non-Glue steps (Lambda/EMR/Redshift) | Step Functions or MWAA |
| Clock or event trigger only | EventBridge Scheduler / rules |
| Lambda calling Lambda as a pipeline | Anti-pattern |
| AWS Data Pipeline | ⚠️ Maintenance, out of scope — distractor |

Almost every orchestrator in this guide is started by an event or a clock. Next, go deeper on the event side: [Guide 22 — EventBridge, SNS & SQS](22-EventBridge-SNS-SQS.md).
