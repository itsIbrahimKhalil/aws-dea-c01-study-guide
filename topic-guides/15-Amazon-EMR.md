# 15 · Amazon EMR — rent a whole big-data kitchen, keep the pantry in S3

> **Exam map:** D1 · Task 1.1, 1.2 — D2 · Task 2.1 — D3 · Task 3.1, 3.3 — D4 · Task 4.3, 4.4 · **Skills:** 1.1.2, 1.2.4, 1.2.5, 2.1.1, 2.1.2, 3.1.4, 3.3.6, 3.3.8, 4.3, 4.4.5 · **Weight:** 🔥🔥🔥 High · **Read time:** ~18 min

## The idea

**Amazon EMR** (originally "Elastic MapReduce") is AWS's managed platform for open-source big-data engines: **Spark, Hive, Trino/Presto, HBase, Flink, Hadoop, Tez**, the table formats **Hudi, Iceberg, Delta Lake**, and notebook tooling (**JupyterHub, Livy**). AWS installs and configures the software. You choose engines and hardware, and you pay while machines run.

Picture a **commercial kitchen rented for a banquet**. The **head chef** (*primary node*) takes orders and hands out work. The **line cooks** (*core nodes*) cook and keep ingredients on their own shelves (**HDFS**, the Hadoop Distributed File System). The **temp cooks** (*task nodes*) only cook. They have no shelves, so sending them home loses nothing. The **warehouse down the road** is **Amazon S3**, which survives after the kitchen closes. The smart operator stores ingredients in the warehouse, rents the kitchen only for the banquet (a *transient cluster*), and fills temp-cook slots with cheap day labour (**Spot Instances**).

EMR wins when a question needs **open-source framework control** (specific Spark/Hive versions, custom JARs, HBase, Trino, Flink) at **petabyte scale** and **lower cost than fully managed ETL**, in exchange for more operational work than AWS Glue. This guide covers deployment options, node roles and Spot, storage, scaling, security, logs and troubleshooting, and the "Glue vs EMR vs EMR Serverless" decisions.

## Three ways to run EMR

| Option | You manage | Billing | Pick when |
|---|---|---|---|
| **EMR on EC2** | Cluster of EC2 nodes, YARN | EC2 + EBS **plus** an EMR per-second uplift (**1-min minimum**) | Full control, HBase/Trino/Flink, SSH, long-running or steady large workloads, Spot-heavy tuning |
| **EMR Serverless** | Nothing: an *application* + release label | **vCPU + memory + storage per second** while workers run (1-min minimum; first **20 GB** disk/worker free) | *"Without managing clusters,"* sporadic or unpredictable Spark/Hive jobs |
| **EMR on EKS** | Your Amazon EKS cluster | vCPU + memory requested by pods, plus EKS and EC2/Fargate | Company already runs Kubernetes; **multi-tenant Spark** on shared capacity |

EMR on Outposts is **out of scope**. Multi-engine clusters run **EMR 7.x**, latest **7.14.0** (Spark 3.5.8, Hive 3.1.3, Trino 479, Flink 1.20, Iceberg 1.10, Java 17 default), on EMR's performance-optimized, API-compatible Spark runtime.

> ⚠️ **2026 status:** Since May 2026 there's a separate **Spark-only train `emr-spark-8.0.0`** (Spark **4.0.2**, Scala 2.13, ANSI SQL on by default) on EC2, EKS and Serverless. It **drops EMRFS in favour of the S3A connector** and leaves out JupyterHub, Zeppelin, Hue, Pig and Oozie. Flink, HBase and Trino stay on 7.x for now. The exam pool predates this, so answer EMRFS questions the classic way.

## Node roles and THE Spot trap

| Node | Runs | HDFS data? | Buy as | Notes |
|---|---|---|---|---|
| **Primary** (formerly "master") | YARN ResourceManager, HDFS NameNode, step executor | No | **On-Demand** | **1**, or **3** for HA. Not resizable |
| **Core** | NodeManager **+ DataNode** | **Yes** | **On-Demand** | Shrinking re-replicates HDFS. Losing nodes loses blocks |
| **Task** | NodeManager only | No | **Spot** | Add or remove freely |

**THE trap:** **Spot on the primary or core nodes** for a critical long job. A Spot reclaim (**2-minute warning**) on the primary kills the cluster, and on core nodes it loses HDFS data and shuffle output. The textbook design is **On-Demand primary + On-Demand core + Spot task nodes**. All-Spot is acceptable only for short, re-runnable jobs whose data lives in S3.

**High availability:** **3 primary nodes** with automatic failover. It requires an **external metastore** for Hive/Hue/Oozie and an **external KDC** for Kerberos, and EMR forces **termination protection** on (overriding auto-termination). A cluster lives in **one AZ**. Resilience comes from S3 data plus relaunch elsewhere. HDFS replication defaults to **1** (<4 core nodes), **2** (<10), **3** (10+), so small-cluster HDFS isn't redundant.

## Instance groups vs instance fleets

| | **Uniform instance groups** | **Instance fleets** |
|---|---|---|
| Types per node type | **1** per group | **Up to 5** (console), **up to 30** with an allocation strategy (CLI/API) |
| Purchase mix | Group is On-Demand *or* Spot | **On-Demand + Spot targets** in one fleet (weighted units/vCPU) |
| Spot allocation | — | **price-capacity-optimized** (default since 6.10), capacity-optimized(-prioritized), lowest-price, diversified |
| Subnets/AZs | One | **Several**; EMR picks the best AZ at launch |
| Spot timeout | — | `TimeoutAction` = **SWITCH_TO_ON_DEMAND** or **TERMINATE_CLUSTER** (provisioning only) |
| Custom auto-scaling policies | Yes (legacy) | No, use managed scaling |

Signals such as *"reduce Spot interruptions," "diversify instance types," "fall back to On-Demand"* → **instance fleets**.

## Storage: HDFS vs EMRFS vs local disk

| Layer | Durability | Use for |
|---|---|---|
| **HDFS** (core-node disks) | **Ephemeral**, gone at termination | Intermediate data, iterative re-reads, HBase on HDFS |
| **EMRFS** (`s3://`) | **Durable** (S3) | **Input and output of record.** **Decouples storage from compute** and enables transient clusters and Spot |
| Instance store / EBS | Ephemeral | Shuffle, spill, scratch (keep EBS utilization **< 90%**) |

- **EMRFS** is EMR's S3 connector. It handles S3 encryption and the S3-optimized Parquet committer.
- **EMRFS consistent view** (DynamoDB-tracked listings) is **obsolete**. S3 has been **strongly consistent since Dec 2020**, and consistent view hit **end of standard support June 1, 2023**. Answers that "enable consistent view" are distractors.
- Best practice: data in S3 as Parquet/ORC or Iceberg, catalog in the Glue Data Catalog, clusters as **disposable compute**.

## Lifecycle, steps and auto-termination

- **Transient** (launch → **steps** → terminate) suits scheduled batch ETL. **Long-running** suits notebooks, HBase, Trino endpoints and streaming, and needs scaling and idle controls.
- **Steps** (Spark app, Hive script, JAR, `command-runner.jar`) each carry **ActionOnFailure**: **TERMINATE_CLUSTER**, **CANCEL_AND_WAIT**, or **CONTINUE**. With step concurrency > 1, only CONTINUE is allowed.
- **Auto-termination policy:** idle timeout **default 60 min**, range **1 min to 7 days**, on by default for console clusters (6.4+). On 6.4+, idle means no YARN apps, HDFS < 10%, no notebook or UI connections, and no pending steps. **Not supported for non-YARN apps** (Presto, Trino, HBase).
- Orchestrate with Step Functions (`createCluster.sync`, `addStep.sync`), MWAA operators, or EventBridge Scheduler ([Guide 20](20-Step-Functions.md), [Guide 21](21-MWAA-Glue-Workflows.md)).

## Scaling

**EMR managed scaling** (5.30+, groups **and** fleets) is the least-effort answer. EMR resizes core and task capacity within your limits:

| Parameter | Meaning |
|---|---|
| `MinimumCapacityUnits` / `MaximumCapacityUnits` | Floor and ceiling (instances/vCPU for groups, units for fleets) |
| `MaximumOnDemandCapacityUnits` | On-Demand cap. Capacity above it comes from **Spot** |
| `MaximumCoreCapacityUnits` | Core cap. Capacity above it goes to **task** nodes |

It scales **YARN apps only** (Spark, Hive, Hadoop, Flink), **not Presto/Trino or HBase**. It's shuffle-aware and AM-aware, and **node labels** (7.x) keep drivers on On-Demand/core nodes. Leave Spark dynamic allocation on. **Custom automatic scaling** (CloudWatch rules on `YARNMemoryAvailablePercentage`, `ContainerPendingRatio`) is legacy and instance-group-only.

## Customizing clusters

- **Bootstrap actions** run scripts on every node **at launch, before apps start** (libraries, agents, files). A failure → **TERMINATED_WITH_ERRORS**.
- **Configuration classifications**: JSON overrides such as `spark-defaults`, `hive-site`, `core-site`, `yarn-site`, `emrfs-site`, `spark-hive-site`, `iceberg-defaults`.
- **Custom AMIs** pre-bake dependencies (faster than long bootstrap scripts) or hardened images.
- **Metastore:** the **Glue Data Catalog as Hive metastore** is serverless, persistent, and **shared with Athena, Glue, Spectrum and Lake Formation**. It's the default answer. An **external RDS/Aurora metastore** fits Hive-metastore compatibility needs or lifting an on-premises metastore. **THE trap:** the default on-cluster metastore **dies with a transient cluster** and "loses" the tables. See [Guide 13](13-Glue-Data-Catalog-Crawlers.md), and table formats in [Guide 04](04-Open-Table-Formats-S3-Tables.md).

## EMR Serverless

- **Application** = type **Spark or Hive** + release label. You submit **job runs** with a **job execution role**.
- **Auto-start** on submission. **Auto-stop** after **15 min idle** (defaults).
- **Maximum capacity** caps vCPU, memory and disk: your cost guard-rail.
- **Pre-initialized capacity** (`initialCapacity`) is a warm pool, so jobs start in **seconds**. Warm workers bill while idle, and jobs burst above the pool up to maximum capacity.
- Workers: **1–32 vCPU**, disk **20–200 GB** (only >20 GB billed), x86_64 or **arm64 (Graviton)**. Attach **VPC subnets** to reach private databases.
- Live **Spark UI / History Server** from the console. **Lake Formation FGAC** for Spark GA since **EMR 7.2 (Jul 2024)**.

| Choose **Glue** when… | Choose **EMR Serverless** when… |
|---|---|
| You want **DynamicFrames, job bookmarks**, Glue Studio visual ETL, Data Quality, crawlers, connectors, Flex | You want **plain open-source Spark/Hive** on an **EMR release**, custom images, fine Spark control |
| *"Least development effort"* | *"Migrate existing EMR Spark code without managing clusters"* |

## EMR on EKS

- **Virtual cluster** = EMR registered against **one Kubernetes namespace** (many per EKS cluster, no billable resources). **Job runs** go through `aws emr-containers start-job-run` with an execution role and a release label.
- **Job templates** hold reusable, governed job parameters. **Pod templates** customize driver/executor pods (node selectors, sidecars, drivers on On-Demand, executors on Spot).
- EC2 or **Fargate** capacity, several Spark versions side by side, **Lake Formation FGAC GA Feb 2025**. Signals: *"already standardized on Kubernetes," "share EKS capacity across teams."* See [Guide 18](18-Containers-Batch-EC2-Compute.md).

## EMR Studio and SageMaker Unified Studio

**EMR Studio** is a free managed **Jupyter IDE** (IAM or Identity Center sign-in) whose Workspaces attach to EMR on EC2, EMR on EKS, or EMR Serverless, with Git integration and schedulable parameterized notebooks. EMR Notebooks merged into it.

> 🆕 **New in exam guide v1.1:** **SageMaker Unified Studio** notebooks run on **EMR on EC2, EMR Serverless and EMR on EKS** (Nov 2025) alongside Glue, Athena and Redshift. *"One governed workspace for engineers, analysts and scientists"* → Unified Studio with EMR as compute ([Guide 41](41-SageMaker-Unified-Studio-Catalog-Governance.md)).

## Security

**Security configurations** are reusable named JSON objects for encryption, authentication and authorization. KMS depth is in [Guide 39](39-Encryption-Key-Management.md).

| Area | Options |
|---|---|
| At rest, EMRFS/S3 | **SSE-S3, SSE-KMS, CSE-KMS, CSE-Custom**, with per-bucket overrides. **SSE-C not supported.** CSE = encrypted **before** leaving the cluster (skill 4.3.4) |
| At rest, local disk | **EBS encryption (KMS)** for root + data volumes, or **LUKS**. HDFS RPC/block encryption is enabled too |
| In transit | **TLS** for app endpoints (certs from a PEM zip in S3 or a custom provider). EMRFS↔S3 always TLS |
| Authentication | **Kerberos** (cluster KDC, external KDC, AD cross-realm trust), LDAP for HiveServer2/Trino |
| Authorization | **Runtime roles**, **Lake Formation** FGAC, **Apache Ranger** plugins, S3 Access Grants |

**IAM roles:** the **service role** (`EMR_DefaultRole`) lets EMR provision EC2 and networking. The **EC2 instance profile** (`EMR_EC2_DefaultRole`) is what node code can access **by default**. **Runtime roles** give per-step least privilege on shared clusters and are **required for Lake Formation FGAC on EMR on EC2 (6.15+)**. EMR Serverless and EKS use a per-job **execution role**. **THE trap:** widening the instance profile on a multi-team cluster hands every team everyone's data. Use runtime roles plus Lake Formation ([Guide 40](40-Lake-Formation.md)).

**Network:** private subnets plus an **S3 gateway endpoint** ([Guide 38](38-Networking-for-Data-Pipelines.md)). **Block public access** is **on by default** per Region and refuses launches whose security groups allow `0.0.0.0/0` or `::/0` inbound, except listed ports (**22** by default). Reach UIs with SSH or Session Manager tunnels or persistent UIs, not open ports.

## Logs, metrics and troubleshooting

Set an S3 **log URI** (console does it by default, CLI/API don't). Under `s3://<log-uri>/<cluster-id>/`:
- `steps/<step-id>/` → **controller, syslog, stderr, stdout**. Start with **stderr**, then controller for load-time errors.
- `containers/` → Spark driver and executor logs (stack traces, OOM kills).
- `node/<instance-id>/` → **bootstrap-action** logs, daemons, YARN/HDFS logs.

Logs are archived **periodically, not in real time** (historically every **5 min**; 6.9+ also uploads on scale-down). **Persistent application UIs** (Spark History Server, Tez UI, YARN timeline) are **off-cluster**, need no tunnel, and last **30 days** after the app ends.

**CloudWatch** (`AWS/ElasticMapReduce`, free, **5-min**): **IsIdle** (alarm after ~30 min idle), **YARNMemoryAvailablePercentage** (low = memory-bound), **ContainerPendingRatio / AppsPending** (work waiting), **HDFSUtilization / CapacityRemainingGB**, **MRUnhealthyNodes** (often full disks), **MRLostNodes**, **MissingBlocks**.

| Symptom | Likely cause | Fix |
|---|---|---|
| "Container killed by YARN for exceeding memory limits" / OOM | Low `memoryOverhead`, skew, big `collect()` | Raise overhead, fewer cores per executor, fix skew ([Guide 16](16-Apache-Spark-Essentials.md)) |
| **TERMINATED_WITH_ERRORS** at launch | Bootstrap failure, bad classification, IAM/KMS, subnet IPs (**VALIDATION_ERROR**) | Read bootstrap logs under `node/` |
| Nodes UNHEALTHY, jobs stall | Local disk/EBS full | Bigger EBS, more nodes, output to S3 |
| HDFS full | Too little core capacity | Add **core** nodes (not task) or use S3 |
| Failures after nodes vanish | Spot reclaim | On-Demand core, fleets, node labels |
| S3 **503 Slow Down** | Hot prefix | Spread keys across prefixes, fewer output files, EMRFS retry/backoff |
| Idle cluster all weekend | No auto-termination | Idle timeout, IsIdle alarm, transient design |

Cross-service playbooks are in [Guide 32](32-Monitoring-Logging-Troubleshooting.md).

## Log analytics at scale (skills 3.3.8, 4.4.5)

For *"terabytes of application or VPC Flow logs per day"* with heavy parsing, joins or years of history, EMR (Spark/Hive/Trino over S3) is the heavy lifter. A typical flow: logs land in S3 (Firehose / CloudWatch Logs subscription), a transient Spark job enriches and compacts them into partitioned Parquet/Iceberg, and **Athena** or Quick Sight query the result. Ad hoc SQL on structured logs → **Athena**. Search and dashboards → **OpenSearch**. Quick log-group queries → **CloudWatch Logs Insights** ([Guide 43](43-Audit-Logging-CloudTrail-Config.md)).

## S3DistCp

`s3-dist-cp` (a step via `command-runner.jar`) is a distributed copier for **S3↔HDFS** and **S3→S3** (cross-bucket and cross-account). Key options: `--src/--dest`, `--srcPattern` (regex filter), **`--groupBy`** (regex capture group → **concatenate small files**), **`--targetSize`** (output size in **MiB**), `--outputCodec` (re-compress), `--deleteOnSuccess`, and manifests for incremental copies.

**THE trap:** S3DistCp concatenation is for **text/log files, not Parquet**. Compact Parquet with a Spark rewrite or Iceberg compaction.

Cost levers (Spot task nodes, fleets, Graviton, managed scaling, transient clusters, EMR Serverless, Savings Plans on the EC2 underneath) are in [Guide 44](44-Cost-Optimization.md).

## Decision table: where should this Spark job run?

| Signal | Best fit |
|---|---|
| *Least operational overhead*, visual ETL, bookmarks, crawlers | **AWS Glue** ([Guide 12](12-AWS-Glue-ETL.md)) |
| *Open-source Spark/Hive, no clusters*, sporadic, migrating EMR code | **EMR Serverless** |
| Full control, HBase/Trino/Flink, SSH, steady scale with Spot | **EMR on EC2** |
| Existing EKS platform, multi-tenant containers | **EMR on EKS** |
| Interactive PySpark notebooks, per-session serverless | **Athena for Apache Spark** ([Guide 26](26-Amazon-Athena.md)) |
| Plain SQL over S3 | **Athena SQL** |
| Real-time stateful streams | **Managed Service for Apache Flink** ([Guide 09](09-Managed-Service-for-Apache-Flink.md)) |

## Question patterns

> *"A nightly Spark job processes 20 TB in S3 for 3 hours. Minimize cost without risking failure from capacity loss."* → **Transient cluster: On-Demand primary and core, Spot task nodes in an instance fleet, terminate after the last step** (Spot on core/primary risks the whole job)

> *"Several business units share one long-running EMR cluster. Each must access only its own S3 prefixes and Lake Formation tables."* → **Runtime roles per step + Lake Formation fine-grained permissions** (a wider instance profile exposes everyone's data; separate clusters add overhead)

> *"Spot capacity for the chosen instance type is often unavailable, delaying launches."* → **Instance fleets with multiple types and subnets, price-capacity-optimized, Spot timeout SWITCH_TO_ON_DEMAND** (instance groups allow one type each)

> *"Hive tables created on transient clusters must survive termination and be queryable in Athena."* → **Glue Data Catalog as the Hive metastore** (the default on-cluster metastore dies with the cluster)

> *"Run open-source Spark jobs a few times a day, sizes unpredictable, no cluster management, start within seconds."* → **EMR Serverless with pre-initialized capacity and auto-stop** ("within seconds" is the pre-initialized signal)

> *"All microservices run on EKS; consolidate Spark there with per-team isolation."* → **EMR on EKS, one virtual cluster per team namespace**

> *"Scale core and task capacity with the least effort and cap On-Demand spend."* → **Managed scaling with MaximumOnDemandCapacityUnits** (custom policies are legacy, instance-group-only)

> *"EMRFS data must be encrypted before it leaves the cluster, keys in KMS."* → **Security configuration with CSE-KMS** (SSE-KMS encrypts at S3; SSE-C isn't supported)

> *"A cluster launch fails citing a security group rule open to 0.0.0.0/0 on port 8443."* → **EMR block public access: restrict the rule or use private subnets with tunnelling** (only port 22 is excepted by default)

> *"Engineers need the Spark UI for a job whose cluster terminated yesterday."* → **Persistent application UIs** (off-cluster, 30 days; no SSH tunnel)

> *"Spark steps fail with 'Container killed by YARN for exceeding memory limits.'"* → **Raise spark.executor.memoryOverhead / reduce cores per executor; check skew** (adding task nodes doesn't raise per-container limits)

> *"Millions of small gzip log files slow a Hive job; combine them into ~256 MiB hourly files first."* → **S3DistCp --groupBy on the hour with --targetSize=256** (for Parquet, compact with Spark instead)

> *"A dev cluster sits unused nights and weekends."* → **Auto-termination idle timeout** (default 60 min, 1 min–7 days; not for Trino/HBase-only clusters)

> *"A 24/7 HBase + Hive cluster must survive primary-node failure."* → **3 primary nodes with an external metastore (and external KDC if Kerberized)**

> *"Petabytes of VPC Flow Logs must be parsed, joined with reference data and aggregated daily at the lowest cost."* → **Transient EMR Spark with Spot task nodes writing partitioned Parquet, queried with Athena** (skill 4.4.5; OpenSearch at this batch volume costs far more)

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Open-source control, HBase/Trino/Flink | EMR on EC2 |
| Open-source Spark, no clusters | EMR Serverless |
| Spark on existing Kubernetes | EMR on EKS (virtual cluster = namespace) |
| Least-effort ETL, bookmarks | Glue |
| Primary node | 1 or 3 (HA), On-Demand |
| Core node | Compute + HDFS, On-Demand |
| Task node | Compute only → Spot |
| Spot on primary/core, critical job | THE trap |
| Multiple types, Spot + On-Demand mix | Instance fleets (5 / 30 types) |
| Spot unavailable at launch | Fleet timeout → SWITCH_TO_ON_DEMAND |
| Durable storage, decoupled compute | EMRFS on S3 |
| Consistent view | Obsolete (S3 strongly consistent) |
| emr-spark-8.0.0 | Spark 4.0.2, S3A replaces EMRFS |
| Run then shut down | Transient cluster + steps |
| Idle cost | Auto-termination (60 min default; 1 min–7 days) |
| Step failure behaviour | TERMINATE_CLUSTER / CANCEL_AND_WAIT / CONTINUE |
| Least-effort autoscaling | Managed scaling (Min/Max/MaxOnDemand/MaxCore) |
| Managed scaling excludes | Presto/Trino, HBase |
| Install software on all nodes | Bootstrap action |
| Override spark-defaults/hive-site | Configuration classifications |
| Shared persistent metastore | Glue Data Catalog |
| Serverless jobs start in seconds | Pre-initialized capacity |
| Serverless idle default / cost cap | Auto-stop 15 min / maximum capacity |
| Encrypt before leaving cluster | CSE-KMS / CSE-Custom |
| SSE-C | Not supported |
| Per-job permissions, shared cluster | Runtime roles (+ Lake Formation) |
| Launch blocked by public SG rule | Block public access (22 excepted) |
| Step error details | log URI → steps/<id>/stderr |
| Spark UI after termination | Persistent app UIs (30 days) |
| Container killed for memory | Raise memoryOverhead |
| Nodes UNHEALTHY | Disk full (MRUnhealthyNodes) |
| Compact small log files | S3DistCp --groupBy --targetSize |
| Compact Parquet | Spark rewrite / Iceberg compaction |

EMR's engine is almost always Spark, so the tuning that decides whether those clusters fly or fall over is next in [Guide 16 — Apache Spark Essentials](16-Apache-Spark-Essentials.md).
