# 18 · Containers, AWS Batch & EC2 Compute — climbing the compute ladder

> **Exam map:** D1 · Task 1.2 · **Skills:** 1.2.1, 1.2.4, 1.2.5 · **Weight:** 🔥 Low · **Read time:** ~14 min

## The idea

Choosing compute for a data job is like **choosing how to get a meal**. A vending machine (**Lambda**) is instant and needs zero effort, but only sells small things. A catered buffet (**AWS Glue**, **EMR Serverless**) serves a big crowd from a fixed menu (Spark, Python) with nobody from your side in the kitchen. A rented commercial kitchen (**EMR on EC2**, **ECS/EKS**, **AWS Batch**) lets you cook anything, but you choose the ovens and staff the shifts. Building your own restaurant (**raw EC2**) gives total control and total responsibility.

Each rung up the **ladder** trades **less operational overhead** for **more control**. The exam's favorite qualifiers — *"least operational overhead," "most cost-effective," "custom container," "runs for hours," "GPU," "existing Kubernetes platform"* — tell you which rung to stand on.

Two acronyms first: a **container** packages code plus dependencies into an image that runs the same anywhere; an **orchestrator** (Amazon **ECS** — Elastic Container Service, or Amazon **EKS** — Elastic Kubernetes Service) decides where containers run, restarts them, and scales them. **Fargate** is the serverless engine that runs those containers without you managing servers. This guide lets you crack *which compute*, *task role vs execution role*, *how to make containers faster and cheaper*, and *when AWS Batch beats Glue or Lambda*.

## The compute ladder

| Rung | Runs | Max duration | You manage | Pick when |
|---|---|---|---|---|
| **Lambda** | Functions (zip or image ≤ 10 GB) | **15 min** | Code only | Small, event-driven, short tasks ([Guide 17](17-Lambda-for-Data-Pipelines.md)) |
| **AWS Glue** | Serverless Spark / Python shell | Hours (job timeout configurable) | Scripts + DPUs | Standard ETL on S3/JDBC/catalog, *least ops* ([Guide 12](12-AWS-Glue-ETL.md)) |
| **EMR Serverless** | Spark, Hive | Hours | App + job config | Open-source Spark/Hive without clusters ([Guide 15](15-Amazon-EMR.md)) |
| **EMR on EC2** | Hadoop ecosystem, Spark, Trino, HBase, Flink | Unlimited | Cluster, instance types, scaling | Custom frameworks, long-running clusters, fine tuning, Spot fleets |
| **EMR on EKS** | Spark on your EKS cluster | Unlimited | EKS + virtual clusters | Consolidate Spark onto a shared Kubernetes platform |
| **AWS Batch** | Containerized batch jobs on EC2/Spot/Fargate (or EKS) | Unlimited | Job definitions, queues, compute environments | Many independent jobs, **custom containers**, GPUs, HPC, array/fan-out jobs |
| **ECS on Fargate** | Containers, serverless | Unlimited | Task definitions | Long-running or scheduled containers without servers |
| **ECS/EKS on EC2** | Containers on your instances | Unlimited | Instances, AMIs, scaling | GPUs, special instance types, maximum Spot/Reserved savings, daemons |
| **EC2** | Anything | Unlimited | Everything | Legacy/licensed software, full OS control |

Decision shortcuts:
- *"Spark ETL, least operational overhead"* → **Glue** (or **EMR Serverless** if the stem says EMR/open-source versions).
- *"Existing Docker image, runs 2 hours nightly, no servers"* → **ECS on Fargate** (scheduled task) or **AWS Batch on Fargate**.
- *"Thousands of independent jobs, queueing, retries, dependencies, Spot"* → **AWS Batch**.
- *"Company already runs Kubernetes and wants Spark on it"* → **EMR on EKS**.
- *"Custom OS-level agents / licensed software pinned to hosts"* → **EC2**.

**THE trap:** picking EC2 or self-managed EKS because it's "flexible" when the question says *least operational overhead*. Flexibility is rarely the asked-for property; managed/serverless rungs win unless a hard requirement (GPU, custom runtime, >15 min, Kubernetes standard) pushes you up.

## Amazon ECR — the image warehouse

**Amazon Elastic Container Registry** stores images for ECS, EKS, Batch and Lambda.
- **Image scanning:** basic scanning (on push, CVE database) or **enhanced scanning with Amazon Inspector** (continuous, OS + language packages).
- **Lifecycle policies** expire old/untagged images (cost and clutter control).
- **Replication** across Regions and accounts; **pull through cache** for upstream public registries; **tag immutability** to stop overwrites; KMS encryption.
- Access from private subnets needs **VPC endpoints** (`ecr.api`, `ecr.dkr`, plus the **S3 gateway endpoint** for image layers) or a NAT gateway.

## Amazon ECS — tasks, services and the two roles

| Concept | Meaning |
|---|---|
| **Task definition** | Blueprint: image(s), CPU/memory, ports, env vars/secrets, volumes, log config, **IAM roles** |
| **Task** | A running instance of a task definition — use for **batch/one-off** work |
| **Service** | Keeps N tasks running, replaces failed ones, integrates with load balancers and **Service Auto Scaling** |
| **Launch types / capacity** | **Fargate** (serverless), **EC2** (your Auto Scaling group via a capacity provider), and **ECS Managed Instances** (AWS-managed EC2 capacity, 2025) |
| **Capacity providers** | Strategy across **FARGATE**, **FARGATE_SPOT**, or ASG providers — e.g., base on Fargate, burst on Fargate Spot |
| **Scheduled tasks** | **EventBridge Scheduler** (or rules) runs a task on cron/rate — the serverless "cron job" |

**THE trap — task role vs task execution role:**

| Role | Used by | Needs permissions to |
|---|---|---|
| **Task execution role** | The **ECS agent / Fargate** on your behalf | **Pull the image from ECR**, **write logs to CloudWatch Logs**, fetch secrets referenced in the task definition |
| **Task role** | **Your application code** inside the container | Call AWS APIs: read S3, write DynamoDB, start Glue jobs, etc. |

*"Container fails to start: can't pull image / CannotPullContainerError"* → execution role (or networking to ECR). *"The app gets AccessDenied reading S3"* → **task role**. Never bake access keys into images.

Fargate facts: tasks up to **16 vCPU / 120 GB memory**; **20 GiB** ephemeral storage by default, configurable up to **200 GiB**; **EFS volumes** for shared persistent storage; **Fargate Spot** runs interruptible tasks at a steep discount with a **2-minute warning** (SIGTERM). Graviton (ARM64) supported.

## Amazon EKS — Kubernetes, managed

- **Pods** run on **managed node groups** (EC2 in ASGs, AWS handles provisioning/updates), **self-managed nodes**, **Fargate profiles** (pods matching a namespace/label run serverless), or **EKS Auto Mode** (AWS manages compute, networking and storage add-ons).
- **Karpenter** — open-source node autoscaler that launches right-sized instances for pending pods (including Spot and Graviton) and **consolidates** underused nodes to cut cost.
- **Pod permissions:** **IRSA** (IAM Roles for Service Accounts, OIDC-based) or the newer, simpler **EKS Pod Identity** — give each workload its own IAM role instead of the node's role.
- Data platform uses: **EMR on EKS** (Spark jobs in virtual clusters with per-job isolation), Airflow/Spark operators, and Flink.

**THE trap:** granting S3 access through the **node instance role** so "every pod can read the lake." Least privilege = **per-pod role** via IRSA or Pod Identity (same idea as the ECS task role).

## Optimizing container usage (1.2.1) and cost (1.2.4)

| Lever | How it helps |
|---|---|
| **Right-size CPU/memory** | Match task/pod requests to observed usage (Container Insights, Compute Optimizer); over-requesting wastes money, under-requesting causes OOM kills and throttling |
| **Scale horizontally on the right signal** | ECS Service Auto Scaling / Kubernetes HPA on CPU, or on **queue backlog per task** (SQS `ApproximateNumberOfMessagesVisible` ÷ running tasks) for worker pools; KEDA-style event scaling on EKS |
| **Spot / Fargate Spot** | 60–90% cheaper for **fault-tolerant, retryable, checkpointed** batch work; diversify instance types; handle the 2-minute notice |
| **Graviton (arm64)** | Better price-performance; build **multi-architecture images** (manifest lists) so the same tag runs on x86 and ARM |
| **Smaller images** | Slim base images and multi-stage builds → faster pulls and task start; pull-through cache/ECR in-Region |
| **Placement** | **binpack** (pack tasks onto fewest instances — cost) vs **spread** (across AZs/instances — availability) on ECS EC2 |
| **Storage choice** | Fargate ephemeral storage up to 200 GiB for scratch; **EFS** for shared inputs/outputs; S3 for durable data |
| **Consolidation** | Karpenter consolidation / right-sized nodes remove idle capacity |
| **Job isolation for Spark** | EMR on EKS gives each Spark job its own pods and config on shared nodes |

## AWS Batch — the job dispatcher

**AWS Batch** runs **containerized batch jobs** at any scale: you submit jobs, Batch queues them, provisions compute, runs, retries, and scales to zero.

| Component | What to know |
|---|---|
| **Job definition** | Container image, vCPU/memory/GPU, command, env, IAM job role, retry strategy, timeout |
| **Job queue** | Jobs wait here; queues have **priority** and map to one or more compute environments (order matters); **fair-share scheduling** policies share capacity between teams |
| **Compute environment** | **Managed** EC2 On-Demand, **EC2 Spot**, **Fargate**, **Fargate Spot** (or EKS); min/desired/**max vCPUs**; unmanaged if you bring your own instances |
| **Allocation strategies** | `BEST_FIT` (cheapest fitting type, may wait), `BEST_FIT_PROGRESSIVE` (widen to other types), `SPOT_CAPACITY_OPTIMIZED` (deepest Spot pools, fewer interruptions), **`SPOT_PRICE_CAPACITY_OPTIMIZED`** (low price *and* low interruption risk — AWS's recommended Spot choice) |
| **Array jobs** | One submission fans out to **2–10,000 child jobs**, each with `AWS_BATCH_JOB_ARRAY_INDEX` to pick its slice (e.g., one file/partition per child) |
| **Dependencies** | A job can depend on others (`SEQUENTIAL` or `N_TO_N` for arrays) — simple DAGs |
| **Retries** | `attempts` plus `evaluateOnExit` rules (e.g., retry on Spot reclaim, fail on app error) |
| **Multi-node parallel** | One job spanning multiple EC2 instances (MPI/HPC, distributed training) |
| **Orchestration** | Step Functions **`batch:submitJob.sync`** waits for completion ([Guide 20](20-Step-Functions.md)); EventBridge for job state changes |

**Batch vs Glue vs Lambda:**
- **Lambda** — seconds to 15 minutes, event-driven, tiny.
- **Glue** — Spark/Python ETL with catalog, bookmarks, connectors; nothing to provision.
- **Batch** — **arbitrary containers** (any language or binary: R, Java tools, genomics, FFmpeg, GPU inference), long runtimes, huge fan-outs, Spot-heavy cost control.

**THE trap:** using Batch for a Spark ETL job over S3 that Glue handles natively — more to build. And using Glue for a non-Spark legacy binary packaged as a Docker image — Glue can't run arbitrary containers; **Batch** (or ECS) can.

## Amazon EC2 for data work

**Instance families** (the letter tells you the bottleneck it solves):

| Family | Optimized for | Data use |
|---|---|---|
| **R** (and X) | Memory | Spark executors, caching, big joins, in-memory analytics |
| **C** | Compute | CPU-heavy transforms, compression, parsing |
| **M** | Balanced | General workers, EMR primary nodes |
| **I** | Local NVMe SSD, high IOPS | Shuffle-heavy Spark, HDFS, databases |
| **D** | Dense HDD storage | HDFS/data warehouse on local disks, high sequential throughput |
| **P / G** (and Trainium/Inferentia) | GPUs/accelerators | ML training/inference, GPU data processing |

A **"g" suffix** (e.g., r7**g**, c7**g**) = **Graviton** (arm64) — typically the best price-performance for Spark and JVM workloads.

**Purchasing:** **On-Demand** (flexible), **Savings Plans / Reserved** (steady 1–3 year baselines), **Spot** (up to ~90% off, **2-minute interruption notice**, plus rebalance recommendations). Spot suits **stateless, retryable, checkpointed** work: EMR task nodes, Batch jobs, Spark executors — not the EMR primary node or a single long job with no checkpoints. Cost levers across services: [Guide 44 — Cost Optimization](44-Cost-Optimization.md).

**Storage:**

| Volume | Profile | Use |
|---|---|---|
| **EBS gp3** | General SSD; **baseline 3,000 IOPS and 125 MiB/s regardless of size**, IOPS and throughput provisioned independently | Default boot/data volumes; cheaper than gp2 |
| **EBS io2 (Block Express)** | Highest IOPS, sub-millisecond, highest durability | Critical databases |
| **EBS st1** | Throughput-optimized HDD, big sequential reads | Log processing, sequential scans (not boot) |
| **EBS sc1** | Cold HDD, cheapest | Infrequently accessed sequential data |
| **Instance store** | Physically attached NVMe, **lost on stop/terminate** | Scratch, **Spark shuffle/spill**, caches, HDFS replicas |

**Networking:** a **cluster placement group** packs instances close together in **one AZ** for low-latency, high-throughput node-to-node traffic (tightly coupled HPC/shuffle-heavy clusters); **spread** placement separates critical instances onto distinct hardware.

**THE trap:** storing data you need later on **instance store**. It is ephemeral — durable data goes to S3 (or EBS); instance store is for scratch and shuffle.

## Question patterns

> *"A team has a Python data-cleansing tool packaged as a Docker image that runs for about 2 hours each night. They want no servers to manage and the lowest ops effort."* → **ECS scheduled task on Fargate via EventBridge Scheduler** (containers beyond Lambda's 15 minutes; no cluster instances).

> *"Genomics researchers submit thousands of independent containerized jobs per day with priorities and retries, and want to minimize cost."* → **AWS Batch with Spot compute environments using SPOT_PRICE_CAPACITY_OPTIMIZED, job queues with priorities** (queueing + retries + Spot, managed).

> *"Process 5,000 files in S3 with the same containerized program, one file per job."* → **AWS Batch array job (size 5,000) using AWS_BATCH_JOB_ARRAY_INDEX** (one submission, fan-out).

> *"An ECS task fails with CannotPullContainerError when pulling from ECR in a private subnet with no NAT gateway."* → **Add ECR API and DKR interface endpoints plus an S3 gateway endpoint (and check the task execution role)** (image pulls need a network path and execution-role permissions).

> *"A containerized ETL app on ECS gets AccessDenied when writing to S3."* → **Grant S3 permissions on the task role** (execution role is only for image pulls, logs and secrets).

> *"Pods in a shared EKS cluster need different S3 permissions per workload following least privilege."* → **EKS Pod Identity (or IRSA) with a role per service account** (not the node instance role).

> *"The company standardizes on Kubernetes and wants to run Spark jobs on its existing EKS clusters with per-job isolation."* → **Amazon EMR on EKS** (Spark on shared EKS nodes; Glue/EMR Serverless don't use your cluster).

> *"A worker service on ECS consumes an SQS queue; backlog grows during peaks while CPU stays low."* → **Target-tracking scaling on backlog per task (queue depth ÷ running tasks)** (CPU is the wrong signal for queue workers).

> *"Reduce the cost of fault-tolerant, checkpointed container batch jobs on ECS without changing code."* → **Capacity provider strategy using FARGATE_SPOT (with a FARGATE base)** (interruptible discount; checkpointing tolerates the 2-minute notice).

> *"Spark jobs on EMR on EC2 spill heavily to disk during shuffles and run slowly."* → **Use instance types with local NVMe instance store (or memory-optimized R instances) for core/task nodes** (fast scratch for shuffle; more memory reduces spill).

> *"A tightly coupled simulation across 20 EC2 instances needs the lowest inter-node latency."* → **Cluster placement group in one AZ (with enhanced networking)** (spread placement maximizes separation, not speed).

> *"Improve price-performance of containerized Java data services on Fargate."* → **Build multi-arch images and run on Graviton (ARM64)** (same image tag, cheaper compute).

> *"A legacy ETL binary requires a specific Linux kernel module and runs continuously."* → **EC2 instances (in an Auto Scaling group)** (OS-level control rules out Fargate/Lambda/Glue).

> *"Old untagged images are accumulating in ECR and increasing storage cost."* → **ECR lifecycle policy expiring untagged images** (automated cleanup).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| < 15 min, event-driven | Lambda |
| Spark ETL, least ops | Glue (or EMR Serverless) |
| Custom frameworks, long-lived cluster, tuning | EMR on EC2 |
| Spark on existing Kubernetes | EMR on EKS |
| Containerized batch, queues, retries, Spot | AWS Batch |
| Fan-out one program over N inputs | Batch array job (2–10,000 children) |
| Job B after job A in Batch | Job dependencies (SEQUENTIAL / N_TO_N) |
| Spot allocation, low price + low interruption | SPOT_PRICE_CAPACITY_OPTIMIZED |
| Wait for Batch job in a workflow | Step Functions batch:submitJob.sync |
| Serverless containers | Fargate (ECS or EKS) |
| Container on a schedule | ECS scheduled task via EventBridge Scheduler |
| Pull image / write logs / fetch secrets | Task execution role |
| App calls AWS APIs from container | Task role |
| Per-pod IAM on EKS | EKS Pod Identity / IRSA |
| Pods serverless on EKS | Fargate profile (or EKS Auto Mode) |
| Right-sized nodes + consolidation | Karpenter |
| Fargate size / storage | 16 vCPU / 120 GB; 20 GiB default → 200 GiB ephemeral |
| Shared files across tasks | EFS volume |
| Cheap interruptible containers | Fargate Spot / EC2 Spot (2-min notice) |
| Queue-worker scaling | Backlog per task (SQS depth ÷ tasks) |
| Pack for cost vs survive failures | binpack vs spread |
| ARM price-performance | Graviton + multi-arch images |
| Faster task start | Smaller images, in-Region ECR |
| Vulnerability scanning | ECR enhanced scanning (Inspector) |
| Clean up old images | ECR lifecycle policy |
| Images across Regions/accounts | ECR replication |
| Private subnet pulls | ECR interface endpoints + S3 gateway endpoint |
| Memory-heavy Spark | R instances |
| CPU-heavy | C instances |
| Shuffle/scratch, fast local disk | I instances / instance store (ephemeral) |
| Dense sequential local storage | D instances / st1 |
| General SSD volume | gp3 (3,000 IOPS / 125 MiB/s baseline, tunable) |
| Highest IOPS database volume | io2 Block Express |
| Low-latency node-to-node | Cluster placement group (one AZ) |
| OS-level control, licensed software | EC2 |

With compute chosen, the next step is wiring those jobs together into dependable pipelines — Step Functions, MWAA and EventBridge — starting with [Guide 20 — Step Functions](20-Step-Functions.md).
