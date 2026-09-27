# 46 · Gap-Fill Services — the spice rack

> **Exam map:** Cross-domain (D1 · 1.1, 1.2, 1.4 — D2 · 2.1, 2.3, 2.4 — D3 · 3.3 — D4 · 4.1–4.5) · **Skills:** 1.1.4, 1.2.8, 1.4.7, 2.1.4, 2.3.6, 2.4.4, 4.1.3, 4.2.2, 4.5.1, 4.5.7 (as they touch these services) · **Weight:** 🔥 Low · **Read time:** ~12 min

## The idea

Every kitchen has a **spice rack**: dozens of small jars you use a pinch at a time. Nobody writes an essay about cumin, but if a recipe says *"earthy, warm, goes in chili"* you'd better grab the right jar. DEA-C01 treats a long tail of services the same way. They rarely anchor a whole scenario, but they appear as **one-keyword answers** (*"share third-party data"* → Data Exchange; *"store config values cheaply"* → Parameter Store) or as **tempting wrong answers** (*"AWS Data Pipeline"*, *"S3 Select"*).

This guide is the rack, one tight entry per jar: **signal → service → THE trap**. It ends with a **red-flag distractor table** of legacy and out-of-scope services and the modern answer each stands in for. The big services each have their own guide; start with [Guide 45](45-Service-Selection-Decision-Guide.md) for the decision matrices.

## Data sharing & APIs

**Amazon API Gateway** is a managed front door for REST, HTTP and WebSocket APIs, used to **expose data** (API → Lambda → DynamoDB/Athena/Redshift Data API) or **accept pushes** (API → Kinesis/SQS via direct service integration). It handles throttling, usage plans with API keys, IAM/Cognito/Lambda authorizers, caching, and **WAF** attachment. **THE trap:** *"make curated data available to partner applications with the least overhead"* → API Gateway + Lambda (or direct integration), not an EC2 web server. Depth: [Guide 36](36-Programming-IaC-CICD.md).

**AWS Data Exchange** 🆕 lets you **find, subscribe to and use third-party data** (or share your own) without building delivery infrastructure.
- **Data set types:** **Files** (S3 objects in **revisions**, which subscribers can **auto-export to their S3 bucket** when a new revision is published), **APIs** (call a provider's API via API Gateway), **Amazon Redshift datashares** (query the provider's live tables, read-only, no ETL), **Amazon S3 data access** (read the provider's S3 objects in place, no copy), and **AWS Lake Formation data permissions** (in preview).
- **Concepts:** **providers** publish **products** (containing data sets, with **offers**: price, duration, terms) in **AWS Marketplace**. **Subscribers** receive **entitlements** to the data sets. **Data grants** let *any* AWS account share a data set directly with a named receiver account for a set period, **without listing a product in Marketplace**. Open Data on AWS sets are also discoverable.
- **THE trap:** *"subscribe to a vendor's weather/financial feed and query it in Redshift without copying"* → **Data Exchange for Amazon Redshift (datashare)**. Building an SFTP pipeline from the vendor is the distractor.

> 🆕 **New in exam guide v1.1:** AWS Data Exchange joined the in-scope list, tied to skill 4.5.7 (data sharing patterns). Sharing-pattern comparisons are in [Guide 41](41-SageMaker-Unified-Studio-Catalog-Governance.md) and [Guide 24](24-Redshift-Loading-Integration-Sharing.md).

**AWS Clean Rooms** (not in the exam's list; one line): several parties analyze combined datasets **without sharing raw data**, using analysis rules. **AWS Entity Resolution** (not in the list): match and link records about the same entity across sources (rule-based or ML). For fuzzy dedup *inside ETL*, the exam's answer is **Glue FindMatches**.

## AWS Systems Manager — the operations toolbox

| Capability | Signal | Notes |
|---|---|---|
| **Parameter Store** | *"store configuration values / connection strings / feature flags centrally"* | **Standard** tier: free, up to **10,000** parameters, **4 KB** values. **Advanced**: up to **100,000**, **8 KB**, **parameter policies** (expiration, notifications), charged per parameter. **SecureString** encrypts with **KMS**. Hierarchies (`/prod/etl/db_host`), versioning |
| **Run Command** | *"run a script on a fleet of EC2/on-prem instances without SSH"* | Documents (SSM documents), rate control, output to S3/CloudWatch |
| **Session Manager** | *"shell access without bastion hosts, open inbound ports or SSH keys; sessions audited"* | IAM-controlled; logs to S3/CloudWatch |
| **Automation (runbooks)** | *"automated remediation / multi-step ops tasks"* | Triggered by EventBridge or alarms; AWS-provided runbooks (e.g. EMR log diagnosis) |
| **Patch Manager** | *"patch EC2 fleets on a schedule"* | Patch baselines, maintenance windows |
| **OpsCenter** | Aggregate operational issues (OpsItems) | CloudWatch alarms can create OpsItems |

**THE trap:** *"database password must rotate automatically every 30 days"* → **Secrets Manager** (built-in rotation with Lambda). **Parameter Store has no native rotation**, even for SecureString. Parameter Store wins on *"cheapest place for non-secret config"* or *"plain config plus a few encrypted values, no rotation needed."* Depth: [Guide 39](39-Encryption-Key-Management.md).

> ⚠️ **2026 status:** Systems Manager **Change Manager** and **Incident Manager** have been **closed to new customers since Nov 7, 2025** (existing customers continue; AWS suggests OpsCenter for incident-style tracking). Treat them as recognizable but unlikely answers.

## Storage odds and ends

**AWS Backup** centrally manages **backup plans** (schedule, retention, lifecycle to cold storage), **vaults** (with **Vault Lock** for WORM), **cross-Region and cross-account copies**, and audit reports. It covers EBS, EFS, RDS/Aurora, DynamoDB, S3, Redshift, FSx, EC2 and more. Signal: *"centralized, policy-based backups across services and accounts, with immutable retention"* → **AWS Backup** (+ Organizations backup policies). Depth: [Guide 31](31-Data-Lifecycle-Retention-Resiliency.md).

**Amazon EBS** is block storage for **one EC2 instance** (per AZ):

| Type | Use |
|---|---|
| **gp3** (general-purpose SSD) | Default. Baseline **3,000 IOPS / 125 MiB/s regardless of size**; IOPS and throughput provisioned **independently** of capacity (cheaper than gp2) |
| **io2 Block Express** (provisioned-IOPS SSD) | Highest IOPS and sub-millisecond latency for demanding databases; Multi-Attach |
| **st1** (throughput HDD) | Big **sequential** reads (log processing, Kafka/HDFS-style) at low cost; not bootable |
| **sc1** (cold HDD) | Cheapest; rarely accessed sequential data |

**THE trap:** *"migrate gp2 volumes to cut cost without losing performance"* → **gp3** (performance decoupled from size).

**Amazon EFS** is a **shared, elastic NFS file system** that many EC2 instances, containers and **Lambda functions** can mount at once.
- Throughput modes: **Elastic** (recommended, scales automatically, pay per use), **Provisioned**, **Bursting**.
- Storage classes: **Standard, Infrequent Access, Archive**, with lifecycle management; **One Zone** variants are cheaper.
- **Lambda + EFS:** the function must be **in a VPC** and mount via an **EFS access point**. Signal: *"Lambda needs large reference files or ML models beyond /tmp, shared across invocations"* (skill 1.4.7). Lambda `/tmp` goes up to 10 GB but isn't shared.
- **THE trap:** a shared file system for many instances → EFS, not EBS (one instance, Multi-Attach only for io1/io2 in one AZ). Depth: [Guide 17](17-Lambda-for-Data-Pipelines.md).

**S3 Glacier** storage classes, S3 Tables and S3 Vectors are covered in [Guides 05](05-S3-Data-Lake-Storage.md) and [04](04-Open-Table-Formats-S3-Tables.md).

## Networking & edge

- **Amazon CloudFront** is a CDN with edge caching for downloads and APIs. **Origin Access Control (OAC)** makes an S3 origin reachable **only through CloudFront**; OAI is the legacy version. Signal: *"serve report files globally with low latency, bucket not public."*
- **Amazon Route 53**: DNS, health checks, routing policies. **Private hosted zones** give internal names inside VPCs. **Resolver inbound/outbound endpoints** handle **hybrid DNS** (on-prem resolving AWS private names and vice versa). Signal: *"on-prem ETL servers must resolve the RDS private endpoint"* → Resolver inbound endpoint.
- **AWS WAF**: layer-7 web ACLs on **CloudFront, ALB, API Gateway**, with managed rule groups, **rate-based rules** (throttle abusive IPs), IP sets (**allowlists/denylists**), and SQL-injection/XSS rules. Signal: *"protect the data API from SQL injection and floods from single IPs."*
- **AWS Shield**: **Standard** is free, automatic L3/L4 DDoS protection. **Advanced** is paid, with enhanced detection, the AWS Shield Response Team, and **DDoS cost protection**.

Depth: [Guide 38](38-Networking-for-Data-Pipelines.md).

## Migration helpers

- **AWS Application Migration Service (MGN)**: **rehost (lift-and-shift) servers** to EC2 via continuous block-level replication, with minimal cutover downtime. Signal: *"move on-prem application servers (including a self-managed DB VM) as-is."* For **database-level** migration with CDC, the answer is DMS ([Guide 10](10-DMS-Database-Ingestion.md)).
- **AWS Application Discovery Service**: inventories on-prem servers, utilization and dependencies (agent or agentless) to plan migrations.

> ⚠️ **2026 status:** Application Discovery Service has been in **maintenance** (no new customers) since Nov 7, 2025, but it's still on the v1.1 in-scope list. Recognize it as the *"discover on-prem servers and dependencies before migrating"* answer.

## Developer & governance odds and ends

- **AWS CodeDeploy**: automated deployments to EC2/on-prem, Lambda (traffic shifting: canary/linear) and ECS (blue/green). Signal: *"shift Lambda traffic gradually with automatic rollback on alarms."* Depth: [Guide 36](36-Programming-IaC-CICD.md).
- **AWS Well-Architected Tool**: free architecture reviews using lenses (e.g. the **Data Analytics Lens**), with high-risk-issue lists, improvement plans and milestones. See [Guide 45](45-Service-Selection-Decision-Guide.md).
- **Amazon Managed Grafana**: managed Grafana workspaces for operational dashboards (CloudWatch, Prometheus, OpenSearch, Athena, Redshift), with SSO via IAM Identity Center/SAML. See [Guide 32](32-Monitoring-Logging-Troubleshooting.md).
- **AWS Organizations / SCPs / Control Tower** (not listed as exam services, but they show up in governance questions): **SCPs** set permission guardrails across accounts (e.g. deny actions outside approved Regions with `aws:RequestedRegion`); **Control Tower** sets up a governed multi-account landing zone with preventive/detective controls. Depth: [Guide 42](42-Privacy-PII-Masking-Sovereignty.md), [Guide 37](37-IAM-for-Data-Engineers.md).

## The AI shelf

**Amazon Q family** 🆕:
- **Amazon Q Developer** is the AI assistant in IDEs, the CLI and the console: code generation, explanations, tests, refactoring. Its chat integration was formerly AWS Chatbot.
- **Amazon Q in AWS Glue** generates Glue ETL scripts from natural language and helps troubleshoot Spark jobs.
- **Amazon Q generative SQL in Redshift Query Editor v2** turns natural-language prompts into SQL.
- **Amazon Q in Quick Sight** is natural-language BI (see [Guide 35](35-Analytics-Visualization-Quick-Notebooks.md)).
- **Amazon Q in SageMaker Unified Studio** writes SQL/code in projects.
- Signal: *"accelerate writing ETL/SQL with natural language"* → **Amazon Q** (not Bedrock model training).

> ⚠️ **2026 status:** **Amazon Q Business** (the enterprise-knowledge assistant) is in **maintenance from Jul 30, 2026** (no new customers). Amazon Quick is the forward path. Q Developer and the embedded Q features continue.

**Amazon Kendra** 🆕 is ML-powered **enterprise search** over documents (connectors to S3, SharePoint, Confluence, databases...). It was used as a **retriever for RAG**.
> ⚠️ **2026 status:** Kendra is in **maintenance from Jul 30, 2026** (no new customers), but it's still in the v1.1 scope. For new RAG builds the modern answer is **Bedrock Knowledge Bases** with a vector store. Details in [Guide 19](19-GenAI-LLMs-Vectors.md).

**Amazon Bedrock** 🆕 is serverless access to foundation models via API: enrichment in pipelines, batch inference, knowledge bases, Bedrock Data Automation for documents/media. Details in [Guide 19](19-GenAI-LLMs-Vectors.md).

**SageMaker AI features a data engineer meets** (model training and inference are out of scope):

| Feature | Signal |
|---|---|
| **Feature Store** | *"serve the same features for training and real-time inference"*: **online store** (low-latency lookups) + **offline store** (S3, queryable with Athena) |
| **Processing jobs** | Run pre/post-processing or evaluation scripts (scikit-learn, Spark) on managed, ephemeral containers |
| **SageMaker Pipelines** | Orchestrate ML workflows (process → train → register); choose it for ML-specific steps, Step Functions/MWAA for general ETL |
| **ML Lineage Tracking** | Track lineage of data, jobs, models and endpoints (skill 2.4.4); **SageMaker Catalog lineage** covers data assets ([Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md)) |
| **Data Wrangler (in SageMaker Canvas)** / **Canvas** | Visual data prep for ML / no-code ML ([Guide 14](14-Glue-DataBrew-Data-Preparation.md)) |

> ⚠️ **2026 status:** SageMaker AI features in **maintenance from Jul 30, 2026**: A2I, Clarify, Debugger, Geospatial, Ground Truth, Model Monitor, Profiler, Role Manager, Studio Lab. Don't expect them as correct answers to new-style questions.

**AWS Glue FindMatches** is an ML transform you train with labeled match/no-match pairs. It finds **duplicate or linked records without a common key** (typos, abbreviations). Signal: *"deduplicate customer records with inconsistent names/addresses"* ([Guide 12](12-AWS-Glue-ETL.md)).

**Amazon Comprehend / Amazon Textract** aren't in the exam's service list, so they only show up as distractors or one-liners. Comprehend does NLP on text (entities, sentiment, PII detection). Textract extracts text, forms and tables from documents. In v1.1 framing, **LLM enrichment via Bedrock** (or Bedrock Data Automation for documents) is the in-scope answer.

## Red-flag distractors

Out-of-scope or legacy services that show up as wrong answers, and the modern answer each is standing in for:

| Distractor | Status | Choose instead |
|---|---|---|
| **AWS Data Pipeline** | Maintenance, closed to new customers (2024); out of scope | **Step Functions**, **MWAA** (or MWAA Serverless), **Glue workflows** |
| **Kinesis Data Analytics for SQL** | Discontinued (apps stopped Jan 27, 2026) | **Managed Service for Apache Flink** (Flink SQL / Studio) |
| **S3 Select / Glacier Select** | Closed to new customers (Jul 2024) | **Athena** (or client-side filtering / byte-range GETs) |
| **S3 Object Lambda** | Maintenance (Nov 2025) | Transform on write with Lambda/Glue, or serve via API Gateway + Lambda |
| **Amazon Glacier vaults** (the original service) | Maintenance (Nov 2025) | **S3 Glacier storage classes** + lifecycle |
| **AWS X-Ray** | Out of scope | **CloudWatch** metrics/logs/alarms for pipelines |
| **Elastic Beanstalk / App Runner / Lightsail** | Out of scope | **Lambda**, **ECS/EKS on Fargate**, **Glue/EMR** for data work |
| **AWS Amplify / AWS AppSync** | Out of scope | **API Gateway** for data APIs |
| **Amazon SES / Pinpoint** | Out of scope | **SNS** for alerts ([Guide 22](22-EventBridge-SNS-SQS.md)) |
| **AWS IoT services** (Events, SiteWise, FleetWise…) | Out of scope | **Kinesis Data Streams / Firehose / MSK** for device telemetry ingestion |
| **AWS Migration Hub** | Out of scope | **DMS**, **DataSync**, **Application Migration Service** |
| **AWS Outposts** | Out of scope | Region services; DataSync/Snow for moving data |
| **Snowmobile** (and Snowcone) | Retired | **Snowball Edge** (still exam-valid ⚠️) or **DataSync** |
| **AWS CodeCommit / AWS Cloud9** | Removed from scope in v1.1 (closed to new customers 2024) | **GitHub/GitLab via AWS CodeConnections**; IDE / SageMaker Unified Studio |
| **AWS SCT** (downloadable) | Removed from service list (still named in skill 2.4.3) | **DMS Schema Conversion** |
| **Glue Ray jobs** | Maintenance (Apr 30, 2026) | **Glue Spark** / **Python shell** jobs, EMR |
| **Timestream for LiveAnalytics** | Closed to new customers (Jun 2025) | **Timestream for InfluxDB**, or DynamoDB/Redshift for time-series analytics |
| **Amazon FinSpace** | Out of scope | Standard lake stack (S3 + Glue + Athena/Redshift) |
| **SNS Message Data Protection** | Maintenance (Apr 30, 2026) | **Macie** for S3, **CloudWatch Logs data protection** for logs, masking in ETL |
| **Amazon Q Business / Kendra** | Maintenance (Jul 30, 2026) | **Amazon Quick**, **Bedrock Knowledge Bases** (Kendra still in scope) |

**THE trap:** a distractor is often *technically capable* (Data Pipeline really could copy RDS to S3 on a schedule). The exam wants the **current, managed, in-scope** service. If you don't recognize a service as in-scope, it's probably there to be eliminated.

## Question patterns

> *"A hedge fund wants a vendor's daily market data available in its Redshift cluster with no ETL and no copies, paying via AWS billing."* → **AWS Data Exchange for Amazon Redshift (datashare subscription)**. An SFTP feed or S3 copy loses on "no copies".

> *"A company wants to share a curated dataset with a partner's AWS account for 90 days without listing a product publicly."* → **Data Exchange data grant** (or a Redshift/Lake Formation share if both sides already use them). Marketplace listing isn't needed.

> *"New revisions of a subscribed Data Exchange file product must land in the team's bucket automatically."* → **Enable auto-export of revisions to S3.**

> *"ETL jobs need a shared database hostname and a few non-secret settings, cheaply, with versioning."* → **Parameter Store (Standard) hierarchy** (Secrets Manager costs more and adds rotation they don't need).

> *"Redshift admin credentials must rotate every 30 days automatically."* → **Secrets Manager** (Parameter Store has no native rotation).

> *"Engineers need shell access to EMR/EC2 nodes in private subnets without bastions or open port 22, with sessions logged."* → **Systems Manager Session Manager.**

> *"A Lambda function must read a 5 GB reference dataset shared by all concurrent invocations."* → **Mount EFS via an access point (function in a VPC)**. `/tmp` isn't shared, and layers are capped at 250 MB unzipped.

> *"Cut EBS costs on gp2 volumes for Kafka brokers without reducing IOPS."* → **Migrate to gp3 and provision IOPS/throughput independently.**

> *"On-prem Airflow workers must resolve names in a Route 53 private hosted zone."* → **Route 53 Resolver inbound endpoint** (conditional forwarding from on-prem DNS).

> *"A public data API is being scraped by a few IPs sending thousands of requests per second."* → **AWS WAF rate-based rule on API Gateway** (Shield Standard handles network-layer DDoS, not per-IP L7 rate limits).

> *"Lift-and-shift 60 on-prem application servers to EC2 with minimal downtime."* → **Application Migration Service** (DMS is for databases; Data Pipeline is a distractor).

> *"Features must be identical for model training and a real-time fraud endpoint."* → **SageMaker Feature Store (offline + online stores).**

> *"Merge two customer databases with no shared ID and inconsistent spellings."* → **Glue FindMatches ML transform** (DISTINCT and exact joins miss near-duplicates).

> *"Schedule a daily copy from RDS to S3; options include AWS Data Pipeline."* → **Glue job or DMS triggered by EventBridge Scheduler / orchestrated by Step Functions** (Data Pipeline is closed to new customers and out of scope).

> *"Engineers want natural-language help generating Redshift SQL and Glue PySpark."* → **Amazon Q** (generative SQL in Query Editor v2, Q in Glue). Training a model in Bedrock is overkill.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Expose/accept data via API | API Gateway (+ Lambda / direct integration) |
| Subscribe to third-party data | AWS Data Exchange |
| Vendor data in Redshift, no copy | Data Exchange Redshift datashare |
| Vendor S3 data in place | Data Exchange for Amazon S3 |
| Share a data set with one account, no listing | Data Exchange data grant |
| New revisions → my bucket | Auto-export to S3 |
| Cheap central config | Parameter Store (Standard: 10,000 params, 4 KB) |
| Big params / expiry policies | Parameter Store Advanced (8 KB, policies) |
| Automatic secret rotation | Secrets Manager (not Parameter Store) |
| Script on fleet, no SSH | Run Command |
| Shell without bastion/port 22 | Session Manager |
| Automated remediation runbooks | Systems Manager Automation |
| Central policy-based backups, WORM vault | AWS Backup (+ Vault Lock) |
| Cheaper gp2 replacement | gp3 |
| Highest IOPS block storage | io2 Block Express |
| Big sequential throughput, cheap | st1 |
| Shared NFS for EC2/containers/Lambda | EFS (Lambda: VPC + access point) |
| S3 only via CDN | CloudFront + OAC |
| Hybrid DNS | Route 53 Resolver endpoints |
| SQLi / rate limiting on APIs | AWS WAF |
| DDoS protection, cost protection | Shield Advanced |
| Lift-and-shift servers | Application Migration Service |
| Discover on-prem servers/dependencies | Application Discovery Service ⚠️ |
| Gradual Lambda/ECS deploys | CodeDeploy |
| AI help writing SQL/ETL | Amazon Q (Developer, Glue, Redshift QEv2) |
| Enterprise document search (legacy RAG) | Kendra ⚠️ → Bedrock Knowledge Bases |
| Train/serve same features | SageMaker Feature Store |
| Managed preprocessing containers | SageMaker Processing |
| ML workflow orchestration | SageMaker Pipelines |
| Fuzzy dedup, no key | Glue FindMatches |
| Analyze combined data without sharing raw | Clean Rooms (outside exam list) |
| Region guardrails across accounts | SCPs (Organizations) / Control Tower |
| Data Pipeline in an answer | Distractor → Step Functions / MWAA / Glue workflows |
| S3 Select in an answer | Legacy → Athena |
| KDA for SQL in an answer | Legacy → Managed Flink |

The spice rack is complete. For the greatest-hits list of traps across every guide, finish with [Guide 47 — Exam Traps & Key Patterns](47-Exam-Traps-Key-Patterns.md).
