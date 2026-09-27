# 38 · Networking for Data Pipelines — private roads, toll gates, and the missing on-ramp

> **Exam map:** D1 · Task 1.1 — D4 · Task 4.1, 4.3 · **Skills:** 1.1.8, 4.1.1, 4.1.5, 4.3.4 · **Weight:** 🔥🔥 Medium · **Read time:** ~18 min

## The idea

Think of your **VPC (Virtual Private Cloud)** as a **gated industrial park** that you own inside AWS. The park is split into **lots (subnets)**. Some lots face the public highway (**public subnets**, with a route to an **Internet Gateway**). Others are deep inside, with no highway access (**private subnets**). Each lot's **route table** is the road sign telling trucks where to drive. Every building has a **doorman (security group)** who remembers who went out and lets their replies back in. Each lot's entrance has a **toll gate (network ACL)** that checks every truck both ways and remembers nothing.

The catch for data engineers: the services your pipelines depend on, like **S3, Glue's API, KMS, STS, CloudWatch Logs and Secrets Manager**, live **outside your park** on AWS's public service network. A Glue job, EMR cluster or Lambda function sitting in a private lot has **no road to reach them** unless you build one. The options are a **NAT Gateway** (an exit to the highway), a **gateway VPC endpoint** (a free private tunnel to S3/DynamoDB), or an **interface endpoint** (a private branch office of the service inside your lot, powered by **AWS PrivateLink**).

Most exam networking questions come down to *"the job times out or hangs; what road is missing?"*, *"keep traffic off the internet"*, or *"a partner's firewall needs a fixed IP to allowlist"*. This guide covers all three, plus the service-specific quirks of Glue, Redshift, EMR, Lambda, MWAA, MSK and Firehose, and short summaries of Route 53, CloudFront, WAF and Shield.

## VPC building blocks

| Component | What it does | Exam-relevant facts |
|---|---|---|
| **Public subnet** | Route `0.0.0.0/0 → Internet Gateway (IGW)` | Resources need a public IP/EIP to be reachable |
| **Private subnet** | No IGW route | Outbound internet only via NAT; AWS services via NAT or endpoints |
| **NAT Gateway** | Outbound-only internet for private subnets | Lives in a public subnet with an **Elastic IP (EIP)**, so outbound traffic comes from a **fixed IP you can allowlist**. Billed **per hour + per GB processed**. One per AZ for HA. A *Regional* availability mode (Nov 2025) spans AZs automatically and needs no public subnet |
| **Security group (SG)** | Instance/ENI-level firewall | **Stateful** (replies allowed automatically), **allow rules only**, default = deny inbound / allow all outbound. Can reference **other SGs** or **prefix lists** as source/destination |
| **Network ACL (NACL)** | Subnet-level firewall | **Stateless** (open **ephemeral ports 1024–65535** for return traffic), **allow and deny** rules, evaluated in **rule-number order** |

**THE trap:** *"Block one malicious IP address"* → **NACL deny rule**. Security groups can't deny. The reverse: *"allow the Glue job's workers to talk to each other"* → a **security group** rule referencing itself, not a NACL.

**Updating security groups (4.1.1).** Edit the inbound/outbound rules on the SG. Changes apply **immediately** to every ENI using that SG, with no restart. Prefer **SG references** over CIDRs: *"allow 5439 from the Glue connection's SG"* keeps working as worker IPs change. For AWS services, use **AWS-managed prefix lists** (e.g., the S3 prefix list `com.amazonaws.<region>.s3` in an outbound HTTPS rule, or CloudFront's origin-facing list). Use **customer-managed prefix lists** to maintain a partner's CIDR set once and reference it from many SGs.

## VPC endpoints: gateway vs interface

| | **Gateway endpoint** | **Interface endpoint (PrivateLink)** |
|---|---|---|
| Services | **S3 and DynamoDB only** | Hundreds: Glue, Athena, Kinesis Data Streams, Firehose, Redshift, Redshift Data API, Secrets Manager, KMS, STS, CloudWatch Logs/Monitoring, SQS, SNS, ECR (api + dkr), Step Functions, EMR/EMR Serverless, Lake Formation… and **S3/DynamoDB too** |
| Mechanism | **Route-table entry** (prefix list → endpoint) | **ENIs with private IPs** in your subnets; **private DNS** makes the normal service hostname resolve to them |
| Cost | **Free** | **Hourly per AZ + per GB** |
| Reachable from on-prem / peered VPC / Transit Gateway | **No** | **Yes** (VPN/Direct Connect) |
| Security | **Endpoint policy** | Endpoint policy + **security group** on the ENIs |

Decision rules:
- **Private-subnet workloads reading S3 in the same Region → S3 gateway endpoint.** It's free, and it also removes NAT data-processing charges, which makes it the classic *"reduce NAT Gateway costs"* answer.
- **On-premises clients need private S3/DynamoDB access over Direct Connect/VPN → interface endpoint**. Gateway endpoints can't be reached from outside the VPC.
- Interface endpoint private DNS needs the VPC's **enableDnsSupport** and **enableDnsHostnames** attributes turned on.
- Lock things down from both sides: **endpoint policy** (which buckets/actions may pass) + **bucket policy `aws:SourceVpce`** (only accept traffic from this endpoint). JSON for both is in [Guide 37 — IAM](37-IAM-for-Data-Engineers.md).

**THE trap:** *"A Glue job / Lambda function / EMR cluster in a private subnet hangs, then times out reading S3 (or calling STS/KMS/Secrets Manager). There's no NAT Gateway."* Nothing is wrong with IAM. There's **no network path**. Fix: an **S3 gateway endpoint** for S3, **interface endpoints** for the other APIs (or a NAT Gateway if internet access is acceptable). Timeouts point to the network; *AccessDenied* points to IAM or KMS.

## AWS PrivateLink beyond AWS services

- **Endpoint services (your own PrivateLink service):** put your service behind a **Network Load Balancer**, publish it as an endpoint service, and let consumer accounts create **interface endpoints** to it. This is **one-way**, needs no VPC peering, and **tolerates overlapping CIDRs**. Signal: *"expose an internal data API to other accounts privately, without peering."*
- **Redshift-managed VPC endpoints:** a PrivateLink connection to a **provisioned RA3 cluster** (with relocation or Multi-AZ enabled) or a **Serverless workgroup**, from another VPC or account. The owner authorizes the grantee account and VPC. Not reachable from the internet.
- **MSK multi-VPC private connectivity:** PrivateLink-based access for Kafka clients in other VPCs or accounts **in the same Region**. You authorize accounts through a **cluster policy**. Requires IAM, TLS or SASL/SCRAM auth (**not unauthenticated**), and overlapping IPs are fine.
- **OpenSearch:** VPC domains place ENIs in your subnets. OpenSearch-managed **VPC endpoints** (PrivateLink) let other VPCs reach a VPC domain. OpenSearch Serverless collections use their own VPC endpoints.

## Service-by-service networking

### AWS Glue
- A **Glue connection** (JDBC, or the *Network* type) carries a **subnet** and **security groups**. Glue creates **ENIs with private IPs only (never public IPs)** in that subnet, roughly one per worker. So a **small subnet can run out of IPs** when many jobs run concurrently, which shows up as errors about insufficient IP addresses.
- The SG must have a **self-referencing inbound rule for all TCP ports** (source = the same SG) so Spark workers can talk to each other. **THE trap:** opening the database port alone isn't enough. Missing this rule makes jobs fail to start or hang.
- Reaching S3 from a VPC-attached job **requires an S3 VPC endpoint** (or NAT). SaaS or public endpoints need a **NAT Gateway**, because a public subnet doesn't help when the ENIs have no public IP. **On-premises JDBC** → Site-to-Site VPN or Direct Connect, plus DNS resolution for the on-prem hostname.
- One job reaches **one VPC/subnet** at a time. To combine sources in different VPCs, use VPC peering or stage through S3. More in [Guide 12 — Glue ETL](12-AWS-Glue-ETL.md).

### Amazon Redshift
- A **cluster subnet group** chooses the subnets. **Publicly accessible** needs a public subnet and an EIP. Since **Jan 10, 2025**, new clusters and Serverless workgroups default to **not publicly accessible**, **encrypted**, and **`require_ssl = true`** (via the `default.redshift-2.0` parameter group).
- **Enhanced VPC routing (EVR)** forces **COPY/UNLOAD** traffic through your VPC, so you can apply endpoint policies, SGs, NACLs and **VPC Flow Logs** to it. Once EVR is on, the cluster **needs a path to S3**: an **S3 gateway endpoint** (same Region) or a **NAT Gateway** (other Regions or services). Otherwise COPY fails.
- **Spectrum + EVR:** create the S3 gateway endpoint and a **Glue interface endpoint** (plus Lake Formation if used). On RA3/DC2 provisioned clusters, **Spectrum's fleet runs outside your VPC**, so its S3 reads don't appear in flow logs, and it **can't read a bucket whose policy only allows specific VPC endpoints**. Use principal-based bucket policies instead. Redshift Serverless queries the lake from in-VPC compute.
- **Firehose → Redshift:** Firehose connects to the cluster over the internet. The provisioned cluster or Serverless workgroup **must be publicly accessible**, with the **Firehose CIDR block for the Region allowlisted** in its security group. There's **no private-subnet option** for this destination. **THE trap:** *"Firehose can't deliver to a private Redshift cluster."* Private alternatives: **Redshift streaming ingestion** from Kinesis Data Streams/MSK (Redshift pulls from inside), or Firehose → S3 → COPY/auto-copy ([Guide 07](07-Amazon-Data-Firehose.md), [Guide 24](24-Redshift-Loading-Integration-Sharing.md)). By contrast, Firehose **can** deliver into **OpenSearch domains/collections inside a VPC** by creating ENIs in your subnets.

### Amazon EMR
- Run clusters in **private subnets**. EMR creates **managed security groups** (primary, core/task, and a **service-access SG** for private subnets). Private subnets need an **S3 gateway endpoint**, plus NAT or interface endpoints for other services.
- **EMR block public access** (on by default per Region) refuses to launch clusters whose SGs allow inbound from `0.0.0.0/0` or `::/0` on ports other than those you allow (port 22 by default).
- Reach the UI or SSH through **Session Manager**, a bastion, or VPN, not public SSH. More in [Guide 15 — EMR](15-Amazon-EMR.md).

### AWS Lambda
- A VPC-attached function uses **Hyperplane ENIs**, shared per **subnet + security group combination**. It **never gets a public IP**, even in a public subnet. Internet access → private subnet + **NAT Gateway**. AWS services → NAT or **VPC endpoints**.
- **THE trap:** *"We moved the function to a public subnet to give it internet access."* That doesn't work. Use a private subnet with a NAT route.
- The execution role needs ENI permissions (`AWSLambdaVPCAccessExecutionRole`). Details in [Guide 17](17-Lambda-for-Data-Pipelines.md).

### Amazon MWAA
- Requires **two private subnets in different AZs** and a security group with a **self-referencing rule**. The VPC can't be changed after the environment is created.
- **Web server access mode:** **public** (internet-reachable URL, still IAM-authenticated) or **private** (reachable only from within the VPC, e.g., via VPN or a bastion).
- **No internet access at all?** Provide endpoints for **S3 (gateway), SQS, CloudWatch Logs, CloudWatch monitoring and KMS**, and package Python dependencies yourself because PyPI isn't reachable. More in [Guide 21](21-MWAA-Glue-Workflows.md).

### Amazon MSK clients
Brokers live in your subnets. Clients connect from the same VPC, a peered VPC, or a Transit Gateway, or through **multi-VPC private connectivity**. Open the broker ports for the auth method you use in the broker SG, and reference the **client SG** rather than CIDRs ([Guide 08](08-Amazon-MSK-Kafka.md)).

## IP allowlisting patterns (skill 1.1.8)

| Situation | Pattern |
|---|---|
| Third-party API or on-prem firewall must allowlist **your pipeline's egress** | Private subnet → **NAT Gateway with an Elastic IP**. Give the partner the EIP(s). Glue/Lambda/EMR traffic all leaves from that fixed address |
| Partners push files and must allowlist **your endpoint** | **AWS Transfer Family** server with a VPC endpoint and **EIPs attached** (fixed public IPs), optionally SG rules for the partners' CIDRs ([Guide 11](11-DataSync-Transfer-Family-Snow-AppFlow.md)) |
| An AWS service must reach your resource | Allowlist the **service's published CIDR** (e.g., the Firehose Region CIDR in the Redshift SG, or Firehose ranges for Splunk). Where AWS publishes one, use the AWS-managed prefix list |
| Many SGs share the same partner list | **Customer-managed prefix list** referenced by each SG |
| Restrict S3 API access to corporate IPs | Bucket policy `aws:SourceIp` (public IPs only; use `aws:SourceVpce` for endpoint traffic) |
| Database accepts Glue only | DB SG inbound rule sourcing the **Glue connection's SG** |

**THE trap:** giving a partner "the IP of the Lambda function" or "the Glue job's IP". These are ephemeral private addresses. Stable egress means **NAT Gateway + EIP**.

## Hybrid connectivity (brief)

| Option | Key facts | Pick when |
|---|---|---|
| **Site-to-Site VPN** | IPsec over the internet, **encrypted**, quick to set up; about **1.25 Gbps per tunnel** | Fast start, modest bandwidth, backup for DX |
| **Direct Connect (DX)** | Dedicated private link (1/10/100/400 Gbps dedicated; smaller hosted connections), consistent latency. **Not encrypted by default** | Large steady transfers, predictable performance |
| DX + encryption | **MACsec** (layer 2, on supported dedicated connections at select locations) or an **IPsec VPN over DX** | *"Private, dedicated AND encrypted in transit"* |
| **Transit Gateway** | Regional hub connecting many VPCs + VPN/DX | Dozens of VPCs/accounts; replaces peering meshes |

**THE trap:** *"Data over Direct Connect must be encrypted in transit."* DX alone isn't encrypted. Add **MACsec or a VPN over DX**, or rely on TLS at the application layer ([Guide 39](39-Encryption-Key-Management.md)).

## Route 53, CloudFront, WAF and Shield (concise)

- **Route 53 private hosted zones:** DNS names visible only inside associated VPCs (e.g., `db.analytics.internal`). **Route 53 Resolver endpoints** handle hybrid DNS. **Inbound** endpoints let on-prem resolvers query VPC names. **Outbound** endpoints plus forwarding rules let VPC workloads (Glue, EMR) resolve **on-prem hostnames** such as a JDBC server.
- **CloudFront:** edge caching and distribution of content or data files globally. **Origin Access Control (OAC)** makes an S3 origin reachable **only via CloudFront**. **Signed URLs/cookies** give time-limited access to private content. Field-level encryption protects specific fields at the edge.
- **AWS WAF:** web ACLs on **CloudFront, ALB, API Gateway (REST)** and others. Rules include IP sets, geo match, **rate-based rules** (throttle abusive clients of a data API) and **AWS Managed Rules** groups (SQLi, known bad inputs, bots).
- **AWS Shield:** **Standard** is free, automatic L3/L4 DDoS protection for everyone. **Advanced** is paid: enhanced detection, the **Shield Response Team**, **DDoS cost protection**, and L7 protection together with WAF. Signal: *"protect a public data API from DDoS and SQL injection"* → **WAF (managed rules + rate-based) on API Gateway/CloudFront**, plus Shield Advanced if they want response-team support and cost protection.

## Encryption in transit: network pointers

Private networking isn't encryption. Enforce TLS anyway: an S3 bucket policy that denies `aws:SecureTransport = false`, Redshift `require_ssl`, RDS/Aurora force-SSL parameters, MSK TLS client-broker, OpenSearch HTTPS-only + node-to-node encryption, and JDBC connections with SSL in Glue. The full table is in [Guide 39 — Encryption, KMS & Secrets](39-Encryption-Key-Management.md).

## Question patterns

> *"A Glue job with a JDBC connection to RDS in a private subnet fails to read its S3 staging bucket; the logs show timeouts. The VPC has no NAT Gateway. What is the MOST cost-effective fix?"* → **Add an S3 gateway VPC endpoint to the subnet's route table** (free; a NAT Gateway works but costs per hour and per GB; timeouts mean network, not IAM).

> *"A newly created Glue connection to Aurora fails during job startup even though the database SG allows port 3306 from the VPC CIDR."* → **Add a self-referencing inbound rule for all TCP on the Glue connection's security group** (Glue workers must reach each other).

> *"A SaaS vendor only accepts API calls from allowlisted IP addresses; the calls come from Lambda functions in private subnets."* → **Route through a NAT Gateway with an Elastic IP and give the vendor the EIP** (Lambda has no stable or public IP of its own).

> *"Redshift has enhanced VPC routing enabled and COPY from S3 in the same Region now fails."* → **Create an S3 gateway endpoint (or NAT path) for the cluster's subnets** (EVR forces COPY through the VPC; it needs a route).

> *"Firehose must deliver to Redshift. Which configuration is required?"* → **Make the cluster publicly accessible and allow the Firehose Region CIDR in its security group** (if the cluster must stay private, use Redshift streaming ingestion from Kinesis or Firehose → S3 → COPY instead).

> *"On-premises ETL servers connected via Direct Connect must write to S3 privately, without traversing the internet."* → **S3 interface endpoint (PrivateLink)** (gateway endpoints aren't reachable from on-premises).

> *"NAT Gateway data-processing charges are high; most traffic is EMR reading and writing S3 in the same Region."* → **S3 gateway endpoint** (free, and it takes S3 traffic off the NAT).

> *"A BI tool in account B's VPC must query a Redshift Serverless workgroup in account A privately, with overlapping CIDRs and no peering."* → **Redshift-managed VPC endpoint authorized for account B** (PrivateLink handles overlapping CIDRs; peering can't).

> *"Kafka producers in five application VPCs across two accounts need private access to one MSK cluster using IAM auth, with minimal network management."* → **MSK multi-VPC private connectivity with a cluster policy** (no peering or Transit Gateway routing to maintain; same Region only).

> *"Block a single abusive IP address from reaching an EC2-based ingestion endpoint in a public subnet."* → **Network ACL deny rule** (security groups can't deny; WAF works only with CloudFront/ALB/API Gateway fronting it).

> *"A public REST data API on API Gateway is receiving bursts of scraping traffic and SQL injection attempts."* → **AWS WAF web ACL with a rate-based rule and AWS Managed Rules (SQLi)** (Shield Standard covers L3/L4 only).

> *"EMR jobs must resolve an on-premises database hostname over the VPN."* → **Route 53 Resolver outbound endpoint with a forwarding rule for the on-prem domain** (private hosted zones only serve names you host in AWS).

> *"Traffic over an existing 10 Gbps dedicated Direct Connect link must be encrypted at layer 2 without lowering throughput."* → **Enable MACsec on the dedicated connection** (a VPN over DX encrypts too, but caps throughput per tunnel).

> *"Expose an internal pricing data service to 20 customer AWS accounts privately and one-way."* → **PrivateLink endpoint service behind a Network Load Balancer** (consumers create interface endpoints; no peering and no route exposure).

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Private subnet job times out on S3 | S3 gateway endpoint (or NAT) |
| Private subnet → STS/KMS/Secrets Manager/Glue API | Interface endpoints (or NAT) |
| Gateway endpoint services | S3 and DynamoDB only; free; route table |
| Interface endpoint | PrivateLink ENIs, private DNS, hourly + per GB, SG-protected |
| On-prem → S3 privately | S3 interface endpoint over DX/VPN |
| Cut NAT costs for S3 traffic | S3 gateway endpoint |
| Fixed egress IP for allowlisting | NAT Gateway + Elastic IP |
| Partners allowlist our SFTP | Transfer Family VPC endpoint with EIPs |
| Stateful, allow-only, instance level | Security group |
| Stateless, allow + deny, subnet level | NACL (open ephemeral ports) |
| Block one IP | NACL deny |
| Maintain partner CIDRs once | Customer-managed prefix list |
| Glue connection worker traffic | Self-referencing all-TCP SG rule |
| Glue ENIs | Private IPs only; size subnets for concurrency |
| Lambda in public subnet for internet | Doesn't work; private subnet + NAT |
| Lambda VPC networking | Hyperplane ENIs per subnet+SG combo |
| Redshift COPY through VPC + flow logs | Enhanced VPC routing (needs S3 endpoint/NAT) |
| Spectrum + bucket locked to VPC endpoint | Fails on provisioned clusters; use principal-based policy |
| Firehose → Redshift | Publicly accessible + Firehose CIDR allowlisted |
| Private Redshift streaming | Redshift streaming ingestion (KDS/MSK) |
| Redshift from another VPC/account | Redshift-managed VPC endpoint (RA3 / Serverless) |
| New Redshift defaults (2025) | Private, encrypted, require_ssl |
| MSK clients across VPCs/accounts | MSK multi-VPC private connectivity |
| Share own service privately | PrivateLink endpoint service (NLB) |
| MWAA with no internet | Endpoints: S3, SQS, Logs, Monitoring, KMS |
| EMR security groups | EMR-managed (+ service-access SG in private subnets) |
| DX encryption | Not by default → MACsec or VPN over DX |
| Quick encrypted hybrid link | Site-to-Site VPN |
| Many VPCs + on-prem hub | Transit Gateway |
| Resolve on-prem names from VPC | Route 53 Resolver outbound endpoint |
| S3 only via CloudFront | Origin Access Control |
| Rate limiting / SQLi on data API | WAF (rate-based + managed rules) |
| DDoS response team + cost protection | Shield Advanced |

Networking decides which road the bytes travel. Next, make sure they're unreadable on every road and at rest in [Guide 39 — Encryption, KMS & Secrets](39-Encryption-Key-Management.md).
