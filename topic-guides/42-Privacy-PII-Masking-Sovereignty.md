# 42 · Privacy, PII, Masking & Sovereignty — find it, disguise it, fence it in

> **Exam map:** D4 · Task 4.3, 4.5 · **Skills:** 4.3.1, 4.5.2, 4.5.3, 4.5.4, 4.5.5 · **Weight:** 🔥🔥 Medium · **Read time:** ~18 min

## The idea

Think of sensitive data as **hazardous material in a warehouse**. First you need a **detector sweep**: which pallets actually contain the hazardous stuff? Then you **repackage** it so the people handling it can work without being exposed. Some boxes get a blacked-out label, some get a code number whose decoder ring sits in a safe, and some get shredded contents. Finally you build a **fence**: the material may never leave the country, not even as a backup copy on a truck at 3 a.m.

Those are the three moves this guide teaches.

1. **Identify** personally identifiable information (**PII**) with Amazon Macie, AWS Glue sensitive data detection and DataBrew.
2. **Transform** it with the right technique: masking, redaction, tokenization, hashing with a secret, encryption, generalization. The choice depends on whether anyone must reverse it and whether analysts still need to join on it.
3. **Contain** it with data sovereignty controls: service control policies (**SCPs**) that deny unapproved AWS Regions, plus the specific settings that stop **replication and backups** from sneaking data across borders.

The exam phrases it as *"according to compliance laws or company policies"* and *"prevent backups or replications of data to disallowed AWS Regions."* By the end you'll pick the right technique in seconds and write the Region-deny SCP from memory.

## PII basics — what counts

- **Direct identifiers** single out a person on their own: name, email, national ID/SSN, passport, phone, card number.
- **Quasi-identifiers** are harmless alone but identifying together: ZIP code + birth date + gender identify most people. Aggregated or "anonymized" datasets leak through these.
- **PHI** (protected health information) is health data tied to an identity, regulated by **HIPAA** in the US.
- **Regulations you'll see named:**
  - **GDPR** (EU: lawful basis, minimization, right to erasure, data transfer rules)
  - **CCPA/CPRA** (California consumer rights)
  - **HIPAA** (PHI)
  - **PCI DSS** (cardholder data: never store CVV, mask the PAN when displayed)

  The exam won't test legal text. It tests the *technical control* that satisfies the stated requirement.

**Pseudonymized vs anonymized.** Pseudonymized data (tokens, keyed hashes) can be re-linked by whoever holds the key or vault, so regulators such as GDPR **still treat it as personal data**. Anonymized data can't reasonably be re-identified. That distinction drives which technique you pick.

## Identifying PII (4.5.2)

| Tool | Scans | How | Output / action |
|---|---|---|---|
| **Amazon Macie** | **S3 only** | **Automated sensitive data discovery**: continuous, **sampling-based** across your whole S3 estate, giving each bucket a **sensitivity score**. **Sensitive data discovery jobs** are targeted scans, one-time or scheduled, of chosen buckets or prefixes. It also evaluates **bucket posture** (public, unencrypted, shared outside the account) | **Policy findings** and **sensitive data findings** go to **EventBridge** and **Security Hub**. Findings are kept 90 days. Detailed discovery results are written to an S3 bucket you configure |
| **AWS Glue Detect PII / sensitive data detection** | Data flowing through a **Glue job** | *Detect PII in each cell* (every row) or *detect fields containing PII* (a **sample** of rows with a detection threshold). Managed entity types grouped as Universal, HIPAA, Networking and country categories, plus **custom entities** (regex + context words). High/low sensitivity | **Enrich** (add a detection-results column), **redact** (fixed string, default `*******`), **partially redact**, or **apply SHA-256 hash**. Per-column overrides are possible |
| **AWS Glue DataBrew** | Datasets in a DataBrew project/job | PII detection in **profile jobs** ([Guide 14](14-Glue-DataBrew-Data-Preparation.md)) | PII **recipe** steps: redact/mask, substitute or shuffle, **hash (with a secret key)**, **deterministic or probabilistic encryption (KMS)**, decrypt |
| **CloudWatch Logs data protection policies** | Log events in a log group (or account-wide) | Managed and custom data identifiers | **Audit and mask** in logs. Only principals with **`logs:Unmask`** see raw values |
| **Bedrock Guardrails sensitive info filters** | LLM prompts and responses | Built-in PII types + regex | Block or mask ([Guide 19](19-GenAI-LLMs-Vectors.md)) |
| Amazon Comprehend (not on the in-scope list) | Free text via API | NLP PII entity detection | Offsets / redaction |

**Macie details the exam likes:**

- **Managed data identifiers** are AWS-built detectors for credentials, financial, health, and personal data across many countries.
- **Custom data identifiers** are your own **regex** plus optional **keywords**, ignore words and maximum match distance (for example, employee IDs like `EMP-\d{6}`).
- **Allow lists** name text or patterns Macie should **ignore**, such as your company's public support phone number or test card numbers. They cut false positives.
- For many accounts, a **Macie delegated administrator** in AWS Organizations manages all member accounts.

**The Macie + Lake Formation pattern (the exam's own example).**

```
Macie finding (S3 object/prefix has PII)
   → EventBridge rule → Lambda
      → tag the Glue Data Catalog table/columns with an LF-Tag (e.g., classification=pii)
         → LF-Tag-based grants: only the "pii-readers" role can see pii columns
```

Macie **finds** PII, and **Lake Formation enforces** who can see it at query time in Athena, Redshift Spectrum, EMR and Glue. Neither does the other's job. See [Guide 40](40-Lake-Formation.md) for LF-Tags and data cell filters.

**THE trap:** *"scan an RDS database or Redshift tables with Macie."* Macie analyzes **S3 objects only**. For data in motion or in tables, use **Glue sensitive data detection** in the ETL job (or scan exports in S3).

## Techniques (4.3.1) — pick by reversibility and analytic utility

| Technique | What it does | Reversible? | Joinable / analyzable? | Pick when |
|---|---|---|---|---|
| **Static masking** | Permanently replaces values in a copy (`XXX-XX-1234`) | No | Partially (last 4 digits) | Non-production/test copies, shared extracts |
| **Dynamic masking** | Masks **at query time** based on who asks. Stored data unchanged | N/A (source intact) | Yes for privileged roles | Same table, different audiences (support sees last 4, fraud sees all) |
| **Partial masking** | Reveals a fragment | No | Limited | PCI display rules, customer service verification |
| **Redaction** | Removes or blanks the value | No | No | Field isn't needed at all (*data minimization*) |
| **Tokenization** | Replaces the value with a random token. The **mapping lives in a secure vault** | **Yes**, via the vault (authorized detokenization) | **Yes**, same token everywhere | Must recover the original later (billing, customer contact) while analytics works on tokens |
| **Pseudonymization** | Umbrella term for consistent substitutes (tokens, keyed hashes) | With the key or vault | Yes | Link records across datasets without exposing identity. Still personal data |
| **Hashing (SHA-256)** | One-way fingerprint, same input gives same output | No | **Yes**, deterministic | Joins and counts on identifiers, but **only safe with a secret** (see below) |
| **Keyed hashing / salting (HMAC-SHA-256)** | Hash of value + a **secret key** (a "pepper") from **Secrets Manager** | No | **Yes** (same key → same output) | The exam's *"key salting."* Defeats dictionary and rainbow-table attacks and keeps joins working |
| **Encryption** (KMS, client-side) | Ciphertext, reversible with the key | **Yes**, with `kms:Decrypt` | **Deterministic** encryption allows equality joins. **Probabilistic** doesn't | Protected storage with authorized recovery. See [Guide 39](39-Encryption-Key-Management.md) |
| **Generalization / bucketing** | Age 37 → "35–44", ZIP 94107 → "941**" | No | Yes, coarser | Reduce quasi-identifier risk while keeping trends |
| **k-anonymity** | Generalize until each record matches at least *k−1* others on quasi-identifiers | No | Aggregate | Releasing record-level datasets |
| **Differential privacy** | Adds calibrated noise to **query results** | No | Aggregates with a privacy guarantee | Statistics without exposing individuals (e.g., AWS Clean Rooms supports it) |
| **Synthetic data** | Generated records with similar statistical shape | N/A | Model and test use | Dev/test and ML experiments with no real people |

**Why a plain hash of PII isn't enough.** US SSNs have only about a billion possible values. An attacker can hash all of them in minutes and look up every "anonymized" SSN, which is a **dictionary attack**. A per-record random salt stops that but also destroys joins, because the same SSN hashes differently every time. The data-engineering answer is **keyed hashing**: HMAC-SHA-256 with one secret key stored in **AWS Secrets Manager**, readable only by the pipeline role. Output stays consistent, so joins work, but it can't be recomputed without the key. Rotate the key and you break linkability on purpose. DataBrew's hashing transform uses a secret for exactly this reason.

**THE trap:** *"analysts must join two datasets on customer email without ever seeing the email"* answered with **encryption** (probabilistic encryption gives a different ciphertext each time, so no joins) or **random tokens per dataset**. Use **deterministic** pseudonyms: **keyed hash** or a **shared tokenization vault**.

**THE trap:** *"the original values must be recoverable by the billing team"* answered with hashing. Hashes are one-way. Use **tokenization** or **encryption**.

## Where to enforce: ingestion vs query time

| Enforcement point | Mechanism | Effect on stored data |
|---|---|---|
| **At ingestion** | Glue Detect PII (redact/hash), DataBrew PII recipe steps, **Firehose Lambda transform**, Lambda on S3 events | **Stored data is changed**. Raw PII never lands in the curated zone |
| **At query time** | **Redshift dynamic data masking** (masking policies attached to columns for roles, [Guide 25](25-Redshift-Performance-Operations-Security.md)), **Lake Formation column exclusion and data cell filters** ([Guide 40](40-Lake-Formation.md)), **Quick Sight row-level and column-level security** ([Guide 35](35-Analytics-Visualization-Quick-Notebooks.md)), secure views | **Stored data unchanged**. Different users see different results |

**THE trap:** *"the stored PII must be irreversibly removed from the data lake"* answered with **Redshift dynamic data masking** or a Lake Formation column filter. Dynamic masking and filters **hide** data at read time. The raw values are still on disk and visible to privileged roles or anyone reading the S3 files directly. Irreversible removal means **transforming at ingestion** (redact or hash) or deleting. Conversely, *"privileged users must still see full values"* rules out static redaction and points to **dynamic** controls.

A common layered design is **raw zone** (encrypted, locked down, short retention) → Glue job with Detect PII (hash emails, redact free-text SSNs) → **curated zone** → Lake Formation filters and Redshift DDM for the few columns that must stay clear-text for some roles.

## Data sovereignty (4.5.3, 4.5.5) — the fence

**Sovereignty** means data (and often its processing) must stay inside approved jurisdictions. On AWS the first rule is simple: **data stays in the Region you put it in**. AWS doesn't move it. The risk is *you*: someone creates a resource in another Region, or turns on a feature that copies data across Regions.

### Layer 1 — deny unapproved Regions with an SCP

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyOutsideApprovedRegions",
      "Effect": "Deny",
      "NotAction": [
        "iam:*", "organizations:*", "sts:*", "route53:*", "cloudfront:*",
        "support:*", "budgets:*", "ce:*", "health:*", "waf:*", "wafv2:*",
        "globalaccelerator:*", "shield:*", "kms:*", "s3:ListAllMyBuckets"
      ],
      "Resource": "*",
      "Condition": {
        "StringNotEquals": {
          "aws:RequestedRegion": ["eu-central-1", "eu-west-1"]
        },
        "ArnNotLike": {
          "aws:PrincipalARN": "arn:aws:iam::*:role/OrgBreakGlassAdmin"
        }
      }
    }
  ]
}
```

- **`Deny` + `NotAction`** exempts **global services**, whose API calls resolve to `us-east-1` and would break under a Region deny. The list above is abbreviated. Start from the AWS Organizations example SCP, which has the full exemption list.
- The **`aws:RequestedRegion`** condition is the Region the API call is made to.
- **`ArnNotLike aws:PrincipalARN`** leaves a break-glass or automation role able to operate.
- SCPs **never grant** anything. They set the maximum permissions for every principal in member accounts. They **don't apply to the management account**.
- **AWS Control Tower** ships this as the **Region deny control**. **`AWS-GR_REGION_DENY`** applies landing-zone-wide ("Deny access to AWS based on the requested AWS Region for the landing zone"). **`CT.MULTISERVICE.PV.1`** applies to chosen OUs. You can't deny your home Region.
- Knock-on effect: Bedrock **cross-Region inference** needs its profile's destination Regions allowed. Use a **geographic** profile such as EU to stay compliant ([Guide 19](19-GenAI-LLMs-Vectors.md)).

### Layer 2 — close the replication and backup side doors

A Region-deny SCP stops people **creating resources** in forbidden Regions. But several features are **configured in an allowed Region** and copy data outward, and some copy into **another account's** resources.

| Copy path | Where the API is called | Control |
|---|---|---|
| **S3 Replication (CRR)** | Source Region (`PutBucketReplication`). S3 then replicates in the background | **Deny `s3:PutReplicationConfiguration`** except for an approved pipeline role. Destination buckets in denied Regions can't be created under the SCP, but a **cross-account destination outside your org** can exist, so the permission deny is what matters. Detect drift with AWS Config |
| **AWS Backup cross-Region/cross-account copy** | Copy rules in the backup plan / `StartCopyJob` in the source Region | Restrict who can create or modify backup plans and start copy jobs. Use AWS Backup's IAM condition keys for copy targets. Manage plans centrally with **Organizations backup policies** that only name approved vaults |
| **Redshift cross-Region snapshot copy** | Source cluster (`EnableSnapshotCopy`) | **Deny `redshift:EnableSnapshotCopy`** (and snapshot sharing to unknown accounts) |
| **RDS/Aurora cross-Region read replica, snapshot copy, automated-backup replication, Aurora Global Database** | Mostly the **destination** Region | The Region-deny SCP blocks these, because the call lands in the forbidden Region |
| **DynamoDB global tables** | `UpdateTable` with replica updates, from the source Region | **Deny `dynamodb:CreateTableReplica`** (and `UpdateTable` replica changes) in SCPs |
| **KMS multi-Region keys** | `ReplicateKey` in the primary Region | Condition key **`kms:ReplicaRegion`** limits allowed replica Regions. `kms:MultiRegion` can block creating multi-Region keys. Note: this copies key material, not data |
| Others to remember | S3 Multi-Region Access Points, MSK Replicator, OpenSearch cross-cluster replication, CloudFront edge caching | Deny the create/config actions, or don't use them for in-scope data |

- **Resource control policies (RCPs)** arrived in AWS Organizations in Nov 2024. They're the **resource-side** counterpart to SCPs, initially for **S3, STS, KMS, SQS and Secrets Manager**. They cap what *any* principal, even one outside your org, can do to your resources. They're great for a **data perimeter** ("only our org may access our buckets"). Use SCPs for "our people may not act in Region X" and RCPs for "no one outside may touch our data."
- **Detect drift** with **AWS Config**: rules that flag replication or backup settings pointing at unapproved destinations, plus **conformance packs** and **aggregators** across the org. Control Tower **detective controls** do the same ([Guide 43](43-Audit-Logging-CloudTrail-Config.md)).
- **Beyond the Region:** the **AWS European Sovereign Cloud** is a separate AWS partition, located and operated within the EU (its first Region is in Brandenburg, Germany). It's for customers whose rules demand EU-only operations, not just EU-located data. Know the name and nothing more for this exam.

**THE trap:** believing the Region-deny SCP alone stops S3 CRR or Redshift snapshot copies. Those are enabled by API calls **in an allowed Region**. The copy is performed by the service, not a principal calling the forbidden Region. You must **deny the replication or copy actions themselves**, then use Config to catch anything that slips through.

**THE trap:** *"restrict Regions for all accounts, including the management account."* SCPs don't affect the management account. Keep workloads out of it.

## Viewing configuration changes (4.5.4) — the evidence trail

**AWS Config** records a **configuration item (CI)** every time a supported resource changes. CIs build a **configuration timeline** per resource ("this bucket gained a replication rule at 14:02, encryption was removed at 14:10"). Config tracks **relationships** (which ENI belongs to which instance, which KMS key a volume uses) and links each change to the **CloudTrail event** showing *who* made it. Rules evaluate compliance. Advanced queries answer "show all buckets with replication configured" with SQL. The distinction to memorize:

- **Config** answers *"what changed, and is it compliant?"*
- **CloudTrail** answers *"who called which API?"*

Depth is in [Guide 43 — Audit Logging, CloudTrail & Config](43-Audit-Logging-CloudTrail-Config.md).

## Question patterns

> *"A company must discover which of its 3,000 S3 buckets contain PII, continuously and cost-effectively, and send alerts when new sensitive data appears."* → **Amazon Macie automated sensitive data discovery** + findings to **EventBridge → SNS**. Sampling-based discovery covers the estate cheaply, while full jobs on every bucket cost more. Glue crawlers don't classify PII.

> *"When Macie finds PII in a data lake prefix, analysts must automatically lose access to those columns in Athena."* → **Macie → EventBridge → Lambda applies LF-Tags → Lake Formation tag-based access control**. Macie detects. Lake Formation enforces.

> *"Macie keeps flagging the company's published support phone number as sensitive."* → **Allow list**. A custom data identifier adds detections, it doesn't suppress them.

> *"A Glue ETL job ingests customer records from a partner. SSNs must never land in the curated zone, but analysts need to count distinct customers."* → **Glue Detect PII with a keyed (secret) hash**, or an HMAC in the job with the key in Secrets Manager. Deterministic output keeps distinct counts. Redaction loses them, and dynamic masking leaves raw SSNs stored.

> *"Hashed email addresses in a shared dataset were reversed by a partner using a list of known emails."* → **Use keyed hashing (HMAC-SHA-256) with a secret key held in Secrets Manager**, i.e., "salting with a key." Plain SHA-256 is vulnerable to dictionary attacks.

> *"Customer service must see only the last four digits of card numbers in Redshift, while the fraud team sees full values, from the same table."* → **Redshift dynamic data masking** with role-based masking policies. Stored data is unchanged. Static masking would break the fraud team.

> *"Personal data must be removed from the stored dataset itself to satisfy a regulator."* → **Transform at ingestion (redact/hash) or delete**. Dynamic masking and Lake Formation column filters only hide data at query time.

> *"Billing must be able to recover original account numbers; analysts must never see them."* → **Tokenization** (vault-based, reversible for authorized billing), or **KMS encryption** with decrypt rights only for billing. Hashing is irreversible.

> *"Application logs in CloudWatch Logs occasionally contain email addresses and credit card numbers; only security investigators may view them."* → **CloudWatch Logs data protection policy** (audit + mask). Grant **`logs:Unmask`** only to investigators.

> *"All workloads must run only in eu-central-1 and eu-west-1 across the organization, while IAM and Route 53 keep working."* → **SCP: Deny with `NotAction` (global services) and `aws:RequestedRegion` StringNotEquals the approved Regions** (or the Control Tower Region deny control). An IAM policy on each role doesn't scale and can be edited by account admins.

> *"Even with a Region-deny SCP, auditors worry that data could be copied to other Regions through S3 replication or Redshift snapshots."* → **SCP deny `s3:PutReplicationConfiguration` and `redshift:EnableSnapshotCopy`** (except approved roles) + **AWS Config rules** to detect drift. The Region condition doesn't cover copies configured in an allowed Region.

> *"Backups must be copied only to vaults in approved Regions and accounts across the organization."* → **AWS Organizations backup policies** that define copy destinations + IAM/SCP restrictions on creating or altering backup plans and copy jobs. Manual per-account plans drift.

> *"Security needs to see when a bucket's replication configuration was changed and by whom."* → **AWS Config configuration timeline** (what changed) linked to the **CloudTrail event** (who). Macie doesn't track configuration changes.

> *"Two firms must compute overlap statistics on their customers without either seeing the other's records, with added noise to prevent re-identification."* → **AWS Clean Rooms with differential privacy**. k-anonymity via generalization applies to releasing record-level data.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Find PII in S3 at scale, continuously | **Macie automated sensitive data discovery** (sampling, sensitivity scores) |
| Deep scan of specific buckets | Macie **sensitive data discovery job** (one-time/scheduled) |
| Your own ID pattern | Macie **custom data identifier** (regex + keywords) |
| Suppress known false positives | Macie **allow list** |
| Bucket public/unencrypted/shared | Macie **policy findings** |
| Act on findings | EventBridge (→ Lambda/SNS), Security Hub |
| PII in RDS/Redshift/streams | Glue Detect PII in the job (Macie = S3 only) |
| Macie + access enforcement | Macie → EventBridge → Lambda → **LF-Tags** → Lake Formation TBAC |
| Glue PII actions | Detect/enrich, **redact**, **partial redact**, **SHA-256 hash** |
| Glue PII scan cheaply | "Detect fields containing PII" (sampling + threshold) |
| No-code PII transforms | **DataBrew** (mask, hash with secret, deterministic/probabilistic encrypt) |
| PII in log events | CloudWatch Logs **data protection policy** + `logs:Unmask` |
| PII in LLM prompts/responses | Bedrock Guardrails sensitive info filters |
| Irreversible, no join needed | **Redaction** |
| Reversible for authorized users | **Tokenization** (vault) or **encryption** |
| Join without seeing values | **Keyed hash (HMAC)** / deterministic token |
| "Key salting" / dictionary attack defense | HMAC-SHA-256, secret key in **Secrets Manager** |
| Same table, different audiences | **Dynamic masking** (Redshift DDM), LF filters, Quick Sight RLS/CLS |
| Remove PII from storage | Transform at **ingestion** (dynamic masking doesn't change stored data) |
| Test/dev copy | Static masking or synthetic data |
| Reduce quasi-identifier risk | Generalization / k-anonymity |
| Aggregates with privacy guarantee | Differential privacy (Clean Rooms) |
| Pseudonymized data under GDPR | Still personal data |
| Restrict Regions org-wide | SCP `Deny` + `NotAction` + **`aws:RequestedRegion`** |
| Managed Region deny | Control Tower `AWS-GR_REGION_DENY` / `CT.MULTISERVICE.PV.1` |
| Block S3 cross-Region copies | Deny **`s3:PutReplicationConfiguration`** |
| Block Redshift cross-Region snapshots | Deny **`redshift:EnableSnapshotCopy`** |
| Govern backup copies org-wide | **Organizations backup policies** + restrict copy jobs/plans |
| Limit KMS multi-Region key replicas | **`kms:ReplicaRegion`** condition |
| Block DynamoDB global table replicas | Deny `dynamodb:CreateTableReplica` |
| Outsiders must not access our data | **RCPs** (resource control policies) |
| SCP doesn't affect | The **management account** |
| Detect drift from sovereignty rules | **AWS Config** rules / conformance packs / aggregator |
| What changed on a resource | **Config timeline** (who = CloudTrail) |
| EU-operated separate partition | AWS European Sovereign Cloud |

Every control in this guide leaves footprints, and auditors will ask to see them. Continue with [Guide 43 — Audit Logging, CloudTrail & Config](43-Audit-Logging-CloudTrail-Config.md).
