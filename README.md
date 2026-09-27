# AWS Certified Data Engineer – Associate (DEA-C01) — Complete Study Guides

**46 exam-focused topic guides, 2 full-length practice exams, a v1.1 new-topics drill and cheat sheets for DEA-C01.** Written the way a good teacher explains, not the way documentation reads.

> **Up to date with exam guide v1.1 (December 12, 2025)**, including the new skills most prep material doesn't cover yet: vector indexes (HNSW/IVF), LLMs in data pipelines, Apache Iceberg and S3 Tables, and SageMaker Catalog / Unified Studio governance. Facts were checked against AWS documentation as of **September 2026**, and every guide flags services that have been renamed, retired or closed to new customers.

## What's inside

| Folder | Contents |
|---|---|
| [`topic-guides/`](topic-guides/00-README.md) | Study index, [exam blueprint](topic-guides/01-Exam-Blueprint.md) (every skill ID mapped to a guide) and 46 topic guides |
| [`practice-exams/`](practice-exams/README.md) | Two 65-question full-length exams at the real domain weights, plus a drill on the v1.1 topics. Answers are hidden and each comes with a full explanation and a link to the guide that teaches it |
| [`cheat-sheets/`](cheat-sheets/) | Master keyword → service card, numbers worth memorizing, decision flowcharts |
| [`STUDY-PLAN.md`](STUDY-PLAN.md) | 6-week, 3-week and 10-day plans, plus a hands-on lab checklist with cost guardrails |

## What makes these different

Every topic guide follows the same format:

1. **Exam map**: the domain, task and skill IDs it covers, exam weight (🔥 to 🔥🔥🔥) and read time, so you always know why you're reading it
2. **The idea**: the topic explained from zero with one real-world analogy (a Kinesis shard is a toll-road lane, a Glue crawler is a librarian cataloguing new books, a Redshift distribution key decides which warehouse shelf a box lands on…)
3. **Core concepts**: the facts, numbers and decision rules the exam tests, with explicit **"THE trap"** callouts for the wrong answers it's designed to catch
4. **Question patterns**: realistic DEA-C01 scenarios, each with its answer and the signal words that decide it
5. **Pocket card**: a keyword → answer table for fast review

Two markers keep you current: **⚠️ 2026 status** (renamed, in maintenance or closed to new customers) and **🆕 new in v1.1**.

The guides focus on the **decisions** the exam tests ("Firehose vs Kinesis Data Streams", "crawler vs partition projection", "Spectrum vs federated query vs zero-ETL", "Step Functions vs MWAA vs Glue workflows", "IAM vs Lake Formation") rather than service trivia on its own.

## The exam at a glance

| | |
|---|---|
| Questions | 65 (50 scored + 15 unscored, and you can't tell which) — multiple choice and multiple response |
| Time | 130 minutes |
| Passing score | 720 of 1,000 (scaled, compensatory across domains) |
| Domains | Ingestion & Transformation **34%** · Data Store Management **26%** · Operations & Support **22%** · Security & Governance **18%** |
| Cost | USD 150 |

Details, v1.1 changes and the full skill map are in the [Exam Blueprint](topic-guides/01-Exam-Blueprint.md).

## Start here

📖 **[Study index](topic-guides/00-README.md)**: all guides in a suggested reading order, grouped by topic.

If you're short on time, start with the heaviest-tested guides:
- [AWS Glue ETL](topic-guides/12-AWS-Glue-ETL.md) and [Glue Data Catalog & Crawlers](topic-guides/13-Glue-Data-Catalog-Crawlers.md): the service that appears in every domain
- [Redshift Loading, Integration & Sharing](topic-guides/24-Redshift-Loading-Integration-Sharing.md): COPY/UNLOAD, Spectrum, federated queries, streaming ingestion, data sharing
- [Amazon Athena](topic-guides/26-Amazon-Athena.md): partitions, projection, formats, workgroups, Iceberg
- [Kinesis Data Streams](topic-guides/06-Kinesis-Data-Streams.md) and [Amazon Data Firehose](topic-guides/07-Amazon-Data-Firehose.md): the streaming block
- [Service Selection Decision Guide](topic-guides/45-Service-Selection-Decision-Guide.md): every "which service?" decision in one place
- [Exam Traps & Key Patterns](topic-guides/47-Exam-Traps-Key-Patterns.md): ⭐ read it last, and again on exam morning

## Exam-technique rules

1. **Find the qualifier before you read the story.** The last sentence tells you whether you're optimizing for *cost*, *operational overhead*, *latency* or *security*. Two options usually "work"; the qualifier picks one.
2. **Turn latency words into a service tier.** *Real-time / milliseconds* → Kinesis Data Streams or MSK with Lambda or Flink. *Near real-time* → Firehose or Redshift streaming ingestion. *Hourly / nightly* → Glue or EMR batch. A mismatched tier eliminates options fast.
3. **"Least operational overhead" is a ladder.** Built-in feature or zero-ETL → serverless managed service → managed cluster → self-managed on EC2. Pick the highest rung that still meets **every** requirement.
4. **Most performance and cost questions come down to three file fixes:** columnar formats, partitioning, and right-sized files. If Athena, Spectrum or Glue is slow or expensive, check these first.
5. **Every stated requirement is there to eliminate something.** "Must preserve order", "must replay", "must not change code", "must stay in eu-central-1": each one kills at least one option. Eliminate first, then choose the cheapest or most managed survivor.
6. **Distrust legacy and out-of-scope answers.** AWS Data Pipeline, Kinesis Data Analytics for SQL, S3 Select, Glue Ray jobs, X-Ray or Elastic Beanstalk as the centerpiece of a new design is almost always a distractor.
7. **In multiple-response questions, judge each option on its own.** Ask "would this option, by itself, be part of a correct solution?" Don't try to build a story that connects all of them.

## Disclaimer

These are community study notes, not affiliated with or endorsed by Amazon Web Services. The content reflects the DEA-C01 exam guide v1.1 and AWS documentation as of September 2026. AWS services change often, so check anything that matters against current AWS documentation. The practice questions are original and are not taken from the real exam.

## Acknowledgments

The guide format ("the idea → core concepts → question patterns → pocket card") is inspired by [RonitSachdev/aws-saa-c03-guides](https://github.com/RonitSachdev/aws-saa-c03-guides) (CC BY 4.0), an excellent SAA-C03 resource. All DEA-C01 content here is original.

## License

[CC BY 4.0](LICENSE). You're free to use, share and adapt these guides as long as you give credit and link back to this repository. If they help you pass, a ⭐ is appreciated.
