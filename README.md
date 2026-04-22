# Cloud-Native Streaming Fraud Detection on AWS: Latency–Cost–Accuracy Trade-offs

**Course:** COMP 6910 – Services Computing, Semantic Web and Cloud Computing  
**Author:** Md Minhajul Abedin (mminhajula@mun.ca)  
**Dataset:** [IEEE-CIS Fraud Detection](https://www.kaggle.com/competitions/ieee-fraud-detection/overview)
**Clean Dataset:** You want to train the model again and skip the data preprocessing part use this [train_clean.csv](https://drive.google.com/file/d/1oXrTK0gx0RDWUxSeGs7RfO_GUYAOhrtC/view?usp=sharing) or you can the dataset from this zip file.

---

## Project Overview

This project implements and benchmarks three cloud-native fraud detection pipelines on AWS, using the IEEE-CIS Fraud Detection dataset replayed as a synthetic event stream. The goal is to empirically measure the trade-offs between **p95/p99 end-to-end latency**, **throughput (tx/s)**, **cost per transaction**, and **detection quality (Precision, Recall, PR-AUC)** across different streaming design choices.

The ML model (LightGBM) is fixed — the novelty is the **systems-level evaluation** across pipeline architectures under controlled burst and enrichment conditions.

---

## Architecture

```
producer.py
        │
        ▼
  ┌─────────────────────────────────────────────┐
  │              AWS SQS Queue                  │
  └─────────────────────────────────────────────┘
        │                        │
        ▼                        ▼
  ┌───────────┐           ┌───────────┐
  │ Lambda A1 │           │ Lambda A2 │
  │ (per-event│           │(micro-batch
  │ scoring)  │           │+enrichment)
  └─────┬─────┘           └─────┬─────┘
        │                        │
        ▼                        ▼
  ┌─────────────────────────────────────────────┐
  │              DynamoDB (fraud-results)        │
  └─────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────┐
  │  B0 – Batch Baseline (EventBridge → Lambda) │
  │  Reads from S3 incoming/, writes to DynamoDB│
  └─────────────────────────────────────────────┘
```

See `aws_sqs_fraud_pipeline_architecture.svg` for the full visual diagram.

---

## Pipelines

| Pipeline | Description | Trigger |
|----------|-------------|---------|
| **B0** | Batch baseline — scans S3 `incoming/` prefix every minute, scores all pending JSON files, moves them to `processed/` | EventBridge (scheduled) |
| **A1** | Per-event streaming — one transaction per SQS message, scored individually by Lambda | SQS → Lambda |
| **A2** | Micro-batch + enrichment — buffers multiple messages per Lambda invocation, simulates identity enrichment (5 ms delay), scores as a batch | SQS → Lambda |

---

## Repository Structure

```
Final Submission/
│
├── fraud.ipynb                          # Main notebook: EDA, preprocessing, model training & evaluation
├── fraud_model.pkl                      # Trained LightGBM model (1.7 MB)
├── feature_columns.json                 # Ordered list of 427 feature columns expected by the model
├── producer.py                          # Sends transactions to SQS (A1, A2) and S3 incoming/ (B0)
├── deploy_fraud_pipeline.ps1            # PowerShell IaC script: deploys all AWS resources
├── aws_sqs_fraud_pipeline_architecture.svg  # Architecture diagram
├── Fraud_Detection_Project_Report.pdf   # Full project report
├── logbook.pdf                          # Weekly project logbook
│
├── lamda/                               # Lambda function source code
│   ├── batch_b0.py                      # B0: Batch baseline Lambda handler
│   ├── lambda_a1.py                     # A1: Per-event streaming Lambda handler
│   └── lambda_a2.py                     # A2: Micro-batch + enrichment Lambda handler
│
└── results/                             # Experimental results
    ├── results.csv                      # Main latency/throughput results across all pipelines
    ├── results_summary.json             # Summary statistics (p95, p99, cost estimates)
    ├── ablation_batchsize_results.csv   # Ablation: batch size effect on A2
    ├── ablation_burst_results.csv       # Ablation: burst traffic behaviour
    ├── ablation_enrichment.csv          # Ablation: enrichment on/off comparison
    └── analyze_results.py               # Script to generate summary stats and plots from results
```

---

## AWS Services Used

- **Amazon SQS** — message queue for A1 and A2 pipelines (replaces Kinesis; handles per-message payloads well within the 256 KB SQS limit)
- **AWS Lambda** — serverless scoring for all three pipelines
- **Amazon S3** — stores model artifacts (`fraud_model.pkl`, `feature_columns.json`) and batch transaction files for B0
- **Amazon DynamoDB** — sink for all scoring results (`fraud-results` table)
- **Amazon EventBridge** — triggers B0 batch Lambda on a schedule

> **Note on file size limits:** AWS Lambda has a 50 MB compressed / 250 MB unzipped deployment package limit. The LightGBM model (`fraud_model.pkl`, ~1.7 MB) and dependencies are packaged as a Lambda Layer to stay within these limits. The model is loaded from S3 at cold start, not bundled directly in the deployment zip.

---

## Lambda Environment Variables

Each Lambda function requires the following environment variables set in the AWS Console or via the deployment script:

| Variable | Description | Default |
|----------|-------------|---------|
| `MODEL_BUCKET` | S3 bucket name where model artifacts are stored | *(required)* |
| `MODEL_KEY` | S3 key for the model file | `model/fraud_model.pkl` |
| `FEATURES_KEY` | S3 key for the feature columns JSON | `model/feature_columns.json` |
| `INCOMING_PREFIX` | S3 prefix for B0 pending transactions | `incoming/` |

---

## Deployment

All AWS resources (SQS queues, Lambda functions, DynamoDB table, EventBridge rule, IAM roles) are provisioned using the included PowerShell script:

```powershell
.\deploy_fraud_pipeline.ps1
```

**Prerequisites:**
- AWS CLI configured with appropriate credentials and region
- PowerShell 5.1+ or PowerShell Core
- Lambda Layer with LightGBM, joblib, and numpy already published (see deployment script for layer ARN)

---

## Running the Experiment

**Step 1 — Upload model artifacts to S3:**
```bash
aws s3 cp fraud_model.pkl s3://<YOUR_BUCKET>/model/fraud_model.pkl
aws s3 cp feature_columns.json s3://<YOUR_BUCKET>/model/feature_columns.json
```

**Step 2 — Deploy infrastructure:**
```powershell
.\deploy_fraud_pipeline.ps1
```

**Step 3 — Send transactions to SQS (A1, A2) and S3 (B0):**
```bash
# Send all rows to all pipelines
python producer.py --pipeline all --limit 100

# Send to a specific pipeline only
python producer.py --pipeline a1 --limit 500
python producer.py --pipeline b0 --limit 100

# With rate control (e.g. 50 ms delay between sends)
python producer.py --pipeline all --delay-ms 50
```

> **SQS size limit handled automatically:** `producer.py` checks each message size before sending. If a transaction payload exceeds 200 KB, it is stored in S3 and a pointer is sent via SQS instead, staying safely within the 256 KB SQS hard limit.

**Step 4 — Analyze results:**
```bash
python results/analyze_results.py
```

Results are written to DynamoDB and exported to `results/results.csv`.

---

## Model Performance

The LightGBM model was trained on the IEEE-CIS Fraud Detection dataset. Since fraud is rare (~3.5% of transactions), accuracy is not a meaningful metric. The model is evaluated on:

| Metric | Score |
|--------|-------|
| Precision | 0.2548 |
| Recall | 0.8231 |
| PR-AUC | 0.6699 |

> High recall is prioritized — catching fraudulent transactions matters more than minimizing false positives in this setting.

---

## Evaluation Metrics (Systems)

- **p95 / p99 end-to-end latency** — from SQS `SentTimestamp` to DynamoDB write
- **Throughput** — transactions scored per second
- **Cost** — estimated USD per 1M transactions (Lambda + SQS + DynamoDB)
- **Detection quality** — Precision, Recall, PR-AUC (fixed; does not vary by pipeline)

---

## Ablation Studies

Three ablation experiments were conducted:

1. **Batch size** — effect of SQS batch window size on A2 latency and throughput
2. **Burst traffic** — pipeline behaviour under sudden spikes in transaction arrival rate
3. **Enrichment on/off** — latency overhead introduced by identity enrichment in A2

Results are in the `results/` folder.
