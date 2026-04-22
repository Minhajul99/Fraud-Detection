"""
producer.py -- Sends transactions to SQS (A1, A2) and S3 incoming/ (B0)

Solves the SQS 256 KB limit using the S3 pointer pattern:
  - If a transaction JSON is < 200 KB  -> send directly in SQS body
  - If a transaction JSON >= 200 KB    -> store in S3, send a pointer in SQS

Usage:
    python producer.py --pipeline all --limit 100
    python producer.py --pipeline a1  --limit 500
    python producer.py --pipeline b0  --limit 100
    python producer.py --pipeline all            (sends ALL rows -- careful!)

Data folder: D:\Education\1. MUN\Cloud Computing\Project\data\
  - test_transaction.csv  (584 MB)  <- main replay file
  - test_identity.csv     (24 MB)   <- optional identity enrichment
"""

import os
import time
import argparse
import json
import boto3
import pandas as pd

# -- Config --------------------------------------------------------------------
BUCKET   = "fraud-detection-minhajul99"
REGION   = "us-east-1"
ACCOUNT  = "427570356240"

QUEUE_A1 = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/fraud-queue-a1"
QUEUE_A2 = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/fraud-queue-a2"

# Data folder -- relative to this script
DATA_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
TRANSACTION_CSV   = os.path.join(DATA_DIR, "test_transaction.csv")
IDENTITY_CSV      = os.path.join(DATA_DIR, "test_identity.csv")

# SQS hard limit is 256 KB -- stay well under
SQS_MAX_BYTES = 200_000

# -- AWS clients ---------------------------------------------------------------
s3  = boto3.client("s3",  region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)


def load_data(limit: int = None) -> pd.DataFrame:
    """
    Load test_transaction.csv and optionally merge with test_identity.csv.
    Applies row limit if specified.
    """
    print(f"Loading transactions from: {TRANSACTION_CSV}")
    tx_df = pd.read_csv(TRANSACTION_CSV).fillna("")

    print(f"Loading identity from:     {IDENTITY_CSV}")
    id_df = pd.read_csv(IDENTITY_CSV).fillna("")

    # Merge identity into transactions (left join -- not all tx have identity)
    df = tx_df.merge(id_df, on="TransactionID", how="left")
    df = df.fillna("")

    # Add has_identity flag (1 if identity data exists, 0 otherwise)
    id_ids = set(id_df["TransactionID"].tolist())
    df["has_identity"] = df["TransactionID"].apply(lambda x: 1 if x in id_ids else 0)

    if limit:
        df = df.head(limit)
        print(f"Limiting to {limit} rows")

    print(f"Total rows to send: {len(df)}")
    return df


def send_to_sqs(tx: dict, queue_url: str):
    """
    Send one transaction to SQS.
    Automatically offloads to S3 if payload exceeds SQS_MAX_BYTES.
    """
    payload = json.dumps(tx)

    if len(payload.encode("utf-8")) > SQS_MAX_BYTES:
        key = f"sqs-payloads/{tx.get('TransactionID', 'unknown')}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=payload,
            ContentType="application/json"
        )
        message_body = json.dumps({
            "__s3_pointer__": True,
            "s3_bucket":      BUCKET,
            "s3_key":         key
        })
    else:
        message_body = payload

    sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)


def send_to_b0(tx: dict):
    """
    Drop transaction JSON into S3 incoming/ for B0 batch pipeline.
    No size limit -- S3 handles up to 5 TB.
    """
    tx_id = tx.get("TransactionID", "unknown")
    key   = f"incoming/{tx_id}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(tx),
        ContentType="application/json"
    )


def run(pipeline: str, limit: int = None, delay_ms: int = 0):
    """
    Main replay loop.
    pipeline : a1 | a2 | b0 | all
    limit    : max rows to send (None = all rows)
    delay_ms : milliseconds to wait between sends (0 = as fast as possible)
    """
    df = load_data(limit)

    sent    = 0
    errors  = 0
    start   = time.time()

    print(f"\nStarting replay -> pipeline={pipeline}, rows={len(df)}, delay={delay_ms}ms")
    print("-" * 60)

    for i, (_, row) in enumerate(df.iterrows()):
        tx = row.to_dict()

        try:
            if pipeline in ("a1", "all"):
                send_to_sqs(tx, QUEUE_A1)

            if pipeline in ("a2", "all"):
                send_to_sqs(tx, QUEUE_A2)

            if pipeline in ("b0", "all"):
                send_to_b0(tx)

            sent += 1

        except Exception as e:
            print(f"  [ERR] TX {tx.get('TransactionID')}: {e}")
            errors += 1

        # Progress update every 100 rows
        if (i + 1) % 100 == 0:
            elapsed = round(time.time() - start, 1)
            rate    = round(sent / elapsed, 1) if elapsed > 0 else 0
            print(f"  Sent {i + 1}/{len(df)} | errors={errors} | {rate} tx/s | {elapsed}s elapsed")

        # Optional delay between sends (for burst/rate testing)
        if delay_ms > 0:
            time.sleep(delay_ms / 1000)

    total = round(time.time() - start, 2)
    rate  = round(sent / total, 1) if total > 0 else 0

    print("-" * 60)
    print(f"Done.")
    print(f"  Sent   : {sent}")
    print(f"  Errors : {errors}")
    print(f"  Time   : {total}s")
    print(f"  Rate   : {rate} tx/s")
    print(f"  Pipeline: {pipeline}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fraud pipeline producer -- replays IEEE-CIS transactions"
    )
    parser.add_argument(
        "--pipeline", default="all",
        choices=["a1", "a2", "b0", "all"],
        help="Which pipeline to send to (default: all)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max rows to send (default: all rows)"
    )
    parser.add_argument(
        "--delay-ms", type=int, default=0,
        help="Delay in ms between each send, for rate control (default: 0)"
    )
    args = parser.parse_args()
    run(args.pipeline, args.limit, args.delay_ms)