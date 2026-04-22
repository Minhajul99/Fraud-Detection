"""
analyze_results.py
==================
Reads all DynamoDB records from fraud-results table and computes:
  - p50 / p95 / p99 end-to-end latency per pipeline
  - p50 / p95 / p99 scoring latency per pipeline
  - Throughput (transactions per second)
  - Fraud detection rate and probability distribution
  - Estimated cost per 1M transactions

Usage:
    python analyze_results.py
    python analyze_results.py --output results.csv
"""

import boto3
import json
import argparse
import csv
import os
from decimal import Decimal
from collections import defaultdict

# ── Config ─────────────────────────────────────────────────────────────────
TABLE_NAME = "fraud-results"
REGION     = "us-east-1"

# AWS Lambda pricing (us-east-1, as of 2024)
LAMBDA_PRICE_PER_REQUEST     = 0.0000002     # $0.20 per 1M requests
LAMBDA_PRICE_PER_GB_SEC      = 0.0000166667  # $0.0000166667 per GB-second
LAMBDA_MEMORY_GB             = 1.0           # 1024 MB

# SQS pricing
SQS_PRICE_PER_MILLION        = 0.40          # $0.40 per 1M requests (standard queue)

# DynamoDB pricing
DYNAMO_PRICE_PER_MILLION_WCU = 1.25          # $1.25 per million write requests

# ── AWS client ─────────────────────────────────────────────────────────────
dynamo = boto3.client("dynamodb", region_name=REGION)


def scan_all_items():
    """Scan entire DynamoDB table handling pagination."""
    print("Scanning DynamoDB table...")
    items     = []
    paginator = dynamo.get_paginator("scan")

    for page in paginator.paginate(TableName=TABLE_NAME):
        for item in page["Items"]:
            items.append(item)

    print(f"  Total records found: {len(items)}")
    return items


def parse_item(item):
    """Convert DynamoDB item format to plain dict."""
    return {
        "TransactionID"    : item.get("TransactionID",     {}).get("S", ""),
        "pipeline"         : item.get("pipeline",          {}).get("S", ""),
        "fraud_prob"       : float(item.get("fraud_prob",  {}).get("S", 0)),
        "prediction"       : item.get("prediction",        {}).get("S", ""),
        "e2e_latency_ms"   : float(item.get("e2e_latency_ms",   {}).get("S", 0)) if "e2e_latency_ms"   in item else None,
        "score_latency_ms" : float(item.get("score_latency_ms", {}).get("S", 0)) if "score_latency_ms" in item else None,
        "batch_score_ms"   : float(item.get("batch_score_ms",   {}).get("S", 0)) if "batch_score_ms"   in item else None,
        "batch_latency_s"  : float(item.get("batch_latency_s",  {}).get("S", 0)) if "batch_latency_s"  in item else None,
        "batch_size"       : int(item.get("batch_size",    {}).get("S", 1))      if "batch_size"       in item else 1,
    }


def percentile(data, p):
    """Compute percentile p (0-100) of a sorted list."""
    if not data:
        return 0
    data  = sorted(data)
    index = int(len(data) * p / 100)
    index = min(index, len(data) - 1)
    return round(data[index], 3)


def compute_metrics(records):
    """Compute all metrics per pipeline."""
    # Group records by pipeline
    by_pipeline = defaultdict(list)
    for r in records:
        by_pipeline[r["pipeline"]].append(r)

    results = {}

    for pipeline, recs in sorted(by_pipeline.items()):
        n = len(recs)

        # ── Latency ──────────────────────────────────────────────────────
        if pipeline == "B0":
            # B0 uses batch_latency_s (time since batch job started)
            latencies_ms = [
                r["batch_latency_s"] * 1000
                for r in recs
                if r["batch_latency_s"] is not None and r["batch_latency_s"] > 0
            ]
        else:
            # A1 and A2 use e2e_latency_ms
            latencies_ms = [
                r["e2e_latency_ms"]
                for r in recs
                if r["e2e_latency_ms"] is not None and 0 < r["e2e_latency_ms"] < 300000
            ]

        score_latencies = [
            r["score_latency_ms"]
            for r in recs
            if r["score_latency_ms"] is not None and r["score_latency_ms"] > 0
        ]

        # ── Fraud metrics ─────────────────────────────────────────────────
        probs       = [r["fraud_prob"] for r in recs]
        fraud_count = sum(1 for r in recs if r["prediction"] == "fraud")
        fraud_rate  = round(fraud_count / n * 100, 2) if n > 0 else 0

        # ── Throughput ────────────────────────────────────────────────────
        # Estimate from p50 latency
        p50_lat = percentile(latencies_ms, 50)
        if pipeline == "B0":
            # B0 processes in batch - throughput is per batch
            throughput = round(1000 / p50_lat, 2) if p50_lat > 0 else 0
        elif pipeline == "A2":
            # A2 batches 10 at a time
            batch_score = [r["batch_score_ms"] for r in recs if r["batch_score_ms"] is not None and r["batch_score_ms"] > 0]
            avg_batch_ms = round(sum(batch_score) / len(batch_score), 3) if batch_score else 0
            throughput   = round(10000 / avg_batch_ms, 2) if avg_batch_ms > 0 else 0
        else:
            throughput = round(1000 / p50_lat, 2) if p50_lat > 0 else 0

        # ── Cost estimate per 1M transactions ─────────────────────────────
        avg_duration_ms = percentile(score_latencies, 50)
        avg_duration_s  = avg_duration_ms / 1000

        lambda_request_cost = LAMBDA_PRICE_PER_REQUEST * 1_000_000
        lambda_compute_cost = LAMBDA_PRICE_PER_GB_SEC * LAMBDA_MEMORY_GB * avg_duration_s * 1_000_000
        dynamo_cost         = DYNAMO_PRICE_PER_MILLION_WCU * 1  # 1 write per tx

        if pipeline == "A1":
            sqs_cost = SQS_PRICE_PER_MILLION * 1  # 1 SQS message per tx
        elif pipeline == "A2":
            sqs_cost = SQS_PRICE_PER_MILLION * 1  # 1 SQS message per tx
        else:
            sqs_cost = 0  # B0 uses S3, not SQS

        total_cost = round(lambda_request_cost + lambda_compute_cost + dynamo_cost + sqs_cost, 4)

        results[pipeline] = {
            "count"           : n,
            "e2e_p50_ms"      : percentile(latencies_ms, 50),
            "e2e_p95_ms"      : percentile(latencies_ms, 95),
            "e2e_p99_ms"      : percentile(latencies_ms, 99),
            "score_p50_ms"    : percentile(score_latencies, 50),
            "score_p95_ms"    : percentile(score_latencies, 95),
            "score_p99_ms"    : percentile(score_latencies, 99),
            "fraud_rate_pct"  : fraud_rate,
            "fraud_count"     : fraud_count,
            "avg_fraud_prob"  : round(sum(probs) / len(probs), 4) if probs else 0,
            "throughput_tps"  : throughput,
            "cost_per_1M_usd" : total_cost,
            "latency_samples" : len(latencies_ms),
        }

    return results


def print_report(results):
    """Print formatted results to console."""
    print()
    print("=" * 70)
    print("  FRAUD DETECTION PIPELINE -- METRICS REPORT")
    print("=" * 70)

    for pipeline in ["A1", "A2", "B0"]:
        if pipeline not in results:
            continue
        m = results[pipeline]
        print()
        print(f"  Pipeline {pipeline}  ({m['count']} records, {m['latency_samples']} latency samples)")
        print(f"  {'-' * 50}")

        if pipeline == "B0":
            unit = "s (batch time)"
            p50  = f"{m['e2e_p50_ms']/1000:.2f}s"
            p95  = f"{m['e2e_p95_ms']/1000:.2f}s"
            p99  = f"{m['e2e_p99_ms']/1000:.2f}s"
        else:
            p50 = f"{m['e2e_p50_ms']:.1f} ms"
            p95 = f"{m['e2e_p95_ms']:.1f} ms"
            p99 = f"{m['e2e_p99_ms']:.1f} ms"

        print(f"  End-to-End Latency:   p50={p50}   p95={p95}   p99={p99}")
        print(f"  Score Latency:        p50={m['score_p50_ms']:.2f}ms   p95={m['score_p95_ms']:.2f}ms   p99={m['score_p99_ms']:.2f}ms")
        print(f"  Throughput:           {m['throughput_tps']} tx/s")
        print(f"  Fraud Rate:           {m['fraud_rate_pct']}%  ({m['fraud_count']}/{m['count']} flagged)")
        print(f"  Avg Fraud Prob:       {m['avg_fraud_prob']}")
        print(f"  Est. Cost / 1M tx:    ${m['cost_per_1M_usd']:.4f}")

    print()
    print("=" * 70)
    print("  COMPARISON SUMMARY")
    print("=" * 70)
    print(f"  {'Metric':<28} {'A1 (per-event)':<20} {'A2 (micro-batch)':<20} {'B0 (batch)':<15}")
    print(f"  {'-' * 83}")

    def val(p, k, fmt="{:.1f}"):
        return fmt.format(results[p][k]) if p in results else "N/A"

    a1, a2, b0 = results.get("A1", {}), results.get("A2", {}), results.get("B0", {})

    print(f"  {'Records':<28} {str(a1.get('count','N/A')):<20} {str(a2.get('count','N/A')):<20} {str(b0.get('count','N/A')):<15}")
    print(f"  {'E2E p50 latency':<28} {str(a1.get('e2e_p50_ms',''))+'ms':<20} {str(a2.get('e2e_p50_ms',''))+'ms':<20} {str(round(b0.get('e2e_p50_ms',0)/1000,2))+'s':<15}")
    print(f"  {'E2E p95 latency':<28} {str(a1.get('e2e_p95_ms',''))+'ms':<20} {str(a2.get('e2e_p95_ms',''))+'ms':<20} {str(round(b0.get('e2e_p95_ms',0)/1000,2))+'s':<15}")
    print(f"  {'E2E p99 latency':<28} {str(a1.get('e2e_p99_ms',''))+'ms':<20} {str(a2.get('e2e_p99_ms',''))+'ms':<20} {str(round(b0.get('e2e_p99_ms',0)/1000,2))+'s':<15}")
    print(f"  {'Score p50':<28} {str(a1.get('score_p50_ms',''))+'ms':<20} {str(a2.get('score_p50_ms',''))+'ms':<20} {str(b0.get('score_p50_ms',''))+'ms':<15}")
    print(f"  {'Throughput':<28} {str(a1.get('throughput_tps',''))+'tx/s':<20} {str(a2.get('throughput_tps',''))+'tx/s':<20} {str(b0.get('throughput_tps',''))+'tx/s':<15}")
    print(f"  {'Fraud rate':<28} {str(a1.get('fraud_rate_pct',''))+'%':<20} {str(a2.get('fraud_rate_pct',''))+'%':<20} {str(b0.get('fraud_rate_pct',''))+'%':<15}")
    print(f"  {'Est cost / 1M tx':<28} {'$'+str(a1.get('cost_per_1M_usd','')):<20} {'$'+str(a2.get('cost_per_1M_usd','')):<20} {'$'+str(b0.get('cost_per_1M_usd','')):<15}")
    print()


def save_csv(results, records, output_path):
    """Save raw records to CSV for charting."""
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "TransactionID", "pipeline", "fraud_prob", "prediction",
            "e2e_latency_ms", "score_latency_ms", "batch_score_ms", "batch_latency_s"
        ])
        writer.writeheader()
        for r in records:
            writer.writerow({
                "TransactionID"   : r["TransactionID"],
                "pipeline"        : r["pipeline"],
                "fraud_prob"      : r["fraud_prob"],
                "prediction"      : r["prediction"],
                "e2e_latency_ms"  : r["e2e_latency_ms"],
                "score_latency_ms": r["score_latency_ms"],
                "batch_score_ms"  : r["batch_score_ms"],
                "batch_latency_s" : r["batch_latency_s"],
            })
    print(f"  Raw records saved to: {output_path}")

    # Also save summary metrics to JSON
    json_path = output_path.replace(".csv", "_summary.json")
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  Summary metrics saved to: {json_path}")


def main():
    parser = argparse.ArgumentParser(description="Analyze fraud pipeline DynamoDB results")
    parser.add_argument("--output", default="results.csv", help="CSV output path (default: results.csv)")
    args = parser.parse_args()

    # Scan DynamoDB
    raw_items = scan_all_items()
    if not raw_items:
        print("No records found in DynamoDB table.")
        return

    # Parse all items
    records = [parse_item(item) for item in raw_items]

    # Filter out test records (TransactionID starting with TEST)
    real_records = [r for r in records if not r["TransactionID"].startswith("TEST")]
    test_records = [r for r in records if r["TransactionID"].startswith("TEST")]

    print(f"  Real transaction records: {len(real_records)}")
    print(f"  Test records (excluded):  {len(test_records)}")

    if not real_records:
        print("\nNo real transaction records found.")
        print("Run the producer first: python producer.py --pipeline all --limit 1000")
        return

    # Compute metrics
    results = compute_metrics(real_records)

    # Print report
    print_report(results)

    # Save to CSV and JSON
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.output)
    save_csv(results, real_records, output_path)

    print("\nDone! Use results.csv to generate charts in your report.")
    print("Use results_summary.json for the comparison table numbers.")


if __name__ == "__main__":
    main()
