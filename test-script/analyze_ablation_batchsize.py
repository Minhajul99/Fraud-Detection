"""
analyze_ablation_batchsize.py
=============================
Reads DynamoDB records for batch_size_1, batch_size_10, batch_size_25
and compares latency and throughput across batch sizes.

Usage:
    python analyze_ablation_batchsize.py
"""

import boto3
import json
import csv
import os

TABLE_NAME = "fraud-results"
REGION     = "us-east-1"

dynamo = boto3.client("dynamodb", region_name=REGION)


def scan_by_experiment(experiment_id):
    items     = []
    paginator = dynamo.get_paginator("scan")
    for page in paginator.paginate(
        TableName=TABLE_NAME,
        FilterExpression="experiment_id = :e",
        ExpressionAttributeValues={":e": {"S": experiment_id}}
    ):
        items.extend(page["Items"])
    return items


def parse_item(item):
    return {
        "TransactionID"  : item.get("TransactionID",  {}).get("S", ""),
        "experiment_id"  : item.get("experiment_id",  {}).get("S", ""),
        "e2e_latency_ms" : float(item.get("e2e_latency_ms", {}).get("S", 0))
                           if "e2e_latency_ms" in item else None,
        "batch_score_ms" : float(item.get("batch_score_ms", {}).get("S", 0))
                           if "batch_score_ms" in item else None,
        "batch_size"     : int(item.get("batch_size", {}).get("S", 1))
                           if "batch_size" in item else 1,
        "fraud_prob"     : float(item.get("fraud_prob", {}).get("S", 0)),
        "prediction"     : item.get("prediction", {}).get("S", ""),
    }


def percentile(data, p):
    if not data:
        return 0
    s = sorted(data)
    i = min(int(len(s) * p / 100), len(s) - 1)
    return round(s[i], 3)


def compute(records, label):
    latencies = [
        r["e2e_latency_ms"]
        for r in records
        if r["e2e_latency_ms"] is not None and 0 < r["e2e_latency_ms"] < 300000
    ]
    batch_scores = [
        r["batch_score_ms"]
        for r in records
        if r["batch_score_ms"] is not None and r["batch_score_ms"] > 0
    ]
    fraud = [r for r in records if r["prediction"] == "fraud"]

    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)
    avg_score = round(sum(batch_scores) / len(batch_scores), 3) if batch_scores else 0
    throughput = round(1000 / p50, 2) if p50 > 0 else 0

    print(f"\n  {label}  ({len(records)} records)")
    print(f"  {'-'*50}")
    print(f"  E2E p50       : {p50:.2f} ms")
    print(f"  E2E p95       : {p95:.2f} ms")
    print(f"  E2E p99       : {p99:.2f} ms")
    print(f"  Avg score time: {avg_score:.3f} ms")
    print(f"  Throughput    : {throughput} tx/s")
    print(f"  Fraud rate    : {len(fraud)}/{len(records)} ({round(len(fraud)/len(records)*100,2)}%)")

    return {
        "label"      : label,
        "count"      : len(records),
        "e2e_p50_ms" : p50,
        "e2e_p95_ms" : p95,
        "e2e_p99_ms" : p99,
        "avg_score_ms": avg_score,
        "throughput"  : throughput,
        "fraud_rate"  : round(len(fraud)/len(records)*100, 2) if records else 0,
    }


def main():
    experiments = [
        ("batch_size_1",  "Batch size 1"),
        ("batch_size_10", "Batch size 10"),
        ("batch_size_25", "Batch size 25"),
    ]

    all_stats   = []
    all_records = []

    for exp_id, label in experiments:
        print(f"Scanning {exp_id}...")
        items   = scan_by_experiment(exp_id)
        records = sorted([parse_item(i) for i in items],
                         key=lambda x: x["TransactionID"])[-200:]
        print(f"  Found: {len(records)} records")
        stats = compute(records, label)
        all_stats.append(stats)
        all_records.extend(records)

    print("\n" + "="*65)
    print("  ABLATION EXPERIMENT 2 -- BATCH SIZE COMPARISON")
    print("="*65)
    print(f"  {'Metric':<22} {'Batch=1':>12} {'Batch=10':>12} {'Batch=25':>12}")
    print(f"  {'-'*58}")

    for metric, label in [
        ("e2e_p50_ms",  "E2E p50 (ms)"),
        ("e2e_p95_ms",  "E2E p95 (ms)"),
        ("e2e_p99_ms",  "E2E p99 (ms)"),
        ("avg_score_ms","Avg score (ms)"),
        ("throughput",  "Throughput (tx/s)"),
        ("fraud_rate",  "Fraud rate (%)"),
    ]:
        vals = [str(s[metric]) for s in all_stats]
        print(f"  {label:<22} {vals[0]:>12} {vals[1]:>12} {vals[2]:>12}")

    print()
    print("  INTERPRETATION:")
    p50s = [s["e2e_p50_ms"] for s in all_stats]
    p95s = [s["e2e_p95_ms"] for s in all_stats]
    print(f"  p50 latency:  batch=1 -> {p50s[0]}ms  batch=10 -> {p50s[1]}ms  batch=25 -> {p50s[2]}ms")
    print(f"  p95 latency:  batch=1 -> {p95s[0]}ms  batch=10 -> {p95s[1]}ms  batch=25 -> {p95s[2]}ms")
    if p50s[0] < p50s[1]:
        print(f"  Smaller batches give LOWER latency but LESS throughput efficiency.")
    else:
        print(f"  Larger batches give LOWER latency in this run (cold start dominated).")

    # Save CSV
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "ablation_batchsize_results.csv")
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "TransactionID","experiment_id","e2e_latency_ms",
            "batch_score_ms","batch_size","fraud_prob","prediction"
        ])
        w.writeheader()
        for r in all_records:
            w.writerow(r)

    # Save summary JSON
    summary = {s["label"]: s for s in all_stats}
    json_path = out_path.replace(".csv", "_summary.json")
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n  Saved: {out_path}")
    print(f"  Saved: {json_path}")
    print("\nDone!")


if __name__ == "__main__":
    main()
