"""
analyze_ablation_burst.py
=========================
Compares burst traffic (no delay) vs controlled rate (100ms delay)
for Pipeline A1.

Usage:
    python analyze_ablation_burst.py
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
        "e2e_latency_ms" : float(item.get("e2e_latency_ms",   {}).get("S", 0))
                           if "e2e_latency_ms"   in item else None,
        "score_latency_ms": float(item.get("score_latency_ms", {}).get("S", 0))
                           if "score_latency_ms" in item else None,
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
    scores = [
        r["score_latency_ms"]
        for r in records
        if r["score_latency_ms"] is not None and r["score_latency_ms"] > 0
    ]
    fraud = [r for r in records if r["prediction"] == "fraud"]
    p50   = percentile(latencies, 50)
    p95   = percentile(latencies, 95)
    p99   = percentile(latencies, 99)
    throughput = round(1000 / p50, 2) if p50 > 0 else 0

    print(f"\n  {label}  ({len(records)} records, {len(latencies)} samples)")
    print(f"  {'-'*50}")
    print(f"  E2E p50        : {p50:.2f} ms")
    print(f"  E2E p95        : {p95:.2f} ms")
    print(f"  E2E p99        : {p99:.2f} ms")
    print(f"  Score p50      : {percentile(scores,50):.3f} ms")
    print(f"  Throughput     : {throughput} tx/s")
    print(f"  Fraud rate     : {len(fraud)}/{len(records)} ({round(len(fraud)/len(records)*100,2)}%)")

    return {
        "label"       : label,
        "count"       : len(records),
        "e2e_p50_ms"  : p50,
        "e2e_p95_ms"  : p95,
        "e2e_p99_ms"  : p99,
        "score_p50_ms": percentile(scores, 50),
        "throughput"  : throughput,
        "fraud_rate"  : round(len(fraud)/len(records)*100, 2) if records else 0,
    }


def main():
    experiments = [
        ("burst_test",      "Burst (no delay, 11.7 tx/s)"),
        ("controlled_test", "Controlled (100ms delay, 5.5 tx/s)"),
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
    print("  ABLATION EXPERIMENT 3 -- BURST vs CONTROLLED RATE")
    print("="*65)
    print(f"  {'Metric':<25} {'Burst':>18} {'Controlled':>18} {'Diff':>10}")
    print(f"  {'-'*71}")

    for metric, label in [
        ("e2e_p50_ms",   "E2E p50 (ms)"),
        ("e2e_p95_ms",   "E2E p95 (ms)"),
        ("e2e_p99_ms",   "E2E p99 (ms)"),
        ("score_p50_ms", "Score p50 (ms)"),
        ("throughput",   "Throughput (tx/s)"),
        ("fraud_rate",   "Fraud rate (%)"),
    ]:
        v_burst = all_stats[0][metric]
        v_ctrl  = all_stats[1][metric]
        diff    = round(v_burst - v_ctrl, 3)
        sign    = "+" if diff > 0 else ""
        print(f"  {label:<25} {v_burst:>18} {v_ctrl:>18} {sign+str(diff):>10}")

    print()
    print("  INTERPRETATION:")
    p50_burst = all_stats[0]["e2e_p50_ms"]
    p50_ctrl  = all_stats[1]["e2e_p50_ms"]
    p99_burst = all_stats[0]["e2e_p99_ms"]
    p99_ctrl  = all_stats[1]["e2e_p99_ms"]

    if p50_burst < p50_ctrl:
        print(f"  Burst traffic has LOWER p50 latency ({p50_burst}ms vs {p50_ctrl}ms).")
        print(f"  This is because burst keeps Lambda containers warm continuously.")
        print(f"  Controlled rate allows containers to cool down between invocations,")
        print(f"  increasing cold start frequency and raising tail latency.")
    else:
        print(f"  Controlled rate has LOWER p50 latency ({p50_ctrl}ms vs {p50_burst}ms).")
        print(f"  Burst traffic causes SQS queue buildup and higher queuing latency.")

    if p99_burst > p99_ctrl:
        print(f"  However burst has HIGHER p99 ({p99_burst}ms) due to SQS queue depth")
        print(f"  building up faster than Lambda can scale.")

    # Save CSV
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "ablation_burst_results.csv")
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "TransactionID","experiment_id","e2e_latency_ms",
            "score_latency_ms","fraud_prob","prediction"
        ])
        w.writeheader()
        for r in all_records:
            w.writerow(r)

    summary = {s["label"]: s for s in all_stats}
    json_path = out_path.replace(".csv", "_summary.json")
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n  Saved: {out_path}")
    print(f"  Saved: {json_path}")
    print("\nDone!")


if __name__ == "__main__":
    main()
