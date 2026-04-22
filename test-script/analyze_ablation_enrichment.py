"""
analyze_ablation_enrichment.py
==============================
Reads DynamoDB records filtered by experiment_id and computes
latency comparison between enrichment ON and enrichment OFF.

Usage:
    python analyze_ablation_enrichment.py
"""

import boto3
import json
import csv
import os
from collections import defaultdict

TABLE_NAME = "fraud-results"
REGION     = "us-east-1"

dynamo = boto3.client("dynamodb", region_name=REGION)


def scan_by_experiment(experiment_id):
    """Scan DynamoDB for records matching a specific experiment_id."""
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
        "TransactionID" : item.get("TransactionID", {}).get("S", ""),
        "pipeline"      : item.get("pipeline",       {}).get("S", ""),
        "experiment_id" : item.get("experiment_id",  {}).get("S", ""),
        "enrichment"    : item.get("enrichment",     {}).get("S", "unknown"),
        "fraud_prob"    : float(item.get("fraud_prob",      {}).get("S", 0)),
        "prediction"    : item.get("prediction",     {}).get("S", ""),
        "e2e_latency_ms": float(item.get("e2e_latency_ms", {}).get("S", 0))
                          if "e2e_latency_ms" in item else None,
        "batch_score_ms": float(item.get("batch_score_ms", {}).get("S", 0))
                          if "batch_score_ms" in item else None,
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
    fraud      = [r for r in records if r["prediction"] == "fraud"]
    probs      = [r["fraud_prob"] for r in records]
    has_id     = sum(1 for r in records if r["enrichment"] == "on")

    print(f"\n  {label}  ({len(records)} records, {len(latencies)} latency samples)")
    print(f"  {'-'*55}")
    print(f"  E2E p50 : {percentile(latencies, 50):.2f} ms")
    print(f"  E2E p95 : {percentile(latencies, 95):.2f} ms")
    print(f"  E2E p99 : {percentile(latencies, 99):.2f} ms")
    print(f"  Fraud   : {len(fraud)}/{len(records)} ({round(len(fraud)/len(records)*100,2)}%)")
    print(f"  Avg prob: {round(sum(probs)/len(probs),4) if probs else 0}")

    return {
        "label"      : label,
        "count"      : len(records),
        "e2e_p50_ms" : percentile(latencies, 50),
        "e2e_p95_ms" : percentile(latencies, 95),
        "e2e_p99_ms" : percentile(latencies, 99),
        "fraud_rate" : round(len(fraud)/len(records)*100, 2) if records else 0,
    }


def main():
    print("Scanning enrichment_on records...")
    on_items  = scan_by_experiment("enrichment_on")
    print(f"  Found: {len(on_items)}")

    print("Scanning enrichment_off records...")
    off_items = scan_by_experiment("enrichment_off")
    print(f"  Found: {len(off_items)}")

    # Take only the most recent 200 records per experiment
    # (sort by TransactionID to get consistent subset)
    on_records  = sorted([parse_item(i) for i in on_items],
                         key=lambda x: x["TransactionID"])[-200:]
    off_records = sorted([parse_item(i) for i in off_items],
                         key=lambda x: x["TransactionID"])[-200:]

    print("\n" + "="*60)
    print("  ABLATION EXPERIMENT 1 -- ENRICHMENT ON vs OFF")
    print("="*60)

    on_stats  = compute(on_records,  "Enrichment ON  (5ms identity delay)")
    off_stats = compute(off_records, "Enrichment OFF (no delay)")

    print("\n" + "="*60)
    print("  COMPARISON")
    print("="*60)
    print(f"  {'Metric':<25} {'Enrichment ON':>15} {'Enrichment OFF':>15} {'Difference':>12}")
    print(f"  {'-'*67}")

    for metric, label in [("e2e_p50_ms","E2E p50 (ms)"),
                           ("e2e_p95_ms","E2E p95 (ms)"),
                           ("e2e_p99_ms","E2E p99 (ms)"),
                           ("fraud_rate","Fraud rate (%)")]:
        v_on  = on_stats[metric]
        v_off = off_stats[metric]
        diff  = round(v_on - v_off, 3)
        sign  = "+" if diff > 0 else ""
        print(f"  {label:<25} {v_on:>15} {v_off:>15} {sign+str(diff):>12}")

    print()
    print("  INTERPRETATION:")
    diff_p50 = round(on_stats["e2e_p50_ms"] - off_stats["e2e_p50_ms"], 2)
    diff_p95 = round(on_stats["e2e_p95_ms"] - off_stats["e2e_p95_ms"], 2)
    if diff_p50 > 0:
        print(f"  Enrichment adds ~{diff_p50:.1f}ms at p50 and ~{diff_p95:.1f}ms at p95")
        print(f"  This reflects the 5ms identity lookup delay for transactions")
        print(f"  where has_identity=1 in the test dataset.")
    else:
        print(f"  Enrichment overhead is within noise ({diff_p50:.1f}ms at p50)")
        print(f"  Most test transactions may not have identity data (has_identity=0)")
        print(f"  so the 5ms delay is rarely triggered.")

    # Save to CSV
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "ablation_enrichment_results.csv")
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "TransactionID","experiment_id","enrichment",
            "e2e_latency_ms","fraud_prob","prediction"
        ])
        w.writeheader()
        for r in on_records + off_records:
            w.writerow({
                "TransactionID" : r["TransactionID"],
                "experiment_id" : r["experiment_id"],
                "enrichment"    : r["enrichment"],
                "e2e_latency_ms": r["e2e_latency_ms"],
                "fraud_prob"    : r["fraud_prob"],
                "prediction"    : r["prediction"],
            })

    # Save summary JSON
    summary = {"enrichment_on": on_stats, "enrichment_off": off_stats}
    json_path = out_path.replace(".csv", "_summary.json")
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n  Saved: {out_path}")
    print(f"  Saved: {json_path}")
    print("\nDone!")


if __name__ == "__main__":
    main()
