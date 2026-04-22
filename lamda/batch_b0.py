import json
import os
import time
import boto3
import joblib
import numpy as np
from io import BytesIO

# ── AWS clients ────────────────────────────────────────────────────────────────
s3     = boto3.client("s3")
dynamo = boto3.resource("dynamodb").Table("fraud-results")

# ── Environment variables (set in Lambda config) ───────────────────────────────
BUCKET          = os.environ["MODEL_BUCKET"]
MODEL_KEY       = os.environ.get("MODEL_KEY",       "model/fraud_model.pkl")
FEATURES_KEY    = os.environ.get("FEATURES_KEY",    "model/feature_columns.json")
INCOMING_PREFIX = os.environ.get("INCOMING_PREFIX", "incoming/")

# ── Categorical columns ────────────────────────────────────────────────────────
CAT_COLS = [
    "ProductCD", "card4", "card6", "P_emaildomain", "R_emaildomain",
    "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9",
    "DeviceType", "DeviceInfo",
    "id_12", "id_15", "id_16", "id_23", "id_27", "id_28", "id_29",
    "id_35", "id_36", "id_37", "id_38"
]

# ── Cold-start: load model + features once per container ──────────────────────
_model    = None
_features = None


def load_artifacts():
    global _model, _features
    if _model is None:
        print("Cold start: loading model from S3...")
        buf = BytesIO()
        s3.download_fileobj(BUCKET, "model/fraud_model_native.txt", buf)
        buf.seek(0)
        # Save to /tmp (Lambda writable directory) then load
        tmp_path = "/tmp/fraud_model_native.txt"
        with open(tmp_path, "wb") as f:
            f.write(buf.read())
        import lightgbm as lgb
        _model = lgb.Booster(model_file=tmp_path)
        print("Model loaded.")
    if _features is None:
        print("Cold start: loading feature columns from S3...")
        obj       = s3.get_object(Bucket=BUCKET, Key=FEATURES_KEY)
        _features = json.loads(obj["Body"].read())
        print(f"Loaded {len(_features)} feature columns.")


def preprocess(tx: dict, features: list) -> np.ndarray:
    """
    Replicates the notebook preprocessing pipeline:
      - has_identity flag, id_* filled with -999 / 'unknown'
      - V_null_count, V* filled with 0
      - C* → 0, D* → 0, M* → -1 (encoded unknown)
      - LabelEncoder → integers (hash-based for Lambda)
    """
    row = []
    for f in features:
        val = tx.get(f, None)

        if val is None or val == "":
            if f == "has_identity":
                row.append(0)
            elif f == "V_null_count":
                row.append(0)
            elif f in CAT_COLS:
                row.append(-1)
            else:
                row.append(0.0)
        else:
            if f in CAT_COLS:
                try:
                    row.append(int(val))
                except (ValueError, TypeError):
                    row.append(abs(hash(str(val))) % 1000)
            else:
                try:
                    row.append(float(val))
                except (ValueError, TypeError):
                    row.append(0.0)

    return np.array([row], dtype=np.float32)


def score_and_save(tx: dict, tx_id: str, batch_start_ts: float):
    """Score a single transaction and write result to DynamoDB."""
    X    = preprocess(tx, _features)

    t0       = time.monotonic()
    prob = float(_model.predict(X)[0])
    score_ms = (time.monotonic() - t0) * 1000

    batch_latency_s = round(time.time() - batch_start_ts, 4)

    dynamo.put_item(Item={
        "TransactionID" : f"B0#{tx_id}",
        "pipeline"        : "B0",
        "fraud_prob"      : str(round(prob, 6)),
        "prediction"      : "fraud" if prob >= 0.5 else "legit",
        "score_latency_ms": str(round(score_ms, 3)),
        "batch_latency_s" : str(batch_latency_s),
    })


def handler(event, context):
    """
    EventBridge triggers this every minute.
    Scans s3://BUCKET/incoming/ for pending transaction JSON files,
    scores each one with the LightGBM model, writes results to DynamoDB,
    then moves the processed file to s3://BUCKET/processed/.

    NOTE: B0 reads directly from S3 — it does NOT use SQS at all.
    Files are dropped into incoming/ by the producer script.
    No 256 KB limit applies here.
    """
    load_artifacts()

    batch_start = time.time()
    scored      = 0
    errors      = 0

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=INCOMING_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/"):
                continue

            try:
                raw   = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                tx    = json.loads(raw)
                tx_id = tx.get("TransactionID", key.split("/")[-1].replace(".json", ""))

                score_and_save(tx, tx_id, batch_start)

                # Move to processed/
                dest_key = key.replace(INCOMING_PREFIX, "processed/", 1)
                s3.copy_object(
                    Bucket=BUCKET,
                    CopySource={"Bucket": BUCKET, "Key": key},
                    Key=dest_key
                )
                s3.delete_object(Bucket=BUCKET, Key=key)

                scored += 1

            except Exception as e:
                print(f"ERROR processing {key}: {e}")
                errors += 1

    total_s = round(time.time() - batch_start, 3)
    print(f"B0 batch complete: scored={scored}, errors={errors}, total_time={total_s}s")

    return {
        "pipeline"    : "B0",
        "scored"      : scored,
        "errors"      : errors,
        "total_time_s": total_s
    }