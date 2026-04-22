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

# ── Environment variables ──────────────────────────────────────────────────────
BUCKET              = os.environ["MODEL_BUCKET"]
MODEL_KEY           = os.environ.get("MODEL_KEY",           "model/fraud_model.pkl")
FEATURES_KEY        = os.environ.get("FEATURES_KEY",        "model/feature_columns.json")
ENRICHMENT_ENABLED  = os.environ.get("ENRICHMENT_ENABLED",  "true").lower() == "true"
EXPERIMENT_ID       = os.environ.get("EXPERIMENT_ID",       "baseline")

# ── Categorical columns ────────────────────────────────────────────────────────
CAT_COLS = {
    'ProductCD', 'card4', 'card6', 'P_emaildomain', 'R_emaildomain',
    'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9',
    'DeviceType', 'DeviceInfo',
    'id_12', 'id_15', 'id_16', 'id_23', 'id_27', 'id_28', 'id_29',
    'id_35', 'id_36', 'id_37', 'id_38'
}

# ── Cold-start cache ───────────────────────────────────────────────────────────
_model    = None
_features = None


def load_artifacts():
    global _model, _features
    if _model is None:
        print("Cold start: loading model from S3...")
        buf = BytesIO()
        s3.download_fileobj(BUCKET, MODEL_KEY, buf)
        buf.seek(0)
        _model = joblib.load(buf)
        print("Model loaded.")
    if _features is None:
        print("Cold start: loading feature columns from S3...")
        obj       = s3.get_object(Bucket=BUCKET, Key=FEATURES_KEY)
        _features = json.loads(obj["Body"].read())
        print(f"Loaded {len(_features)} feature columns.")


def resolve_payload(record: dict):
    """
    Handles the SQS 256 KB limit via S3 pointer pattern.
    Returns None if the message body is not valid JSON (skip bad messages).
    """
    try:
        body = json.loads(record["body"])
    except (json.JSONDecodeError, Exception) as e:
        print(f"Skipping malformed message {record.get('messageId','?')}: {e}")
        return None

    if body.get("__s3_pointer__"):
        bucket = body["s3_bucket"]
        key    = body["s3_key"]
        print(f"Resolving S3 pointer: s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        tx  = json.loads(obj["Body"].read())
        s3.delete_object(Bucket=bucket, Key=key)
        return tx

    return body


def preprocess(tx: dict, features: list) -> list:
    """
    Build a single feature row in the correct column order.
    Returns a plain list (caller stacks rows into np.array).
    Missing categoricals → -1, missing numerics → 0.0
    """
    row = []
    for f in features:
        val = tx.get(f, None)
        if val is None or val == "":
            row.append(-1 if f in CAT_COLS else 0.0)
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
    return row


def enrich(tx: dict) -> dict:
    """
    Simulate identity enrichment latency (5 ms) when identity exists.
    Controlled by ENRICHMENT_ENABLED environment variable:
      - true  -> applies 5ms delay for transactions with identity data (default)
      - false -> skips enrichment entirely (ablation experiment)
    """
    if ENRICHMENT_ENABLED and tx.get("has_identity", 0) == 1:
        time.sleep(0.005)
    return tx


def handler(event, context):
    load_artifacts()

    records      = event["Records"]
    X_rows       = []
    tx_ids       = []
    ingest_times = []

    for record in records:
        ingest_ts = float(record["attributes"]["SentTimestamp"]) / 1000

        # Resolve payload -- skip malformed messages
        tx = resolve_payload(record)
        if tx is None:
            continue

        tx = enrich(tx)
        X_rows.append(preprocess(tx, _features))
        tx_ids.append(tx.get("TransactionID", record["messageId"]))
        ingest_times.append(ingest_ts)

    # If all records were malformed, return early
    if not X_rows:
        print("No valid records in batch -- all skipped")
        return {"pipeline": "A2", "scored": 0, "batch_score_ms": 0}

    X = np.array(X_rows, dtype=np.float32)

    score_start    = time.monotonic()
    probs          = _model.predict_proba(X)[:, 1]
    batch_score_ms = (time.monotonic() - score_start) * 1000
    now            = time.time()

    with dynamo.batch_writer() as batch:
        for i, (tx_id, prob) in enumerate(zip(tx_ids, probs)):
            batch.put_item(Item={
                "TransactionID"    : f"{EXPERIMENT_ID}#{tx_id}",
                "pipeline"         : "A2",
                "fraud_prob"       : str(round(float(prob), 6)),
                "prediction"       : "fraud" if prob >= 0.5 else "legit",
                "batch_score_ms"   : str(round(batch_score_ms, 3)),
                "e2e_latency_ms"   : str(round((now - ingest_times[i]) * 1000, 3)),
                "batch_size"       : str(len(records)),
                "experiment_id"    : EXPERIMENT_ID,
                "enrichment"       : "on" if ENRICHMENT_ENABLED else "off",
            })

    print(f"A2 batch scored {len(records)} records in {batch_score_ms:.1f}ms")

    return {
        "pipeline"       : "A2",
        "scored"         : len(records),
        "batch_score_ms" : round(batch_score_ms, 3)
    }
