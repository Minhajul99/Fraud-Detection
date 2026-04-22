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
BUCKET         = os.environ["MODEL_BUCKET"]
MODEL_KEY      = os.environ.get("MODEL_KEY",      "model/fraud_model.pkl")
FEATURES_KEY   = os.environ.get("FEATURES_KEY",   "model/feature_columns.json")
EXPERIMENT_ID  = os.environ.get("EXPERIMENT_ID",  "baseline")

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


def resolve_payload(record: dict) -> dict:
    """
    Handles the SQS 256 KB limit via S3 pointer pattern.

    If the producer stored the transaction in S3 (because it was > 200 KB),
    the SQS message body will contain:
        {"__s3_pointer__": true, "s3_bucket": "...", "s3_key": "..."}

    This function fetches the real transaction from S3 and deletes the
    temporary pointer object after reading.

    If the message is a direct payload (small transaction), it is returned
    as-is without any S3 interaction.
    """
    body = json.loads(record["body"])

    if body.get("__s3_pointer__"):
        bucket = body["s3_bucket"]
        key    = body["s3_key"]
        print(f"Resolving S3 pointer: s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        tx  = json.loads(obj["Body"].read())
        # Clean up — pointer object no longer needed
        s3.delete_object(Bucket=bucket, Key=key)
        return tx

    return body  # small payload — already the transaction dict


def preprocess(tx: dict, features: list) -> np.ndarray:
    """
    Build the exact feature vector the LightGBM model expects.
    Assembles features in correct column order.
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
    return np.array([row], dtype=np.float32)


def handler(event, context):
    load_artifacts()

    for record in event["Records"]:
        ingest_ts = float(record["attributes"]["SentTimestamp"]) / 1000

        # ── Resolve payload (handles both direct and S3-pointer messages) ──
        tx    = resolve_payload(record)
        tx_id = tx.get("TransactionID", record["messageId"])

        X = preprocess(tx, _features)

        score_start = time.monotonic()
        prob        = float(_model.predict_proba(X)[0][1])
        score_ms    = (time.monotonic() - score_start) * 1000
        e2e_ms      = (time.time() - ingest_ts) * 1000

        dynamo.put_item(Item={
            "TransactionID"    : f"{EXPERIMENT_ID}#A1#{tx_id}",
            "pipeline"         : "A1",
            "fraud_prob"       : str(round(prob, 6)),
            "prediction"       : "fraud" if prob >= 0.5 else "legit",
            "score_latency_ms" : str(round(score_ms, 3)),
            "e2e_latency_ms"   : str(round(e2e_ms, 3)),
            "experiment_id"    : EXPERIMENT_ID,
        })

        print(f"A1 scored TX={tx_id}  prob={prob:.4f}  e2e={e2e_ms:.1f}ms")

    return {"pipeline": "A1", "scored": len(event["Records"])}
