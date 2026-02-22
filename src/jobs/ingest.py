import os
import time
import argparse
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.config.settings import RAW_DIR, CURATED_DIR, DEFAULT_SHUFFLE_PARTITIONS
from src.utils.spark import get_spark
from src.config.settings import AUDIT_DIR
from src.utils.audit import write_audit

RAW_SCHEMA = T.StructType([
    T.StructField("event_id", T.LongType(), True),
    T.StructField("user_id", T.LongType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("event_ts", T.DoubleType(), True),
    T.StructField("event_date", T.StringType(), True),
])

ALLOWED_EVENT_TYPES = ["click", "view", "purchase"]

def _parquet_exists(path: str) -> bool:
    # Spark writes a folder with part files + _SUCCESS
    return os.path.isdir(path) and os.path.exists(os.path.join(path, "_SUCCESS"))

def ingest(dt: str, force: bool = False) -> dict:
    spark = get_spark(app_name=f"ingest_{dt}", shuffle_partitions=DEFAULT_SHUFFLE_PARTITIONS)

    raw_path = os.path.join(RAW_DIR, f"dt={dt}", "events.jsonl")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    out_path = os.path.join(CURATED_DIR, f"dt={dt}")

    if (not force) and _parquet_exists(out_path):
        msg = f"[ingest] dt={dt} curated already exists, skipping (use --force to overwrite)"
        print(msg)
        audit_path = write_audit(
            audit_dir=os.path.join(AUDIT_DIR, f"dt={dt}"),
            name="ingest",
            payload={"dt": dt, "status": "skipped", "reason": "curated_exists", "curated_path": out_path},
        )
        print(f"[ingest] audit: {audit_path}")
        return {"dt": dt, "status": "skipped", "curated_path": out_path}

    os.makedirs(out_path, exist_ok=True)

    t0 = time.time()

    df = spark.read.schema(RAW_SCHEMA).json(raw_path)

    df_valid = (
        df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("event_date") == F.lit(dt))
        .filter(F.col("event_type").isin(ALLOWED_EVENT_TYPES))
    )

    df_dedup = df_valid.dropDuplicates(["event_id"])

    curated = (
        df_dedup
        .select("event_id", "user_id", "event_type", "event_ts", "event_date")
        .withColumn("ingested_at", F.current_timestamp())
    )

    curated.write.mode("overwrite").parquet(out_path)

    raw_cnt = df.count()
    valid_cnt = df_valid.count()
    dedup_cnt = curated.count()

    elapsed = time.time() - t0
    dup_removed = valid_cnt - dedup_cnt
    dup_rate = (dup_removed / valid_cnt) if valid_cnt else 0.0

    print(f"[ingest] dt={dt} raw={raw_cnt:,} valid={valid_cnt:,} dedup={dedup_cnt:,} dup_rate={dup_rate:.4f} elapsed={elapsed:.2f}s")
    print(f"[ingest] wrote: {out_path}")

    spark.stop()

    audit_path = write_audit(
        audit_dir=os.path.join(AUDIT_DIR, f"dt={dt}"),
        name="ingest",
        payload={
            "dt": dt,
            "status": "success",
            "raw": raw_cnt,
            "valid": valid_cnt,
            "dedup": dedup_cnt,
            "dup_removed": dup_removed,
            "dup_rate": dup_rate,
            "elapsed_seconds": elapsed,
            "raw_path": raw_path,
            "curated_path": out_path,
        },
    )
    print(f"[ingest] audit: {audit_path}")

    return {
        "dt": dt,
        "raw": raw_cnt,
        "valid": valid_cnt,
        "dedup": dedup_cnt,
        "dup_removed": dup_removed,
        "dup_rate": dup_rate,
        "elapsed_seconds": elapsed,
        "curated_path": out_path,
    }

def parse_args():
    p = argparse.ArgumentParser(description="Ingest raw JSONL -> curated Parquet (validate + dedup).")
    p.add_argument("--dt", type=str, required=True, help="Date partition YYYY-MM-DD")
    p.add_argument("--force", action="store_true", help="Overwrite curated output if it exists")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    ingest(args.dt, force=args.force)