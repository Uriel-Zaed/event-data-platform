import os
import sys
import time
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.config.settings import RAW_DIR, CURATED_DIR, DEFAULT_SHUFFLE_PARTITIONS
from src.utils.spark import get_spark


RAW_SCHEMA = T.StructType([
    T.StructField("event_id", T.LongType(), True),
    T.StructField("user_id", T.LongType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("event_ts", T.DoubleType(), True),
    T.StructField("event_date", T.StringType(), True),
])

ALLOWED_EVENT_TYPES = ["click", "view", "purchase"]


def ingest(dt: str) -> None:
    spark = get_spark(app_name=f"ingest_{dt}", shuffle_partitions=DEFAULT_SHUFFLE_PARTITIONS)

    raw_path = os.path.join(RAW_DIR, f"dt={dt}", "events.jsonl")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    out_path = os.path.join(CURATED_DIR, f"dt={dt}")

    t0 = time.time()

    df = (
        spark.read
        .schema(RAW_SCHEMA)
        .json(raw_path)
    )

    # Basic validation rules
    df_valid = (
        df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("event_date") == F.lit(dt))
        .filter(F.col("event_type").isin(ALLOWED_EVENT_TYPES))
    )

    # Dedup (global within the day)
    df_dedup = df_valid.dropDuplicates(["event_id"])

    # Keep curated columns + add ingestion timestamp
    curated = (
        df_dedup
        .select("event_id", "user_id", "event_type", "event_ts", "event_date")
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Write curated parquet
    os.makedirs(out_path, exist_ok=True)
    (
        curated
        .write
        .mode("overwrite")
        .parquet(out_path)
    )

    # Quick stats (forces execution)
    raw_cnt = df.count()
    valid_cnt = df_valid.count()
    dedup_cnt = curated.count()

    print(f"[dt={dt}] raw={raw_cnt:,} valid={valid_cnt:,} dedup={dedup_cnt:,}")
    print(f"[dt={dt}] wrote parquet to: {out_path}")
    print(f"[dt={dt}] elapsed: {time.time() - t0:.2f}s")

    spark.stop()


if __name__ == "__main__":
    ingest("2026-02-15")
    ingest("2026-02-16")
    ingest("2026-02-17")