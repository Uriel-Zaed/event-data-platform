import os
import time
import argparse
from pyspark.sql import functions as F

from src.config.settings import CURATED_DIR, METRICS_DIR, DEFAULT_SHUFFLE_PARTITIONS
from src.utils.spark import get_spark

def aggregate(dt: str) -> dict:
    spark = get_spark(app_name=f"aggregate_{dt}", shuffle_partitions=DEFAULT_SHUFFLE_PARTITIONS)

    curated_path = os.path.join(CURATED_DIR, f"dt={dt}")
    if not os.path.exists(curated_path):
        raise FileNotFoundError(f"Curated path not found: {curated_path}")

    out_path = os.path.join(METRICS_DIR, f"dt={dt}")
    os.makedirs(out_path, exist_ok=True)

    t0 = time.time()
    df = spark.read.parquet(curated_path)

    daily = (
        df.agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("dau"),
        )
        .withColumn("event_date", F.lit(dt))
        .select("event_date", "total_events", "dau")
    )

    by_type = (
        df.groupBy("event_type")
          .agg(F.count("*").alias("events"))
          .withColumn("event_date", F.lit(dt))
          .select("event_date", "event_type", "events")
    )

    daily_out = os.path.join(out_path, "daily_metrics")
    type_out = os.path.join(out_path, "events_by_type")

    daily.write.mode("overwrite").parquet(daily_out)
    by_type.write.mode("overwrite").parquet(type_out)

    elapsed = time.time() - t0

    print("[aggregate] Daily metrics:")
    daily.show(truncate=False)
    print("[aggregate] Events by type:")
    by_type.orderBy(F.col("events").desc()).show(truncate=False)
    print(f"[aggregate] dt={dt} wrote: {out_path} elapsed={elapsed:.2f}s")

    spark.stop()

    return {
        "dt": dt,
        "elapsed_seconds": elapsed,
        "metrics_path": out_path,
    }

def parse_args():
    p = argparse.ArgumentParser(description="Aggregate curated Parquet -> daily metrics.")
    p.add_argument("--dt", type=str, required=True, help="Date partition YYYY-MM-DD")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    aggregate(args.dt)