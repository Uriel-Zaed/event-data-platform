import os
import sys
import time
from pyspark.sql import functions as F

from src.config.settings import CURATED_DIR, METRICS_DIR, DEFAULT_SHUFFLE_PARTITIONS
from src.utils.spark import get_spark


def aggregate(dt: str) -> None:
    spark = get_spark(app_name=f"aggregate_{dt}", shuffle_partitions=DEFAULT_SHUFFLE_PARTITIONS)

    curated_path = os.path.join(CURATED_DIR, f"dt={dt}")
    if not os.path.exists(curated_path):
        raise FileNotFoundError(f"Curated path not found: {curated_path}")

    out_path = os.path.join(METRICS_DIR, f"dt={dt}")
    os.makedirs(out_path, exist_ok=True)

    t0 = time.time()

    df = spark.read.parquet(curated_path)

    # Core daily metrics (single-row dataframe)
    daily = (
        df.agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("dau"),
        )
        .withColumn("event_date", F.lit(dt))
        .select("event_date", "total_events", "dau")
    )

    # Event-type breakdown (3 rows)
    by_type = (
        df.groupBy("event_type")
          .agg(F.count("*").alias("events"))
          .withColumn("event_date", F.lit(dt))
          .select("event_date", "event_type", "events")
    )

    # Write outputs
    daily.write.mode("overwrite").parquet(os.path.join(out_path, "daily_metrics"))
    by_type.write.mode("overwrite").parquet(os.path.join(out_path, "events_by_type"))

    # Print for visibility
    print("Daily metrics:")
    daily.show(truncate=False)

    print("Events by type:")
    by_type.orderBy(F.col("events").desc()).show(truncate=False)

    print(f"[dt={dt}] wrote metrics to: {out_path}")
    print(f"[dt={dt}] elapsed: {time.time() - t0:.2f}s")

    spark.stop()


if __name__ == "__main__":
    aggregate("2026-02-15")
    aggregate("2026-02-16")
    aggregate("2026-02-17")