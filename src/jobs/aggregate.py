import os
import time
import argparse
from pyspark.sql import functions as F

from src.config.settings import CURATED_DIR, METRICS_DIR, DEFAULT_SHUFFLE_PARTITIONS, AUDIT_DIR
from src.utils.spark import get_spark
from src.utils.audit import write_audit

from src.utils.logging import get_logger
from src.config.settings import DATA_DIR


def _parquet_exists(path: str) -> bool:
    return os.path.isdir(path) and os.path.exists(os.path.join(path, "_SUCCESS"))


def aggregate(dt: str, force: bool = False) -> dict:
    logger = get_logger("aggregate", dt=dt, base_dir=DATA_DIR)

    curated_path = os.path.join(CURATED_DIR, f"dt={dt}")
    if not os.path.exists(curated_path):
        logger.error(f"Curated path not found: {curated_path}")
        raise FileNotFoundError(f"Curated path not found: {curated_path}")

    out_path = os.path.join(METRICS_DIR, f"dt={dt}")
    daily_out = os.path.join(out_path, "daily_metrics")
    type_out = os.path.join(out_path, "events_by_type")

    # Idempotency: skip if outputs already exist
    if (not force) and _parquet_exists(daily_out) and _parquet_exists(type_out):
        msg = f"[aggregate] dt={dt} metrics already exist, skipping (use --force to overwrite)"
        print(msg)
        audit_path = write_audit(
            audit_dir=os.path.join(AUDIT_DIR, f"dt={dt}"),
            name="aggregate",
            payload={
                "dt": dt,
                "status": "skipped",
                "reason": "metrics_exist",
                "metrics_path": out_path,
                "daily_out": daily_out,
                "events_by_type_out": type_out,
            },
        )
        logger.warning(f"[aggregate] audit: {audit_path}")
        return {"dt": dt, "status": "skipped", "metrics_path": out_path}

    os.makedirs(out_path, exist_ok=True)

    spark = get_spark(app_name=f"aggregate_{dt}", shuffle_partitions=DEFAULT_SHUFFLE_PARTITIONS)

    t0 = time.time()
    try:
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

        daily.write.mode("overwrite").parquet(daily_out)
        by_type.write.mode("overwrite").parquet(type_out)

        elapsed = time.time() - t0

        # Show output (nice for demo runs)
        logger.info("[aggregate] Daily metrics:")
        daily.show(truncate=False)

        logger.info("[aggregate] Events by type:")
        by_type.orderBy(F.col("events").desc()).show(truncate=False)

        logger.info(f"[aggregate] dt={dt} wrote: {out_path} elapsed={elapsed:.2f}s")

        # Collect the single-row daily metrics to store in audit JSON
        daily_row = daily.collect()[0].asDict()

        audit_path = write_audit(
            audit_dir=os.path.join(AUDIT_DIR, f"dt={dt}"),
            name="aggregate",
            payload={
                "dt": dt,
                "status": "success",
                "elapsed_seconds": elapsed,
                "metrics_path": out_path,
                "daily_out": daily_out,
                "events_by_type_out": type_out,
                "daily_metrics": daily_row,  # includes total_events and dau
            },
        )
        logger.info(f"[aggregate] audit: {audit_path}")

        return {"dt": dt, "status": "success", "elapsed_seconds": elapsed, "metrics_path": out_path}

    except Exception as e:
        elapsed = time.time() - t0
        audit_path = write_audit(
            audit_dir=os.path.join(AUDIT_DIR, f"dt={dt}"),
            name="aggregate",
            payload={
                "dt": dt,
                "status": "failed",
                "elapsed_seconds": elapsed,
                "error": repr(e),
                "metrics_path": out_path,
            },
        )
        logger.exception(f"[aggregate] FAILED dt={dt}. audit: {audit_path}")
        raise
    finally:
        spark.stop()


def parse_args():
    p = argparse.ArgumentParser(description="Aggregate curated Parquet -> daily metrics.")
    p.add_argument("--dt", type=str, required=True, help="Date partition YYYY-MM-DD")
    p.add_argument("--force", action="store_true", help="Overwrite outputs even if they exist")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    aggregate(args.dt, force=args.force)