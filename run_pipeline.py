import os
import time
import argparse
from datetime import datetime

from src.jobs.generate_raw import generate_raw
from src.jobs.ingest import ingest
from src.jobs.aggregate import aggregate
from src.config.settings import AUDIT_DIR, CURATED_DIR
from src.utils.audit import write_audit

from src.utils.logging import get_logger
from src.config.settings import DATA_DIR

def _parquet_exists(path: str) -> bool:
    return os.path.isdir(path) and os.path.exists(os.path.join(path, "_SUCCESS"))


def parse_args():
    p = argparse.ArgumentParser(description="Run the full batch pipeline for a single dt.")
    p.add_argument("--dt", type=str, required=False, help="Date partition YYYY-MM-DD (default: today)")
    p.add_argument("--events", type=int, default=200_000, help="Events to generate (default: 200000)")
    p.add_argument("--force", action="store_true", help="Overwrite outputs even if they exist")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    dt = args.dt or datetime.now().strftime("%Y-%m-%d")

    logger = get_logger("pipeline", dt=dt, base_dir=DATA_DIR)

    t0 = time.time()

    curated_path = os.path.join(CURATED_DIR, f"dt={dt}")
    curated_exists = _parquet_exists(curated_path)

    # Consistency rule:
    # - If curated exists and not forcing, don't regenerate raw (avoid mismatch)
    # - Otherwise generate raw and force downstream to match the new raw
    generated = False
    downstream_force = args.force

    try:
        raw_path = None
        if args.force or (not curated_exists):
            raw_path = generate_raw(dt=dt, events=args.events)
            generated = True
            downstream_force = True
        else:
            raw_path = os.path.join(os.path.abspath("data"), "raw", f"dt={dt}", "events.jsonl")

        ingest_res = ingest(dt, force=downstream_force)
        agg_res = aggregate(dt, force=downstream_force)

        elapsed = time.time() - t0

        audit_path = write_audit(
            audit_dir=os.path.join(AUDIT_DIR, f"dt={dt}"),
            name="pipeline",
            payload={
                "dt": dt,
                "status": "success",
                "elapsed_seconds": elapsed,
                "events_requested": args.events,
                "generated_raw": generated,
                "downstream_force": downstream_force,
                "raw_path": raw_path,
                "ingest": ingest_res,
                "aggregate": agg_res,
            },
        )
        logger.info(f"[pipeline] Done dt={dt} elapsed={elapsed:.2f}s audit={audit_path}")

    except Exception as e:
        elapsed = time.time() - t0
        audit_path = write_audit(
            audit_dir=os.path.join(AUDIT_DIR, f"dt={dt}"),
            name="pipeline",
            payload={
                "dt": dt,
                "status": "failed",
                "elapsed_seconds": elapsed,
                "events_requested": args.events,
                "error": repr(e),
            },
        )
        logger.exception(f"[pipeline] FAILED dt={dt} elapsed={elapsed:.2f}s audit={audit_path}")
        raise