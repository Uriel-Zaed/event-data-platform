import argparse
from datetime import datetime

from src.jobs.generate_raw import generate_raw
from src.jobs.ingest import ingest
from src.jobs.aggregate import aggregate

def parse_args():
    p = argparse.ArgumentParser(description="Run the full batch pipeline for a single dt.")
    p.add_argument("--dt", type=str, required=False, help="Date partition YYYY-MM-DD (default: today)")
    p.add_argument("--events", type=int, default=200_000, help="Events to generate (default: 200000)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    dt = args.dt or datetime.now().strftime("%Y-%m-%d")

    generate_raw(dt=dt, events=args.events)
    ingest(dt)
    aggregate(dt)

    print(f"[pipeline] Done for dt={dt}")