import os
import json
import random
import time
import argparse
from datetime import datetime

from src.config.settings import RAW_DIR

EVENT_TYPES = ["click", "view", "purchase"]

def generate_event(day: str) -> dict:
    return {
        "event_id": random.randint(1, 2_000_000),
        "user_id": random.randint(1, 200_000),
        "event_type": random.choice(EVENT_TYPES),
        "event_ts": time.time(),
        "event_date": day,
    }

def generate_raw(dt: str, events: int) -> str:
    os.makedirs(RAW_DIR, exist_ok=True)

    day_dir = os.path.join(RAW_DIR, f"dt={dt}")
    os.makedirs(day_dir, exist_ok=True)

    out_path = os.path.join(day_dir, "events.jsonl")

    t0 = time.time()
    with open(out_path, "w", buffering=1024 * 1024) as f:
        for _ in range(events):
            f.write(json.dumps(generate_event(dt)) + "\n")

    elapsed = time.time() - t0
    print(f"[generate_raw] dt={dt} events={events:,} path={out_path} elapsed={elapsed:.2f}s")

    return out_path


def parse_args():
    p = argparse.ArgumentParser(description="Generate raw JSONL events partitioned by dt.")
    p.add_argument("--dt", type=str, required=False, help="Date partition YYYY-MM-DD (default: today)")
    p.add_argument("--events", type=int, default=200_000, help="Events to generate (default: 200000)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    dt = args.dt or datetime.now().strftime("%Y-%m-%d")
    generate_raw(dt=dt, events=args.events)