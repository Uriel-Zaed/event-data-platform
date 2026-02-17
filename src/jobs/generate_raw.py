import os
import json
import random
import time
from datetime import datetime, timedelta

from src.config.settings import RAW_DIR

EVENT_TYPES = ["click", "view", "purchase"]

def generate_event(day: str) -> dict:
    return {
        "event_id": random.randint(1, 2_000_000),
        "user_id": random.randint(1, 200_000),
        "event_type": random.choice(EVENT_TYPES),
        "event_ts": time.time(),
        "event_date": day,  # string YYYY-MM-DD
    }

def generate_raw(days: int = 3, events_per_day: int = 500_000) -> None:
    os.makedirs(RAW_DIR, exist_ok=True)

    start_day = datetime.now().date() - timedelta(days=days - 1)

    for i in range(days):
        day = (start_day + timedelta(days=i)).strftime("%Y-%m-%d")
        day_dir = os.path.join(RAW_DIR, f"dt={day}")
        os.makedirs(day_dir, exist_ok=True)

        out_path = os.path.join(day_dir, "events.jsonl")
        with open(out_path, "w", buffering=1024 * 1024) as f:
            for _ in range(events_per_day):
                f.write(json.dumps(generate_event(day)) + "\n")

        print(f"Wrote {events_per_day} events to {out_path}")

if __name__ == "__main__":
    start = time.time()
    generate_raw(days=3, events_per_day=200_000)
    end = time.time()
    print(f"Took {end - start} seconds")