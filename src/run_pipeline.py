import sys
from jobs.generate_raw import generate_raw
from jobs.ingest import ingest
from jobs.aggregate import aggregate

if __name__ == "__main__":
    dt = sys.argv[1]

    generate_raw(days=1, events_per_day=200_000)

    ingest(dt)
    aggregate(dt)