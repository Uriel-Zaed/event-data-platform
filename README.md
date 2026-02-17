# Event Data Platform (Mini Batch Data Engineering Project)

A small production-style batch data platform that ingests raw JSONL user events, validates and deduplicates them using Spark, stores curated Parquet, and produces daily metrics.

## Architecture

Raw (JSONL) -> Curated (Parquet) -> Metrics (Parquet)

- **Raw**: `data/raw/dt=YYYY-MM-DD/events.jsonl`
- **Curated**: `data/curated/dt=YYYY-MM-DD/` (Parquet)
- **Metrics**: `data/metrics/dt=YYYY-MM-DD/`
  - `daily_metrics/` (total_events, DAU)
  - `events_by_type/` (counts per event_type)

## Jobs

- `jobs/generate_raw.py` - generate partitioned raw JSONL data
- `jobs/ingest.py` - schema validation + filtering + dedup by event_id + write curated Parquet
- `jobs/aggregate.py` - compute daily metrics from curated Parquet

## How to Run

Create raw data:
```bash
python -m jobs.generate_raw YYYY-MM-DD