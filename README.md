# Event Data Platform (Batch Pipeline with PySpark)

A production-style batch data pipeline that ingests raw event logs, performs validation and deduplication, and produces daily analytics metrics using PySpark.

This project demonstrates core Data Engineering concepts including partitioned storage, idempotent processing, audit logging, observability, and Spark performance tuning.

---

## 🚀 Features

- Partitioned raw → curated → metrics data layers
- Schema validation and data quality filtering
- Event deduplication using business keys
- Daily aggregations (DAU, event counts)
- Idempotent pipeline execution
- Audit trail per job and pipeline run
- Structured logging (console + file)
- Configurable shuffle partitions
- End-to-end orchestration via CLI

---

## 🏗️ Architecture

Raw JSONL (dt partitioned)
→
Validation + Dedup (Spark)
→
Curated Parquet (dt partitioned)
→
Daily Metrics Aggregation
→
Analytics-ready Parquet




### Data layers

| Layer   | Purpose |
|--------|--------|
| raw     | Simulated event ingestion (JSONL) |
| curated | Cleaned, validated, deduplicated events |
| metrics | Daily analytics aggregates |
| audit   | Job execution metadata |
| logs    | Structured execution logs |



---

## ⚙️ Setup

### 1. Create environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install pyspark
```

2. Run pipeline
```bash
python run_pipeline.py --dt 2026-02-17 --events 50000 --force
```

3. Re-run (idempotent)
```bash
python run_pipeline.py --dt 2026-02-17
```