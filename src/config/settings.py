import os


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

DATA_DIR = os.path.join(BASE_DIR, "data")

RAW_DIR = os.path.join(DATA_DIR, "raw")
CURATED_DIR = os.path.join(DATA_DIR, "curated")
METRICS_DIR = os.path.join(DATA_DIR, "metrics")
AUDIT_DIR = os.path.join(DATA_DIR, "audit")

DEFAULT_SHUFFLE_PARTITIONS = 10