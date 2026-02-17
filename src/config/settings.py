import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

RAW_DIR = os.path.join(DATA_DIR, "raw")
CURATED_DIR = os.path.join(DATA_DIR, "curated")
METRICS_DIR = os.path.join(DATA_DIR, "metrics")

DEFAULT_SHUFFLE_PARTITIONS = 10  # your machine has 10 cores