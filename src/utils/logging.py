import os
import logging
from typing import Optional


def get_logger(name: str, dt: Optional[str] = None, base_dir: Optional[str] = None) -> logging.Logger:
    """
    Create a logger that writes to console + optional dt file.

    - name: logger name
    - dt: partition date (creates data/logs/dt=...)
    - base_dir: repo data directory
    """

    logger = logging.getLogger(name)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler (optional)
    if dt and base_dir:
        log_dir = os.path.join(base_dir, "logs", f"dt={dt}")
        os.makedirs(log_dir, exist_ok=True)

        fh = logging.FileHandler(os.path.join(log_dir, f"{name}.log"))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger